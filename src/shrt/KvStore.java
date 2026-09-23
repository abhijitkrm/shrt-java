package shrt;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * External-KV backend (DragonflyDB / Redis / any RESP server). The corpus
 * lives in the KV store; this process keeps only a bounded hot FIFO cache
 * + batched hit counters — memory stays flat as links grow.
 *
 * Keys:  l:{code} -> "{expires_ms}|{created_ms}|{url}"  (PX self-evicts)
 *        h:{code} -> hit counter (INCRBY, flushed in 5ms batches)
 *
 * Multi-instance: the KV IS the shared state — no tailing, no convergence,
 * admin mutations work on any node.
 */
public final class KvStore implements StoreApi {
    private static final int SHARDS = 256;
    private static final char[] ALPHABET =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    private final Kv kv;
    private final int instance;
    private final char prefix;
    private final long cacheTtlMs;
    private final int capPerShard;
    private final CacheShard[] cache = new CacheShard[SHARDS];
    private final Object[] dirtyLocks = new Object[SHARDS];
    @SuppressWarnings("unchecked")
    private final Map<String, Long>[] dirty = new Map[SHARDS];
    private volatile boolean stop;
    private final Thread flusher;
    private final Thread janitor;
    private final boolean layoutHash;  // KV_LAYOUT=hash
    private final long buckets;        // KV_BUCKETS
    private final java.util.concurrent.ThreadLocalRandom rng = null;

    private static final class CacheEntry {
        String u; long e; long at;
        CacheEntry(String u, long e, long at) { this.u = u; this.e = e; this.at = at; }
    }
    private static final class CacheShard {
        final HashMap<String, CacheEntry> m = new HashMap<>();
        final ArrayDeque<String> order = new ArrayDeque<>();
    }

    public KvStore(String addr, int instance, int cacheEntries, long cacheTtlMs) throws IOException {
        kv = new Kv(addr, 16);
        this.instance = Math.max(0, Math.min(61, instance));
        this.prefix = ALPHABET[this.instance];
        this.cacheTtlMs = cacheTtlMs;
        this.capPerShard = Math.max(16, cacheEntries / SHARDS);
        for (int i = 0; i < SHARDS; i++) {
            cache[i] = new CacheShard();
            dirtyLocks[i] = new Object();
            dirty[i] = new HashMap<>();
        }
        flusher = new Thread(() -> {
            while (!stop) {
                try { Thread.sleep(5); } catch (InterruptedException ie) { return; }
                try { flushHits(); } catch (IOException ignored) {}
            }
        }, "kv-flush");
        flusher.setDaemon(true);
        flusher.start();
        layoutHash = "hash".equals(System.getenv("KV_LAYOUT"));
        long b = 1_000_000;
        String bv = System.getenv("KV_BUCKETS");
        if (bv != null) try { b = Math.max(1, Long.parseLong(bv)); } catch (NumberFormatException ignored) {}
        buckets = b;
        long sweepMs = 3_600_000;
        String sv = System.getenv("KV_SWEEP_MS");
        if (sv != null) try { sweepMs = Math.max(50, Long.parseLong(sv)); } catch (NumberFormatException ignored) {}
        if (layoutHash) {
            long sm = sweepMs;
            janitor = new Thread(() -> {
                long waited = 0;
                while (!stop) {
                    try { Thread.sleep(50); } catch (InterruptedException ie) { return; }
                    waited += 50;
                    if (waited >= sm) {
                        waited = 0;
                        try { sweepExpired(); } catch (IOException ignored) {}
                    }
                }
            }, "kv-janitor");
            janitor.setDaemon(true);
            janitor.start();
        } else {
            janitor = null;
        }
    }

    private String bkey(String code) {
        return "l:" + ((shardOf(code) & 0xFFFFFFFFL) % buckets);
    }
    private static String hfield(String c) { return "h:" + c; }

    private byte[] kvGet(String code) throws IOException {
        return layoutHash ? kv.hget(bkey(code), code) : kv.get(lkey(code));
    }

    // janitor: HDEL fields whose embedded expiry has passed (no PX on fields)
    private void sweepExpired() throws IOException {
        List<String> bucketList = new ArrayList<>();
        kv.scanEach("l:*", bucketList::add);
        long now = nowMs();
        List<String[]> dels = new ArrayList<>();
        for (String b : bucketList) {
            List<String> dead = new ArrayList<>();
            kv.hscanEach(b, (f, v) -> {
                if (f.startsWith("h:")) return;
                long[] ec = dec(v);
                if (ec != null && ec[0] != 0 && ec[0] <= now) dead.add(f);
            });
            for (String f : dead) dels.add(new String[]{"HDEL", b, f});
        }
        if (!dels.isEmpty()) kv.pipe(dels);
    }

    private static long nowMs() { return System.currentTimeMillis(); }

    private static int shardOf(String code) {
        int h = (int) 2166136261L;
        for (int i = 0; i < code.length(); i++) {
            h ^= code.charAt(i);
            h *= 16777619;
        }
        return h & (SHARDS - 1);
    }

    private static String lkey(String c) { return "l:" + c; }
    private static String hkey(String c) { return "h:" + c; }

    // "v1|{e}|{c}|{u}" — legacy "{e}|{c}|{u}" and "{e}|{u}" decode with c=0
    private static String enc(long e, long c, String u) { return "v1|" + e + "|" + c + "|" + u; }

    private static long[] dec(String v) {
        if (v.startsWith("v1|")) v = v.substring(3);
        int p = v.indexOf('|');
        if (p < 0) return null;
        try {
            long e = Long.parseLong(v.substring(0, p));
            String rest = v.substring(p + 1);
            int q = rest.indexOf('|');
            if (q >= 0)
                return new long[]{e, Long.parseLong(rest.substring(0, q))};
            return new long[]{e, 0};
        } catch (NumberFormatException ex) {
            return null;
        }
    }
    private static String decUrl(String v) {
        if (v.startsWith("v1|")) v = v.substring(3);
        int p = v.indexOf('|');
        if (p < 0) return null;
        String rest = v.substring(p + 1);
        int q = rest.indexOf('|');
        return q >= 0 ? rest.substring(q + 1) : rest;
    }

    private void flushHits() throws IOException {
        if (layoutHash) {
            List<String[]> deltas = new ArrayList<>();
            for (int i = 0; i < SHARDS; i++) {
                synchronized (dirtyLocks[i]) {
                    for (var e : dirty[i].entrySet())
                        deltas.add(new String[]{bkey(e.getKey()), hfield(e.getKey()),
                                                Long.toString(e.getValue())});
                    dirty[i].clear();
                }
            }
            kv.hincrbyMany(deltas);
            return;
        }
        List<String[]> deltas = new ArrayList<>();
        for (int i = 0; i < SHARDS; i++) {
            synchronized (dirtyLocks[i]) {
                for (var e : dirty[i].entrySet())
                    deltas.add(new String[]{hkey(e.getKey()), Long.toString(e.getValue())});
                dirty[i].clear();
            }
        }
        kv.incrbyMany(deltas);
    }

    private void bump(String code) {
        if (!trackHits()) return;
        int i = shardOf(code);
        synchronized (dirtyLocks[i]) {
            dirty[i].merge(code, 1L, Long::sum);
        }
    }

    private static boolean trackHits() {
        String h = System.getenv("TRACK_HITS");
        return h == null || !h.equals("0");
    }

    private String cacheGet(String code) {
        CacheShard sh = cache[shardOf(code)];
        synchronized (sh) {
            CacheEntry e = sh.m.get(code);
            if (e == null) return null;
            long now = nowMs();
            if (cacheTtlMs > 0 && now - e.at > cacheTtlMs) {
                sh.m.remove(code);
                return null;
            }
            if (e.e != 0 && e.e <= now) return null;
            return e.u;
        }
    }
    private void cachePut(String code, String u, long e) {
        CacheShard sh = cache[shardOf(code)];
        synchronized (sh) {
            if (sh.m.containsKey(code)) {
                sh.m.get(code).u = u;
                sh.m.get(code).e = e;
                sh.m.get(code).at = nowMs();
                return;
            }
            while (sh.m.size() >= capPerShard && !sh.order.isEmpty())
                sh.m.remove(sh.order.pollFirst());
            sh.order.addLast(code);
            sh.m.put(code, new CacheEntry(u, e, nowMs()));
        }
    }
    private void cacheDel(String code) {
        CacheShard sh = cache[shardOf(code)];
        synchronized (sh) { sh.m.remove(code); }
    }

    private String genCode() {
        var r = java.util.concurrent.ThreadLocalRandom.current();
        StringBuilder c = new StringBuilder(8);
        c.append(prefix);
        for (int i = 1; i < 8; i++) c.append(ALPHABET[r.nextInt(62)]);
        return c.toString();
    }

    @Override
    public String resolve(String code) {
        String u = cacheGet(code);
        if (u != null) { Metrics.cacheHit(); bump(code); return u; }
        Metrics.cacheMiss();
        long t0 = System.nanoTime();
        byte[] v;
        try { v = kvGet(code); }
        catch (IOException e) { return null; }
        finally { Metrics.storeRead((System.nanoTime() - t0) / 1000); }
        if (v == null) return null;
        String vs = new String(v, java.nio.charset.StandardCharsets.UTF_8);
        long[] ec = dec(vs);
        String us = decUrl(vs);
        if (ec == null || us == null) return null;
        long e = ec[0];
        if (e != 0 && e <= nowMs()) return null;
        cachePut(code, us, e);
        bump(code);
        return us;
    }

    @Override
    public String shorten(String url, String alias, long ttlMs) {
        Metrics.storeWrite();
        long now = nowMs();
        long exp = ttlMs > 0 ? now + ttlMs : 0;
        try {
            if (alias != null) {
                return (layoutHash
                        ? kv.hsetnx(bkey(alias), alias, enc(exp, now, url))
                        : kv.set(lkey(alias), enc(exp, now, url), ttlMs, true)) ? alias : null;
            }
            for (;;) {
                String c = genCode();
                boolean ok = layoutHash
                        ? kv.hsetnx(bkey(c), c, enc(exp, now, url))
                        : kv.set(lkey(c), enc(exp, now, url), ttlMs, true);
                if (ok) return c;
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public List<String> shortenMany(List<String> urls, long ttlMs) {
        long now = nowMs();
        long exp = ttlMs > 0 ? now + ttlMs : 0;
        List<String> codes = new ArrayList<>(urls.size());
        List<String[]> cmds = new ArrayList<>(urls.size());
        for (String u : urls) {
            String c = genCode();
            codes.add(c);
            if (layoutHash) {
                cmds.add(new String[]{"HSETNX", bkey(c), c, enc(exp, now, u)});
            } else {
                List<String> a = new ArrayList<>(List.of("SET", lkey(c), enc(exp, now, u)));
                if (ttlMs > 0) { a.add("PX"); a.add(Long.toString(ttlMs)); }
                a.add("NX");
                cmds.add(a.toArray(new String[0]));
            }
        }
        List<Kv.Resp> rs;
        try { rs = kv.pipe(cmds); }
        catch (IOException e) { rs = List.of(); }
        for (int i = 0; i < urls.size(); i++) {
            boolean ok = layoutHash
                ? (i < rs.size() && rs.get(i).kind == ':' && rs.get(i).num == 1)
                : (i < rs.size() && rs.get(i).kind == '+' && "OK".equals(rs.get(i).text()));
            if (!ok) {
                String c2 = shorten(urls.get(i), null, ttlMs);
                if (c2 != null) codes.set(i, c2);
            }
        }
        return codes;
    }

    @Override
    public Store.MutResult update(String code, String url, long ttlMs, boolean hasTtl) {
        byte[] v;
        try { v = kvGet(code); }
        catch (IOException e) { return Store.MutResult.MISSING; }
        if (v == null) return Store.MutResult.MISSING;
        String vs = new String(v, java.nio.charset.StandardCharsets.UTF_8);
        long[] ec = dec(vs);
        if (ec == null) return Store.MutResult.MISSING;
        long exp = hasTtl ? (ttlMs > 0 ? nowMs() + ttlMs : 0) : ec[0];
        try {
            if (layoutHash) {
                kv.hset(bkey(code), code, enc(exp, ec[1], url));
            } else {
                long px = exp > 0 ? exp - nowMs() : 0;
                if (!kv.set(lkey(code), enc(exp, ec[1], url), px, false))
                    return Store.MutResult.MISSING;
            }
        } catch (IOException e) {
            return Store.MutResult.MISSING;
        }
        cacheDel(code);
        return Store.MutResult.OK;
    }

    @Override
    public Store.MutResult remove(String code) {
        if (layoutHash) {
            String b = bkey(code);
            long n;
            try { n = kv.hdel(b, code); }
            catch (IOException e) { return Store.MutResult.MISSING; }
            if (n <= 0) return Store.MutResult.MISSING;
            try { kv.hdel(b, hfield(code)); } catch (IOException ignored) {}
        } else {
            long n;
            try { n = kv.del(lkey(code)); }
            catch (IOException e) { return Store.MutResult.MISSING; }
            if (n <= 0) return Store.MutResult.MISSING;
            try { kv.del(hkey(code)); } catch (IOException ignored) {}
        }
        cacheDel(code);
        return Store.MutResult.OK;
    }

    @Override
    public Store.Pair<List<Store.Link>, Integer> list(int limit, int offset, String sort, String q) {
        List<String> keys = new ArrayList<>();
        try { kv.scanEach("l:*", keys::add); }
        catch (IOException e) { return new Store.Pair<>(List.of(), 0); }
        if (layoutHash) {
            Map<String, Long> hits = new HashMap<>();
            List<String[]> rows = new ArrayList<>(); // {code, url, e, c}
            long now = nowMs();
            for (String b : keys) {
                try {
                    kv.hscanEach(b, (f, v) -> {
                        if (f.startsWith("h:")) {
                            try { hits.put(f.substring(2), Long.parseLong(v)); }
                            catch (NumberFormatException ignored) {}
                            return;
                        }
                        long[] ec = dec(v);
                        String u = decUrl(v);
                        if (ec == null || u == null) return;
                        if (ec[0] != 0 && ec[0] <= now) return;
                        if (!q.isEmpty() && !f.contains(q) && !u.contains(q)) return;
                        rows.add(new String[]{f, u, Long.toString(ec[0]), Long.toString(ec[1])});
                    });
                } catch (IOException ignored) {}
            }
            List<Store.Link> items = new ArrayList<>(rows.size());
            for (String[] r : rows) {
                long e = Long.parseLong(r[2]);
                items.add(new Store.Link(r[0], r[1],
                        hits.getOrDefault(r[0], 0L),
                        Long.parseLong(r[3]), e != 0 ? e : null));
            }
            if ("hits".equals(sort))
                items.sort(Comparator.comparingLong(Store.Link::hits).reversed());
            int total = items.size();
            if (offset > total) offset = total;
            int end = Math.min(total, offset + limit);
            return new Store.Pair<>(items.subList(offset, end), total);
        }
        List<String[]> cmds = new ArrayList<>(keys.size() * 2);
        for (String k : keys) {
            cmds.add(new String[]{"GET", k});
            cmds.add(new String[]{"GET", hkey(k.substring(2))});
        }
        List<Kv.Resp> rs;
        try { rs = kv.pipe(cmds); }
        catch (IOException e) { rs = List.of(); }
        List<Store.Link> items = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String code = keys.get(i).substring(2);
            if (2 * i >= rs.size() || rs.get(2 * i).isNull()) continue;
            String vs = rs.get(2 * i).text();
            long[] ec = dec(vs);
            String u = decUrl(vs);
            if (ec == null || u == null) continue;
            if (!q.isEmpty() && !code.contains(q) && !u.contains(q)) continue;
            long hits = 0;
            if (2 * i + 1 < rs.size() && !rs.get(2 * i + 1).isNull()) {
                try { hits = Long.parseLong(rs.get(2 * i + 1).text()); }
                catch (NumberFormatException ignored) {}
            }
            items.add(new Store.Link(code, u, hits, ec[1], ec[0] != 0 ? ec[0] : null));
        }
        if ("hits".equals(sort))
            items.sort(Comparator.comparingLong(Store.Link::hits).reversed());
        int total = items.size();
        if (offset > total) offset = total;
        int end = Math.min(total, offset + limit);
        return new Store.Pair<>(items.subList(offset, end), total);
    }

    @Override
    public Store.Link stats(String code) {
        byte[] v;
        try { v = kvGet(code); }
        catch (IOException e) { return null; }
        if (v == null) return null;
        String vs = new String(v, java.nio.charset.StandardCharsets.UTF_8);
        long[] ec = dec(vs);
        String u = decUrl(vs);
        if (ec == null || u == null) return null;
        long hits = 0;
        try {
            byte[] hv = layoutHash ? kv.hget(bkey(code), hfield(code)) : kv.get(hkey(code));
            if (hv != null)
                hits = Long.parseLong(new String(hv, java.nio.charset.StandardCharsets.UTF_8));
        } catch (IOException | NumberFormatException ignored) {}
        int i = shardOf(code);
        synchronized (dirtyLocks[i]) {
            hits += dirty[i].getOrDefault(code, 0L);
        }
        return new Store.Link(code, u, hits, ec[1], ec[0] != 0 ? ec[0] : null);
    }

    @Override
    public int seed(List<String> urls) {
        shortenMany(urls, 0);
        try { flushHits(); } catch (IOException ignored) {}
        return urls.size();
    }

    @Override
    /** /api/health probe: RESP PING round-trip. */
    public boolean healthy() {
        try {
            return "PONG".equals(kv.cmd("PING").text());
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    public boolean isEmpty() {
        boolean[] any = {false};
        try { kv.scanEach("l:*", k -> any[0] = true); }
        catch (IOException e) { return true; }
        return !any[0];
    }

    @Override public int instance() { return instance; }
    @Override public boolean persistent() { return true; }
    @Override public void pollTailsNow() {}
    @Override public void flush() {
        try { flushHits(); } catch (IOException ignored) {}
    }
    @Override public void compact() {}
    @Override public void close() {
        stop = true;
        flusher.interrupt();
        try { flusher.join(500); } catch (InterruptedException ignored) {}
        try { flushHits(); } catch (IOException ignored) {}
    }
}
