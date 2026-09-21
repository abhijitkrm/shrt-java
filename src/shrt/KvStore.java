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

    private static String enc(long e, long c, String u) { return e + "|" + c + "|" + u; }

    // "{e}|{c}|{u}" — legacy "{e}|{u}" decodes with c=0
    private static long[] dec(String v) {
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
        int p = v.indexOf('|');
        if (p < 0) return null;
        String rest = v.substring(p + 1);
        int q = rest.indexOf('|');
        return q >= 0 ? rest.substring(q + 1) : rest;
    }

    private void flushHits() throws IOException {
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
        if (u != null) { bump(code); return u; }
        byte[] v;
        try { v = kv.get(lkey(code)); }
        catch (IOException e) { return null; }
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
        long now = nowMs();
        long exp = ttlMs > 0 ? now + ttlMs : 0;
        try {
            if (alias != null) {
                return kv.set(lkey(alias), enc(exp, now, url), ttlMs, true) ? alias : null;
            }
            for (;;) {
                String c = genCode();
                if (kv.set(lkey(c), enc(exp, now, url), ttlMs, true)) return c;
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
            List<String> a = new ArrayList<>(List.of("SET", lkey(c), enc(exp, now, u)));
            if (ttlMs > 0) { a.add("PX"); a.add(Long.toString(ttlMs)); }
            a.add("NX");
            cmds.add(a.toArray(new String[0]));
        }
        List<Kv.Resp> rs;
        try { rs = kv.pipe(cmds); }
        catch (IOException e) { rs = List.of(); }
        for (int i = 0; i < urls.size(); i++) {
            boolean ok = i < rs.size() && rs.get(i).kind == '+' && "OK".equals(rs.get(i).text());
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
        try { v = kv.get(lkey(code)); }
        catch (IOException e) { return Store.MutResult.MISSING; }
        if (v == null) return Store.MutResult.MISSING;
        String vs = new String(v, java.nio.charset.StandardCharsets.UTF_8);
        long[] ec = dec(vs);
        if (ec == null) return Store.MutResult.MISSING;
        long exp = hasTtl ? (ttlMs > 0 ? nowMs() + ttlMs : 0) : ec[0];
        long px = exp > 0 ? exp - nowMs() : 0;
        try {
            if (!kv.set(lkey(code), enc(exp, ec[1], url), px, false))
                return Store.MutResult.MISSING;
        } catch (IOException e) {
            return Store.MutResult.MISSING;
        }
        cacheDel(code);
        return Store.MutResult.OK;
    }

    @Override
    public Store.MutResult remove(String code) {
        long n;
        try { n = kv.del(lkey(code)); }
        catch (IOException e) { return Store.MutResult.MISSING; }
        if (n <= 0) return Store.MutResult.MISSING;
        try { kv.del(hkey(code)); } catch (IOException ignored) {}
        cacheDel(code);
        return Store.MutResult.OK;
    }

    @Override
    public Store.Pair<List<Store.Link>, Integer> list(int limit, int offset, String sort, String q) {
        List<String> keys = new ArrayList<>();
        try { kv.scanEach("l:*", keys::add); }
        catch (IOException e) { return new Store.Pair<>(List.of(), 0); }
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
        try { v = kv.get(lkey(code)); }
        catch (IOException e) { return null; }
        if (v == null) return null;
        String vs = new String(v, java.nio.charset.StandardCharsets.UTF_8);
        long[] ec = dec(vs);
        String u = decUrl(vs);
        if (ec == null || u == null) return null;
        long hits = 0;
        try {
            byte[] hv = kv.get(hkey(code));
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
