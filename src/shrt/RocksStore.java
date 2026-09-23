// RocksStore.java — embedded RocksDB backend (STORE=rocksdb).
//
// Schema: CF "links" (code -> "{exp}|{created}|{url}") and CF "hits"
// (code -> u64 via the built-in UInt64AddOperator merge — counters
// accumulate without read-modify-write, flushed as a WriteBatch of merge
// operands every 5 ms). Expiry is embedded in the value and enforced on
// read; a sweep thread (ROCKSDB_SWEEP_MS, default 1h) deletes expired keys
// (rocksdbjni does not expose compaction filters). Reads go through a
// bounded hot FIFO cache so the DB only sees misses; bloom filters make
// absent lookups cheap.
//
// Embedded means single-writer: RocksDB holds an exclusive LOCK on the DB
// dir — one process per ROCKSDB_PATH. For multi-instance/multi-node use
// the RESP KV backend.

package shrt;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.UInt64AddOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public final class RocksStore implements StoreApi {
    static { RocksDB.loadLibrary(); } // natives must load before any RocksObject ctor

    private static final int SHARDS = 256;
    private static final char[] ALPHABET =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final byte[] CF_LINKS = "links".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CF_HITS = "hits".getBytes(StandardCharsets.UTF_8);

    private final RocksDB db;
    private final ColumnFamilyHandle links;
    private final ColumnFamilyHandle hits;
    private final WriteOptions wo = new WriteOptions().setSync(false);
    private final ReadOptions ro = new ReadOptions();
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
    private final Thread sweeper;

    private static final class CacheEntry {
        String u; long e; long at;
        CacheEntry(String u, long e, long at) { this.u = u; this.e = e; this.at = at; }
    }
    private static final class CacheShard {
        final HashMap<String, CacheEntry> m = new HashMap<>();
        final ArrayDeque<String> order = new ArrayDeque<>();
    }

    public RocksStore(String path, int instance, int cacheEntries, long cacheTtlMs)
            throws IOException {
        this.instance = Math.max(0, Math.min(61, instance));
        this.prefix = ALPHABET[this.instance];
        this.cacheTtlMs = cacheTtlMs;
        this.capPerShard = Math.max(16, cacheEntries / SHARDS);
        for (int i = 0; i < SHARDS; i++) {
            cache[i] = new CacheShard();
            dirtyLocks[i] = new Object();
            dirty[i] = new HashMap<>();
        }

        try {
            BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                    .setFilterPolicy(new BloomFilter(10))
                    .setBlockCache(new LRUCache(64 << 20))
                    .setCacheIndexAndFilterBlocks(false);
            ColumnFamilyOptions linksOpt = new ColumnFamilyOptions()
                    .setTableFormatConfig(tbl)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION);
            ColumnFamilyOptions hitsOpt = new ColumnFamilyOptions()
                    .setMergeOperator(new UInt64AddOperator());
            Options opt = new Options().setCreateIfMissing(true);

            List<byte[]> cfNames;
            try {
                cfNames = RocksDB.listColumnFamilies(opt, path);
            } catch (RocksDBException e) {
                cfNames = List.of();
            }
            List<ColumnFamilyDescriptor> desc = new ArrayList<>();
            if (cfNames.isEmpty()) {
                desc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                        new ColumnFamilyOptions()));
                desc.add(new ColumnFamilyDescriptor(CF_LINKS, linksOpt));
                desc.add(new ColumnFamilyDescriptor(CF_HITS, hitsOpt));
            } else {
                for (byte[] n : cfNames) {
                    String ns = new String(n, StandardCharsets.UTF_8);
                    desc.add(new ColumnFamilyDescriptor(n,
                            ns.equals("hits") ? hitsOpt : linksOpt));
                }
            }
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            DBOptions dbo = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
            db = RocksDB.open(dbo, path, desc, handles);
            ColumnFamilyHandle l = null, h = null;
            for (ColumnFamilyHandle hd : handles) {
                String n = new String(hd.getName(), StandardCharsets.UTF_8);
                if (n.equals("links")) l = hd;
                else if (n.equals("hits")) h = hd;
                else hd.close();
            }
            if (l == null || h == null) throw new IOException("rocksdb: missing CFs");
            links = l; hits = h;
        } catch (RocksDBException e) {
            throw new IOException("rocksdb open: " + e.getMessage(), e);
        }

        flusher = new Thread(() -> {
            while (!stop) {
                try { Thread.sleep(5); } catch (InterruptedException ie) { return; }
                try { flushHits(); } catch (Exception ignored) {}
            }
        }, "rocks-flush");
        flusher.setDaemon(true);
        flusher.start();

        long sweepMs = 3_600_000;
        String sv = System.getenv("ROCKSDB_SWEEP_MS");
        if (sv != null) try { sweepMs = Math.max(50, Long.parseLong(sv)); } catch (NumberFormatException ignored) {}
        final long sm = sweepMs;
        sweeper = new Thread(() -> {
            long waited = 0;
            while (!stop) {
                try { Thread.sleep(50); } catch (InterruptedException ie) { return; }
                waited += 50;
                if (waited >= sm) {
                    waited = 0;
                    try { sweepExpired(); } catch (Exception ignored) {}
                }
            }
        }, "rocks-sweep");
        sweeper.setDaemon(true);
        sweeper.start();
    }

    private static long nowMs() { return System.currentTimeMillis(); }
    private static int shardOf(String code) {
        int h = (int) 2166136261L;
        for (int i = 0; i < code.length(); i++) { h ^= code.charAt(i); h *= 16777619; }
        return h & (SHARDS - 1);
    }
    private static boolean trackHits() {
        String hv = System.getenv("TRACK_HITS");
        return hv == null || !hv.equals("0");
    }

    private static byte[] enc(long e, long c, String u) {
        return ("v1|" + e + "|" + c + "|" + u).getBytes(StandardCharsets.UTF_8);
    }
    private static long[] dec(String v) {
        if (v.startsWith("v1|")) v = v.substring(3);
        int p = v.indexOf('|');
        if (p < 0) return null;
        try {
            long e = Long.parseLong(v.substring(0, p));
            String rest = v.substring(p + 1);
            int q = rest.indexOf('|');
            long c = 0;
            if (q >= 0) c = Long.parseLong(rest.substring(0, q));
            return new long[]{e, c};
        } catch (NumberFormatException ex) { return null; }
    }
    private static String decUrl(String v) {
        if (v.startsWith("v1|")) v = v.substring(3);
        int p = v.indexOf('|');
        if (p < 0) return null;
        String rest = v.substring(p + 1);
        int q = rest.indexOf('|');
        return q >= 0 ? rest.substring(q + 1) : rest;
    }
    private static byte[] u64(long n) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(n).array();
    }
    private static long readU64(byte[] v) {
        if (v == null || v.length < 8) return 0;
        return ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private void flushHits() throws RocksDBException {
        try (WriteBatch wb = new WriteBatch()) {
            boolean any = false;
            for (int i = 0; i < SHARDS; i++) {
                synchronized (dirtyLocks[i]) {
                    for (Map.Entry<String, Long> e : dirty[i].entrySet()) {
                        wb.merge(hits, e.getKey().getBytes(StandardCharsets.UTF_8),
                                 u64(e.getValue()));
                        any = true;
                    }
                    dirty[i].clear();
                }
            }
            if (any) db.write(wo, wb);
        }
    }

    private void sweepExpired() throws RocksDBException {
        long now = nowMs();
        List<byte[]> dead = new ArrayList<>();
        try (RocksIterator it = db.newIterator(links, ro)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String v = new String(it.value(), StandardCharsets.UTF_8);
                long[] ec = dec(v);
                if (ec != null && ec[0] != 0 && ec[0] <= now) dead.add(it.key());
            }
        }
        if (dead.isEmpty()) return;
        try (WriteBatch wb = new WriteBatch()) {
            for (byte[] k : dead) wb.delete(links, k);
            db.write(wo, wb);
        }
    }

    private void bump(String code) {
        if (!trackHits()) return;
        int i = shardOf(code);
        synchronized (dirtyLocks[i]) {
            dirty[i].merge(code, 1L, Long::sum);
        }
    }

    private String cacheGet(String code) {
        CacheShard sh = cache[shardOf(code)];
        synchronized (sh) {
            CacheEntry e = sh.m.get(code);
            if (e == null) return null;
            long now = nowMs();
            if ((cacheTtlMs > 0 && now - e.at > cacheTtlMs) || (e.e != 0 && e.e <= now)) {
                sh.m.remove(code);
                return null;
            }
            return e.u;
        }
    }
    private void cachePut(String code, String u, long e) {
        CacheShard sh = cache[shardOf(code)];
        synchronized (sh) {
            if (sh.m.containsKey(code)) { sh.m.get(code).u = u; sh.m.get(code).e = e; sh.m.get(code).at = nowMs(); return; }
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
        char[] c = new char[8];
        c[0] = prefix;
        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 1; i < 8; i++) c[i] = ALPHABET[r.nextInt(62)];
        return new String(c);
    }

    private byte[] dbGet(String code) throws RocksDBException {
        return db.get(links, ro, code.getBytes(StandardCharsets.UTF_8));
    }
    private long dbHits(String code) {
        try {
            return readU64(db.get(hits, ro, code.getBytes(StandardCharsets.UTF_8)));
        } catch (RocksDBException e) { return 0; }
    }

    // ---- StoreApi ----

    @Override
    public String resolve(String code) {
        String u = cacheGet(code);
        if (u != null) { Metrics.cacheHit(); bump(code); return u; }
        Metrics.cacheMiss();
        long t0 = System.nanoTime();
        byte[] v;
        try { v = dbGet(code); } catch (RocksDBException e) { return null; }
        finally { Metrics.storeRead((System.nanoTime() - t0) / 1000); }
        if (v == null) return null;
        String sv = new String(v, StandardCharsets.UTF_8);
        long[] ec = dec(sv);
        String us = decUrl(sv);
        if (ec == null || us == null) return null;
        if (ec[0] != 0 && ec[0] <= nowMs()) return null;
        cachePut(code, us, ec[0]);
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
                if (dbGet(alias) != null) return null;
                db.put(links, wo, alias.getBytes(StandardCharsets.UTF_8), enc(exp, now, url));
                cachePut(alias, url, exp);
                return alias;
            }
            for (;;) {
                String c = genCode();
                if (dbGet(c) != null) continue;
                db.put(links, wo, c.getBytes(StandardCharsets.UTF_8), enc(exp, now, url));
                cachePut(c, url, exp);
                return c;
            }
        } catch (RocksDBException e) { return null; }
    }

    @Override
    public List<String> shortenMany(List<String> urls, long ttlMs) {
        long now = nowMs();
        long exp = ttlMs > 0 ? now + ttlMs : 0;
        List<String> codes = new ArrayList<>(urls.size());
        List<Integer> retry = new ArrayList<>();
        try (WriteBatch wb = new WriteBatch()) {
            for (int i = 0; i < urls.size(); i++) {
                String c = genCode();
                if (dbGet(c) != null) { retry.add(i); codes.add(null); continue; }
                codes.add(c);
                wb.put(links, c.getBytes(StandardCharsets.UTF_8), enc(exp, now, urls.get(i)));
            }
            db.write(wo, wb);
            for (int i = 0; i < urls.size(); i++) {
                if (codes.get(i) != null) cachePut(codes.get(i), urls.get(i), exp);
            }
        } catch (RocksDBException e) {
            for (int i = 0; i < urls.size(); i++) if (codes.get(i) != null) retry.add(i);
        }
        for (int i : retry) {
            String c2 = shorten(urls.get(i), null, ttlMs);
            codes.set(i, c2);
        }
        return codes;
    }

    @Override
    public Store.MutResult update(String code, String url, long ttlMs, boolean hasTtl) {
        try {
            byte[] v = dbGet(code);
            if (v == null) return Store.MutResult.MISSING;
            String sv = new String(v, StandardCharsets.UTF_8);
            long[] ec = dec(sv);
            if (ec == null) return Store.MutResult.MISSING;
            long exp = !hasTtl ? ec[0] : ttlMs > 0 ? nowMs() + ttlMs : 0;
            db.put(links, wo, code.getBytes(StandardCharsets.UTF_8), enc(exp, ec[1], url));
            cacheDel(code);
            return Store.MutResult.OK;
        } catch (RocksDBException e) { return Store.MutResult.MISSING; }
    }

    @Override
    public Store.MutResult remove(String code) {
        try {
            if (dbGet(code) == null) return Store.MutResult.MISSING;
            try (WriteBatch wb = new WriteBatch()) {
                byte[] k = code.getBytes(StandardCharsets.UTF_8);
                wb.delete(links, k);
                wb.delete(hits, k);
                db.write(wo, wb);
            }
            cacheDel(code);
            return Store.MutResult.OK;
        } catch (RocksDBException e) { return Store.MutResult.MISSING; }
    }

    @Override
    public Store.Pair<List<Store.Link>, Integer> list(int limit, int offset, String sort, String q) {
        List<Store.Link> items = new ArrayList<>();
        long now = nowMs();
        try (RocksIterator it = db.newIterator(links, ro)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String code = new String(it.key(), StandardCharsets.UTF_8);
                String sv = new String(it.value(), StandardCharsets.UTF_8);
                long[] ec = dec(sv);
                String u = decUrl(sv);
                if (ec == null || u == null) continue;
                if (ec[0] != 0 && ec[0] <= now) continue;
                if (q != null && !q.isEmpty() && !code.contains(q) && !u.contains(q)) continue;
                long hits = dbHits(code);
                int i = shardOf(code);
                synchronized (dirtyLocks[i]) { hits += dirty[i].getOrDefault(code, 0L); }
                items.add(new Store.Link(code, u, hits, ec[1], ec[0] != 0 ? ec[0] : null));
            }
        }
        boolean byHits = "hits".equals(sort);
        items.sort((a, b) -> byHits
                ? Long.compare(b.hits(), a.hits())
                : Long.compare(b.createdAt(), a.createdAt()));
        int total = items.size();
        if (offset >= items.size()) items = new ArrayList<>();
        else items = new ArrayList<>(items.subList(offset, (int) Math.min(offset + limit, items.size())));
        return new Store.Pair<>(items, total);
    }

    @Override
    public Store.Link stats(String code) {
        byte[] v;
        try { v = dbGet(code); } catch (RocksDBException e) { return null; }
        if (v == null) return null;
        String sv = new String(v, StandardCharsets.UTF_8);
        long[] ec = dec(sv);
        String u = decUrl(sv);
        if (ec == null || u == null) return null;
        if (ec[0] != 0 && ec[0] <= nowMs()) return null;
        long h = dbHits(code);
        int i = shardOf(code);
        synchronized (dirtyLocks[i]) { h += dirty[i].getOrDefault(code, 0L); }
        return new Store.Link(code, u, h, ec[1], ec[0] != 0 ? ec[0] : null);
    }

    @Override
    public int seed(List<String> urls) {
        shortenMany(urls, 0);
        try { flushHits(); } catch (Exception ignored) {}
        return urls.size();
    }

    @Override
    /** /api/health probe: a point read proves the DB is open & readable. */
    public boolean healthy() {
        try {
            db.get(new byte[]{0});
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    public boolean isEmpty() {
        try (RocksIterator it = db.newIterator(links, ro)) {
            it.seekToFirst();
            return !it.isValid();
        }
    }

    @Override public int instance() { return instance; }
    @Override public boolean persistent() { return true; }
    @Override public void pollTailsNow() {}
    @Override public void flush() { try { flushHits(); } catch (Exception ignored) {} }
    @Override public void compact() {
        try { db.compactRange(links); } catch (RocksDBException ignored) {}
    }
    @Override public void close() {
        stop = true;
        flusher.interrupt();
        sweeper.interrupt();
        try { flusher.join(2000); sweeper.join(2000); } catch (InterruptedException ignored) {}
        try { flushHits(); } catch (Exception ignored) {}
        links.close(); hits.close(); ro.close(); wo.close();
        db.close();
    }
}
