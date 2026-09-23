package shrt;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * The index: ConcurrentHashMap (Java's striped map — the native answer to the
 * 256-shard scheme in the other ports) + buffered AOF appends.
 *
 *  - writes: map put + buffered append; flush batch every FLUSH_MS (<=5ms to
 *    page cache), fsync every FSYNC_MS (<=500ms durable)
 *  - reads: CHM get + atomic hit increments; sibling logs tailed lazily on miss
 */
public final class Store implements StoreApi {
    static final int FLUSH_MS = 5, FSYNC_MS = 500, TAIL_MIN_INTERVAL = 200;
    static final long FLUSH_BYTES = 256 << 10;
    static final int CODE_LEN = 8, MAX_INSTANCES = 62, MAX_STRAY_HITS = 10_000;

    public enum MutResult { OK, MISSING, REMOTE }

    /** Public view of a stored entry. */
    public record Link(String code, String url, long hits,
                       long createdAt, Long expiresAt) {}

    static final class Entry {
        volatile String u;
        final long a;
        volatile long e;
        final int i;
        final AtomicLong h = new AtomicLong(), oh = new AtomicLong();
        Entry(String u, long a, long e, int i) { this.u = u; this.a = a; this.e = e; this.i = i; }
    }

    final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>(1 << 16);
    final ConcurrentHashMap<String, AtomicLong> dirty = new ConcurrentHashMap<>();
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();

    // tail state (guarded by tailLock)
    final Object tailLock = new Object();
    final Map<String, TailReader> readers = new HashMap<>();
    final Map<String, long[]> stray = new HashMap<>(); // [total, own]
    long lastPoll;

    final Path dir;
    final String ownName;
    final int instance;
    final char prefix;
    volatile Aof aof;
    final Object aofMu = new Object();
    InstanceLock lock;
    final boolean persistent;
    volatile boolean stop, closed;
    final List<Thread> threads = new ArrayList<>();
    final SecureRandom rng = new SecureRandom();

    private Store(Path dir, int instance, char prefix, boolean persistent) {
        this.dir = dir; this.instance = instance; this.prefix = prefix;
        this.persistent = persistent;
        this.ownName = "data-" + instance + ".log";
    }

    static long now() { return System.currentTimeMillis(); }
    static boolean trackHits() {
        String v = System.getenv("HITS");
        return v == null || !v.equals("0");
    }

    static int alphaIdx(char c) { return Codec.ALPHABET.indexOf(c); }

    /** dir ":memory:" disables persistence. instance < 0 auto-claims. */
    public static Store open(String dirName, int want) throws IOException {
        if (dirName.equals(":memory:"))
            return new Store(null, 0, Codec.ALPHABET.charAt(0), false);
        Path dir = Paths.get(dirName);
        Files.createDirectories(dir);
        InstanceLock lk = InstanceLock.claim(dir, want);
        int inst = lk.instance;
        if (inst >= MAX_INSTANCES) { lk.release(); throw new IOException("instance >= " + MAX_INSTANCES); }
        Store s = new Store(dir, inst, Codec.ALPHABET.charAt(inst), true);
        s.lock = lk;
        s.loadAll();
        s.aof = new Aof(dir.resolve(s.ownName));
        s.startThreads();
        return s;
    }

    // ---------- load ----------

    private void loadAll() throws IOException {
        // snapshot first, then log (log rows are newer)
        Path snap = dir.resolve("data-" + instance + ".snap");
        Codec.Op o = new Codec.Op();
        if (Files.exists(snap))
            Aof.replay(snap, l -> { if (Codec.parseOp(l, o)) apply(o); });
        Path log = dir.resolve(ownName);
        if (Files.exists(log))
            Aof.replay(log, l -> { if (Codec.parseOp(l, o)) apply(o); });
        // adopt sibling logs lazily via tailMissed on demand
    }

    /** Apply one parsed op (log replay / tail). Last-write-wins on rows. */
    private void apply(Codec.Op o) {
        if (o.x != null) { data.remove(o.x); strayRemove(o.x); return; }
        if (o.h != null) {
            boolean own = instance == (int) o.i;
            Entry e = data.get(o.h);
            if (e != null) {
                e.h.addAndGet(o.d);
                if (own) e.oh.addAndGet(o.d);
                return;
            }
            synchronized (tailLock) {
                if (stray.size() >= MAX_STRAY_HITS) stray.remove(stray.keySet().iterator().next());
                long[] st = stray.computeIfAbsent(o.h, k -> new long[2]);
                st[0] += o.d;
                if (own) st[1] += o.d;
            }
            return;
        }
        if (o.c == null) return;
        long stT = 0, stO = 0;
        synchronized (tailLock) {
            long[] st = stray.remove(o.c);
            if (st != null) { stT = st[0]; stO = st[1]; }
        }
        Entry e = new Entry(o.u, o.a, o.hasE ? o.e : 0, (int) o.i);
        e.h.set(o.n + stT);
        e.oh.set(o.n + stO);
        data.put(o.c, e); // overwrite — log rows are last-write-wins
    }

    private void strayRemove(String code) {
        synchronized (tailLock) { stray.remove(code); }
    }

    private List<String> shardFiles() {
        try (Stream<Path> s = Files.list(dir)) {
            List<String> out = new ArrayList<>();
            s.forEach(p -> {
                String f = p.getFileName().toString();
                if (f.startsWith("data-") && f.endsWith(".log") && !f.equals(ownName))
                    out.add(f);
            });
            return out;
        } catch (IOException e) { return List.of(); }
    }

    private void pollTails() {
        synchronized (tailLock) {
            for (String f : shardFiles())
                readers.computeIfAbsent(f, k -> openTail(f));
            Codec.Op o = new Codec.Op();
            for (TailReader t : readers.values())
                try { t.readNew(l -> { if (Codec.parseOp(l, o)) apply(o); }); }
                catch (IOException ignored) {}
            lastPoll = now();
        }
    }

    private TailReader openTail(String f) {
        try { return new TailReader(dir.resolve(f)); }
        catch (IOException e) { return null; }
    }

    /** On-miss tailing: owner log by prefix, else rate-limited full poll. */
    private void tailMissed(String code) {
        if (code.isEmpty() || aof == null) return;
        synchronized (tailLock) {
            int owner = alphaIdx(code.charAt(0));
            if (owner >= 0 && owner != instance) {
                String name = "data-" + owner + ".log";
                TailReader t = readers.computeIfAbsent(name, this::openTail);
                if (t != null) {
                    Codec.Op o = new Codec.Op();
                    try { t.readNew(l -> { if (Codec.parseOp(l, o)) apply(o); }); }
                    catch (IOException ignored) {}
                }
                lastPoll = now();
                return;
            }
            if (now() - lastPoll >= TAIL_MIN_INTERVAL) {
                for (String f : shardFiles())
                    readers.computeIfAbsent(f, this::openTail);
                Codec.Op o = new Codec.Op();
                for (TailReader t : readers.values())
                    if (t != null)
                        try { t.readNew(l -> { if (Codec.parseOp(l, o)) apply(o); }); }
                        catch (IOException ignored) {}
                lastPoll = now();
            }
        }
    }

    // ---------- threads ----------

    private void startThreads() {
        Thread fl = new Thread(() -> {
            while (!stop) {
                try { Thread.sleep(FLUSH_MS); } catch (InterruptedException ignored) {}
                try { flush(); } catch (Throwable ignored) {}
            }
        });
        fl.setDaemon(true); fl.start();
        Thread fs = new Thread(() -> {
            while (!stop) {
                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                long last = lastFsync;
                if (now() - last >= FSYNC_MS) {
                    synchronized (aofMu) {
                        try { if (aof != null) { aof.fsync(); lastFsync = now(); } }
                        catch (IOException ignored) {}
                    }
                }
            }
        });
        fs.setDaemon(true); fs.start();
        String tv = System.getenv("TAIL_MS");
        long tailMs = tv != null ? Long.parseLong(tv) : 0;
        if (tailMs > 0) {
            Thread tp = new Thread(() -> {
                while (!stop) {
                    try { Thread.sleep(tailMs); } catch (InterruptedException ignored) {}
                    try { pollTails(); } catch (Throwable ignored) {}
                }
            });
            tp.setDaemon(true); tp.start();
            threads.add(tp);
        }
        threads.add(fl); threads.add(fs);
    }

    volatile long lastFsync;

    // ---------- writes ----------

    private String genCode() {
        StringBuilder c = new StringBuilder(CODE_LEN);
        c.append(prefix);
        byte[] b = new byte[CODE_LEN - 1];
        ThreadLocalRandom.current().nextBytes(b);
        for (byte x : b) c.append(Codec.ALPHABET.charAt((x & 0xff) % MAX_INSTANCES));
        return c.toString();
    }

    /** Create a link; returns the code, or null if the alias is taken. */
    public String shorten(String url, String alias, long ttlMs) {
        gate.readLock().lock();
        try {
            long now = now();
            long exp = ttlMs > 0 ? now + ttlMs : 0;
            String code;
            if (alias != null) {
                if (data.putIfAbsent(alias, new Entry(url, now, exp, instance)) != null)
                    return null;
                code = alias;
            } else {
                String c;
                do { c = genCode(); }
                while (data.putIfAbsent(c, new Entry(url, now, exp, instance)) != null);
                code = c;
            }
            Aof a = aof;
            if (a != null) {
                StringBuilder sb = new StringBuilder(64);
                Codec.rowLine(sb, code, url, now, exp, instance, 0);
                synchronized (aofMu) {
                    a.push(sb.toString());
                    if (a.pendingBytes() > FLUSH_BYTES) try { a.flush(); } catch (IOException ignored) {}
                }
            }
            return code;
        } finally { gate.readLock().unlock(); }
    }

    /** Bulk create; returns codes aligned with input order. */
    public List<String> shortenMany(List<String> urls, long ttlMs) {
        gate.readLock().lock();
        try {
            long now = now();
            long exp = ttlMs > 0 ? now + ttlMs : 0;
            List<String> codes = new ArrayList<>(urls.size());
            byte[] rnd = new byte[urls.size() * (CODE_LEN - 1)];
            ThreadLocalRandom.current().nextBytes(rnd);
            StringBuilder lines = new StringBuilder(urls.size() * 48);
            for (int i = 0; i < urls.size(); i++) {
                StringBuilder cb = new StringBuilder(CODE_LEN);
                cb.append(prefix);
                for (int k = 0; k < CODE_LEN - 1; k++)
                    cb.append(Codec.ALPHABET.charAt((rnd[i * (CODE_LEN - 1) + k] & 0xff) % MAX_INSTANCES));
                String code = cb.toString();
                while (data.putIfAbsent(code, new Entry(urls.get(i), now, exp, instance)) != null)
                    code = genCode();
                Codec.rowLine(lines, code, urls.get(i), now, exp, instance, 0);
                lines.append('\n');
                codes.add(code);
            }
            Aof a = aof;
            if (a != null) {
                // encode OFF the lock: only the arraycopy is serialized
                byte[] lb = lines.toString().getBytes(StandardCharsets.UTF_8);
                synchronized (aofMu) {
                    a.pushBytes(lb);
                    if (a.pendingBytes() > FLUSH_BYTES) try { a.flush(); } catch (IOException ignored) {}
                }
            }
            return codes;
        } finally { gate.readLock().unlock(); }
    }

    /** Target url or null (miss/expired). Counts a hit on success. */
    public String resolve(String code) {
        Entry e = data.get(code);
        if (e != null) {
            long exp = e.e;
            if (exp != 0 && exp <= now()) return null;
            if (trackHits()) { e.h.incrementAndGet(); e.oh.incrementAndGet(); markDirty(code); }
            return e.u;
        }
        if (aof == null) return null;
        tailMissed(code);
        Entry e2 = data.get(code);
        if (e2 != null && (e2.e == 0 || e2.e > now())) {
            if (trackHits()) { e2.h.incrementAndGet(); e2.oh.incrementAndGet(); markDirty(code); }
            return e2.u;
        }
        return null;
    }

    private void markDirty(String code) {
        dirty.computeIfAbsent(code, k -> new AtomicLong()).incrementAndGet();
    }

    /** Update url/ttl. Durable only on owning instance. */
    public MutResult update(String code, String url, long ttlMs, boolean hasTtl) {
        gate.readLock().lock();
        try {
            Entry cur = data.get(code);
            if (cur == null) return MutResult.MISSING;
            if (cur.i != instance) return MutResult.REMOTE;
            long exp = hasTtl ? (ttlMs > 0 ? now() + ttlMs : 0) : cur.e;
            long a = cur.a;
            // serialized per-key replace so tail-applied rows can't interleave
            data.compute(code, (k, old) -> {
                if (old == null || old.i != instance) return old;
                Entry ne = new Entry(url, old.a, exp, old.i);
                ne.h.set(old.h.get()); ne.oh.set(old.oh.get());
                return ne;
            });
            Aof aof = this.aof;
            if (aof != null) {
                StringBuilder sb = new StringBuilder(64);
                Codec.rowLine(sb, code, url, a, exp, instance, 0);
                synchronized (aofMu) { aof.push(sb.toString()); }
            }
            return MutResult.OK;
        } finally { gate.readLock().unlock(); }
    }

    /** Delete a link. Same owner rule as update(). */
    public MutResult remove(String code) {
        gate.readLock().lock();
        try {
            Entry cur = data.get(code);
            if (cur == null) return MutResult.MISSING;
            if (cur.i != instance) return MutResult.REMOTE;
            data.remove(code, cur);
            Aof aof = this.aof;
            if (aof != null) {
                StringBuilder sb = new StringBuilder(24);
                Codec.delLine(sb, code);
                synchronized (aofMu) { aof.push(sb.toString()); }
            }
            return MutResult.OK;
        } finally { gate.readLock().unlock(); }
    }

    /** O(n) scan for UI listing — admin path, not the hot path. */
    public Pair<List<Link>, Integer> list(int limit, int offset, String sort, String q) {
        List<Link> items = new ArrayList<>();
        for (var en : data.entrySet()) {
            String code = en.getKey(); Entry e = en.getValue();
            if (!q.isEmpty() && !code.contains(q) && !e.u.contains(q)) continue;
            items.add(new Link(code, e.u, e.h.get(), e.a, e.e != 0 ? e.e : null));
        }
        if (sort.equals("hits")) items.sort(Comparator.comparingLong(Link::hits).reversed());
        else items.sort(Comparator.comparingLong(Link::createdAt).reversed());
        int total = items.size();
        return new Pair<>(items.stream().skip(Math.min(offset, total)).limit(limit).toList(), total);
    }

    public Link stats(String code) {
        Entry e = data.get(code);
        return e == null ? null
            : new Link(code, e.u, e.h.get(), e.a, e.e != 0 ? e.e : null);
    }

    public int seed(List<String> urls) {
        shortenMany(urls, 0);
        flush();
        return urls.size();
    }

    /** In-process engine is healthy whenever the process is. */
    public boolean healthy() { return true; }

    public boolean isEmpty() { return data.isEmpty(); }
    public int instance() { return instance; }
    public boolean persistent() { return persistent; }

    // ---------- durability ----------

    /** Poll sibling logs now (public — tests + TAIL_MS thread use it). */
    public void pollTailsNow() { if (persistent) pollTails(); }

    /** Persist hit deltas + queued rows; one write boundary. */
    public void flush() {
        Aof a = aof;
        if (a == null) return;
        gate.readLock().lock();
        try {
            StringBuilder lines = new StringBuilder(256);
            for (var en : dirty.entrySet()) {
                long d = en.getValue().get();
                if (d == 0) continue;
                if (dirty.remove(en.getKey(), en.getValue())) {
                    // re-add raced increments captured post-remove are kept by
                    // the fresh AtomicLong a resolver may have installed
                    Codec.hitLine(lines, en.getKey(), d, instance);
                    lines.append('\n');
                }
            }
            synchronized (aofMu) {
                a.pushRaw(lines.toString());
                try { a.flush(); } catch (IOException ignored) {}
            }
        } finally { gate.readLock().unlock(); }
    }

    /** Snapshot own rows then truncate own log. */
    public void compact() {
        if (aof == null) return;
        gate.writeLock().lock();
        try {
            flush();
            Path snap = dir.resolve("data-" + instance + ".snap");
            Path tmp = dir.resolve("data-" + instance + ".snap.tmp");
            StringBuilder sb = new StringBuilder(1 << 20);
            for (var en : data.entrySet()) {
                Entry e = en.getValue();
                if (e.i == instance) {
                    Codec.rowLine(sb, en.getKey(), e.u, e.a, e.e, e.i, e.oh.get());
                    sb.append('\n');
                }
            }
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
            try (var ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) { ch.force(true); }
            Files.move(tmp, snap, StandardCopyOption.REPLACE_EXISTING);
            synchronized (aofMu) { aof.truncate(); }
        } catch (IOException ignored) {
        } finally { gate.writeLock().unlock(); }
    }

    /** Stop timers, flush, fsync, release the instance lock. */
    public void close() {
        if (closed) return;
        closed = true;
        stop = true;
        for (Thread t : threads) try { t.join(2000); } catch (InterruptedException ignored) {}
        Aof a = aof;
        if (a != null) {
            flush();
            synchronized (aofMu) { a.close(); }
        }
        synchronized (tailLock) { readers.values().forEach(TailReader::close); readers.clear(); }
        if (lock != null) lock.release();
    }

    public record Pair<A, B>(A first, B second) {}
}
