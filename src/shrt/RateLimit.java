package shrt;

import java.util.HashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-IP token bucket for write endpoints (abuse control on a public
 * shortener). Off by default: RATE_LIMIT=&lt;req/s per IP&gt; enables it;
 * RATE_LIMIT_BURST sets bucket capacity (default = RATE_LIMIT).
 * POST /api/shorten costs 1 token; /api/shorten/bulk costs urls count.
 */
public final class RateLimit {
    private static final int SHARDS = 64;
    private static final int MAX_KEYS = 1 << 12; // per shard
    private static final long IDLE_MS = 60_000;

    private static final class Bucket { double tokens; long lastMs; }

    private static final class Shard {
        final HashMap<Long, Bucket> m = new HashMap<>();
    }

    private final Shard[] shards = new Shard[SHARDS];
    private final double rate, burst;

    /** Requests rejected (exported via /metrics). */
    public static final LongAdder LIMITED = new LongAdder();

    private static volatile RateLimit global;

    public static RateLimit global() {
        RateLimit g = global;
        if (g == null) {
            synchronized (RateLimit.class) {
                if (global == null) global = new RateLimit();
                g = global;
            }
        }
        return g;
    }

    /** Test hook: re-read env and clear state. */
    static void reloadForTest() {
        synchronized (RateLimit.class) { global = new RateLimit(); }
    }

    /** Test hook: install a limiter with explicit settings. */
    static void initForTest(double rate, double burst) {
        synchronized (RateLimit.class) { global = new RateLimit(rate, burst); }
    }

    private RateLimit(double rate, double burst) {
        for (int i = 0; i < SHARDS; i++) shards[i] = new Shard();
        this.rate = Math.max(0, rate);
        this.burst = Math.max(1, burst);
    }

    RateLimit() {
        Shard[] sh = shards;
        for (int i = 0; i < SHARDS; i++) sh[i] = new Shard();
        String r = System.getenv("RATE_LIMIT");
        String b = System.getenv("RATE_LIMIT_BURST");
        double rt = r == null ? 0 : Double.parseDouble(r);
        double br = b == null ? rt : Double.parseDouble(b);
        if (br < 1) br = 1;
        rate = Math.max(0, rt);
        burst = br;
    }

    private static long ipKey(String ip) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < ip.length(); i++) {
            h ^= ip.charAt(i);
            h *= 0x100000001b3L;
        }
        return h;
    }

    /** Spend cost tokens from ip's bucket; true when allowed. */
    public boolean allow(String ip, double cost) {
        if (rate <= 0) return true;
        long k = ipKey(ip);
        Shard sh = shards[(int) (k & (SHARDS - 1))];
        long now = System.currentTimeMillis();
        synchronized (sh) {
            if (sh.m.size() >= MAX_KEYS)
                sh.m.entrySet().removeIf(e -> now - e.getValue().lastMs > IDLE_MS);
            Bucket b = sh.m.get(k);
            if (b == null) {
                b = new Bucket();
                b.tokens = burst;
                b.lastMs = now;
                sh.m.put(k, b);
            }
            b.tokens = Math.min(b.tokens + (now - b.lastMs) / 1000.0 * rate, burst);
            b.lastMs = now;
            if (b.tokens >= cost) { b.tokens -= cost; return true; }
            return false;
        }
    }
}
