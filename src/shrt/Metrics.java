package shrt;

import java.util.concurrent.atomic.LongAdder;

/** Process-wide request counters + Prometheus text exposition. */
public final class Metrics {
    private Metrics() {}
    private static final LongAdder requests = new LongAdder();
    private static final long started = System.currentTimeMillis();

    // ops: redirect, shorten, shorten_bulk, update, delete, list, stats,
    // health, metrics, ui, other
    public static final int OP_REDIRECT = 0, OP_SHORTEN = 1, OP_BULK = 2,
            OP_UPDATE = 3, OP_DELETE = 4, OP_LIST = 5, OP_STATS = 6,
            OP_HEALTH = 7, OP_METRICS = 8, OP_UI = 9, OP_OTHER = 10;
    private static final String[] OPS = {
        "redirect", "shorten", "shorten_bulk", "update", "delete", "list",
        "stats", "health", "metrics", "ui", "other" };
    private static final LongAdder[] opCounts = new LongAdder[11];
    private static final LongAdder[] statusCounts = new LongAdder[4]; // 2xx 3xx 4xx 5xx
    private static final LongAdder cacheHit = new LongAdder();
    private static final LongAdder cacheMiss = new LongAdder();
    private static final LongAdder storeReads = new LongAdder();
    private static final LongAdder storeReadUs = new LongAdder();
    private static final LongAdder storeWrites = new LongAdder();
    private static final LongAdder linksTotal = new LongAdder();
    static {
        for (int i = 0; i < 11; i++) opCounts[i] = new LongAdder();
        for (int i = 0; i < 4; i++) statusCounts[i] = new LongAdder();
    }

    public static void tick() { requests.increment(); }

    public static void op(int i) { opCounts[i].increment(); }
    public static void status(int code) {
        int i = code >= 200 && code < 300 ? 0
              : code >= 300 && code < 400 ? 1
              : code >= 400 && code < 500 ? 2 : 3;
        statusCounts[i].increment();
    }
    public static void cacheHit()  { cacheHit.increment(); }
    public static void cacheMiss() { cacheMiss.increment(); }
    public static void storeRead(long us) { storeReads.increment(); storeReadUs.add(us); }
    public static void storeWrite() { storeWrites.increment(); }
    public static void linksDelta(long n) { linksTotal.add(n); }

    public static String snapshot() {
        return "{\"requests\":" + requests.sum()
             + ",\"uptime_ms\":" + (System.currentTimeMillis() - started) + "}";
    }

    private static void line(StringBuilder b, String m, String labels, long v) {
        b.append(m);
        if (!labels.isEmpty()) b.append('{').append(labels).append('}');
        b.append(' ').append(v).append('\n');
    }

    /** Prometheus text exposition — /metrics endpoint. */
    public static String prometheus(long rateLimited) {
        StringBuilder b = new StringBuilder(1024);
        b.append("# HELP shrt_requests_total Requests by operation\n");
        b.append("# TYPE shrt_requests_total counter\n");
        for (int i = 0; i < OPS.length; i++)
            line(b, "shrt_requests_total", "op=\"" + OPS[i] + "\"", opCounts[i].sum());
        b.append("# HELP shrt_responses_total Responses by status class\n");
        b.append("# TYPE shrt_responses_total counter\n");
        String[] cls = {"2xx", "3xx", "4xx", "5xx"};
        for (int i = 0; i < 4; i++)
            line(b, "shrt_responses_total", "class=\"" + cls[i] + "\"", statusCounts[i].sum());
        b.append("# HELP shrt_cache_lookups_total Local hot-cache lookups\n");
        b.append("# TYPE shrt_cache_lookups_total counter\n");
        line(b, "shrt_cache_lookups_total", "result=\"hit\"", cacheHit.sum());
        line(b, "shrt_cache_lookups_total", "result=\"miss\"", cacheMiss.sum());
        b.append("# HELP shrt_store_reads_total Backing-store point reads (cache misses)\n");
        b.append("# TYPE shrt_store_reads_total counter\n");
        line(b, "shrt_store_reads_total", "", storeReads.sum());
        b.append("# HELP shrt_store_read_us_total Cumulative backing-store read latency (us)\n");
        b.append("# TYPE shrt_store_read_us_total counter\n");
        line(b, "shrt_store_read_us_total", "", storeReadUs.sum());
        b.append("# HELP shrt_store_writes_total Backing-store writes\n");
        b.append("# TYPE shrt_store_writes_total counter\n");
        line(b, "shrt_store_writes_total", "", storeWrites.sum());
        b.append("# HELP shrt_rate_limited_total Requests rejected by the rate limiter\n");
        b.append("# TYPE shrt_rate_limited_total counter\n");
        line(b, "shrt_rate_limited_total", "", rateLimited);
        b.append("# HELP shrt_links_total Live links created minus deleted\n");
        b.append("# TYPE shrt_links_total gauge\n");
        line(b, "shrt_links_total", "", linksTotal.sum());
        b.append("# HELP shrt_uptime_seconds Process uptime\n");
        b.append("# TYPE shrt_uptime_seconds gauge\n");
        line(b, "shrt_uptime_seconds", "", (System.currentTimeMillis() - started) / 1000);
        return b.toString();
    }
}
