package shrt;

import java.util.concurrent.atomic.LongAdder;

/** Process-wide request counters. */
public final class Metrics {
    private Metrics() {}
    private static final LongAdder requests = new LongAdder();
    private static final long started = System.currentTimeMillis();

    public static void tick() { requests.increment(); }

    public static String snapshot() {
        return "{\"requests\":" + requests.sum()
             + ",\"uptime_ms\":" + (System.currentTimeMillis() - started) + "}";
    }
}
