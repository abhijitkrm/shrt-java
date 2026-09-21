package shrt;

import java.io.IOException;
import java.util.List;

/**
 * The store surface the HTTP layer uses. {@link Store} (in-process AOF
 * engine) and {@link KvStore} (external RESP backend) both implement it.
 */
public interface StoreApi {
    String shorten(String url, String alias, long ttlMs);
    List<String> shortenMany(List<String> urls, long ttlMs);
    String resolve(String code);
    Store.MutResult update(String code, String url, long ttlMs, boolean hasTtl);
    Store.MutResult remove(String code);
    Store.Pair<List<Store.Link>, Integer> list(int limit, int offset, String sort, String q);
    Store.Link stats(String code);
    int seed(List<String> urls);
    boolean isEmpty();
    int instance();
    boolean persistent();
    void pollTailsNow();
    void flush();
    void compact();
    void close();

    /** STORE env dispatch: aof|local (default) | dragonfly|redis|kv.
     *  DRAGONFLY_ADDR/KV_ADDR (default 127.0.0.1:6379), CACHE (100000),
     *  CACHE_TTL_MS (5000 — staleness bound for cached entries). */
    static StoreApi openEnv(String dir, int instance) throws IOException {
        String mode = System.getenv("STORE");
        if (mode == null) mode = "aof";
        switch (mode) {
            case "dragonfly", "redis", "kv" -> {
                String addr = System.getenv("DRAGONFLY_ADDR");
                if (addr == null || addr.isEmpty()) addr = System.getenv("KV_ADDR");
                if (addr == null || addr.isEmpty()) addr = "127.0.0.1:6379";
                int cache = envInt("CACHE", 100000);
                long ttl = envInt("CACHE_TTL_MS", 5000);
                return new KvStore(addr, instance, cache, ttl);
            }
            default -> {
                return Store.open(dir, instance);
            }
        }
    }

    private static int envInt(String k, int def) {
        String v = System.getenv(k);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
}
