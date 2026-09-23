package shrt;

import java.io.IOException;
import java.util.List;

/** Live tests for the RESP-KV backend. Gated on SHRT_KV_ADDR — skipped
 *  when unset/unreachable so the suite stays hermetic.
 *    SHRT_KV_ADDR=127.0.0.1:6379 java -cp build shrt.TestMain */
public final class KvTests {
    private static final Object DB_LOCK = new Object();

    private static StoreApi kv() throws IOException {
        String addr = System.getenv("SHRT_KV_ADDR");
        if (addr == null || addr.isEmpty()) return null;
        KvStore st;
        try {
            st = new KvStore(addr, 0, 1000, 50);
        } catch (IOException e) {
            return null;
        }
        new Kv(addr, 1).flushdb();
        return st;
    }

    private interface K { void run(StoreApi s) throws Exception; }
    private static void gated(String name, K body) {
        T.test(name, () -> {
            synchronized (DB_LOCK) {
                StoreApi s = kv();
                if (s == null) { System.out.println("    (skip: SHRT_KV_ADDR)"); return; }
                try { body.run(s); } finally { s.close(); }
            }
        });
    }

    public static void register() {
        gated("kv_legacy_value_decode", s -> {
            String lay = System.getenv("KV_LAYOUT");
            if ("hash".equals(lay)) {
                System.out.println("    (skip: KV_LAYOUT=hash)");
                return;
            }
            Kv k = new Kv(System.getenv("SHRT_KV_ADDR"), 1);
            k.set("l:legacy1", "0|https://one.example", 0, false);
            k.set("l:legacy2", "0|0|https://two.example", 0, false);
            T.checkEq(s.resolve("legacy1"), "https://one.example");
            T.checkEq(s.resolve("legacy2"), "https://two.example");
            s.shorten("https://v1.example", "v1check", 0);
            String raw = new String(k.get("l:v1check"), java.nio.charset.StandardCharsets.UTF_8);
            T.check(raw.startsWith("v1|"), "v1 tag: " + raw);
        });

        gated("kv_shorten_resolve", s -> {
            T.checkEq(s.shorten("https://a.com", "gh", 0), "gh");
            T.checkEq(s.resolve("gh"), "https://a.com");
            T.check(s.shorten("https://b.com", "gh", 0) == null, "alias collision");
            String c = s.shorten("https://c.com", null, 0);
            T.check(c != null && c.length() == 8, "gen code");
            T.checkEq(s.resolve(c), "https://c.com");
        });

        gated("kv_hits_batched", s -> {
            s.shorten("https://a.com", "h", 0);
            for (int i = 0; i < 5; i++) s.resolve("h");
            s.flush();
            Store.Link st = s.stats("h");
            T.check(st != null && st.hits() == 5, "hits=" + (st == null ? -1 : st.hits()));
        });

        gated("kv_update_remove", s -> {
            s.shorten("https://a.com", "u", 0);
            T.checkEq(s.update("u", "https://b.com", 0, false), Store.MutResult.OK);
            T.checkEq(s.resolve("u"), "https://b.com");
            T.checkEq(s.update("missing", "https://x.com", 0, false), Store.MutResult.MISSING);
            T.checkEq(s.remove("u"), Store.MutResult.OK);
            T.check(s.resolve("u") == null, "resolve after remove");
            T.checkEq(s.remove("u"), Store.MutResult.MISSING);
        });

        gated("kv_ttl", s -> {
            s.shorten("https://t.com", "ttl", 80);
            T.check(s.resolve("ttl") != null, "resolve before expiry");
            Thread.sleep(120);
            T.check(s.resolve("ttl") == null, "resolve after expiry");
        });

        gated("kv_list_stats", s -> {
            s.shorten("https://one.com", "one", 0);
            s.shorten("https://two.com", "two", 0);
            s.resolve("one");
            s.flush();
            var pr = s.list(10, 0, "", "");
            T.checkEq(pr.second(), 2);
            boolean found = pr.first().stream()
                    .anyMatch(l -> l.code().equals("one") && l.hits() == 1);
            T.check(found, "one/1 in list");
            Store.Link st = s.stats("two");
            T.check(st != null && st.url().equals("https://two.com") && st.createdAt() > 0,
                    "stats two");
        });

        gated("kv_bulk", s -> {
            List<String> urls = new java.util.ArrayList<>();
            for (int i = 0; i < 50; i++) urls.add("https://b.com/" + i);
            List<String> codes = s.shortenMany(urls, 0);
            T.checkEq(codes.size(), 50);
            for (int i = 0; i < 50; i++) T.checkEq(s.resolve(codes.get(i)), urls.get(i));
        });

        gated("kv_cold_miss", s -> {
            s.shorten("https://stay.com", "stay", 0);
            String addr = System.getenv("SHRT_KV_ADDR");
            StoreApi s2 = new KvStore(addr, 1, 100, 50);
            try {
                T.checkEq(s2.resolve("stay"), "https://stay.com");
            } finally { s2.close(); }
        });

        gated("kv_cache_bounded", s -> {
            java.util.List<String> codes = new java.util.ArrayList<>();
            for (int i = 0; i < 200; i++)
                codes.add(s.shorten("https://x.com/" + i, null, 0));
            for (String c : codes)
                T.check(s.resolve(c) != null, "cold miss " + c);
        });
    }
}
