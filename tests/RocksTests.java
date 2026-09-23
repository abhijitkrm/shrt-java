package shrt;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Tests for the embedded RocksDB backend — in-process, no server.
 *    java -cp classes:lib/rocksdbjni-10.2.1.jar shrt.TestMain */
public final class RocksTests {
    private static final Object DB_LOCK = new Object();
    private static int seq = 0;

    private static StoreApi rocks() throws IOException {
        try {
            Path dir = Files.createTempDirectory("shrt-rocks-" + (seq++));
            return new RocksStore(dir.toString(), 0, 1000, 50);
        } catch (Throwable t) {
            return null; // rocksdbjni jar/native absent
        }
    }

    private interface K { void run(StoreApi s) throws Exception; }
    private static void gated(String name, K body) {
        T.test(name, () -> {
            synchronized (DB_LOCK) {
                StoreApi s = rocks();
                if (s == null) { System.out.println("    (skip: rocksdbjni unavailable)"); return; }
                try { body.run(s); } finally { s.close(); }
            }
        });
    }

    public static void register() {
        gated("rocks_shorten_resolve", s -> {
            T.checkEq(s.shorten("https://a.com", "gh", 0), "gh");
            T.checkEq(s.resolve("gh"), "https://a.com");
            T.check(s.shorten("https://b.com", "gh", 0) == null, "alias collision");
            String c = s.shorten("https://c.com", null, 0);
            T.check(c != null && c.length() == 8, "gen code");
            T.checkEq(s.resolve(c), "https://c.com");
        });

        gated("rocks_hits_batched", s -> {
            s.shorten("https://a.com", "h", 0);
            for (int i = 0; i < 5; i++) s.resolve("h");
            s.flush();
            Store.Link st = s.stats("h");
            T.check(st != null && st.hits() == 5, "hits=" + (st == null ? -1 : st.hits()));
        });

        gated("rocks_update_remove", s -> {
            s.shorten("https://a.com", "u", 0);
            T.checkEq(s.update("u", "https://b.com", 0, false), Store.MutResult.OK);
            T.checkEq(s.resolve("u"), "https://b.com");
            T.checkEq(s.update("missing", "https://x.com", 0, false), Store.MutResult.MISSING);
            T.checkEq(s.remove("u"), Store.MutResult.OK);
            T.check(s.resolve("u") == null, "resolve after remove");
            T.checkEq(s.remove("u"), Store.MutResult.MISSING);
        });

        gated("rocks_ttl", s -> {
            s.shorten("https://t.com", "ttl", 80);
            T.check(s.resolve("ttl") != null, "resolve before expiry");
            Thread.sleep(120);
            T.check(s.resolve("ttl") == null, "resolve after expiry");
        });

        gated("rocks_list_stats", s -> {
            s.shorten("https://one.com", "one", 0);
            s.shorten("https://two.com", "two", 0);
            s.resolve("one");
            s.flush();
            Store.Pair<List<Store.Link>, Integer> r = s.list(10, 0, "", "");
            T.checkEq(r.second(), 2);
            boolean found = false;
            for (Store.Link l : r.first())
                if (l.code().equals("one") && l.hits() == 1) found = true;
            T.check(found, "one/1 in list");
            Store.Link st = s.stats("two");
            T.check(st != null && st.url().equals("https://two.com") && st.createdAt() > 0,
                    "stats");
        });

        gated("rocks_bulk", s -> {
            List<String> urls = new java.util.ArrayList<>();
            for (int i = 0; i < 100; i++) urls.add("https://b.example/" + i);
            List<String> codes = s.shortenMany(urls, 0);
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < codes.size(); i++) {
                String c = codes.get(i);
                T.check(c != null && !seen.contains(c), "code " + i);
                seen.add(c);
                T.checkEq(s.resolve(c), urls.get(i));
            }
        });

        gated("rocks_cold_miss", s -> {
            for (int i = 0; i < 200; i++)
                s.shorten("https://x" + (char) ('a' + i % 26) + ".com/" + i, null, 0);
            T.check(s.resolve("nope-missing") == null, "phantom resolve");
            Store.Pair<List<Store.Link>, Integer> r = s.list(500, 0, "", "");
            T.checkEq(r.second(), 200);
        });

        // restart: same path reopens with corpus + counters intact
        gated("rocks_restart", s0 -> {
            Path dir = Files.createTempDirectory("shrt-rocks-restart");
            StoreApi s1 = new RocksStore(dir.toString(), 0, 1000, 50);
            s1.shorten("https://keep.com", "keep", 0);
            s1.resolve("keep");
            s1.flush();
            s1.close();
            StoreApi s2 = new RocksStore(dir.toString(), 0, 1000, 50);
            try {
                T.checkEq(s2.resolve("keep"), "https://keep.com");
                Store.Link st = s2.stats("keep");
                T.check(st != null && st.hits() == 2, "hits=" + (st == null ? -1 : st.hits()));
            } finally { s2.close(); }
        });
    }
}
