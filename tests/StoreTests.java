package shrt;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public final class StoreTests {

    static String tmpdir() throws IOException {
        return Files.createTempDirectory("shrt-java-test-").toString();
    }
    static void rm(String dir) throws IOException {
        if (dir == null) return;
        try (var s = Files.walk(Paths.get(dir))) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.delete(p); } catch (IOException ignored) {}
            });
        }
    }
    static void sleepMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    public static void register() {

        T.test("shorten_generates_random_8char_codes", () -> {
            Store s = Store.open(":memory:", -1);
            String c1 = s.shorten("https://a.com", null, 0);
            String c2 = s.shorten("https://b.com", null, 0);
            T.check(c1 != null && c1.length() == 8, "code len");
            T.check(c2 != null && !c1.equals(c2), "codes differ");
            s.close();
        });

        T.test("resolve_counts_hits", () -> {
            Store s = Store.open(":memory:", -1);
            String c = s.shorten("https://a.com", null, 0);
            T.checkEq(s.resolve(c), "https://a.com");
            T.checkEq(s.resolve(c), "https://a.com");
            var l = s.stats(c);
            T.check(l != null && l.hits() == 2, "hits==2 got " + (l == null ? -1 : l.hits()));
            s.close();
        });

        T.test("resolve_misses", () -> {
            Store s = Store.open(":memory:", -1);
            T.check(s.resolve("nonexist") == null, "miss");
            s.close();
        });

        T.test("alias_collision", () -> {
            Store s = Store.open(":memory:", -1);
            T.check(s.shorten("https://a.com", "mine", 0) != null, "first alias");
            T.check(s.shorten("https://b.com", "mine", 0) == null, "dup alias -> null");
            T.checkEq(s.resolve("mine"), "https://a.com");
            s.close();
        });

        T.test("expired_links_stop_resolving", () -> {
            Store s = Store.open(":memory:", -1);
            String c = s.shorten("https://a.com", null, 1); // 1ms ttl
            sleepMs(5);
            T.check(s.resolve(c) == null, "expired");
            s.close();
        });

        T.test("shorten_many_aligned", () -> {
            Store s = Store.open(":memory:", -1);
            List<String> urls = List.of("https://a.com", "https://b.com", "https://c.com");
            var codes = s.shortenMany(urls, 0);
            T.checkEq(codes.size(), 3);
            for (int i = 0; i < 3; i++) T.checkEq(s.resolve(codes.get(i)), urls.get(i));
            s.close();
        });

        T.test("persists_across_reopen", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store s1 = Store.open(dir, -1);
                String c = s1.shorten("https://a.com", null, 0);
                s1.resolve(c);
                s1.close();

                Store s2 = Store.open(dir, 0);
                var l = s2.stats(c);
                T.check(l != null && l.url().equals("https://a.com"), "reopened url");
                T.check(l.hits() >= 1, "hits persisted");
                T.checkEq(s2.resolve(c), "https://a.com");
                s2.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("codes_prefix_sharded", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store a = Store.open(dir, 0);
                Store b = Store.open(dir, 1);
                String ca = a.shorten("https://a.com", null, 0);
                String cb = b.shorten("https://b.com", null, 0);
                T.check(ca.charAt(0) == Codec.ALPHABET.charAt(0), "a prefix");
                T.check(cb.charAt(0) == Codec.ALPHABET.charAt(1), "b prefix");
                a.close(); b.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("sibling_tailing_converges", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store a = Store.open(dir, 0);
                Store b = Store.open(dir, 1);
                String ca = a.shorten("https://a.com", null, 0);
                a.flush();
                // b doesn't own it but should resolve via lazy tailing
                T.checkEq(b.resolve(ca), "https://a.com");
                a.close(); b.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("sibling_sees_hits_via_tail", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store a = Store.open(dir, 0);
                Store b = Store.open(dir, 1);
                String ca = a.shorten("https://a.com", null, 0);
                a.flush();
                b.resolve(ca); // merge row
                b.resolve(ca); // hit on b's view
                b.flush();
                a.pollTailsNow();
                var l = a.stats(ca);
                T.check(l != null && l.hits() >= 2, "owner sees hits got " + (l == null ? -1 : l.hits()));
                a.close(); b.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("remove_tombstone_survives_reopen", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store s1 = Store.open(dir, 0);
                String c = s1.shorten("https://a.com", null, 0);
                T.checkEq(s1.remove(c), Store.MutResult.OK);
                s1.close();
                Store s2 = Store.open(dir, 0);
                T.check(s2.resolve(c) == null, "deleted after reopen");
                s2.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("update_persists_across_reopen", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store s1 = Store.open(dir, -1);
                String c = s1.shorten("https://old.example", null, 0);
                s1.resolve(c);
                T.checkEq(s1.update(c, "https://new.example", 60_000, true), Store.MutResult.OK);
                T.checkEq(s1.resolve(c), "https://new.example");
                s1.close();

                Store s2 = Store.open(dir, 0);
                var e = s2.stats(c);
                T.check(e != null, "stats");
                T.checkEq(e.url(), "https://new.example");
                T.check(e.expiresAt() != null && e.expiresAt() > System.currentTimeMillis(), "ttl");
                T.check(e.hits() >= 1, "hits");
                s2.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("remote_owned_mutations_return_remote", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store a = Store.open(dir, 0);
                Store b = Store.open(dir, 1);
                String code = a.shorten("https://a.com", null, 0);
                a.flush();
                T.checkEq(b.resolve(code), "https://a.com"); // b learns it via tail
                T.checkEq(b.update(code, "https://x.example", 0, false), Store.MutResult.REMOTE);
                T.checkEq(b.remove(code), Store.MutResult.REMOTE);
                a.close(); b.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });

        T.test("list_paginates_sorts_filters", () -> {
            Store s = Store.open(":memory:", -1);
            for (int i = 0; i < 10; i++) {
                String c = s.shorten("https://e.com/" + i, "al" + i, 0);
                for (int k = 0; k <= i; k++) s.resolve(c);
            }
            var page1 = s.list(5, 0, "hits", "");
            T.checkEq(page1.first().size(), 5);
            T.checkEq(page1.second(), 10);
            T.check(page1.first().get(0).hits() >= page1.first().get(4).hits(), "sorted desc");
            var page2 = s.list(5, 5, "hits", "");
            T.checkEq(page2.first().size(), 5);
            var filtered = s.list(50, 0, "hits", "al3");
            T.checkEq(filtered.first().size(), 1);
            var qurl = s.list(50, 0, "hits", "e.com/7");
            T.checkEq(qurl.first().size(), 1);
            s.close();
        });

        T.test("compact_preserves_rows_and_truncates", () -> {
            String dir;
            try { dir = tmpdir(); } catch (IOException e) { throw new RuntimeException(e); }
            try {
                Store s1 = Store.open(dir, 0);
                String c1 = s1.shorten("https://a.com", null, 0);
                String c2 = s1.shorten("https://b.com", null, 0);
                s1.resolve(c1); s1.resolve(c1); s1.resolve(c2);
                s1.flush();
                s1.compact();
                long logLen = Files.size(Paths.get(dir, "data-0.log"));
                T.check(logLen == 0, "log truncated, got " + logLen);
                s1.close();

                Store s2 = Store.open(dir, 0);
                T.checkEq(s2.resolve(c1), "https://a.com");
                T.checkEq(s2.resolve(c2), "https://b.com");
                var l = s2.stats(c1);
                T.check(l != null && l.hits() >= 2, "hits survived compact");
                s2.close();
            } catch (IOException e) { throw new RuntimeException(e); }
            finally { try { rm(dir); } catch (IOException ignored) {} }
        });
    }
}
