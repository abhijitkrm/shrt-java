package shrt;

import java.util.*;

/** Tiny test registry/framework (no JUnit dep). */
public final class T {
    @FunctionalInterface
    public interface Body { void run() throws Exception; }
    public record Case(String name, Body body) {}
    public static final List<Case> cases = new ArrayList<>();
    static int failed = 0;

    public static void test(String name, Body body) {
        cases.add(new Case(name, body));
    }

    public static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }
    public static void checkEq(Object a, Object b) {
        if (!Objects.equals(a, b)) throw new AssertionError(a + " != " + b);
    }

    public static int run() {
        int passed = 0;
        for (Case c : cases) {
            try {
                c.body().run();
                passed++;
                System.out.println("  ok   " + c.name());
            } catch (Throwable t) {
                failed++;
                System.out.println("  FAIL " + c.name() + "  -- " + t);
            }
        }
        System.out.printf("%d/%d passed%n", passed, cases.size());
        return failed == 0 ? 0 : 1;
    }
}
