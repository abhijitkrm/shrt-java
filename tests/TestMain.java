package shrt;

public final class TestMain {
    public static void main(String[] args) {
        StoreTests.register();
        ApiTests.register();
        System.exit(T.run());
    }
}
