package shrt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * shrt — high-performance URL shortener (Java port).
 *
 * Env config:
 *   PORT        3000    base listen port (WORKERS=N binds PORT+i per worker)
 *   DATA_DIR    data    shard log directory (data-<i>.log, data-<i>.snap)
 *   SERVER      mini    "mini" (custom engine) or "jdk" (HttpServer)
 *   WORKERS     1       processes; each binds PORT+i (Java has no SO_REUSEPORT)
 *   INSTANCE    auto    instance id (auto-claimed via instance-<i>.lock files)
 *   SEED        0       bulk-insert N links if empty (random codes)
 *   HITS        1       "0" disables hit counting
 *   TAIL_MS     0       >0 enables periodic sibling-log polling
 *   CORS_ORIGIN *       value of Access-Control-Allow-Origin
 *   ADMIN_TOKEN unset   enables PATCH/DELETE; requests need x-admin-token: <value>
 *   LINK_TTL_MS 86400000  default AND max link lifetime
 */
public final class Main {

    static int envInt(String k, int def) {
        String v = System.getenv(k);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    static String envStr(String k, String def) {
        String v = System.getenv(k);
        return v != null ? v : def;
    }
    static int port() { return envInt("PORT", 3000); }
    static String dataDir() { return envStr("DATA_DIR", "data"); }

    /** Bulk-insert N links if the store is empty (through the write path). */
    static void seed(int n) {
        try {
            StoreApi s = StoreApi.openEnv(dataDir(), 0);
            if (s.isEmpty()) {
                List<String> urls = new ArrayList<>(n);
                for (int i = 0; i < n; i++) urls.add("https://example.com/" + i);
                s.seed(urls);
            }
            s.close();
        } catch (IOException e) {
            System.err.println("seed: " + e);
        }
    }

    /** Spawn workers as child JVMs; worker i binds PORT+i, INSTANCE=i. */
    static void runSupervisor(int n) throws IOException {
        String java = System.getProperty("java.home") + "/bin/java";
        String cp = System.getProperty("java.class.path");
        List<Process> children = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ProcessBuilder pb = new ProcessBuilder(
                java, "-cp", cp, "shrt.Main");
            pb.environment().put("SHRT_CHILD", "1");
            pb.environment().put("INSTANCE", String.valueOf(i));
            pb.environment().put("PORT", String.valueOf(port() + i));
            pb.environment().put("WORKERS", "1");
            pb.environment().put("SEED", "0");
            pb.inheritIO();
            children.add(pb.start());
        }
        System.out.println("supervisor: " + children.size() + " workers on ports "
            + port() + "-" + (port() + children.size() - 1)
            + " (pid " + ProcessHandle.current().pid() + ")");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Process c : children) c.destroy();
        }));
        for (Process c : children)
            try { c.waitFor(); } catch (InterruptedException ignored) {}
    }

    static void serve() throws IOException {
        StoreApi st;
        try {
            st = StoreApi.openEnv(dataDir(), envInt("INSTANCE", -1));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        ServerSocket ss = null;
        if (!envStr("SERVER", "mini").equals("jdk")) {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port()), 1024);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(st::close));

        String cors = App.corsOrigin();
        if (envStr("SERVER", "mini").equals("jdk")) {
            System.out.println("jdk listening on :" + port()
                + " (pid " + ProcessHandle.current().pid() + ")");
            ServerJdk.serve(port(), st, cors);
        } else {
            System.out.println("mini listening on :" + port()
                + " (pid " + ProcessHandle.current().pid() + ")");
            ServerMini.serve(ss, st, cors);
        }
    }

    public static void main(String[] args) throws IOException {
        int workers = envInt("WORKERS", 1);
        int seedN = envInt("SEED", 0);
        if (workers > 1 && System.getenv("SHRT_CHILD") == null) {
            if (seedN > 0) seed(seedN);
            runSupervisor(workers);
            return;
        }
        if (seedN > 0) seed(seedN);
        serve();
    }
}
