package shrt;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * shrt bench — spawns real shrt servers (java -cp ... shrt.Main) and drives
 * load with a raw-TCP generator (keep-alive + optional pipelining).
 *
 * Usage: java -cp classes shrt.Bench   env: BENCH_DURATION=5 CONNECTIONS=64
 */
public final class Bench {

    static int envInt(String k, int def) {
        String v = System.getenv(k);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    static int durationSecs() { return envInt("BENCH_DURATION", 5); }
    static int connections() { return envInt("CONNECTIONS", 64); }

    static Socket connect(int port) throws IOException {
        Socket s = new Socket("127.0.0.1", port);
        s.setTcpNoDelay(true);
        return s;
    }

    // ---------- response reader ----------

    static final class RConn {
        final Socket s; final InputStream in;
        byte[] buf = new byte[64 << 10]; int len = 0, pos = 0;
        RConn(Socket s) throws IOException { this.s = s; this.in = s.getInputStream(); }
        int fill() throws IOException {
            if (pos > 0) {
                if (pos > len) { pos = 0; len = 0; } // corrupt: reset
                else { System.arraycopy(buf, pos, buf, 0, len - pos); len -= pos; pos = 0; }
            }
            if (len == buf.length) { // grow for big bodies
                byte[] nb = new byte[buf.length * 2];
                System.arraycopy(buf, 0, nb, 0, len);
                buf = nb;
            }
            int n = in.read(buf, len, buf.length - len);
            if (n > 0) len += n;
            return n;
        }
        /** Read one response; returns status or -1. */
        int readResp() throws IOException {
            for (;;) {
                // find header end
                int he = -1;
                for (int i = pos; i + 3 < len; i++)
                    if (buf[i] == '\r' && buf[i+1] == '\n' && buf[i+2] == '\r' && buf[i+3] == '\n') { he = i; break; }
                if (he < 0) { if (fill() < 0) return -1; continue; }
                // status code
                int sp = -1;
                for (int i = pos; i < he; i++) if (buf[i] == ' ') { sp = i; break; }
                int code = sp < 0 ? 0 : atoi(sp + 1, he);
                // headers
                long cl = 0; boolean chunked = false;
                int hp = pos;
                int rlEnd = -1;
                for (int i = pos; i < he; i++) if (buf[i] == '\r' && buf[i+1] == '\n') { rlEnd = i; break; }
                hp = rlEnd < 0 ? he : rlEnd + 2;
                while (hp < he) {
                    int e = -1;
                    for (int i = hp; i + 1 < he; i++) if (buf[i] == '\r' && buf[i+1] == '\n') { e = i; break; }
                    if (e < 0) e = he;
                    String h = new String(buf, hp, e - hp, StandardCharsets.ISO_8859_1);
                    int colon = h.indexOf(':');
                    if (colon > 0) {
                        String k = h.substring(0, colon).trim().toLowerCase();
                        String v = h.substring(colon + 1).trim();
                        if (k.equals("content-length")) cl = Long.parseLong(v);
                        else if (k.equals("transfer-encoding") && v.contains("chunked")) chunked = true;
                    }
                    hp = e + 2;
                }
                int bodyStart = he + 4;
                if (chunked) {
                    int p = bodyStart;
                    for (;;) {
                        int e = -1;
                        while (true) {
                            for (int i = p; i + 1 < len; i++) if (buf[i] == '\r' && buf[i+1] == '\n') { e = i; break; }
                            if (e >= 0) break;
                            pos = p;
                            if (fill() < 0) return -1;
                            p = 0; // buffer compacted to 0
                        }
                        long sz = Long.parseLong(new String(buf, p, e - p, StandardCharsets.ISO_8859_1).trim(), 16);
                        if (sz == 0) { pos = e + 4; break; }
                        long need = e + 2 + sz + 2;
                        while (len < need) { pos = 0; p = 0; if (fill() < 0) return -1; }
                        p = (int) need;
                    }
                } else {
                    while (len < bodyStart + cl) if (fill() < 0) return -1;
                    pos = bodyStart + (int) cl;
                }
                return code;
            }
        }
        int atoi(int from, int to) {
            int v = 0;
            for (int i = from; i < to && buf[i] >= '0' && buf[i] <= '9'; i++) v = v * 10 + buf[i] - '0';
            return v;
        }
    }

    // ---------- loadgen ----------

    record Result(long reqs, long non2xx, long errs, double avg, double p99) {}

    static Result blast(int[] ports, int conns, List<byte[]> reqs, int pipe, int durMs) {
        AtomicLong total = new AtomicLong(), non2xx = new AtomicLong(), errs = new AtomicLong();
        List<Double> lats = Collections.synchronizedList(new ArrayList<>());
        long deadline = System.nanoTime() + durMs * 1_000_000L;
        List<Thread> ths = new ArrayList<>();
        for (int c = 0; c < conns; c++) {
            final int ci = c;
            ths.add(new Thread(() -> {
                int port = ports[ci % ports.length];
                try {
                    RConn rc = new RConn(connect(port));
                    OutputStream out = rc.s.getOutputStream();
                    int i = ci % reqs.size();
                    List<Double> my = new ArrayList<>();
                    while (System.nanoTime() < deadline) {
                        long t0 = System.nanoTime();
                        int batch = 0;
                        for (int k = 0; k < pipe; k++) {
                            try { out.write(reqs.get(i)); }
                            catch (IOException e) { errs.incrementAndGet(); break; }
                            batch++;
                            i = (i + 1) % reqs.size();
                        }
                        out.flush();
                        if (batch == 0) break;
                        int got = 0;
                        while (got < batch) {
                            int code = rc.readResp();
                            if (code < 0) { errs.incrementAndGet(); break; }
                            got++;
                            if (code < 200 || code >= 400) non2xx.incrementAndGet();
                        }
                        if (got == 0) break;
                        double el = (System.nanoTime() - t0) / 1e6 / got;
                        total.addAndGet(got);
                        my.add(el);
                    }
                    rc.s.close();
                    lats.addAll(my);
                } catch (IOException e) { errs.incrementAndGet(); }
            }));
        }
        ths.forEach(Thread::start);
        ths.forEach(t -> { try { t.join(); } catch (InterruptedException ignored) {} });
        lats.sort(null);
        double avg = lats.isEmpty() ? 0 : lats.stream().mapToDouble(d -> d).average().orElse(0);
        double p99 = lats.isEmpty() ? 0 : lats.get((int) (lats.size() * 0.99));
        return new Result(total.get(), non2xx.get(), errs.get(), avg, p99);
    }

    static void report(String name, Result r, double rowsPerReq) {
        double rps = r.reqs() / (double) durationSecs();
        System.out.printf("%-28s %10.0f req/s  %10.0f rows/s  lat avg %6.2fms  p99 %6.2fms  non2xx/3xx %7d  err %d%n",
            name, rps, rps * rowsPerReq, r.avg(), r.p99(), r.non2xx(), r.errs());
    }

    // ---------- server lifecycle ----------

    static boolean waitHealthy(int port) {
        long deadline = System.currentTimeMillis() + 15_000;
        byte[] req = "GET /api/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = connect(port)) {
                s.getOutputStream().write(req);
                s.getOutputStream().flush();
                byte[] b = s.getInputStream().readNBytes(256);
                if (new String(b, StandardCharsets.ISO_8859_1).contains("200")) return true;
            } catch (IOException ignored) {}
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        }
        return false;
    }

    static Process startServer(Map<String, String> env, int port) throws IOException {
        String java = System.getProperty("java.home") + "/bin/java";
        String cp = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(java, "-XX:+UseZGC", "-Xmx4g", "-cp", cp, "shrt.Main");
        pb.environment().putAll(env);
        pb.redirectOutput(new File("/dev/null"));
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        if (!waitHealthy(port)) {
            p.destroyForcibly();
            throw new RuntimeException("server :" + port + " did not start");
        }
        return p;
    }

    static void stopServer(Process p) {
        p.destroy();
        try { p.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
    }

    // ---------- request templates ----------

    static byte[] getReq(String path) {
        return ("GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);
    }
    static byte[] postReq(String path, String body) {
        return ("POST " + path + " HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: "
            + body.getBytes(StandardCharsets.UTF_8).length + "\r\n\r\n" + body)
            .getBytes(StandardCharsets.ISO_8859_1);
    }

    static List<String> makeCodes(int port, int n) throws IOException {
        List<String> codes = new ArrayList<>(n);
        RConn rc = new RConn(connect(port));
        OutputStream out = rc.s.getOutputStream();
        for (int i = 0; i < n; i++) {
            String code = "bk" + i;
            byte[] r = postReq("/api/shorten",
                "{\"url\":\"https://bench.example/" + i + "\",\"alias\":\"" + code + "\"}");
            out.write(r); out.flush();
            int status = rc.readResp();
            if (status != 201) throw new RuntimeException("alias seed failed: " + status);
            codes.add(code);
        }
        rc.s.close();
        return codes;
    }

    static void rmrf(Path p) {
        try (var s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> {
                try { Files.delete(f); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        int keyspace = 50_000;
        String tmp = System.getProperty("java.io.tmpdir");
        int portBase = 5600;
        int dur = durationSecs() * 1000;
        int conns = connections();

        System.out.printf("bench: %d conns x %ds, keyspace %d%n%n", conns, durationSecs(), keyspace);

        byte[] writeReq = postReq("/api/shorten", "{\"url\":\"https://bench.example/write\"}");
        int BULK_N = 1000;
        StringBuilder sb = new StringBuilder("{\"urls\":[");
        for (int i = 0; i < BULK_N; i++) {
            if (i > 0) sb.append(',');
            sb.append("\"https://b.example/").append(i).append('"');
        }
        sb.append("]}");
        byte[] bulkReq = postReq("/api/shorten/bulk", sb.toString());

        int[] port = {portBase};
        BiFunction<String, String[], Map<String, String>> envFor = (tag, extra) -> {
            port[0] += 10;
            Map<String, String> e = new HashMap<>();
            e.put("PORT", String.valueOf(port[0]));
            Path dir = Paths.get(tmp, "bench-java-" + tag + "-" + port[0]);
            rmrf(dir);
            e.put("DATA_DIR", dir.toString());
            for (int i = 0; i + 1 < extra.length; i += 2) e.put(extra[i], extra[i + 1]);
            return e;
        };
        String[] extra = {};

        // --- mini, single instance ---
        {
            Map<String, String> e = envFor.apply("mini1", new String[]{"SERVER", "mini", "SEED", String.valueOf(keyspace)});
            Process srv = startServer(e, port[0]);
            var codes = makeCodes(port[0], 100);
            List<byte[]> reqs = codes.stream().map(c -> getReq("/" + c)).toList();
            report("redirect (mini)", blast(new int[]{port[0]}, conns, reqs, 1, dur), 1.0);
            report("redirect (mini, p10)", blast(new int[]{port[0]}, conns, reqs, 10, dur), 1.0);
            List<byte[]> mixed = new ArrayList<>(reqs.subList(0, 95));
            for (int i = 0; i < 5; i++) mixed.add(writeReq);
            report("mixed 95/5 (mini)", blast(new int[]{port[0]}, conns, mixed, 1, dur), 1.0);
            report("shorten (mini)", blast(new int[]{port[0]}, conns, List.of(writeReq), 1, dur), 1.0);
            report("bulk x1000 (mini)", blast(new int[]{port[0]}, conns / 4, List.of(bulkReq), 1, dur), BULK_N);
            stopServer(srv);
        }

        // --- jdk comparison ---
        {
            Map<String, String> e = envFor.apply("jdk1", new String[]{"SERVER", "jdk", "SEED", String.valueOf(keyspace)});
            Process srv = startServer(e, port[0]);
            var codes = makeCodes(port[0], 100);
            List<byte[]> reqs = codes.stream().map(c -> getReq("/" + c)).toList();
            report("redirect (jdk)", blast(new int[]{port[0]}, conns, reqs, 1, dur), 1.0);
            report("shorten (jdk)", blast(new int[]{port[0]}, conns, List.of(writeReq), 1, dur), 1.0);
            stopServer(srv);
        }

        // --- mini multi-instance: 4 procs on consecutive ports ---
        {
            int base = port[0] + 10;
            Map<String, String> e = envFor.apply("mini4", new String[]{
                "SERVER", "mini", "WORKERS", "4", "SEED", String.valueOf(keyspace)});
            // supervisor binds base..base+3
            Process srv = startServer(e, base);
            int[] ports = {base, base + 1, base + 2, base + 3};
            report("bulk x1000 (mini x4)", blast(ports, conns / 2, List.of(bulkReq), 1, dur), BULK_N);
            var codes = makeCodes(base, 100);
            List<byte[]> reqs = codes.stream().map(c -> getReq("/" + c)).toList();
            // warm once on every port so lazy tailing merges aliases everywhere
            for (String c : codes) {
                for (int p : ports) {
                    try (Socket s = connect(p)) {
                        s.getOutputStream().write(getReq("/" + c));
                        s.getOutputStream().flush();
                        s.getInputStream().readNBytes(512);
                    } catch (IOException ignored) {}
                }
            }
            report("redirect (mini x4)", blast(ports, conns, reqs, 1, dur), 1.0);
            stopServer(srv);
        }
    }

    interface BiFunction<A, B, C> { C apply(A a, B b); }
}
