package shrt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thread-per-connection HTTP/1.1 server (the `mini` frontend):
 * one read batch answered by a single write, keep-alive + pipelining.
 */
public final class ServerMini {

    /** Write a full HTTP/1.1 reply into `out`. */
    static void writeReply(ByteArrayOutputStream out, App.Reply r,
                           boolean cors, String corsOrigin, boolean keepAlive) {
        byte[] body = r.body.getBytes(StandardCharsets.UTF_8);
        out.writeBytes(("HTTP/1.1 " + r.status + " " + statusText(r.status) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        if (r.location != null)
            out.writeBytes(("location: " + r.location + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        out.writeBytes(("content-type: " + (r.ctype != null ? r.ctype : "application/json") + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        out.writeBytes(("content-length: " + body.length + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        if (cors) {
            out.writeBytes(("access-control-allow-origin: " + corsOrigin + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            out.writeBytes("access-control-allow-methods: GET,POST,PATCH,DELETE,OPTIONS\r\n".getBytes(StandardCharsets.ISO_8859_1));
            out.writeBytes("access-control-allow-headers: content-type,x-admin-token\r\n".getBytes(StandardCharsets.ISO_8859_1));
            out.writeBytes("access-control-max-age: 86400\r\n".getBytes(StandardCharsets.ISO_8859_1));
        }
        if (!keepAlive) out.writeBytes("connection: close\r\n".getBytes(StandardCharsets.ISO_8859_1));
        out.writeBytes("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        out.writeBytes(body);
    }

    static String statusText(int s) {
        return switch (s) {
            case 200 -> "OK"; case 201 -> "Created"; case 204 -> "No Content";
            case 302 -> "Found"; case 400 -> "Bad Request"; case 404 -> "Not Found";
            case 409 -> "Conflict"; case 413 -> "Content Too Large";
            case 500 -> "Internal Server Error";
            default -> "OK";
        };
    }

    static int bodyLimit(String path) {
        return path.startsWith("/api/shorten/bulk") ? App.MAX_BULK_BODY : App.MAX_BODY;
    }

    /** One connection: read batches -> process all complete requests -> one write. */
    static void connLoop(Socket s, StoreApi st, String corsOrigin) throws IOException {
        String peer = s.getInetAddress() != null ? s.getInetAddress().getHostAddress() : "";
        boolean trustProxy = System.getenv("TRUST_PROXY") != null;
        s.setTcpNoDelay(true);
        InputStream in = s.getInputStream();
        OutputStream sout = s.getOutputStream();
        byte[] buf = new byte[64 << 10];
        int len = 0, parsed = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(16 << 10);
        try {
            for (;;) {
                int n = in.read(buf, len, buf.length - len);
                if (n < 0) {
                    if (parsed == len) return; // clean EOF
                    return; // partial request dropped
                }
                len += n;
                for (;;) {
                    // find end of headers
                    int he = -1;
                    for (int i = parsed; i + 3 < len; i++)
                        if (buf[i] == '\r' && buf[i+1] == '\n' && buf[i+2] == '\r' && buf[i+3] == '\n') { he = i; break; }
                    if (he < 0) break;
                    // request line
                    int rl = indexOf(buf, parsed, he, "\r\n");
                    int lineEnd = rl < 0 ? he : rl;
                    int sp1 = indexOf(buf, parsed, lineEnd, " ");
                    int sp2 = sp1 < 0 ? -1 : indexOf(buf, sp1 + 1, lineEnd, " ");
                    if (sp1 < 0 || sp2 < 0) return;
                    String method = new String(buf, parsed, sp1 - parsed, StandardCharsets.ISO_8859_1);
                    String path = new String(buf, sp1 + 1, sp2 - sp1 - 1, StandardCharsets.ISO_8859_1);
                    // headers: content-length, connection, x-admin-token
                    int cl = 0;
                    boolean keepAlive = true, kaSet = false;
                    String adminToken = "";
                    String xff = null;
                    int hp = rl < 0 ? he : rl + 2;
                    while (hp < he) {
                        int e = indexOf(buf, hp, he, "\r\n");
                        if (e < 0) e = he;
                        int colon = indexOf(buf, hp, e, ":");
                        if (colon > 0) {
                            String hk = new String(buf, hp, colon - hp, StandardCharsets.ISO_8859_1).trim().toLowerCase();
                            String hv = new String(buf, colon + 1, e - colon - 1, StandardCharsets.ISO_8859_1).trim();
                            switch (hk) {
                                case "content-length" -> { try { cl = Integer.parseInt(hv); } catch (NumberFormatException ignored) {} }
                                case "connection" -> { keepAlive = !hv.equalsIgnoreCase("close"); kaSet = true; }
                                case "x-admin-token" -> adminToken = hv;
                                case "x-forwarded-for" -> xff = hv;
                            }
                        }
                        hp = e + 2;
                    }
                    int total = he + 4 + cl;
                    if (len < total) break; // body incomplete
                    boolean needsBody = method.equals("POST") || method.equals("PATCH");
                    if (needsBody && cl > bodyLimit(path)) {
                        App.Reply r = new App.Reply(413, null, "{\"error\":\"body too large\"}", null);
                        writeReply(out, r, true, corsOrigin, false);
                        sout.write(out.toByteArray());
                        return;
                    }
                    String body = new String(buf, he + 4, cl, StandardCharsets.UTF_8);
                    String client = peer;
                    if (trustProxy && xff != null && !xff.isEmpty()) {
                        int ci = xff.indexOf(',');
                        String f = (ci < 0 ? xff : xff.substring(0, ci)).trim();
                        if (!f.isEmpty()) client = f;
                    }
                    App.Reply r = App.handle(st, method, path, body, adminToken, client);
                    boolean ka = kaSet ? keepAlive : true;
                    writeReply(out, r, true, corsOrigin, ka);
                    parsed = total;
                    if (!ka) { sout.write(out.toByteArray()); return; }
                }
                // compact buffer
                if (parsed > 0) {
                    System.arraycopy(buf, parsed, buf, 0, len - parsed);
                    len -= parsed;
                    parsed = 0;
                }
                if (len == buf.length) return; // header too large
                if (out.size() > 0) {
                    sout.write(out.toByteArray());
                    out.reset();
                }
            }
        } finally {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    private static int indexOf(byte[] b, int from, int to, String needle) {
        byte n0 = (byte) needle.charAt(0);
        int nl = needle.length();
        outer:
        for (int i = from; i + nl <= to; i++) {
            if (b[i] == n0) {
                for (int k = 1; k < nl; k++) if (b[i + k] != (byte) needle.charAt(k)) continue outer;
                return i;
            }
        }
        return -1;
    }

    /** Accept loop on an already-bound socket. */
    public static void serve(ServerSocket ss, StoreApi st, String corsOrigin) throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        while (true) {
            Socket s = ss.accept();
            pool.execute(() -> {
                try { connLoop(s, st, corsOrigin); } catch (IOException ignored) {}
            });
        }
    }
}
