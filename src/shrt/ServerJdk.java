package shrt;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/** JDK built-in HttpServer frontend (`SERVER=jdk`) — the fallback. */
public final class ServerJdk {
    public static void serve(int port, Store st, String corsOrigin) throws IOException {
        HttpServer srv = HttpServer.create(new InetSocketAddress(port), 0);
        srv.setExecutor(Executors.newCachedThreadPool());
        srv.createContext("/", ex -> {
            try {
                String method = ex.getRequestMethod();
                String path = ex.getRequestURI().getRawPath();
                String q = ex.getRequestURI().getRawQuery();
                if (q != null) path += "?" + q;
                String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                String admin = ex.getRequestHeaders().getFirst("x-admin-token");
                App.Reply r = App.handle(st, method, path, body, admin != null ? admin : "");
                ex.getResponseHeaders().set("content-type",
                    r.ctype != null ? r.ctype : "application/json");
                if (r.location != null) ex.getResponseHeaders().set("location", r.location);
                ex.getResponseHeaders().set("access-control-allow-origin", corsOrigin);
                ex.getResponseHeaders().set("access-control-allow-methods", "GET,POST,PATCH,DELETE,OPTIONS");
                ex.getResponseHeaders().set("access-control-allow-headers", "content-type,x-admin-token");
                byte[] b = r.body.getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(r.status, b.length > 0 ? b.length : -1);
                if (b.length > 0) try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            } catch (IOException ignored) {
            } finally {
                ex.close();
            }
        });
        srv.start();
        // block forever — main thread parks
        try { Thread.currentThread().join(); } catch (InterruptedException ignored) {}
    }
}
