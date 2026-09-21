package shrt;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Transport-agnostic request handler — shared by both HTTP frontends. */
public final class App {
    public static final int MAX_BODY = 4096;
    public static final int MAX_BULK_BODY = 1 << 20;
    public static final int MAX_BULK_URLS = 10_000;
    public static final int MAX_LIST_LIMIT = 1000;

    /** Outcome of handling one request. */
    public static final class Reply {
        public final int status;
        public final String location; // redirect target
        public final String body;
        public final String ctype;    // null -> application/json
        Reply(int s, String loc, String b, String ct) {
            status = s; location = loc; body = b; ctype = ct;
        }
    }

    static long linkTtlMs() {
        String v = System.getenv("LINK_TTL_MS");
        return v != null ? Long.parseLong(v) : 86_400_000L;
    }
    static String corsOrigin() {
        String v = System.getenv("CORS_ORIGIN");
        return v != null ? v : "*";
    }

    // ---------- minimal JSON ----------
    public sealed interface J {
        record Obj(List<Map.Entry<String, J>> kv) implements J {
            public J find(String k) {
                for (var e : kv) if (e.getKey().equals(k)) return e.getValue();
                return null;
            }
        }
        record Arr(List<J> items) implements J {}
        record Str(String v) implements J {}
        record Num(double v) implements J {}
        record Lit() implements J {} // true/false/null
    }

    private static final class Jp {
        final String s; int p = 0; boolean ok = true;
        Jp(String s) { this.s = s; }
        void ws() { while (p < s.length() && " \t\n\r".indexOf(s.charAt(p)) >= 0) p++; }
        boolean lit(String w) {
            if (!s.startsWith(w, p)) return false;
            p += w.length(); return true;
        }
        J val() {
            ws();
            if (p >= s.length()) { ok = false; return new J.Lit(); }
            char c = s.charAt(p);
            return switch (c) {
                case '{' -> obj();
                case '[' -> arr();
                case '"' -> new J.Str(str());
                case 't' -> { if (lit("true")) yield new J.Lit(); ok = false; yield new J.Lit(); }
                case 'f' -> { if (lit("false")) yield new J.Lit(); ok = false; yield new J.Lit(); }
                case 'n' -> { if (lit("null")) yield new J.Lit(); ok = false; yield new J.Lit(); }
                default -> num();
            };
        }
        String str() {
            StringBuilder out = new StringBuilder();
            if (p >= s.length() || s.charAt(p) != '"') { ok = false; return ""; }
            p++;
            while (p < s.length() && s.charAt(p) != '"') {
                char c = s.charAt(p);
                if (c == '\\' && p + 1 < s.length()) {
                    p++;
                    char e = s.charAt(p);
                    switch (e) {
                        case 'n' -> out.append('\n');
                        case 'r' -> out.append('\r');
                        case 't' -> out.append('\t');
                        case 'u' -> { if (p + 4 < s.length()) { out.append('?'); p += 4; } }
                        default -> out.append(e);
                    }
                    p++;
                    continue;
                }
                out.append(c); p++;
            }
            if (p >= s.length()) { ok = false; return out.toString(); }
            p++;
            return out.toString();
        }
        J num() {
            int st = p;
            if (p < s.length() && (s.charAt(p) == '-' || s.charAt(p) == '+')) p++;
            while (p < s.length() && (Character.isDigit(s.charAt(p)) || s.charAt(p) == '.'
                   || s.charAt(p) == 'e' || s.charAt(p) == 'E'
                   || s.charAt(p) == '-' || s.charAt(p) == '+')) p++;
            if (p == st) { ok = false; return new J.Lit(); }
            try { return new J.Num(Double.parseDouble(s.substring(st, p))); }
            catch (NumberFormatException e) { ok = false; return new J.Lit(); }
        }
        J arr() {
            List<J> items = new ArrayList<>();
            p++; ws();
            if (p < s.length() && s.charAt(p) == ']') { p++; return new J.Arr(items); }
            for (;;) {
                items.add(val());
                if (!ok) return new J.Arr(items);
                ws();
                if (p < s.length() && s.charAt(p) == ',') { p++; continue; }
                if (p < s.length() && s.charAt(p) == ']') { p++; return new J.Arr(items); }
                ok = false;
                return new J.Arr(items);
            }
        }
        J obj() {
            List<Map.Entry<String, J>> kv = new ArrayList<>();
            p++; ws();
            if (p < s.length() && s.charAt(p) == '}') { p++; return new J.Obj(kv); }
            for (;;) {
                ws();
                String k = str();
                if (!ok) return new J.Obj(kv);
                ws();
                if (p >= s.length() || s.charAt(p) != ':') { ok = false; return new J.Obj(kv); }
                p++;
                kv.add(Map.entry(k, val()));
                if (!ok) return new J.Obj(kv);
                ws();
                if (p < s.length() && s.charAt(p) == ',') { p++; continue; }
                if (p < s.length() && s.charAt(p) == '}') { p++; return new J.Obj(kv); }
                ok = false;
                return new J.Obj(kv);
            }
        }
    }

    static J parseJson(String s) {
        Jp p = new Jp(s);
        J v = p.val();
        p.ws();
        return p.ok && p.p == s.length() ? v : null;
    }

    // ---------- validation ----------

    static boolean codeOk(String s) {
        if (s.isEmpty() || s.length() > 64) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '-') return false;
        }
        return true;
    }

    static boolean isValidUrl(String u) {
        if (u.isEmpty() || u.length() > 2048) return false;
        int pos;
        if (u.startsWith("http://")) pos = 7;
        else if (u.startsWith("https://")) pos = 8;
        else return false;
        int end = u.length();
        for (int i = pos; i < u.length(); i++) {
            char c = u.charAt(i);
            if (c == '/' || c == '?' || c == '#') { end = i; break; }
        }
        if (end == pos) return false;
        for (int i = pos; i < end; i++) {
            char c = u.charAt(i);
            if (c == ' ' || c == '"' || c == '\\' || c < 0x21) return false;
        }
        return true;
    }

    // ---------- responses ----------

    static Reply mk(int status, String body) { return new Reply(status, null, body, null); }
    static Reply bad(String err) { return mk(400, "{\"error\":\"" + err + "\"}"); }
    static Reply notFound() { return mk(404, "{\"error\":\"not found\"}"); }

    private static volatile String uiHtml;
    static String uiHtml() {
        if (uiHtml == null) {
            synchronized (App.class) {
                if (uiHtml == null) {
                    try { uiHtml = Files.readString(Path.of("ui/index.html")); }
                    catch (IOException e) { uiHtml = ""; }
                }
            }
        }
        return uiHtml;
    }

    static void linkJson(StringBuilder b, Store.Link l) {
        b.append("{\"code\":\"").append(l.code()).append("\",\"url\":\"");
        for (int i = 0; i < l.url().length(); i++) {
            char c = l.url().charAt(i);
            if (c == '"' || c == '\\') b.append('\\');
            b.append(c);
        }
        b.append("\",\"hits\":").append(l.hits())
         .append(",\"created_at\":").append(l.createdAt())
         .append(",\"expires_at\":");
        if (l.expiresAt() != null) b.append(l.expiresAt()); else b.append("null");
        b.append('}');
    }

    static boolean adminOk(String token) {
        String want = System.getenv("ADMIN_TOKEN");
        return want != null && !want.isEmpty() && token.equals(want);
    }

    static String urlDecode(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length()) {
                int hi = Character.digit(s.charAt(i + 1), 16);
                int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) { out.append((char) (hi * 16 + lo)); i += 2; continue; }
            }
            out.append(c == '+' ? ' ' : c);
        }
        return out.toString();
    }

    static Map<String, String> parseQuery(String q) {
        Map<String, String> m = new HashMap<>();
        int i = 0;
        while (i < q.length()) {
            int amp = q.indexOf('&', i);
            String kv = q.substring(i, amp < 0 ? q.length() : amp);
            int eq = kv.indexOf('=');
            if (eq >= 0) m.put(urlDecode(kv.substring(0, eq)), urlDecode(kv.substring(eq + 1)));
            i = amp < 0 ? q.length() : amp + 1;
        }
        return m;
    }

    // ---------- handlers ----------

    static Reply shortenOne(Store st, J.Obj p) {
        J u = p.find("url");
        if (!(u instanceof J.Str us) || !isValidUrl(us.v())) return bad("invalid url");
        String alias = null;
        if (p.find("alias") instanceof J.Str a) {
            if (!codeOk(a.v())) return bad("invalid alias");
            alias = a.v();
        }
        long ttl = linkTtlMs();
        J t = p.find("ttl_ms");
        if (t != null) {
            if (!(t instanceof J.Num tn) || tn.v() <= 0) return bad("invalid ttl_ms");
            ttl = Math.min((long) tn.v(), linkTtlMs());
        }
        String code = st.shorten(us.v(), alias, ttl);
        if (code == null) return mk(409, "{\"error\":\"alias taken\"}");
        return mk(201, "{\"code\":\"" + code + "\",\"short_url\":\"/" + code + "\"}");
    }

    static boolean okBulkUrl(String u) {
        if (u.length() <= 7 || u.length() > 2048) return false;
        if (!u.startsWith("http://") && !u.startsWith("https://")) return false;
        for (int i = 0; i < u.length(); i++) {
            char c = u.charAt(i);
            if (c == '"' || c == '\\' || c == '\n' || c == '\r') return false;
        }
        return true;
    }

    static Reply bulkReply(Store st, List<String> us) {
        List<String> codes = st.shortenMany(us, linkTtlMs());
        StringBuilder b = new StringBuilder(codes.size() * 10 + 24);
        b.append("{\"count\":").append(codes.size()).append(",\"codes\":[");
        for (int i = 0; i < codes.size(); i++) {
            if (i > 0) b.append(',');
            b.append('"').append(codes.get(i)).append('"');
        }
        b.append("]}");
        return mk(201, b.toString());
    }

    static Reply shortenBulk(Store st, J.Obj p) {
        String msg = "urls must be 1-" + MAX_BULK_URLS + " valid http(s) urls";
        if (!(p.find("urls") instanceof J.Arr urls) || urls.items().isEmpty()
            || urls.items().size() > MAX_BULK_URLS) return bad(msg);
        List<String> us = new ArrayList<>(urls.items().size());
        for (J j : urls.items()) {
            if (!(j instanceof J.Str js) || !okBulkUrl(js.v())) return bad(msg);
            us.add(js.v());
        }
        return bulkReply(st, us);
    }

    /** Zero-object fast path for {"urls":["..",".."]} — the bulk hot body.
     *  Returns null on anything unusual so caller falls back to full JSON. */
    static List<String> tryBulkUrls(String body) {
        int i = body.indexOf("\"urls\"");
        if (i < 0) return null;
        i += 6;
        int L = body.length();
        while (i < L && Character.isWhitespace(body.charAt(i))) i++;
        if (i >= L || body.charAt(i) != ':') return null;
        i++;
        while (i < L && Character.isWhitespace(body.charAt(i))) i++;
        if (i >= L || body.charAt(i) != '[') return null;
        i++;
        List<String> out = new ArrayList<>(1024);
        for (;;) {
            while (i < L && Character.isWhitespace(body.charAt(i))) i++;
            if (i >= L) return null;
            char c = body.charAt(i);
            if (c == ']') return out;
            if (c == ',') { i++; continue; }
            if (c != '"') return null;
            int j = i + 1;
            boolean esc = false;
            while (j < L) {
                char ch = body.charAt(j);
                if (ch == '\\') { esc = true; j += 2; continue; }
                if (ch == '"') break;
                j++;
            }
            if (j >= L) return null;
            if (esc) {
                Jp jp = new Jp(body.substring(i, j + 1));
                out.add(jp.str());
                if (!jp.ok) return null;
            } else {
                out.add(body.substring(i + 1, j));
            }
            i = j + 1;
        }
    }

    // ---------- handler ----------

    public static Reply handle(Store st, String method, String path,
                               String body, String adminToken) {
        int qi = path.indexOf('?');
        String pathname = qi < 0 ? path : path.substring(0, qi);
        String query = qi < 0 ? "" : path.substring(qi + 1);

        Metrics.tick();

        if (method.equals("OPTIONS")) return new Reply(204, null, "", null);

        if (method.equals("GET")) {
            switch (pathname) {
                case "/api/health" -> { return mk(200, "{\"ok\":true}"); }
                case "/api/metrics" -> { return mk(200, Metrics.snapshot()); }
                case "/" -> {
                    String html = uiHtml();
                    if (html.isEmpty()) return notFound();
                    return new Reply(200, null, html, "text/html; charset=utf-8");
                }
                case "/api/links" -> {
                    var pq = parseQuery(query);
                    long limit = 50;
                    String ls = pq.get("limit");
                    if (ls != null) {
                        try { long n = Long.parseLong(ls); if (n != 0) limit = n; }
                        catch (NumberFormatException ignored) {}
                    }
                    limit = Math.max(1, Math.min(limit, MAX_LIST_LIMIT));
                    long offset = 0;
                    String os = pq.get("offset");
                    if (os != null) {
                        try { long n = Long.parseLong(os); if (n > 0) offset = n; }
                        catch (NumberFormatException ignored) {}
                    }
                    String sort = "hits".equals(pq.get("sort")) ? "hits" : "created";
                    String q = pq.getOrDefault("q", "");
                    var res = st.list((int) limit, (int) offset, sort, q);
                    StringBuilder b = new StringBuilder(res.first().size() * 96 + 32);
                    b.append("{\"links\":[");
                    for (int i = 0; i < res.first().size(); i++) {
                        if (i > 0) b.append(',');
                        linkJson(b, res.first().get(i));
                    }
                    b.append("],\"total\":").append(res.second()).append('}');
                    return mk(200, b.toString());
                }
                default -> {
                    if (pathname.startsWith("/api/stats/")) {
                        var link = st.stats(pathname.substring(11));
                        if (link == null) return notFound();
                        StringBuilder b = new StringBuilder(128);
                        linkJson(b, link);
                        return mk(200, b.toString());
                    }
                    String code = pathname.substring(1);
                    if (codeOk(code)) {
                        String target = st.resolve(code);
                        if (target != null) return new Reply(302, target, "", null);
                    }
                    return notFound();
                }
            }
        }

        if (method.equals("POST")) {
            if (!pathname.equals("/api/shorten") && !pathname.equals("/api/shorten/bulk"))
                return notFound();
            if (pathname.equals("/api/shorten/bulk")) {
                List<String> us = tryBulkUrls(body);
                if (us != null) {
                    String msg = "urls must be 1-" + MAX_BULK_URLS + " valid http(s) urls";
                    if (us.isEmpty() || us.size() > MAX_BULK_URLS) return bad(msg);
                    for (String u : us) if (!okBulkUrl(u)) return bad(msg);
                    return bulkReply(st, us);
                }
            }
            J pj = parseJson(body);
            if (!(pj instanceof J.Obj p)) return bad("invalid json");
            return pathname.equals("/api/shorten") ? shortenOne(st, p) : shortenBulk(st, p);
        }

        if (method.equals("PATCH") || method.equals("DELETE")) {
            if (!pathname.startsWith("/api/links/") || !adminOk(adminToken))
                return notFound();
            String code = pathname.substring(11);
            if (!codeOk(code)) return bad("invalid code");
            if (method.equals("DELETE")) {
                return switch (st.remove(code)) {
                    case OK -> new Reply(204, null, "", null);
                    case MISSING -> notFound();
                    case REMOTE -> mk(409, "{\"error\":\"owned by another instance\"}");
                };
            }
            J pj = parseJson(body);
            if (!(pj instanceof J.Obj p)) return bad("invalid json");
            if (p.find("url") instanceof J.Str u) {
                if (!isValidUrl(u.v())) return bad("invalid url");
                long ttl = 0;
                boolean hasTtl = false;
                if (p.find("ttl_ms") instanceof J.Num t) {
                    ttl = Math.min((long) t.v(), linkTtlMs());
                    hasTtl = true;
                }
                return switch (st.update(code, u.v(), ttl, hasTtl)) {
                    case OK -> mk(200, "{\"ok\":true}");
                    case MISSING -> notFound();
                    case REMOTE -> mk(409, "{\"error\":\"owned by another instance\"}");
                };
            }
            return bad("nothing to update");
        }
        return notFound();
    }
}
