package shrt;

/** AOF wire codec: row {"c","u","a","e","i","n"}, hit {"h","d","i"}, del {"x"}.
 *  Hand-serialized — no JSON library on the hot path. */
public final class Codec {
    public static final String ALPHABET =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private Codec() {}

    /** One parsed log line. */
    public static final class Op {
        public String c, u, h, x;
        public long a, e, i, n, d;
        public boolean hasE;
    }

    static void esc(StringBuilder dst, String s) {
        for (int k = 0; k < s.length(); k++) {
            char ch = s.charAt(k);
            switch (ch) {
                case '"'  -> dst.append("\\\"");
                case '\\' -> dst.append("\\\\");
                case '\n' -> dst.append("\\n");
                case '\r' -> dst.append("\\r");
                case '\t' -> dst.append("\\t");
                default -> {
                    if (ch < 0x20) dst.append(String.format("\\u%04x", (int) ch));
                    else dst.append(ch);
                }
            }
        }
    }

    static String unescape(String b) {
        StringBuilder out = new StringBuilder(b.length());
        for (int i = 0; i < b.length();) {
            char c = b.charAt(i);
            if (c != '\\' || i + 1 >= b.length()) { out.append(c); i++; continue; }
            char e = b.charAt(i + 1);
            switch (e) {
                case 'n' -> { out.append('\n'); i += 2; }
                case 'r' -> { out.append('\r'); i += 2; }
                case 't' -> { out.append('\t'); i += 2; }
                case 'u' -> {
                    if (i + 5 < b.length()) {
                        int cp = Integer.parseInt(b.substring(i + 2, i + 6), 16);
                        out.append((char) cp);
                        i += 6;
                    } else i += 2;
                }
                default -> { out.append(e); i += 2; }
            }
        }
        return out.toString();
    }

    public static void rowLine(StringBuilder b, String c, String u,
                               long a, long e, long i, long n) {
        b.append("{\"c\":\"").append(c).append("\",\"u\":\"");
        esc(b, u);
        b.append("\",\"a\":").append(a).append(",\"e\":");
        if (e == 0) b.append("null"); else b.append(e);
        b.append(",\"i\":").append(i).append(",\"n\":").append(n).append('}');
    }

    public static void hitLine(StringBuilder b, String c, long d, long i) {
        b.append("{\"h\":\"").append(c).append("\",\"d\":").append(d)
         .append(",\"i\":").append(i).append('}');
    }

    public static void delLine(StringBuilder b, String c) {
        b.append("{\"x\":\"").append(c).append("\"}");
    }

    /** Tolerant single-line parser; returns false on non-row lines. */
    public static boolean parseOp(String line, Op o) {
        o.c = o.u = o.h = o.x = null;
        o.a = o.e = o.i = o.n = o.d = 0;
        o.hasE = false;
        int pos = 0, L = line.length();
        while (pos < L) {
            while (pos < L && line.charAt(pos) != '"') pos++;
            if (pos >= L) break;
            pos++;
            int ks = pos;
            while (pos < L && line.charAt(pos) != '"') pos++;
            if (pos >= L) return false;
            char key = pos > ks ? line.charAt(ks) : 0;
            pos++;
            while (pos < L && line.charAt(pos) != ':') pos++;
            if (pos >= L) return false;
            pos++;
            while (pos < L && (line.charAt(pos) == ' ' || line.charAt(pos) == '\t')) pos++;
            if (pos >= L) return false;
            if (line.charAt(pos) == '"') {
                pos++;
                StringBuilder sb = null;
                int vs = pos;
                while (pos < L) {
                    char ch = line.charAt(pos);
                    if (ch == '\\') {
                        if (sb == null) { sb = new StringBuilder(); sb.append(line, vs, pos); }
                        if (pos + 1 < L) {
                            char e = line.charAt(pos + 1);
                            sb.append(switch (e) {
                                case 'n' -> '\n'; case 'r' -> '\r'; case 't' -> '\t';
                                default -> e;
                            });
                        }
                        pos += 2;
                        continue;
                    }
                    if (ch == '"') break;
                    pos++;
                }
                String s = sb != null ? sb.append(line, vs, Math.min(pos, L)).toString()
                                      : line.substring(vs, Math.min(pos, L));
                pos++;
                switch (key) {
                    case 'c' -> o.c = s;
                    case 'u' -> o.u = s;
                    case 'h' -> o.h = s;
                    case 'x' -> o.x = s;
                    default -> {}
                }
            } else {
                int vs = pos;
                while (pos < L && line.charAt(pos) != ',' && line.charAt(pos) != '}') pos++;
                String num = line.substring(vs, pos).trim();
                long v = num.equals("null") || num.isEmpty() ? 0 : Long.parseLong(num);
                switch (key) {
                    case 'a' -> o.a = v;
                    case 'e' -> { if (!num.equals("null")) { o.e = v; o.hasE = true; } }
                    case 'i' -> o.i = v;
                    case 'n' -> o.n = v;
                    case 'd' -> o.d = v;
                    default -> {}
                }
            }
        }
        return o.c != null || o.h != null || o.x != null;
    }
}
