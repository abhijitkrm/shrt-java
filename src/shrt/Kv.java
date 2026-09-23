package shrt;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Minimal RESP client (Redis / DragonflyDB / KeyDB). Zero deps.
 * Bounded pool of sockets; a command checks one out, writes, reads,
 * releases. Pipelines collapse N round-trips into one flush + one read.
 */
public final class Kv {
    public static final class Resp {
        public final byte kind;          // '+', '-', ':', '$', '*'
        public final byte[] str;         // '+'/'-'/'$' payload (null for nil)
        public final long num;           // ':'
        public final List<Resp> arr;     // '*'
        Resp(byte k, byte[] s, long n, List<Resp> a) { kind = k; str = s; num = n; arr = a; }
        public boolean isNull() { return (kind == '$' || kind == '*') && num < 0; }
        public String text() { return str == null ? null : new String(str, StandardCharsets.UTF_8); }
    }

    private static final class Conn {
        final Socket s;
        final OutputStream out;
        final BufferedInputStream in;
        Conn(Socket s) throws IOException {
            this.s = s;
            this.out = s.getOutputStream();
            this.in = new BufferedInputStream(s.getInputStream(), 64 << 10);
        }
    }

    private final String host;
    private final int port;
    private final ArrayBlockingQueue<Conn> pool;

    public Kv(String addr, int nconn) throws IOException {
        int c = addr.lastIndexOf(':');
        host = c < 0 ? addr : addr.substring(0, c);
        port = c < 0 ? 6379 : Integer.parseInt(addr.substring(c + 1));
        pool = new ArrayBlockingQueue<>(Math.max(1, nconn));
        for (int i = 0; i < nconn; i++) pool.add(dial());
        Resp r = cmd("PING");
        if (r.kind != '+' || !"PONG".equals(r.text()))
            throw new IOException("kv: PING failed on " + addr);
    }

    private Conn dial() throws IOException {
        Socket s = new Socket(host, port);
        s.setTcpNoDelay(true);
        return new Conn(s);
    }

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** One command; args[0] is the verb. */
    public Resp cmd(String... args) throws IOException {
        List<Resp> rs = pipe(java.util.Collections.singletonList(args));
        return rs.get(0);
    }

    /** N commands, one round-trip. Each String[] is one command's args. */
    public List<Resp> pipe(List<String[]> cmds) throws IOException {
        Conn c;
        try {
            c = pool.take();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", ie);
        }
        try {
            // serialize: *N\r\n then $len\r\npayload\r\n per arg
            int total = 0;
            for (String[] args : cmds) total += args.length;
            var buf = new java.io.ByteArrayOutputStream(total * 32 + 64);
            for (String[] args : cmds) {
                buf.write('*');
                buf.write(Integer.toString(args.length).getBytes());
                buf.write("\r\n".getBytes());
                for (String a : args) {
                    byte[] ab = b(a);
                    buf.write('$');
                    buf.write(Integer.toString(ab.length).getBytes());
                    buf.write("\r\n".getBytes());
                    buf.write(ab);
                    buf.write("\r\n".getBytes());
                }
            }
            c.out.write(buf.toByteArray());
            c.out.flush();
            List<Resp> rs = new ArrayList<>(cmds.size());
            for (int i = 0; i < cmds.size(); i++) rs.add(read(c.in));
            pool.offer(c);
            return rs;
        } catch (IOException e) {
            try { c.s.close(); } catch (IOException ignored) {}
            // refill so the pool doesn't drain on repeated failures
            try { pool.offer(dial()); } catch (IOException ignored) {}
            throw e;
        }
    }

    private static String line(BufferedInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int prev = -1, ch;
        while ((ch = in.read()) >= 0) {
            if (prev == '\r' && ch == '\n') {
                sb.setLength(sb.length() - 1);
                return sb.toString();
            }
            sb.append((char) ch);
            prev = ch;
        }
        throw new IOException("kv: eof");
    }

    private static Resp read(BufferedInputStream in) throws IOException {
        int k = in.read();
        if (k < 0) throw new IOException("kv: eof");
        switch (k) {
            case '+': case '-':
                return new Resp((byte) k, b(line(in)), 0, null);
            case ':':
                return new Resp((byte) k, null, Long.parseLong(line(in)), null);
            case '$': {
                long n = Long.parseLong(line(in));
                if (n < 0) return new Resp((byte) k, null, -1, null);
                byte[] buf = in.readNBytes((int) n + 2);
                if (buf.length < n + 2) throw new IOException("kv: eof");
                byte[] v = new byte[(int) n];
                System.arraycopy(buf, 0, v, 0, (int) n);
                return new Resp((byte) k, v, n, null);
            }
            case '*': {
                long n = Long.parseLong(line(in));
                if (n < 0) return new Resp((byte) k, null, -1, null);
                List<Resp> arr = new ArrayList<>((int) n);
                for (int i = 0; i < n; i++) arr.add(read(in));
                return new Resp((byte) k, null, n, arr);
            }
        }
        throw new IOException("kv: bad reply " + k);
    }

    // ---- typed helpers ----

    public byte[] get(String key) throws IOException {
        Resp r = cmd("GET", key);
        return r.isNull() ? null : r.str;
    }

    public boolean set(String key, String val, long pxMs, boolean nx) throws IOException {
        List<String> args = new ArrayList<>(List.of("SET", key, val));
        if (pxMs > 0) { args.add("PX"); args.add(Long.toString(pxMs)); }
        if (nx) args.add("NX");
        Resp r = cmd(args.toArray(new String[0]));
        return r.kind == '+' && "OK".equals(r.text());
    }

    public long del(String key) throws IOException {
        return cmd("DEL", key).num;
    }

    public byte[] hget(String key, String field) throws IOException {
        Resp r = cmd("HGET", key, field);
        return r.str;
    }
    public boolean hsetnx(String key, String field, String val) throws IOException {
        return cmd("HSETNX", key, field, val).num == 1;
    }
    public void hset(String key, String field, String val) throws IOException {
        cmd("HSET", key, field, val);
    }
    public long hdel(String key, String field) throws IOException {
        return cmd("HDEL", key, field).num;
    }
    public void hincrbyMany(List<String[]> deltas) throws IOException {
        if (deltas.isEmpty()) return;
        List<String[]> cmds = new ArrayList<>(deltas.size());
        for (String[] d : deltas) cmds.add(new String[]{"HINCRBY", d[0], d[1], d[2]});
        pipe(cmds);
    }
    public void hscanEach(String key, BiConsumer<String, String> cb) throws IOException {
        String cursor = "0";
        do {
            Resp r = cmd("HSCAN", key, cursor, "COUNT", "1000");
            if (r.arr == null || r.arr.size() != 2) return;
            cursor = r.arr.get(0).text();
            List<Resp> items = r.arr.get(1).arr;
            for (int i = 0; i + 1 < items.size(); i += 2)
                cb.accept(items.get(i).text(), items.get(i + 1).text());
        } while (!"0".equals(cursor));
    }

    public void incrbyMany(List<String[]> deltas) throws IOException {
        if (deltas.isEmpty()) return;
        List<String[]> cmds = new ArrayList<>(deltas.size());
        for (String[] d : deltas)
            cmds.add(new String[]{"INCRBY", d[0], d[1]});
        pipe(cmds);
    }

    public void scanEach(String pat, java.util.function.Consumer<String> cb) throws IOException {
        String cursor = "0";
        for (;;) {
            Resp r = cmd("SCAN", cursor, "MATCH", pat, "COUNT", "500");
            if (r.kind != '*' || r.arr == null || r.arr.size() != 2) return;
            cursor = r.arr.get(0).text();
            if (r.arr.get(1).arr != null)
                for (Resp k : r.arr.get(1).arr) cb.accept(k.text());
            if ("0".equals(cursor)) return;
        }
    }

    public void flushdb() throws IOException { cmd("FLUSHDB"); }
}
