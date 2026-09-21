package shrt;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.function.Consumer;

/** Append-only log: buffered appends, explicit flush/fsync, replay + tails. */
public final class Aof {
    private static final int BUF_CAP = 64 << 10;

    private final RandomAccessFile raf;
    private final FileChannel ch;
    private byte[] buf = new byte[BUF_CAP];
    private int len;
    private long pending;

    public Aof(Path path) throws IOException {
        raf = new RandomAccessFile(path.toFile(), "rw");
        ch = raf.getChannel();
        ch.position(ch.size()); // append mode
    }

    /** Push pre-encoded bytes — the cheap lock-held path. */
    public synchronized void pushBytes(byte[] b) {
        if (b.length == 0) return;
        ensure(b.length);
        System.arraycopy(b, 0, buf, len, b.length);
        len += b.length;
        pending += b.length;
    }

    public synchronized void pushRaw(String s) {
        if (s.isEmpty()) return;
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ensure(b.length);
        System.arraycopy(b, 0, buf, len, b.length);
        len += b.length;
        pending += b.length;
    }

    public synchronized void push(String line) {
        pushRaw(line + "\n");
    }

    private void ensure(int add) {
        if (len + add <= buf.length) return;
        int cap = buf.length;
        while (cap < len + add) cap <<= 1;
        byte[] nb = new byte[cap];
        System.arraycopy(buf, 0, nb, 0, len);
        buf = nb;
    }

    public synchronized long pendingBytes() { return pending; }

    public synchronized void flush() throws IOException {
        if (pending == 0) return;
        ByteBuffer w = ByteBuffer.wrap(buf, 0, len);
        while (w.hasRemaining()) ch.write(w); // write() may short-write
        len = 0;
        pending = 0;
    }

    /** Flush pending under the lock, then force() WITHOUT it — fsync can take
     *  hundreds of ms and must not stall writers. */
    public void fsync() throws IOException {
        synchronized (this) { flush(); }
        ch.force(false);
    }

    public synchronized void truncate() throws IOException {
        flush();
        ch.truncate(0);
        ch.position(0);
    }

    public synchronized void close() {
        try { flush(); ch.close(); raf.close(); } catch (IOException ignored) {}
    }

    /** Replay whole file, applying each line. */
    public static void replay(Path p, Consumer<String> apply) throws IOException {
        if (!Files.exists(p)) return;
        try (var lines = Files.lines(p, StandardCharsets.UTF_8)) {
            lines.forEach(l -> { if (!l.isEmpty()) apply.accept(l); });
        }
    }
}

/** Incremental reader that picks up appended bytes (sibling logs). */
final class TailReader {
    private final Path path;
    private FileChannel ch;
    private final ByteBuffer carry = ByteBuffer.allocate(1 << 20);
    private int carryLen = 0;

    TailReader(Path path) throws IOException {
        this.path = path;
        if (Files.exists(path)) {
            ch = FileChannel.open(path, StandardOpenOption.READ);
        }
    }

    void readNew(Consumer<String> apply) throws IOException {
        if (ch == null) {
            if (!Files.exists(path)) return;
            ch = FileChannel.open(path, StandardOpenOption.READ);
        }
        ByteBuffer tmp = ByteBuffer.allocate(64 << 10);
        for (;;) {
            tmp.clear();
            int n = ch.read(tmp);
            if (n <= 0) break;
            // append to carry
            byte[] b = tmp.array();
            if (carryLen + n > carry.capacity()) {
                // line too long: drop carry (corrupt line protection)
                carryLen = 0;
            }
            carry.put(b, 0, n);
            carryLen += n;
        }
        // drain complete lines
        carry.flip();
        carry.limit(carryLen);
        int start = 0;
        for (int i = 0; i < carryLen; i++) {
            if (carry.get(i) == '\n') {
                String line = StandardCharsets.UTF_8.decode(
                    ByteBuffer.wrap(carry.array(), start, i - start)).toString();
                if (!line.isEmpty() && line.charAt(line.length() - 1) == '\r')
                    line = line.substring(0, line.length() - 1);
                if (!line.isEmpty()) apply.accept(line);
                start = i + 1;
            }
        }
        int rem = carryLen - start;
        System.arraycopy(carry.array(), start, carry.array(), 0, rem);
        carryLen = rem;
        carry.clear();
    }

    void close() { try { if (ch != null) ch.close(); } catch (IOException ignored) {} }
}

/** Instance claiming: holds a FileLock on instance-<i>.lock while alive. */
final class InstanceLock {
    final int instance;
    private final FileChannel ch;
    private final FileLock lock;
    final Path path;

    private InstanceLock(int inst, FileChannel ch, FileLock lock, Path p) {
        this.instance = inst; this.ch = ch; this.lock = lock; this.path = p;
    }

    /** Try to claim instance i; returns null if another live process holds it. */
    static InstanceLock tryClaim(Path dir, int i) throws IOException {
        Path p = dir.resolve("instance-" + i + ".lock");
        FileChannel ch = FileChannel.open(p,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock l = ch.tryLock();
        if (l == null) { ch.close(); return null; }
        ch.write(ByteBuffer.wrap(String.valueOf(ProcessHandle.current().pid())
            .getBytes(StandardCharsets.UTF_8)));
        return new InstanceLock(i, ch, l, p);
    }

    /** Find and hold the first free instance id (0..1023). */
    static InstanceLock claim(Path dir, int want) throws IOException {
        if (want >= 0) {
            InstanceLock l = tryClaim(dir, want);
            if (l != null) return l;
            throw new IOException("instance " + want + " already claimed");
        }
        for (int i = 0; i < 1024; i++) {
            InstanceLock l = tryClaim(dir, i);
            if (l != null) return l;
        }
        throw new IOException("no free instance slots");
    }

    void release() {
        try { lock.release(); ch.close(); Files.deleteIfExists(path); }
        catch (IOException ignored) {}
    }
}
