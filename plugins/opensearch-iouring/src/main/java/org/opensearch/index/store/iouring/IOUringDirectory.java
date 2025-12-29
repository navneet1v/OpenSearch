package org.opensearch.index.store.iouring;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lucene Directory backed by io_uring for read operations.
 *
 * Write operations are delegated to FSDirectory.
 */
public final class IOUringDirectory extends FSDirectory {

    private static final int DEFAULT_QUEUE_DEPTH = 1024;

    private final IOUringScheduler scheduler;

    /**
     * Track open file descriptors per file name.
     * Lucene IndexInput instances are thread-confined,
     * but Directory is not.
     */
    private final Map<String, FileHandle> openFiles =
            new ConcurrentHashMap<>();

    /* ============================
     * Constructors
     * ============================ */

    public IOUringDirectory(Path path) throws IOException {
        this(path, FSLockFactory.getDefault());
    }

    public IOUringDirectory(Path path, LockFactory lockFactory)
            throws IOException {
        super(path, lockFactory);

        if (!Files.isDirectory(path)) {
            throw new IOException("Path is not a directory: " + path);
        }

        this.scheduler = new IOUringScheduler(DEFAULT_QUEUE_DEPTH);
    }

    /* ============================
     * Core Lucene Override
     * ============================ */

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {

        ensureOpen();
        FileHandle handle = openFiles.computeIfAbsent(name, n -> {
            try {
                return FileHandle.open(getDirectory().resolve(n));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        return new IOUringIndexInput(
                "IOUringIndexInput(path=\"" + getDirectory().resolve(name) + "\")",
                scheduler,
                handle.fd(),
                handle.length()
        );
    }

    @Override
    public void close() throws IOException {
        // Close scheduler first so no new IO can be submitted
        scheduler.close();

        // Close all file descriptors
        IOUtils.close(openFiles.values());
        openFiles.clear();

        super.close();
    }


    /* ============================
     * Internal File Handle
     * ============================ */

    private static final class FileHandle implements Closeable {

        private final int fd;
        private final long length;

        private FileHandle(int fd, long length) {
            this.fd = fd;
            this.length = length;
        }

        static FileHandle open(Path path) throws IOException {
            int fd = PosixFD.openReadOnly(path);
            long length = Files.size(path);
            return new FileHandle(fd, length);
        }

        int fd() {
            return fd;
        }

        long length() {
            return length;
        }

        @Override
        public void close() throws IOException {
            PosixFD.close(fd);
        }
    }
}
