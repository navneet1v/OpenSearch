package org.opensearch.index.store.iouring;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.iouring.api.IoUringFileChannel;

import java.io.IOException;
import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Lucene Directory backed by io_uring for read operations.
 *
 * Write operations are delegated to FSDirectory.
 */
public final class IOUringDirectory extends FSDirectory {
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
    }

    /* ============================
     * Core Lucene Override
     * ============================ */

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {

        ensureOpen();
        return new IOUringIndexInput(
                "IOUringIndexInput(path=\"" + getDirectory().resolve(name) + "\")",
            IoUringFileChannel.open(getDirectory().resolve(name), StandardOpenOption.READ),
            context
        );
    }
}
