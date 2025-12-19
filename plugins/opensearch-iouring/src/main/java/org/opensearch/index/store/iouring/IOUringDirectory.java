/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import org.apache.lucene.store.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

/**
 * A Lucene Directory implementation using IO-uring for read operations.
 *
 * <p>This directory provides high-performance asynchronous I/O for index reads
 * using Linux's IO-uring subsystem. Write operations delegate to the standard
 * NIOFSDirectory implementation.</p>
 *
 * <p>Configuration:</p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "settings": {
 *     "index.store.type": "io_uring"
 *   }
 * }
 * }</pre>
 */
public class IOUringDirectory extends FSDirectory {

    private static final Logger logger = LogManager.getLogger(IOUringDirectory.class);

    // IO-uring components
    private final IOUringRing ring;
    private final IOUringCompletionHandler completionHandler;

    // Fallback for write operations
    private final NIOFSDirectory fallbackDirectory;

    // Configuration
    private static final int DEFAULT_QUEUE_DEPTH = 256;

    // Whether IO-uring is enabled
    private final boolean ioUringEnabled;

    /**
     * Create a new IOUringDirectory with default settings.
     */
    public IOUringDirectory(Path path) throws IOException {
        this(path, FSLockFactory.getDefault(), DEFAULT_QUEUE_DEPTH);
    }

    /**
     * Create a new IOUringDirectory with custom lock factory.
     */
    public IOUringDirectory(Path path, LockFactory lockFactory) throws IOException {
        this(path, lockFactory, DEFAULT_QUEUE_DEPTH);
    }

    /**
     * Create a new IOUringDirectory with custom queue depth.
     */
    public IOUringDirectory(Path path, LockFactory lockFactory, int queueDepth) throws IOException {
        super(path, lockFactory);

        // Create fallback directory
        this.fallbackDirectory = new NIOFSDirectory(path, lockFactory);

        // Try to initialize IO-uring
        IOUringRing tempRing = null;
        IOUringCompletionHandler tempHandler = null;
        boolean enabled = false;

        if (IOUringRing.isAvailable()) {
            try {
                tempRing = new IOUringRing(queueDepth);
                tempHandler = new IOUringCompletionHandler(tempRing);
                enabled = true;
                logger.info("IOUringDirectory initialized: path={}, queueDepth={}",
                    path, queueDepth);
            } catch (Exception e) {
                logger.warn("Failed to initialize IO-uring, falling back to NIO: {}",
                    e.getMessage());
                if (tempRing != null) {
                    try { tempRing.close(); } catch (Exception ignored) {}
                }
            }
        } else {
            logger.info("IO-uring not available ({}), using NIO fallback",
                IOUringRing.getUnavailabilityReason());
        }

        this.ring = tempRing;
        this.completionHandler = tempHandler;
        this.ioUringEnabled = enabled;
    }

    /**
     * Check if IO-uring is enabled for this directory.
     */
    public boolean isIOUringEnabled() {
        return ioUringEnabled;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();

        if (!ioUringEnabled) {
            return fallbackDirectory.openInput(name, context);
        }

        Path filePath = getDirectory().resolve(name);

        try {
            return new IOUringIndexInput(
                "IOUringIndexInput(" + name + ")",
                filePath,
                ring,
                completionHandler
            );
        } catch (Exception e) {
            logger.warn("Failed to open {} with IO-uring, falling back to NIO: {}",
                name, e.getMessage());
            return fallbackDirectory.openInput(name, context);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        // Delegate writes to fallback (IO-uring writes are future work)
        return fallbackDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
        throws IOException {
        ensureOpen();
        return fallbackDirectory.createTempOutput(prefix, suffix, context);
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return fallbackDirectory.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        fallbackDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        return fallbackDirectory.fileLength(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        fallbackDirectory.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        fallbackDirectory.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        fallbackDirectory.rename(source, dest);
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return fallbackDirectory.getPendingDeletions();
    }

    @Override
    public synchronized void close() throws IOException {
        if (completionHandler != null) {
            completionHandler.close();
        }

        if (ring != null) {
            ring.close();
        }

        fallbackDirectory.close();

        logger.info("IOUringDirectory closed: {}", getDirectory());

        super.close();
    }

    /**
     * Get statistics about the IO-uring completion handler.
     */
    public String getStats() {
        if (!ioUringEnabled || completionHandler == null) {
            return "IO-uring disabled";
        }

        return String.format(
            "completed=%d, errors=%d, pending=%d, queueDepth=%d",
            completionHandler.getCompletedCount(),
            completionHandler.getErrorCount(),
            completionHandler.getPendingCount(),
            ring.getQueueDepth()
        );
    }
}
