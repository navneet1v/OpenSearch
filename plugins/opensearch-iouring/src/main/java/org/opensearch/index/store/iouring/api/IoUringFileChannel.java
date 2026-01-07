/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.api;


import org.opensearch.index.store.iouring.PosixFD;
import org.opensearch.index.store.iouring.core.BufferPool;
import org.opensearch.index.store.iouring.core.IoUringRing;
import org.opensearch.index.store.iouring.native_.SqeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * FileChannel implementation backed by io_uring.
 *
 * <p>Drop-in replacement for {@link java.nio.channels.FileChannel} that uses
 * io_uring for read, write, fsync, and close operations.
 *
 * <p>Thread Safety:
 * <ul>
 *   <li>Position-based operations ({@code read(buf)}, {@code write(buf)},
 *       {@code position()}) are synchronized on an internal position lock</li>
 *   <li>Positioned operations ({@code read(buf, pos)}, {@code write(buf, pos)})
 *       can run concurrently</li>
 *   <li>{@code close()} waits for in-flight operations via thread counting</li>
 * </ul>
 *
 * <p>Buffer Handling:
 * <ul>
 *   <li>Direct buffers are used directly with io_uring</li>
 *   <li>Heap buffers are transparently copied to/from pooled direct buffers</li>
 * </ul>
 *
 * <p>Unsupported Operations (fall back to standard FileChannel):
 * <ul>
 *   <li>{@link #map(MapMode, long, long)}</li>
 *   <li>{@link #lock(long, long, boolean)} / {@link #tryLock(long, long, boolean)}</li>
 *   <li>{@link #truncate(long)}</li>
 * </ul>
 */
public class IoUringFileChannel extends FileChannel {

    // ═══════════════════════════════════════════════════════════════════════════
    // Constants
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Buffer size for transfer operations.
     */
    private static final int TRANSFER_BUFFER_SIZE = 65536;

    // ═══════════════════════════════════════════════════════════════════════════
    // Instance State
    // ═══════════════════════════════════════════════════════════════════════════

    private final int fd;
    private final Path path;
    private final boolean readable;
    private final boolean writable;
    private final Set<OpenOption> openOptions;

    private final IoUringRing ring;
    private final BufferPool bufferPool;

    // Position management
    private final Object positionLock = new Object();
    private long position;

    // Close coordination
    private final Object closeLock = new Object();
    private volatile boolean closed;
    private int activeOperations;

    // Fallback channel for unsupported operations
    private volatile FileChannel fallbackChannel;
    private final Object fallbackLock = new Object();


    /**
     * Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum
     * lookup. There are two reasons for using this instead of directly referencing
     * ExtendedOpenOption.DIRECT:
     *
     * <ol>
     *   <li>ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes
     *       un-suppressible(?) warning to be emitted when compiling with --release flag and value N,
     *       where N is smaller than the version of javac used for compilation. For details, please
     *       refer to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8259039.
     *   <li>It is possible that Lucene is run using JDK that does not support
     *       ExtendedOpenOption.DIRECT. In such a case, dynamic lookup allows us to bail out with
     *       UnsupportedOperationException with meaningful error message.
     * </ol>
     *
     * <p>This reference is {@code null}, if the JDK does not support direct I/O.
     */
    static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz =
                Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option =
                Arrays.stream(clazz.getEnumConstants())
                    .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
                    .findFirst()
                    .orElse(null);
        } catch (
            @SuppressWarnings("unused")
            Exception e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Private constructor - use static open methods.
     */
    private IoUringFileChannel(int fd, Path path, boolean readable, boolean writable,
                               Set<OpenOption> openOptions) {
        this.fd = fd;
        this.path = path;
        this.readable = readable;
        this.writable = writable;
        this.openOptions = openOptions;
        this.ring = IoUringRing.getInstance();
        this.bufferPool = BufferPool.getInstance();
        this.position = 0;
        this.closed = false;
        this.activeOperations = 0;
    }

    /**
     * Opens a file channel using io_uring.
     *
     * @param path    Path to the file
     * @param options Open options
     * @return IoUringFileChannel instance
     * @throws IOException if file cannot be opened
     */
    public static IoUringFileChannel open(Path path, OpenOption... options) throws IOException {
        // currently doing forceful directIO
        Set<OpenOption> optionSet = toSet(options);
        optionSet.add(getDirectOpenOption());
        return open(path, optionSet, new FileAttribute<?>[0]);
    }

    /**
     * Opens a file channel using io_uring.
     *
     * @param path    Path to the file
     * @param options Set of open options
     * @return IoUringFileChannel instance
     * @throws IOException if file cannot be opened
     */
    public static IoUringFileChannel open(Path path, Set<? extends OpenOption> options)
        throws IOException {
        return open(path, options, new FileAttribute<?>[0]);
    }

    /**
     * Opens a file channel using io_uring with file attributes.
     *
     * @param path    Path to the file
     * @param options Set of open options
     * @param attrs   File attributes for creation (ignored, use file system defaults)
     * @return IoUringFileChannel instance
     * @throws IOException if file cannot be opened
     */
    public static IoUringFileChannel open(Path path, Set<? extends OpenOption> options,
                                          FileAttribute<?>... attrs) throws IOException {
        Set<OpenOption> optionSet = new HashSet<>(options);
        int fd = PosixFD.open(path, optionSet);
        IoUringFileChannel channel = new IoUringFileChannel(fd, path, optionSet.contains(StandardOpenOption.READ), optionSet.contains(StandardOpenOption.WRITE), optionSet);
        channel.fallbackChannel = FileChannel.open(path, optionSet, attrs);
        return channel;
    }

    /**
     * Converts varargs to Set.
     */
    private static Set<OpenOption> toSet(OpenOption[] options) {
        Set<OpenOption> set = new HashSet<>();
        for (OpenOption opt : options) {
            set.add(opt);
        }
        return set;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Operation Guards
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Checks that the channel is open.
     */
    private void ensureOpen() throws ClosedChannelException {
        if (closed) {
            throw new ClosedChannelException();
        }
    }

    /**
     * Checks that the channel is readable.
     */
    private void ensureReadable() throws NonReadableChannelException {
        if (!readable) {
            throw new NonReadableChannelException();
        }
    }

    /**
     * Checks that the channel is writable.
     */
    private void ensureWritable() throws NonWritableChannelException {
        if (!writable) {
            throw new NonWritableChannelException();
        }
    }

    /**
     * Begins an operation - increments active count.
     * Must be paired with endOperation().
     */
    private void beginOperation() throws ClosedChannelException {
        synchronized (closeLock) {
            ensureOpen();
            activeOperations++;
        }
    }

    /**
     * Ends an operation - decrements active count and notifies waiters.
     */
    private void endOperation() {
        synchronized (closeLock) {
            activeOperations--;
            if (activeOperations == 0) {
                closeLock.notifyAll();
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Read Operations
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ensureOpen();
        ensureReadable();

        synchronized (positionLock) {
            int n = readInternal(dst, position);
            if (n > 0) {
                position += n;
            }
            return n;
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position);
        }
        ensureOpen();
        ensureReadable();

        return readInternal(dst, position);
    }

    /**
     * Internal read implementation.
     */
    private int readInternal(ByteBuffer dst, long offset) throws IOException {
        int remaining = dst.remaining();
        if (remaining == 0) {
            return 0;
        }

        beginOperation();
        try {
            if (SqeUtils.canUseDirectly(dst)) {
                return readDirect(dst, offset);
            } else {
                return readHeap(dst, offset);
            }
        } finally {
            endOperation();
        }
    }

    /**
     * Reads into a direct buffer.
     */
    private int readDirect(ByteBuffer dst, long offset) throws IOException {
        int position = dst.position();
        int remaining = dst.remaining();
        long bufferAddr = SqeUtils.getBufferAddress(dst);

        CompletableFuture<Integer> future = ring.submit(sqe -> {
            SqeUtils.prepareRead(sqe, fd, bufferAddr, remaining, offset);
        });

        try {
            int bytesRead = future.join();

            if (bytesRead > 0) {
                dst.position(position + bytesRead);
            }

            // Convert 0 to -1 for EOF
            return bytesRead == 0 ? -1 : bytesRead;

        } catch (CompletionException e) {
            throw unwrapException(e);
        }
    }

    /**
     * Reads into a heap buffer using a pooled direct buffer.
     */
    private int readHeap(ByteBuffer dst, long offset) throws IOException {
        int remaining = dst.remaining();
        ByteBuffer direct = bufferPool.acquire(remaining);

        try {
            direct.clear();
            direct.limit(remaining);
            long bufferAddr = SqeUtils.getBufferAddress(direct);

            CompletableFuture<Integer> future = ring.submit(
                sqe -> SqeUtils.prepareRead(sqe, fd, bufferAddr, remaining, offset)
            );

            int bytesRead;
            try {
                bytesRead = future.join();
            } catch (CompletionException e) {
                throw unwrapException(e);
            }

            if (bytesRead > 0) {
                direct.flip();
                direct.limit(bytesRead);
                dst.put(direct);
            }

            return bytesRead == 0 ? -1 : bytesRead;

        } finally {
            bufferPool.release(direct);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > dsts.length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        ensureReadable();

        synchronized (positionLock) {
            long totalRead = 0;

            for (int i = offset; i < offset + length; i++) {
                if (!dsts[i].hasRemaining()) {
                    continue;
                }

                int n = readInternal(dsts[i], position + totalRead);
                if (n < 0) {
                    // EOF
                    if (totalRead == 0) {
                        return -1;
                    }
                    break;
                }

                totalRead += n;

                // Short read - don't continue
                if (dsts[i].hasRemaining()) {
                    break;
                }
            }

            if (totalRead > 0) {
                position += totalRead;
            }

            return totalRead;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Write Operations
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public int write(ByteBuffer src) throws IOException {
        ensureOpen();
        ensureWritable();

        synchronized (positionLock) {
            int n = writeInternal(src, position);
            if (n > 0) {
                position += n;
            }
            return n;
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position);
        }
        ensureOpen();
        ensureWritable();

        return writeInternal(src, position);
    }

    /**
     * Internal write implementation.
     */
    private int writeInternal(ByteBuffer src, long offset) throws IOException {
        int remaining = src.remaining();
        if (remaining == 0) {
            return 0;
        }

        beginOperation();
        try {
            if (SqeUtils.canUseDirectly(src)) {
                return writeDirect(src, offset);
            } else {
                return writeHeap(src, offset);
            }
        } finally {
            endOperation();
        }
    }

    /**
     * Writes from a direct buffer.
     */
    private int writeDirect(ByteBuffer src, long offset) throws IOException {
        int position = src.position();
        int remaining = src.remaining();
        long bufferAddr = SqeUtils.getBufferAddress(src);

        CompletableFuture<Integer> future = ring.submit(sqe -> {
            SqeUtils.prepareWrite(sqe, fd, bufferAddr, remaining, offset);
        });

        try {
            int bytesWritten = future.join();

            if (bytesWritten > 0) {
                src.position(position + bytesWritten);
            }

            return bytesWritten;

        } catch (CompletionException e) {
            throw unwrapException(e);
        }
    }

    /**
     * Writes from a heap buffer using a pooled direct buffer.
     */
    private int writeHeap(ByteBuffer src, long offset) throws IOException {
        int remaining = src.remaining();
        int srcPosition = src.position();
        ByteBuffer direct = bufferPool.acquire(remaining);

        try {
            // Copy data to direct buffer
            direct.clear();
            direct.put(src);
            direct.flip();

            // Reset src position in case of failure
            src.position(srcPosition);

            long bufferAddr = SqeUtils.getBufferAddress(direct);

            CompletableFuture<Integer> future = ring.submit(
                sqe -> SqeUtils.prepareWrite(sqe, fd, bufferAddr, remaining, offset)
            );

            int bytesWritten;
            try {
                bytesWritten = future.join();
            } catch (CompletionException e) {
                throw unwrapException(e);
            }

            // Update src position on success
            if (bytesWritten > 0) {
                src.position(srcPosition + bytesWritten);
            }

            return bytesWritten;

        } catch (Throwable t) {
            bufferPool.release(direct);
            throw t;
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > srcs.length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        ensureWritable();

        synchronized (positionLock) {
            long totalWritten = 0;

            for (int i = offset; i < offset + length; i++) {
                if (!srcs[i].hasRemaining()) {
                    continue;
                }

                int n = writeInternal(srcs[i], position + totalWritten);
                if (n <= 0) {
                    break;
                }

                totalWritten += n;

                // Short write - don't continue
                if (srcs[i].hasRemaining()) {
                    break;
                }
            }

            if (totalWritten > 0) {
                position += totalWritten;
            }

            return totalWritten;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Position
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public long position() throws IOException {
        ensureOpen();
        synchronized (positionLock) {
            return position;
        }
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + newPosition);
        }
        ensureOpen();
        synchronized (positionLock) {
            position = newPosition;
        }
        return this;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Size
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public long size() throws IOException {
        ensureOpen();
        return fallbackChannel.size();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Force (fsync)
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public void force(boolean metaData) throws IOException {
        ensureOpen();
        fallbackChannel.force(metaData);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Transfer Operations
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
        throws IOException {
        if (position < 0 || count < 0) {
            throw new IllegalArgumentException("Position and count must be non-negative");
        }
        ensureOpen();
        ensureReadable();

        ByteBuffer buffer = bufferPool.acquire(TRANSFER_BUFFER_SIZE);
        try {
            long transferred = 0;

            while (transferred < count) {
                buffer.clear();
                int toRead = (int) Math.min(buffer.capacity(), count - transferred);
                buffer.limit(toRead);

                int n = readInternal(buffer, position + transferred);
                if (n <= 0) {
                    break;
                }

                buffer.flip();
                while (buffer.hasRemaining()) {
                    int written = target.write(buffer);
                    if (written <= 0) {
                        break;
                    }
                }

                transferred += n;
            }

            return transferred;

        } finally {
            bufferPool.release(buffer);
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
        throws IOException {
        if (position < 0 || count < 0) {
            throw new IllegalArgumentException("Position and count must be non-negative");
        }
        ensureOpen();
        ensureWritable();

        ByteBuffer buffer = bufferPool.acquire(TRANSFER_BUFFER_SIZE);
        try {
            long transferred = 0;

            while (transferred < count) {
                buffer.clear();
                int toRead = (int) Math.min(buffer.capacity(), count - transferred);
                buffer.limit(toRead);

                int n = src.read(buffer);
                if (n <= 0) {
                    break;
                }

                buffer.flip();
                int written = writeInternal(buffer, position + transferred);
                transferred += written;

                if (written < n) {
                    // Short write
                    break;
                }
            }

            return transferred;

        } finally {
            bufferPool.release(buffer);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Fallback Operations
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Gets or creates the fallback channel for unsupported operations.
     */
    private FileChannel getFallbackChannel() throws IOException {
        FileChannel fc = fallbackChannel;
        if (fc != null && fc.isOpen()) {
            return fc;
        }

        synchronized (fallbackLock) {
            fc = fallbackChannel;
            if (fc != null && fc.isOpen()) {
                return fc;
            }

            fc = FileChannel.open(path, openOptions.toArray(new OpenOption[0]));

            // Sync position
            synchronized (positionLock) {
                fc.position(position);
            }

            fallbackChannel = fc;
            return fc;
        }
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        ensureOpen();
        return getFallbackChannel().map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        ensureOpen();
        return getFallbackChannel().lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        ensureOpen();
        return getFallbackChannel().tryLock(position, size, shared);
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative: " + size);
        }
        ensureOpen();
        ensureWritable();

        getFallbackChannel().truncate(size);

        // Adjust position if beyond new size
        synchronized (positionLock) {
            if (position > size) {
                position = size;
            }
        }

        return this;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Close
    // ═══════════════════════════════════════════════════════════════════════════


    @Override
    protected void implCloseChannel() throws IOException {
        if (closed) {
            return;
        }

        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;

            while (activeOperations > 0) {
                try {
                    closeLock.wait(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Close fallback channel if created (it has its own fd)
        FileChannel fc = fallbackChannel;
        if (fc != null) {
            try {
                fc.close();
            } catch (IOException ignored) {
            }
        }

        // Close our fd
        PosixFD.close(fd);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Utility Methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Unwraps a CompletionException to get the underlying IOException.
     */
    private IOException unwrapException(CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException ioe) {
            return ioe;
        }
        return new IOException("Operation failed", cause);
    }

    /**
     * Returns the path this channel was opened on.
     *
     * @return Path
     */
    public Path getPath() {
        return path;
    }

    /**
     * Returns the underlying file descriptor.
     * For advanced use only.
     *
     * @return File descriptor
     */
    public int getFd() {
        return fd;
    }

    private static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
        }
        return ExtendedOpenOption_DIRECT;
    }
}
