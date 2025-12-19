/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.*;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An IndexInput implementation using IO-uring for asynchronous reads.
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Asynchronous I/O via IO-uring submission/completion queues</li>
 *   <li>Zero-copy buffer access using Panama MemorySegments</li>
 *   <li>Support for file descriptor registration (optional optimization)</li>
 *   <li>Proper slice support for Lucene's file access patterns</li>
 * </ul>
 */
public class IOUringIndexInput extends IndexInput implements RandomAccessInput {

    private static final Logger logger = LogManager.getLogger(IOUringIndexInput.class);

    // Shared IO-uring resources
    private final IOUringRing ring;
    private final IOUringCompletionHandler completionHandler;

    // File state
    private final int fd;
    private final long fileLength;
    private final Path filePath;

    // Current position
    private long position;

    // Native I/O buffer
    private final MemorySegment ioBuffer;
    private final long ioBufferSize;
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB

    // Slice support
    private final boolean isSlice;
    private final long sliceOffset;
    private final IOUringIndexInput parent;

    // I/O timeout
    private static final long IO_TIMEOUT_SECONDS = 30;

    /**
     * Create a new IOUringIndexInput.
     *
     * @param resourceDescription Description for debugging
     * @param path File path
     * @param ring IO-uring ring
     * @param completionHandler Completion handler
     */
    public IOUringIndexInput(String resourceDescription,
                             Path path,
                             IOUringRing ring,
                             IOUringCompletionHandler completionHandler) throws IOException {
        super(resourceDescription);

        this.filePath = path;
        this.ring = ring;
        this.completionHandler = completionHandler;
        this.isSlice = false;
        this.sliceOffset = 0;
        this.parent = null;

        // Open file
        this.fd = NativeFileUtils.open(path.toString());
        this.fileLength = NativeFileUtils.fileSize(fd);
        this.position = 0;

        // Allocate I/O buffer
        this.ioBufferSize = DEFAULT_BUFFER_SIZE;
        this.ioBuffer = ring.allocateAligned(ioBufferSize);

        logger.debug("Opened IOUringIndexInput: {} (length={})", path, fileLength);
    }

    /**
     * Slice constructor (internal).
     */
    private IOUringIndexInput(IOUringIndexInput parent,
                              String sliceDescription,
                              long offset,
                              long length) {
        super(sliceDescription);

        this.parent = parent;
        this.filePath = parent.filePath;
        this.ring = parent.ring;
        this.completionHandler = parent.completionHandler;
        this.fd = parent.fd;
        this.fileLength = length;
        this.position = 0;
        this.isSlice = true;
        this.sliceOffset = parent.sliceOffset + offset;

        // Allocate own buffer
        this.ioBufferSize = DEFAULT_BUFFER_SIZE;
        this.ioBuffer = ring.allocateAligned(ioBufferSize);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0) return;

        // Bounds check
        if (position + len > fileLength) {
            throw new EOFException(String.format(
                "read past EOF: pos=%d len=%d fileLength=%d in %s",
                position, len, fileLength, toString()));
        }

        int totalRead = 0;

        while (totalRead < len) {
            int toRead = (int) Math.min(len - totalRead, ioBufferSize);

            // Calculate actual file offset
            long fileOffset = sliceOffset + position;

            // Generate request ID and register
            long requestId = completionHandler.nextRequestId();
            CompletableFuture<Integer> future = completionHandler.registerRequest(requestId);

            // Submit read
            int submitResult = ring.submitRead(fd, ioBuffer, toRead, fileOffset, requestId);

            if (submitResult == -11) { // EAGAIN - queue full
                completionHandler.cancelRequest(requestId);
                ring.submit(); // Flush pending

                // Retry
                requestId = completionHandler.nextRequestId();
                future = completionHandler.registerRequest(requestId);
                submitResult = ring.submitRead(fd, ioBuffer, toRead, fileOffset, requestId);
            }

            if (submitResult != 0) {
                completionHandler.cancelRequest(requestId);
                throw new IOException("Failed to submit read: error " + submitResult);
            }

            // Submit to kernel
            int submitted = ring.submit();
            if (submitted < 0) {
                completionHandler.cancelRequest(requestId);
                throw new IOException("Failed to submit to kernel: error " + submitted);
            }

            // Wait for completion
            int bytesRead;
            try {
                bytesRead = future.get(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                completionHandler.cancelRequest(requestId);
                throw new IOException("Read timed out after " + IO_TIMEOUT_SECONDS + "s", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                completionHandler.cancelRequest(requestId);
                throw new IOException("Read interrupted", e);
            } catch (ExecutionException e) {
                throw new IOException("Read failed", e.getCause());
            }

            if (bytesRead <= 0) {
                throw new EOFException("Unexpected end of file");
            }

            // Copy from native buffer to Java array
            MemorySegment.copy(ioBuffer, ValueLayout.JAVA_BYTE, 0,
                b, offset + totalRead, bytesRead);

            totalRead += bytesRead;
            position += bytesRead;
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (position >= fileLength) {
            throw new EOFException("read past EOF");
        }

        byte[] b = new byte[1];
        readBytes(b, 0, 1);
        return b[0];
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > fileLength) {
            throw new IllegalArgumentException(
                "seek position " + pos + " out of bounds [0, " + fileLength + "]");
        }
        this.position = pos;
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public long length() {
        return fileLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > fileLength) {
            throw new IllegalArgumentException(String.format(
                "slice(offset=%d, length=%d) out of bounds (fileLength=%d)",
                offset, length, fileLength));
        }

        return new IOUringIndexInput(this, sliceDescription, offset, length);
    }

    @Override
    public void close() throws IOException {
        // Free I/O buffer
        if (ioBuffer != null) {
            ring.freeAligned(ioBuffer);
        }

        // Only close fd if not a slice
        if (!isSlice && fd >= 0) {
            NativeFileUtils.close(fd);
            logger.debug("Closed IOUringIndexInput: {}", filePath);
        }
    }

    // =========================================================================
    // RandomAccessInput implementation
    // =========================================================================

    @Override
    public byte readByte(long pos) throws IOException {
        long savedPos = position;
        try {
            seek(pos);
            return readByte();
        } finally {
            position = savedPos;
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        byte[] b = new byte[2];
        long savedPos = position;
        try {
            seek(pos);
            readBytes(b, 0, 2);
        } finally {
            position = savedPos;
        }
        return (short) (((b[0] & 0xFF) << 8) | (b[1] & 0xFF));
    }

    @Override
    public int readInt(long pos) throws IOException {
        byte[] b = new byte[4];
        long savedPos = position;
        try {
            seek(pos);
            readBytes(b, 0, 4);
        } finally {
            position = savedPos;
        }
        return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) |
            ((b[2] & 0xFF) << 8)  | (b[3] & 0xFF);
    }

    @Override
    public long readLong(long pos) throws IOException {
        byte[] b = new byte[8];
        long savedPos = position;
        try {
            seek(pos);
            readBytes(b, 0, 8);
        } finally {
            position = savedPos;
        }
        return ((long)(b[0] & 0xFF) << 56) | ((long)(b[1] & 0xFF) << 48) |
            ((long)(b[2] & 0xFF) << 40) | ((long)(b[3] & 0xFF) << 32) |
            ((long)(b[4] & 0xFF) << 24) | ((long)(b[5] & 0xFF) << 16) |
            ((long)(b[6] & 0xFF) << 8)  | ((long)(b[7] & 0xFF));
    }

    @Override
    public String toString() {
        return "IOUringIndexInput(" + filePath +
            (isSlice ? " [slice offset=" + sliceOffset + " len=" + fileLength + "]" : "") + ")";
    }
}
