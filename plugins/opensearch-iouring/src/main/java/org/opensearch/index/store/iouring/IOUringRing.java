/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import org.opensearch.index.store.iouring.ffi.opensearch_iouring_h;
import org.opensearch.index.store.iouring.ffi.osur_completion_t;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.*;
    import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * High-level Java wrapper around the native IO-uring ring.
 *
 * This class provides a clean, safe Java API for IO-uring operations,
 * managing native memory through Panama's Arena API.
 *
 * <p>Thread Safety: Each ring instance should be used from a single thread,
 * or external synchronization must be provided.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (IOUringRing ring = new IOUringRing(256)) {
 *     long requestId = ring.submitRead(fd, buffer, length, offset, userData);
 *     ring.submit();
 *     CompletionResult result = ring.waitCompletion();
 * }
 * }</pre>
 */
public class IOUringRing implements Closeable {

    private static final Logger logger = LogManager.getLogger(IOUringRing.class);

    // Ring flags (mirror native constants)
    public static final int FLAG_DEFAULT = 0;
    public static final int FLAG_SQPOLL = 1;
    public static final int FLAG_IOPOLL = 2;
    public static final int FLAG_SINGLE_ISSUER = 4;

    // Native ring handle (pointer)
    private final MemorySegment ringHandle;

    // Arena for memory management
    private final Arena arena;

    // Preallocated completion structure for polling
    private final MemorySegment completionSegment;

    // Configuration
    private final int queueDepth;
    private final int flags;

    // State
    private volatile boolean closed = false;

    // Static initialization check
    private static final boolean NATIVE_AVAILABLE;
    private static final String NATIVE_ERROR;

    static {
        boolean available = false;
        String error = null;

        try {
            // Verify native library is loadable and IO-uring is supported
            int supported = opensearch_iouring_h.osur_is_supported();
            if (supported == 1) {
                available = true;
                MemorySegment versionPtr = opensearch_iouring_h.osur_version();
                String version = versionPtr.getString(0);
                logger.info("IO-uring native library loaded successfully, version: {}", version);
            } else {
                error = "IO-uring not supported on this system (kernel too old?)";
            }
        } catch (UnsatisfiedLinkError e) {
            error = "Failed to load native library: " + e.getMessage();
            logger.warn("IO-uring native library not available: {}", error);
        } catch (Exception e) {
            error = "Error initializing IO-uring: " + e.getMessage();
            logger.warn("IO-uring initialization failed: {}", error);
        }

        NATIVE_AVAILABLE = available;
        NATIVE_ERROR = error;
    }

    /**
     * Check if IO-uring is available on this system.
     */
    public static boolean isAvailable() {
        return NATIVE_AVAILABLE;
    }

    /**
     * Get the reason IO-uring is not available (if applicable).
     */
    public static String getUnavailabilityReason() {
        return NATIVE_ERROR;
    }

    /**
     * Create a new IO-uring ring with default flags.
     *
     * @param queueDepth Size of submission/completion queues (power of 2)
     * @throws IOException If ring creation fails
     */
    public IOUringRing(int queueDepth) throws IOException {
        this(queueDepth, FLAG_DEFAULT);
    }

    /**
     * Create a new IO-uring ring with custom flags.
     *
     * @param queueDepth Size of submission/completion queues (power of 2)
     * @param flags Ring configuration flags
     * @throws IOException If ring creation fails
     */
    public IOUringRing(int queueDepth, int flags) throws IOException {
        if (!NATIVE_AVAILABLE) {
            throw new IOException("IO-uring not available: " + NATIVE_ERROR);
        }

        if (queueDepth <= 0 || (queueDepth & (queueDepth - 1)) != 0) {
            throw new IllegalArgumentException(
                "queueDepth must be a positive power of 2, got: " + queueDepth);
        }

        this.queueDepth = queueDepth;
        this.flags = flags;

        // Create arena for this ring's memory
        this.arena = Arena.ofConfined();

        // Allocate space for the ring handle pointer
        MemorySegment ringPtrPtr = arena.allocate(ValueLayout.ADDRESS);

        // Create the native ring
        int result = opensearch_iouring_h.osur_ring_create(queueDepth, flags, ringPtrPtr);

        if (result != 0) {
            arena.close();
            throw new IOException("Failed to create IO-uring ring: " + getErrorString(result));
        }

        // Get the ring handle
        this.ringHandle = ringPtrPtr.get(ValueLayout.ADDRESS, 0);

        // Preallocate completion segment for polling
        this.completionSegment = arena.allocate(osur_completion_t.layout());

        logger.debug("Created IO-uring ring: queueDepth={}, flags={}", queueDepth, flags);
    }

    /**
     * Submit a read operation.
     *
     * @param fd File descriptor
     * @param buffer Native memory buffer to read into
     * @param length Number of bytes to read
     * @param offset File offset
     * @param userData User data for completion identification
     * @return 0 on success, negative error code on failure
     */
    public int submitRead(int fd, MemorySegment buffer, int length, long offset, long userData) {
        checkNotClosed();
        Objects.requireNonNull(buffer, "buffer cannot be null");

        return opensearch_iouring_h.osur_submit_read(
            ringHandle, fd, buffer, length, offset, userData);
    }

    /**
     * Submit a write operation.
     */
    public int submitWrite(int fd, MemorySegment buffer, int length, long offset, long userData) {
        checkNotClosed();
        Objects.requireNonNull(buffer, "buffer cannot be null");

        return opensearch_iouring_h.osur_submit_write(
            ringHandle, fd, buffer, length, offset, userData);
    }

    /**
     * Submit a read using a registered file descriptor.
     */
    public int submitReadRegistered(int fdIndex, MemorySegment buffer,
                                    int length, long offset, long userData) {
        checkNotClosed();
        return opensearch_iouring_h.osur_submit_read_registered(
            ringHandle, fdIndex, buffer, length, offset, userData);
    }

    /**
     * Submit an fsync operation.
     */
    public int submitFsync(int fd, long userData) {
        checkNotClosed();
        return opensearch_iouring_h.osur_submit_fsync(ringHandle, fd, userData);
    }

    /**
     * Submit all pending operations to the kernel.
     *
     * @return Number of submitted operations, or negative error code
     */
    public int submit() {
        checkNotClosed();
        return opensearch_iouring_h.osur_submit(ringHandle);
    }

    /**
     * Submit and wait for at least one completion.
     */
    public int submitAndWait(int minComplete) {
        checkNotClosed();
        return opensearch_iouring_h.osur_submit_and_wait(ringHandle, minComplete);
    }

    /**
     * Poll for a completion (non-blocking).
     *
     * @return CompletionResult if available, null otherwise
     */
    public CompletionResult pollCompletion() {
        checkNotClosed();

        int result = opensearch_iouring_h.osur_poll_completion(ringHandle, completionSegment);

        if (result == 1) {
            return extractCompletion();
        }
        return null;
    }

    /**
     * Wait for a completion (blocking).
     *
     * @return CompletionResult
     * @throws IOException If wait fails
     */
    public CompletionResult waitCompletion() throws IOException {
        checkNotClosed();

        int result = opensearch_iouring_h.osur_wait_completion(ringHandle, completionSegment);

        if (result != 0) {
            throw new IOException("Wait for completion failed: " + getErrorString(result));
        }

        return extractCompletion();
    }

    /**
     * Wait for a completion with timeout.
     *
     * @param timeoutMs Timeout in milliseconds (-1 for infinite)
     * @return CompletionResult, or null on timeout
     * @throws IOException If wait fails (other than timeout)
     */
    public CompletionResult waitCompletion(int timeoutMs) throws IOException {
        checkNotClosed();

        int result = opensearch_iouring_h.osur_wait_completion_timeout(
            ringHandle, completionSegment, timeoutMs);

        if (result == -110) { // ETIMEDOUT
            return null;
        }

        if (result != 0) {
            throw new IOException("Wait for completion failed: " + getErrorString(result));
        }

        return extractCompletion();
    }

    /**
     * Get the number of ready completions.
     */
    public int completionReady() {
        checkNotClosed();
        return opensearch_iouring_h.osur_completion_ready(ringHandle);
    }

    /**
     * Register file descriptors for optimized access.
     */
    public void registerFiles(int[] fds) throws IOException {
        checkNotClosed();

        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment fdsSegment = tempArena.allocateFrom(ValueLayout.JAVA_INT, fds);

            int result = opensearch_iouring_h.osur_register_files(
                ringHandle, fdsSegment, fds.length);

            if (result != 0) {
                throw new IOException("Failed to register files: " + getErrorString(result));
            }
        }

        logger.debug("Registered {} file descriptors", fds.length);
    }

    /**
     * Unregister all file descriptors.
     */
    public void unregisterFiles() throws IOException {
        checkNotClosed();

        int result = opensearch_iouring_h.osur_unregister_files(ringHandle);

        if (result != 0) {
            throw new IOException("Failed to unregister files: " + getErrorString(result));
        }
    }

    /**
     * Allocate page-aligned native memory for I/O operations.
     */
    public MemorySegment allocateAligned(long size) {
        return allocateAligned(size, 4096);
    }

    /**
     * Allocate aligned native memory with custom alignment.
     */
    public MemorySegment allocateAligned(long size, long alignment) {
        MemorySegment ptr = opensearch_iouring_h.osur_alloc_aligned(size, alignment);
        if (ptr.equals(MemorySegment.NULL)) {
            throw new OutOfMemoryError("Failed to allocate " + size + " bytes");
        }
        return ptr.reinterpret(size);
    }

    /**
     * Free memory allocated by allocateAligned.
     */
    public void freeAligned(MemorySegment ptr) {
        if (ptr != null && !ptr.equals(MemorySegment.NULL)) {
            opensearch_iouring_h.osur_free_aligned(ptr);
        }
    }

    /**
     * Get the queue depth.
     */
    public int getQueueDepth() {
        return queueDepth;
    }

    /**
     * Get the number of pending submissions.
     */
    public int getPending() {
        checkNotClosed();
        return opensearch_iouring_h.osur_ring_pending(ringHandle);
    }

    /**
     * Check if the ring is closed.
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Destroy the native ring
        opensearch_iouring_h.osur_ring_destroy(ringHandle);

        // Close the arena (frees all memory)
        arena.close();

        logger.debug("IO-uring ring closed");
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IOUringRing is closed");
        }
    }

    private CompletionResult extractCompletion() {
        long userData = osur_completion_t.user_data(completionSegment);
        int result = osur_completion_t.result(completionSegment);
        int flags = osur_completion_t.flags(completionSegment);

        return new CompletionResult(userData, result, flags);
    }

    private static String getErrorString(int errorCode) {
        if (errorCode >= 0) {
            return "Success";
        }
        MemorySegment msgPtr = opensearch_iouring_h.osur_strerror(errorCode);
        return msgPtr.getString(0);
    }

    /**
     * Result of a completed I/O operation.
     */
    public record CompletionResult(long userData, int result, int flags) {

        /**
         * Check if the operation succeeded.
         */
        public boolean isSuccess() {
            return result >= 0;
        }

        /**
         * Get bytes transferred (for successful operations).
         */
        public int bytesTransferred() {
            return Math.max(0, result);
        }

        /**
         * Get error code (for failed operations).
         */
        public int errorCode() {
            return result < 0 ? -result : 0;
        }

        /**
         * Get error message (for failed operations).
         */
        public String errorMessage() {
            if (result >= 0) {
                return null;
            }
            MemorySegment msgPtr = opensearch_iouring_h.osur_strerror(result);
            return msgPtr.getString(0);
        }
    }
}
