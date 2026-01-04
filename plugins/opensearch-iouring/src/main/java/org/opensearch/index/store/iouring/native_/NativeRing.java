/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.native_;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Low-level wrapper over liburing via Panama FFM.
 *
 * <p>Provides direct access to io_uring ring operations for submitting
 * I/O operations and retrieving completions.
 *
 * <p>Thread Safety: Thread-safe. liburing handles internal synchronization
 * for submission and completion queue access.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Create instance with {@code new NativeRing()}
 *   <li>Initialize with {@link #init(int, int)}
 *   <li>Use for operations
 *   <li>Close with {@link #close()}
 * </ol>
 */
public final class NativeRing implements AutoCloseable {

    // ═══════════════════════════════════════════════════════════════════════════
    // Constants
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Size allocated for io_uring struct.
     * Actual size varies by liburing version but 512 bytes is sufficient.
     */
    private static final long IO_URING_STRUCT_SIZE = 512;

    /**
     * Size of io_uring_cqe struct (16 bytes standard).
     */
    private static final long CQE_SIZE = 16;

    /**
     * CQE field offsets.
     */
    private static final long CQE_USER_DATA_OFFSET = 0;
    private static final long CQE_RES_OFFSET = 8;
    private static final long CQE_FLAGS_OFFSET = 12;

    /**
     * Maximum CQEs to retrieve in a single batch operation.
     */
    private static final int MAX_CQE_BATCH = 256;

    // ═══════════════════════════════════════════════════════════════════════════
    // Native Library Binding
    // ═══════════════════════════════════════════════════════════════════════════

    private static final Linker LINKER;
    private static final SymbolLookup LIBURING;
    private static final boolean LIBRARY_AVAILABLE;
    private static final String LIBRARY_LOAD_ERROR;

    // Method handles for liburing functions
    private static final MethodHandle io_uring_queue_init;
    private static final MethodHandle io_uring_queue_exit;
    private static final MethodHandle io_uring_get_sqe;
    private static final MethodHandle io_uring_submit;
    private static final MethodHandle io_uring_wait_cqe;
    private static final MethodHandle io_uring_peek_batch_cqe;
    private static final MethodHandle io_uring_cq_advance;

    static {
        LINKER = Linker.nativeLinker();

        SymbolLookup lookup = null;
        String loadError = null;

        // Try to load liburing with common names
        String[] libraryNames = {"liburing.so.2", "liburing.so", "uring"};

        for (String name : libraryNames) {
            try {
                lookup = SymbolLookup.libraryLookup(name, Arena.global());
                break;
            } catch (IllegalArgumentException e) {
                loadError = e.getMessage();
            }
        }

        LIBURING = lookup;
        LIBRARY_AVAILABLE = (lookup != null);
        LIBRARY_LOAD_ERROR = loadError;

        if (LIBRARY_AVAILABLE) {
            try {
                // int io_uring_queue_init(unsigned entries, struct io_uring *ring, unsigned flags)
                io_uring_queue_init = LINKER.downcallHandle(
                    LIBURING.find("io_uring_queue_init").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_INT
                    )
                );

                // void io_uring_queue_exit(struct io_uring *ring)
                io_uring_queue_exit = LINKER.downcallHandle(
                    LIBURING.find("io_uring_queue_exit").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
                );

                // struct io_uring_sqe *io_uring_get_sqe(struct io_uring *ring)
                io_uring_get_sqe = LINKER.downcallHandle(
                    LIBURING.find("io_uring_get_sqe").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
                );

                // int io_uring_submit(struct io_uring *ring)
                io_uring_submit = LINKER.downcallHandle(
                    LIBURING.find("io_uring_submit").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
                );

                // int io_uring_wait_cqe(struct io_uring *ring, struct io_uring_cqe **cqe_ptr)
                io_uring_wait_cqe = LINKER.downcallHandle(
                    LIBURING.find("io_uring_wait_cqe").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS
                    )
                );

                // unsigned io_uring_peek_batch_cqe(struct io_uring *ring,
                //                                  struct io_uring_cqe **cqes, unsigned count)
                io_uring_peek_batch_cqe = LINKER.downcallHandle(
                    LIBURING.find("io_uring_peek_batch_cqe").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_INT
                    )
                );

                // void io_uring_cq_advance(struct io_uring *ring, unsigned nr)
                io_uring_cq_advance = LINKER.downcallHandle(
                    LIBURING.find("io_uring_cq_advance").orElseThrow(),
                    FunctionDescriptor.ofVoid(
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_INT
                    )
                );

            } catch (Throwable t) {
                throw new ExceptionInInitializerError(
                    "Failed to bind liburing functions: " + t.getMessage());
            }
        } else {
            io_uring_queue_init = null;
            io_uring_queue_exit = null;
            io_uring_get_sqe = null;
            io_uring_submit = null;
            io_uring_wait_cqe = null;
            io_uring_peek_batch_cqe = null;
            io_uring_cq_advance = null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Instance State
    // ═══════════════════════════════════════════════════════════════════════════

    private final Arena arena;
    private final MemorySegment ring;

    // Scratch space for CQE pointer retrieval
    private final MemorySegment cqePtrBuffer;
    private final MemorySegment cqePtrArray;

    private volatile boolean initialized;
    private volatile boolean closed;

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates an uninitialized ring.
     * Must call {@link #init(int, int)} before use.
     *
     * @throws UnsupportedOperationException if liburing is not available
     */
    public NativeRing() {
        if (!LIBRARY_AVAILABLE) {
            throw new UnsupportedOperationException(
                "liburing is not available: " + LIBRARY_LOAD_ERROR);
        }

        this.arena = Arena.ofShared();
        this.ring = arena.allocate(IO_URING_STRUCT_SIZE, 8);

        // Buffer to receive single CQE pointer from io_uring_wait_cqe
        this.cqePtrBuffer = arena.allocate(ValueLayout.ADDRESS);

        // Array of CQE pointers for batch operations
        this.cqePtrArray = arena.allocate(
            MemoryLayout.sequenceLayout(MAX_CQE_BATCH, ValueLayout.ADDRESS));

        this.initialized = false;
        this.closed = false;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Initialization
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Initializes the io_uring ring.
     *
     * @param entries Number of SQ entries (rounded up to power of 2 by kernel)
     * @param flags   IORING_SETUP_* flags, 0 for defaults
     * @return 0 on success, negative errno on failure
     * @throws IllegalStateException if already initialized or closed
     */
    public int init(int entries, int flags) {
        if (closed) {
            throw new IllegalStateException("Ring is closed");
        }
        if (initialized) {
            throw new IllegalStateException("Ring is already initialized");
        }

        try {
            int result = (int) io_uring_queue_init.invokeExact(entries, ring, flags);
            if (result == 0) {
                initialized = true;
            }
            return result;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke io_uring_queue_init", t);
        }
    }

    /**
     * Initializes with default flags.
     *
     * @param entries Number of SQ entries
     * @return 0 on success, negative errno on failure
     */
    public int init(int entries) {
        return init(entries, 0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SQE Operations
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Gets an SQE for preparing an operation.
     *
     * @return MemorySegment pointing to SQE (64 bytes), or null if submission queue is full
     * @throws IllegalStateException if not initialized or closed
     */
    public MemorySegment getSqe() {
        checkState();

        try {
            MemorySegment sqePtr = (MemorySegment) io_uring_get_sqe.invokeExact(ring);

            if (sqePtr.equals(MemorySegment.NULL)) {
                return null;
            }

            // Reinterpret to proper size for SQE access
            return sqePtr.reinterpret(SqeUtils.SQE_SIZE);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke io_uring_get_sqe", t);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Submission
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Submits all pending SQEs to the kernel.
     *
     * @return Number of SQEs submitted, or negative errno on failure
     * @throws IllegalStateException if not initialized or closed
     */
    public int submit() {
        checkState();

        try {
            return (int) io_uring_submit.invokeExact(ring);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke io_uring_submit", t);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Completion
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Waits for at least one completion, then retrieves up to maxCount.
     * Blocks until at least one completion is available.
     *
     * <p>Output array format: {@code [userData0, result0, userData1, result1, ...]}
     *
     * <p>After processing, caller must call {@link #advanceCq(int)} with the
     * returned count to mark completions as consumed.
     *
     * @param output   Array to fill with [userData, result] pairs (size >= maxCount * 2)
     * @param maxCount Maximum completions to retrieve (capped at 256)
     * @return Number of completions retrieved, or negative errno on failure
     * @throws IllegalStateException    if not initialized or closed
     * @throws IllegalArgumentException if output array is too small
     */
    public int waitCqes(long[] output, int maxCount) {
        checkState();

        int effectiveMax = Math.min(maxCount, MAX_CQE_BATCH);
        if (output.length < effectiveMax * 2) {
            throw new IllegalArgumentException(
                "Output array too small. Need at least " + (effectiveMax * 2) +
                    " elements for " + effectiveMax + " completions");
        }

        try {
            // Wait for at least one completion
            int waitResult = (int) io_uring_wait_cqe.invokeExact(ring, cqePtrBuffer);
            if (waitResult < 0) {
                return waitResult;
            }

            // Peek all available completions
            int count = (int) io_uring_peek_batch_cqe.invokeExact(
                ring, cqePtrArray, effectiveMax);

            // Extract user_data and result from each CQE
            extractCompletions(output, count);

            return count;

        } catch (Throwable t) {
            throw new RuntimeException("Failed during waitCqes", t);
        }
    }

    /**
     * Peeks for completions without blocking.
     *
     * <p>Output array format: {@code [userData0, result0, userData1, result1, ...]}
     *
     * <p>After processing, caller must call {@link #advanceCq(int)} with the
     * returned count to mark completions as consumed.
     *
     * @param output   Array to fill with [userData, result] pairs
     * @param maxCount Maximum completions to retrieve (capped at 256)
     * @return Number of completions retrieved (0 if none available)
     * @throws IllegalStateException    if not initialized or closed
     * @throws IllegalArgumentException if output array is too small
     */
    public int peekCqes(long[] output, int maxCount) {
        checkState();

        int effectiveMax = Math.min(maxCount, MAX_CQE_BATCH);
        if (output.length < effectiveMax * 2) {
            throw new IllegalArgumentException(
                "Output array too small. Need at least " + (effectiveMax * 2) +
                    " elements for " + effectiveMax + " completions");
        }

        try {
            int count = (int) io_uring_peek_batch_cqe.invokeExact(
                ring, cqePtrArray, effectiveMax);

            if (count > 0) {
                extractCompletions(output, count);
            }

            return count;

        } catch (Throwable t) {
            throw new RuntimeException("Failed during peekCqes", t);
        }
    }

    /**
     * Extracts user_data and result from CQE pointers into output array.
     */
    private void extractCompletions(long[] output, int count) {
        for (int i = 0; i < count; i++) {
            // Get CQE pointer from array
            MemorySegment cqePtr = cqePtrArray.getAtIndex(ValueLayout.ADDRESS, i);

            // Reinterpret to access CQE fields
            MemorySegment cqe = cqePtr.reinterpret(CQE_SIZE);

            // Extract fields
            long userData = cqe.get(ValueLayout.JAVA_LONG, CQE_USER_DATA_OFFSET);
            int result = cqe.get(ValueLayout.JAVA_INT, CQE_RES_OFFSET);

            // Store in output array
            output[i * 2] = userData;
            output[i * 2 + 1] = result;
        }
    }

    /**
     * Advances the completion queue head after processing completions.
     * Must be called after processing results from {@link #waitCqes} or {@link #peekCqes}.
     *
     * @param count Number of completions processed
     * @throws IllegalStateException    if not initialized or closed
     * @throws IllegalArgumentException if count is negative
     */
    public void advanceCq(int count) {
        checkState();

        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative: " + count);
        }
        if (count == 0) {
            return;
        }

        try {
            io_uring_cq_advance.invokeExact(ring, count);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke io_uring_cq_advance", t);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // State
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Checks if the ring is initialized and operational.
     *
     * @return true if ring is initialized and not closed
     */
    public boolean isOpen() {
        return initialized && !closed;
    }

    /**
     * Checks if the ring has been closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Checks state and throws if not usable.
     */
    private void checkState() {
        if (closed) {
            throw new IllegalStateException("Ring is closed");
        }
        if (!initialized) {
            throw new IllegalStateException("Ring is not initialized");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Closes the ring and releases all native resources.
     * Safe to call multiple times.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        if (initialized) {
            try {
                io_uring_queue_exit.invokeExact(ring);
            } catch (Throwable t) {
                // Log but don't throw - we're closing
                System.err.println("Warning: io_uring_queue_exit failed: " + t.getMessage());
            }
        }

        arena.close();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Static Utilities
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Checks if io_uring is available on this system.
     * Creates a temporary ring to verify functionality.
     *
     * @return true if io_uring is available and functional
     */
    public static boolean isAvailable() {
        if (!LIBRARY_AVAILABLE) {
            return false;
        }

        try (NativeRing testRing = new NativeRing()) {
            int result = testRing.init(1, 0);
            return result == 0;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * Returns the reason why liburing is not available, if applicable.
     *
     * @return Error message or null if library is available
     */
    public static String getLibraryLoadError() {
        return LIBRARY_AVAILABLE ? null : LIBRARY_LOAD_ERROR;
    }
}
