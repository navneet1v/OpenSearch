/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import org.opensearch.index.store.iouring.native_.NativeRing;
import org.opensearch.index.store.iouring.native_.SqeUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Singleton manager for the shared io_uring ring.
 *
 * <p>Handles operation submission and completion processing for the entire JVM.
 * Uses a single io_uring ring shared across all threads.
 *
 * <p>Thread Safety: All methods are thread-safe. Multiple threads can submit
 * operations concurrently. Completion processing is handled by a dedicated
 * background thread.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>Lazily initialized on first {@link #getInstance()} call</li>
 *   <li>Registers JVM shutdown hook for cleanup</li>
 *   <li>Can be explicitly shut down via {@link #shutdown()}</li>
 * </ul>
 *
 * Example usage:
 * <pre>
 * IoUringRing ring = IoUringRing.getInstance();
 *
 * CompletableFuture future = ring.submit(sqe -&gt; {
 *     SqeUtils.prepareRead(sqe, fd, bufferAddr, length, offset);
 * });
 *
 * int bytesRead = future.join();
 * </pre>
 */
public final class IoUringRing {

    // ═══════════════════════════════════════════════════════════════════════════
    // Singleton
    // ═══════════════════════════════════════════════════════════════════════════

    private static volatile IoUringRing instance;
    private static final Object INSTANCE_LOCK = new Object();

    /**
     * Maximum completions to retrieve per poll/wait cycle.
     * Kept small to minimize latency in interactive workloads.
     */
    private static final int COMPLETION_BATCH_SIZE = 8;

    /**
     * Returns the singleton instance.
     * Initializes the ring on first call.
     *
     * @return Singleton IoUringRing instance
     * @throws IllegalStateException if io_uring is not available or initialization fails
     */
    public static IoUringRing getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new IoUringRing(IoUringConfig.defaults());
                    instance.registerShutdownHook();
                }
            }
        }
        return instance;
    }

    /**
     * Initializes the singleton with custom configuration.
     * Must be called before first {@link #getInstance()} call.
     *
     * @param config Configuration to use
     * @throws IllegalStateException if already initialized
     */
    public static void initialize(IoUringConfig config) {
        synchronized (INSTANCE_LOCK) {
            if (instance != null) {
                throw new IllegalStateException("IoUringRing already initialized");
            }
            instance = new IoUringRing(config);
            instance.registerShutdownHook();
        }
    }

    /**
     * Returns the singleton if already initialized, null otherwise.
     * Does not trigger initialization.
     *
     * @return Singleton instance or null
     */
    public static IoUringRing getInstanceOrNull() {
        return instance;
    }

    /**
     * Checks if io_uring is available on this system.
     * Does not initialize the singleton.
     *
     * @return true if io_uring is available
     */
    public static boolean isAvailable() {
        return NativeRing.isAvailable();
    }

    /**
     * Resets the singleton instance.
     * For testing purposes only.
     */
    static void reset() {
        synchronized (INSTANCE_LOCK) {
            if (instance != null) {
                instance.shutdown();
                instance = null;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Pending Operation Tracking
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Holds state for a pending operation.
     */
    private record PendingOperation(
        CompletableFuture<Integer> future,
        Runnable onComplete
    ) {
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Instance State
    // ═══════════════════════════════════════════════════════════════════════════

    private final IoUringConfig config;
    private final NativeRing nativeRing;
    private final ConcurrentHashMap<Long, PendingOperation> pendingOps;
    private final AtomicLong opIdGenerator;
    private final Thread completionPoller;
    private final AtomicReference<Thread> shutdownHook;

    private volatile boolean running;
    private volatile boolean shutdownInitiated;

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates and initializes the ring.
     *
     * @param config Configuration
     * @throws IllegalStateException if initialization fails
     */
    private IoUringRing(IoUringConfig config) {
        this.config = config;
        this.nativeRing = new NativeRing();
        this.pendingOps = new ConcurrentHashMap<>();
        this.opIdGenerator = new AtomicLong(0);
        this.shutdownHook = new AtomicReference<>();
        this.running = false;
        this.shutdownInitiated = false;

        // Initialize the native ring
        int result = nativeRing.init(config.ringSize());
        if (result < 0) {
            nativeRing.close();
            throw new IllegalStateException(
                "Failed to initialize io_uring: " + ErrorHandler.errnoName(-result) +
                    " (" + ErrorHandler.errnoDescription(-result) + ")");
        }

        // Start completion poller
        this.running = true;
        this.completionPoller = new Thread(this::pollCompletions, "iouring-completion-poller");
        this.completionPoller.setDaemon(true);
        this.completionPoller.start();
    }

    /**
     * Registers JVM shutdown hook.
     */
    private void registerShutdownHook() {
        Thread hook = new Thread(this::shutdown, "iouring-shutdown-hook");
        if (shutdownHook.compareAndSet(null, hook)) {
            try {
                Runtime.getRuntime().addShutdownHook(hook);
            } catch (IllegalStateException e) {
                // JVM already shutting down
            }
        }
    }

    /**
     * Unregisters the shutdown hook.
     */
    private void unregisterShutdownHook() {
        Thread hook = shutdownHook.getAndSet(null);
        if (hook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException e) {
                // JVM shutting down, ignore
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Submission
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Submits an operation and returns a future for the result.
     *
     * <p>The preparer consumer receives an SQE and must prepare it
     * (set opcode, fd, addr, len, etc.). User data is set automatically.
     *
     * @param preparer Consumer that prepares the SQE
     * @return CompletableFuture that completes with the CQE result (int)
     * @throws IllegalStateException if ring is shut down
     */
    public CompletableFuture<Integer> submit(Consumer<MemorySegment> preparer) {
        return submit(preparer, null);
    }

    /**
     * Submits an operation with a cleanup callback.
     *
     * <p>The onComplete callback is invoked after the future completes,
     * regardless of success or failure. Useful for releasing buffers.
     *
     * @param preparer   Consumer that prepares the SQE
     * @param onComplete Callback invoked after completion (may be null)
     * @return CompletableFuture that completes with the CQE result
     * @throws IllegalStateException if ring is shut down
     */
    public CompletableFuture<Integer> submit(Consumer<MemorySegment> preparer,
                                             Runnable onComplete) {
        if (!running) {
            CompletableFuture<Integer> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                new IllegalStateException("IoUringRing is shut down"));
            invokeOnComplete(onComplete);
            return failed;
        }

        // Generate unique operation ID
        long opId = opIdGenerator.incrementAndGet();

        // Create future and register pending operation
        CompletableFuture<Integer> future = new CompletableFuture<>();
        PendingOperation pendingOp = new PendingOperation(future, onComplete);
        pendingOps.put(opId, pendingOp);

        // Get SQE
        MemorySegment sqe = nativeRing.getSqe();
        if (sqe == null) {
            // Queue full - submit pending and retry
            nativeRing.submit();
            sqe = nativeRing.getSqe();

            if (sqe == null) {
                // Still full - fail the operation
                pendingOps.remove(opId);
                invokeOnComplete(onComplete);

                CompletableFuture<Integer> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                    new IllegalStateException("Submission queue is full"));
                return failed;
            }
        }

        // Prepare the SQE
        try {
            preparer.accept(sqe);
            SqeUtils.setUserData(sqe, opId);
        } catch (Throwable t) {
            pendingOps.remove(opId);
            invokeOnComplete(onComplete);

            CompletableFuture<Integer> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }

        // Submit to kernel
        int submitted = nativeRing.submit();
        if (submitted < 0) {
            pendingOps.remove(opId);
            invokeOnComplete(onComplete);

            CompletableFuture<Integer> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                ErrorHandler.translateError(-submitted, null, "submit failed"));
            return failed;
        }

        return future;
    }

    /**
     * Safely invokes the onComplete callback.
     */
    private void invokeOnComplete(Runnable onComplete) {
        if (onComplete != null) {
            try {
                onComplete.run();
            } catch (Throwable t) {
                // Log but don't propagate
                System.err.println("Warning: onComplete callback threw exception: " + t.getMessage());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Completion Polling
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Completion polling loop with adaptive backoff.
     */
    private void pollCompletions() {
        // Pre-allocate buffer for completions: [userData, result] pairs
        long[] completions = new long[COMPLETION_BATCH_SIZE * 2];

        long backoffNs = config.pollBackoffInitialNs();
        int emptyPolls = 0;

        while (running) {
            try {
                int count = nativeRing.peekCqes(completions, COMPLETION_BATCH_SIZE);

                if (count > 0) {
                    processCompletions(completions, count);
                    nativeRing.advanceCq(count);

                    backoffNs = config.pollBackoffInitialNs();
                    emptyPolls = 0;

                } else if (count == 0) {
                    emptyPolls++;

                    if (emptyPolls >= config.pollBackoffRetryLimit()) {
                        count = nativeRing.waitCqes(completions, COMPLETION_BATCH_SIZE);

                        if (count > 0) {
                            processCompletions(completions, count);
                            nativeRing.advanceCq(count);
                        }

                        backoffNs = config.pollBackoffInitialNs();
                        emptyPolls = 0;

                    } else {
                        if (backoffNs > 0) {
                            LockSupport.parkNanos(backoffNs);
                            backoffNs = Math.min(backoffNs * 2, config.pollBackoffMaxNs());
                        } else {
                            Thread.onSpinWait();
                        }
                    }

                } else {
                    if (count == -ErrorHandler.EINTR) {
                        continue;
                    }
                    System.err.println("Warning: peekCqes returned error: " +
                        ErrorHandler.errnoName(-count));
                }

            } catch (Throwable t) {
                if (running) {
                    System.err.println("Error in completion poller: " + t.getMessage());
                    t.printStackTrace();
                }
            }
        }

        drainRemainingCompletions(completions);
    }

    /**
     * Processes completed operations.
     */
    private void processCompletions(long[] completions, int count) {
        for (int i = 0; i < count; i++) {
            long opId = completions[i * 2];
            int result = (int) completions[i * 2 + 1];

            PendingOperation pendingOp = pendingOps.remove(opId);
            if (pendingOp == null) {
                // Unknown operation - should not happen
                System.err.println("Warning: received completion for unknown operation: " + opId);
                continue;
            }

            // Complete the future
            CompletableFuture<Integer> future = pendingOp.future();
            if (result < 0) {
                future.completeExceptionally(
                    ErrorHandler.translateError(-result, null, "operation failed"));
            } else {
                future.complete(result);
            }

            // Invoke cleanup callback
            invokeOnComplete(pendingOp.onComplete());
        }
    }

    /**
     * Drains remaining completions during shutdown.
     */
    private void drainRemainingCompletions(long[] completions) {
        try {
            int count;
            while ((count = nativeRing.peekCqes(completions, COMPLETION_BATCH_SIZE)) > 0) {
                processCompletions(completions, count);
                nativeRing.advanceCq(count);
            }
        } catch (Throwable t) {
            // Ignore errors during drain
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // State
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Checks if the ring is operational.
     *
     * @return true if ring is accepting submissions
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns count of currently pending operations.
     * For monitoring/debugging.
     *
     * @return Pending operation count
     */
    public int getPendingCount() {
        return pendingOps.size();
    }

    /**
     * Returns the configuration.
     *
     * @return Configuration
     */
    public IoUringConfig getConfig() {
        return config;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Shutdown
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Initiates graceful shutdown.
     *
     * <ul>
     *   <li>Stops accepting new submissions</li>
     *   <li>Waits for pending operations to complete (with timeout)</li>
     *   <li>Closes the native ring</li>
     *   <li>Clears the buffer pool</li>
     * </ul>
     *
     * <p>Safe to call multiple times.
     */
    public void shutdown() {
        if (shutdownInitiated) {
            return;
        }

        synchronized (this) {
            if (shutdownInitiated) {
                return;
            }
            shutdownInitiated = true;
        }

        // Unregister shutdown hook (if not called from hook itself)
        unregisterShutdownHook();

        // Stop accepting new submissions
        running = false;

        // Wait for pending operations with timeout
        long deadlineMs = System.currentTimeMillis() + config.shutdownTimeoutMs();
        while (!pendingOps.isEmpty() && System.currentTimeMillis() < deadlineMs) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Warn about abandoned operations
        int abandoned = pendingOps.size();
        if (abandoned > 0) {
            System.err.println("Warning: " + abandoned +
                " operations abandoned during shutdown");

            // Fail remaining futures
            for (PendingOperation op : pendingOps.values()) {
                op.future().completeExceptionally(
                    new IOException("Operation abandoned during shutdown"));
                invokeOnComplete(op.onComplete());
            }
            pendingOps.clear();
        }

        // Stop completion poller
        try {
            // Submit a NOP to wake up the poller if it's in blocking wait
            if (nativeRing.isOpen()) {
                MemorySegment sqe = nativeRing.getSqe();
                if (sqe != null) {
                    SqeUtils.prepareNop(sqe);
                    SqeUtils.setUserData(sqe, 0); // Special ID for shutdown NOP
                    nativeRing.submit();
                }
            }

            completionPoller.join(1000);

            if (completionPoller.isAlive()) {
                completionPoller.interrupt();
                completionPoller.join(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Close native ring
        nativeRing.close();

        // Clear buffer pool
        BufferPool.getInstance().clear();
    }
}
