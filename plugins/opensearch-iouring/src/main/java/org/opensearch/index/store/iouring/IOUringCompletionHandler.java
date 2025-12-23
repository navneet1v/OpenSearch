/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles asynchronous completion notifications from IO-uring.
 *
 * This class runs a background thread that polls the completion queue
 * and notifies waiting callers when their I/O operations complete.
 *
 * <p>Design:</p>
 * <ul>
 *   <li>Single completion thread polls the ring's CQ</li>
 *   <li>Pending requests tracked via ConcurrentHashMap</li>
 *   <li>Callers receive CompletableFuture for async notification</li>
 *   <li>Supports timeout-based waiting</li>
 * </ul>
 */
public class IOUringCompletionHandler implements Closeable {

    private static final Logger logger = LogManager.getLogger(IOUringCompletionHandler.class);

    private final IOUringRing ring;
    private final ConcurrentHashMap<Long, CompletableFuture<Integer>> pendingRequests;
    private final Thread completionThread;
    private final AtomicBoolean running;
    private final AtomicLong requestIdGenerator;

    // Statistics
    private final AtomicLong completedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong timeoutCount = new AtomicLong(0);

    // Configuration
    private static final int POLL_TIMEOUT_MS = 100; // Poll timeout for graceful shutdown

    /**
     * Create a completion handler for the given ring.
     *
     * @param ring The IO-uring ring to poll for completions
     */
    public IOUringCompletionHandler(IOUringRing ring) {
        this.ring = ring;
        this.pendingRequests = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(true);
        this.requestIdGenerator = new AtomicLong(0);

        // Start the completion polling thread
        this.completionThread = Thread.ofPlatform()
            .name("iouring-completion-handler")
            .daemon(true)
            .start(this::completionLoop);

        logger.info("IO-uring completion handler started");
    }

    /**
     * Generate a unique request ID.
     */
    public long nextRequestId() {
        return requestIdGenerator.incrementAndGet();
    }

    /**
     * Register a pending I/O request.
     *
     * @param requestId Unique request identifier
     * @return Future that completes when I/O finishes
     */
    public CompletableFuture<Integer> registerRequest(long requestId) {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        CompletableFuture<Integer> existing = pendingRequests.putIfAbsent(requestId, future);
        if (existing != null) {
            throw new IllegalStateException("Duplicate request ID: " + requestId);
        }

        return future;
    }

    /**
     * Cancel a pending request.
     */
    public boolean cancelRequest(long requestId) {
        CompletableFuture<Integer> future = pendingRequests.remove(requestId);
        if (future != null) {
            future.completeExceptionally(new CancellationException("Request cancelled"));
            return true;
        }
        return false;
    }

    /**
     * Get the number of pending requests.
     */
    public int getPendingCount() {
        return pendingRequests.size();
    }

    /**
     * Get completion statistics.
     */
    public long getCompletedCount() {
        return completedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    public long getTimeoutCount() {
        return timeoutCount.get();
    }

    /**
     * The main completion polling loop.
     */
    private void completionLoop() {
        logger.debug("Completion loop started");

        while (running.get()) {
            try {
                // Wait for completion with timeout (allows checking running flag)
                IOUringRing.CompletionResult completion = ring.waitCompletion(POLL_TIMEOUT_MS);

                if (completion != null) {
                    handleCompletion(completion);
                }

            } catch (IOException e) {
                if (running.get()) {
                    logger.error("Error in completion loop", e);
                    errorCount.incrementAndGet();

                    // Brief pause to avoid tight error loop
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        logger.debug("Completion loop stopped");
    }

    /**
     * Handle a single completion.
     */
    private void handleCompletion(IOUringRing.CompletionResult completion) {
        long requestId = completion.userData();
        int result = completion.result();

        CompletableFuture<Integer> future = pendingRequests.remove(requestId);

        if (future != null) {
            if (completion.isSuccess()) {
                future.complete(result);
                completedCount.incrementAndGet();
            } else {
                future.completeExceptionally(
                    new IOException("I/O operation failed: " + completion.errorMessage()));
                errorCount.incrementAndGet();
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Completed request {}: result={}", requestId, result);
            }
        } else {
            // Request may have been cancelled
            logger.debug("Completion for unknown/cancelled request: {}", requestId);
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return; // Already closed
        }

        logger.debug("Closing completion handler...");

        // Wait for completion thread to finish
        try {
            completionThread.join(5000);
            if (completionThread.isAlive()) {
                logger.warn("Completion thread did not stop gracefully, interrupting...");
                completionThread.interrupt();
                completionThread.join(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for completion thread");
        }

        // Complete remaining pending requests with errors
        int remaining = pendingRequests.size();
        if (remaining > 0) {
            logger.warn("Completing {} pending requests with errors", remaining);
            pendingRequests.forEach((id, future) -> {
                future.completeExceptionally(new IOException("Completion handler closed"));
            });
            pendingRequests.clear();
        }

        logger.info("IO-uring completion handler closed. Stats: completed={}, errors={}, timeouts={}",
            completedCount.get(), errorCount.get(), timeoutCount.get());
    }
}
