package org.opensearch.index.store.iouring;

import org.opensearch.index.store.iouring.ffi.osur_completion_t;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static org.opensearch.index.store.iouring.ffi.opensearch_iouring_h.*;

/**
 * Central async scheduler for io_uring.
 *
 * Owns:
 *  - ring lifecycle
 *  - submission
 *  - CQ polling
 *  - completion dispatch
 */
final class IOUringScheduler implements AutoCloseable {

    private final MemorySegment ring;
    private final ExecutorService poller;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final AtomicLong requestIds = new AtomicLong(1);
    private final ConcurrentHashMap<Long, CompletableFuture<Integer>> inflight =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Boolean> processed =
            new ConcurrentHashMap<>();
    private final Arena sharedArena = Arena.ofShared();

    IOUringScheduler(int queueDepth) {
        this.ring = createRing(queueDepth);
        this.poller = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "osur-cq-poller");
            t.setDaemon(true);
            return t;
        });
        startPoller();
    }

    /* ============================
     * Submission
     * ============================ */

    long submitRead(
            int fd,
            MemorySegment buffer,
            int length,
            long offset
    ) {
        Objects.requireNonNull(buffer, "buffer can't be null");

        if (!running.get()) {
            throw new RejectedExecutionException("Scheduler is closed");
        }

        long id = requestIds.getAndIncrement();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        inflight.put(id, future);
        processed.put(id, false);

        int rc = osur_submit_read(
                ring,
                fd,
                buffer,
                length,
                offset,
                id
        );

        if (rc != 0) {
            inflight.remove(id);
            throw new IllegalStateException("osur_submit_read failed: " + rc);
        }
        osur_submit(ring);
        return id;
    }

    int waitForCompletion(long requestId) throws IOException {
        CompletableFuture<Integer> future = inflight.get(requestId);
        if (future == null) {
            throw new IllegalStateException("Unknown request id " + requestId);
        }

        try {
            int result = future.get();
            if (processed.containsKey(requestId)) {
                inflight.remove(requestId);
            } else {
                processed.put(requestId, true);
            }
            return result;
        } catch (ExecutionException e) {
            inflight.remove(requestId);
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException(cause);
        } catch (InterruptedException e) {
            inflight.remove(requestId);
            throw new IOException("Interrupted while waiting for IO", e);
        }
    }

    /* ============================
     * CQ Poller
     * ============================ */

    private void startPoller() {
        poller.submit(() -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment cqe =
                        arena.allocate(osur_completion_t.layout());

                while (running.get()) {
                    int rc = osur_wait_completion(ring, cqe);
                    if (rc != 0) {
                        continue; // interrupted or spurious
                    }

                    long id = osur_completion_t.user_data(cqe);
                    int result = osur_completion_t.result(cqe);

                    CompletableFuture<Integer> future =
                            inflight.get(id);

                    if (future != null) {
                        if (result >= 0) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(
                                    new IOException("IO failed: " + (-result))
                            );
                        }
                    }

                    if (processed.containsKey(id)) {
                        inflight.remove(id);
                    } else {
                        processed.put(id, true);
                    }
                }
            }
        });
    }

    /* ============================
     * Lifecycle
     * ============================ */

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        poller.shutdownNow();
        inflight.forEach((id, future) ->
                future.completeExceptionally(
                        new IOException("Scheduler shutting down"))
        );
        inflight.clear();

        osur_ring_destroy(ring);
        sharedArena.close();
    }

    /* ============================
     * Native helpers
     * ============================ */

    private MemorySegment createRing(int depth) {
        MemorySegment out = sharedArena.allocate(ADDRESS);
        int rc = osur_ring_create(depth, OSUR_RING_DEFAULT(), out);
        if (rc != 0) {
            throw new IllegalStateException(
                    "osur_ring_create failed: " + (-rc));
        }
        return out.get(ADDRESS, 0);
    }
}
