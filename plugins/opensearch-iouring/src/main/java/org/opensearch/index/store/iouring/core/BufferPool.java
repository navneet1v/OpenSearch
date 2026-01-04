/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool of direct ByteBuffers for heap-to-direct conversion.
 *
 * <p>io_uring requires native memory addresses for I/O operations. When users
 * provide heap buffers, we need to copy data to/from direct buffers. This pool
 * maintains reusable direct buffers to avoid allocation overhead.
 *
 * <p>Buffers are organized in size tiers. When acquiring a buffer, the smallest
 * tier that satisfies the requested size is used. Buffers larger than the largest
 * tier are allocated on demand and not pooled.
 *
 * <p>Thread Safety: All methods are thread-safe.
 *
 * <p>Example usage:
 * <pre>{@code
 * BufferPool pool = BufferPool.getInstance();
 * ByteBuffer buffer = pool.acquire(4096);
 * try {
 *     // Use buffer for I/O
 * } finally {
 *     pool.release(buffer);
 * }
 * }</pre>
 */
public final class BufferPool {

    // ═══════════════════════════════════════════════════════════════════════════
    // Singleton
    // ═══════════════════════════════════════════════════════════════════════════

    private static volatile BufferPool instance;
    private static final Object INSTANCE_LOCK = new Object();

    /**
     * Returns the singleton instance.
     * Creates the pool with default configuration on first access.
     *
     * @return Singleton BufferPool instance
     */
    public static BufferPool getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new BufferPool(IoUringConfig.defaults());
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
                throw new IllegalStateException("BufferPool already initialized");
            }
            instance = new BufferPool(config);
        }
    }

    /**
     * Resets the singleton instance.
     * For testing purposes only.
     */
    static void reset() {
        synchronized (INSTANCE_LOCK) {
            if (instance != null) {
                instance.clear();
                instance = null;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Instance Fields
    // ═══════════════════════════════════════════════════════════════════════════

    private final int[] tierSizes;
    private final int[] tierMaxCounts;
    private final ConcurrentLinkedQueue<ByteBuffer>[] pools;
    private final AtomicInteger[] poolCounts;

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates a buffer pool with the specified configuration.
     *
     * @param config Configuration specifying tier sizes and limits
     */
    @SuppressWarnings("unchecked")
    private BufferPool(IoUringConfig config) {
        int tierCount = config.bufferPoolTierCount();

        this.tierSizes = config.bufferPoolTiers();
        this.tierMaxCounts = config.bufferPoolMaxPerTier();
        this.pools = new ConcurrentLinkedQueue[tierCount];
        this.poolCounts = new AtomicInteger[tierCount];

        for (int i = 0; i < tierCount; i++) {
            pools[i] = new ConcurrentLinkedQueue<>();
            poolCounts[i] = new AtomicInteger(0);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Acquire
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Acquires a direct buffer of at least the specified size.
     *
     * <p>Returns a pooled buffer if available, otherwise allocates a new one.
     * The returned buffer has:
     * <ul>
     *   <li>position = 0</li>
     *   <li>limit = capacity</li>
     *   <li>capacity >= minSize</li>
     * </ul>
     *
     * @param minSize Minimum required size in bytes
     * @return Direct ByteBuffer with capacity at least minSize
     * @throws IllegalArgumentException if minSize is negative
     */
    public ByteBuffer acquire(int minSize) {
        if (minSize < 0) {
            throw new IllegalArgumentException("Size cannot be negative: " + minSize);
        }

        if (minSize == 0) {
            // Return smallest tier for zero-size request
            return acquireFromTier(0);
        }

        int tier = selectTier(minSize);

        if (tier < 0) {
            // Larger than any tier - allocate directly, won't be pooled
            return ByteBuffer.allocateDirect(minSize);
        }

        return acquireFromTier(tier);
    }

    /**
     * Acquires a buffer from the specified tier.
     */
    private ByteBuffer acquireFromTier(int tier) {
        ConcurrentLinkedQueue<ByteBuffer> pool = pools[tier];

        ByteBuffer buffer = pool.poll();
        if (buffer != null) {
            poolCounts[tier].decrementAndGet();
            buffer.clear();
            return buffer;
        }

        // Pool empty - allocate new buffer
        return ByteBuffer.allocateDirect(tierSizes[tier]);
    }

    /**
     * Selects the appropriate tier for the requested size.
     *
     * @param size Requested buffer size
     * @return Tier index, or -1 if size exceeds all tiers
     */
    private int selectTier(int size) {
        for (int i = 0; i < tierSizes.length; i++) {
            if (size <= tierSizes[i]) {
                return i;
            }
        }
        return -1;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Release
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns a buffer to the pool for reuse.
     *
     * <p>The buffer is added to the appropriate tier pool if:
     * <ul>
     *   <li>It is a direct buffer</li>
     *   <li>Its capacity matches a tier size exactly</li>
     *   <li>The tier pool is not at maximum capacity</li>
     * </ul>
     *
     * <p>Silently ignores:
     * <ul>
     *   <li>null buffers</li>
     *   <li>non-direct buffers</li>
     *   <li>buffers with non-tier capacities</li>
     * </ul>
     *
     * @param buffer Buffer to return (may be null)
     */
    public void release(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }

        int capacity = buffer.capacity();
        int tier = findExactTier(capacity);

        if (tier < 0) {
            // Not a pooled size - let GC handle it
            return;
        }

        // Check if pool has room
        int currentCount = poolCounts[tier].get();
        if (currentCount >= tierMaxCounts[tier]) {
            // Pool full - let GC handle it
            return;
        }

        // Try to increment count and add to pool
        if (poolCounts[tier].compareAndSet(currentCount, currentCount + 1)) {
            buffer.clear();
            pools[tier].offer(buffer);
        }
        // If CAS fails, another thread got there first - let GC handle this buffer
    }

    /**
     * Finds the tier that exactly matches the given capacity.
     *
     * @param capacity Buffer capacity
     * @return Tier index, or -1 if no exact match
     */
    private int findExactTier(int capacity) {
        for (int i = 0; i < tierSizes.length; i++) {
            if (capacity == tierSizes[i]) {
                return i;
            }
        }
        return -1;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Management
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Clears all pooled buffers.
     * Useful for testing or releasing memory under pressure.
     */
    public void clear() {
        for (int i = 0; i < pools.length; i++) {
            pools[i].clear();
            poolCounts[i].set(0);
        }
    }

    /**
     * Returns the total number of buffers currently pooled across all tiers.
     *
     * @return Total pooled buffer count
     */
    public int getPooledCount() {
        int total = 0;
        for (AtomicInteger count : poolCounts) {
            total += count.get();
        }
        return total;
    }

    /**
     * Returns the number of buffers pooled in a specific tier.
     *
     * @param tier Tier index (0-based)
     * @return Pooled buffer count for the tier
     * @throws IndexOutOfBoundsException if tier is invalid
     */
    public int getPooledCount(int tier) {
        return poolCounts[tier].get();
    }

    /**
     * Returns the number of tiers in this pool.
     *
     * @return Tier count
     */
    public int getTierCount() {
        return tierSizes.length;
    }

    /**
     * Returns the buffer size for a specific tier.
     *
     * @param tier Tier index (0-based)
     * @return Buffer size in bytes
     * @throws IndexOutOfBoundsException if tier is invalid
     */
    public int getTierSize(int tier) {
        return tierSizes[tier];
    }

    /**
     * Returns the maximum pool size for a specific tier.
     *
     * @param tier Tier index (0-based)
     * @return Maximum buffer count
     * @throws IndexOutOfBoundsException if tier is invalid
     */
    public int getTierMaxCount(int tier) {
        return tierMaxCounts[tier];
    }

    /**
     * Returns statistics about the buffer pool.
     *
     * @return Pool statistics
     */
    public Stats getStats() {
        int[] counts = new int[pools.length];
        long totalPooledBytes = 0;
        long totalCapacityBytes = 0;

        for (int i = 0; i < pools.length; i++) {
            counts[i] = poolCounts[i].get();
            totalPooledBytes += (long) counts[i] * tierSizes[i];
            totalCapacityBytes += (long) tierMaxCounts[i] * tierSizes[i];
        }

        return new Stats(
            tierSizes.clone(),
            tierMaxCounts.clone(),
            counts,
            totalPooledBytes,
            totalCapacityBytes
        );
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Stats Record
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Statistics about the buffer pool state.
     */
    public record Stats(
        int[] tierSizes,
        int[] tierMaxCounts,
        int[] tierCurrentCounts,
        long totalPooledBytes,
        long totalCapacityBytes
    ) {
        /**
         * Returns the total number of currently pooled buffers.
         */
        public int totalPooledCount() {
            int total = 0;
            for (int count : tierCurrentCounts) {
                total += count;
            }
            return total;
        }

        /**
         * Returns the pool utilization as a percentage (0.0 to 1.0).
         */
        public double utilization() {
            if (totalCapacityBytes == 0) {
                return 0.0;
            }
            return (double) totalPooledBytes / totalCapacityBytes;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BufferPool.Stats{\n");
            for (int i = 0; i < tierSizes.length; i++) {
                sb.append("  tier[").append(i).append("]: ")
                    .append(tierSizes[i]).append(" bytes, ")
                    .append(tierCurrentCounts[i]).append("/").append(tierMaxCounts[i])
                    .append(" buffers\n");
            }
            sb.append("  totalPooled: ").append(totalPooledBytes).append(" bytes\n");
            sb.append("  utilization: ").append(String.format("%.1f%%", utilization() * 100));
            sb.append("\n}");
            return sb.toString();
        }
    }
}
