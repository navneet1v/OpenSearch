/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * Configuration for io_uring ring initialization and operation.
 *
 * <p>Immutable after construction. Use {@link #builder()} for custom configuration
 * or {@link #defaults()} for sensible defaults.
 *
 * <p>Example usage:
 * <pre>{@code
 * IoUringConfig config = IoUringConfig.builder()
 *     .ringSize(512)
 *     .shutdownTimeoutMs(10_000)
 *     .build();
 * }</pre>
 */
public final class IoUringConfig {

    // ═══════════════════════════════════════════════════════════════════════════
    // Default Values
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Default ring size (SQ entries).
     */
    public static final int DEFAULT_RING_SIZE = 256;

    /**
     * Default buffer pool tier sizes in bytes.
     */
    public static final int[] DEFAULT_BUFFER_POOL_TIERS = {4096, 16384, 65536, 262144};

    /**
     * Default maximum buffers per pool tier.
     */
    public static final int[] DEFAULT_BUFFER_POOL_MAX_PER_TIER = {64, 32, 16, 8};

    /**
     * Default shutdown timeout in milliseconds.
     */
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

    /**
     * Default initial backoff delay for completion polling in nanoseconds.
     */
    public static final long DEFAULT_POLL_BACKOFF_INITIAL_NS = 100;

    /**
     * Default maximum backoff delay for completion polling in nanoseconds (1ms).
     */
    public static final long DEFAULT_POLL_BACKOFF_MAX_NS = 1_000_000;

    /**
     * Default maximum empty polls before blocking wait.
     */
    public static final int DEFAULT_POLL_BACKOFF_RETRY_LIMIT = 1000;

    // ═══════════════════════════════════════════════════════════════════════════
    // Instance Fields
    // ═══════════════════════════════════════════════════════════════════════════

    private final int ringSize;
    private final int[] bufferPoolTiers;
    private final int[] bufferPoolMaxPerTier;
    private final long shutdownTimeoutMs;
    private final long pollBackoffInitialNs;
    private final long pollBackoffMaxNs;
    private final int pollBackoffRetryLimit;

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    private IoUringConfig(Builder builder) {
        this.ringSize = builder.ringSize;
        this.bufferPoolTiers = builder.bufferPoolTiers.clone();
        this.bufferPoolMaxPerTier = builder.bufferPoolMaxPerTier.clone();
        this.shutdownTimeoutMs = builder.shutdownTimeoutMs;
        this.pollBackoffInitialNs = builder.pollBackoffInitialNs;
        this.pollBackoffMaxNs = builder.pollBackoffMaxNs;
        this.pollBackoffRetryLimit = builder.pollBackoffRetryLimit;
    }

    /**
     * Returns a new builder with default values.
     *
     * @return New builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default configuration.
     *
     * @return Default configuration instance
     */
    public static IoUringConfig defaults() {
        return builder().build();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Accessors
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns the ring size (number of SQ entries).
     * The kernel will round this up to the next power of 2.
     *
     * @return Ring size
     */
    public int ringSize() {
        return ringSize;
    }

    /**
     * Returns the buffer pool tier sizes in bytes.
     * Each tier represents a different buffer size class.
     *
     * @return Copy of tier sizes array
     */
    public int[] bufferPoolTiers() {
        return bufferPoolTiers.clone();
    }

    /**
     * Returns the maximum buffers allowed per pool tier.
     *
     * @return Copy of max-per-tier array
     */
    public int[] bufferPoolMaxPerTier() {
        return bufferPoolMaxPerTier.clone();
    }

    /**
     * Returns the number of buffer pool tiers.
     *
     * @return Number of tiers
     */
    public int bufferPoolTierCount() {
        return bufferPoolTiers.length;
    }

    /**
     * Returns the buffer size for a specific tier.
     *
     * @param tier Tier index (0-based)
     * @return Buffer size in bytes
     * @throws IndexOutOfBoundsException if tier is invalid
     */
    public int bufferPoolTierSize(int tier) {
        return bufferPoolTiers[tier];
    }

    /**
     * Returns the maximum buffers for a specific tier.
     *
     * @param tier Tier index (0-based)
     * @return Maximum buffer count
     * @throws IndexOutOfBoundsException if tier is invalid
     */
    public int bufferPoolTierMax(int tier) {
        return bufferPoolMaxPerTier[tier];
    }

    /**
     * Returns the shutdown timeout in milliseconds.
     * This is the maximum time to wait for pending operations during shutdown.
     *
     * @return Shutdown timeout in milliseconds
     */
    public long shutdownTimeoutMs() {
        return shutdownTimeoutMs;
    }

    /**
     * Returns the initial backoff delay for completion polling in nanoseconds.
     *
     * @return Initial backoff delay
     */
    public long pollBackoffInitialNs() {
        return pollBackoffInitialNs;
    }

    /**
     * Returns the maximum backoff delay for completion polling in nanoseconds.
     *
     * @return Maximum backoff delay
     */
    public long pollBackoffMaxNs() {
        return pollBackoffMaxNs;
    }

    /**
     * Returns the maximum number of empty polls before falling back to blocking wait.
     *
     * @return Retry limit
     */
    public int pollBackoffRetryLimit() {
        return pollBackoffRetryLimit;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Object Methods
    // ═══════════════════════════════════════════════════════════════════════════

    @Override
    public String toString() {
        return "IoUringConfig{" +
            "ringSize=" + ringSize +
            ", bufferPoolTiers=" + Arrays.toString(bufferPoolTiers) +
            ", bufferPoolMaxPerTier=" + Arrays.toString(bufferPoolMaxPerTier) +
            ", shutdownTimeoutMs=" + shutdownTimeoutMs +
            ", pollBackoffInitialNs=" + pollBackoffInitialNs +
            ", pollBackoffMaxNs=" + pollBackoffMaxNs +
            ", pollBackoffRetryLimit=" + pollBackoffRetryLimit +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IoUringConfig that)) return false;
        return ringSize == that.ringSize &&
            shutdownTimeoutMs == that.shutdownTimeoutMs &&
            pollBackoffInitialNs == that.pollBackoffInitialNs &&
            pollBackoffMaxNs == that.pollBackoffMaxNs &&
            pollBackoffRetryLimit == that.pollBackoffRetryLimit &&
            Arrays.equals(bufferPoolTiers, that.bufferPoolTiers) &&
            Arrays.equals(bufferPoolMaxPerTier, that.bufferPoolMaxPerTier);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ringSize, shutdownTimeoutMs, pollBackoffInitialNs,
            pollBackoffMaxNs, pollBackoffRetryLimit);
        result = 31 * result + Arrays.hashCode(bufferPoolTiers);
        result = 31 * result + Arrays.hashCode(bufferPoolMaxPerTier);
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Builder
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Builder for {@link IoUringConfig}.
     */
    public static final class Builder {

        private int ringSize = DEFAULT_RING_SIZE;
        private int[] bufferPoolTiers = DEFAULT_BUFFER_POOL_TIERS.clone();
        private int[] bufferPoolMaxPerTier = DEFAULT_BUFFER_POOL_MAX_PER_TIER.clone();
        private long shutdownTimeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS;
        private long pollBackoffInitialNs = DEFAULT_POLL_BACKOFF_INITIAL_NS;
        private long pollBackoffMaxNs = DEFAULT_POLL_BACKOFF_MAX_NS;
        private int pollBackoffRetryLimit = DEFAULT_POLL_BACKOFF_RETRY_LIMIT;

        private Builder() {
        }

        /**
         * Sets the ring size (number of SQ entries).
         * The kernel will round this up to the next power of 2.
         *
         * @param ringSize Ring size (must be positive)
         * @return this builder
         * @throws IllegalArgumentException if ringSize <= 0
         */
        public Builder ringSize(int ringSize) {
            if (ringSize <= 0) {
                throw new IllegalArgumentException("Ring size must be positive: " + ringSize);
            }
            this.ringSize = ringSize;
            return this;
        }

        /**
         * Sets the buffer pool tier sizes.
         * Each tier represents a different buffer size class.
         * Tiers should be in ascending order.
         *
         * @param tiers Buffer sizes in bytes (must have at least one tier)
         * @return this builder
         * @throws IllegalArgumentException if tiers is empty or contains non-positive values
         */
        public Builder bufferPoolTiers(int... tiers) {
            if (tiers == null || tiers.length == 0) {
                throw new IllegalArgumentException("At least one buffer pool tier is required");
            }
            for (int i = 0; i < tiers.length; i++) {
                if (tiers[i] <= 0) {
                    throw new IllegalArgumentException(
                        "Buffer pool tier size must be positive: " + tiers[i] + " at index " + i);
                }
            }
            this.bufferPoolTiers = tiers.clone();
            return this;
        }

        /**
         * Sets the maximum buffers per pool tier.
         * Array length must match the number of tiers.
         *
         * @param maxPerTier Maximum buffer count per tier
         * @return this builder
         * @throws IllegalArgumentException if array is empty or contains negative values
         */
        public Builder bufferPoolMaxPerTier(int... maxPerTier) {
            if (maxPerTier == null || maxPerTier.length == 0) {
                throw new IllegalArgumentException("At least one max-per-tier value is required");
            }
            for (int i = 0; i < maxPerTier.length; i++) {
                if (maxPerTier[i] < 0) {
                    throw new IllegalArgumentException(
                        "Max per tier cannot be negative: " + maxPerTier[i] + " at index " + i);
                }
            }
            this.bufferPoolMaxPerTier = maxPerTier.clone();
            return this;
        }

        /**
         * Sets the shutdown timeout in milliseconds.
         *
         * @param timeoutMs Shutdown timeout (must be positive)
         * @return this builder
         * @throws IllegalArgumentException if timeoutMs <= 0
         */
        public Builder shutdownTimeoutMs(long timeoutMs) {
            if (timeoutMs <= 0) {
                throw new IllegalArgumentException("Shutdown timeout must be positive: " + timeoutMs);
            }
            this.shutdownTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Sets the initial backoff delay for completion polling.
         *
         * @param nanos Initial delay in nanoseconds (must be non-negative)
         * @return this builder
         * @throws IllegalArgumentException if nanos < 0
         */
        public Builder pollBackoffInitialNs(long nanos) {
            if (nanos < 0) {
                throw new IllegalArgumentException(
                    "Poll backoff initial delay cannot be negative: " + nanos);
            }
            this.pollBackoffInitialNs = nanos;
            return this;
        }

        /**
         * Sets the maximum backoff delay for completion polling.
         *
         * @param nanos Maximum delay in nanoseconds (must be non-negative)
         * @return this builder
         * @throws IllegalArgumentException if nanos < 0
         */
        public Builder pollBackoffMaxNs(long nanos) {
            if (nanos < 0) {
                throw new IllegalArgumentException(
                    "Poll backoff max delay cannot be negative: " + nanos);
            }
            this.pollBackoffMaxNs = nanos;
            return this;
        }

        /**
         * Sets the maximum empty polls before falling back to blocking wait.
         *
         * @param limit Retry limit (must be non-negative)
         * @return this builder
         * @throws IllegalArgumentException if limit < 0
         */
        public Builder pollBackoffRetryLimit(int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException(
                    "Poll backoff retry limit cannot be negative: " + limit);
            }
            this.pollBackoffRetryLimit = limit;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return Immutable configuration instance
         * @throws IllegalStateException if buffer pool tiers and max-per-tier arrays have different lengths
         */
        public IoUringConfig build() {
            if (bufferPoolTiers.length != bufferPoolMaxPerTier.length) {
                throw new IllegalStateException(
                    "Buffer pool tiers and max-per-tier arrays must have same length. " +
                        "Tiers: " + bufferPoolTiers.length +
                        ", MaxPerTier: " + bufferPoolMaxPerTier.length);
            }
            if (pollBackoffInitialNs > pollBackoffMaxNs) {
                throw new IllegalStateException(
                    "Poll backoff initial delay cannot exceed max delay. " +
                        "Initial: " + pollBackoffInitialNs + "ns, Max: " + pollBackoffMaxNs + "ns");
            }
            return new IoUringConfig(this);
        }
    }
}
