/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import org.junit.Test;

import static org.junit.Assert.*;

public class IoUringConfigTests {

    @Test
    public void testDefaultValues() {
        IoUringConfig config = IoUringConfig.defaults();

        assertEquals(IoUringConfig.DEFAULT_RING_SIZE, config.ringSize());
        assertEquals(IoUringConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, config.shutdownTimeoutMs());
        assertEquals(IoUringConfig.DEFAULT_POLL_BACKOFF_INITIAL_NS, config.pollBackoffInitialNs());
        assertEquals(IoUringConfig.DEFAULT_POLL_BACKOFF_MAX_NS, config.pollBackoffMaxNs());
        assertEquals(IoUringConfig.DEFAULT_POLL_BACKOFF_RETRY_LIMIT, config.pollBackoffRetryLimit());
        assertArrayEquals(IoUringConfig.DEFAULT_BUFFER_POOL_TIERS, config.bufferPoolTiers());
        assertArrayEquals(IoUringConfig.DEFAULT_BUFFER_POOL_MAX_PER_TIER, config.bufferPoolMaxPerTier());
    }

    @Test
    public void testBuilderCustomValues() {
        IoUringConfig config = IoUringConfig.builder()
            .ringSize(512)
            .shutdownTimeoutMs(10000)
            .pollBackoffInitialNs(200)
            .pollBackoffMaxNs(2000000)
            .pollBackoffRetryLimit(500)
            .bufferPoolTiers(8192, 32768)
            .bufferPoolMaxPerTier(32, 16)
            .build();

        assertEquals(512, config.ringSize());
        assertEquals(10000, config.shutdownTimeoutMs());
        assertEquals(200, config.pollBackoffInitialNs());
        assertEquals(2000000, config.pollBackoffMaxNs());
        assertEquals(500, config.pollBackoffRetryLimit());
        assertArrayEquals(new int[]{8192, 32768}, config.bufferPoolTiers());
        assertArrayEquals(new int[]{32, 16}, config.bufferPoolMaxPerTier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativeRingSizeThrows() {
        IoUringConfig.builder().ringSize(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderZeroRingSizeThrows() {
        IoUringConfig.builder().ringSize(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativeShutdownTimeoutThrows() {
        IoUringConfig.builder().shutdownTimeoutMs(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativePollBackoffInitialThrows() {
        IoUringConfig.builder().pollBackoffInitialNs(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativePollBackoffMaxThrows() {
        IoUringConfig.builder().pollBackoffMaxNs(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativePollBackoffRetryLimitThrows() {
        IoUringConfig.builder().pollBackoffRetryLimit(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderEmptyBufferPoolTiersThrows() {
        IoUringConfig.builder().bufferPoolTiers().build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativeTierSizeThrows() {
        IoUringConfig.builder().bufferPoolTiers(4096, -1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderNegativeMaxPerTierThrows() {
        IoUringConfig.builder().bufferPoolMaxPerTier(32, -1).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderMismatchedArrayLengthsThrows() {
        IoUringConfig.builder()
            .bufferPoolTiers(4096, 8192, 16384)
            .bufferPoolMaxPerTier(32, 16)
            .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderInitialExceedsMaxBackoffThrows() {
        IoUringConfig.builder()
            .pollBackoffInitialNs(1000)
            .pollBackoffMaxNs(100)
            .build();
    }

    @Test
    public void testArrayImmutability() {
        int[] tiers = {4096, 8192};
        IoUringConfig config = IoUringConfig.builder()
            .bufferPoolTiers(tiers)
            .bufferPoolMaxPerTier(32, 16)
            .build();

        // Modify original array
        tiers[0] = 9999;

        // Config should be unaffected
        assertEquals(4096, config.bufferPoolTiers()[0]);
    }

    @Test
    public void testReturnedArrayImmutability() {
        IoUringConfig config = IoUringConfig.defaults();
        int[] tiers = config.bufferPoolTiers();
        int original = tiers[0];
        tiers[0] = 9999;

        // Config should be unaffected
        assertEquals(original, config.bufferPoolTiers()[0]);
    }

    @Test
    public void testTierAccessors() {
        IoUringConfig config = IoUringConfig.defaults();

        assertEquals(4, config.bufferPoolTierCount());
        assertEquals(4096, config.bufferPoolTierSize(0));
        assertEquals(64, config.bufferPoolTierMax(0));
    }

    @Test
    public void testEqualsAndHashCode() {
        IoUringConfig config1 = IoUringConfig.defaults();
        IoUringConfig config2 = IoUringConfig.defaults();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEquals() {
        IoUringConfig config1 = IoUringConfig.defaults();
        IoUringConfig config2 = IoUringConfig.builder().ringSize(512).build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testToString() {
        IoUringConfig config = IoUringConfig.defaults();
        String str = config.toString();

        assertTrue(str.contains("ringSize"));
        assertTrue(str.contains("bufferPoolTiers"));
    }
}
