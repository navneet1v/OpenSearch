/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BufferPoolTests {

    private BufferPool pool;

    @Before
    public void setUp() {
        // Reset singleton for isolated tests
        BufferPool.reset();
        pool = BufferPool.getInstance();
    }

    @After
    public void tearDown() {
        if (pool != null) {
            pool.clear();
        }
        BufferPool.reset();
    }

    @Test
    public void testAcquireReturnsDirectBuffer() {
        ByteBuffer buffer = pool.acquire(1024);
        assertNotNull(buffer);
        assertTrue(buffer.isDirect());
        pool.release(buffer);
    }

    @Test
    public void testAcquireReturnsCorrectTierSize() {
        // Default tiers: 4096, 16384, 65536, 262144
        ByteBuffer small = pool.acquire(100);
        assertEquals(4096, small.capacity());

        ByteBuffer medium = pool.acquire(5000);
        assertEquals(16384, medium.capacity());

        ByteBuffer large = pool.acquire(20000);
        assertEquals(65536, large.capacity());

        pool.release(small);
        pool.release(medium);
        pool.release(large);
    }

    @Test
    public void testAcquireZeroSizeReturnsSmallestTier() {
        ByteBuffer buffer = pool.acquire(0);
        assertNotNull(buffer);
        assertEquals(4096, buffer.capacity());
        pool.release(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireNegativeSizeThrows() {
        pool.acquire(-1);
    }

    @Test
    public void testReleaseAndReuse() {
        ByteBuffer buffer1 = pool.acquire(4096);
        pool.release(buffer1);

        ByteBuffer buffer2 = pool.acquire(4096);
        assertSame("Should reuse pooled buffer", buffer1, buffer2);
        pool.release(buffer2);
    }

    @Test
    public void testReleaseNullIsIgnored() {
        pool.release(null); // Should not throw
    }

    @Test
    public void testReleaseHeapBufferIsIgnored() {
        ByteBuffer heapBuffer = ByteBuffer.allocate(4096);
        int countBefore = pool.getPooledCount();
        pool.release(heapBuffer);
        assertEquals(countBefore, pool.getPooledCount());
    }

    @Test
    public void testReleaseNonTierSizeIsIgnored() {
        ByteBuffer oddSize = ByteBuffer.allocateDirect(1234);
        int countBefore = pool.getPooledCount();
        pool.release(oddSize);
        assertEquals(countBefore, pool.getPooledCount());
    }

    @Test
    public void testPoolMaxCapacity() {
        // Default max per tier[0] is 64
        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < 70; i++) {
            buffers.add(pool.acquire(4096));
        }

        // Release all
        for (ByteBuffer buf : buffers) {
            pool.release(buf);
        }

        // Pool should cap at max (64)
        assertTrue(pool.getPooledCount(0) <= 64);
    }

    @Test
    public void testClear() {
        ByteBuffer buffer = pool.acquire(4096);
        pool.release(buffer);
        assertTrue(pool.getPooledCount() > 0);

        pool.clear();
        assertEquals(0, pool.getPooledCount());
    }

    @Test
    public void testGetStats() {
        ByteBuffer buffer = pool.acquire(4096);
        pool.release(buffer);

        BufferPool.Stats stats = pool.getStats();
        assertNotNull(stats);
        assertTrue(stats.totalPooledCount() > 0);
        assertEquals(4, stats.tierSizes().length);
    }

    @Test
    public void testConcurrentAcquireRelease() throws InterruptedException {
        int threadCount = 8;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        ByteBuffer buf = pool.acquire(4096);
                        assertNotNull(buf);
                        pool.release(buf);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
    }

    @Test
    public void testAcquireLargerThanAllTiers() {
        // Larger than max tier (262144)
        ByteBuffer buffer = pool.acquire(500000);
        assertNotNull(buffer);
        assertEquals(500000, buffer.capacity());
        // This buffer won't be pooled on release
        pool.release(buffer);
    }

    @Test
    public void testBufferClearedOnAcquire() {
        ByteBuffer buffer = pool.acquire(4096);
        buffer.putInt(12345);
        buffer.flip();
        pool.release(buffer);

        ByteBuffer reused = pool.acquire(4096);
        assertEquals(0, reused.position());
        assertEquals(reused.capacity(), reused.limit());
        pool.release(reused);
    }
}
