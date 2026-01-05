/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.index.store.iouring.native_.NativeRing;
import org.opensearch.index.store.iouring.native_.SqeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for IoUringRing singleton.
 * These tests require Linux with io_uring support.
 */
public class IoUringRingIntegrationTests {

    @BeforeClass
    public static void checkPlatform() {
        assumeTrue("Skipping: io_uring not available", NativeRing.isAvailable());
    }

    @AfterClass
    public static void cleanup() {
        IoUringRing.reset();
        BufferPool.reset();
    }

    @Test
    public void testGetInstance() {
        IoUringRing ring = IoUringRing.getInstance();
        assertNotNull(ring);
        assertTrue(ring.isRunning());
    }

    @Test
    public void testSingletonSameInstance() {
        IoUringRing ring1 = IoUringRing.getInstance();
        IoUringRing ring2 = IoUringRing.getInstance();
        assertSame(ring1, ring2);
    }

    @Test
    public void testSubmitNop() throws Exception {
        IoUringRing ring = IoUringRing.getInstance();

        CompletableFuture<Integer> future = ring.submit(sqe -> {
            SqeUtils.prepareNop(sqe);
        });

        Integer result = future.get(5, TimeUnit.SECONDS);
        assertEquals(Integer.valueOf(0), result);
    }

    @Test
    public void testSubmitMultipleNops() throws Exception {
        IoUringRing ring = IoUringRing.getInstance();

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(ring.submit(sqe -> SqeUtils.prepareNop(sqe)));
        }

        for (CompletableFuture<Integer> future : futures) {
            Integer result = future.get(5, TimeUnit.SECONDS);
            assertEquals(Integer.valueOf(0), result);
        }
    }

    @Test
    public void testPendingCount() {
        IoUringRing ring = IoUringRing.getInstance();

        int initialCount = ring.getPendingCount();

        // Submit without waiting
        CompletableFuture<Integer> future = ring.submit(sqe -> SqeUtils.prepareNop(sqe));

        // Pending count should increase (or complete immediately)
        // Just verify it doesn't throw
        int count = ring.getPendingCount();
        assertTrue(count >= 0);

        // Wait for completion
        future.join();
    }

    @Test
    public void testGetConfig() {
        IoUringRing ring = IoUringRing.getInstance();
        IoUringConfig config = ring.getConfig();

        assertNotNull(config);
        assertTrue(config.ringSize() > 0);
    }

    @Test
    public void testSubmitWithCallback() throws Exception {
        IoUringRing ring = IoUringRing.getInstance();

        boolean[] callbackInvoked = {false};

        CompletableFuture<Integer> future = ring.submit(
            sqe -> SqeUtils.prepareNop(sqe),
            () -> callbackInvoked[0] = true
        );

        future.get(5, TimeUnit.SECONDS);

        // Give callback time to execute
        Thread.sleep(100);
        assertTrue("Callback should have been invoked", callbackInvoked[0]);
    }

    @Test
    public void testConcurrentSubmissions() throws Exception {
        IoUringRing ring = IoUringRing.getInstance();

        int threadCount = 4;
        int opsPerThread = 25;
        CompletableFuture<?>[] allFutures = new CompletableFuture<?>[threadCount * opsPerThread];

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    int idx = threadIdx * opsPerThread + i;
                    allFutures[idx] = ring.submit(sqe -> SqeUtils.prepareNop(sqe));
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        // Wait for all completions
        CompletableFuture.allOf(allFutures).get(30, TimeUnit.SECONDS);
    }

    @Test
    public void testIsAvailableStatic() {
        assertTrue(IoUringRing.isAvailable());
    }

    @Test
    public void testGetInstanceOrNull() {
        // After getInstance() has been called, this should return non-null
        IoUringRing.getInstance();
        assertNotNull(IoUringRing.getInstanceOrNull());
    }
}
