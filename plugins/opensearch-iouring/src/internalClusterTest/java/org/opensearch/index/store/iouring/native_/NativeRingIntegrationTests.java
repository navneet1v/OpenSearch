/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.native_;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for NativeRing.
 * These tests require Linux with io_uring support.
 */
public class NativeRingIntegrationTests {

    private NativeRing ring;

    @BeforeClass
    public static void checkPlatform() {
        assumeTrue("Skipping: io_uring not available", NativeRing.isAvailable());
    }

    @Before
    public void setUp() {
        ring = new NativeRing();
    }

    @After
    public void tearDown() {
        if (ring != null) {
            ring.close();
        }
    }

    @Test
    public void testInitAndClose() {
        int result = ring.init(32);
        assertEquals(0, result);
        assertTrue(ring.isOpen());

        ring.close();
        assertTrue(ring.isClosed());
    }

    @Test(expected = IllegalStateException.class)
    public void testInitTwiceThrows() {
        ring.init(32);
        ring.init(32); // Should throw
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSqeBeforeInitThrows() {
        ring.getSqe();
    }

    @Test
    public void testGetSqeReturnsValidSegment() {
        ring.init(32);
        MemorySegment sqe = ring.getSqe();

        assertNotNull(sqe);
        assertEquals(SqeUtils.SQE_SIZE, sqe.byteSize());
    }

    @Test
    public void testGetSqeMultipleTimes() {
        ring.init(32);

        for (int i = 0; i < 10; i++) {
            MemorySegment sqe = ring.getSqe();
            assertNotNull("SQE " + i + " should not be null", sqe);
            SqeUtils.prepareNop(sqe);
            SqeUtils.setUserData(sqe, i);
        }

        int submitted = ring.submit();
        assertEquals(10, submitted);
    }

    @Test
    public void testSubmitEmpty() {
        ring.init(32);
        int submitted = ring.submit();
        assertEquals(0, submitted);
    }

    @Test
    public void testSubmitNop() {
        ring.init(32);

        MemorySegment sqe = ring.getSqe();
        SqeUtils.prepareNop(sqe);
        SqeUtils.setUserData(sqe, 42);

        int submitted = ring.submit();
        assertEquals(1, submitted);
    }

    @Test
    public void testWaitCqes() {
        ring.init(32);

        // Submit a NOP
        MemorySegment sqe = ring.getSqe();
        SqeUtils.prepareNop(sqe);
        SqeUtils.setUserData(sqe, 123);
        ring.submit();

        // Wait for completion
        long[] completions = new long[2];
        int count = ring.waitCqes(completions, 1);

        assertEquals(1, count);
        assertEquals(123, completions[0]); // user_data
        assertEquals(0, completions[1]);   // result (NOP returns 0)

        ring.advanceCq(count);
    }

    @Test
    public void testPeekCqes() {
        ring.init(32);

        // Submit multiple NOPs
        for (int i = 0; i < 5; i++) {
            MemorySegment sqe = ring.getSqe();
            SqeUtils.prepareNop(sqe);
            SqeUtils.setUserData(sqe, i + 1);
        }
        ring.submit();

        // Wait for at least one
        long[] waitBuf = new long[2];
        ring.waitCqes(waitBuf, 1);

        // Peek all available
        long[] completions = new long[10];
        int count = ring.peekCqes(completions, 5);

        assertTrue(count >= 1);
        ring.advanceCq(count);
    }

    @Test
    public void testAdvanceCqZero() {
        ring.init(32);
        ring.advanceCq(0); // Should not throw
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdvanceCqNegativeThrows() {
        ring.init(32);
        ring.advanceCq(-1);
    }

    @Test
    public void testCloseIdempotent() {
        ring.init(32);
        ring.close();
        ring.close(); // Should not throw
        assertTrue(ring.isClosed());
    }

    @Test(expected = IllegalStateException.class)
    public void testSubmitAfterCloseThrows() {
        ring.init(32);
        ring.close();
        ring.submit();
    }

    @Test
    public void testIsAvailable() {
        // Already checked in @BeforeClass, but verify method works
        assertTrue(NativeRing.isAvailable());
    }

    @Test
    public void testGetLibraryLoadError() {
        // Should be null since we passed the availability check
        assertNull(NativeRing.getLibraryLoadError());
    }
}
