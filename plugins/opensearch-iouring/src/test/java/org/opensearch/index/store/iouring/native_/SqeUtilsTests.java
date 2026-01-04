/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.native_;

import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class SqeUtilsTests {

    @Test
    public void testPrepareRead() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareRead(sqe, 5, 0x1000L, 4096, 0);

            assertEquals(SqeUtils.IORING_OP_READ, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(5, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
            assertEquals(0x1000L, sqe.get(ValueLayout.JAVA_LONG, SqeUtils.SQE_ADDR_OFFSET));
            assertEquals(4096, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_LEN_OFFSET));
            assertEquals(0L, sqe.get(ValueLayout.JAVA_LONG, SqeUtils.SQE_OFF_OFFSET));
        }
    }

    @Test
    public void testPrepareWrite() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareWrite(sqe, 7, 0x2000L, 8192, 1024);

            assertEquals(SqeUtils.IORING_OP_WRITE, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(7, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
            assertEquals(0x2000L, sqe.get(ValueLayout.JAVA_LONG, SqeUtils.SQE_ADDR_OFFSET));
            assertEquals(8192, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_LEN_OFFSET));
            assertEquals(1024L, sqe.get(ValueLayout.JAVA_LONG, SqeUtils.SQE_OFF_OFFSET));
        }
    }

    @Test
    public void testPrepareFsyncWithDatasync() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareFsync(sqe, 3, true);

            assertEquals(SqeUtils.IORING_OP_FSYNC, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(3, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
            assertEquals(SqeUtils.IORING_FSYNC_DATASYNC, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_OP_FLAGS_OFFSET));
        }
    }

    @Test
    public void testPrepareFsyncWithoutDatasync() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareFsync(sqe, 3, false);

            assertEquals(SqeUtils.IORING_OP_FSYNC, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(3, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
            assertEquals(0, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_OP_FLAGS_OFFSET));
        }
    }

    @Test
    public void testPrepareClose() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareClose(sqe, 10);

            assertEquals(SqeUtils.IORING_OP_CLOSE, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(10, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
        }
    }

    @Test
    public void testPrepareNop() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareNop(sqe);

            assertEquals(SqeUtils.IORING_OP_NOP, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
        }
    }

    @Test
    public void testSetAndGetUserData() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);
            SqeUtils.clear(sqe);

            long userData = 0xDEADBEEFCAFEBABEL;
            SqeUtils.setUserData(sqe, userData);

            assertEquals(userData, SqeUtils.getUserData(sqe));
        }
    }

    @Test
    public void testSetFlags() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);
            SqeUtils.clear(sqe);

            SqeUtils.setFlags(sqe, SqeUtils.IOSQE_ASYNC);

            assertEquals(SqeUtils.IOSQE_ASYNC, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_FLAGS_OFFSET));
        }
    }

    @Test
    public void testClear() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            // Fill with non-zero data
            for (int i = 0; i < SqeUtils.SQE_SIZE; i++) {
                sqe.set(ValueLayout.JAVA_BYTE, i, (byte) 0xFF);
            }

            SqeUtils.clear(sqe);

            // Verify all bytes are zero
            for (int i = 0; i < SqeUtils.SQE_SIZE; i++) {
                assertEquals(0, sqe.get(ValueLayout.JAVA_BYTE, i));
            }
        }
    }

    @Test
    public void testGetBufferAddressDirectBuffer() {
        ByteBuffer direct = ByteBuffer.allocateDirect(1024);
        long addr = SqeUtils.getBufferAddress(direct);
        assertTrue(addr != 0);
    }

    @Test
    public void testGetBufferAddressWithPosition() {
        ByteBuffer direct = ByteBuffer.allocateDirect(1024);
        long addrAtZero = SqeUtils.getBufferAddress(direct);

        direct.position(100);
        long addrAt100 = SqeUtils.getBufferAddress(direct);

        assertEquals(100, addrAt100 - addrAtZero);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBufferAddressHeapBufferThrows() {
        ByteBuffer heap = ByteBuffer.allocate(1024);
        SqeUtils.getBufferAddress(heap);
    }

    @Test
    public void testCanUseDirectlyWithDirectBuffer() {
        ByteBuffer direct = ByteBuffer.allocateDirect(1024);
        assertTrue(SqeUtils.canUseDirectly(direct));
    }

    @Test
    public void testCanUseDirectlyWithHeapBuffer() {
        ByteBuffer heap = ByteBuffer.allocate(1024);
        assertFalse(SqeUtils.canUseDirectly(heap));
    }

    @Test
    public void testCanUseDirectlyWithNull() {
        assertFalse(SqeUtils.canUseDirectly(null));
    }

    @Test
    public void testPrepareStatxSize() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqe = arena.allocate(SqeUtils.SQE_SIZE, 8);

            SqeUtils.prepareStatxSize(sqe, 5, 0x3000L);

            assertEquals(SqeUtils.IORING_OP_STATX, sqe.get(ValueLayout.JAVA_BYTE, SqeUtils.SQE_OPCODE_OFFSET));
            assertEquals(5, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_FD_OFFSET));
            assertEquals(0x3000L, sqe.get(ValueLayout.JAVA_LONG, SqeUtils.SQE_OFF_OFFSET));
            assertEquals(SqeUtils.STATX_SIZE, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_LEN_OFFSET));
            assertEquals(SqeUtils.AT_EMPTY_PATH, sqe.get(ValueLayout.JAVA_INT, SqeUtils.SQE_OP_FLAGS_OFFSET));
        }
    }

    @Test
    public void testGetStatxSize() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment statxBuffer = arena.allocate(SqeUtils.STATX_STRUCT_SIZE, 8);
            statxBuffer.fill((byte) 0);

            long expectedSize = 123456789L;
            statxBuffer.set(ValueLayout.JAVA_LONG, SqeUtils.STATX_SIZE_OFFSET, expectedSize);

            assertEquals(expectedSize, SqeUtils.getStatxSize(statxBuffer));
        }
    }

    @Test
    public void testOpcodeConstants() {
        assertEquals(0, SqeUtils.IORING_OP_NOP);
        assertEquals(1, SqeUtils.IORING_OP_READV);
        assertEquals(2, SqeUtils.IORING_OP_WRITEV);
        assertEquals(3, SqeUtils.IORING_OP_FSYNC);
        assertEquals(19, SqeUtils.IORING_OP_CLOSE);
        assertEquals(22, SqeUtils.IORING_OP_READ);
        assertEquals(23, SqeUtils.IORING_OP_WRITE);
    }

    @Test
    public void testErrnoConstants() {
        assertEquals(2, SqeUtils.ENOENT);
        assertEquals(9, SqeUtils.EBADF);
        assertEquals(11, SqeUtils.EAGAIN);
        assertEquals(13, SqeUtils.EACCES);
        assertEquals(22, SqeUtils.EINVAL);
    }

    @Test
    public void testSqeSize() {
        assertEquals(64, SqeUtils.SQE_SIZE);
    }
}
