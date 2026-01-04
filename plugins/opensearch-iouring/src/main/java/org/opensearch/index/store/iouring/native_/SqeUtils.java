/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.native_;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

/**
 * Static utilities for SQE preparation and buffer handling.
 * Direct mapping to io_uring structures and operations.
 *
 * <p>Thread Safety: All methods are stateless and thread-safe.
 */
public final class SqeUtils {

    private SqeUtils() {
        // Non-instantiable
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SQE Structure Layout (64 bytes total)
    // ═══════════════════════════════════════════════════════════════════════════

    /*
     * struct io_uring_sqe {
     *     __u8    opcode;         // offset 0
     *     __u8    flags;          // offset 1
     *     __u16   ioprio;         // offset 2
     *     __s32   fd;             // offset 4
     *     __u64   off;            // offset 8  (union: off, addr2)
     *     __u64   addr;           // offset 16 (union: addr, splice_off_in)
     *     __u32   len;            // offset 24
     *     __u32   op_flags;       // offset 28 (union: rw_flags, fsync_flags, etc.)
     *     __u64   user_data;      // offset 32
     *     __u16   buf_index;      // offset 40 (union: buf_index, buf_group)
     *     __u16   personality;    // offset 42
     *     __s32   splice_fd_in;   // offset 44 (union: splice_fd_in, file_index)
     *     __u64   addr3;          // offset 48
     *     __u64   __pad2;         // offset 56
     * };
     */

    // SQE field offsets
    public static final long SQE_OPCODE_OFFSET = 0;
    public static final long SQE_FLAGS_OFFSET = 1;
    public static final long SQE_IOPRIO_OFFSET = 2;
    public static final long SQE_FD_OFFSET = 4;
    public static final long SQE_OFF_OFFSET = 8;
    public static final long SQE_ADDR_OFFSET = 16;
    public static final long SQE_LEN_OFFSET = 24;
    public static final long SQE_OP_FLAGS_OFFSET = 28;
    public static final long SQE_USER_DATA_OFFSET = 32;
    public static final long SQE_BUF_INDEX_OFFSET = 40;
    public static final long SQE_PERSONALITY_OFFSET = 42;
    public static final long SQE_SPLICE_FD_IN_OFFSET = 44;
    public static final long SQE_ADDR3_OFFSET = 48;

    public static final int SQE_SIZE = 64;

    // ═══════════════════════════════════════════════════════════════════════════
    // io_uring Opcodes
    // ═══════════════════════════════════════════════════════════════════════════

    public static final byte IORING_OP_NOP = 0;
    public static final byte IORING_OP_READV = 1;
    public static final byte IORING_OP_WRITEV = 2;
    public static final byte IORING_OP_FSYNC = 3;
    public static final byte IORING_OP_READ_FIXED = 4;
    public static final byte IORING_OP_WRITE_FIXED = 5;
    public static final byte IORING_OP_POLL_ADD = 6;
    public static final byte IORING_OP_POLL_REMOVE = 7;
    public static final byte IORING_OP_SYNC_FILE_RANGE = 8;
    public static final byte IORING_OP_SENDMSG = 9;
    public static final byte IORING_OP_RECVMSG = 10;
    public static final byte IORING_OP_TIMEOUT = 11;
    public static final byte IORING_OP_TIMEOUT_REMOVE = 12;
    public static final byte IORING_OP_ACCEPT = 13;
    public static final byte IORING_OP_ASYNC_CANCEL = 14;
    public static final byte IORING_OP_LINK_TIMEOUT = 15;
    public static final byte IORING_OP_CONNECT = 16;
    public static final byte IORING_OP_FALLOCATE = 17;
    public static final byte IORING_OP_OPENAT = 18;
    public static final byte IORING_OP_CLOSE = 19;
    public static final byte IORING_OP_FILES_UPDATE = 20;
    public static final byte IORING_OP_STATX = 21;
    public static final byte IORING_OP_READ = 22;
    public static final byte IORING_OP_WRITE = 23;
    public static final byte IORING_OP_FADVISE = 24;
    public static final byte IORING_OP_MADVISE = 25;
    public static final byte IORING_OP_SEND = 26;
    public static final byte IORING_OP_RECV = 27;
    public static final byte IORING_OP_OPENAT2 = 28;

    // ═══════════════════════════════════════════════════════════════════════════
    // SQE Flags
    // ═══════════════════════════════════════════════════════════════════════════

    public static final byte IOSQE_FIXED_FILE = 1;
    public static final byte IOSQE_IO_DRAIN = 2;
    public static final byte IOSQE_IO_LINK = 4;
    public static final byte IOSQE_IO_HARDLINK = 8;
    public static final byte IOSQE_ASYNC = 16;
    public static final byte IOSQE_BUFFER_SELECT = 32;
    public static final byte IOSQE_CQE_SKIP_SUCCESS = 64;

    // ═══════════════════════════════════════════════════════════════════════════
    // FSYNC Flags
    // ═══════════════════════════════════════════════════════════════════════════

    public static final int IORING_FSYNC_DATASYNC = 1;

    // ═══════════════════════════════════════════════════════════════════════════
    // Statx Constants
    // ═══════════════════════════════════════════════════════════════════════════

    public static final int AT_FDCWD = -100;
    public static final int AT_EMPTY_PATH = 0x1000;
    public static final int AT_STATX_SYNC_AS_STAT = 0x0000;

    public static final int STATX_SIZE = 0x00000200;
    public static final int STATX_BASIC_STATS = 0x000007ff;

    // Statx structure offsets
    public static final long STATX_SIZE_OFFSET = 40;

    public static final int STATX_STRUCT_SIZE = 256;

    // ═══════════════════════════════════════════════════════════════════════════
    // Common errno values
    // ═══════════════════════════════════════════════════════════════════════════

    public static final int EPERM = 1;
    public static final int ENOENT = 2;
    public static final int ESRCH = 3;
    public static final int EINTR = 4;
    public static final int EIO = 5;
    public static final int ENXIO = 6;
    public static final int E2BIG = 7;
    public static final int EBADF = 9;
    public static final int EAGAIN = 11;
    public static final int ENOMEM = 12;
    public static final int EACCES = 13;
    public static final int EFAULT = 14;
    public static final int EBUSY = 16;
    public static final int EEXIST = 17;
    public static final int ENODEV = 19;
    public static final int ENOTDIR = 20;
    public static final int EISDIR = 21;
    public static final int EINVAL = 22;
    public static final int ENFILE = 23;
    public static final int EMFILE = 24;
    public static final int EFBIG = 27;
    public static final int ENOSPC = 28;
    public static final int ESPIPE = 29;
    public static final int EROFS = 30;
    public static final int EPIPE = 32;
    public static final int ENOTEMPTY = 39;
    public static final int ETIME = 62;
    public static final int ECANCELED = 125;

    // ═══════════════════════════════════════════════════════════════════════════
    // SQE Preparation Methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Clears an SQE by zeroing all fields.
     * Should be called before preparing a new operation.
     *
     * @param sqe SQE memory segment to clear
     */
    public static void clear(MemorySegment sqe) {
        sqe.fill((byte) 0);
    }

    /**
     * Prepares a read operation.
     *
     * @param sqe        SQE to prepare
     * @param fd         File descriptor
     * @param bufferAddr Native address of the buffer
     * @param length     Number of bytes to read
     * @param offset     File offset, -1 for current position
     */
    public static void prepareRead(MemorySegment sqe, int fd, long bufferAddr,
                                   int length, long offset) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_READ);
        sqe.set(ValueLayout.JAVA_INT, SQE_FD_OFFSET, fd);
        sqe.set(ValueLayout.JAVA_LONG, SQE_OFF_OFFSET, offset);
        sqe.set(ValueLayout.JAVA_LONG, SQE_ADDR_OFFSET, bufferAddr);
        sqe.set(ValueLayout.JAVA_INT, SQE_LEN_OFFSET, length);
    }

    /**
     * Prepares a write operation.
     *
     * @param sqe        SQE to prepare
     * @param fd         File descriptor
     * @param bufferAddr Native address of the buffer
     * @param length     Number of bytes to write
     * @param offset     File offset, -1 for current position
     */
    public static void prepareWrite(MemorySegment sqe, int fd, long bufferAddr,
                                    int length, long offset) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_WRITE);
        sqe.set(ValueLayout.JAVA_INT, SQE_FD_OFFSET, fd);
        sqe.set(ValueLayout.JAVA_LONG, SQE_OFF_OFFSET, offset);
        sqe.set(ValueLayout.JAVA_LONG, SQE_ADDR_OFFSET, bufferAddr);
        sqe.set(ValueLayout.JAVA_INT, SQE_LEN_OFFSET, length);
    }

    /**
     * Prepares an fsync operation.
     *
     * @param sqe      SQE to prepare
     * @param fd       File descriptor
     * @param datasync true for fdatasync (data only), false for fsync (data + metadata)
     */
    public static void prepareFsync(MemorySegment sqe, int fd, boolean datasync) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_FSYNC);
        sqe.set(ValueLayout.JAVA_INT, SQE_FD_OFFSET, fd);
        if (datasync) {
            sqe.set(ValueLayout.JAVA_INT, SQE_OP_FLAGS_OFFSET, IORING_FSYNC_DATASYNC);
        }
    }

    /**
     * Prepares a close operation.
     *
     * @param sqe SQE to prepare
     * @param fd  File descriptor to close
     */
    public static void prepareClose(MemorySegment sqe, int fd) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_CLOSE);
        sqe.set(ValueLayout.JAVA_INT, SQE_FD_OFFSET, fd);
    }

    /**
     * Prepares a statx operation for querying file metadata.
     *
     * @param sqe         SQE to prepare
     * @param fd          File descriptor
     * @param flags       AT_* flags (use AT_EMPTY_PATH for fd-relative)
     * @param mask        STATX_* mask for requested fields
     * @param statxAddr   Native address of statx buffer (256 bytes)
     * @param pathAddr    Native address of path string (0 for empty path)
     */
    public static void prepareStatx(MemorySegment sqe, int fd, int flags, int mask,
                                    long statxAddr, long pathAddr) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_STATX);
        sqe.set(ValueLayout.JAVA_INT, SQE_FD_OFFSET, fd);
        sqe.set(ValueLayout.JAVA_LONG, SQE_OFF_OFFSET, statxAddr);
        sqe.set(ValueLayout.JAVA_LONG, SQE_ADDR_OFFSET, pathAddr);
        sqe.set(ValueLayout.JAVA_INT, SQE_LEN_OFFSET, mask);
        sqe.set(ValueLayout.JAVA_INT, SQE_OP_FLAGS_OFFSET, flags);
    }

    /**
     * Prepares a statx operation for querying file size by file descriptor.
     * Convenience method that uses AT_EMPTY_PATH with STATX_SIZE mask.
     *
     * @param sqe       SQE to prepare
     * @param fd        File descriptor
     * @param statxAddr Native address of statx buffer (256 bytes)
     */
    public static void prepareStatxSize(MemorySegment sqe, int fd, long statxAddr) {
        prepareStatx(sqe, fd, AT_EMPTY_PATH, STATX_SIZE, statxAddr, 0);
    }

    /**
     * Prepares a NOP operation.
     * Useful for waking up the ring or testing.
     *
     * @param sqe SQE to prepare
     */
    public static void prepareNop(MemorySegment sqe) {
        clear(sqe);
        sqe.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFFSET, IORING_OP_NOP);
    }

    /**
     * Sets the user data field on an SQE.
     * This value is returned in the corresponding CQE for correlation.
     *
     * @param sqe      SQE to modify
     * @param userData User data value
     */
    public static void setUserData(MemorySegment sqe, long userData) {
        sqe.set(ValueLayout.JAVA_LONG, SQE_USER_DATA_OFFSET, userData);
    }

    /**
     * Gets the user data field from an SQE.
     *
     * @param sqe SQE to read from
     * @return User data value
     */
    public static long getUserData(MemorySegment sqe) {
        return sqe.get(ValueLayout.JAVA_LONG, SQE_USER_DATA_OFFSET);
    }

    /**
     * Sets SQE flags.
     *
     * @param sqe   SQE to modify
     * @param flags IOSQE_* flags
     */
    public static void setFlags(MemorySegment sqe, byte flags) {
        sqe.set(ValueLayout.JAVA_BYTE, SQE_FLAGS_OFFSET, flags);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Buffer Utilities
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Gets the native memory address of a ByteBuffer at its current position.
     *
     * The buffer must be backed by native memory (e.g., direct buffer, mapped buffer)
     * because io_uring requires a stable native address to perform I/O.
     *
     * Heap buffers cannot be used because they reside in JVM-managed memory
     * and do not have a stable native address.
     *
     * Note: Alignment is NOT required for io_uring with normal buffered I/O.
     * Alignment only matters when using O_DIRECT.
     *
     * @param buffer ByteBuffer backed by native memory
     * @return Native address at buffer's current position
     * @throws IllegalArgumentException if buffer is a heap buffer
     */
    public static long getBufferAddress(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            return MemorySegment.ofBuffer(buffer).address() + buffer.position();
        }
        throw new IllegalArgumentException(
            "Heap buffers cannot be used with io_uring. " +
                "Buffer must be backed by native memory (direct or mapped) " +
                "to provide a stable address for kernel I/O.");
    }

    /**
     * Checks if a ByteBuffer can be used directly with io_uring.
     *
     * Returns true for buffers backed by native memory (direct, mapped).
     * Returns false for heap buffers which require copying to a direct buffer first.
     *
     * @param buffer ByteBuffer to check
     * @return true if buffer can be used directly with io_uring
     */
    public static boolean canUseDirectly(ByteBuffer buffer) {
        return buffer != null && buffer.isDirect();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Statx Result Parsing
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Extracts file size from a statx result buffer.
     *
     * @param statxBuffer Memory segment containing statx result (256 bytes)
     * @return File size in bytes
     */
    public static long getStatxSize(MemorySegment statxBuffer) {
        return statxBuffer.get(ValueLayout.JAVA_LONG, STATX_SIZE_OFFSET);
    }
}
