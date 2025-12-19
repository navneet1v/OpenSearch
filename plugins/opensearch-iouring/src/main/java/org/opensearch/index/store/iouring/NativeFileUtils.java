/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * Native file operations using Panama FFM API.
 *
 * Provides direct access to POSIX file operations without JNI overhead.
 */
public final class NativeFileUtils {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup STDLIB = Linker.nativeLinker().defaultLookup();

    // open(const char *pathname, int flags) -> int
    private static final MethodHandle OPEN;

    // close(int fd) -> int
    private static final MethodHandle CLOSE;

    // fstat(int fd, struct stat *statbuf) -> int
    private static final MethodHandle FSTAT;

    // Constants
    private static final int O_RDONLY = 0;
    private static final int O_WRONLY = 1;
    private static final int O_RDWR = 2;
    private static final int O_CREAT = 64;
    private static final int O_TRUNC = 512;

    // stat structure offset for st_size (x86_64 Linux)
    private static final long STAT_SIZE = 144;
    private static final long ST_SIZE_OFFSET = 48;

    static {
        try {
            OPEN = LINKER.downcallHandle(
                STDLIB.find("open").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT
                )
            );

            CLOSE = LINKER.downcallHandle(
                STDLIB.find("close").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT
                )
            );

            FSTAT = LINKER.downcallHandle(
                STDLIB.find("fstat").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS
                )
            );
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private NativeFileUtils() {}

    /**
     * Open a file for reading.
     *
     * @param path File path
     * @return File descriptor
     * @throws IOException If open fails
     */
    public static int open(String path) throws IOException {
        return open(path, O_RDONLY);
    }

    /**
     * Open a file with specified flags.
     *
     * @param path File path
     * @param flags Open flags (O_RDONLY, O_WRONLY, O_RDWR, etc.)
     * @return File descriptor
     * @throws IOException If open fails
     */
    public static int open(String path, int flags) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSegment = arena.allocateFrom(path);

            int fd = (int) OPEN.invokeExact(pathSegment, flags);

            if (fd < 0) {
                throw new IOException("Failed to open file: " + path + " (errno=" + (-fd) + ")");
            }

            return fd;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to open file: " + path, t);
        }
    }

    /**
     * Close a file descriptor.
     *
     * @param fd File descriptor
     * @throws IOException If close fails
     */
    public static void close(int fd) throws IOException {
        try {
            int result = (int) CLOSE.invokeExact(fd);

            if (result < 0) {
                throw new IOException("Failed to close fd " + fd + " (errno=" + (-result) + ")");
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to close fd " + fd, t);
        }
    }

    /**
     * Get file size.
     *
     * @param fd File descriptor
     * @return File size in bytes
     * @throws IOException If fstat fails
     */
    public static long fileSize(int fd) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment statBuf = arena.allocate(STAT_SIZE);

            int result = (int) FSTAT.invokeExact(fd, statBuf);

            if (result < 0) {
                throw new IOException("Failed to get file size (errno=" + (-result) + ")");
            }

            return statBuf.get(ValueLayout.JAVA_LONG, ST_SIZE_OFFSET);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to get file size", t);
        }
    }

    /**
     * Open a file for reading and return its size.
     *
     * @param path File path
     * @return Array of [fd, fileSize]
     * @throws IOException If operation fails
     */
    public static long[] openWithSize(String path) throws IOException {
        int fd = open(path);
        try {
            long size = fileSize(fd);
            return new long[] { fd, size };
        } catch (IOException e) {
            close(fd);
            throw e;
        }
    }
}
