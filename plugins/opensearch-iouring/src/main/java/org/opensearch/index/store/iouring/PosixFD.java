/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import java.lang.invoke.MethodHandle;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.Set;

/**
 * POSIX file descriptor operations using Panama FFM.
 */
public final class PosixFD {

    // POSIX open flags (Linux)
    private static final int O_RDONLY = 0;
    private static final int O_WRONLY = 1;
    private static final int O_RDWR = 2;
    private static final int O_CREAT = 0100;
    private static final int O_TRUNC = 01000;
    private static final int O_APPEND = 02000;
    private static final int O_DSYNC = 010000;
    private static final int O_SYNC = 04010000;
    private static final int O_DIRECT = 040000;

    private static final int DEFAULT_MODE = 0644;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC = LINKER.defaultLookup();

    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;
    private static final MethodHandle STRERROR;

    static {
        try {
            OPEN = LINKER.downcallHandle(
                LIBC.find("open").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)
            );
            CLOSE = LINKER.downcallHandle(
                LIBC.find("close").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, JAVA_INT)
            );
            STRERROR = LINKER.downcallHandle(
                LIBC.find("strerror").orElseThrow(),
                FunctionDescriptor.of(ADDRESS, JAVA_INT)
            );
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    private PosixFD() {}

    /**
     * Opens a file with the specified options.
     */
    public static int open(Path path, Set<? extends OpenOption> options) throws IOException {
        int flags = toFlags(options);
        String pathStr = path.toAbsolutePath().toString();
        
        // Check if file exists first for better error messages
        java.io.File file = new java.io.File(pathStr);
        if (!file.exists() && !options.contains(StandardOpenOption.CREATE) && !options.contains(StandardOpenOption.CREATE_NEW)) {
            throw new IOException("File does not exist: " + path);
        }
        if (file.exists() && !file.canRead()) {
            throw new IOException("Cannot read file (permission denied): " + path);
        }
        
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateFrom(pathStr);
            int fd = (int) OPEN.invokeExact(cPath, flags, DEFAULT_MODE);
            if (fd < 0) {
                throw new IOException("open() returned " + fd + " for " + path + " (flags=0x" + Integer.toHexString(flags) + ")");
            }
            return fd;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("open failed for " + path, t);
        }
    }

    /**
     * Closes a file descriptor.
     */
    public static void close(int fd) throws IOException {
        try {
            int result = (int) CLOSE.invokeExact(fd);
            if (result < 0) {
                throw new IOException("close failed for fd " + fd);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("close failed", t);
        }
    }

    private static String getErrorMessage(int errno) {
        try {
            MemorySegment errStr = (MemorySegment) STRERROR.invokeExact(errno);
            return errStr.getString(0);
        } catch (Throwable t) {
            return switch (errno) {
                case 2 -> "ENOENT (No such file or directory)";
                case 13 -> "EACCES (Permission denied)";
                case 17 -> "EEXIST (File exists)";
                case 21 -> "EISDIR (Is a directory)";
                case 22 -> "EINVAL (Invalid argument)";
                case 28 -> "ENOSPC (No space left on device)";
                default -> "Unknown error (" + errno + ")";
            };
        }
    }

    private static int toFlags(Set<? extends OpenOption> options) {
        boolean read = options.contains(StandardOpenOption.READ);
        boolean write = options.contains(StandardOpenOption.WRITE);
        boolean append = options.contains(StandardOpenOption.APPEND);

        if (!read && !write && !append) {
            read = true;
        }
        if (append) {
            write = true;
        }

        int flags = (read && write) ? O_RDWR : (write ? O_WRONLY : O_RDONLY);

        if (options.contains(StandardOpenOption.CREATE) || options.contains(StandardOpenOption.CREATE_NEW)) {
            flags |= O_CREAT;
        }
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING) && write) {
            flags |= O_TRUNC;
        }
        if (append) {
            flags |= O_APPEND;
        }
        if (options.contains(StandardOpenOption.SYNC)) {
            flags |= O_SYNC;
        }
        if (options.contains(StandardOpenOption.DSYNC)) {
            flags |= O_DSYNC;
        }
        boolean hasDirect = options.stream().anyMatch(option -> option.toString().equalsIgnoreCase("DIRECT"));
        if (hasDirect) {
            flags |= O_DIRECT;
        }
        return flags;
    }
}
