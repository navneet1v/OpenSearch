package org.opensearch.index.store.iouring;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.FunctionDescriptor;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.*;

/**
 * Minimal POSIX FD helpers using Panama.
 */
final class PosixFD {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC =
            SymbolLookup.loaderLookup();

    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;

    static {
        try {
            OPEN = LINKER.downcallHandle(
                    LIBC.find("open").orElseThrow(),
                    FunctionDescriptor.of(
                            JAVA_INT,
                            ADDRESS,
                            JAVA_INT
                    )
            );
            CLOSE = LINKER.downcallHandle(
                    LIBC.find("close").orElseThrow(),
                    FunctionDescriptor.of(
                            JAVA_INT,
                            JAVA_INT
                    )
            );
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    private PosixFD() {}

    static int openReadOnly(Path path) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            var cPath = arena.allocateFrom(path.toString());
            int fd = (int) OPEN.invoke(cPath, 0 /* O_RDONLY */);
            if (fd < 0) {
                throw new IOException("open failed for " + path);
            }
            return fd;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    static void close(int fd) throws IOException {
        try {
            CLOSE.invoke(fd);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }
}
