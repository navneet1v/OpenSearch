package org.opensearch.index.store.iouring;

import java.lang.foreign.*;
import java.lang.invoke.*;
import static java.lang.foreign.ValueLayout.*;

import org.opensearch.index.store.iouring.ffi.opensearch_iouring_h;

public final class IOUringNative {

    static {
        System.loadLibrary("opensearch_iouring");
    }

    private IOUringNative() {}

    public static MemorySegment createRing(int depth) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            int rc = opensearch_iouring_h.osur_ring_create(depth, Osur.OSUR_RING_DEFAULT(), out);
            if (rc != 0) {
                throw new IllegalStateException("ring_create failed: " + rc);
            }
            return out.get(ADDRESS, 0);
        }
    }

    public static void destroyRing(MemorySegment ring) {
        opensearch_iouring_h.osur_ring_destroy(ring);
    }
}
