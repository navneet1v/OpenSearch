package org.opensearch.index.store.iouring;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Synchronous Lucene IndexInput backed by async io_uring.
 */
final class IOUringIndexInput extends BufferedIndexInput {

    private final IOUringScheduler scheduler;
    private final int fd;
    private final long length;
    private boolean isClone=false;

    private long offset;

    IOUringIndexInput(
            String resourceDesc,
            IOUringScheduler scheduler,
            int fd,
            long length
    ) {
        super(resourceDesc);
        this.scheduler = scheduler;
        this.fd = fd;
        this.length = length;
        this.offset = 0;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        long pos = getFilePointer() + offset;

        if (pos + b.remaining() > length) {
            throw new EOFException(
                    "Read past EOF: pos=" + pos + " len=" + b.remaining());
        }

        int toReadBytes = b.remaining();
        int bytesRead = 0;
        long reqId;
        try (Arena arena = Arena.ofConfined()) {
            while (toReadBytes > 0) {
                System.out.println("readInternal: " + toReadBytes);
                int chunk = Math.min(toReadBytes, getBufferSize());
                b.limit(b.position() + chunk);
                MemorySegment buffer = arena.allocate(chunk, getBufferSize());
                reqId = scheduler.submitRead(
                    fd,
                    buffer,
                    chunk,
                    pos
                );
                bytesRead = scheduler.waitForCompletion(reqId);
                if (bytesRead <= 0) {
                    throw new EOFException("Unexpected EOF");
                }
                copyNativeToByteBuffer(buffer, bytesRead, b);
                toReadBytes -= bytesRead;
                pos += bytesRead;
            }
        }
    }

    /**
     * Copy data from native MemorySegment to ByteBuffer.
     * Handles both heap and direct ByteBuffers.
     */
    private void copyNativeToByteBuffer(MemorySegment source,
                                        int length,
                                        ByteBuffer dest) {
        // Create a segment view of the destination buffer
        // Works for both heap and direct ByteBuffers
        MemorySegment destSegment = MemorySegment.ofBuffer(dest);

        // Copy from native source to destination
        // MemorySegment.copy handles heap vs native destination transparently
        MemorySegment.copy(
            source, 0,              // source segment, source offset
            destSegment, 0,         // dest segment, dest offset (relative to buffer position)
            length                  // number of bytes
        );
        dest.position(dest.position() + length);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String desc, long offset, long sliceLength)
            throws IOException {

        if (offset < 0 || sliceLength < 0 || offset + sliceLength > length) {
            throw new IllegalArgumentException("Invalid slice");
        }

        IOUringIndexInput slice =
                new IOUringIndexInput(
                        desc,
                        scheduler,
                        fd,
                        sliceLength
                );
        slice.seek(this.offset + offset);
        return slice;
    }

    @Override
    public BufferedIndexInput clone() {
        IOUringIndexInput clone = (IOUringIndexInput) super.clone();
        clone.isClone = true;
        return clone;
    }

    @Override
    public void close() {
        if (!isClone) {
            try {
                PosixFD.close(fd);
                scheduler.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
