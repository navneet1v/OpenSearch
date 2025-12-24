package org.opensearch.index.store.iouring;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Synchronous Lucene IndexInput backed by async io_uring.
 */
final class IOUringIndexInput extends IndexInput {

    private final IOUringScheduler scheduler;
    private final int fd;
    private final long length;

    private long position;

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
        this.position = 0;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len)
            throws IOException {

        if (position + len > length) {
            throw new EOFException(
                    "Read past EOF: pos=" + position + " len=" + len);
        }

        int remaining = len;
        int dstOffset = offset;

        try (Arena arena = Arena.ofConfined()) {
            while (remaining > 0) {
                int chunk = Math.min(remaining, 128 * 1024); // 128KB max

                MemorySegment buffer =
                        arena.allocate(JAVA_BYTE, chunk);

                long reqId = scheduler.submitRead(
                        fd,
                        buffer,
                        chunk,
                        position
                );

                int bytesRead = scheduler.waitForCompletion(reqId);
                if (bytesRead <= 0) {
                    throw new EOFException("Unexpected EOF");
                }

                buffer.asByteBuffer()
                        .get(b, dstOffset, bytesRead);

                position += bytesRead;
                dstOffset += bytesRead;
                remaining -= bytesRead;
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        byte[] one = new byte[1];
        readBytes(one, 0, 1);
        return one[0];
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) {
        if (pos < 0 || pos > length) {
            throw new IllegalArgumentException("Invalid seek: " + pos);
        }
        this.position = pos;
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
                        offset + sliceLength
                );
        slice.seek(offset);
        return slice;
    }

    @Override
    public void close() {
        // fd lifecycle managed by Directory
    }
}
