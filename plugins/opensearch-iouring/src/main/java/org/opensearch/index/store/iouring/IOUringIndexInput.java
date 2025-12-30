package org.opensearch.index.store.iouring;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
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
        while (toReadBytes > 0) {
            int chunk = Math.min(toReadBytes, getBufferSize());
            b.limit(b.position() + chunk);
            MemorySegment buffer = MemorySegment.ofBuffer(b);
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
            toReadBytes -= bytesRead;
            pos += bytesRead;
        }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    public long length() {
        return length - offset;
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
