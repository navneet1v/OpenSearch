/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.api;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opensearch.index.store.iouring.core.BufferPool;
import org.opensearch.index.store.iouring.core.IoUringRing;
import org.opensearch.index.store.iouring.native_.NativeRing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for IoUringFileChannel.
 * These tests require Linux with io_uring support.
 */
public class IoUringFileChannelIntegrationTests {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Path testFile;

    @BeforeClass
    public static void checkPlatform() {
        assumeTrue("Skipping: io_uring not available", NativeRing.isAvailable());
    }

    @AfterClass
    public static void cleanup() {
        IoUringRing.reset();
        BufferPool.reset();
    }

    @Before
    public void setUp() throws IOException {
        testFile = tempFolder.newFile("test.dat").toPath();
    }

    @After
    public void tearDown() {
        // Temp folder handles cleanup
    }

    @Test
    public void testOpenReadOnly() throws IOException {
        Files.writeString(testFile, "test content");

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            assertTrue(channel.isOpen());
            assertEquals(0, channel.position());
        }
    }

    @Test
    public void testOpenWriteOnly() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            assertTrue(channel.isOpen());
        }
    }

    @Test
    public void testOpenReadWrite() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            assertTrue(channel.isOpen());
        }
    }

    @Test
    public void testReadWriteRoundTrip() throws IOException {
        byte[] testData = "Hello, io_uring!".getBytes();

        // Write
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.wrap(testData);
            int written = channel.write(buf);
            assertEquals(testData.length, written);
        }

        // Read back
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(testData.length);
            int read = channel.read(buf);
            assertEquals(testData.length, read);
            buf.flip();
            byte[] result = new byte[read];
            buf.get(result);
            assertArrayEquals(testData, result);
        }
    }

    @Test
    public void testReadWithDirectBuffer() throws IOException {
        byte[] testData = "Direct buffer test".getBytes();
        Files.write(testFile, testData);

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer direct = ByteBuffer.allocateDirect(testData.length);
            int read = channel.read(direct);

            assertEquals(testData.length, read);
            direct.flip();
            byte[] result = new byte[read];
            direct.get(result);
            assertArrayEquals(testData, result);
        }
    }

    @Test
    public void testReadWithHeapBuffer() throws IOException {
        byte[] testData = "Heap buffer test".getBytes();
        Files.write(testFile, testData);

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer heap = ByteBuffer.allocate(testData.length);
            int read = channel.read(heap);

            assertEquals(testData.length, read);
            heap.flip();
            byte[] result = new byte[read];
            heap.get(result);
            assertArrayEquals(testData, result);
        }
    }

    @Test
    public void testPositionedRead() throws IOException {
        Files.writeString(testFile, "0123456789");

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(3);
            int read = channel.read(buf, 5);

            assertEquals(3, read);
            buf.flip();
            byte[] result = new byte[3];
            buf.get(result);
            assertEquals("567", new String(result));

            // Position should be unchanged
            assertEquals(0, channel.position());
        }
    }

    @Test
    public void testPositionedWrite() throws IOException {
        Files.writeString(testFile, "0123456789");

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap("XXX".getBytes());
            int written = channel.write(buf, 3);

            assertEquals(3, written);
            assertEquals(0, channel.position());
        }

        String content = Files.readString(testFile);
        assertEquals("012XXX6789", content);
    }

    @Test
    public void testPositionManagement() throws IOException {
        Files.writeString(testFile, "0123456789");

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            assertEquals(0, channel.position());

            channel.position(5);
            assertEquals(5, channel.position());

            ByteBuffer buf = ByteBuffer.allocate(3);
            channel.read(buf);
            assertEquals(8, channel.position());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePositionThrows() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            channel.position(-1);
        }
    }

    @Test
    public void testSize() throws IOException {
        byte[] data = new byte[1024];
        new Random().nextBytes(data);
        Files.write(testFile, data);

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            assertEquals(1024, channel.size());
        }
    }

    @Test
    public void testForce() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.wrap("test".getBytes());
            channel.write(buf);
            channel.force(true);  // Should not throw
            channel.force(false); // Should not throw
        }
    }

    @Test(expected = NonReadableChannelException.class)
    public void testReadFromWriteOnlyThrows() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocate(10);
            channel.read(buf);
        }
    }

    @Test(expected = NonWritableChannelException.class)
    public void testWriteToReadOnlyThrows() throws IOException {
        Files.writeString(testFile, "test");
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.wrap("data".getBytes());
            channel.write(buf);
        }
    }

    @Test(expected = ClosedChannelException.class)
    public void testReadAfterCloseThrows() throws IOException {
        Files.writeString(testFile, "test");
        IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ);
        channel.close();

        ByteBuffer buf = ByteBuffer.allocate(10);
        channel.read(buf);
    }

    @Test
    public void testScatterRead() throws IOException {
        Files.writeString(testFile, "AAABBBCCC");

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer[] bufs = {
                ByteBuffer.allocate(3),
                ByteBuffer.allocate(3),
                ByteBuffer.allocate(3)
            };

            long read = channel.read(bufs, 0, 3);
            assertEquals(9, read);

            bufs[0].flip();
            bufs[1].flip();
            bufs[2].flip();

            byte[] b0 = new byte[3];
            byte[] b1 = new byte[3];
            byte[] b2 = new byte[3];
            bufs[0].get(b0);
            bufs[1].get(b1);
            bufs[2].get(b2);

            assertEquals("AAA", new String(b0));
            assertEquals("BBB", new String(b1));
            assertEquals("CCC", new String(b2));
        }
    }

    @Test
    public void testGatherWrite() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer[] bufs = {
                ByteBuffer.wrap("AAA".getBytes()),
                ByteBuffer.wrap("BBB".getBytes()),
                ByteBuffer.wrap("CCC".getBytes())
            };

            long written = channel.write(bufs, 0, 3);
            assertEquals(9, written);
        }

        String content = Files.readString(testFile);
        assertEquals("AAABBBCCC", content);
    }

    @Test
    public void testLargeFile() throws IOException {
        int size = 1024 * 1024; // 1MB
        byte[] data = new byte[size];
        new Random(42).nextBytes(data);

        // Write
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }

        // Read and verify
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            while (buf.hasRemaining()) {
                int read = channel.read(buf);
                if (read == -1) break;
            }
            buf.flip();
            byte[] result = new byte[size];
            buf.get(result);
            assertArrayEquals(data, result);
        }
    }

    @Test
    public void testConcurrentReads() throws Exception {
        // Prepare file
        byte[] data = new byte[4096];
        new Random(42).nextBytes(data);
        Files.write(testFile, data);

        int threadCount = 4;
        int readsPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        Random rand = new Random();
                        for (int i = 0; i < readsPerThread; i++) {
                            int pos = rand.nextInt(data.length - 100);
                            ByteBuffer buf = ByteBuffer.allocate(100);
                            int read = channel.read(buf, pos);
                            if (read != 100) {
                                errors.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(0, errors.get());
        }

        executor.shutdown();
    }

    @Test
    public void testGetPath() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            assertEquals(testFile, channel.getPath());
        }
    }

    @Test
    public void testGetFd() throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            assertTrue(channel.getFd() > 0);
        }
    }
}
