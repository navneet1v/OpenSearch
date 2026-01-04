/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.store.iouring;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.iouring.api.IoUringFileChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FileChannelBenchmark {

    @Param({"4096", "65536", "262144"})
    private int bufferSize;

    @Param({"1048576", "10485760"})
    private int fileSize;

    private Path testFile;
    private byte[] testData;
    private Random random;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        testFile = Files.createTempFile("benchmark", ".dat");
        testData = new byte[fileSize];
        random = new Random(42);
        random.nextBytes(testData);
        Files.write(testFile, testData);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        Files.deleteIfExists(testFile);
    }

    @Benchmark
    public void sequentialReadJavaFileChannel(Blackhole bh) throws IOException {
        try (FileChannel channel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            while (channel.read(buf) != -1) {
                buf.flip();
                bh.consume(buf);
                buf.clear();
            }
        }
    }

    @Benchmark
    public void sequentialReadIoUring(Blackhole bh) throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            while (channel.read(buf) != -1) {
                buf.flip();
                bh.consume(buf);
                buf.clear();
            }
        }
    }

    @Benchmark
    public void randomReadJavaFileChannel(Blackhole bh) throws IOException {
        try (FileChannel channel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            for (int i = 0; i < 100; i++) {
                long pos = random.nextInt(fileSize - bufferSize);
                buf.clear();
                channel.read(buf, pos);
                buf.flip();
                bh.consume(buf);
            }
        }
    }

    @Benchmark
    public void randomReadIoUring(Blackhole bh) throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(testFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            for (int i = 0; i < 100; i++) {
                long pos = random.nextInt(fileSize - bufferSize);
                buf.clear();
                channel.read(buf, pos);
                buf.flip();
                bh.consume(buf);
            }
        }
    }

    @Benchmark
    public void sequentialWriteJavaFileChannel(Blackhole bh) throws IOException {
        Path writeFile = Files.createTempFile("write_bench", ".dat");
        try (FileChannel channel = FileChannel.open(writeFile,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            int written = 0;
            while (written < fileSize) {
                buf.clear();
                int toWrite = Math.min(bufferSize, fileSize - written);
                buf.limit(toWrite);
                written += channel.write(buf);
            }
            bh.consume(written);
        } finally {
            Files.deleteIfExists(writeFile);
        }
    }

    @Benchmark
    public void sequentialWriteIoUring(Blackhole bh) throws IOException {
        Path writeFile = Files.createTempFile("write_bench", ".dat");
        try (IoUringFileChannel channel = IoUringFileChannel.open(writeFile,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
            int written = 0;
            while (written < fileSize) {
                buf.clear();
                int toWrite = Math.min(bufferSize, fileSize - written);
                buf.limit(toWrite);
                written += channel.write(buf);
            }
            bh.consume(written);
        } finally {
            Files.deleteIfExists(writeFile);
        }
    }
}
