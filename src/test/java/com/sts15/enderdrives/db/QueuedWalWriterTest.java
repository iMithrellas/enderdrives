package com.sts15.enderdrives.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class QueuedWalWriterTest {

    @TempDir Path directory;

    @Test
    void batchesBurstsIntoCompatibleOrderedFrames() throws Exception {
        Path path = directory.resolve("items.wal");
        try (QueuedWalWriter writer = QueuedWalWriter.open(path, 60_000, 4 * 1024 * 1024, error -> {})) {
            for (int i = 0; i < 2_000; i++) assertTrue(writer.append(payload(i)));
            writer.flush();
            assertEquals(2_000, writer.stats().durableTransactions());
            assertEquals(0, writer.stats().pendingBytes());
            assertEquals(1, writer.stats().forceCount());
            try (DataInputStream input = new DataInputStream(Files.newInputStream(path))) {
                for (int i = 0; i < 2_000; i++) {
                    int length = input.readInt();
                    byte[] bytes = input.readNBytes(length);
                    assertArrayEquals(payload(i), bytes);
                    CRC32 crc = new CRC32();
                    crc.update(bytes);
                    assertEquals(crc.getValue(), input.readLong());
                }
                assertEquals(-1, input.read());
            }
        }
    }

    @Test
    void idleTailIsSyncedWithoutAnotherMutation() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (QueuedWalWriter writer = new QueuedWalWriter(sink, 10, 4096, error -> {})) {
            assertTrue(writer.append(payload(1)));
            assertTrue(sink.forced.await(3, TimeUnit.SECONDS));
            writer.flush();
            assertEquals(1, writer.stats().durableTransactions());
        }
    }

    @Test
    void largeBatchesSyncBeforeTheirTimer() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4 * 1024 * 1024, error -> {})) {
            assertTrue(writer.append(new byte[600_000]));
            assertTrue(writer.append(new byte[600_000]));
            assertTrue(sink.forced.await(3, TimeUnit.SECONDS));
            writer.flush();
            assertEquals(2, writer.stats().durableTransactions());
        }
    }

    @Test
    void blockedForceDoesNotBlockAdmissionAndInFlightBytesStayBounded() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.releaseForce = new CountDownLatch(1);
        // Each four-byte payload costs 80 accounted bytes; one in flight and one queued fit.
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 160, error -> {});
        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                assertTrue(writer.append(payload(1)));
                var firstBarrier = executor.submit(() -> { writer.flush(); return null; });
                assertTrue(sink.forced.await(3, TimeUnit.SECONDS));
                assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
                    assertTrue(writer.append(payload(2)));
                    assertFalse(writer.append(payload(3)));
                });
                assertEquals(160, writer.stats().pendingBytes());
                assertEquals(1, writer.stats().rejectedTransactions());
                sink.releaseForce.countDown();
                firstBarrier.get(3, TimeUnit.SECONDS);
                // The barrier captures its own sequence, not later admissions.
                assertEquals(1, writer.stats().durableTransactions());
                writer.flush();
                assertEquals(List.of(1, 2), sink.values);
                assertEquals(0, writer.stats().pendingBytes());
                assertTrue(writer.append(payload(3)));
            } finally {
                sink.releaseForce.countDown();
                writer.close();
            }
        }
    }

    @Test
    void oversizedTransactionIsRejectedBeforeAnyWrite() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 100, error -> {})) {
            assertFalse(writer.append(new byte[101]));
            writer.flush();
            assertEquals(0, writer.stats().acceptedTransactions());
            assertEquals(0, writer.stats().forceCount());
            assertTrue(sink.values.isEmpty());
        }
    }

    @Test
    void closeDrainsAndPreventsFurtherAdmissions() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.releaseForce = new CountDownLatch(1);
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4096, error -> {});
        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                assertTrue(writer.append(payload(1)));
                var closing = executor.submit(() -> { writer.close(); return null; });
                assertTrue(sink.forced.await(3, TimeUnit.SECONDS));
                assertFalse(closing.isDone());
                assertThrows(IOException.class, () -> writer.append(payload(2)));
                sink.releaseForce.countDown();
                closing.get(3, TimeUnit.SECONDS);
                assertTrue(sink.closed);
                assertEquals(1, writer.stats().durableTransactions());
                writer.close();
            } finally {
                sink.releaseForce.countDown();
                writer.close();
            }
        }
    }

    @Test
    void appendFailureRetainsPendingStateAndIsNeverRetried() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.appendFailure = true;
        CountDownLatch failed = new CountDownLatch(1);
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4096, error -> failed.countDown());
        assertTrue(writer.append(payload(1)));
        assertTrue(writer.append(payload(2)));
        assertThrows(IOException.class, writer::flush);
        assertTrue(failed.await(3, TimeUnit.SECONDS));
        assertThrows(IOException.class, () -> writer.append(payload(3)));
        assertThrows(IOException.class, writer::close);
        assertEquals(List.of(1), sink.values);
        assertEquals(160, writer.stats().pendingBytes());
        assertEquals(0, writer.stats().durableTransactions());
        assertTrue(sink.closed);
    }

    @Test
    void forceFailureCannotAdvanceDurableBoundary() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.forceFailure = true;
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4096, error -> {});
        assertTrue(writer.append(payload(1)));
        assertThrows(IOException.class, writer::flush);
        assertThrows(IOException.class, writer::close);
        assertEquals(0, writer.stats().durableTransactions());
        assertEquals(80, writer.stats().pendingBytes());
        assertNotNull(writer.failure());
    }

    @Test
    void closeFailureIsReportedToTheOwner() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.closeFailure = true;
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4096, error -> {});
        assertThrows(IOException.class, writer::close);
        assertNotNull(writer.failure());
    }

    @Test
    void interruptedBarrierStillWaitsForDurabilityAndRestoresInterrupt() throws Exception {
        RecordingSink sink = new RecordingSink();
        sink.releaseForce = new CountDownLatch(1);
        QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4096, error -> {});
        CompletableFuture<Boolean> completed = new CompletableFuture<>();
        Thread barrier = new Thread(() -> {
            try {
                writer.flush();
                completed.complete(Thread.currentThread().isInterrupted());
            } catch (IOException e) {
                completed.completeExceptionally(e);
            }
        });
        try {
            assertTrue(writer.append(payload(1)));
            barrier.start();
            assertTrue(sink.forced.await(3, TimeUnit.SECONDS));
            barrier.interrupt();
            sink.releaseForce.countDown();
            assertTrue(completed.get(3, TimeUnit.SECONDS));
            assertEquals(1, writer.stats().durableTransactions());
        } finally {
            sink.releaseForce.countDown();
            barrier.join(3000);
            writer.close();
        }
    }

    @Test
    void concurrentProducersPreserveEachProducersOrder() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (QueuedWalWriter writer = new QueuedWalWriter(sink, 60_000, 4 * 1024 * 1024, error -> {})) {
            try (var executor = Executors.newFixedThreadPool(4)) {
                List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
                for (int producer = 0; producer < 4; producer++) {
                    int id = producer;
                    futures.add(executor.submit(() -> {
                        for (int i = 0; i < 500; i++) {
                            assertTrue(writer.append(payload(id * 1000 + i)));
                        }
                        return null;
                    }));
                }
                for (var future : futures) future.get(3, TimeUnit.SECONDS);
            }
            writer.flush();
            int[] next = new int[4];
            for (int value : sink.values) assertEquals(next[value / 1000]++, value % 1000);
            assertArrayEquals(new int[]{500, 500, 500, 500}, next);
        }
    }

    @Test
    void closeBarrierAllowsCheckpointTruncationAndANewGeneration() throws Exception {
        Path path = directory.resolve("items.wal");
        try (QueuedWalWriter writer = QueuedWalWriter.open(path, 60_000, 4096, error -> {})) {
            assertTrue(writer.append(payload(1)));
            writer.flush();
            assertEquals(16, Files.size(path));
        }
        // A checkpoint can only truncate after close has returned ownership of the file.
        try (var file = new java.io.RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(0);
            file.getFD().sync();
        }
        try (QueuedWalWriter writer = QueuedWalWriter.open(path, 60_000, 4096, error -> {})) {
            assertTrue(writer.append(payload(2)));
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(path))) {
            assertEquals(4, input.readInt());
            assertEquals(2, input.readInt());
            input.readLong();
            assertEquals(-1, input.read());
        }
    }

    private static byte[] payload(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static class RecordingSink implements QueuedWalWriter.Sink {
        final List<Integer> values = new ArrayList<>();
        final CountDownLatch forced = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(0);
        boolean appendFailure;
        boolean forceFailure;
        boolean closeFailure;
        volatile boolean closed;

        @Override
        public void append(byte[] payload) throws IOException {
            values.add(ByteBuffer.wrap(payload).getInt());
            if (appendFailure) throw new IOException("Injected partial write");
        }

        @Override
        public void force() throws IOException {
            forced.countDown();
            try {
                if (!releaseForce.await(5, TimeUnit.SECONDS)) throw new IOException("Timed out waiting for test disk");
            } catch (InterruptedException e) {
                throw new IOException("Writer was interrupted during I/O", e);
            }
            if (forceFailure) throw new IOException("Injected force failure");
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeFailure) throw new IOException("Injected close failure");
        }
    }
}
