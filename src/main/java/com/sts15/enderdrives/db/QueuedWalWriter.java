package com.sts15.enderdrives.db;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * Single-owner, bounded writer for length/payload/CRC WAL frames. Admission never performs I/O.
 * A successful append transfers ownership of the payload to this writer; callers must not modify it.
 * Queued and in-flight records count against the budget until their force succeeds.
 */
public final class QueuedWalWriter implements AutoCloseable {

    private static final int BATCH_BYTES = 1024 * 1024;
    private static final int RECORD_OVERHEAD = Integer.BYTES + Long.BYTES + 64;

    private final Object lock = new Object();
    private final ArrayDeque<PendingRecord> queue = new ArrayDeque<>();
    private final Sink sink;
    private final long intervalNanos;
    private final long maxPendingBytes;
    private final Consumer<IOException> failureHandler;
    private final Thread thread;
    // Retain the unsuccessful batch as well as the queue on failure, until the owner releases us.
    private List<PendingRecord> inFlight = List.of();
    private long acceptedSequence;
    private long durableSequence;
    private long forceThrough;
    private long queuedBytes;
    private long pendingBytes;
    private long highWaterBytes;
    private long rejectedTransactions;
    private long forceCount;
    private long forceNanos;
    private boolean closing;
    private boolean terminated;
    private volatile IOException failure;

    public static QueuedWalWriter open(
            Path path, long intervalMillis, long maxPendingBytes, Consumer<IOException> failureHandler
    ) throws IOException {
        validateSettings(intervalMillis, maxPendingBytes);
        return new QueuedWalWriter(new FileSink(path), intervalMillis, maxPendingBytes, failureHandler);
    }

    QueuedWalWriter(Sink sink, long intervalMillis, long maxPendingBytes, Consumer<IOException> failureHandler) {
        validateSettings(intervalMillis, maxPendingBytes);
        this.sink = sink;
        this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        this.maxPendingBytes = maxPendingBytes;
        this.failureHandler = failureHandler;
        thread = new Thread(this::run, "EnderDB-WalWriter");
        thread.setDaemon(true);
        thread.start();
    }

    private static void validateSettings(long intervalMillis, long maxPendingBytes) {
        if (intervalMillis <= 0 || maxPendingBytes <= RECORD_OVERHEAD) {
            throw new IllegalArgumentException("WAL interval and queue budget must be positive");
        }
    }

    /** Returns false on saturation, before the caller publishes any corresponding RAM mutation. */
    public boolean append(byte[] payload) throws IOException {
        if (payload.length == 0) throw new IllegalArgumentException("Empty WAL transaction");
        synchronized (lock) {
            checkFailure();
            if (closing) throw new IOException("Item WAL writer is closing");
            long cost = (long) payload.length + RECORD_OVERHEAD;
            if (cost > maxPendingBytes - pendingBytes) {
                rejectedTransactions++;
                return false;
            }
            queue.addLast(new PendingRecord(++acceptedSequence, payload, System.nanoTime()));
            pendingBytes += cost;
            queuedBytes += cost;
            highWaterBytes = Math.max(highWaterBytes, pendingBytes);
            if (queue.size() == 1 || queuedBytes >= BATCH_BYTES) lock.notifyAll();
            return true;
        }
    }

    /**
     * Durability barrier for all transactions admitted before this call. Interruptions are restored
     * after completion: abandoning an admitted transaction would make its outcome ambiguous.
     */
    public void flush() throws IOException {
        boolean interrupted = false;
        try {
            synchronized (lock) {
                long target = acceptedSequence;
                forceThrough = Math.max(forceThrough, target);
                lock.notifyAll();
                while (durableSequence < target) {
                    checkFailure();
                    if (terminated) throw new IOException("Item WAL writer stopped before the durability barrier");
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                checkFailure();
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public IOException failure() {
        return failure;
    }

    public Stats stats() {
        synchronized (lock) {
            return new Stats(acceptedSequence, durableSequence, pendingBytes, highWaterBytes,
                    rejectedTransactions, forceCount, forceNanos);
        }
    }

    /** Drain, force, and close before returning. Never interrupt a thread doing channel I/O. */
    @Override
    public void close() throws IOException {
        synchronized (lock) {
            closing = true;
            lock.notifyAll();
        }
        boolean interrupted = false;
        try {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            checkFailure();
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            while (true) {
                List<PendingRecord> batch;
                synchronized (lock) {
                    while (true) {
                        if (queue.isEmpty()) {
                            if (closing) return;
                            lock.wait();
                            continue;
                        }
                        long remaining = intervalNanos - (System.nanoTime() - queue.getFirst().queuedAtNanos());
                        if (!closing && forceThrough <= durableSequence && queuedBytes < BATCH_BYTES && remaining > 0) {
                            TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                            continue;
                        }
                        break;
                    }
                    batch = new ArrayList<>();
                    long batchBytes = 0;
                    do {
                        PendingRecord record = queue.removeFirst();
                        batch.add(record);
                        batchBytes += record.cost();
                        queuedBytes -= record.cost();
                    } while (!queue.isEmpty() && batchBytes < BATCH_BYTES);
                    inFlight = batch;
                }

                // The state/queue lock is never held during file writes or forces.
                for (PendingRecord record : batch) sink.append(record.payload());
                long started = System.nanoTime();
                sink.force();
                long elapsed = System.nanoTime() - started;
                synchronized (lock) {
                    for (PendingRecord record : batch) pendingBytes -= record.cost();
                    durableSequence = batch.getLast().sequence();
                    inFlight = List.of();
                    forceCount++;
                    forceNanos += elapsed;
                    lock.notifyAll();
                }
            }
        } catch (Throwable error) {
            failure = error instanceof IOException io ? io : new IOException("Item WAL writer failed", error);
            if (error instanceof Error fatal) throw fatal;
        } finally {
            try {
                sink.close();
            } catch (IOException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            } finally {
                synchronized (lock) {
                    terminated = true;
                    lock.notifyAll();
                }
                if (failure != null) failureHandler.accept(failure);
            }
        }
    }

    private void checkFailure() throws IOException {
        if (failure != null) throw new IOException("Item WAL writer has failed", failure);
    }

    public record Stats(long acceptedTransactions, long durableTransactions, long pendingBytes,
                        long highWaterBytes, long rejectedTransactions, long forceCount, long forceNanos) {}

    private record PendingRecord(long sequence, byte[] payload, long queuedAtNanos) {
        long cost() { return (long) payload.length + RECORD_OVERHEAD; }
    }

    // An injectable sink allows deterministic slow-disk, partial-write, and force-failure tests.
    interface Sink extends AutoCloseable {
        void append(byte[] payload) throws IOException;
        void force() throws IOException;
        @Override void close() throws IOException;
    }

    private static final class FileSink implements Sink {
        private final FileOutputStream file;
        private final DataOutputStream output;

        private FileSink(Path path) throws IOException {
            file = new FileOutputStream(path.toFile(), true);
            output = new DataOutputStream(new BufferedOutputStream(file, 512 * 1024));
        }

        @Override
        public void append(byte[] payload) throws IOException {
            CRC32 crc = new CRC32();
            crc.update(payload);
            output.writeInt(payload.length);
            output.write(payload);
            output.writeLong(crc.getValue());
        }

        @Override
        public void force() throws IOException {
            output.flush();
            file.getChannel().force(false);
        }

        @Override
        public void close() throws IOException {
            // Do not retry buffered writes after a partial write or force failure.
            file.close();
        }
    }
}
