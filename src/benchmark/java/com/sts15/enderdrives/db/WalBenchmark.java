package com.sts15.enderdrives.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

/** Standalone WAL benchmark. Measures admission latency AND completion through a final disk force. */
public final class WalBenchmark {

    private static final int TRANSACTION_MAGIC = 0x45445732;
    private static final int KEY_COUNT = 256;
    private static final long TICK_NANOS = 50_000_000L;

    private WalBenchmark() {}

    public static void main(String[] args) throws Exception {
        if (args.length > 8) {
            throw new IllegalArgumentException("Usage: WalBenchmark [directory mutationsPerTick ticks keyBytes syncMs rounds paced queueMiB]");
        }
        Path parent = Path.of(args.length > 0 ? args[0] : "build/wal-benchmark");
        int mutations = positive(args, 1, 500);
        int ticks = positive(args, 2, 100);
        int keyBytes = positive(args, 3, 128);
        int syncMs = positive(args, 4, 250);
        int rounds = positive(args, 5, 3);
        boolean paced = args.length <= 6 || Boolean.parseBoolean(args[6]);
        long queueBytes = positive(args, 7, 64) * 1024L * 1024L;
        if (keyBytes < Integer.BYTES || keyBytes > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("keyBytes must be between 4 and 16777216");
        }
        Math.multiplyExact(mutations, ticks);
        Files.createDirectories(parent);
        Path directory = Files.createTempDirectory(parent, "run-");
        var store = Files.getFileStore(directory);
        System.out.printf("WAL benchmark: Java %s, %s %s, filesystem=%s (%s)%n",
                System.getProperty("java.version"), System.getProperty("os.name"),
                System.getProperty("os.arch"), store.type(), store.name());
        System.out.printf("Directory: %s%nmutations/tick=%d ticks=%d keyBytes=%d syncMs=%d rounds=%d paced=%s queueMiB=%d%n",
                directory.toAbsolutePath(), mutations, ticks, keyBytes, syncMs, rounds, paced, queueBytes / (1024 * 1024));
        System.out.println("WAL-only workload: serialization + append/admission. Tick times exclude pacing sleeps.");
        System.out.println("Durable elapsed time includes pacing and the final drain; no Minecraft, AE2 or checkpoint work is simulated.");

        byte[][] keys = new byte[KEY_COUNT][keyBytes];
        Random random = new Random(42);
        for (int i = 0; i < keys.length; i++) {
            random.nextBytes(keys[i]);
            ByteBuffer.wrap(keys[i]).putInt(i);
        }

        // Warm both code paths before sampling. Separate files keep every measured trial comparable.
        run(directory.resolve("warmup-sync.wal"), false, Math.min(mutations, 100), 4, keys, syncMs, queueBytes, false);
        run(directory.resolve("warmup-queued.wal"), true, Math.min(mutations, 100), 4, keys, syncMs, queueBytes, false);
        String header = "round,mode,transactions,tick_p50_ms,tick_p95_ms,tick_p99_ms,tick_max_ms,ticks_over_50ms,submit_ms,drain_ms,durable_elapsed_ms,durable_ops_per_second,forces,peak_pending_bytes,rejected";
        StringBuilder csv = new StringBuilder(header).append('\n');
        System.out.println(header);
        for (int round = 1; round <= rounds; round++) {
            // Alternate order to reduce warm-cache and run-order bias.
            for (int trial = 0; trial < 2; trial++) {
                boolean queued = (round + trial) % 2 == 0;
                String mode = queued ? "queued" : "synchronous";
                Path path = directory.resolve(round + "-" + mode + ".wal");
                Result result = run(path, queued, mutations, ticks, keys, syncMs, queueBytes, paced);
                String row = result.csv(round, mode);
                csv.append(row).append('\n');
                System.out.println(row);
            }
            if (Files.mismatch(directory.resolve(round + "-synchronous.wal"),
                    directory.resolve(round + "-queued.wal")) != -1) {
                throw new IOException("WAL output differs between modes in round " + round);
            }
        }
        Path results = directory.resolve("results.csv");
        Files.writeString(results, csv);
        System.out.println("Verified every WAL frame and identical output between modes. Results: " + results.toAbsolutePath());
    }

    private static Result run(Path path, boolean queued, int mutations, int ticks, byte[][] keys,
                              int syncMs, long queueBytes, boolean paced) throws IOException {
        long[] tickTimes = new long[ticks];
        long started;
        long drainStarted;
        long completed;
        long forces;
        long peak = 0;
        long rejected = 0;
        int operations = Math.multiplyExact(mutations, ticks);
        if (queued) {
            try (QueuedWalWriter writer = QueuedWalWriter.open(path, syncMs, queueBytes, error -> {})) {
                started = System.nanoTime();
                feed(mutations, ticks, keys, paced, tickTimes, payload -> {
                    if (!writer.append(payload)) {
                        throw new IOException("Queue saturated: comparison aborted rather than counting rejected mutations as throughput. "
                                + "Increase queueMiB, reduce the load, or use paced=true. Stats: " + writer.stats());
                    }
                });
                drainStarted = System.nanoTime();
                writer.flush();
                completed = System.nanoTime();
                var stats = writer.stats();
                if (stats.durableTransactions() != operations) throw new IOException("Durability barrier missed transactions");
                forces = stats.forceCount();
                peak = stats.highWaterBytes();
                rejected = stats.rejectedTransactions();
            }
        } else {
            try (FileOutputStream file = new FileOutputStream(path.toFile(), true)) {
                started = System.nanoTime();
                feed(mutations, ticks, keys, paced, tickTimes, payload -> {
                    // Match the original appendWalRecordsLocked path, including its position query,
                    // unbuffered frame writes and one content force for every transaction.
                    file.getChannel().position();
                    DataOutputStream output = new DataOutputStream(file);
                    output.writeInt(payload.length);
                    output.write(payload);
                    output.writeLong(checksum(payload));
                    output.flush();
                    file.getChannel().force(false);
                });
                drainStarted = System.nanoTime();
                completed = drainStarted; // Every successful synchronous append was already forced.
                forces = operations;
            }
        }
        verifyReplay(path, operations, keys);
        long submission = Arrays.stream(tickTimes).sum();
        long overBudget = Arrays.stream(tickTimes).filter(time -> time > TICK_NANOS).count();
        Arrays.sort(tickTimes);
        return new Result(operations, percentile(tickTimes, 0.50), percentile(tickTimes, 0.95),
                percentile(tickTimes, 0.99), tickTimes[ticks - 1], overBudget, submission,
                completed - drainStarted, completed - started, forces, peak, rejected);
    }

    private static void feed(int mutations, int ticks, byte[][] keys, boolean paced,
                             long[] tickTimes, Appender appender) throws IOException {
        int sequence = 0;
        for (int tick = 0; tick < ticks; tick++) {
            long started = System.nanoTime();
            for (int i = 0; i < mutations; i++, sequence++) {
                byte[] key = keys[sequence % keys.length];
                long delta = (sequence / keys.length) % 2 == 0 ? 1L : -1L;
                appender.append(transaction(key, delta));
            }
            tickTimes[tick] = System.nanoTime() - started;
            if (paced && tick < ticks - 1) {
                long remaining;
                while ((remaining = TICK_NANOS - (System.nanoTime() - started)) > 0) {
                    LockSupport.parkNanos(remaining);
                    if (Thread.currentThread().isInterrupted()) throw new IOException("Benchmark interrupted");
                }
            }
        }
    }

    private static byte[] transaction(byte[] key, long delta) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(record)) {
            output.writeUTF("global");
            output.writeInt(0);
            output.writeInt(key.length);
            output.write(key);
            output.writeLong(delta);
        }
        ByteArrayOutputStream transaction = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(transaction)) {
            output.writeInt(TRANSACTION_MAGIC);
            output.writeInt(1);
            output.writeInt(record.size());
            output.write(record.toByteArray());
        }
        return transaction.toByteArray();
    }

    private static void verifyReplay(Path path, int operations, byte[][] keys) throws IOException {
        long[] counts = new long[keys.length];
        long[] expected = new long[keys.length];
        try (DataInputStream input = new DataInputStream(Files.newInputStream(path))) {
            for (int sequence = 0; sequence < operations; sequence++) {
                int length = input.readInt();
                if (length <= 0 || length > keys[0].length + 128) throw new IOException("Invalid frame length");
                byte[] payload = input.readNBytes(length);
                if (payload.length != length || input.readLong() != checksum(payload)) throw new IOException("Invalid WAL frame");
                int keyId = sequence % keys.length;
                long delta = (sequence / keys.length) % 2 == 0 ? 1L : -1L;
                if (!Arrays.equals(transaction(keys[keyId], delta), payload)) throw new IOException("Reordered or altered transaction");
                try (DataInputStream transaction = new DataInputStream(new ByteArrayInputStream(payload))) {
                    if (transaction.readInt() != TRANSACTION_MAGIC || transaction.readInt() != 1) throw new IOException("Invalid transaction");
                    transaction.readInt();
                    transaction.readUTF();
                    transaction.readInt();
                    byte[] key = transaction.readNBytes(transaction.readInt());
                    counts[ByteBuffer.wrap(key).getInt()] += transaction.readLong();
                }
                expected[keyId] += delta;
            }
            if (input.read() != -1 || !Arrays.equals(counts, expected)) throw new IOException("Replay differs from accepted workload");
        }
    }

    private static long checksum(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        return crc.getValue();
    }

    private static int positive(String[] args, int index, int fallback) {
        int value = args.length > index ? Integer.parseInt(args[index]) : fallback;
        if (value <= 0) throw new IllegalArgumentException("Expected positive argument at position " + index);
        return value;
    }

    private static long percentile(long[] sorted, double percentile) {
        return sorted[(int) Math.ceil(sorted.length * percentile) - 1];
    }

    private interface Appender {
        void append(byte[] payload) throws IOException;
    }

    private record Result(long transactions, long p50, long p95, long p99, long max,
                          long overBudget, long submit, long drain, long durableElapsed,
                          long forces, long peakBytes, long rejected) {
        String csv(int round, String mode) {
            return String.format(Locale.ROOT, "%d,%s,%d,%.3f,%.3f,%.3f,%.3f,%d,%.3f,%.3f,%.3f,%.1f,%d,%d,%d",
                    round, mode, transactions, p50 / 1e6, p95 / 1e6, p99 / 1e6, max / 1e6,
                    overBudget, submit / 1e6, drain / 1e6, durableElapsed / 1e6,
                    transactions * 1e9 / durableElapsed, forces, peakBytes, rejected);
        }
    }
}
