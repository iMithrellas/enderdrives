# Item WAL benchmark

The benchmark compares the original `appendWalRecordsLocked` I/O sequence (position query, unbuffered length/payload/CRC writes, then `force(false)` for every mutation) with the production `QueuedWalWriter`. Both paths serialize identical EDW2 transactions with alternating insert/extract deltas across 256 synthetic item keys.

It runs on real files in a new `run-*` directory, checks every frame and replayed count, and requires byte-identical WAL output from both modes. It warms up both paths and alternates their execution order between rounds. Queue saturation aborts the comparison instead of counting rejected work as throughput.

## Running

Use JDK 25. The repository's `gradle.properties` specifies a Windows JDK path; on another system, override it with `-Dorg.gradle.java.home="$JAVA_HOME"`.

```sh
bash gradlew -Dorg.gradle.java.home="$JAVA_HOME" walBenchmark \
  -PwalBenchmarkDir=/path/on/server-storage/wal-benchmark \
  -PwalMutations=500 \
  -PwalTicks=100 \
  -PwalKeyBytes=128 \
  -PwalSyncMs=250 \
  -PwalRounds=3 \
  -PwalPaced=true \
  -PwalQueueMiB=64
```

All parameters are optional. The defaults are shown above, except the directory defaults to `build/wal-benchmark`. Use a directory on the actual server storage to characterize its sync latency. `walPaced=true` attempts a burst every 50 ms; a burst that takes longer proceeds without an additional sleep. `false` tests unpaced producer throughput and can saturate the queue.

The benchmark source set has no Minecraft runtime dependencies. It can also be compiled and run directly:

```sh
mkdir -p build/wal-benchmark-classes
javac -d build/wal-benchmark-classes \
  src/main/java/com/sts15/enderdrives/db/QueuedWalWriter.java \
  src/benchmark/java/com/sts15/enderdrives/db/WalBenchmark.java
java -cp build/wal-benchmark-classes com.sts15.enderdrives.db.WalBenchmark \
  /path/on/server-storage/wal-benchmark 500 100 128 250 3 true 64
```

## Interpreting results

Each run prints CSV and saves `results.csv` next to its WAL files:

- `tick_p50_ms`, `tick_p95_ms`, `tick_p99_ms`, `tick_max_ms`: time spent serializing and submitting one burst, excluding pacing sleeps.
- `ticks_over_50ms`: bursts exceeding a normal Minecraft tick budget due to this workload alone.
- `submit_ms`: sum of burst submission times.
- `drain_ms`: time to make all remaining accepted transactions durable after the final burst.
- `durable_elapsed_ms`, `durable_ops_per_second`: end-to-end timing including pacing and the final durability barrier.
- `forces`: completed disk syncs.
- `peak_pending_bytes`: maximum accounted queued plus in-flight memory; includes record overhead.
- `rejected`: must be zero for a comparable completed workload.

This is a WAL microbenchmark, not a TPS benchmark. It includes transaction serialization but excludes semantic item lookup, Minecraft NBT codecs, AE2 callbacks, network traffic, and full database checkpoints. Queued admission is intentionally measured separately from durability. The two modes have different crash-loss windows.

## Measured baseline

Local run: Linux amd64, Java 25.0.4.1, Btrfs on `/dev/mapper/root`; 500 mutations per burst, 100 paced bursts, 128-byte keys, 250 ms sync target, 64 MiB budget. Each round submitted and durably completed 50,000 transactions.

| Round | Mode | Burst p50 | Burst p99 | Bursts >50 ms | Final drain | Disk syncs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Synchronous | 241.936 ms | 399.359 ms | 100 | 0 ms | 50,000 |
| 1 | Queued | 0.307 ms | 1.203 ms | 0 | 1.838 ms | 19 |
| 2 | Synchronous | 252.896 ms | 287.420 ms | 100 | 0 ms | 50,000 |
| 2 | Queued | 0.267 ms | 0.588 ms | 0 | 1.701 ms | 19 |
| 3 | Synchronous | 246.990 ms | 283.818 ms | 100 | 0 ms | 50,000 |
| 3 | Queued | 0.254 ms | 0.591 ms | 0 | 3.769 ms | 19 |

Synchronous runs took 24.70–25.43 seconds to durably finish; queued runs took about 4.96 seconds, including the 20-burst-per-second pacing. Queued peak accounted memory was 720,000 bytes, with zero rejections. Every output frame and replay result matched between modes. These measurements describe this machine's filesystem and storage, not a guaranteed speedup on other servers.

A second workload increased the burst to **1,000 mutations** and the synthetic key size to **1,024 bytes**, over 50 paced bursts (one round):

| Mode | Burst p50 | Burst p99 | Bursts >50 ms | Final drain | Disk syncs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Synchronous | 487.505 ms | 588.316 ms | 50 | 0 ms | 50,000 |
| Queued | 1.401 ms | 3.440 ms | 0 | 2.214 ms | 55 |

Queued mode durably completed the workload in 2.457 seconds, including pacing, versus 24.502 seconds synchronously. Peak accounted memory was 2,172,032 bytes, with zero rejections and matching WAL/replay results. This workload also exercises size-triggered batches before the 250 ms timer expires.

## In-server comparison

1. Use the same copied world and automation layout for both runs. Record storage type, Java version, mod versions, database size and configuration.
2. Start with `async_item_wal=false`, warm the workload, and capture Spark plus MSPT percentiles during sustained traffic. Include enough time for multiple checkpoints.
3. Restart with `async_item_wal=true`, keep the same traffic, and collect the same measurements. Exercise 100, 500 and 1,000 mutations per tick and representative large item components.
4. Watch `/enderdrives stats` for rejected mutations and growing pending bytes. Its queued-WAL counters cover the current generation and reset at checkpoints.
5. Compare tick latency, storage throughput, time spent waiting for the database lock, and checkpoint spikes. Verify item counts after a clean stop/restart in each mode.

The automated tests cover slow storage without producer blocking, in-flight queue accounting, ordered concurrent admission, interval and size triggers, write/force/close failure handling, interrupted barriers, checkpoint crash windows, truncated-tail recovery, scope migration and repeated shutdown. Run them with:

```sh
bash gradlew -Dorg.gradle.java.home="$JAVA_HOME" test
```
