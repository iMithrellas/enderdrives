package com.sts15.enderdrives.db;

import net.minecraft.SharedConstants;
import net.minecraft.server.Bootstrap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Tests the real item mutation, checkpoint and replay paths against an isolated database directory. */
@Timeout(15)
class EnderDBWalIntegrationTest {

    // These tests exercise persistence/counts independently of the Minecraft item codec.
    private static final byte[] KEY = {0, 1, 2, 3};
    @TempDir Path directory;

    @BeforeAll
    static void bootstrap() {
        SharedConstants.tryDetectVersion();
        Bootstrap.bootStrap();
    }

    @AfterEach
    void cleanup() throws Exception {
        call("closeWalQuietly");
        EnderDBManager.dbMap.clear();
        EnderDBManager.clearRAMCaches();
        EnderDBManager.running = false;
        EnderDBManager.dirty = false;
        set("initialized", false);
        setLifecycle("STOPPED");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void acknowledgedDeltasReplayInOrderAfterADurabilityBarrier(boolean queued) throws Exception {
        open(queued, 4 * 1024 * 1024);
        assertEquals(32, EnderDBManager.insertItem("global", 7, KEY, 32, 10));
        assertEquals(-12, EnderDBManager.saveItem("global", 7, KEY, -12));
        EnderDBManager.flushWALQueue();
        recover();
        assertEquals(20, count("global"));
        assertTrue(EnderDBManager.commitDatabase());
        recover();
        assertEquals(20, count("global"));
    }

    @Test
    void queueSaturationRejectsBeforePublishingTheMutation() throws Exception {
        open(true, 200); // One transaction fits; two do not, and the timer is 60 seconds.
        assertEquals(5, EnderDBManager.insertItem("global", 7, KEY, 5, 10));
        assertEquals(0, EnderDBManager.insertItem("global", 7, KEY, 3, 10));
        assertEquals(5, count("global"));
        assertEquals(1, EnderDBManager.getQueuedWalStats().rejectedTransactions());
        EnderDBManager.flushWALQueue();
        assertEquals(3, EnderDBManager.insertItem("global", 7, KEY, 3, 10));
        assertEquals(8, count("global"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointDrainsBeforeTruncationAndReplaysLaterMutationsExactlyOnce(boolean queued) throws Exception {
        open(queued, 4 * 1024 * 1024);
        for (int i = 0; i < 500; i++) assertEquals(1, EnderDBManager.saveItem("global", 7, KEY, 1));
        assertTrue(EnderDBManager.commitDatabase());
        assertEquals(0, Files.size(wal()));
        assertEquals(1, EnderDBManager.saveItem("global", 7, KEY, 1));
        EnderDBManager.flushWALQueue();
        recover();
        assertEquals(501, count("global"));
        assertTrue(EnderDBManager.commitDatabase());
        recover();
        assertEquals(501, count("global"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void markedSnapshotSurvivesCrashOnEitherSideOfWalTruncation(boolean truncate) throws Exception {
        open(true, 4096);
        assertEquals(5, EnderDBManager.saveItem("global", 7, KEY, 5));
        EnderDBManager.flushWALQueue();
        Object checkpoints = call("collectWalCheckpointsLocked");
        assertEquals(true, call("writeDatabaseSnapshotLocked", new Class<?>[]{List.class}, checkpoints));
        if (truncate) assertEquals(true, call("truncateCurrentWALLocked"));
        // Deliberately omit the second snapshot to exercise the existing crash-recovery markers.
        recover();
        assertEquals(5, count("global"));
    }

    @Test
    void incompleteTailIsQuarantinedWithoutLosingTheDurablePrefix() throws Exception {
        open(true, 4096);
        assertEquals(9, EnderDBManager.saveItem("global", 7, KEY, 9));
        EnderDBManager.flushWALQueue();
        call("closeWalQuietly");
        long validLength = Files.size(wal());
        try (DataOutputStream output = new DataOutputStream(new FileOutputStream(wal().toFile(), true))) {
            output.writeInt(100);
            output.writeShort(42);
        }
        recover();
        assertEquals(9, count("global"));
        assertEquals(validLength, Files.size(wal()));
        try (var files = Files.list(directory)) {
            assertTrue(files.anyMatch(path -> path.getFileName().toString().startsWith("enderdrives.wal.corrupt.")));
        }
        assertTrue(EnderDBManager.commitDatabase());
        recover();
        assertEquals(9, count("global"));
    }

    @Test
    void scopeMigrationIsAnAtomicDurabilityBarrierInQueuedMode() throws Exception {
        open(true, 4096);
        assertEquals(6, EnderDBManager.saveItem("global", 7, KEY, 6));
        assertTrue(EnderDBManager.migrateScope("global", "player_test"));
        assertEquals(2, EnderDBManager.getQueuedWalStats().durableTransactions());
        assertEquals(0, count("global"));
        assertEquals(6, count("player_test"));
        recover();
        assertEquals(0, count("global"));
        assertEquals(6, count("player_test"));
    }

    @Test
    void shutdownPersistsAnEmptyDatabaseAndCanBeRepeated() throws Exception {
        open(true, 4096);
        assertEquals(5, EnderDBManager.saveItem("global", 7, KEY, 5));
        assertEquals(-5, EnderDBManager.saveItem("global", 7, KEY, -5));
        EnderDBManager.shutdown();
        EnderDBManager.shutdown();
        recover();
        assertEquals(0, count("global"));
        assertTrue(EnderDBManager.dbMap.isEmpty());
    }

    @Test
    void failedWriterPreventsCheckpointAndShutdownFromDiscardingRecoveryData() throws Exception {
        open(true, 4096);
        assertEquals(10, EnderDBManager.saveItem("global", 7, KEY, 10));
        assertTrue(EnderDBManager.commitDatabase());
        byte[] snapshot = Files.readAllBytes(directory.resolve("enderdrives.bin"));
        call("closeWalQuietly");
        FileOutputStream file = new FileOutputStream(wal().toFile(), true);
        QueuedWalWriter.Sink sink = new QueuedWalWriter.Sink() {
            @Override public void append(byte[] payload) throws IOException {
                file.write(new byte[]{0, 0});
                throw new IOException("Injected partial disk write");
            }
            @Override public void force() throws IOException { file.getChannel().force(false); }
            @Override public void close() throws IOException { file.close(); }
        };
        set("queuedWalWriter", new QueuedWalWriter(sink, 60_000, 4096, error -> {}));
        assertEquals(3, EnderDBManager.saveItem("global", 7, KEY, 3));
        EnderDBManager.flushWALQueue();
        assertFalse(EnderDBManager.isReady());
        assertEquals(0, EnderDBManager.saveItem("global", 7, KEY, 1));
        assertFalse(EnderDBManager.commitDatabase());
        EnderDBManager.shutdown();
        assertEquals(13, count("global")); // Keep acknowledged RAM for diagnostics after failure.
        assertArrayEquals(snapshot, Files.readAllBytes(directory.resolve("enderdrives.bin")));
        assertArrayEquals(new byte[]{0, 0}, Files.readAllBytes(wal()));
    }

    private void open(boolean queued, long queueBytes) throws Exception {
        call("closeWalQuietly");
        EnderDBManager.dbMap.clear();
        EnderDBManager.clearRAMCaches();
        EnderDBManager.dirty = false;
        set("dbFile", directory.resolve("enderdrives.bin").toFile());
        set("currentWAL", wal().toFile());
        set("asyncItemWal", queued);
        set("walSyncIntervalMs", 60_000L);
        set("walQueueBytes", queueBytes);
        set("loadedWalCheckpoints", Map.of());
        call("openWALStream");
        ready();
    }

    private void recover() throws Exception {
        call("closeWalQuietly");
        EnderDBManager.dbMap.clear();
        EnderDBManager.clearRAMCaches();
        EnderDBManager.dirty = false;
        set("loadedWalCheckpoints", Map.of());
        call("loadDatabase");
        call("replayWALs");
        call("openWALStream");
        ready();
    }

    private void ready() throws Exception {
        set("initialized", true);
        EnderDBManager.running = true;
        setLifecycle("RUNNING");
    }

    private long count(String scope) {
        return EnderDBManager.getItemCount(scope, 7, KEY);
    }

    private Path wal() { return directory.resolve("enderdrives.wal"); }

    private static void set(String name, Object value) throws Exception {
        var field = EnderDBManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(null, value);
    }

    private static void setLifecycle(String name) throws Exception {
        var field = EnderDBManager.class.getDeclaredField("lifecycle");
        for (Object value : field.getType().getEnumConstants()) {
            if (((Enum<?>) value).name().equals(name)) {
                set("lifecycle", value);
                return;
            }
        }
        throw new IllegalArgumentException(name);
    }

    private static Object call(String name) throws Exception {
        return call(name, new Class<?>[0]);
    }

    private static Object call(String name, Class<?>[] parameters, Object... args) throws Exception {
        var method = EnderDBManager.class.getDeclaredMethod(name, parameters);
        method.setAccessible(true);
        try {
            return method.invoke(null, args);
        } catch (InvocationTargetException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            throw error;
        }
    }
}
