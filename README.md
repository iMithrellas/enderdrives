These drives link together based on the frequency set for each drive. All drives with the same frequency and privacy share the same contents, regardless of who crafted them. Whether in your AE2 system, another dimension, or on another player’s system, the contents are globally synchronized.

***

## EnderDrives

**EnderDrives** is a mod that offers a way to store your items **in the End — but digitally**. Each drive has a type limit based on its capacity.  
This is an artificial limit for gameplay mechanics, not due to storage limitations, but it is heavily suggested to try and keep the least amount of types in the system as possible.  
The system has been reliably tested up to **250,000 records**.  Exceed that with caution.

***

## Scope / Privacy Options

**Global:**  
Items stored under this scope are shared server-wide. Any player with a drive set to the same frequency can access the same shared inventory.

**Private:**  
Drives in private mode are tied to your UUID and only you can assign them to your private space. Other players can use the drive through your ME system, but the inventory is yours alone.

**Team:**  
Team drives are accessible by all members of your **FTB Team**. Any member can craft a drive and set it to a team frequency. Internally, the data is tied to the party owner’s UUID, so even if the team disbands, the original owner still has access.

***

## Powered by EnderDB

To handle storage at such large scales, EnderDrives uses a purpose-built database called **EnderDB**. All items are stored with their full data component (NBT), ensuring perfect accuracy in identifying and tracking items.  
Each stored item is indexed by its **frequency**, **scope**, and **full NBT**, along with a **count**. The count is stored as a signed 64-bit long, allowing up to **9 quintillion items** in a single record.

***

## EnderDB Features

The goal of EnderDB is **speed**, **reliability**, and **efficiency**. EnderDB uses:

*   **Rotating Write-Ahead Logging (WAL)** for data integrity
*   **Batch Commits** for performance
*   **CRC32 Checksums** for data integrity
*   **Compact Binary Storage** for performance
*   **In-Memory Caching** for performance
*   **Dedicated Background Threading**
*   **Safe Handling of Shutdowns and Crashes**

Item WAL transactions are checksummed and replayed as complete transactions. By default, item mutations wait for a disk sync before returning success. EnderDB persistence is separate from Minecraft's world and inventory saves.

### Queued item WAL (optional)

For automation-heavy servers, the item database can batch writes on a dedicated background thread. In the world's `serverconfig/enderdrives-server.toml`, set:

```toml
[ender_db]
async_item_wal = true
item_wal_sync_interval_ms = 250
item_wal_queue_mib = 64
```

Restart the server after changing these settings. `async_item_wal` defaults to `false`, retaining synchronous per-mutation durability. These options apply to the item database; fluid and tape storage have their own write paths.

Queued mode returns after reserving space for the transaction, and inventory changes are immediately visible in RAM. The writer batches transactions until the sync interval or approximately 1 MiB of queued data is reached. The memory budget includes in-flight transactions until their disk sync succeeds. If it fills, new mutations return zero accepted items before changing inventory contents.

A JVM crash or power loss can roll back accepted transactions that have not reached durable storage. The interval is a target, not a maximum during storage stalls. Rolling back one side of a transfer can lose or duplicate items relative to another saved inventory. Normal shutdown, explicit WAL flushes, scope migrations, and database checkpoints wait for durability. A writer failure disables further item mutations and preserves recovery files.

`/enderdrives stats` includes accepted/durable transaction counts, pending/peak queue bytes, rejected mutations, and sync counts for the current WAL generation. These counters reset when a checkpoint reopens the WAL. Checkpoints still serialize the full database while holding its lock, so large databases can experience checkpoint pauses.

See [WAL benchmarking](docs/wal-benchmark.md) for a reproducible synchronous-versus-queued comparison and in-server validation steps.

***

**Data is saved in your world folder:**  
`saves/world/data/enderdrives/enderdrives.bin`  
Write-ahead log (WAL) files are stored alongside this file for recovery and performance.
