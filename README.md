# IronWal

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![Crates.io](https://img.shields.io/crates/v/ironwal.svg)](https://crates.io/crates/ironwal)
[![Documentation](https://docs.rs/ironwal/badge.svg)](https://docs.rs/ironwal)

IronWal is a high-durability, deterministic Write-Ahead Log (WAL) designed for systems of record, such as financial ledgers, game state synchronization, and event stores. It prioritizes data integrity, precise replication control, and predictable resource usage over raw, unsafe throughput, providing a robust foundation for building crash-safe applications.

## Installation

Add `ironwal` to your `Cargo.toml` dependencies:

```toml
[dependencies]
ironwal = "0.6"
```

LZ4 compression support is included in the default features. To be explicit:

```toml
[dependencies]
ironwal = { version = "0.6", features = ["compression"] }
```

## Quick Start

The core type is `Wal`: you append binary entries to named **streams** and read them back by ID or by iteration.

```rust
use ironwal::{Wal, WalOptions};

fn main() -> ironwal::Result<()> {
    // All data lives under this directory, one subdirectory per stream.
    let wal = Wal::new(WalOptions::new("./my_wal_data"))?;

    // Append an entry; it is assigned a monotonically increasing ID.
    let id = wal.append("orders", b"order_created")?;

    // Random access by ID...
    let entry = wal.get("orders", id)?;

    // ...or sequential scans from any starting ID.
    for entry in wal.iter("orders", 0)? {
        let data = entry?;
        // process data...
    }

    Ok(())
}
```

Atomic batch writes (`append_batch`, `TxnWriter`) and disk reclamation (`truncate`, `delete_stream`) are covered in the **[Usage Guide](README.USAGE.md)**.

## Core Concepts

* **Stream** — an independent, append-only sequence of records, addressed by name. Each stream maps to its own directory on disk.
* **Entry** — a single binary record. Every entry in a stream gets a monotonically increasing 64-bit sequence ID.
* **Segment** — streams are stored as segment files that rotate automatically at configurable size/count thresholds. Truncation reclaims disk space by deleting whole segments below a safe ID.

## Key Features

### Deterministic Storage
Stream names map explicitly to physical directories on disk. This deterministic structure simplifies external backups, replication strategies, and manual auditing of the log files.

### Data Integrity & Safety
Every frame written to disk is protected by a CRC32 checksum to detect bit rot and disk corruption. The library includes a rigorous recovery system capable of handling partial writes, power failures, and corrupted indices without data loss.

### Hybrid Compression
To balance storage efficiency and latency, IronWal employs a hybrid compression strategy. Frames can be stored as raw binary or compressed using the LZ4 format, automatically decided based on configurable size thresholds.

### Per-Stream Concurrency
`Wal` is cheaply cloneable and thread-safe. Locking is per stream, so appending to one stream never blocks writers on other streams — throughput scales with the number of independent streams being written.

### Resource Protection
Open file descriptors are held in bounded LRU caches, for both writing and reading. This prevents the application from exhausting system file handle limits, even when managing thousands of active streams.

### Tunable Durability
Developers can choose between three synchronization modes: `Strict` (fsync on every write), `BatchOnly` (fsync only on batches/transactions), and `Async` (rely on OS page cache), allowing for precise trade-offs between latency and safety.

## Sharded WAL (optional `sharded` feature)

Everything above is the core, always-available API. For write volumes beyond what a single stream can absorb, the optional `ShardedWal` builds on top of `Wal`: it partitions writes across a fixed number of internal streams (`shard_00`, `shard_01`, …) by hashing a key you supply. The key only routes the write — it is not stored — and each entry is addressed by `(shard_id, seq_id)`. On top of this partitioning, `ShardedWal` adds **checkpoints** — a consistent cut of every shard's durable offset, tagged with an ID you choose (a Raft index, transaction ID, ULID, …) — and **pruning** of segments older than a checkpoint.

Note that this is a different mechanism from the per-stream concurrency above: the core `Wal` already lets independent streams proceed in parallel. Reach for `ShardedWal` only when the throughput of a *single* logical stream is the bottleneck. In exchange you give up per-key `get` (reads are per-shard iteration), atomicity of batches that span shards, and the ability to change the shard count without a migration.

```toml
[dependencies]
ironwal = { version = "0.6", features = ["sharded"] }
```

```rust
use ironwal::sharded::ShardedWal;
use ironwal::WalOptions;

// 16 shards, fixed at creation.
let wal = ShardedWal::new(WalOptions::new("./data"), 16)?;

// The key picks the shard; it is not stored with the entry.
let (shard_id, seq_id) = wal.append(b"user_123", b"profile_data")?;

// Record every shard's durable offset under an ID you choose.
wal.create_checkpoint(b"raft_index_5000")?;

// Reclaim disk space for segments entirely before that checkpoint.
wal.prune_before_checkpoint(b"raft_index_5000")?;
```

Restoring from checkpoints, batch writes, durability caveats under `BatchOnly`/`Async`, and shard-count sizing are covered in the [Usage Guide](README.USAGE.md#sharded-wal-optional-feature).

## Notable Users

[Hi Stakes Markets Game](https://www.histakesgame.com) -  The worlds most advanced financial simulator, available on iPhone and Android.

## Documentation

For a detailed guide on configuration, API usage, and architectural concepts, please see the **[Usage Guide](README.USAGE.md)**.

You can also view the full API reference documentation on [docs.rs](https://docs.rs/ironwal).

## License

This library is distributed under the terms of the **Mozilla Public License 2.0 (MPL-2.0)**.
