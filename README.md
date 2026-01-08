# IronWal

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![Crates.io](https://img.shields.io/crates/v/ironwal.svg)](https://crates.io/crates/ironwal)
[![Documentation](https://docs.rs/ironwal/badge.svg)](https://docs.rs/ironwal)

IronWal is a high-durability, deterministic Write-Ahead Log (WAL) designed for systems of record, such as financial ledgers, game state synchronization, and event stores. It prioritizes data integrity, precise replication control, and predictable resource usage over raw, unsafe throughput, providing a robust foundation for building crash-safe applications.

## Key Features

### Deterministic Storage
Stream keys map explicitly to physical directories on disk. This deterministic structure simplifies external backups, replication strategies, and manual auditing of the log files.

### Data Integrity & Safety
Every frame written to disk is protected by a CRC32 checksum to detect bit rot and disk corruption. The library includes a rigorous recovery system capable of handling partial writes, power failures, and corrupted indices without data loss.

### Hybrid Compression
To balance storage efficiency and latency, IronWal employs a hybrid compression strategy. Frames can be stored as raw binary or compressed using the LZ4 format, automatically decided based on configurable size thresholds.

### Fine-Grained Concurrency
Writes are managed via a sharded locking strategy (`StreamStateMap`), ensuring that appending data to one stream never blocks writers operating on different streams. This allows the system to scale throughput linearly with thread count.

### Resource Protection
The library utilizes a strictly managed LRU cache for open file descriptors (`fibre_cache`). This prevents the application from exhausting system file handle limits, even when managing thousands of active streams.

### Tunable Durability
Developers can choose between three synchronization modes: `Strict` (fsync on every write), `BatchOnly` (fsync only on batches/transactions), and `Async` (rely on OS page cache), allowing for precise trade-offs between latency and safety.

## Installation

Add `ironwal` to your `Cargo.toml` dependencies:

```toml
[dependencies]
ironwal = "0.5.0"
```

To enable LZ4 compression support (recommended), ensure the default features are active or explicitly enable `compression`:

```toml
[dependencies]
ironwal = { version = "0.5.0", features = ["compression"] }
```

## Documentation

For a detailed guide on configuration, API usage, and architectural concepts, please see the **[Usage Guide](README.USAGE.md)**.

You can also view the full API reference documentation on [docs.rs](https://docs.rs/ironwal).

## License

This library is distributed under the terms of the **Mozilla Public License 2.0 (MPL-2.0)**.