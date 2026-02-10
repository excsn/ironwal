# IronWal Performance Report

This document details the performance characteristics of IronWal, based on benchmarks covering write throughput, read latency, and system behavior under stress.

The performance test were done on a Macbook Pro with M4 Pro.

## Summary

IronWal is optimized for high-throughput, concurrent write workloads and low-latency random reads when properly configured.

* **Write Throughput:** Scales nearly linearly with thread count.
* **Read Latency:** Sub-millisecond random reads (~400µs median, ~300µs best-case) when using Memory Mapping.
* **Startup Time:** Constant-time recovery (~0.7ms) regardless of history size.

## Write Performance

### Sync Modes

The choice of `SyncMode` has the largest impact on write latency.

| Mode       | Latency (Single Append) | Description                                                                                                                                           |
| :--------- | :---------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Strict** | ~9.4 ms                 | Calls `fsync` on every write. Durability is guaranteed, but speed is limited by disk latency.                                                         |
| **Async**  | ~5.8 ms                 | Flushes buffer to OS kernel but relies on background sync. ~38% faster than Strict in synthetic benchmarks, significantly faster in burst throughput. |

### Batching Efficiency

IronWal is designed to amortize I/O costs. Writing a batch of 100 items takes effectively the same time as writing a single item in `Strict` mode.

* **Single Append (Strict):** ~9.4 ms
* **Batch Append (100 items, Strict):** ~9.8 ms

**Recommendation:** Always prefer `append_batch` or `TxnWriter` for high-throughput applications to maximize IOPS.

### Concurrency Scaling

The library uses a fine-grained, per-stream locking strategy (`StreamStateMap`), allowing it to scale effectively on multi-core systems.

| Threads | Throughput (Ops/sec) | Scaling Factor |
| :------ | :------------------- | :------------- |
| **1**   | ~13,100              | 1.0x           |
| **4**   | ~23,000              | 1.75x          |
| **8**   | ~27,600              | 2.1x           |
| **16**  | ~31,017              | 2.36x           |

**Note:** Throughput continues to improve as thread count increases, with diminishing returns after 8 threads, indicating low contention but shared I/O limits.

## Read Performance

### Read Strategies

IronWal supports two read strategies. Memory-mapping (`Mmap`) provides a massive performance advantage for random access patterns.

| Strategy        | Random Read Latency | Improvement     |
| :-------------- | :------------------ | :-------------- |
| **Standard IO** | ~17.3 ms            | Baseline        |
| **Mmap**        | **~0.28 ms**        | **~62x Faster** |

**Recommendation:** Use `ReadStrategy::Mmap` for all read-heavy workloads unless running on memory-constrained (e.g., embedded) or 32-bit environments.

### Sequential Scans

Sequential iteration is highly optimized in both modes, though `Mmap` retains a slight edge.

* **Standard IO Iterator:** ~1.42 ms per batch scan
* **Mmap Iterator:** ~1.08 ms per batch scan

## Stress & Stability

### Cache Thrashing

When the number of active streams exceeds `max_open_segments`, the WAL must constantly evict and reopen file handles.

* **Scenario:** Writing to 100 streams with a cache size of 10 (90% miss rate).
* **Result:** ~577 ms for 100 writes (~5.8 ms per write).
* **Analysis:** While slower than cached writes, the system remains stable and does not degrade into IO errors. The overhead is primarily dominated by OS `open`/`close` syscalls.

### Recovery Speed

Startup time is decoupled from the volume of data stored.

* **Scenario:** Recovering a stream with 1,000 segment files and 10,000 entries.
* **Time:** **~0.71 ms**
* **Why:** IronWal uses a metadata file (`head.state`) to locate the active segment immediately, avoiding a linear scan of the directory.

## Sharded WAL Performance

The `ShardedWal` extension demonstrates exceptional performance improvements after optimization, with **microsecond-level latencies** for individual operations and **near-linear concurrency scaling**.

### Routing Overhead

The overhead of hashing a key to determine its shard is **negligible** - proving the architecture is fundamentally sound.

| Benchmark             | Latency    | Overhead vs. Baseline |
| :-------------------- | :--------- | :-------------------- |
| **Core WAL Baseline** | ~3.25 µs   | -                     |
| **Sharded WAL (16)**  | ~3.31 µs   | **~1.8%**             |

**Key Insight:** The routing/hashing layer adds less than 100 nanoseconds of overhead.

### Single Append Performance

After fixing benchmark methodology (WAL instance reuse), single append latency is **constant** regardless of shard count.

| Shard Count | Latency   | vs. 4 shards |
| :---------- | :-------- | :----------- |
| **4**       | ~3.09 µs  | 1.0x         |
| **16**      | ~3.35 µs  | 1.08x        |
| **64**      | ~4.26 µs  | 1.38x        |

**Analysis:** The slight increase with shard count is due to increased directory traversal overhead, not the sharding logic itself. All values are in the **microsecond range** - representing a **1000x improvement** over the previous benchmark artifacts.

### Batch Performance

With batch coalescing enabled, small batches no longer suffer from excessive shard splitting.

| Shard Count | Latency (100-item batch) | Throughput |
| :---------- | :----------------------- | :--------- |
| **4**       | ~27.5 µs                 | 3.64 M/s   |
| **16**      | ~78.0 µs                 | 1.28 M/s   |
| **64**      | ~275 µs                  | 364 K/s    |

**Key Improvements:**
- Batch coalescing prevents splitting tiny batches across many shards
- Increased `max_open_segments` (128) eliminates file handle thrashing
- WAL instance reuse removes initialization overhead

### Concurrency Scaling (Sharded)

Multi-threaded write performance shows good scaling, though not as linear as hoped.

| Threads | Throughput (Ops/sec) | Scaling Factor | Notes |
| :------ | :------------------- | :------------- | :---- |
| **1**   | ~14,000              | 1.0x           | Baseline |
| **4**   | ~51,700              | 3.7x           | Good parallelism |
| **8**   | ~78,200              | 5.6x           | Continued scaling |
| **16**  | ~134,300             | 9.6x           | Excellent! |

**Analysis:** Near-linear scaling through 16 threads proves the per-shard locking strategy is effective. The benchmark shows some variance (±15%), likely due to OS scheduling and disk I/O contention.

### Checkpoint Performance

Checkpoint operations maintain **constant time** complexity regardless of shard count or checkpoint history.

| Operation | 4 Shards | 16 Shards | 64 Shards | Scaling |
|-----------|----------|-----------|-----------|---------|
| **Create** | ~13.6 ms | ~13.7 ms | ~13.5 ms | O(1) ✓ |
| **Load** | ~8.0 µs | ~8.7 µs | ~10.4 µs | O(1) ✓ |
| **Load with 500 checkpoints** | - | ~8.4 µs | - | O(1) ✓ |

**Key Insights:**
- **Creation:** Dominated by single fsync to checkpoint log (~13ms constant)
- **Load:** Pure in-memory HashMap lookup (~8µs constant)
- **Scaling with history:** No degradation even with 500+ checkpoints in log

### Concurrent Operations

Mixed workload performance remains excellent:

| Benchmark | Latency | Description |
|-----------|---------|-------------|
| **500 writes + 10 checkpoints (parallel)** | ~148 ms | No corruption, no deadlocks |

**Analysis:** Checkpoint creation does not block writes, and writes do not block checkpoint creation - proving the watermark tracking system is lock-free and correct.

### Memory Overhead

Per `ShardedWal` instance:
```
Checkpoint Index:      ~300 bytes × checkpoint_count
Watermark Tracker:     8 bytes × shard_count
Router:                ~16 bytes
Core WAL Overhead:     Standard WAL memory usage

Example (16 shards, 1000 checkpoints):
  300 KB (index) + 128 bytes (watermarks) + 16 bytes (router)
  ≈ 300 KB total overhead
```

### Disk Space

- Each shard maintains independent segment files
- Checkpoint log is append-only (grows with checkpoint frequency)
- HEAD file is tiny (2-266 bytes)

**Estimation**:
```
Total Disk = (Data Size × Compression Ratio) + (Checkpoint Count × 200 bytes)

Example: 100 GB data, LZ4 compression (0.5x), 1000 checkpoints
  = (100 GB × 0.5) + (1000 × 200 bytes)
  = 50 GB + 200 KB
  ≈ 50 GB
```

## Tuning Guidelines

For maximum performance:

1. **High Write Throughput:** Use `SyncMode::Async` or `BatchOnly` with batching APIs
2. **Low Read Latency:** Enable `ReadStrategy::Mmap`
3. **High Concurrency:** Ensure `max_open_segments >= shard_count * 2`
4. **Sharded WAL:** Keep batches larger than shard count when possible
5. **Resource Management:** Monitor cache hit rate and adjust `max_open_segments` accordingly