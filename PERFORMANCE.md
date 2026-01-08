# IronWal Performance Report

This document details the performance characteristics of IronWal, based on benchmarks covering write throughput, read latency, and system behavior under stress.

The performance test were done on a Macbook Pro with M4 Pro.

## Summary

IronWal is optimized for high-throughput, concurrent write workloads and low-latency random reads when properly configured.

*   **Write Throughput:** Scales nearly linearly with thread count.
*   **Read Latency:** Sub-millisecond random reads (~200µs) when using Memory Mapping.
*   **Startup Time:** Constant-time recovery (~0.7ms) regardless of history size.

## Write Performance

### Sync Modes

The choice of `SyncMode` has the largest impact on write latency.

| Mode | Latency (Single Append) | Description |
| :--- | :--- | :--- |
| **Strict** | ~11.0 ms | Calls `fsync` on every write. Durability is guaranteed, but speed is limited by disk seek time. |
| **Async** | ~6.6 ms | Flushes buffer to OS kernel but relies on background sync. ~40% faster than Strict in synthetic benchmarks, significantly faster in burst throughput. |

### Batching Efficiency

IronWal is designed to amortize I/O costs. Writing a batch of 100 items takes effectively the same time as writing a single item in `Strict` mode.

*   **Single Append (Strict):** ~11.0 ms
*   **Batch Append (100 items, Strict):** ~11.3 ms

**Recommendation:** Always prefer `append_batch` or `TxnWriter` for high-throughput applications to maximize IOPS.

### Concurrency Scaling

The library uses a fine-grained, per-stream locking strategy (`StreamStateMap`), allowing it to scale effectively on multi-core systems.

| Threads | Throughput (Ops/sec) | Scaling Factor |
| :--- | :--- | :--- |
| **1** | ~12,500 | 1.0x |
| **4** | ~20,300 | 1.6x |
| **8** | ~25,000 | 2.0x |
| **16** | ~28,500 | 2.3x |

**Note:** Throughput continues to improve as thread count increases, indicating low contention on the internal map.

## Read Performance

### Read Strategies

IronWal supports two read strategies. Memory-mapping (`Mmap`) provides a massive performance advantage for random access patterns.

| Strategy | Random Read Latency | Improvement |
| :--- | :--- | :--- |
| **Standard IO** | ~15.0 ms | Baseline |
| **Mmap** | **~0.2 ms** | **75x Faster** |

**Recommendation:** Use `ReadStrategy::Mmap` for all read-heavy workloads unless running on memory-constrained (e.g., embedded) or 32-bit environments.

### Sequential Scans

Sequential iteration is highly optimized in both modes, though `Mmap` retains a slight edge.

*   **Standard IO Iterator:** ~1.5 ms per batch scan
*   **Mmap Iterator:** ~1.0 ms per batch scan

## Stress & Stability

### Cache Thrashing
When the number of active streams exceeds `max_open_segments`, the WAL must constantly evict and reopen file handles.

*   **Scenario:** Writing to 100 streams with a cache size of 10 (90% miss rate).
*   **Result:** ~575ms for 100 writes (~5.7ms per write).
*   **Analysis:** While slower than cached writes, the system remains stable and does not degrade into IO errors. The overhead is primarily dominated by OS `open`/`close` syscalls.

### Recovery Speed
Startup time is decoupled from the volume of data stored.

*   **Scenario:** Recovering a stream with 1,000 segment files and 10,000 entries.
*   **Time:** **~0.7 ms**
*   **Why:** IronWal uses a metadata file (`head.state`) to locate the active segment immediately, avoiding a linear scan of the directory.

## Tuning Guidelines

For maximum performance:

1.  **High Write Throughput:** Use `SyncMode::BatchOnly` or `Async` and group writes into batches.
2.  **Low Read Latency:** Enable `ReadStrategy::Mmap`.
3.  **High Concurrency:** Ensure your application writes to distinct streams (e.g., one stream per user entity) to leverage the sharded locking.
4.  **Resource Management:** Set `max_open_segments` high enough to cover your "working set" of active streams to avoid thrashing penalties.