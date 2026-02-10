# Usage Guide: IronWal

This guide provides a comprehensive overview of how to integrate, configure, and operate the IronWal library in your Rust applications.

## Core Concepts

IronWal operates on a few fundamental abstractions designed to map logical data to physical storage efficiently:

*   **Stream:** A logical, independent sequence of append-only records (e.g., "user_123_ledger"). Physically, this corresponds to a directory on disk.
*   **Entry:** A single blob of binary data (`Vec<u8>`) appended to a stream. Entries are assigned a monotonically increasing 64-bit Sequence ID.
*   **Segment:** To manage file sizes, streams are broken down into Segment files (e.g., `00000000000000000000.wal`). The WAL handles rotation automatically based on size or count thresholds.
*   **Frame:** The atomic unit written to disk. A frame contains a header (CRC, size, metadata) and a payload (which may contain a batch of user entries, optionally compressed).

## Quick Start

### Basic Read/Write
This example demonstrates initializing the WAL, appending a single record, and reading it back.

```rust
use ironwal::{Wal, WalOptions, SyncMode};

fn main() -> ironwal::Result<()> {
    // 1. Configure the WAL
    let mut opts = WalOptions::new("./my_wal_data");
    opts.sync_mode = SyncMode::Strict; // Ensure durability

    // 2. Open the WAL
    let wal = Wal::new(opts)?;

    // 3. Append data to a stream named "logs"
    let id = wal.append("logs", b"Hello, IronWal!")?;
    println!("Written entry at ID: {}", id);

    // 4. Read the data back
    if let Some(data) = wal.get("logs", id)? {
        println!("Read: {:?}", String::from_utf8_lossy(&data));
    }

    Ok(())
}
```

### Atomic Batch Writes
To improve throughput and ensure atomicity, write multiple records at once.

```rust
fn batch_example(wal: &Wal) -> ironwal::Result<()> {
    let batch = vec![
        b"Transaction Start".as_slice(),
        b"Update A".as_slice(),
        b"Update B".as_slice(),
    ];

    // These writes are atomic; either all exist or none do.
    let range = wal.append_batch("transactions", &batch)?;
    
    println!("Batch written to IDs {} to {}", range.start, range.end);
    Ok(())
}
```

## Configuration

The `WalOptions` struct controls the behavior of the storage engine. It allows you to tune the system for specific hardware or durability requirements.

### Key Configuration Fields

*   **`root_path`** (`PathBuf`): The base directory where all stream data is stored.
*   **`max_segment_size`** (`u64`): The soft limit (in bytes) for a segment file before rotation occurs. Default: 64 MB.
*   **`max_entries_per_segment`** (`u64`): Soft limit for the number of entries per file. Default: Disabled (`u64::MAX`).
*   **`max_open_segments`** (`usize`): The size of the LRU cache for open file descriptors. Increasing this improves performance for high-concurrency workloads but consumes more OS resources. Default: 50.
*   **`block_cache_size`** (`Option<u64>`): If set, enables an in-memory LRU cache for decompressed read blocks.

### Enums and Options

*   **`SyncMode`**:
    *   `Strict`: Calls `fsync` after every single append. Highest safety, lowest throughput.
    *   `BatchOnly`: Calls `fsync` only on batch writes.
    *   `Async`: Relies on OS background flushing. Highest throughput, risk of data loss on power failure.
*   **`CompressionType`**:
    *   `None`: Raw binary storage.
    *   `Lz4`: Uses LZ4 frame format. Effective for larger payloads or batches.
*   **`ReadStrategy`**:
    *   `StandardIo`: Uses `File::seek` and `read`. Safe and predictable memory usage.
    *   `Mmap`: Memory-maps segment files. Significantly faster for random reads but can be risky on 32-bit systems or under extreme memory pressure.

## Working with Data

### Writing Data

The primary entry point for writing is the `Wal` struct. Writes are thread-safe and use fine-grained locking, so multiple threads can write to different streams simultaneously without contention.

*   `fn append(&self, stream: &str, entry: &[u8]) -> Result<u64>`
    *   Appends a single entry and returns its assigned Sequence ID.
*   `fn append_batch(&self, stream: &str, entries: &[&[u8]]) -> Result<Range<u64>>`
    *   Atomically writes multiple entries. Returns the range of Sequence IDs assigned.
*   `fn writer(&self, stream: &str) -> TxnWriter`
    *   Creates a `TxnWriter` helper for accumulating data in memory before committing it as a batch.

### Reading Data

IronWal supports both random access and sequential scanning.

*   `fn get(&self, stream: &str, id: u64) -> Result<Option<Vec<u8>>>`
    *   Retrieves a specific entry by its ID. Returns `None` if the ID does not exist or has been truncated.
*   `fn iter(&self, stream: &str, start_id: u64) -> Result<WalIterator>`
    *   Returns an iterator that yields entries sequentially starting from `start_id`. It seamlessly handles crossing segment file boundaries.

### Maintenance and Truncation

To manage disk usage, you can prune old data that has already been processed or snapshot.

*   `fn truncate(&self, stream: &str, safe_id: u64) -> Result<usize>`
    *   Deletes all segment files where *all* contained records have IDs strictly less than `safe_id`. This method guarantees that the active segment is never deleted, ensuring writer safety. Returns the number of deleted files.

## Sharded WAL (Optional Feature)

For applications that need to scale write throughput beyond a single stream, IronWal provides `ShardedWal` — a horizontal sharding extension with built-in checkpointing.

### When to Use Sharding

Use `ShardedWal` when:

* Write throughput exceeds ~100MB/s on a single stream
* You need consistent snapshots across multiple independent data streams
* You're building distributed systems requiring coordinated checkpoints (Raft, event sourcing)
* You want to parallelize writes across multiple physical disks

**Do not use** sharding if:

* Your write volume is low (<10MB/s)
* You need cross-key transactions (shards are independent)
* You need to change shard count dynamically (requires migration)

---

### Basic Usage

```rust
use ironwal::sharded::ShardedWal;
use ironwal::WalOptions;

// Enable the feature in Cargo.toml:
// ironwal = { version = "0.6", features = ["sharded"] }

let mut opts = WalOptions::default();
opts.root_path = "./data".into();

let wal = ShardedWal::new(opts, 16)?;

// Append with automatic routing
let (shard_id, seq_id) = wal.append(b"user_123", b"profile_data")?;
println!("Routed to shard {} at offset {}", shard_id, seq_id);
```

---

### Batch Writes

```rust
// Prepare batch
let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
    .map(|i| {
        let key = format!("user_{}", i).into_bytes();
        let value = format!("data_{}", i).into_bytes();
        (key, value)
    })
    .collect();

// Write batch (automatically grouped by shard)
let results = wal.append_batch(&batch)?;

// Results maintain input order
for ((key, _), (shard_id, seq_id)) in batch.iter().zip(results.iter()) {
    println!("{:?} -> shard {} at {}",
             String::from_utf8_lossy(key), shard_id, seq_id);
}
```

**Important:** Batches spanning multiple shards are **not atomic across shards**.
Each shard commits its portion independently.

---

### Checkpointing

Checkpoints capture a consistent point-in-time snapshot across all shards.

```rust
// Write some data
for i in 0..1000 {
    let key = format!("key_{}", i);
    wal.append(key.as_bytes(), b"data")?;
}

// Create checkpoint
wal.create_checkpoint(b"raft_index_5000")?;

// Or structured IDs
use ulid::Ulid;
let checkpoint_id = Ulid::new();
wal.create_checkpoint(checkpoint_id.to_string().as_bytes())?;
```

**Checkpoint ID Requirements**

* Must be non-empty
* Maximum 65,535 bytes
* Any binary data allowed (UTF-8 recommended)

---

### Loading Checkpoints

```rust
// Load specific checkpoint
let checkpoint = wal.load_checkpoint(b"raft_index_5000")?;

println!("Checkpoint created at: {}", checkpoint.timestamp);
println!("Shard count: {}", checkpoint.shard_count);

for (shard_id, offset) in checkpoint.offsets.iter().enumerate() {
    println!("Shard {}: offset {}", shard_id, offset);
}

// Load latest
let (checkpoint_id, checkpoint) = wal.load_latest_checkpoint()?;
println!("Latest: {:?}", String::from_utf8_lossy(&checkpoint_id));
```

---

### Reading Data

Unlike single-stream WAL, `ShardedWal` uses iterator-based reads and does **not** provide key `get()`.

```rust
let checkpoint = wal.load_checkpoint(b"snapshot_1")?;

for shard_id in 0..wal.shard_count() {
    let offset = checkpoint.offsets[shard_id as usize];

    // Skip shards with no data
    if offset == 0 {
        continue;
    }

    let mut iter = wal.iter_shard(shard_id, offset)?;

    while let Some(entry) = iter.next() {
        let data = entry?;
        process_entry(shard_id, data)?;
    }
}
```

---

### Maintenance

#### Pruning Old Data

```rust
wal.create_checkpoint(b"prune_before_this")?;

for i in 10000..20000 {
    wal.append(format!("key_{}", i).as_bytes(), b"data")?;
}

wal.create_checkpoint(b"current")?;

let stats = wal.prune_before_checkpoint(b"prune_before_this")?;

println!("Pruned {} segments from {} shards",
         stats.segments_deleted, stats.shards_pruned);
```

**Safety Guarantees**

* Active segments are never deleted
* Only complete segments strictly before checkpoint are removed
* Data at or after checkpoint remains intact

---

#### Checkpoint Log Compaction

```rust
for i in 0..1000 {
    wal.create_checkpoint(format!("ckpt_{}", i).as_bytes())?;
}

let stats = wal.compact_checkpoints(10)?;

println!("Compacted {} -> {} checkpoints",
         stats.checkpoints_before, stats.checkpoints_after);
println!("Reclaimed {} bytes", stats.bytes_reclaimed);
```

**When to Compact**

* Log exceeds size threshold (e.g. 10MB)
* Checkpoint count grows large (>1000)
* Periodic maintenance (cron)

---

### Shard Count Selection

| Write Throughput | Recommended Shards |
| ---------------- | ------------------ |
| < 50 MB/s        | 4–8                |
| 50–200 MB/s      | 16–32              |
| 200–500 MB/s     | 64–128             |
| > 500 MB/s       | 256+               |

**Rule of thumb:**
`shard_count ≈ write_throughput_MB_per_sec / 10`

---

### Architecture Details

* Each shard is a complete independent IronWal stream
* Keys routed via `hash(key) % shard_count`
* Checkpoints stored in append-only `checkpoints.log`
* `HEAD` file points to latest checkpoint for O(1) access
* Watermarks track durable shard offsets for consistency

For complete details, see `docs/SHARDED_WAL.md`.

## Error Handling

All operations return a `Result<T, ironwal::Error>`.

### Common Error Variants

*   **`Error::Io`**: Standard I/O errors (permissions, disk full, etc.).
*   **`Error::Corruption`**: Critical data integrity issues, such as invalid file headers or impossible seek offsets.
*   **`Error::CrcMismatch`**: The checksum of a read frame did not match the data on disk, indicating bit rot or partial writes.
*   **`Error::SegmentNotFound`**: Internal error where an index points to a missing file.
*   **`Error::StreamNotFound`**: Attempting an operation on a stream that has not been initialized (mostly internal).

## Recovery & Resilience

IronWal is designed to recover automatically from crashes.

*   **Startup Scan**: On initialization, the WAL scans directories to rebuild the internal state.
*   **Self-Healing**: If the `segments.idx` file is corrupt or missing, the WAL will rebuild it by scanning the actual segment files.
*   **Partial Writes**: If a crash occurs during a write, the WAL detects the truncated frame at the end of the log and treats it as non-existent (returning `None` on read). The next write will repair the file by truncating the garbage tail before appending.