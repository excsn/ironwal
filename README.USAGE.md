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