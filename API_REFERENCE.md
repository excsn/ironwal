# IronWal API Reference

## 1. Introduction / Core Concepts

IronWal is a durable Write-Ahead Log library designed for sequential, append-only storage.

### Core Concepts
*   **Wal**: The primary handle for interacting with the log. It manages multiple "streams" of data.
*   **Stream**: A logical sequence of records identified by a string key.
*   **Entry**: A single unit of binary data stored in the log.
*   **Segment**: Physical files on disk that back the streams.
*   **Sequence ID**: A monotonically increasing `u64` identifier assigned to every entry within a stream.

### Primary Entry Point
*   `struct Wal`: The main struct used to append and read data. Initialized via `Wal::new(options)`.

### Common Patterns
*   **Result Type**: Most operations return `ironwal::Result<T>`, which aliases to `std::result::Result<T, ironwal::Error>`.
*   **Streams**: APIs universally take a `stream: &str` argument to scope operations to a specific log sequence.

## 2. Configuration

### `struct WalOptions`
Configuration for initializing the WAL.

**Fields:**
*   `pub root_path: PathBuf` - Directory where stream data is stored.
*   `pub max_segment_size: u64` - Soft limit for segment file size before rotation.
*   `pub max_entries_per_segment: u64` - Soft limit for entry count before rotation.
*   `pub max_open_segments: usize` - Capacity of the LRU cache for open **write** file descriptors. Default: 128.
*   `pub max_open_read_segments: usize` - Capacity of the LRU cache for open **read** file descriptors on rotated (immutable) segments. Active segments always use a transient handle and are not counted. Default: 128.
*   `pub write_buffer_size: usize` - Size of the in-memory write buffer (`BufWriter`).
*   `pub read_buffer_size: usize` - Size of the in-memory read buffer (`BufReader`).
*   `pub block_cache_size: Option<u64>` - Size of the read block cache (in bytes). `None` disables it.
*   `pub sync_mode: SyncMode` - Strategy for flushing data to disk.
*   `pub compression: CompressionType` - Compression algorithm for stored frames.
*   `pub read_strategy: ReadStrategy` - method used for reading segments (IO vs Mmap).
*   `pub on_corruption: CorruptionPolicy` - Action to take when corrupted data is found.
*   `pub min_compression_size: usize` - Minimum payload size required to trigger compression.
*   `pub archive_path: Option<PathBuf>` - Where `truncate` moves freed segments (`{archive_path}/{stream}/{segment}`) instead of deleting them; `restore_archived` moves them back. Must lie outside `root_path`. Default: None.

### `enum SyncMode`
Defines durability guarantees.
*   `Strict`: `fsync` after every append. Appends to one stream that land while an `fsync` runs share the next one.
*   `BatchOnly`: `fsync` on every batch or txn commit, shared the same way. A single `append` is not synced.
*   `Async`: Relies on OS background flush.

### `enum ReadStrategy`
Defines file access method.
*   `StandardIo`: Uses `std::fs::File` read/seek.
*   `Mmap`: Uses memory-mapped files.

### `enum CompressionType`
*   `None`: Raw storage.
*   `Lz4`: LZ4 frame compression.

### `enum CorruptionPolicy`
*   `Truncate`: Truncate file at corruption point.
*   `Error`: Return fatal error.

## 3. Main Types and Their Public Methods

### `struct Wal`
The primary interface for the library.

**Methods:**
*   `pub fn new(options: WalOptions) -> Result<Self>`
    *   Initializes the WAL, recovering state from disk if necessary.
*   `pub fn append(&self, stream: &str, entry: &[u8]) -> Result<u64>`
    *   Appends a single entry. Returns the new Sequence ID.
*   `pub fn append_batch(&self, stream: &str, entries: &[&[u8]]) -> Result<std::ops::Range<u64>>`
    *   Appends multiple entries atomically. Returns the range of assigned IDs. Outside `SyncMode::Async` it returns once the batch is synced.
*   `pub fn syncs(&self) -> u64`
    *   How many `fsync` calls the WAL has issued since it opened.
*   `pub fn writer(&self, stream: &str) -> TxnWriter`
    *   Creates a transaction writer for buffering appends in memory.
*   `pub fn get(&self, stream: &str, id: u64) -> Result<Option<Vec<u8>>>`
    *   Retrieves a specific entry by ID. Returns `None` if the ID is not found, has been truncated or the stream does not exist.
*   `pub fn get_batch(&self, stream: &str, ids: &[u64]) -> Result<Vec<Option<Vec<u8>>>>`
    *   Retrieves multiple entries in one call, grouped by segment. Results are in input order; missing IDs (and a missing stream) yield `None`.
*   `pub fn iter(&self, stream: &str, start_id: u64) -> Result<WalIterator>`
    *   Returns an iterator over entries starting from `start_id`. A `start_id` below the first retained entry (its segment freed by `truncate`) begins at the first retained entry; `iter_with_ids` reports where that was.
*   `pub fn iter_with_ids(&self, stream: &str, start_id: u64) -> Result<WalIdIterator>`
    *   Like `iter`, but yields `(id, entry)` pairs.
*   `pub fn truncate(&self, stream: &str, safe_id: u64) -> Result<usize>`
    *   Frees segments containing only records older than `safe_id`: deleted by default, moved to `archive_path` when one is configured. Returns the count freed. Never frees the active segment.
*   `pub fn restore_archived(&self, stream: &str) -> Result<usize>`
    *   Moves every archived segment of the stream back and rebuilds `segments.idx` from the segment files present, so the restored history reads again from its first ID. Returns the count restored, 0 with nothing archived or no `archive_path`. Idempotent and crash-safe (rerun to finish an interrupted restore). Stop appending to the stream before calling.
*   `pub fn delete_stream(&self, stream: &str) -> Result<()>`
    *   Removes a stream entirely: all segment files, its index and its in-memory state. A subsequent append recreates the stream starting from ID 0. Idempotent; crash-safe (an interrupted delete is completed on the next `Wal::new`). Stop appending to the stream before calling; a racing append may recreate the stream mid-deletion.
*   `pub fn current_segment_start_id(&self, stream: &str) -> Option<u64>`
    *   Returns the start ID of the active segment file (useful for monitoring).

### `struct TxnWriter`
Helper for building batches in memory.

**Methods:**
*   `pub fn add(&mut self, data: impl Into<Vec<u8>>)`
    *   Adds an item to the pending batch.
*   `pub fn commit(self) -> Result<std::ops::Range<u64>>`
    *   Writes all added items to the WAL as a single atomic batch.

### `struct WalIterator`
Iterator for sequential reads. Implements `Iterator<Item = Result<Vec<u8>>>`.

**Methods:**
*   (Standard Iterator methods like `next()`).

### `struct WalIdIterator`
Iterator for sequential reads carrying sequence IDs. Implements `Iterator<Item = Result<(u64, Vec<u8>)>>`.

**Methods:**
*   (Standard Iterator methods like `next()`).

## 4. Sharded WAL (Optional Feature)

Enable with `features = ["sharded"]` in `Cargo.toml`.

### `struct ShardedWal`
A horizontal sharding wrapper around `Wal` with consistent checkpointing.

**Construction:**
*   `pub fn new(opts: WalOptions, shard_count: u16) -> Result<Self>`
    *   Creates a sharded WAL with `shard_count` independent shards.
    *   `shard_count` must be > 0 and is fixed for the lifetime of the WAL.

**Write Operations:**
*   `pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(u16, u64)>`
    *   Appends `value` to the shard determined by `hash(key) % shard_count`.
    *   Returns `(shard_id, sequence_id)`.
*   `pub fn append_batch(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<(u16, u64)>>`
    *   Appends multiple entries, automatically grouped by shard.
    *   **Not atomic across shards** - each shard commits independently.
    *   Returns results in same order as input.
*   `pub fn append_batch_for_key(&self, key: &[u8], values: &[&[u8]]) -> Result<Vec<(u16, u64)>>`
    *   Appends a batch of values for a single key.
    *   Optimized version of `append_batch` that avoids repeated hashing.
    *   Returns vector of `(shard_id, sequence_id)`.

**Durability Controls:**
*   `pub fn flush_shard(&self, shard_id: u16) -> Result<()>`
    *   Flushes the write buffer and calls `fsync` on the active segment for `shard_id`, then advances its watermark to the current write position.
    *   **Required before `create_checkpoint`** when using `SyncMode::BatchOnly` or `Async` and writes were made via single `append` calls (not `append_batch`).
    *   Always performs `fsync` regardless of the configured `SyncMode`.
    *   Returns `Error::Config` if `shard_id >= shard_count`.
*   `pub fn flush_all(&self) -> Result<()>`
    *   Flushes and fsyncs all shards and advances all watermarks.
    *   Equivalent to calling `flush_shard` for every shard ID.
    *   The recommended call before `create_checkpoint` when operating under `SyncMode::BatchOnly` or `Async`.

**Checkpoint Operations:**
*   `pub fn create_checkpoint(&self, user_id: &[u8]) -> Result<()>`
    *   Creates a consistent snapshot of all shard offsets.
    *   `user_id` is a user-defined identifier (e.g., Raft index, transaction ID).
    *   Must be non-empty and ≤ 65535 bytes.
    *   Under `SyncMode::BatchOnly` or `Async`: call `flush_all()` before this to ensure single-append writes are reflected in the checkpoint offsets.
*   `pub fn load_checkpoint(&self, user_id: &[u8]) -> Result<CheckpointData>`
    *   Loads a specific checkpoint by its user-defined ID.
*   `pub fn load_latest_checkpoint(&self) -> Result<(Vec<u8>, CheckpointData)>`
    *   Loads the most recent checkpoint.
    *   Returns `(user_id, checkpoint_data)`.
*   `pub fn list_checkpoints(&self) -> Vec<(Vec<u8>, u64)>`
    *   Lists all checkpoints, sorted by timestamp (oldest to newest).
    *   Returns a vector of `(user_id, timestamp)` tuples.

**Read Operations:**
*   `pub fn iter_shard(&self, shard_id: u16, start_offset: u64) -> Result<WalIterator>`
    *   Returns an iterator for a specific shard starting at `start_offset`.
    *   This is the primary way to read data from a sharded WAL.
*   `pub fn iter_shard_with_ids(&self, shard_id: u16, start_offset: u64) -> Result<WalIdIterator>`
    *   Like `iter_shard`, but yields `(seq_id, entry)` pairs.
    *   Returns `Error::Config` if `shard_id >= shard_count`.

**Maintenance:**
*   `pub fn prune_before_checkpoint(&self, user_id: &[u8]) -> Result<PruneStats>`
    *   Deletes segments from all shards containing data before the checkpoint.
    *   Returns statistics about pruned segments.
*   `pub fn compact_checkpoints(&self, keep_latest_n: usize) -> Result<CompactionStats>`
    *   Compacts the checkpoint log, keeping only the `keep_latest_n` most recent.

**Accessors:**
*   `pub fn shard_count(&self) -> u16`
    *   Returns the number of shards.
*   `pub fn checkpoint_count(&self) -> usize`
    *   Returns the total number of checkpoints stored.
*   `pub fn inner(&self) -> &Wal`
    *   Returns a reference to the underlying `Wal` for advanced use cases.

### `struct CheckpointData`
Data returned when loading a checkpoint.

**Fields:**
*   `pub offsets: Vec<u64>` - Durable offset for each shard (indexed by shard_id).
*   `pub timestamp: u64` - Unix timestamp (nanoseconds) when checkpoint was created.
*   `pub shard_count: u16` - Number of shards (for validation).

### `struct PruneStats`
Statistics from a pruning operation.

**Fields:**
*   `pub shards_pruned: u16` - Number of shards that had segments deleted.
*   `pub segments_deleted: usize` - Total segments deleted across all shards.

### `struct CompactionStats`
Statistics from checkpoint log compaction.

**Fields:**
*   `pub checkpoints_before: usize` - Checkpoint count before compaction.
*   `pub checkpoints_after: usize` - Checkpoint count after compaction.
*   `pub bytes_reclaimed: u64` - Bytes freed from the checkpoint log.

### Error Variants (Sharded)

Additional error variants when using the `sharded` feature:

*   `CheckpointNotFound(String)`: Requested checkpoint does not exist.
*   `CheckpointCorrupted { offset: u64, reason: String }`: Checkpoint data is invalid.
*   `ShardCountMismatch { expected: u16, found: u16 }`: Checkpoint shard count differs from current configuration.
*   `InvalidCheckpointId(String)`: User-provided checkpoint ID is invalid (empty or too long).
*   `NoCheckpoints`: Attempted to load latest checkpoint when none exist.

### Usage Pattern Example
```rust
use ironwal::sharded::ShardedWal;
use ironwal::WalOptions;

// Initialize
let wal = ShardedWal::new(WalOptions::default(), 16)?;

// Write data
for i in 0..1000 {
    let key = format!("user_{}", i);
    wal.append(key.as_bytes(), b"data")?;
}

// Create checkpoint
wal.create_checkpoint(b"snapshot_1")?;

// Restore from checkpoint
let (id, checkpoint) = wal.load_latest_checkpoint()?;
for shard_id in 0..16 {
    let offset = checkpoint.offsets[shard_id as usize];
    let mut iter = wal.iter_shard(shard_id, offset)?;
    // Process entries...
}

// Cleanup
wal.prune_before_checkpoint(b"snapshot_1")?;
wal.compact_checkpoints(5)?;
```

## 9. Error Handling

### `enum Error`
The primary error type.

**Variants:**
*   `Io(std::io::Error)`: Underlying I/O failure.
*   `Corruption(String)`: Data integrity issue.
*   `CrcMismatch { expected: u32, actual: u32, offset: u64 }`: Checksum validation failed.
*   `Config(String)`: Invalid configuration.
*   `SegmentNotFound(u64)`: Internal index error.
*   `StreamNotFound(String)`: Operation on unknown stream.
*   `InvalidFilename(String)`: File system structure issue.
*   `Serialization(String)`: Error serializing internal state.

### `type Result<T>`
Alias for `std::result::Result<T, ironwal::Error>`.