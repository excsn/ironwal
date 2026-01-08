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
*   `pub max_open_segments: usize` - Capacity of the LRU cache for open file descriptors.
*   `pub write_buffer_size: usize` - Size of the in-memory write buffer (`BufWriter`).
*   `pub read_buffer_size: usize` - Size of the in-memory read buffer (`BufReader`).
*   `pub block_cache_size: Option<u64>` - Size of the read block cache (in bytes). `None` disables it.
*   `pub sync_mode: SyncMode` - Strategy for flushing data to disk.
*   `pub compression: CompressionType` - Compression algorithm for stored frames.
*   `pub read_strategy: ReadStrategy` - method used for reading segments (IO vs Mmap).
*   `pub on_corruption: CorruptionPolicy` - Action to take when corrupted data is found.
*   `pub min_compression_size: usize` - Minimum payload size required to trigger compression.

### `enum SyncMode`
Defines durability guarantees.
*   `Strict`: `fsync` after every append.
*   `BatchOnly`: `fsync` only on batch/txn commit.
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
    *   Appends multiple entries atomically. Returns the range of assigned IDs.
*   `pub fn writer(&self, stream: &str) -> TxnWriter`
    *   Creates a transaction writer for buffering appends in memory.
*   `pub fn get(&self, stream: &str, id: u64) -> Result<Option<Vec<u8>>>`
    *   Retrieves a specific entry by ID. Returns `None` if not found.
*   `pub fn iter(&self, stream: &str, start_id: u64) -> Result<WalIterator>`
    *   Returns an iterator over entries starting from `start_id`.
*   `pub fn truncate(&self, stream: &str, safe_id: u64) -> Result<usize>`
    *   Deletes segments containing only records older than `safe_id`. Returns count of deleted segments.
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