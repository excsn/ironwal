use std::path::PathBuf;

/// Defines how often the WAL flushes data to the physical disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
  /// Call `fsync` after every single `append` operation.
  /// Safest, but highest latency.
  Strict,

  /// Call `fsync` only when `append_batch` is called, or when a
  /// `TxnWriter` is explicitly committed. Single `append` writes
  /// go to the OS buffer only.
  BatchOnly,

  /// Never call `fsync` automatically. Relies on the OS background
  /// flush mechanism. Fastest, but risks data loss on power failure.
  Async,
}

/// Defines the strategy used for reading segment files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStrategy {
  /// Uses standard `File::seek` and `File::read`.
  /// Safe, reliable, and respectful of memory limits.
  StandardIo,

  /// Memory-maps the segment files.
  /// Fastest for raw/uncompressed reads.
  /// WARNING: Counts against open file limits and carries SIGBUS risks.
  Mmap,
}

/// Defines the compression algorithm used for Frame Payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
  /// No compression. Raw binary copy.
  None,

  /// Uses the LZ4 Frame format. Supports concatenation and streaming.
  /// Good balance of speed and ratio.
  #[cfg(feature = "compression")]
  Lz4,
}

/// Policy for handling corrupted data during recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionPolicy {
  /// Truncate the file at the last valid record and log a warning.
  /// Allows the system to start, losing only the corrupted tail.
  Truncate,

  /// Return a fatal error. The system will not start until human
  /// intervention fixes the file.
  Error,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
  /// Base directory where stream directories are created.
  pub root_path: PathBuf,

  // --- Segment Rotation ---
  /// Soft limit for segment file size in bytes.
  /// A new segment is created if the current one exceeds this size.
  /// Default: 64 MB.
  pub max_segment_size: u64,

  /// Soft limit for number of entries per segment.
  /// A new segment is created if the current one exceeds this count.
  /// Default: u64::MAX (Disabled).
  pub max_entries_per_segment: u64,

  // --- Resources ---
  /// Maximum number of file descriptors to keep open simultaneously
  /// for writing. Uses an LRU policy to enforce this limit.
  /// Default: 128.
  pub max_open_segments: usize,

  /// Maximum number of file descriptors to keep open for reading rotated
  /// (immutable) segments. Active segments always use a transient handle
  /// and are not counted against this limit.
  /// Default: 128.
  pub max_open_read_segments: usize,

  /// Size of the in-memory write buffer before flushing to the OS kernel.
  /// Default: 64 KB.
  pub write_buffer_size: usize,

  /// Size of the buffer used for reading/scanning files.
  /// Default: 128 KB.
  pub read_buffer_size: usize,

  /// Size of the Read Block Cache (in bytes).
  /// If None, caching is disabled.
  /// Default: None.
  pub block_cache_size: Option<u64>,

  // --- Behavior ---
  pub sync_mode: SyncMode,

  /// Under `SyncMode::Async`, keeps appends in the write buffer until it fills, a read of the stream, a segment
  /// roll, a flush or the WAL closing, instead of writing each append to the OS before it returns. Saves a `write`
  /// call per append. Appends that have returned can be lost if the process dies, up to `write_buffer_size` bytes
  /// per stream. Without it only an OS crash or power loss can lose an append. Ignored in the other sync modes.
  /// Default: false.
  pub buffer_appends: bool,
  pub compression: CompressionType,
  pub read_strategy: ReadStrategy,
  pub on_corruption: CorruptionPolicy,

  /// If a batch is smaller than this (in bytes), it will be written
  /// uncompressed even if `compression` is enabled.
  /// Default: 1 KB.
  pub min_compression_size: usize,

  /// Where `truncate` moves the segments it frees instead of unlinking them, laid out as
  /// `{archive_path}/{stream}/{segment}`; `restore_archived` moves them back. Must lie
  /// outside `root_path`. Default: None (freed segments are deleted).
  pub archive_path: Option<PathBuf>,
}

impl Default for WalOptions {
  fn default() -> Self {
    Self {
      root_path: PathBuf::from("./wal_data"),
      max_segment_size: 64 * 1024 * 1024, // 64 MB
      max_entries_per_segment: u64::MAX,
      max_open_segments: 128,
      max_open_read_segments: 128,
      write_buffer_size: 64 * 1024, // 64 KB
      read_buffer_size: 128 * 1024, // 128 KB
      block_cache_size: None,
      sync_mode: SyncMode::Strict,
      buffer_appends: false,
      compression: CompressionType::None,
      read_strategy: ReadStrategy::StandardIo,
      on_corruption: CorruptionPolicy::Truncate,
      min_compression_size: 1024, // 1 KB
      archive_path: None,
    }
  }
}

impl WalOptions {
  pub fn new(path: impl Into<PathBuf>) -> Self {
    Self {
      root_path: path.into(),
      ..Default::default()
    }
  }
}
