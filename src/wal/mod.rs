mod stream_map;

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use byteorder::{LittleEndian, WriteBytesExt};
use fibre_cache::policy::lru::LruPolicy;
use fibre_cache::{Cache, CacheBuilder, EvictionListener, EvictionReason};
use parking_lot::RwLock;
use tracing::error;

use crate::batch::TxnWriter;
use crate::config::{SyncMode, WalOptions};
use crate::error::{Error, Result};
use crate::index::{self, SegmentIndex};
use crate::segment::{ActiveSegment, CachedReadDescriptor, FrameLocation, SegmentReader};
use crate::state::{StreamState, StreamStateFile};
use crate::util::{self, segment_filename};
use crate::wal::stream_map::StreamStateMap;

/// A unique identifier for a cached data block (a decompressed frame).
/// This composite key prevents collisions between different streams.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockCacheKey {
  /// A hash of the stream key to keep the key size manageable.
  /// Known trade-off: a 64-bit cross-stream hash collision (combined with an
  /// identical segment start ID and frame offset) would serve cached entries
  /// from the wrong stream. The probability is negligible in practice.
  pub stream_hash: u64,
  /// The start ID of the segment file containing the block.
  pub segment_start_id: u64,
  /// The byte offset of the frame's header within the segment file.
  pub frame_offset: u64,
}

pub struct WalState {
  options: WalOptions,

  /// fsyncs issued since the WAL opened.
  syncs: Arc<AtomicU64>,

  /// One gate per stream that writes wait at for their fsync.
  gates: RwLock<HashMap<String, Arc<SyncGate>>>,

  // Manages per-stream locking internally.
  streams: StreamStateMap,

  // Caches memory-mapped index files (segments.idx)
  index_cache: Cache<PathBuf, SegmentIndex>,

  /// Cache of open file descriptors for WRITING.
  /// Strictly limited by `max_open_segments`.
  write_cache: Cache<PathBuf, ActiveSegment>,

  /// Cache for READ blocks (decompressed frames).
  /// Maps (Stream, File, Offset) -> Decompressed Batch
  read_cache: Option<Cache<BlockCacheKey, Vec<Vec<u8>>>>,

  /// Cache of open file descriptors for READING rotated (immutable) segments.
  /// Active segments are always bypassed and use a transient handle instead.
  /// Under high read concurrency, multiple threads may race to insert the same
  /// entry; this is intentionally benign — both opens are read-only and the
  /// last writer simply overwrites with an equivalent descriptor.
  read_descriptor_cache: Cache<PathBuf, CachedReadDescriptor>,

  /// Per-stream generation counters, bumped by [`Wal::delete_stream`] and
  /// folded into the block cache hash. A recreated stream regenerates
  /// identical `(segment_start_id, frame_offset)` pairs, so without the salt
  /// the block cache would serve blocks from the deleted incarnation.
  stream_generations: RwLock<HashMap<PathBuf, u64>>,
}

impl WalState {
  fn stream_cache_hash(&self, stream: &str, stream_dir: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    stream.hash(&mut hasher);
    let generation = self
      .stream_generations
      .read()
      .get(stream_dir)
      .copied()
      .unwrap_or(0);
    generation.hash(&mut hasher);
    hasher.finish()
  }

  fn get_or_create_segment(
    &self,
    stream: &str,
    state: &mut StreamState,
  ) -> Result<Arc<ActiveSegment>> {
    let stream_dir = util::ensure_safe_path(&self.options.root_path, stream)?;
    if !stream_dir.exists() {
      fs::create_dir_all(&stream_dir)?;
      // If we just created the stream, we MUST create the index with the first segment ID (0).
      index::append_index(&stream_dir, 0)?;
    }

    let segment_path = stream_dir.join(segment_filename(state.active_segment_start_id));

    // 1. Optimistic fetch.
    if let Some(segment) = self.write_cache.fetch(&segment_path) {
      return Ok(segment);
    }

    // 2. On miss, perform the I/O operation
    let exists = segment_path.exists();
    let new_segment = ActiveSegment::create(
      segment_path.clone(),
      state.active_segment_start_id,
      &self.options,
      self.syncs.clone(),
    )?;
    if !exists && self.options.sync_mode != SyncMode::Async {
      util::sync_dir(&stream_dir, &self.syncs)?;
    }

    if exists && !state.tail_repaired {
      // Opening an existing file whose tail we haven't verified in this
      // process: repair to ensure we don't append after a corrupted tail.
      // Reopens after LRU eviction skip this — eviction flushes the buffer,
      // so the tail cannot be torn within a process lifetime.
      new_segment.repair()?;
    }
    state.tail_repaired = true;

    // 3. Insert the known-good segment into the cache. The cost is 1 (one file handle).
    self
      .write_cache
      .insert(segment_path.clone(), new_segment, 1);

    // 4. Fetch it again to get the Arc managed by the cache.
    Ok(self.write_cache.fetch(&segment_path).unwrap())
  }

  /// Rotates the active segment for a stream.
  /// Handles index updates, cache invalidation, and state file persistence.
  fn rotate_segment(&self, stream: &str, state: &mut StreamState) -> Result<Arc<ActiveSegment>> {
    let stream_dir = util::ensure_safe_path(&self.options.root_path, stream)?;

    let old_segment_path = stream_dir.join(segment_filename(state.active_segment_start_id));
    let index_path = stream_dir.join(SegmentIndex::FILENAME);

    // 1. Force flush and close of the old segment via cache invalidation.
    // Eviction only hands the buffer to the OS outside Strict mode, and a batch that spans
    // the rotation has entries in this file that its caller waits to see synced.
    if self.options.sync_mode != SyncMode::Async
      && let Some(old) = self.write_cache.fetch(&old_segment_path)
    {
      old.flush()?;
    }
    self.write_cache.invalidate(&old_segment_path);

    // 2. Advance the ID in memory
    state.active_segment_start_id = state.next_id;

    // 3. Update Index: Append new start ID to segments.idx
    index::append_index(&stream_dir, state.active_segment_start_id)?;
    // Invalidate the index cache because the file on disk has changed
    self.index_cache.invalidate(&index_path);

    // 4. Update Head State
    let head_state = StreamStateFile {
      active_segment_name: segment_filename(state.active_segment_start_id),
      version: StreamStateFile::VERSION,
    };
    head_state.write_to(&stream_dir)?;

    // 5. Create/Get the new segment. Outside Async mode this syncs the stream directory, which makes
    // the new segment file and the head rename durable. The index and the head are not synced.
    self.get_or_create_segment(stream, state)
  }

  fn gate(&self, stream: &str) -> Arc<SyncGate> {
    if let Some(gate) = self.gates.read().get(stream) {
      return gate.clone();
    }
    self.gates.write().entry(stream.to_string()).or_insert_with(|| Arc::new(SyncGate::default())).clone()
  }

  /// Returns once every entry before `end` is synced. One caller syncs for everyone written by then
  /// and the others wait for it.
  fn sync_through(&self, stream: &str, stream_lock: &parking_lot::Mutex<StreamState>, end: u64) -> Result<()> {
    let gate = self.gate(stream);
    let mut held = gate.state.lock();
    loop {
      if held.synced >= end {
        return Ok(());
      }
      if held.syncing {
        gate.done.wait(&mut held);
        continue;
      }
      held.syncing = true;
      let target = held.written;
      drop(held);
      let result = (|| {
        let segment = {
          let mut state = stream_lock.lock();
          self.get_or_create_segment(stream, &mut state)?
        };
        segment.flush()
      })();
      held = gate.state.lock();
      held.syncing = false;
      if result.is_ok() {
        held.synced = held.synced.max(target);
      }
      gate.done.notify_all();
      result?;
    }
  }

  fn write_one(&self, stream: &str, state: &mut StreamState, entry: &[&[u8]]) -> Result<u64> {
    let segment = self.get_or_create_segment(stream, state)?;

    // --- Check for Rotation ---
    // Estimate: payload size + fixed frame overhead
    // Entry count is derived from the sequencer state (exact across restarts
    // and descriptor reopens, unlike a per-file-handle counter).
    let entry_size_estimate = (crate::frame::FrameHeader::SIZE + 4 + entry[0].len()) as u64;
    let entries_in_segment = state.next_id - state.active_segment_start_id;
    let needs_rotation = (segment.size() + entry_size_estimate) > self.options.max_segment_size
      || entries_in_segment >= self.options.max_entries_per_segment;

    let final_segment = if needs_rotation {
      self.rotate_segment(stream, state)?
    } else {
      segment
    };

    // --- Write the entry ---
    let range = final_segment.append(entry, state.next_id, &self.options)?;
    state.next_id = range.end;

    final_segment.flush_buffer()?;

    Ok(range.start)
  }

  fn write_batch(
    &self,
    stream: &str,
    state: &mut StreamState,
    entries: &[&[u8]],
  ) -> Result<std::ops::Range<u64>> {
    if entries.is_empty() {
      return Ok(state.next_id..state.next_id);
    }

    let overall_start_id = state.next_id;
    let mut entries_processed = 0;

    while entries_processed < entries.len() {
      let segment = self.get_or_create_segment(stream, state)?;

      // --- Calculate how many entries can fit ---
      let remaining_space = self.options.max_segment_size.saturating_sub(segment.size());
      let mut chunk_end_idx = entries_processed;
      let mut chunk_byte_size = crate::frame::FrameHeader::SIZE as u64;

      for entry in &entries[entries_processed..] {
        let entry_bytes = (entry.len() + 4) as u64;
        if chunk_byte_size + entry_bytes > remaining_space {
          break;
        }
        chunk_byte_size += entry_bytes;
        chunk_end_idx += 1;
      }

      let force_write = chunk_end_idx == entries_processed && segment.size() == 0;

      // If we can't fit anything (and the segment isn't fresh), we rotate immediately.
      if chunk_end_idx == entries_processed && !force_write {
        self.rotate_segment(stream, state)?;
        continue;
      }

      // We have a valid chunk (or a giant entry on a fresh segment)
      if chunk_end_idx == entries_processed {
        chunk_end_idx += 1;
      }

      let chunk = &entries[entries_processed..chunk_end_idx];

      // --- Write the chunk ---
      let range = segment.append(chunk, state.next_id, &self.options)?;
      state.next_id = range.end;
      entries_processed += chunk.len();

      // --- Check for Rotation (After Write) ---
      // The Arc we appended through still tracks the file size; no need to
      // re-fetch from the cache.
      let entries_in_segment = state.next_id - state.active_segment_start_id;
      let needs_rotation = entries_processed < entries.len() // More data to write
        && (segment.size() >= self.options.max_segment_size
        || entries_in_segment >= self.options.max_entries_per_segment);

      if needs_rotation {
        self.rotate_segment(stream, state)?;
      }
    }

    // Flush the userspace BufWriter to the OS kernel so that subsequent reads on new
    // file descriptors can immediately see the data. The fsync, when the mode asks for
    // one, happens after the stream's lock is released, so concurrent writers share it.
    let segment = self.get_or_create_segment(stream, state)?;
    segment.flush_buffer()?;

    Ok(overall_start_id..state.next_id)
  }
}

#[derive(Default)]
struct GateState {
  /// Every entry before this is written to the OS.
  written: u64,
  /// Every entry before this is synced.
  synced: u64,
  syncing: bool,
}

#[derive(Default)]
struct SyncGate {
  state: parking_lot::Mutex<GateState>,
  done: parking_lot::Condvar,
}

impl SyncGate {
  fn written(&self, end: u64) {
    let mut held = self.state.lock();
    held.written = held.written.max(end);
  }
}

/// Listener to ensure data is flushed when a file descriptor is evicted/closed.
#[derive(Clone, Copy)]
struct FlushOnEvict {
  sync_mode: SyncMode,
}

impl EvictionListener<PathBuf, ActiveSegment> for FlushOnEvict {
  fn on_evict(&self, key: PathBuf, segment: Arc<ActiveSegment>, _reason: EvictionReason) {
    let result = match self.sync_mode {
      // In Strict mode, we must ensure data is physically on disk before closing.
      SyncMode::Strict => segment.flush(),
      // In other modes, we just need to ensure the userspace buffer is handed to the OS.
      // The OS will handle the physical write in the background.
      _ => segment.flush_buffer(),
    };

    if let Err(e) = result {
      error!(target: "ironwal", "Failed to flush segment on eviction. Data loss possible. Path: {:?}, Error: {}", key, e);
    }
  }
}

/// The main entry point for the IronWal library.
/// Thread-safe and cloneable (internally strictly synchronized).
#[derive(Clone)]
pub struct Wal {
  inner: Arc<WalState>,
}

impl Wal {
  pub fn new(options: WalOptions) -> Result<Self> {
    fs::create_dir_all(&options.root_path)?;

    // 1. Initialize caches
    let index_cache = CacheBuilder::new()
      .shards(1)
      .capacity(options.max_open_segments as u64)
      .cache_policy_factory(|| Box::new(LruPolicy::new()))
      .build()
      .map_err(|e| Error::Config(format!("Failed to build index cache: {}", e)))?;

    let write_cache = CacheBuilder::new()
      .shards(1)
      .capacity(options.max_open_segments as u64)
      .cache_policy_factory(|| Box::new(LruPolicy::new()))
      .maintenance_chance(1)
      .eviction_listener(FlushOnEvict {
        sync_mode: options.sync_mode,
      })
      .build()
      .map_err(|e| Error::Config(format!("Failed to build write cache: {}", e)))?;

    let read_cache = if let Some(size) = options.block_cache_size {
      let cache = CacheBuilder::new()
        .capacity(size)
        .build()
        .map_err(|e| Error::Config(format!("Failed to build read cache: {}", e)))?;
      Some(cache)
    } else {
      None
    };

    let read_descriptor_cache = CacheBuilder::new()
      .shards(4)
      .capacity(options.max_open_read_segments as u64)
      .cache_policy_factory(|| Box::new(LruPolicy::new()))
      .build()
      .map_err(|e| Error::Config(format!("Failed to build read descriptor cache: {}", e)))?;

    // 2. Scan and recover state from filesystem
    let syncs = Arc::new(AtomicU64::new(0));
    let streams_map = Self::recover_all_streams(&options, &index_cache, &syncs)?;
    let streams = StreamStateMap::new(streams_map);

    Ok(Self {
      inner: Arc::new(WalState {
        options,
        syncs,
        gates: RwLock::new(HashMap::new()),
        streams,
        index_cache,
        write_cache,
        read_cache,
        read_descriptor_cache,
        stream_generations: RwLock::new(HashMap::new()),
      }),
    })
  }

  /// Returns a cached `CachedReadDescriptor` for a rotated segment, opening the
  /// file if the cache does not already hold it.
  ///
  /// Under concurrent read pressure, multiple threads may race on a cold path
  /// and each open the file independently before the first insert completes.
  /// This is intentionally benign: the segment is read-only and the last
  /// insert simply replaces an equivalent descriptor.
  fn get_or_load_read_descriptor(&self, path: &PathBuf) -> Result<Arc<CachedReadDescriptor>> {
    if let Some(d) = self.inner.read_descriptor_cache.fetch(path) {
      return Ok(d);
    }
    let desc = CachedReadDescriptor::open(path, &self.inner.options)?;
    self.inner.read_descriptor_cache.insert(path.clone(), desc, 1);
    Ok(self.inner.read_descriptor_cache.fetch(path).unwrap())
  }

  /// Returns the Start ID of the currently active segment.
  /// Useful for monitoring rotation and testing.
  pub fn current_segment_start_id(&self, stream: &str) -> Option<u64> {
    self
      .inner
      .streams
      .get(stream)
      .map(|s| s.lock().active_segment_start_id)
  }

  /// Creates an iterator to read entries sequentially starting from `start_id`.
  ///
  /// Note: if `start_id` falls in a region already deleted by [`Wal::truncate`],
  /// the iterator yields nothing. Start from a known-surviving ID (e.g. the
  /// `safe_id` passed to the last truncation) to read the remaining history.
  pub fn iter(&self, stream: &str, start_id: u64) -> Result<crate::iter::WalIterator> {
    crate::iter::WalIterator::new(self.clone(), stream.to_string(), start_id)
  }

  /// Like [`Wal::iter`], but yields `(id, entry)` pairs.
  pub fn iter_with_ids(&self, stream: &str, start_id: u64) -> Result<crate::iter::WalIdIterator> {
    crate::iter::WalIdIterator::new(self.clone(), stream.to_string(), start_id)
  }

  // Helper for Iterator to resolve paths (since fields are private/wrapped)
  pub(crate) fn get_stream_dir(&self, stream: &str) -> Result<std::path::PathBuf> {
    util::ensure_safe_path(&self.inner.options.root_path, stream)
  }

  pub(crate) fn options(&self) -> WalOptions {
    self.inner.options.clone()
  }

  // Helper to interact with the index cache
  pub(crate) fn get_or_load_index(
    &self,
    stream_dir: &std::path::Path,
    idx_path: &std::path::Path,
  ) -> Result<Arc<SegmentIndex>> {
    if let Some(idx) = self.inner.index_cache.fetch(idx_path) {
      Ok(idx)
    } else {
      let idx = SegmentIndex::open(stream_dir)?;
      self
        .inner
        .index_cache
        .insert(idx_path.to_path_buf(), idx, 1);
      Ok(self.inner.index_cache.fetch(idx_path).unwrap())
    }
  }

  /// Scans all subdirectories to recover the state of each stream.
  fn recover_all_streams(
    options: &WalOptions,
    index_cache: &Cache<PathBuf, SegmentIndex>,
    syncs: &AtomicU64,
  ) -> Result<HashMap<String, StreamState>> {
    let mut streams = HashMap::new();
    if !options.root_path.exists() {
      return Ok(streams);
    }

    for entry in fs::read_dir(&options.root_path)? {
      let entry = entry?;
      if entry.file_type()?.is_dir() {
        let stream_key = entry.file_name().to_string_lossy().to_string();
        let stream_dir = entry.path();

        if stream_key.ends_with(util::DELETE_MARKER_SUFFIX) {
          // Leftover from a delete_stream interrupted by a crash.
          if let Err(e) = fs::remove_dir_all(&stream_dir) {
            tracing::warn!(target: "ironwal", "Failed to remove delete marker {}: {}", stream_dir.display(), e);
          }
          continue;
        }

        if let Some(state) = Self::recover_stream(&stream_dir, options, index_cache, syncs)? {
          streams.insert(stream_key, state);
        }
      }
    }
    Ok(streams)
  }

  /// Recovers the state for a single stream directory.
  fn recover_stream(
    stream_dir: &Path,
    options: &WalOptions,
    index_cache: &Cache<PathBuf, SegmentIndex>,
    syncs: &AtomicU64,
  ) -> Result<Option<StreamState>> {
    let index_path = stream_dir.join("segments.idx");

    // 1. Initial Index Load. Rolls do not sync the index, so it can lag the segment files.
    let newest_on_disk = Self::segment_ids_on_disk(stream_dir)?.into_iter().max();
    let index_current = SegmentIndex::open(stream_dir).ok().is_some_and(|index| index.iter().last() >= newest_on_disk);
    if !index_current {
      Self::rebuild_index_from_scan(stream_dir, syncs)?;
    }
    let mut index = index_cache
      .fetch(&index_path)
      .or_else(|| {
        let idx = SegmentIndex::open(stream_dir).ok()?;
        index_cache.insert(index_path.clone(), idx, 1);
        index_cache.fetch(&index_path)
      })
      .unwrap();

    // 2. Determine Active Segment from Head State
    let head_state = StreamStateFile::read_from(stream_dir).unwrap_or_else(|e| {
      tracing::warn!(target: "ironwal", "Ignoring unreadable head.state in {}: {}", stream_dir.display(), e);
      None
    });
    let mut active_segment_start_id = 0;
    if let Some(state) = head_state {
      if let Some(id) = util::parse_segment_id(&state.active_segment_name) {
        active_segment_start_id = id;
      }
    }

    // 3. Check for Zombie Index (Index is stale compared to Head)
    // The index should contain the active segment.
    let last_index_id = index.iter().last().unwrap_or(0);

    if active_segment_start_id > last_index_id {
      tracing::warn!(target: "ironwal", "Index is stale (Last: {}, Active: {}). Rebuilding...", last_index_id, active_segment_start_id);

      // Force Rebuild
      Self::rebuild_index_from_scan(stream_dir, syncs)?;

      // Invalidate and Reload
      index_cache.invalidate(&index_path);
      let new_idx = SegmentIndex::open(stream_dir)?;
      index_cache.insert(index_path.clone(), new_idx, 1);

      // Update our reference
      index = index_cache.fetch(&index_path).unwrap();
    }

    // Also respect the index if it claims a higher ID than head (rare, but possible if head write failed)
    let updated_last = index.iter().last().unwrap_or(0);
    if updated_last > active_segment_start_id {
      active_segment_start_id = updated_last;
    }

    let active_segment_path = stream_dir.join(util::segment_filename(active_segment_start_id));
    if !active_segment_path.exists() {
      return Ok(Some(StreamState {
        next_id: active_segment_start_id,
        active_segment_start_id,
        tail_repaired: true,
      }));
    }

    let mut reader = SegmentReader::open(&active_segment_path, options)?;
    let (entries_in_segment, valid_len) = reader.recover_scan()?;
    drop(reader); // Release any mmap before truncating below.

    // Handle a torn tail (crash mid-write) right away, so readers never
    // encounter it and the counted entries always match the file contents.
    let disk_len = fs::metadata(&active_segment_path)?.len();
    if valid_len < disk_len {
      if options.on_corruption == crate::config::CorruptionPolicy::Error {
        return Err(Error::Corruption(format!(
          "Torn tail in {}: {} valid bytes of {} on disk",
          active_segment_path.display(),
          valid_len,
          disk_len
        )));
      }
      tracing::warn!(
        target: "ironwal",
        "Truncating torn tail of {} from {} to {} bytes",
        active_segment_path.display(), disk_len, valid_len
      );
      let file = OpenOptions::new().write(true).open(&active_segment_path)?;
      file.set_len(valid_len)?;
      util::sync_file(&file, syncs)?;
    }

    let next_id = active_segment_start_id + entries_in_segment;

    Ok(Some(StreamState {
      next_id,
      active_segment_start_id,
      tail_repaired: true,
    }))
  }

  /// Slow-path recovery: Scans a stream directory for all `.wal` files
  /// and rebuilds the `segments.idx` file from scratch.
  fn rebuild_index_from_scan(stream_dir: &Path, syncs: &AtomicU64) -> Result<()> {
    let mut start_ids = Self::segment_ids_on_disk(stream_dir)?;
    start_ids.sort_unstable();

    let temp_path = stream_dir.join("segments.idx.tmp");
    let final_path = stream_dir.join("segments.idx");

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&temp_path)?;
    for id in start_ids {
      file.write_u64::<LittleEndian>(id)?;
    }
    util::sync_file(&file, syncs)?;
    drop(file);

    fs::rename(&temp_path, &final_path)?;

    Ok(())
  }

  /// The start ID of every segment file in `stream_dir`, in directory order.
  fn segment_ids_on_disk(stream_dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(stream_dir)? {
      if let Some(id) = util::parse_segment_id(&entry?.file_name().to_string_lossy()) {
        ids.push(id);
      }
    }
    Ok(ids)
  }

  // --- Write Operations ---

  /// Appends a single entry to the stream.
  ///
  /// Note: stream names are sanitized to a filesystem-safe character set;
  /// names that differ only in replaced characters (e.g. `"a/b"` and `"a_b"`)
  /// alias to the same stream.
  pub fn append(&self, stream: &str, entry: &[u8]) -> Result<u64> {
    let stream_lock = self.inner.streams.get_or_create(stream);
    let id = {
      let mut state = stream_lock.lock();
      let id = self.inner.write_one(stream, &mut state, &[entry])?;
      self.inner.gate(stream).written(id + 1);
      id
    };
    if self.inner.options.sync_mode == SyncMode::Strict {
      self.inner.sync_through(stream, &stream_lock, id + 1)?;
    }
    Ok(id)
  }

  /// How many fsyncs the WAL has issued since it opened.
  pub fn syncs(&self) -> u64 {
    self.inner.syncs.load(Ordering::Relaxed)
  }

  /// Appends a batch of entries atomically to the WAL.
  ///
  /// Outside `SyncMode::Async` it returns once the batch is synced. Batches written on one
  /// stream while an fsync runs share the next one.
  pub fn append_batch(&self, stream: &str, entries: &[&[u8]]) -> Result<std::ops::Range<u64>> {
    let stream_lock = self.inner.streams.get_or_create(stream);
    let range = {
      let mut state = stream_lock.lock();
      let range = self.inner.write_batch(stream, &mut state, entries)?;
      self.inner.gate(stream).written(range.end);
      range
    };
    if self.inner.options.sync_mode != SyncMode::Async && !range.is_empty() {
      self.inner.sync_through(stream, &stream_lock, range.end)?;
    }
    Ok(range)
  }

  /// Returns a `TxnWriter` handle for accumulating a batch of writes in memory.
  pub fn writer(&self, stream: &str) -> TxnWriter {
    TxnWriter::new(self.clone(), stream.to_string())
  }

  // --- Read Operations ---

  /// Reads a single entry by its sequence ID.
  pub fn get(&self, stream: &str, id: u64) -> Result<Option<Vec<u8>>> {
    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;

    // 1. Locate the segment that contains this ID via the index cache.
    let index = {
      let idx_path = stream_dir.join(SegmentIndex::FILENAME);
      if let Some(idx) = self.inner.index_cache.fetch(&idx_path) {
        idx
      } else {
        // A missing index means the stream doesn't exist (never written, or
        // removed by delete_stream).
        let idx = match SegmentIndex::open(&stream_dir) {
          Ok(idx) => idx,
          Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
          Err(e) => return Err(e),
        };
        self.inner.index_cache.insert(idx_path.clone(), idx, 1);
        self.inner.index_cache.fetch(&idx_path).unwrap()
      }
    };

    let segment_start_id = index.find_segment_start_id(id)?;
    let segment_path = stream_dir.join(segment_filename(segment_start_id));

    if !segment_path.exists() {
      return Ok(None);
    }

    // 2. Compute stream hash once (used for the block cache key in both paths).
    let stream_hash = self.inner.stream_cache_hash(stream, &stream_dir);

    // 3. Determine whether the target segment is still being actively written to.
    //    Active segments bypass the read descriptor cache to avoid SIGBUS and
    //    stale-EOF risks from a file that is currently growing.
    //    A rotation that occurs between this check and the read is safe: the file
    //    simply stops growing, so opening it as "active" is still correct.
    let is_active = self.inner.streams
      .get(stream)
      .map_or(false, |lock| lock.lock().active_segment_start_id == segment_start_id);

    if is_active {
      // Transient handle respecting the configured ReadStrategy — bypasses the
      // persistent descriptor cache. Safe for mmap: file only grows, reader
      // bounds-checks against the snapshot length, SIGBUS is impossible.
      let mut reader = match SegmentReader::open(&segment_path, &self.inner.options) {
        Ok(r) => r,
        // Deleted by a concurrent truncate between the exists() check and here.
        Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
      };

      let frame_loc = match reader.find_frame(id, None)? {
        Some(loc) => loc,
        None => return Ok(None),
      };

      let cache_key = BlockCacheKey { stream_hash, segment_start_id, frame_offset: frame_loc.offset };

      if let Some(read_cache) = &self.inner.read_cache {
        if let Some(batch) = read_cache.fetch(&cache_key) {
          let idx = (id - frame_loc.header.start_id) as usize;
          return if idx < batch.len() {
            Ok(Some(batch[idx].clone()))
          } else {
            Err(Error::Corruption("Found frame but ID offset exceeds batch length".into()))
          };
        }
      }

      let batch = match reader.read_at(&frame_loc) {
        Ok(b) => b,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
      };

      let idx = (id - frame_loc.header.start_id) as usize;
      if idx >= batch.len() {
        return Err(Error::Corruption("Found frame but ID offset exceeds batch length".into()));
      }
      let result = batch[idx].clone();
      if let Some(read_cache) = &self.inner.read_cache {
        let weight = batch.iter().map(|v| v.len() as u64).sum::<u64>();
        read_cache.insert(cache_key, batch, weight.max(1));
      }
      Ok(Some(result))

    } else {
      // Rotated (immutable) segment — use (or populate) the read descriptor cache.
      let desc = match self.get_or_load_read_descriptor(&segment_path) {
        Ok(d) => d,
        // Deleted by a concurrent truncate between the exists() check and here.
        Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
      };

      let frame_loc = match desc.find_frame(id)? {
        Some(loc) => loc,
        None => return Ok(None),
      };

      let cache_key = BlockCacheKey { stream_hash, segment_start_id, frame_offset: frame_loc.offset };

      if let Some(read_cache) = &self.inner.read_cache {
        if let Some(batch) = read_cache.fetch(&cache_key) {
          let idx = (id - frame_loc.header.start_id) as usize;
          return if idx < batch.len() {
            Ok(Some(batch[idx].clone()))
          } else {
            Err(Error::Corruption("Found frame but ID offset exceeds batch length".into()))
          };
        }
      }

      let batch = desc.read_frame(&frame_loc)?;

      let idx = (id - frame_loc.header.start_id) as usize;
      if idx >= batch.len() {
        return Err(Error::Corruption("Found frame but ID offset exceeds batch length".into()));
      }
      let result = batch[idx].clone();
      if let Some(read_cache) = &self.inner.read_cache {
        let weight = batch.iter().map(|v| v.len() as u64).sum::<u64>();
        read_cache.insert(cache_key, batch, weight.max(1));
      }
      Ok(Some(result))
    }
  }

  /// Retrieves multiple entries by their IDs in a single operation.
  /// This is significantly more efficient than calling `get()` multiple times
  /// when using StandardIO read strategy.
  pub fn get_batch(&self, stream: &str, ids: &[u64]) -> Result<Vec<Option<Vec<u8>>>> {
    if ids.is_empty() {
      return Ok(Vec::new());
    }

    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;
    let idx_path = stream_dir.join(SegmentIndex::FILENAME);

    // Load index once.
    let index = if let Some(idx) = self.inner.index_cache.fetch(&idx_path) {
      idx
    } else {
      // A missing index means the stream doesn't exist (never written, or
      // removed by delete_stream).
      let idx = match SegmentIndex::open(&stream_dir) {
        Ok(idx) => idx,
        Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
          return Ok(vec![None; ids.len()]);
        }
        Err(e) => return Err(e),
      };
      self.inner.index_cache.insert(idx_path.clone(), idx, 1);
      self.inner.index_cache.fetch(&idx_path).unwrap()
    };

    // Pre-compute stream hash (same for every entry in this call).
    let stream_hash = self.inner.stream_cache_hash(stream, &stream_dir);

    // Obtain the active segment ID once.  A rotation that occurs mid-loop is
    // safe: the reader opens an immutable file that simply stopped growing.
    let active_seg_id: Option<u64> = self.inner.streams
      .get(stream)
      .map(|lock| lock.lock().active_segment_start_id);

    // Group IDs by segment.
    let mut segment_groups: HashMap<u64, Vec<(usize, u64)>> = HashMap::new();
    for (idx, &id) in ids.iter().enumerate() {
      let segment_start_id = index.find_segment_start_id(id)?;
      segment_groups.entry(segment_start_id).or_default().push((idx, id));
    }

    let mut results = vec![None; ids.len()];

    for (segment_start_id, mut group) in segment_groups {
      let segment_path = stream_dir.join(segment_filename(segment_start_id));
      if !segment_path.exists() {
        continue;
      }

      // Ascending IDs let the active-segment scan resume from the previous
      // frame instead of rewinding to offset 0 for every lookup.
      group.sort_unstable_by_key(|&(_, id)| id);

      let is_active = active_seg_id == Some(segment_start_id);

      if is_active {
        // Transient handle respecting the configured ReadStrategy — bypasses
        // the persistent descriptor cache. Mmap is safe here for the same
        // reason as in get(): the file only grows, bounds are fixed at open time.
        let mut reader = match SegmentReader::open(&segment_path, &self.inner.options) {
          Ok(r) => r,
          // Deleted by a concurrent truncate between the exists() check and here.
          Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
          Err(e) => return Err(e),
        };
        let mut last_loc: Option<FrameLocation> = None;

        for (result_idx, id) in group {
          let frame_loc = match reader.find_frame(id, last_loc.as_ref())? {
            Some(loc) => loc,
            None => continue,
          };
          last_loc = Some(frame_loc.clone());

          let cache_key = BlockCacheKey { stream_hash, segment_start_id, frame_offset: frame_loc.offset };

          let batch = if let Some(read_cache) = &self.inner.read_cache {
            if let Some(cached) = read_cache.fetch(&cache_key) {
              cached
            } else {
              let b = reader.read_at(&frame_loc)?;
              let weight = b.iter().map(|v| v.len() as u64).sum::<u64>().max(1);
              read_cache.insert(cache_key, b.clone(), weight);
              Arc::new(b)
            }
          } else {
            Arc::new(reader.read_at(&frame_loc)?)
          };

          let idx_in_batch = (id - frame_loc.header.start_id) as usize;
          if idx_in_batch < batch.len() {
            results[result_idx] = Some(batch[idx_in_batch].clone());
          }
        }
      } else {
        // Cached descriptor shared across all IDs in this rotated segment group.
        let desc = match self.get_or_load_read_descriptor(&segment_path) {
          Ok(d) => d,
          // Deleted by a concurrent truncate between the exists() check and here.
          Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
          Err(e) => return Err(e),
        };

        for (result_idx, id) in group {
          let frame_loc = match desc.find_frame(id)? {
            Some(loc) => loc,
            None => continue,
          };

          let cache_key = BlockCacheKey { stream_hash, segment_start_id, frame_offset: frame_loc.offset };

          let batch = if let Some(read_cache) = &self.inner.read_cache {
            if let Some(cached) = read_cache.fetch(&cache_key) {
              cached
            } else {
              let b = desc.read_frame(&frame_loc)?;
              let weight = b.iter().map(|v| v.len() as u64).sum::<u64>().max(1);
              read_cache.insert(cache_key, b.clone(), weight);
              Arc::new(b)
            }
          } else {
            Arc::new(desc.read_frame(&frame_loc)?)
          };

          let idx_in_batch = (id - frame_loc.header.start_id) as usize;
          if idx_in_batch < batch.len() {
            results[result_idx] = Some(batch[idx_in_batch].clone());
          }
        }
      }
    }

    Ok(results)
  }

  // --- Maintenance ---

  /// Truncates the stream's history, freeing all segment files containing
  /// records with IDs strictly less than `safe_id`. With `archive_path` set the
  /// files move to `{archive_path}/{stream}/` instead of being deleted, and
  /// [`Wal::restore_archived`] brings them back.
  ///
  /// Returns the number of segments freed.
  pub fn truncate(&self, stream: &str, safe_id: u64) -> Result<usize> {
    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;
    let idx_path = stream_dir.join(SegmentIndex::FILENAME);

    // --- Phase 1: Identify Candidates and Active ID (Under Lock) ---
    // We synchronize against writers by acquiring the stream lock.
    let (initial_candidates, active_segment_id) = {
      // Ensure the stream exists in memory so we can lock it.
      let stream_lock = self.inner.streams.get_or_create(stream);
      let state = stream_lock.lock();

      let index = self.get_or_load_index(&stream_dir, &idx_path)?;

      let all_ids: Vec<u64> = index.iter().collect();
      if all_ids.len() < 2 {
        return Ok(0);
      }

      // Use the trusted in-memory state for the active segment ID
      let active_id = state.active_segment_start_id;

      let first_segment_to_keep = index.find_segment_start_id(safe_id)?;

      let candidates: Vec<u64> = all_ids
        .into_iter()
        .take_while(|&id| id < first_segment_to_keep)
        .collect();

      (candidates, active_id)
    }; // Lock released here.

    // --- Phase 1.5: Final Safety Filter (No Lock) ---
    let to_delete: Vec<u64> = initial_candidates
      .into_iter()
      .filter(|&id| id != active_segment_id)
      .collect();

    if to_delete.is_empty() {
      return Ok(0);
    }

    // --- Phase 2: Perform Deletion or Archival (No Lock) ---
    let mut deleted_ids = Vec::new();
    let archive_dir = match self.archive_dir(stream)? {
      Some(dir) => {
        fs::create_dir_all(&dir)?;
        Some(dir)
      }
      None => None,
    };

    for &start_id in &to_delete {
      let filename = segment_filename(start_id);
      let path = stream_dir.join(&filename);

      self.inner.write_cache.invalidate(&path);
      self.inner.read_descriptor_cache.invalidate(&path);

      let freed = match &archive_dir {
        Some(dir) => move_file(&path, &dir.join(&filename)),
        None => fs::remove_file(&path),
      };
      match freed {
        Ok(_) => deleted_ids.push(start_id),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
          deleted_ids.push(start_id);
        }
        Err(e) => {
          tracing::warn!(target: "ironwal", "Failed to free segment {}: {}. It will remain in index.", path.display(), e);
        }
      }
    }

    if deleted_ids.is_empty() {
      return Ok(0);
    }

    // --- Phase 3: Rebuild Index (Under Lock) ---
    {
      // Re-acquire lock to safely update index without racing against writers
      let stream_lock = self.inner.streams.get_or_create(stream);
      let _state = stream_lock.lock();

      self.inner.index_cache.invalidate(&idx_path);
      let current_index = self.get_or_load_index(&stream_dir, &idx_path)?;
      let deleted_set: std::collections::HashSet<u64> = deleted_ids.iter().copied().collect();
      let new_ids: Vec<u64> = current_index
        .iter()
        .filter(|id| !deleted_set.contains(id))
        .collect();

      let temp_path = stream_dir.join("segments.idx.tmp");
      let final_path = stream_dir.join("segments.idx");

      let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

      for id in new_ids {
        file.write_u64::<LittleEndian>(id)?;
      }
      util::sync_file(&file, &self.inner.syncs)?;
      drop(file);

      if let Err(e) = fs::rename(&temp_path, &final_path) {
        tracing::error!(target: "ironwal", "Failed to update segments.idx after truncation: {}. Index is inconsistent with disk.", e);
        return Err(Error::Io(e));
      }

      self.inner.index_cache.invalidate(&idx_path);
    }

    Ok(deleted_ids.len())
  }

  /// The archive directory for `stream`, when `archive_path` is configured.
  fn archive_dir(&self, stream: &str) -> Result<Option<PathBuf>> {
    match &self.inner.options.archive_path {
      Some(root) => Ok(Some(util::ensure_safe_path(root, stream)?)),
      None => Ok(None),
    }
  }

  /// Moves every segment `truncate` archived for `stream` back into the stream and
  /// rebuilds `segments.idx` from the segment files present, so the restored history
  /// reads again from its first ID. Returns the number of segments restored; 0 when
  /// nothing is archived or no `archive_path` is configured. Idempotent and crash-safe:
  /// files move one at a time and the index is rewritten last, atomically, from what is
  /// on disk, so an interrupted restore is completed by running it again. Stop appending
  /// to the stream before calling.
  pub fn restore_archived(&self, stream: &str) -> Result<usize> {
    let Some(archive_dir) = self.archive_dir(stream)? else { return Ok(0) };
    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;
    let idx_path = stream_dir.join(SegmentIndex::FILENAME);

    let stream_lock = self.inner.streams.get_or_create(stream);
    let _state = stream_lock.lock();

    let mut restored = 0usize;
    if let Ok(entries) = fs::read_dir(&archive_dir) {
      for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if util::parse_segment_id(name).is_none() {
          continue;
        }
        let target = stream_dir.join(name);
        self.inner.write_cache.invalidate(&target);
        self.inner.read_descriptor_cache.invalidate(&target);
        move_file(&entry.path(), &target)?;
        restored += 1;
      }
    }

    let mut ids: Vec<u64> = fs::read_dir(&stream_dir)?
      .flatten()
      .filter_map(|e| e.file_name().to_str().and_then(util::parse_segment_id))
      .collect();
    ids.sort_unstable();

    let temp_path = stream_dir.join("segments.idx.tmp");
    let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path)?;
    for id in ids {
      file.write_u64::<LittleEndian>(id)?;
    }
    util::sync_file(&file, &self.inner.syncs)?;
    drop(file);
    fs::rename(&temp_path, &idx_path)?;
    self.inner.index_cache.invalidate(&idx_path);

    Ok(restored)
  }

  /// Removes a stream entirely: all segment files, `segments.idx`, the head
  /// state file and the in-memory stream state. A subsequent append recreates
  /// the stream starting from ID 0. Idempotent: deleting a stream that does
  /// not exist is a no-op.
  ///
  /// Safe on a live `Wal` with respect to readers and other streams. An
  /// append to the same stream racing this call may recreate the stream
  /// mid-deletion; stop appending to the stream before deleting it.
  ///
  /// Crash-safe: the stream directory is atomically renamed to a marker name
  /// before removal, and leftover markers are cleaned up by [`Wal::new`].
  pub fn delete_stream(&self, stream: &str) -> Result<()> {
    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;

    // Remove every map entry aliasing to this directory (keys are raw stream
    // names, several of which may sanitize to the same path), then wait out
    // in-flight appends by locking the removed states for the whole deletion.
    let removed = self.inner.streams.remove_matching(|key| {
      util::ensure_safe_path(&self.inner.options.root_path, key).map_or(false, |dir| dir == stream_dir)
    });
    let _guards: Vec<_> = removed.iter().map(|state| state.lock()).collect();
    self.inner.gates.write().retain(|key, _| util::ensure_safe_path(&self.inner.options.root_path, key).ok().is_none_or(|dir| dir != stream_dir));

    {
      let mut generations = self.inner.stream_generations.write();
      *generations.entry(stream_dir.clone()).or_insert(0) += 1;
    }

    self
      .inner
      .index_cache
      .invalidate(&stream_dir.join(SegmentIndex::FILENAME));

    if !stream_dir.exists() {
      return Ok(());
    }

    for entry in fs::read_dir(&stream_dir)? {
      let entry = entry?;
      let filename = entry.file_name().to_string_lossy().to_string();
      if util::parse_segment_id(&filename).is_some() {
        let path = entry.path();
        self.inner.write_cache.invalidate(&path);
        self.inner.read_descriptor_cache.invalidate(&path);
      }
    }

    let marker = {
      let mut name = stream_dir.file_name().unwrap_or_default().to_os_string();
      name.push(util::DELETE_MARKER_SUFFIX);
      stream_dir.with_file_name(name)
    };
    if marker.exists() {
      fs::remove_dir_all(&marker)?;
    }
    fs::rename(&stream_dir, &marker)?;
    util::sync_dir(&self.inner.options.root_path, &self.inner.syncs)?;
    fs::remove_dir_all(&marker)?;

    Ok(())
  }

  /// Gets the current state of a stream (for watermark initialization).
  ///
  /// Returns None if the stream doesn't exist yet.
  #[cfg(feature = "sharded")]
  pub(crate) fn get_stream_state(&self, stream: &str) -> Result<Option<StreamState>> {
    Ok(self.inner.streams.get(stream).map(|lock| lock.lock().clone()))
  }

  /// Flushes the BufWriter and calls `fsync` on the active segment for `stream`.
  /// No-op if the stream has no cached active segment (nothing written yet).
  #[cfg(feature = "sharded")]
  pub(crate) fn sync_stream(&self, stream: &str) -> Result<()> {
    let stream_dir = self.get_stream_dir(stream)?;
    let seg_start_id = {
      let lock = self.inner.streams.get_or_create(stream);
      lock.lock().active_segment_start_id
    };
    let path = stream_dir.join(segment_filename(seg_start_id));
    if let Some(segment) = self.inner.write_cache.fetch(&path) {
      segment.flush()?;
    }
    Ok(())
  }
}

/// Rename, falling back to copy-then-remove when the destination sits on another device.
fn move_file(from: &Path, to: &Path) -> std::io::Result<()> {
  match fs::rename(from, to) {
    Ok(()) => Ok(()),
    Err(e) if e.raw_os_error() == Some(libc_exdev()) => {
      fs::copy(from, to)?;
      fs::remove_file(from)
    }
    Err(e) => Err(e),
  }
}

#[cfg(unix)]
fn libc_exdev() -> i32 {
  18
}

#[cfg(not(unix))]
fn libc_exdev() -> i32 {
  17
}
