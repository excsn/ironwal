mod stream_map;

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use fibre_cache::policy::lru::LruPolicy;
use fibre_cache::{Cache, CacheBuilder, EvictionListener, EvictionReason};
use tracing::error;

use crate::batch::TxnWriter;
use crate::config::{SyncMode, WalOptions};
use crate::error::{Error, Result};
use crate::index::{self, SegmentIndex};
use crate::segment::{ActiveSegment, SegmentReader};
use crate::state::{StreamState, StreamStateFile};
use crate::util::{self, segment_filename};
use crate::wal::stream_map::StreamStateMap;

/// A unique identifier for a cached data block (a decompressed frame).
/// This composite key prevents collisions between different streams.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockCacheKey {
  /// A hash of the stream key to keep the key size manageable.
  pub stream_hash: u64,
  /// The start ID of the segment file containing the block.
  pub segment_start_id: u64,
  /// The byte offset of the frame's header within the segment file.
  pub frame_offset: u64,
}

pub struct WalState {
  options: WalOptions,

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
}

impl WalState {
  fn get_or_create_segment(&self, stream: &str, state: &mut StreamState) -> Result<Arc<ActiveSegment>> {
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
    let new_segment = ActiveSegment::create(segment_path.clone(), state.active_segment_start_id, &self.options)?;

    if exists {
      // If we are opening an existing file for writing (e.g. after a restart),
      // we MUST repair it to ensure we don't append after a corrupted tail.
      new_segment.repair()?;
    }

    // 3. Insert the known-good segment into the cache. The cost is 1 (one file handle).
    self.write_cache.insert(segment_path.clone(), new_segment, 1);

    // 4. Fetch it again to get the Arc managed by the cache.
    Ok(self.write_cache.fetch(&segment_path).unwrap())
  }

  /// Rotates the active segment for a stream.
  /// Handles index updates, cache invalidation, and state file persistence.
  fn rotate_segment(&self, stream: &str, state: &mut StreamState) -> Result<Arc<ActiveSegment>> {
    let stream_dir = util::ensure_safe_path(&self.options.root_path, stream)?;

    let old_segment_path = stream_dir.join(segment_filename(state.active_segment_start_id));
    let index_path = stream_dir.join(SegmentIndex::FILENAME);

    // 1. Force flush and close of the old segment via cache invalidation
    self.write_cache.invalidate(&old_segment_path);

    // 2. Advance the ID in memory
    state.active_segment_start_id = state.next_id;

    // 3. Update Index: Append new start ID to segments.idx
    index::append_index(&stream_dir, state.active_segment_start_id)?;
    // Invalidate the index cache because the file on disk has changed
    self.index_cache.invalidate(&index_path);

    // 4. Update Head State: Atomic Write
    let head_state = StreamStateFile {
      active_segment_name: segment_filename(state.active_segment_start_id),
      version: 1,
    };
    head_state.write_to(&stream_dir)?;

    // 5. Create/Get the new segment
    self.get_or_create_segment(stream, state)
  }

  fn write_one(&self, stream: &str, state: &mut StreamState, entry: &[&[u8]]) -> Result<u64> {
    let segment = self.get_or_create_segment(stream, state)?;

    // --- Check for Rotation ---
    // Estimate: payload size + fixed frame overhead
    let entry_size_estimate = entry[0].len() as u64 + 32;
    let needs_rotation = (segment.size() + entry_size_estimate) > self.options.max_segment_size
      || segment.count() >= self.options.max_entries_per_segment;

    let final_segment = if needs_rotation {
      self.rotate_segment(stream, state)?
    } else {
      segment
    };

    // --- Write the entry ---
    let range = final_segment.append(entry, state.next_id, &self.options)?;
    state.next_id = range.end;

    if self.options.sync_mode == SyncMode::Strict {
      final_segment.flush()?;
    } else {
      // For non-strict modes, we must at least flush the application buffer
      final_segment.flush_buffer()?;
    }

    Ok(range.start)
  }

  fn write_batch(&self, stream: &str, state: &mut StreamState, entries: &[&[u8]]) -> Result<std::ops::Range<u64>> {
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
      let mut chunk_byte_size = 0;

      for entry in &entries[entries_processed..] {
        let record_size_estimate = (entry.len() + 32) as u64;

        // If we have picked at least one entry, and this one overflows, stop here.
        if chunk_byte_size + record_size_estimate > remaining_space && chunk_end_idx > entries_processed {
          break;
        }

        chunk_byte_size += record_size_estimate;
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
      let segment_ref_check = self.get_or_create_segment(stream, state)?;

      let needs_rotation = entries_processed < entries.len() // More data to write
        && (segment_ref_check.size() >= self.options.max_segment_size
        || segment_ref_check.count() >= self.options.max_entries_per_segment);

      if needs_rotation {
        self.rotate_segment(stream, state)?;
      }
    }

    if self.options.sync_mode == SyncMode::Strict || self.options.sync_mode == SyncMode::BatchOnly {
      // Get the final segment that was written to and flush it.
      let segment = self.get_or_create_segment(stream, state)?;
      segment.flush()?;
    }

    Ok(overall_start_id..state.next_id)
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

    // 2. Scan and recover state from filesystem
    let streams_map = Self::recover_all_streams(&options, &index_cache)?;
    let streams = StreamStateMap::new(streams_map);

    Ok(Self {
      inner: Arc::new(WalState {
        options,
        streams, // Replaces state_lock: Mutex::new(state)
        index_cache,
        write_cache,
        read_cache,
      }),
    })
  }

  /// Returns the Start ID of the currently active segment.
  /// Useful for monitoring rotation and testing.
  pub fn current_segment_start_id(&self, stream: &str) -> Option<u64> {
    self.inner.streams.get(stream).map(|s| s.lock().active_segment_start_id)
  }

  /// Creates an iterator to read entries sequentially starting from `start_id`.
  pub fn iter(&self, stream: &str, start_id: u64) -> Result<crate::iter::WalIterator> {
    crate::iter::WalIterator::new(self.clone(), stream.to_string(), start_id)
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
      self.inner.index_cache.insert(idx_path.to_path_buf(), idx, 1);
      Ok(self.inner.index_cache.fetch(idx_path).unwrap())
    }
  }

  /// Scans all subdirectories to recover the state of each stream.
  fn recover_all_streams(
    options: &WalOptions,
    index_cache: &Cache<PathBuf, SegmentIndex>,
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

        if let Some(state) = Self::recover_stream(&stream_dir, options, index_cache)? {
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
  ) -> Result<Option<StreamState>> {
    let index_path = stream_dir.join("segments.idx");

    // 1. Initial Index Load
    if let Err(_) = SegmentIndex::open(stream_dir) {
      Self::rebuild_index_from_scan(stream_dir)?;
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
    let head_state = StreamStateFile::read_from(stream_dir)?;
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
      Self::rebuild_index_from_scan(stream_dir)?;

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
      }));
    }

    let mut reader = SegmentReader::open(&active_segment_path, options)?;
    let entries_in_segment = reader.recover_scan()?;
    let next_id = active_segment_start_id + entries_in_segment;

    Ok(Some(StreamState {
      next_id,
      active_segment_start_id,
    }))
  }

  /// Slow-path recovery: Scans a stream directory for all `.wal` files
  /// and rebuilds the `segments.idx` file from scratch.
  fn rebuild_index_from_scan(stream_dir: &Path) -> Result<()> {
    let mut start_ids = Vec::new();
    for entry in fs::read_dir(stream_dir)? {
      let entry = entry?;
      let filename = entry.file_name().to_string_lossy().to_string();
      if let Some(id) = util::parse_segment_id(&filename) {
        start_ids.push(id);
      }
    }

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
    file.sync_all()?;
    drop(file);

    fs::rename(&temp_path, &final_path)?;

    Ok(())
  }

  // --- Write Operations ---

  /// Appends a single entry to the stream.
  pub fn append(&self, stream: &str, entry: &[u8]) -> Result<u64> {
    let stream_lock = self.inner.streams.get_or_create(stream);
    let mut state = stream_lock.lock();
    self.inner.write_one(stream, &mut state, &[entry])
  }

  /// Appends a batch of entries atomically to the WAL.
  pub fn append_batch(&self, stream: &str, entries: &[&[u8]]) -> Result<std::ops::Range<u64>> {
    let stream_lock = self.inner.streams.get_or_create(stream);
    let mut state = stream_lock.lock();
    self.inner.write_batch(stream, &mut state, entries)
  }

  /// Returns a `TxnWriter` handle for accumulating a batch of writes in memory.
  pub fn writer(&self, stream: &str) -> TxnWriter {
    TxnWriter::new(self.clone(), stream.to_string())
  }

  // --- Read Operations ---

  /// Reads a single entry by its sequence ID.
  pub fn get(&self, stream: &str, id: u64) -> Result<Option<Vec<u8>>> {
    let stream_dir = util::ensure_safe_path(&self.inner.options.root_path, stream)?;

    // 1. Find which segment file contains this ID
    // We use the index cache to avoid disk IO on segments.idx
    let index = {
      let idx_path = stream_dir.join(SegmentIndex::FILENAME);
      // We must ensure the index is loaded. The `recover_stream` step ensures this on startup.
      // In a real system we might need retry/reload logic here if it was evicted.
      if let Some(idx) = self.inner.index_cache.fetch(&idx_path) {
        idx
      } else {
        // Fallback: Try to open it and cache it
        let idx = SegmentIndex::open(&stream_dir)?;
        self.inner.index_cache.insert(idx_path.clone(), idx, 1);
        self.inner.index_cache.fetch(&idx_path).unwrap()
      }
    };

    // Find the file that starts *before* or *at* our ID
    let segment_start_id = index.find_segment_start_id(id)?;
    let segment_path = stream_dir.join(segment_filename(segment_start_id));

    if !segment_path.exists() {
      return Ok(None);
    }

    // 2. Open the segment reader
    let mut reader = SegmentReader::open(&segment_path, &self.inner.options)?;

    // 3. Scan headers to find the specific frame
    // This is cheap (no decompression)
    let frame_loc = match reader.find_frame(id)? {
      Some(loc) => loc,
      None => return Ok(None),
    };

    // 4. Construct Cache Key
    let mut hasher = DefaultHasher::new();
    stream.hash(&mut hasher);
    let stream_hash = hasher.finish();

    let cache_key = BlockCacheKey {
      stream_hash,
      segment_start_id,
      frame_offset: frame_loc.offset,
    };

    // 5. Check Cache
    if let Some(read_cache) = &self.inner.read_cache {
      if let Some(batch) = read_cache.fetch(&cache_key) {
        // Hit! Extract item from batch
        // Calculate index within the batch
        let index_in_batch = (id - frame_loc.header.start_id) as usize;
        if index_in_batch < batch.len() {
          return Ok(Some(batch[index_in_batch].clone()));
        } else {
          return Err(Error::Corruption(
            "Found frame but ID offset exceeds batch length".into(),
          ));
        }
      }
    }

    // 6. Miss (or no cache): Read, Decompress, Cache
    let batch = match reader.read_at(&frame_loc) {
      Ok(b) => b,
      Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
        // We found a header, but the payload was truncated.
        // This implies a partial write at the end of the log (e.g., power loss).
        // We treat this as "record not found" to allow recovery.
        return Ok(None);
      }
      Err(e) => return Err(e),
    };

    // Validate bounds again
    let index_in_batch = (id - frame_loc.header.start_id) as usize;
    if index_in_batch >= batch.len() {
      return Err(Error::Corruption(
        "Found frame but ID offset exceeds batch length".into(),
      ));
    }

    let result = batch[index_in_batch].clone();

    // Insert into cache
    if let Some(read_cache) = &self.inner.read_cache {
      // Weight is approximate (sum of data lengths)
      let weight = batch.iter().map(|v| v.len() as u64).sum::<u64>();
      read_cache.insert(cache_key, batch, weight.max(1));
    }

    Ok(Some(result))
  }

  // --- Maintenance ---

  /// Truncates the stream's history, deleting all segment files containing
  /// records with IDs strictly less than `safe_id`.
  ///
  /// Returns the number of segments deleted.
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

    // --- Phase 2: Perform Deletion (No Lock) ---
    let mut deleted_ids = Vec::new();

    for &start_id in &to_delete {
      let filename = segment_filename(start_id);
      let path = stream_dir.join(&filename);

      self.inner.write_cache.invalidate(&path);

      match fs::remove_file(&path) {
        Ok(_) => deleted_ids.push(start_id),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
          deleted_ids.push(start_id);
        }
        Err(e) => {
          tracing::warn!(target: "ironwal", "Failed to delete segment {}: {}. It will remain in index.", path.display(), e);
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
      let new_ids: Vec<u64> = current_index.iter().filter(|id| !deleted_ids.contains(id)).collect();

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
      file.sync_all()?;
      drop(file);

      if let Err(e) = fs::rename(&temp_path, &final_path) {
        tracing::error!(target: "ironwal", "Failed to update segments.idx after truncation: {}. Index is inconsistent with disk.", e);
        return Err(Error::Io(e));
      }

      self.inner.index_cache.invalidate(&idx_path);
    }

    Ok(deleted_ids.len())
  }
}
