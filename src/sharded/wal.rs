//! ShardedWal implementation - the main public API.

use crate::WalOptions;
use crate::error::{Error, Result};
use crate::iter::WalIterator;
use crate::sharded::checkpoint::{CheckpointData, CheckpointManager, CompactionStats, PruneStats};
use crate::sharded::router::Router;
use crate::sharded::watermark::WatermarkTracker;
use crate::wal::Wal;

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

/// A sharded WAL for horizontal scaling and consistent checkpointing.
///
/// `ShardedWal` wraps the core `Wal` and automatically routes writes to
/// independent shards based on key hashing. It provides consistent snapshot
/// capabilities across all shards via checkpoints.
///
/// # Example
///
/// ```no_run
/// use ironwal::sharded::ShardedWal;
/// use ironwal::WalOptions;
///
/// # fn main() -> ironwal::Result<()> {
/// let opts = WalOptions::default();
/// let sharded = ShardedWal::new(opts, 16)?;
///
/// // Writes are automatically routed
/// let (shard_id, seq_id) = sharded.append(b"user_123", b"data")?;
///
/// // Create consistent snapshot
/// sharded.create_checkpoint(b"snapshot_1")?;
///
/// // Restore from checkpoint
/// let (checkpoint_id, data) = sharded.load_latest_checkpoint()?;
/// for (shard_id, offset) in data.offsets.iter().enumerate() {
///     let mut iter = sharded.iter_shard(shard_id as u16, *offset)?;
///     // Process entries...
/// }
/// # Ok(())
/// # }
/// ```
pub struct ShardedWal {
  inner: Arc<Wal>,
  router: Router,
  watermarks: WatermarkTracker,
  checkpoint_manager: CheckpointManager,
}

impl ShardedWal {
  /// Creates a new sharded WAL.
  ///
  /// # Arguments
  ///
  /// * `opts` - WAL configuration options
  /// * `shard_count` - Number of shards (must be > 0)
  ///
  /// # Behavior
  ///
  /// - Creates shard directories if they don't exist
  /// - Rebuilds checkpoint index from `checkpoints.log`
  /// - Initializes watermarks from current WAL state
  ///
  /// # Errors
  ///
  /// Returns error if:
  /// - `shard_count` is zero
  /// - Existing checkpoints have different shard count
  /// - I/O operations fail
  pub fn new(opts: WalOptions, shard_count: u16) -> Result<Self> {
    if shard_count == 0 {
      return Err(Error::Config(
        "shard_count must be greater than zero".into(),
      ));
    }

    let root = opts.root_path.clone();
    let sync_mode = opts.sync_mode;
    let max_open_segments = opts.max_open_segments;

    // Initialize core WAL
    let inner = Arc::new(Wal::new(opts)?);

    // Create router
    let router = Router::new(shard_count);

    // Warn if cache is undersized for shard count
    if max_open_segments < shard_count as usize {
      tracing::warn!(
        target: "ironwal::sharded",
        "max_open_segments ({}) is less than shard_count ({}). \
         This will cause file handle thrashing. \
         Recommend setting max_open_segments >= shard_count * 2",
        max_open_segments,
        shard_count
      );
    }

    // Initialize checkpoint manager
    let checkpoint_manager = CheckpointManager::new(root.clone(), shard_count)?;

    // Initialize watermarks from WAL state
    let watermarks = WatermarkTracker::new(shard_count, sync_mode);
    watermarks.initialize_from_wal(&inner, &router)?;

    Ok(Self {
      inner,
      router,
      watermarks,
      checkpoint_manager,
    })
  }

  /// Appends a single entry to the appropriate shard.
  ///
  /// # Arguments
  ///
  /// * `key` - Key used for shard routing (not stored, only for hashing)
  /// * `value` - Data to append
  ///
  /// # Returns
  ///
  /// A tuple of `(shard_id, sequence_id)` indicating where the entry was written.
  ///
  /// # Example
  ///
  /// ```no_run
  /// # use ironwal::sharded::ShardedWal;
  /// # use ironwal::WalOptions;
  /// # fn main() -> ironwal::Result<()> {
  /// # let wal = ShardedWal::new(WalOptions::default(), 16)?;
  /// let (shard_id, seq_id) = wal.append(b"user_123", b"profile_data")?;
  /// println!("Written to shard {} at offset {}", shard_id, seq_id);
  /// # Ok(())
  /// # }
  /// ```
  pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(u16, u64)> {
    let shard_id = self.router.route(key);
    let stream = self.router.shard_name(shard_id);

    let seq_id = self.inner.append(&stream, value)?;

    // Update watermark if appropriate for sync mode
    self.watermarks.update(shard_id, seq_id + 1);

    Ok((shard_id, seq_id))
  }

  /// Appends a batch of entries.
  ///
  /// Entries are grouped by shard and written as batches to each shard.
  /// This is more efficient than individual appends.
  ///
  /// # Note
  ///
  /// Batches spanning multiple shards are **not atomic** across shards.
  /// Each shard's batch is committed independently.
  ///
  /// # Returns
  ///
  /// A vector of `(shard_id, sequence_id)` in the same order as input.
  pub fn append_batch(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<(u16, u64)>> {
    if entries.is_empty() {
      return Ok(Vec::new());
    }

    // Hash once and cache routing decisions
    let routing: Vec<u16> = entries
      .iter()
      .map(|(key, _)| self.router.route(key))
      .collect();

    let first_shard = routing[0];

    let all_same_shard = routing.iter().skip(1).all(|&shard| shard == first_shard);

    if all_same_shard {
      // Single-shard fast path
      let values: Vec<&[u8]> = entries.iter().map(|(_, v)| v.as_slice()).collect();
      return self.append_batch_single_key(&entries[0].0, &values);
    }

    // Multi-shard path - use cached routing
    let mut shard_groups: HashMap<u16, Vec<&[u8]>> = HashMap::new();

    for (&shard_id, (_, value)) in routing.iter().zip(entries.iter()) {
      shard_groups
        .entry(shard_id)
        .or_default()
        .push(value.as_slice());
    }

    // Write to shards
    let mut shard_results: HashMap<u16, std::ops::Range<u64>> = HashMap::new();

    for (shard_id, group) in shard_groups {
      let stream = self.router.shard_name(shard_id);
      let range = self.inner.append_batch(&stream, &group)?;
      self.watermarks.set_durable(shard_id, range.end);
      shard_results.insert(shard_id, range);
    }

    // Build results using cached routing (cursor-based reconstruction)
    let mut results = Vec::with_capacity(entries.len());
    let mut shard_cursors: HashMap<u16, u64> = HashMap::new();

    for &shard_id in &routing {
      let range = &shard_results[&shard_id];
      let cursor = shard_cursors.entry(shard_id).or_insert(range.start);
      results.push((shard_id, *cursor));
      *cursor += 1;
    }

    Ok(results)
  }

  /// Appends a batch of values for a single key.
  ///
  /// This is an optimized version of `append_batch` for when all values belong to the same key.
  /// It avoids calculating the hash for every entry and guarantees a single shard write.
  ///
  /// # Arguments
  ///
  /// * `key` - The key associated with all values in the batch.
  /// * `values` - A slice of byte slices to append.
  pub fn append_batch_single_key(&self, key: &[u8], values: &[&[u8]]) -> Result<Vec<(u16, u64)>> {
    if values.is_empty() {
      return Ok(Vec::new());
    }

    let shard_id = self.router.route(key);
    let stream = self.router.shard_name(shard_id);

    let range = self.inner.append_batch(&stream, values)?;
    self.watermarks.set_durable(shard_id, range.end);

    Ok(range.map(|seq_id| (shard_id, seq_id)).collect())
  }

  /// Creates a checkpoint capturing the current durable state of all shards.
  ///
  /// # Arguments
  ///
  /// * `user_id` - User-defined checkpoint identifier (e.g., Raft index, transaction ID)
  ///
  /// # Atomicity
  ///
  /// The checkpoint represents a consistent cut across all shards at the time
  /// of the watermark snapshot. All data at or before the checkpoint offsets
  /// is guaranteed to be durable.
  pub fn create_checkpoint(&self, user_id: &[u8]) -> Result<()> {
    let offsets = self.watermarks.snapshot();
    self.checkpoint_manager.create(user_id, offsets)
  }

  /// Loads a specific checkpoint by user_id.
  pub fn load_checkpoint(&self, user_id: &[u8]) -> Result<CheckpointData> {
    self.checkpoint_manager.load(user_id)
  }

  /// Loads the latest checkpoint.
  ///
  /// # Returns
  ///
  /// A tuple of `(user_id, checkpoint_data)`.
  ///
  /// # Errors
  ///
  /// Returns `Error::NoCheckpoints` if no checkpoints exist.
  pub fn load_latest_checkpoint(&self) -> Result<(Vec<u8>, CheckpointData)> {
    self.checkpoint_manager.load_latest()
  }

  /// Returns an iterator for a specific shard starting at the given offset.
  /// This is the primary way to read data from a sharded WAL.
  ///
  /// # Returns
  ///
  /// Returns an empty iterator if the shard stream doesn't exist yet.
  ///
  /// # Example
  ///
  /// ```no_run
  /// # use ironwal::sharded::ShardedWal;
  /// # use ironwal::WalOptions;
  /// # fn main() -> ironwal::Result<()> {
  /// # let wal = ShardedWal::new(WalOptions::default(), 16)?;
  /// let (_, checkpoint) = wal.load_latest_checkpoint()?;
  ///
  /// for shard_id in 0..16 {
  ///     let offset = checkpoint.offsets[shard_id as usize];
  ///     let mut iter = wal.iter_shard(shard_id, offset)?;
  ///     
  ///     while let Some(entry) = iter.next() {
  ///         let data = entry?;
  ///         // Process entry...
  ///     }
  /// }
  /// # Ok(())
  /// # }
  /// ```
  pub fn iter_shard(&self, shard_id: u16, start_offset: u64) -> Result<WalIterator> {
    if shard_id >= self.router.shard_count() {
      return Err(Error::Config(format!(
        "Invalid shard_id: {} (max: {})",
        shard_id,
        self.router.shard_count() - 1
      )));
    }

    let stream = self.router.shard_name(shard_id);
    self.inner.iter(&stream, start_offset)
  }

  /// Prunes all shards to the offsets specified in a checkpoint.
  ///
  /// This deletes segment files containing data strictly before the checkpoint.
  ///
  /// # Safety
  ///
  /// - Active segments are never deleted (protected by core WAL)
  /// - Only complete segments before the checkpoint are removed
  /// - Gracefully handles shards that don't exist yet
  pub fn prune_before_checkpoint(&self, user_id: &[u8]) -> Result<PruneStats> {
    let checkpoint = self.checkpoint_manager.load(user_id)?;

    let mut shards_pruned = 0;
    let mut total_deleted = 0;

    for (shard_id, offset) in checkpoint.offsets.iter().enumerate() {
      let stream = self.router.shard_name(shard_id as u16);

      // Skip shards with offset 0 (no data written yet)
      if *offset == 0 {
        continue;
      }

      match self.inner.truncate(&stream, *offset) {
        Ok(deleted) => {
          if deleted > 0 {
            shards_pruned += 1;
            total_deleted += deleted;
          }
        }
        Err(Error::StreamNotFound(_)) => {
          // Stream doesn't exist - skip it (no data to prune)
          continue;
        }
        Err(e) => return Err(e),
      }
    }

    Ok(PruneStats {
      shards_pruned,
      segments_deleted: total_deleted,
    })
  }

  /// Compacts the checkpoint log, keeping only the N most recent checkpoints.
  pub fn compact_checkpoints(&self, keep_latest_n: usize) -> Result<CompactionStats> {
    self.checkpoint_manager.compact(keep_latest_n)
  }

  /// Returns the number of shards.
  pub fn shard_count(&self) -> u16 {
    self.router.shard_count()
  }

  /// Returns the total number of checkpoints stored.
  ///
  /// This is useful for monitoring checkpoint log growth and deciding
  /// when to run compaction.
  pub fn checkpoint_count(&self) -> usize {
    self.checkpoint_manager.checkpoint_count()
  }

  /// Returns a reference to the underlying WAL.
  ///
  /// This allows advanced users to access WAL internals if needed.
  pub fn inner(&self) -> &Wal {
    &self.inner
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;

  fn test_env(shard_count: u16) -> (ShardedWal, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    let wal = ShardedWal::new(opts, shard_count).unwrap();
    (wal, dir)
  }

  #[test]
  fn test_append_routes_to_shard() {
    let (wal, _dir) = test_env(4);

    let (shard_id, seq_id) = wal.append(b"key1", b"value1").unwrap();

    assert!(shard_id < 4);
    assert_eq!(seq_id, 0); // First write to this shard
  }

  #[test]
  fn test_same_key_same_shard() {
    let (wal, _dir) = test_env(16);

    let (shard1, _) = wal.append(b"user_123", b"data1").unwrap();
    let (shard2, _) = wal.append(b"user_123", b"data2").unwrap();

    assert_eq!(shard1, shard2);
  }

  #[test]
  fn test_checkpoint_and_restore() {
    let (wal, _dir) = test_env(4);

    // Write some data
    wal.append(b"key1", b"value1").unwrap();
    wal.append(b"key2", b"value2").unwrap();

    // Create checkpoint
    wal.create_checkpoint(b"checkpoint_1").unwrap();

    // Load it back
    let data = wal.load_checkpoint(b"checkpoint_1").unwrap();
    assert_eq!(data.offsets.len(), 4);
  }

  #[test]
  fn test_batch_append() {
    let (wal, _dir) = test_env(4);

    let batch = vec![
      (b"key1".to_vec(), b"val1".to_vec()),
      (b"key2".to_vec(), b"val2".to_vec()),
      (b"key3".to_vec(), b"val3".to_vec()),
    ];

    let results = wal.append_batch(&batch).unwrap();

    assert_eq!(results.len(), 3);

    // Each entry should have a valid shard and sequence ID
    for (shard_id, seq_id) in results {
      assert!(shard_id < 4);
      assert!(seq_id < 100); // Reasonable check
    }
  }

  #[test]
  fn test_iter_shard() {
    let (wal, _dir) = test_env(4);

    // Write to same key to ensure same shard
    let (shard_id, seq_id1) = wal.append(b"key1", b"data1").unwrap();
    let (shard_id2, seq_id2) = wal.append(b"key1", b"data2").unwrap();

    assert_eq!(shard_id, shard_id2, "Same key should route to same shard");

    let mut iter = wal.iter_shard(shard_id, 0).unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), b"data1");
    assert_eq!(iter.next().unwrap().unwrap(), b"data2");
    assert!(iter.next().is_none());
  }

  #[test]
  fn test_persistence_restart() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let shard_count = 4;

    // Scope 1: Write and close
    {
      let mut opts = WalOptions::default();
      opts.root_path = root.clone();
      let wal = ShardedWal::new(opts, shard_count).unwrap();
      wal.append(b"key1", b"value1").unwrap();
      wal.create_checkpoint(b"ckpt_1").unwrap();
    }

    // Scope 2: Reopen and verify
    {
      let mut opts = WalOptions::default();
      opts.root_path = root.clone();
      let wal = ShardedWal::new(opts, shard_count).unwrap();

      let (user_id, ckpt) = wal.load_latest_checkpoint().unwrap();
      assert_eq!(user_id, b"ckpt_1");

      // Find the shard for "key1"
      let shard_id = wal.router.route(b"key1");
      let offset = ckpt.offsets[shard_id as usize];
      assert!(offset > 0);

      let mut iter = wal.iter_shard(shard_id, 0).unwrap();
      let entry = iter.next().unwrap().unwrap();
      assert_eq!(entry, b"value1");
    }
  }

  #[test]
  fn test_prune_segments() {
    let dir = TempDir::new().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    // Force small segments to ensure rotation
    opts.max_segment_size = 64;

    let wal = ShardedWal::new(opts, 1).unwrap();

    // Write enough data to create multiple segments
    for i in 0..20 {
      wal
        .append(b"key", format!("data_{}", i).as_bytes())
        .unwrap();
    }

    wal.create_checkpoint(b"ckpt_1").unwrap();

    // Write more to ensure we have active segments that shouldn't be pruned
    for i in 20..30 {
      wal
        .append(b"key", format!("data_{}", i).as_bytes())
        .unwrap();
    }

    let stats = wal.prune_before_checkpoint(b"ckpt_1").unwrap();

    // We expect at least one segment to be deleted if rotation worked
    assert!(stats.segments_deleted > 0);
    assert_eq!(stats.shards_pruned, 1);
  }

  #[test]
  fn test_compact_checkpoints() {
    let (wal, _dir) = test_env(4);

    for i in 0..10 {
      wal
        .create_checkpoint(format!("ckpt_{}", i).as_bytes())
        .unwrap();
    }

    assert_eq!(wal.checkpoint_count(), 10);

    let stats = wal.compact_checkpoints(3).unwrap();

    assert_eq!(stats.checkpoints_before, 10);
    assert_eq!(stats.checkpoints_after, 3);
    assert_eq!(wal.checkpoint_count(), 3);

    // Verify oldest kept is ckpt_7 (7, 8, 9 remain)
    let oldest = wal.load_checkpoint(b"ckpt_7");
    assert!(oldest.is_ok());

    // Verify pruned is gone
    let pruned = wal.load_checkpoint(b"ckpt_6");
    assert!(pruned.is_err());
  }

  #[test]
  fn test_concurrent_writes() {
    use std::sync::Arc;
    use std::thread;

    let dir = TempDir::new().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    opts.sync_mode = crate::config::SyncMode::Strict;

    let wal = Arc::new(ShardedWal::new(opts, 8).unwrap());
    let mut handles = vec![];

    for i in 0..8 {
      let wal = wal.clone();
      handles.push(thread::spawn(move || {
        let key = format!("thread_{}", i).into_bytes();
        for j in 0..50 {
          wal
            .append(&key, &format!("val_{}", j).into_bytes())
            .unwrap();
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    wal.create_checkpoint(b"final").unwrap();
    let (_, ckpt) = wal.load_latest_checkpoint().unwrap();

    let mut total_count = 0;
    for shard_id in 0..8 {
      let offset = ckpt.offsets[shard_id as usize];
      if offset > 0 {
        let mut iter = wal.iter_shard(shard_id, 0).unwrap();
        while let Some(_) = iter.next() {
          total_count += 1;
        }
      }
    }

    assert_eq!(total_count, 8 * 50);
  }

  #[test]
  fn test_resource_constraint_lru() {
    let dir = TempDir::new().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    // Force LRU eviction constantly by allowing only 1 open segment
    opts.max_open_segments = 1;

    let wal = ShardedWal::new(opts, 4).unwrap();

    // Write to random keys to hit different shards, forcing file open/close thrashing
    for i in 0..100 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data")
        .unwrap();
    }

    // Verify we can still read back without errors
    for i in 0..4 {
      let _ = wal.iter_shard(i, 0).unwrap();
    }
  }

  #[test]
  fn test_edge_cases() {
    let (wal, _dir) = test_env(4);

    // 1. Empty batch
    let res = wal.append_batch(&[]);
    assert!(res.is_ok());
    assert!(res.unwrap().is_empty());

    // 2. Invalid shard iteration
    let res = wal.iter_shard(99, 0);
    assert!(matches!(res, Err(Error::Config(_))));
  }

  #[test]
  fn test_watermark_accuracy() {
    let (wal, _dir) = test_env(4);

    // Write a batch
    let batch = vec![
      (b"k1".to_vec(), b"v1".to_vec()),
      (b"k2".to_vec(), b"v2".to_vec()),
    ];

    let results = wal.append_batch(&batch).unwrap();

    // Create checkpoint immediately
    wal.create_checkpoint(b"snap").unwrap();
    let data = wal.load_checkpoint(b"snap").unwrap();

    for (shard_id, seq_id) in results {
      // The checkpoint offset for this shard should be strictly greater than the sequence ID
      // (because offset points to the *next* ID to be written)
      let offset = data.offsets[shard_id as usize];
      assert!(
        offset > seq_id,
        "Checkpoint offset {} should be > seq_id {}",
        offset,
        seq_id
      );
    }
  }
}
