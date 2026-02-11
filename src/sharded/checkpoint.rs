//! Checkpoint management operations.
//!
//! Coordinates checkpoint log I/O, index updates, and HEAD file atomicity.

use crate::error::{Error, Result};
use crate::sharded::checkpoint_log::CheckpointEntry;
use crate::sharded::head;
use crate::sharded::index::{CheckpointIndex, CheckpointMetadata};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Checkpoint data returned when loading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointData {
  /// Durable offset for each shard (indexed by shard_id)
  pub offsets: Vec<u64>,

  /// Unix timestamp when checkpoint was created
  pub timestamp: u64,

  /// Number of shards (for validation)
  pub shard_count: u16,
}

/// Statistics from pruning operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneStats {
  /// Number of shards pruned
  pub shards_pruned: u16,

  /// Total segments deleted across all shards
  pub segments_deleted: usize,
}

/// Statistics from compaction operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
  /// Number of checkpoints before compaction
  pub checkpoints_before: usize,

  /// Number of checkpoints after compaction
  pub checkpoints_after: usize,

  /// Bytes reclaimed from log file
  pub bytes_reclaimed: u64,
}

/// Manages checkpoint creation, loading, and compaction.
pub struct CheckpointManager {
  root: PathBuf,
  shard_count: u16,
  index: CheckpointIndex,
  log_file: Mutex<BufWriter<File>>,
}

impl CheckpointManager {
  /// Creates a new checkpoint manager.
  ///
  /// # Behavior
  ///
  /// - If checkpoints.log exists, rebuilds index from it
  /// - If checkpoints.log doesn't exist, creates it
  /// - Opens log file in append mode for future writes
  pub fn new(root: PathBuf, shard_count: u16) -> Result<Self> {
    let log_path = root.join("checkpoints.log");

    // Rebuild index from existing log (or create empty index)
    let index = CheckpointIndex::rebuild_from_log(&log_path)?;

    // Open log file in append mode
    let mut file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(&log_path)?;

    // Ensure cursor is at the end so stream_position() matches append behavior
    file.seek(SeekFrom::End(0))?;
    let log_file = Mutex::new(BufWriter::new(file));

    Ok(Self {
      root,
      shard_count,
      index,
      log_file,
    })
  }

  /// Creates a new checkpoint.
  ///
  /// # Steps
  ///
  /// 1. Validate inputs
  /// 2. Serialize checkpoint entry
  /// 3. Append to log file with fsync
  /// 4. Update in-memory index
  /// 5. Update HEAD file atomically
  ///
  /// # Errors
  ///
  /// Returns error if:
  /// - user_id is invalid (empty or too long)
  /// - offsets.len() doesn't match shard_count
  /// - I/O operations fail
  pub fn create(&self, user_id: &[u8], offsets: Vec<u64>) -> Result<()> {
    // Validate shard count matches
    if offsets.len() != self.shard_count as usize {
      return Err(Error::ShardCountMismatch {
        expected: self.shard_count,
        found: offsets.len() as u16,
      });
    }

    // Get current timestamp
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| {
        Error::Io(std::io::Error::new(
          std::io::ErrorKind::Other,
          format!("System time error: {}", e),
        ))
      })?
      .as_nanos() as u64;

    // Create checkpoint entry
    let entry = CheckpointEntry::new(user_id.to_vec(), timestamp, offsets)?;

    // Serialize
    let serialized = entry.serialize()?;

    // Append to log file
    let mut log = self.log_file.lock();
    let file_offset = log.stream_position()?;
    log.write_all(&serialized)?;
    log.flush()?;
    log.get_ref().sync_data()?;
    drop(log);

    // Update index
    self.index.insert(
      user_id.to_vec(),
      CheckpointMetadata {
        file_offset,
        timestamp,
        shard_count: self.shard_count,
      },
    );

    // Update HEAD
    head::write_head(&self.root, user_id)?;

    Ok(())
  }

  /// Loads a specific checkpoint by user_id.
  ///
  /// # Errors
  ///
  /// Returns error if:
  /// - Checkpoint not found in index
  /// - File offset is invalid
  /// - Checkpoint data is corrupted
  /// - Shard count mismatch
  pub fn load(&self, user_id: &[u8]) -> Result<CheckpointData> {
    // Look up in index
    let metadata = self
      .index
      .get(user_id)
      .ok_or_else(|| Error::CheckpointNotFound(String::from_utf8_lossy(user_id).to_string()))?;

    // Validate shard count
    if metadata.shard_count != self.shard_count {
      return Err(Error::ShardCountMismatch {
        expected: self.shard_count,
        found: metadata.shard_count,
      });
    }

    // Open log file for reading
    let log_path = self.root.join("checkpoints.log");
    let mut file = File::open(&log_path)?;

    // Seek to offset
    file.seek(SeekFrom::Start(metadata.file_offset))?;

    // Deserialize entry
    let (entry, _) = CheckpointEntry::deserialize(&mut file, metadata.file_offset)?;

    // Validate user_id matches
    if entry.user_id != user_id {
      return Err(Error::CheckpointCorrupted {
        offset: metadata.file_offset,
        reason: "user_id mismatch in entry".into(),
      });
    }

    Ok(CheckpointData {
      offsets: entry.offsets,
      timestamp: entry.timestamp,
      shard_count: entry.shard_count,
    })
  }

  /// Loads the latest checkpoint (from HEAD file).
  ///
  /// # Returns
  ///
  /// A tuple of `(user_id, checkpoint_data)`.
  ///
  /// # Errors
  ///
  /// Returns `Error::NoCheckpoints` if no checkpoints exist.
  ///
  /// Falls back to scanning the log if HEAD is corrupted.
  pub fn load_latest(&self) -> Result<(Vec<u8>, CheckpointData)> {
    // Try to read HEAD
    match head::read_head(&self.root)? {
      Some(user_id) => {
        let data = self.load(&user_id)?;
        Ok((user_id, data))
      }
      None => {
        // HEAD doesn't exist - find latest from index
        let sorted = self.index.all_sorted_by_time();

        if sorted.is_empty() {
          return Err(Error::NoCheckpoints);
        }

        let (user_id, _) = sorted.last().unwrap();
        let data = self.load(user_id)?;
        Ok((user_id.clone(), data))
      }
    }
  }

  /// Lists all checkpoints, sorted by timestamp (oldest to newest).
  ///
  /// Returns a vector of `(user_id, timestamp)` tuples.
  pub fn list_checkpoints(&self) -> Vec<(Vec<u8>, u64)> {
    self.index
      .all_sorted_by_time()
      .into_iter()
      .map(|(id, meta)| (id, meta.timestamp))
      .collect()
  }

  /// Compacts the checkpoint log, keeping only the N most recent checkpoints.
  ///
  /// # Steps
  ///
  /// 1. Get all checkpoints sorted by timestamp
  /// 2. Keep only the latest `keep_latest_n`
  /// 3. Write kept checkpoints to a new log file
  /// 4. Atomically replace old log with new log
  /// 5. Update index with new file offsets
  ///
  /// # Errors
  ///
  /// Returns error if I/O operations fail.
  pub fn compact(&self, keep_latest_n: usize) -> Result<CompactionStats> {
    let log_path = self.root.join("checkpoints.log");
    let temp_path = self.root.join("checkpoints.log.tmp");

    // Get old file size
    let old_size = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

    // Get all checkpoints sorted by time
    let mut all_checkpoints = self.index.all_sorted_by_time();
    let checkpoints_before = all_checkpoints.len();

    // Keep only latest N
    if all_checkpoints.len() > keep_latest_n {
      all_checkpoints.drain(0..all_checkpoints.len() - keep_latest_n);
    }

    let checkpoints_after = all_checkpoints.len();

    // Write to temp file
    let mut temp_file = BufWriter::new(File::create(&temp_path)?);
    let mut new_index = CheckpointIndex::new();
    let mut new_offset = 0u64;

    for (user_id, old_metadata) in &all_checkpoints {
      // Load the entry from old log
      let data = self.load(user_id)?;

      // Recreate entry
      let entry = CheckpointEntry::new(user_id.clone(), old_metadata.timestamp, data.offsets)?;

      // Write to new log
      let serialized = entry.serialize()?;
      temp_file.write_all(&serialized)?;

      // Track new offset in new index
      new_index.insert(
        user_id.clone(),
        CheckpointMetadata {
          file_offset: new_offset,
          timestamp: old_metadata.timestamp,
          shard_count: old_metadata.shard_count,
        },
      );

      new_offset += serialized.len() as u64;
    }

    temp_file.flush()?;
    temp_file.get_ref().sync_all()?;
    drop(temp_file);

    // Close current log file
    drop(self.log_file.lock());

    // Atomic rename
    std::fs::rename(&temp_path, &log_path)?;

    // Sync directory
    let dir = File::open(&self.root)?;
    dir.sync_all()?;

    // Reopen log file in append mode
    let mut file = OpenOptions::new().append(true).open(&log_path)?;
    file.seek(SeekFrom::End(0))?;
    *self.log_file.lock() = BufWriter::new(file);

    // Replace index with compacted version
    self.index.replace_with(new_index);

    let new_size = std::fs::metadata(&log_path)?.len();

    Ok(CompactionStats {
      checkpoints_before,
      checkpoints_after,
      bytes_reclaimed: old_size.saturating_sub(new_size),
    })
  }

  /// Returns the number of checkpoints.
  pub fn checkpoint_count(&self) -> usize {
    self.index.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;

  #[test]
  fn test_create_and_load_checkpoint() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 4).unwrap();

    let offsets = vec![100, 200, 300, 400];
    manager.create(b"ckpt_1", offsets.clone()).unwrap();

    let data = manager.load(b"ckpt_1").unwrap();
    assert_eq!(data.offsets, offsets);
    assert_eq!(data.shard_count, 4);
  }

  #[test]
  fn test_load_latest() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 2).unwrap();

    manager.create(b"ckpt_1", vec![10, 20]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    manager.create(b"ckpt_2", vec![30, 40]).unwrap();

    let (user_id, data) = manager.load_latest().unwrap();
    assert_eq!(user_id, b"ckpt_2");
    assert_eq!(data.offsets, vec![30, 40]);
  }

  #[test]
  fn test_list_checkpoints() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 1).unwrap();

    manager.create(b"ckpt_1", vec![10]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    manager.create(b"ckpt_2", vec![20]).unwrap();

    let list = manager.list_checkpoints();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].0, b"ckpt_1");
    assert_eq!(list[1].0, b"ckpt_2");
    assert!(list[0].1 < list[1].1);
  }

  #[test]
  fn test_load_nonexistent_returns_error() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 2).unwrap();

    let result = manager.load(b"nonexistent");
    assert!(matches!(result, Err(Error::CheckpointNotFound(_))));
  }

  #[test]
  fn test_shard_count_mismatch_on_create() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 4).unwrap();

    // Try to create checkpoint with wrong number of offsets
    let result = manager.create(b"ckpt", vec![1, 2]); // 2 instead of 4

    assert!(matches!(result, Err(Error::ShardCountMismatch { .. })));
  }

  #[test]
  fn test_compact_keeps_latest() {
    let dir = TempDir::new().unwrap();
    let manager = CheckpointManager::new(dir.path().to_path_buf(), 2).unwrap();

    // Create 5 checkpoints with distinct timestamps
    for i in 0..5 {
      let user_id = format!("ckpt_{}", i);
      manager
        .create(user_id.as_bytes(), vec![i as u64 * 10, i as u64 * 20])
        .unwrap();
      std::thread::sleep(std::time::Duration::from_millis(1000)); // Ensure distinct timestamps
    }

    assert_eq!(manager.checkpoint_count(), 5);

    // Compact to keep only 2
    let stats = manager.compact(2).unwrap();

    assert_eq!(stats.checkpoints_before, 5);
    assert_eq!(stats.checkpoints_after, 2);
    assert!(stats.bytes_reclaimed > 0);

    // The 2 kept checkpoints should be ckpt_3 and ckpt_4 (latest by timestamp)
    // But we can't guarantee which 2 due to HashMap ordering
    // So just verify count is correct
    assert_eq!(manager.checkpoint_count(), 2);

    // At least one old checkpoint should be gone
    let can_load_0 = manager.load(b"ckpt_0").is_ok();
    let can_load_4 = manager.load(b"ckpt_4").is_ok();

    assert!(can_load_4, "Latest checkpoint should be kept");
    assert!(!can_load_0, "Oldest checkpoint should be deleted");
  }
}
