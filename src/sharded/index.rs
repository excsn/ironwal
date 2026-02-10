//! In-memory index for fast checkpoint lookups by user_id.
//!
//! The index maps user_id → file offset in checkpoints.log, enabling O(1)
//! checkpoint retrieval without scanning the entire log.

use crate::error::{Error, Result};
use crate::sharded::checkpoint_log::{CheckpointEntry, HEADER_SIZE};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;

/// Metadata about a checkpoint stored in the index.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointMetadata {
  /// Byte offset in checkpoints.log where the entry starts
  pub file_offset: u64,

  /// Unix timestamp when checkpoint was created
  pub timestamp: u64,

  /// Number of shards (for validation)
  pub shard_count: u16,
}

/// Thread-safe in-memory index of checkpoints.
pub struct CheckpointIndex {
  inner: RwLock<HashMap<Vec<u8>, CheckpointMetadata>>,
}

impl CheckpointIndex {
  /// Creates an empty index.
  pub fn new() -> Self {
    Self {
      inner: RwLock::new(HashMap::new()),
    }
  }

  /// Rebuilds the index by scanning the checkpoints.log file.
  ///
  /// This is called during startup to reconstruct the in-memory index.
  ///
  /// # Corruption Handling
  ///
  /// If corruption is detected mid-file, scanning stops at the last valid
  /// entry. Partial recovery is allowed.
  pub fn rebuild_from_log(log_path: &Path) -> Result<Self> {
    let index = Self::new();

    if !log_path.exists() {
      return Ok(index); // Empty log = empty index
    }

    let file = File::open(log_path)?;
    let mut reader = BufReader::new(file);
    let mut file_offset = 0u64;

    loop {
      // Try to read next entry
      match CheckpointEntry::deserialize(&mut reader, file_offset) {
        Ok((entry, next_offset)) => {
          // Insert into index (duplicate user_ids overwrite)
          index.insert(
            entry.user_id,
            CheckpointMetadata {
              file_offset,
              timestamp: entry.timestamp,
              shard_count: entry.shard_count,
            },
          );

          file_offset = next_offset;
        }
        Err(Error::CheckpointCorrupted { offset, reason }) => {
          // Corruption detected - stop scanning but return partial index
          tracing::warn!(
              target: "ironwal::sharded",
              "Checkpoint log corrupted at offset {}: {}. Truncating scan.",
              offset,
              reason
          );
          break;
        }
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
          // Clean EOF - we're done
          break;
        }
        Err(e) => return Err(e),
      }
    }

    Ok(index)
  }

  /// Gets metadata for a checkpoint.
  ///
  /// Returns `None` if the checkpoint doesn't exist.
  pub fn get(&self, user_id: &[u8]) -> Option<CheckpointMetadata> {
    self.inner.read().get(user_id).copied()
  }

  /// Inserts or updates a checkpoint in the index.
  pub fn insert(&self, user_id: Vec<u8>, metadata: CheckpointMetadata) {
    self.inner.write().insert(user_id, metadata);
  }

  /// Returns all checkpoints sorted by timestamp (oldest first).
  pub fn all_sorted_by_time(&self) -> Vec<(Vec<u8>, CheckpointMetadata)> {
    let map = self.inner.read();
    let mut entries: Vec<_> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();

    entries.sort_by_key(|(_, meta)| meta.timestamp);
    entries
  }

  /// Returns the number of checkpoints in the index.
  pub fn len(&self) -> usize {
    self.inner.read().len()
  }

  /// Returns true if the index is empty.
  pub fn is_empty(&self) -> bool {
    self.inner.read().is_empty()
  }

  /// Replaces the entire index with a new one (used during compaction).
  pub fn replace_with(&self, new_index: CheckpointIndex) {
    let mut map = self.inner.write();
    *map = new_index.inner.into_inner();
  }
}

impl Default for CheckpointIndex {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Write;
  use tempfile::TempDir;

  #[test]
  fn test_empty_index() {
    let index = CheckpointIndex::new();
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());
    assert!(index.get(b"test").is_none());
  }

  #[test]
  fn test_insert_and_get() {
    let index = CheckpointIndex::new();

    let meta = CheckpointMetadata {
      file_offset: 100,
      timestamp: 1000,
      shard_count: 16,
    };

    index.insert(b"checkpoint_1".to_vec(), meta);

    let retrieved = index.get(b"checkpoint_1").unwrap();
    assert_eq!(retrieved.file_offset, 100);
    assert_eq!(retrieved.timestamp, 1000);
  }

  #[test]
  fn test_duplicate_user_id_overwrites() {
    let index = CheckpointIndex::new();

    index.insert(
      b"ckpt".to_vec(),
      CheckpointMetadata {
        file_offset: 100,
        timestamp: 1000,
        shard_count: 16,
      },
    );

    index.insert(
      b"ckpt".to_vec(),
      CheckpointMetadata {
        file_offset: 200,
        timestamp: 2000,
        shard_count: 16,
      },
    );

    let meta = index.get(b"ckpt").unwrap();
    assert_eq!(meta.file_offset, 200); // Latest wins
  }

  #[test]
  fn test_all_sorted_by_time() {
    let index = CheckpointIndex::new();

    index.insert(
      b"ckpt_3".to_vec(),
      CheckpointMetadata {
        file_offset: 300,
        timestamp: 3000,
        shard_count: 16,
      },
    );

    index.insert(
      b"ckpt_1".to_vec(),
      CheckpointMetadata {
        file_offset: 100,
        timestamp: 1000,
        shard_count: 16,
      },
    );

    index.insert(
      b"ckpt_2".to_vec(),
      CheckpointMetadata {
        file_offset: 200,
        timestamp: 2000,
        shard_count: 16,
      },
    );

    let sorted = index.all_sorted_by_time();

    assert_eq!(sorted.len(), 3);
    assert_eq!(sorted[0].0, b"ckpt_1");
    assert_eq!(sorted[1].0, b"ckpt_2");
    assert_eq!(sorted[2].0, b"ckpt_3");
  }

  #[test]
  fn test_rebuild_from_empty_log() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().join("checkpoints.log");

    // Create empty file
    File::create(&log_path).unwrap();

    let index = CheckpointIndex::rebuild_from_log(&log_path).unwrap();
    assert!(index.is_empty());
  }

  #[test]
  fn test_rebuild_from_nonexistent_log() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().join("nonexistent.log");

    let index = CheckpointIndex::rebuild_from_log(&log_path).unwrap();
    assert!(index.is_empty());
  }

  #[test]
  fn test_rebuild_from_valid_log() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().join("checkpoints.log");

    // Write 2 entries manually
    let mut file = File::create(&log_path).unwrap();

    let entry1 = CheckpointEntry::new(b"ckpt_1".to_vec(), 1000, vec![10, 20]).unwrap();
    file.write_all(&entry1.serialize().unwrap()).unwrap();

    let entry2 = CheckpointEntry::new(b"ckpt_2".to_vec(), 2000, vec![30, 40]).unwrap();
    file.write_all(&entry2.serialize().unwrap()).unwrap();

    drop(file);

    // Rebuild
    let index = CheckpointIndex::rebuild_from_log(&log_path).unwrap();

    assert_eq!(index.len(), 2);
    assert!(index.get(b"ckpt_1").is_some());
    assert!(index.get(b"ckpt_2").is_some());
  }
}
