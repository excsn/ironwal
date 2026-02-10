//! HEAD file management for tracking the latest checkpoint.
//!
//! The HEAD file stores the user_id of the most recent checkpoint, enabling
//! O(1) "get latest checkpoint" operations without scanning the log.
//!
//! ## Atomicity
//!
//! Updates use the temp-file + rename + directory fsync pattern to ensure
//! durability across crashes and power failures.

use crate::error::{Error, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Filename for the HEAD pointer file
const HEAD_FILENAME: &str = "HEAD";

/// Reads the user_id from the HEAD file.
///
/// Returns `None` if the file doesn't exist or is corrupted.
///
/// # Errors
///
/// Only returns error for I/O failures other than corruption/missing file.
pub fn read_head(root: &Path) -> Result<Option<Vec<u8>>> {
  let path = root.join(HEAD_FILENAME);

  if !path.exists() {
    return Ok(None);
  }

  let mut file = match File::open(&path) {
    Ok(f) => f,
    Err(_) => return Ok(None),
  };

  // Read user_id_len
  let user_id_len = match file.read_u16::<LittleEndian>() {
    Ok(len) => len as usize,
    Err(_) => {
      // Corrupted - return None to trigger fallback
      tracing::warn!(target: "ironwal::sharded", "HEAD file corrupted, will scan checkpoint log");
      return Ok(None);
    }
  };

  if user_id_len == 0 {
    tracing::warn!(target: "ironwal::sharded", "HEAD file has zero-length user_id, will scan checkpoint log");
    return Ok(None);
  }

  // Read user_id
  let mut user_id = vec![0u8; user_id_len];
  match file.read_exact(&mut user_id) {
    Ok(_) => Ok(Some(user_id)),
    Err(_) => {
      // Corrupted - return None to trigger fallback
      tracing::warn!(target: "ironwal::sharded", "HEAD file corrupted (incomplete user_id), will scan checkpoint log");
      Ok(None)
    }
  }
}

/// Atomically writes a new user_id to the HEAD file.
///
/// Uses temp file + rename + directory fsync for durability.
///
/// # Errors
///
/// Returns error if:
/// - user_id is empty
/// - user_id exceeds 65535 bytes
/// - I/O operations fail
pub fn write_head(root: &Path, user_id: &[u8]) -> Result<()> {
  if user_id.is_empty() {
    return Err(Error::InvalidCheckpointId("user_id cannot be empty".into()));
  }

  if user_id.len() > u16::MAX as usize {
    return Err(Error::InvalidCheckpointId(format!(
      "user_id too long: {} bytes",
      user_id.len()
    )));
  }

  let temp_path = root.join(format!("{}.tmp", HEAD_FILENAME));
  let final_path = root.join(HEAD_FILENAME);

  // 1. Write to temp file
  let mut file = File::create(&temp_path)?;
  file.write_u16::<LittleEndian>(user_id.len() as u16)?;
  file.write_all(user_id)?;
  file.sync_all()?;
  drop(file);

  // 2. Atomic rename
  fs::rename(&temp_path, &final_path)?;

  // 3. Sync directory to persist rename
  let dir = OpenOptions::new().read(true).open(root)?;
  dir.sync_all()?;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;

  #[test]
  fn test_read_nonexistent_returns_none() {
    let dir = TempDir::new().unwrap();
    let result = read_head(dir.path()).unwrap();
    assert_eq!(result, None);
  }

  #[test]
  fn test_write_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let user_id = b"checkpoint_42";

    write_head(dir.path(), user_id).unwrap();
    let read_back = read_head(dir.path()).unwrap().unwrap();

    assert_eq!(read_back, user_id);
  }

  #[test]
  fn test_write_overwrites_previous() {
    let dir = TempDir::new().unwrap();

    write_head(dir.path(), b"first").unwrap();
    write_head(dir.path(), b"second").unwrap();

    let result = read_head(dir.path()).unwrap().unwrap();
    assert_eq!(result, b"second");
  }

  #[test]
  fn test_rejects_empty_user_id() {
    let dir = TempDir::new().unwrap();
    let result = write_head(dir.path(), b"");
    assert!(result.is_err());
  }

  // HEAD file atomicity and corruption recovery tests

  #[test]
  fn test_atomic_update_survives_corruption() {
    let dir = TempDir::new().unwrap();

    write_head(dir.path(), b"checkpoint_1").unwrap();

    // Simulate crash during write by creating corrupt temp file
    let temp_path = dir.path().join("HEAD.tmp");
    fs::write(&temp_path, b"garbage").unwrap();

    // Previous HEAD should still be intact
    let result = read_head(dir.path()).unwrap().unwrap();
    assert_eq!(result, b"checkpoint_1");
  }

  #[test]
  fn test_large_user_id() {
    let dir = TempDir::new().unwrap();
    let large_id = vec![0xAB; 1000];

    write_head(dir.path(), &large_id).unwrap();
    let result = read_head(dir.path()).unwrap().unwrap();

    assert_eq!(result, large_id);
  }
}
