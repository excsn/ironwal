//! Recovery and crash simulation tests

use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_restart_with_checkpoints() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Write data and create checkpoints
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.sync_mode = SyncMode::Strict;
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..50 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data1")
        .unwrap();
    }
    wal.create_checkpoint(b"checkpoint_1").unwrap();

    for i in 50..100 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data2")
        .unwrap();
    }
    wal.create_checkpoint(b"checkpoint_2").unwrap();
  }

  // Phase 2: Restart and verify both checkpoints
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    // Both checkpoints should exist
    let ckpt1 = wal.load_checkpoint(b"checkpoint_1").unwrap();
    let ckpt2 = wal.load_checkpoint(b"checkpoint_2").unwrap();

    // Second checkpoint should have higher offsets
    let sum1: u64 = ckpt1.offsets.iter().sum();
    let sum2: u64 = ckpt2.offsets.iter().sum();
    assert!(sum2 > sum1);

    // Latest should be checkpoint_2
    let (latest_id, _) = wal.load_latest_checkpoint().unwrap();
    assert_eq!(latest_id, b"checkpoint_2");
  }
}

#[test]
fn test_head_file_recovery() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Create checkpoints
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    wal.create_checkpoint(b"ckpt_1").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    wal.create_checkpoint(b"ckpt_2").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    wal.create_checkpoint(b"ckpt_3").unwrap();
  }

  // Corrupt HEAD file
  let head_path = root.join("HEAD");
  fs::write(&head_path, b"garbage").unwrap();

  // Restart - should fall back to scanning checkpoints.log
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    // Should still be able to load latest (via log scan fallback)
    let (latest_id, _) = wal.load_latest_checkpoint().unwrap();
    assert_eq!(latest_id, b"ckpt_3");
  }
}

#[test]
fn test_checkpoint_log_rebuild() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Create checkpoints
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..5 {
      let checkpoint_id = format!("ckpt_{}", i);
      wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
      std::thread::sleep(std::time::Duration::from_millis(10));
    }
  }

  // Restart - index should be rebuilt from checkpoints.log
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    // All checkpoints should be accessible
    for i in 0..5 {
      let checkpoint_id = format!("ckpt_{}", i);
      let data = wal.load_checkpoint(checkpoint_id.as_bytes()).unwrap();
      assert_eq!(data.shard_count, 4);
    }
  }
}

#[test]
fn test_shard_count_mismatch_error() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Create with 4 shards
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    wal.append(b"key", b"data").unwrap();
    wal.create_checkpoint(b"ckpt").unwrap();
  }

  // Try to open with 8 shards
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 8).unwrap();

    // Loading checkpoint should fail with shard count mismatch
    let result = wal.load_checkpoint(b"ckpt");

    assert!(result.is_err());
    assert!(matches!(
      result.unwrap_err(),
      ironwal::Error::ShardCountMismatch { .. }
    ));
  }
}

#[test]
fn test_watermark_initialization_on_restart() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Write data without checkpoint
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.sync_mode = SyncMode::Strict;
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..100 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data")
        .unwrap();
    }
  }

  // Phase 2: Restart and create checkpoint
  // Watermarks should initialize from WAL state
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    opts.sync_mode = SyncMode::Strict;
    let wal = ShardedWal::new(opts, 4).unwrap();

    // Create checkpoint - should capture current state
    wal.create_checkpoint(b"after_restart").unwrap();

    let checkpoint = wal.load_checkpoint(b"after_restart").unwrap();

    // Checkpoint offsets should reflect the 100 entries written before restart
    let total: u64 = checkpoint.offsets.iter().sum();
    assert_eq!(total, 100, "Watermarks should initialize from WAL state");
  }
}

#[test]
fn test_empty_wal_checkpoint() {
  let (wal, _dir) = {
    let dir = TempDir::new().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    let wal = ShardedWal::new(opts, 4).unwrap();
    (wal, dir)
  };

  // Create checkpoint with no data written
  wal.create_checkpoint(b"empty_checkpoint").unwrap();

  let data = wal.load_checkpoint(b"empty_checkpoint").unwrap();

  // All offsets should be 0
  assert_eq!(data.offsets, vec![0, 0, 0, 0]);
}

#[test]
fn test_concurrent_restart_and_checkpoint() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Create initial checkpoint
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    wal.append(b"key", b"data").unwrap();
    wal.create_checkpoint(b"initial").unwrap();
  }

  // Restart multiple times, creating checkpoints
  for i in 0..5 {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    // Write data
    wal
      .append(format!("key_{}", i).as_bytes(), b"data")
      .unwrap();

    // Create checkpoint
    let checkpoint_id = format!("restart_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
  }

  // Final restart - all checkpoints should exist
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    wal.load_checkpoint(b"initial").unwrap();
    for i in 0..5 {
      let checkpoint_id = format!("restart_{}", i);
      wal.load_checkpoint(checkpoint_id.as_bytes()).unwrap();
    }
  }
}
