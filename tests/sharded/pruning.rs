//! Pruning and compaction tests

use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::fs;
use tempfile::TempDir;

fn setup(shard_count: u16) -> (ShardedWal, TempDir) {
  let dir = TempDir::new().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = SyncMode::Strict;
  // Very small segments to force rotation
  opts.max_segment_size = 2_000; // 2KB\
  let wal = ShardedWal::new(opts, shard_count).unwrap();
  (wal, dir)
}

#[test]
fn test_prune_before_checkpoint() {
  let (wal, _dir) = setup(4);

  // Write enough data to force multiple segments
  // With 2KB segment size and 200 byte entries, ~10 entries per segment
  for i in 0..100 {
    let data = vec![0xAB; 200];
    wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
  }

  // Create checkpoint
  wal.create_checkpoint(b"midpoint").unwrap();

  // Write significantly more data to force new segments
  for i in 100..300 {
    let data = vec![0xCD; 200];
    wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
  }

  // Prune before checkpoint
  let stats = wal.prune_before_checkpoint(b"midpoint").unwrap();

  println!(
    "Pruned {} segments from {} shards",
    stats.segments_deleted, stats.shards_pruned
  );
  assert!(
    stats.segments_deleted > 0,
    "Expected segments to be deleted"
  );

  // Data after checkpoint should still be readable
  let checkpoint = wal.load_checkpoint(b"midpoint").unwrap();
  for shard_id in 0..4 {
    let offset = checkpoint.offsets[shard_id as usize];
    if offset > 0 {
      let mut iter = wal.iter_shard(shard_id, offset).unwrap();
      // Should be able to read from checkpoint offset
      let _first = iter.next();
    }
  }
}

#[test]
fn test_prune_nonexistent_checkpoint() {
  let (wal, _dir) = setup(4);

  wal.append(b"key", b"data").unwrap();

  let result = wal.prune_before_checkpoint(b"does_not_exist");

  assert!(result.is_err());
  assert!(matches!(
    result.unwrap_err(),
    ironwal::Error::CheckpointNotFound(_)
  ));
}

#[test]
fn test_prune_empty_checkpoint() {
  let (wal, _dir) = setup(4);

  // Create checkpoint with no data
  wal.create_checkpoint(b"empty").unwrap();

  // Pruning should succeed but delete nothing
  let stats = wal.prune_before_checkpoint(b"empty").unwrap();

  assert_eq!(stats.segments_deleted, 0);
}

#[test]
fn test_compact_checkpoints() {
  let (wal, _dir) = setup(4);

  // Create 10 checkpoints
  for i in 0..10 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data")
      .unwrap();
    let checkpoint_id = format!("ckpt_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
  }

  // Compact to keep only 3
  let stats = wal.compact_checkpoints(3).unwrap();

  assert_eq!(stats.checkpoints_before, 10);
  assert_eq!(stats.checkpoints_after, 3);
  assert!(stats.bytes_reclaimed > 0);

  // Oldest 7 should be gone
  for i in 0..7 {
    let checkpoint_id = format!("ckpt_{}", i);
    assert!(wal.load_checkpoint(checkpoint_id.as_bytes()).is_err());
  }

  // Latest 3 should exist
  for i in 7..10 {
    let checkpoint_id = format!("ckpt_{}", i);
    assert!(wal.load_checkpoint(checkpoint_id.as_bytes()).is_ok());
  }
}

#[test]
fn test_compact_keep_more_than_exists() {
  let (wal, _dir) = setup(4);

  // Create 5 checkpoints
  for i in 0..5 {
    let checkpoint_id = format!("ckpt_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
  }

  // Compact to keep 10 (more than exist)
  let stats = wal.compact_checkpoints(10).unwrap();

  assert_eq!(stats.checkpoints_before, 5);
  assert_eq!(stats.checkpoints_after, 5);

  // All should still exist
  for i in 0..5 {
    let checkpoint_id = format!("ckpt_{}", i);
    assert!(wal.load_checkpoint(checkpoint_id.as_bytes()).is_ok());
  }
}

#[test]
fn test_prune_then_write() {
  let (wal, _dir) = setup(4);

  // Write data
  for i in 0..100 {
    let data = vec![i as u8; 100];
    wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
  }

  wal.create_checkpoint(b"before_prune").unwrap();

  // Prune
  wal.prune_before_checkpoint(b"before_prune").unwrap();

  // Should still be able to write
  for i in 100..200 {
    let data = vec![i as u8; 100];
    wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
  }

  // Create new checkpoint
  wal.create_checkpoint(b"after_prune").unwrap();

  let checkpoint = wal.load_checkpoint(b"after_prune").unwrap();
  let total: u64 = checkpoint.offsets.iter().sum();

  assert_eq!(total, 200, "Should have 200 total entries");
}

#[test]
fn test_prune_restart_prune() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Write and checkpoint
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.sync_mode = SyncMode::Strict;
    opts.max_segment_size = 10_000;
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..300 {
      let data = vec![0xAB; 100];
      wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
    }

    wal.create_checkpoint(b"ckpt_1").unwrap();

    for i in 300..600 {
      let data = vec![0xCD; 100];
      wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
    }

    wal.create_checkpoint(b"ckpt_2").unwrap();
  }

  // Phase 2: Restart and prune
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    opts.sync_mode = SyncMode::Strict;
    opts.max_segment_size = 10_000;
    let wal = ShardedWal::new(opts, 4).unwrap();

    // Prune before ckpt_2
    let stats = wal.prune_before_checkpoint(b"ckpt_2").unwrap();

    assert!(stats.segments_deleted > 0);

    // Should still be able to read from ckpt_2
    let checkpoint = wal.load_checkpoint(b"ckpt_2").unwrap();
    for shard_id in 0..4 {
      let offset = checkpoint.offsets[shard_id as usize];
      let _iter = wal.iter_shard(shard_id, offset).unwrap();
    }
  }
}

#[test]
fn test_compact_after_restart() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Create checkpoints
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..10 {
      let checkpoint_id = format!("ckpt_{}", i);
      wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
      std::thread::sleep(std::time::Duration::from_millis(10));
    }
  }

  // Phase 2: Restart and compact
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    let stats = wal.compact_checkpoints(3).unwrap();

    assert_eq!(stats.checkpoints_before, 10);
    assert_eq!(stats.checkpoints_after, 3);

    // Latest 3 should be accessible
    for i in 7..10 {
      let checkpoint_id = format!("ckpt_{}", i);
      assert!(wal.load_checkpoint(checkpoint_id.as_bytes()).is_ok());
    }
  }
}
