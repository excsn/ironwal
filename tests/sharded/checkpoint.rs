//! Checkpoint integration tests

use ironwal::WalOptions;
use ironwal::sharded::ShardedWal;
use tempfile::TempDir;

fn setup(shard_count: u16) -> (ShardedWal, TempDir) {
  let dir = TempDir::new().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  let wal = ShardedWal::new(opts, shard_count).unwrap();
  (wal, dir)
}

#[test]
fn test_checkpoint_basic() {
  let (wal, _dir) = setup(4);

  // Write some data
  for i in 0..100 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data")
      .unwrap();
  }

  // Create checkpoint
  wal.create_checkpoint(b"checkpoint_1").unwrap();

  // Load it back
  let data = wal.load_checkpoint(b"checkpoint_1").unwrap();

  assert_eq!(data.shard_count, 4);
  assert_eq!(data.offsets.len(), 4);

  // All offsets should be > 0 since we wrote data
  let total: u64 = data.offsets.iter().sum();
  assert!(total > 0);
}

#[test]
fn test_checkpoint_iteration_from_offset() {
  let (wal, _dir) = setup(4);

  // Write 50 entries
  for i in 0..50 {
    wal
      .append(
        format!("key_{}", i).as_bytes(),
        format!("value_{}", i).as_bytes(),
      )
      .unwrap();
  }

  // Checkpoint at this point
  wal.create_checkpoint(b"ckpt_50").unwrap();

  // Write 50 more entries
  for i in 50..100 {
    wal
      .append(
        format!("key_{}", i).as_bytes(),
        format!("value_{}", i).as_bytes(),
      )
      .unwrap();
  }

  // Load checkpoint
  let checkpoint = wal.load_checkpoint(b"ckpt_50").unwrap();

  // Iterate from checkpoint offsets - should only see entries after checkpoint
  let mut count = 0;
  for shard_id in 0..4 {
    let offset = checkpoint.offsets[shard_id as usize];
    let mut iter = wal.iter_shard(shard_id, offset).unwrap();
    while iter.next().is_some() {
      count += 1;
    }
  }

  // Should see approximately 50 entries (the ones written after checkpoint)
  assert!(
    count >= 45 && count <= 55,
    "Expected ~50 entries, got {}",
    count
  );
}

#[test]
fn test_load_latest_checkpoint() {
  let (wal, _dir) = setup(4);

  wal.create_checkpoint(b"first").unwrap();
  std::thread::sleep(std::time::Duration::from_millis(10));
  wal.create_checkpoint(b"second").unwrap();
  std::thread::sleep(std::time::Duration::from_millis(10));
  wal.create_checkpoint(b"third").unwrap();

  let (user_id, _data) = wal.load_latest_checkpoint().unwrap();

  assert_eq!(user_id, b"third");
}

#[test]
fn test_checkpoint_overwrite_same_id() {
  let (wal, _dir) = setup(4);

  // Write some data
  for i in 0..10 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data1")
      .unwrap();
  }

  wal.create_checkpoint(b"reused_id").unwrap();
  let checkpoint1 = wal.load_checkpoint(b"reused_id").unwrap();

  // Write more data
  for i in 10..20 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data2")
      .unwrap();
  }

  // Reuse same checkpoint ID
  wal.create_checkpoint(b"reused_id").unwrap();
  let checkpoint2 = wal.load_checkpoint(b"reused_id").unwrap();

  // Second checkpoint should have higher offsets
  let sum1: u64 = checkpoint1.offsets.iter().sum();
  let sum2: u64 = checkpoint2.offsets.iter().sum();

  assert!(sum2 > sum1, "Reused checkpoint should have higher offsets");
}

#[test]
fn test_checkpoint_restart_persistence() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Create checkpoint
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..100 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data")
        .unwrap();
    }

    wal.create_checkpoint(b"persistent_checkpoint").unwrap();
  }

  // Phase 2: Restart and verify checkpoint exists
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    let data = wal.load_checkpoint(b"persistent_checkpoint").unwrap();

    assert_eq!(data.shard_count, 4);

    // Should be able to iterate from checkpoint offsets
    for shard_id in 0..4 {
      let offset = data.offsets[shard_id as usize];
      let _iter = wal.iter_shard(shard_id, offset).unwrap();
    }
  }
}

#[test]
fn test_no_checkpoints_error() {
  let (wal, _dir) = setup(4);

  let result = wal.load_latest_checkpoint();

  assert!(result.is_err());
  assert!(matches!(result.unwrap_err(), ironwal::Error::NoCheckpoints));
}

#[test]
fn test_checkpoint_not_found() {
  let (wal, _dir) = setup(4);

  wal.create_checkpoint(b"exists").unwrap();

  let result = wal.load_checkpoint(b"does_not_exist");

  assert!(result.is_err());
  assert!(matches!(
    result.unwrap_err(),
    ironwal::Error::CheckpointNotFound(_)
  ));
}

#[test]
fn test_checkpoint_with_empty_shards() {
  let (wal, _dir) = setup(16);

  // Only write to one or two shards
  wal.append(b"key1", b"data1").unwrap();
  wal.append(b"key1", b"data2").unwrap();

  wal.create_checkpoint(b"sparse_checkpoint").unwrap();

  let data = wal.load_checkpoint(b"sparse_checkpoint").unwrap();

  assert_eq!(data.offsets.len(), 16);

  // Most offsets should be 0 (no data written)
  let zero_count = data.offsets.iter().filter(|&&x| x == 0).count();
  assert!(zero_count >= 14, "Expected most shards to be empty");
}

#[test]
fn test_multiple_checkpoints() {
  let (wal, _dir) = setup(4);

  // Create 10 checkpoints
  for i in 0..10 {
    for j in 0..10 {
      wal
        .append(format!("key_{}", i * 10 + j).as_bytes(), b"data")
        .unwrap();
    }

    let checkpoint_id = format!("checkpoint_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
  }

  // All should be loadable
  for i in 0..10 {
    let checkpoint_id = format!("checkpoint_{}", i);
    let data = wal.load_checkpoint(checkpoint_id.as_bytes()).unwrap();
    assert_eq!(data.shard_count, 4);
  }
}
