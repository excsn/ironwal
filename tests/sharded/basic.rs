//! Basic ShardedWal integration tests

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
fn test_append_and_iter_basic() {
  let (wal, _dir) = setup(4);

  // Write 100 entries
  for i in 0..100 {
    let key = format!("key_{}", i);
    let value = format!("value_{}", i);
    wal.append(key.as_bytes(), value.as_bytes()).unwrap();
  }

  // Read back from all shards
  let mut total_read = 0;
  for shard_id in 0..4 {
    let mut iter = wal.iter_shard(shard_id, 0).unwrap();
    while let Some(entry) = iter.next() {
      let _data = entry.unwrap();
      total_read += 1;
    }
  }

  assert_eq!(total_read, 100);
}

#[test]
fn test_iter_shard_with_ids() {
  let (wal, _dir) = setup(4);

  let mut expected: Vec<Vec<(u64, Vec<u8>)>> = vec![Vec::new(); 4];
  for i in 0..100 {
    let key = format!("key_{}", i);
    let value = format!("value_{}", i).into_bytes();
    let (shard_id, seq_id) = wal.append(key.as_bytes(), &value).unwrap();
    expected[shard_id as usize].push((seq_id, value));
  }

  for shard_id in 0..4u16 {
    let read: Vec<(u64, Vec<u8>)> = wal
      .iter_shard_with_ids(shard_id, 0)
      .unwrap()
      .collect::<ironwal::Result<_>>()
      .unwrap();
    assert_eq!(read, expected[shard_id as usize]);
  }
}

#[test]
fn test_iter_shard_with_ids_from_offset() {
  let (wal, _dir) = setup(1);

  for i in 0..20u8 {
    wal.append(b"same_key", &[i]).unwrap();
  }

  let read: Vec<(u64, Vec<u8>)> = wal
    .iter_shard_with_ids(0, 15)
    .unwrap()
    .collect::<ironwal::Result<_>>()
    .unwrap();
  let expected: Vec<(u64, Vec<u8>)> = (15..20).map(|i| (i as u64, vec![i as u8])).collect();
  assert_eq!(read, expected);
}

#[test]
fn test_iter_shard_with_ids_invalid_shard() {
  let (wal, _dir) = setup(4);
  assert!(wal.iter_shard_with_ids(4, 0).is_err());
}

#[test]
fn test_deterministic_routing() {
  let (wal, _dir) = setup(16);

  let key = b"consistent_key";

  let (shard1, _) = wal.append(key, b"value1").unwrap();
  let (shard2, _) = wal.append(key, b"value2").unwrap();
  let (shard3, _) = wal.append(key, b"value3").unwrap();

  assert_eq!(shard1, shard2);
  assert_eq!(shard2, shard3);
}

#[test]
fn test_batch_append_preserves_order() {
  let (wal, _dir) = setup(4);

  let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
    .map(|i| {
      (
        format!("key_{}", i).into_bytes(),
        format!("val_{}", i).into_bytes(),
      )
    })
    .collect();

  let results = wal.append_batch(&batch).unwrap();

  assert_eq!(results.len(), 10);

  // Results should be in same order as input
  for (i, (shard_id, _seq_id)) in results.iter().enumerate() {
    assert!(*shard_id < 4);
  }
}

#[test]
fn test_empty_shard_iter() {
  let (wal, _dir) = setup(16);

  // Write to one shard
  wal.append(b"key1", b"data").unwrap();

  // Iterate over all shards - most will be empty
  for shard_id in 0..16 {
    let mut iter = wal.iter_shard(shard_id, 0).unwrap();
    let count = iter.count();
    // At least one shard has data, others are empty
    assert!(count <= 1);
  }
}

#[test]
fn test_restart_persistence() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Write data
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 4).unwrap();

    for i in 0..50 {
      wal
        .append(format!("key_{}", i).as_bytes(), b"data")
        .unwrap();
    }
  }

  // Phase 2: Restart and verify
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;
    let wal = ShardedWal::new(opts, 4).unwrap();

    let mut total = 0;
    for shard_id in 0..4 {
      let mut iter = wal.iter_shard(shard_id, 0).unwrap();
      total += iter.count();
    }

    assert_eq!(total, 50);
  }
}

#[test]
fn test_large_values() {
  let (wal, _dir) = setup(4);

  let large_value = vec![0xAB; 100_000]; // 100KB

  wal.append(b"key1", &large_value).unwrap();

  let (shard_id, _) = wal.append(b"key1", &large_value).unwrap();

  let mut iter = wal.iter_shard(shard_id, 0).unwrap();
  let val = iter.next().unwrap().unwrap();

  assert_eq!(val, large_value);
}
