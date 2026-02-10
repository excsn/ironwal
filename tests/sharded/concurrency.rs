//! Concurrent operations tests

use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn setup(shard_count: u16) -> (ShardedWal, TempDir) {
  let dir = TempDir::new().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = SyncMode::BatchOnly; // Faster for concurrency tests
  let wal = ShardedWal::new(opts, shard_count).unwrap();
  (wal, dir)
}

#[test]
fn test_concurrent_appends() {
  let (wal, _dir) = setup(16);
  let wal = Arc::new(wal);

  let mut handles = vec![];

  // Spawn 10 threads, each writing 100 entries
  for thread_id in 0..10 {
    let wal_clone = wal.clone();
    handles.push(thread::spawn(move || {
      for i in 0..100 {
        let key = format!("thread_{}_key_{}", thread_id, i);
        let value = format!("value_{}", i);
        wal_clone.append(key.as_bytes(), value.as_bytes()).unwrap();
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // Count total entries across all shards
  let mut total = 0;
  for shard_id in 0..16 {
    let mut iter = wal.iter_shard(shard_id, 0).unwrap();
    while iter.next().is_some() {
      total += 1;
    }
  }

  assert_eq!(total, 1000, "Should have all 1000 entries");
}

#[test]
fn test_concurrent_batch_appends() {
  let (wal, _dir) = setup(8);
  let wal = Arc::new(wal);

  let mut handles = vec![];

  for thread_id in 0..5 {
    let wal_clone = wal.clone();
    handles.push(thread::spawn(move || {
      for batch_id in 0..20 {
        let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
          .map(|i| {
            let key = format!("t{}_b{}_k{}", thread_id, batch_id, i);
            let value = format!("value_{}", i);
            (key.into_bytes(), value.into_bytes())
          })
          .collect();

        wal_clone.append_batch(&batch).unwrap();
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // Total: 5 threads * 20 batches * 10 entries = 1000
  let mut total = 0;
  for shard_id in 0..8 {
    let mut iter = wal.iter_shard(shard_id, 0).unwrap();
    while iter.next().is_some() {
      total += 1;
    }
  }

  assert_eq!(total, 1000);
}

#[test]
fn test_concurrent_writes_and_checkpoint() {
  let (wal, _dir) = setup(8);
  let wal = Arc::new(wal);

  let wal_writer = wal.clone();
  let write_handle = thread::spawn(move || {
    for i in 0..500 {
      let key = format!("key_{}", i);
      wal_writer.append(key.as_bytes(), b"data").unwrap();
    }
  });

  let wal_checkpoint = wal.clone();
  let checkpoint_handle = thread::spawn(move || {
    // Create checkpoints while writes are happening
    for i in 0..10 {
      thread::sleep(std::time::Duration::from_millis(5));
      let checkpoint_id = format!("ckpt_{}", i);
      wal_checkpoint
        .create_checkpoint(checkpoint_id.as_bytes())
        .unwrap();
    }
  });

  write_handle.join().unwrap();
  checkpoint_handle.join().unwrap();

  // All checkpoints should be loadable
  for i in 0..10 {
    let checkpoint_id = format!("ckpt_{}", i);
    wal.load_checkpoint(checkpoint_id.as_bytes()).unwrap();
  }
}

#[test]
fn test_concurrent_checkpoint_loads() {
  let (wal, _dir) = setup(4);

  // Create some checkpoints
  for i in 0..5 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data")
      .unwrap();
    let checkpoint_id = format!("ckpt_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
    thread::sleep(std::time::Duration::from_millis(10));
  }

  let wal = Arc::new(wal);
  let mut handles = vec![];

  // Spawn threads to concurrently load checkpoints
  for thread_id in 0..10 {
    let wal_clone = wal.clone();
    handles.push(thread::spawn(move || {
      for _ in 0..20 {
        for i in 0..5 {
          let checkpoint_id = format!("ckpt_{}", i);
          let data = wal_clone.load_checkpoint(checkpoint_id.as_bytes()).unwrap();
          assert_eq!(data.shard_count, 4);
        }
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }
}

#[test]
fn test_concurrent_writes_and_prune() {
  let (wal, _dir) = setup(4);

  // Write initial data and checkpoint
  for i in 0..100 {
    let data = vec![0xAB; 100];
    wal.append(format!("key_{}", i).as_bytes(), &data).unwrap();
  }
  wal.create_checkpoint(b"prune_point").unwrap();

  let wal = Arc::new(wal);

  // Concurrent writer
  let wal_writer = wal.clone();
  let write_handle = thread::spawn(move || {
    for i in 100..200 {
      let data = vec![0xCD; 100];
      wal_writer
        .append(format!("key_{}", i).as_bytes(), &data)
        .unwrap();
    }
  });

  // Concurrent pruner
  let wal_pruner = wal.clone();
  let prune_handle = thread::spawn(move || {
    thread::sleep(std::time::Duration::from_millis(10));
    wal_pruner.prune_before_checkpoint(b"prune_point").unwrap();
  });

  write_handle.join().unwrap();
  prune_handle.join().unwrap();

  // Should still be able to read from checkpoint forward
  let checkpoint = wal.load_checkpoint(b"prune_point").unwrap();
  for shard_id in 0..4 {
    let offset = checkpoint.offsets[shard_id as usize];
    if offset > 0 {
      let _iter = wal.iter_shard(shard_id, offset).unwrap();
    }
  }
}

#[test]
fn test_concurrent_compaction() {
  let (wal, _dir) = setup(4);

  // Create many checkpoints
  for i in 0..20 {
    let checkpoint_id = format!("ckpt_{}", i);
    wal.create_checkpoint(checkpoint_id.as_bytes()).unwrap();
    thread::sleep(std::time::Duration::from_millis(5));
  }

  let wal = Arc::new(wal);

  // Concurrent checkpoint creation
  let wal_creator = wal.clone();
  let create_handle = thread::spawn(move || {
    for i in 20..30 {
      let checkpoint_id = format!("ckpt_{}", i);
      wal_creator
        .create_checkpoint(checkpoint_id.as_bytes())
        .unwrap();
      thread::sleep(std::time::Duration::from_millis(10));
    }
  });

  // Concurrent compaction
  let wal_compactor = wal.clone();
  let compact_handle = thread::spawn(move || {
    thread::sleep(std::time::Duration::from_millis(50));
    wal_compactor.compact_checkpoints(10).unwrap();
  });

  create_handle.join().unwrap();
  compact_handle.join().unwrap();

  // Should have some checkpoints remaining
  let (_, checkpoint) = wal.load_latest_checkpoint().unwrap();
  assert_eq!(checkpoint.shard_count, 4);
}

#[test]
fn test_many_concurrent_readers() {
  let (wal, _dir) = setup(8);

  // Write data
  for i in 0..1000 {
    wal
      .append(format!("key_{}", i).as_bytes(), b"data")
      .unwrap();
  }

  let wal = Arc::new(wal);
  let mut handles = vec![];

  // Spawn 20 reader threads
  for _ in 0..20 {
    let wal_clone = wal.clone();
    handles.push(thread::spawn(move || {
      let mut total_read = 0;
      for shard_id in 0..8 {
        let mut iter = wal_clone.iter_shard(shard_id, 0).unwrap();
        while iter.next().is_some() {
          total_read += 1;
        }
      }
      total_read
    }));
  }

  for h in handles {
    let count = h.join().unwrap();
    assert_eq!(count, 1000, "Each reader should see all 1000 entries");
  }
}
