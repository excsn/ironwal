//! Checkpoint log compaction tests

use ironwal::WalOptions;
use ironwal::sharded::ShardedWal;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

fn setup(shard_count: u16) -> (ShardedWal, TempDir) {
  let dir = TempDir::new().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  let wal = ShardedWal::new(opts, shard_count).unwrap();
  (wal, dir)
}

#[test]
fn test_compaction_persists_across_restart() {
  let dir = TempDir::new().unwrap();
  let root = dir.path().to_path_buf();

  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    let wal = ShardedWal::new(opts, 2).unwrap();

    for i in 0..10 {
      wal
        .create_checkpoint(format!("ckpt_{:02}", i).as_bytes())
        .unwrap();
      // Timestamps are nanosecond-resolution; keep them strictly ordered.
      std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let stats = wal.compact_checkpoints(3).unwrap();
    assert_eq!(stats.checkpoints_before, 10);
    assert_eq!(stats.checkpoints_after, 3);
  }

  // The compacted log must rebuild correctly on restart.
  let mut opts = WalOptions::default();
  opts.root_path = root.clone();
  let wal = ShardedWal::new(opts, 2).unwrap();

  assert_eq!(wal.checkpoint_count(), 3);
  let (latest, _) = wal.load_latest_checkpoint().unwrap();
  assert_eq!(latest, b"ckpt_09");
  for (id, _) in wal.list_checkpoints() {
    wal.load_checkpoint(&id).unwrap();
  }
}

#[test]
fn test_create_during_compaction_is_never_lost() {
  // Regression test: `compact_checkpoints` used to release the log lock
  // between renaming the new log and swapping in the new writer/index. A
  // `create_checkpoint` in that window appended to the unlinked old file and
  // was silently lost, leaving HEAD pointing at an unloadable checkpoint.
  let (wal, _dir) = setup(2);
  let wal = Arc::new(wal);

  for i in 0..50 {
    wal
      .create_checkpoint(format!("seed_{:03}", i).as_bytes())
      .unwrap();
  }

  let stop = Arc::new(AtomicBool::new(false));
  let writer = {
    let wal = wal.clone();
    let stop = stop.clone();
    std::thread::spawn(move || {
      let mut last = Vec::new();
      let mut i = 0u64;
      while !stop.load(Ordering::Relaxed) {
        let id = format!("live_{:06}", i).into_bytes();
        wal.create_checkpoint(&id).unwrap();
        last = id;
        i += 1;
      }
      last
    })
  };

  for _ in 0..20 {
    wal.compact_checkpoints(10).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1));
  }

  stop.store(true, Ordering::Relaxed);
  let last_created = writer.join().unwrap();

  // HEAD must never dangle: the latest checkpoint is always loadable.
  let (latest, _) = wal.load_latest_checkpoint().unwrap();
  assert_eq!(latest, last_created);

  // Every checkpoint still in the index must be loadable at its recorded
  // offset (a stale offset into the compacted log would fail here).
  for (id, _) in wal.list_checkpoints() {
    wal
      .load_checkpoint(&id)
      .unwrap_or_else(|e| panic!("Checkpoint {:?} unloadable: {}", String::from_utf8_lossy(&id), e));
  }

  // And the state must survive a restart (index rebuilt from the log file).
  let root = _dir.path().to_path_buf();
  drop(wal);
  let mut opts = WalOptions::default();
  opts.root_path = root;
  let reopened = ShardedWal::new(opts, 2).unwrap();
  let (latest, _) = reopened.load_latest_checkpoint().unwrap();
  assert_eq!(latest, last_created);
}
