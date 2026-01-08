mod common;
use common::TestEnv;
use ironwal::{SyncMode, WalOptions};

#[test]
fn test_single_append_get() {
  let env = TestEnv::with_default();
  let stream = "stream_a";

  // Write sequential items
  let id1 = env.wal.append(stream, b"item_1").unwrap();
  let id2 = env.wal.append(stream, b"item_2").unwrap();

  // Verify sequence IDs
  assert_eq!(id1, 0);
  assert_eq!(id2, 1);

  // Read back
  let val1 = env.wal.get(stream, 0).unwrap().expect("Item 0 not found");
  let val2 = env.wal.get(stream, 1).unwrap().expect("Item 1 not found");

  assert_eq!(val1, b"item_1");
  assert_eq!(val2, b"item_2");
}

#[test]
fn test_batch_write_read() {
  let env = TestEnv::with_default();
  let stream = "stream_b";

  // Write a batch of 3
  let batch: Vec<&[u8]> = vec![b"b_1", b"b_2", b"b_3"];
  let range = env.wal.append_batch(stream, &batch).unwrap();

  assert_eq!(range, 0..3);

  // Random access check
  let val = env.wal.get(stream, 1).unwrap().unwrap();
  assert_eq!(val, b"b_2");

  // Append single after batch
  let id = env.wal.append(stream, b"b_4").unwrap();
  assert_eq!(id, 3);
}

#[test]
fn test_txn_writer() {
  let env = TestEnv::with_default();
  let stream = "stream_txn";

  let mut writer = env.wal.writer(stream);
  writer.add(b"t1");
  writer.add(b"t2");

  // Commit should flush to WAL
  let range = writer.commit().unwrap();
  assert_eq!(range, 0..2);

  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"t1");
  assert_eq!(env.wal.get(stream, 1).unwrap().unwrap(), b"t2");
}

#[test]
fn test_restart_consistency() {
  // Create a persistent directory path outside the TestEnv scope
  let dir = tempfile::tempdir().unwrap();
  let root = dir.path().to_path_buf();

  // Phase 1: Write data and drop WAL
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.sync_mode = SyncMode::Strict; // Ensure flush to disk

    let wal = ironwal::Wal::new(opts).unwrap();
    wal.append("s1", b"persistent_data").unwrap();
    // wal dropped here
  }

  // Phase 2: Open new WAL instance on same directory
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;

    let wal = ironwal::Wal::new(opts).unwrap();

    // Verify data exists
    let val = wal.get("s1", 0).unwrap().expect("Data lost on restart");
    assert_eq!(val, b"persistent_data");

    // Verify sequence ID continued correctly
    let id = wal.append("s1", b"new_data").unwrap();
    assert_eq!(id, 1);
  }
}

#[test]
fn test_multiple_streams_isolation() {
  let env = TestEnv::with_default();

  env.wal.append("stream_A", b"A1").unwrap();
  env.wal.append("stream_B", b"B1").unwrap();

  // Sequence IDs should be independent
  let id_a2 = env.wal.append("stream_A", b"A2").unwrap();
  let id_b2 = env.wal.append("stream_B", b"B2").unwrap();

  assert_eq!(id_a2, 1);
  assert_eq!(id_b2, 1);

  // Data check
  assert_eq!(env.wal.get("stream_A", 1).unwrap().unwrap(), b"A2");
  assert_eq!(env.wal.get("stream_B", 0).unwrap().unwrap(), b"B1");
}
