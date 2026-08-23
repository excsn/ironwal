mod common;
use common::TestEnv;
use ironwal::WalOptions;

fn segment_filename(id: u64) -> String {
  format!("{:020}.wal", id)
}

#[test]
fn test_delete_stream_basic() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;

  let env = TestEnv::new(opts);
  let stream = "delete_basic";

  for i in 0..30u8 {
    env.wal.append(stream, &[i; 100]).unwrap();
  }

  let stream_dir = env.root.join(stream);
  assert!(stream_dir.join(segment_filename(0)).exists());
  assert!(stream_dir.join(segment_filename(20)).exists());

  env.wal.delete_stream(stream).unwrap();

  assert!(!stream_dir.exists(), "Stream directory should be gone");
  assert!(env.wal.get(stream, 0).unwrap().is_none());
  assert!(env.wal.get(stream, 25).unwrap().is_none());
  assert!(env.wal.iter(stream, 0).unwrap().next().is_none());

  // Recreation starts from ID 0.
  let id = env.wal.append(stream, b"reborn").unwrap();
  assert_eq!(id, 0);
  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"reborn");
  assert!(stream_dir.join(segment_filename(0)).exists());
}

#[test]
fn test_delete_stream_nonexistent() {
  let env = TestEnv::with_default();
  env.wal.delete_stream("never_written").unwrap();
  env.wal.delete_stream("never_written").unwrap();
}

#[test]
fn test_delete_stream_idempotent() {
  let env = TestEnv::with_default();
  let stream = "delete_twice";

  env.wal.append(stream, b"data").unwrap();
  env.wal.delete_stream(stream).unwrap();
  env.wal.delete_stream(stream).unwrap();

  assert_eq!(env.wal.append(stream, b"again").unwrap(), 0);
}

#[test]
fn test_delete_stream_other_streams_untouched() {
  let env = TestEnv::with_default();

  env.wal.append("victim", b"gone").unwrap();
  env.wal.append("survivor", b"kept").unwrap();

  env.wal.delete_stream("victim").unwrap();

  assert!(env.root.join("survivor").exists());
  assert_eq!(env.wal.get("survivor", 0).unwrap().unwrap(), b"kept");
}

#[test]
fn test_delete_stream_block_cache_not_stale() {
  let mut opts = WalOptions::default();
  opts.block_cache_size = Some(16 * 1024 * 1024);

  let env = TestEnv::new(opts);
  let stream = "delete_cache";

  env.wal.append(stream, b"old_incarnation").unwrap();
  // Populate the block cache with the frame at (segment 0, offset 0).
  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"old_incarnation");

  env.wal.delete_stream(stream).unwrap();

  // The recreated stream reuses segment 0 and offset 0; the read must not
  // come from the deleted incarnation's cached block.
  env.wal.append(stream, b"new_incarnation").unwrap();
  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"new_incarnation");
  assert_eq!(
    env.wal.get_batch(stream, &[0]).unwrap(),
    vec![Some(b"new_incarnation".to_vec())]
  );
}

#[test]
fn test_delete_stream_survives_restart() {
  let env = TestEnv::with_default();
  let stream = "delete_restart";

  for _ in 0..5 {
    env.wal.append(stream, b"data").unwrap();
  }
  env.wal.delete_stream(stream).unwrap();

  let common::TestEnv { wal: old_wal, _dir, root } = env;
  drop(old_wal);

  let wal = ironwal::Wal::new(ironwal::WalOptions::new(root.clone())).unwrap();
  assert!(wal.get(stream, 0).unwrap().is_none());
  assert_eq!(wal.append(stream, b"fresh").unwrap(), 0);
}

#[test]
fn test_delete_marker_cleanup_on_open() {
  let env = TestEnv::with_default();
  env.wal.append("live", b"data").unwrap();

  // Simulate a crash between rename and remove_dir_all.
  let marker = env.root.join("crashed#deleted");
  std::fs::create_dir_all(&marker).unwrap();
  std::fs::write(marker.join(segment_filename(0)), b"junk").unwrap();

  let common::TestEnv { wal: old_wal, _dir, root } = env;
  drop(old_wal);

  let wal = ironwal::Wal::new(ironwal::WalOptions::new(root.clone())).unwrap();

  assert!(!marker.exists(), "Marker directory should be removed on open");
  assert_eq!(wal.get("live", 0).unwrap().unwrap(), b"data");
  assert!(wal.get("crashed", 0).unwrap().is_none());
}

#[test]
fn test_delete_stream_write_descriptor_not_reused() {
  let env = TestEnv::with_default();
  let stream = "delete_fd";

  env.wal.append(stream, b"first").unwrap();
  env.wal.delete_stream(stream).unwrap();

  // A stale cached descriptor would append to the unlinked inode and this
  // read would miss.
  env.wal.append(stream, b"second").unwrap();
  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"second");

  let entries: Vec<Vec<u8>> = env
    .wal
    .iter(stream, 0)
    .unwrap()
    .collect::<ironwal::Result<_>>()
    .unwrap();
  assert_eq!(entries, vec![b"second".to_vec()]);
}
