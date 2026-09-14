mod common;
use common::TestEnv;
use ironwal::WalOptions;

fn segment_filename(id: u64) -> String {
  format!("{:020}.wal", id)
}

#[test]
fn test_basic_truncation() {
  let mut opts = WalOptions::default();
  // STRATEGY: Rotate based on COUNT, not size.
  // This ensures deterministic boundaries regardless of header overhead.
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;

  let env = TestEnv::new(opts);
  let stream = "truncate_basic";

  // Create 3 segments:
  // Seg 0:  IDs 0..9   (10 items)
  // Seg 10: IDs 10..19 (10 items)
  // Seg 20: IDs 20..29 (10 items) -> Active
  for _ in 0..30 {
    env.wal.append(stream, &[0u8; 100]).unwrap();
  }

  let stream_dir = env.root.join(stream);

  // Verify pre-conditions
  assert!(stream_dir.join(segment_filename(0)).exists());
  assert!(stream_dir.join(segment_filename(10)).exists());
  assert!(stream_dir.join(segment_filename(20)).exists());

  // Truncate(15):
  // - Record 15 lives in Seg 10 (starts at 10).
  // - We must KEEP Seg 10.
  // - We delete everything BEFORE Seg 10 (i.e., Seg 0).
  let deleted = env.wal.truncate(stream, 15).unwrap();

  // Expectation: Delete exactly 1 segment (Seg 0).
  assert_eq!(deleted, 1, "Should delete exactly 1 segment (Start ID 0)");

  // Verify file 00...00.wal is gone
  let seg0 = stream_dir.join(segment_filename(0));
  assert!(!seg0.exists(), "Segment 0 should be deleted");

  // Verify file 00...10.wal exists
  let seg10 = stream_dir.join(segment_filename(10));
  assert!(seg10.exists(), "Segment 10 must exist");

  // Verify we can still read from Seg 10
  let val = env.wal.get(stream, 10).unwrap();
  assert!(val.is_some(), "Should be able to read preserved data");

  // Verify we CANNOT read from Seg 0 (it's gone)
  let val_old = env.wal.get(stream, 0).unwrap();
  assert!(val_old.is_none(), "Deleted data should be inaccessible");
}

#[test]
fn test_truncate_active_safety() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;

  let env = TestEnv::new(opts);
  let stream = "truncate_safety";

  // Create 2 segments:
  // Seg 0: IDs 0..9
  // Seg 10: IDs 10..14 (Active)
  for _ in 0..15 {
    env.wal.append(stream, &[0u8; 100]).unwrap();
  }

  // Truncate up to ID 100 (Way past head).
  let deleted = env.wal.truncate(stream, 100).unwrap();

  assert_eq!(deleted, 1, "Should delete old segment but preserve active one");

  // Verify we can still write
  env.wal.append(stream, b"new_data").unwrap();
}

#[test]
fn test_archived_segments_restore_the_history() {
  let archive = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;
  opts.archive_path = Some(archive.path().to_path_buf());
  let env = TestEnv::new(opts.clone());
  let stream = "truncate_archive";

  for i in 0..30u8 {
    env.wal.append(stream, &[i]).unwrap();
  }
  assert_eq!(env.wal.truncate(stream, 25).unwrap(), 2, "segments 0 and 10 are freed");

  let stream_dir = env.root.join(stream);
  let archived = archive.path().join(stream);
  assert!(!stream_dir.join(segment_filename(0)).exists());
  assert!(!stream_dir.join(segment_filename(10)).exists());
  assert!(archived.join(segment_filename(0)).exists(), "freed segments moved to the archive");
  assert!(archived.join(segment_filename(10)).exists());
  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 0).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (20..30).collect::<Vec<_>>(), "the stream reads as truncated");
  assert!(env.wal.get(stream, 5).unwrap().is_none());

  assert_eq!(env.wal.restore_archived(stream).unwrap(), 2);
  assert!(!archived.join(segment_filename(0)).exists(), "the archive is emptied");
  assert!(stream_dir.join(segment_filename(0)).exists());
  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 0).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (0..30).collect::<Vec<_>>(), "the whole history reads again");
  assert_eq!(env.wal.get(stream, 5).unwrap().unwrap(), vec![5u8]);
  assert_eq!(env.wal.restore_archived(stream).unwrap(), 0, "nothing left to restore");

  // the restored history survives a reopen; appends continue past the active segment
  let id = env.wal.append(stream, &[30]).unwrap();
  assert_eq!(id, 30);
  let common::TestEnv { wal: old_wal, _dir, root } = env;
  drop(old_wal);
  let mut reopen = opts;
  reopen.root_path = root;
  let wal = ironwal::Wal::new(reopen).unwrap();
  let ids: Vec<u64> = wal.iter_with_ids(stream, 0).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (0..31).collect::<Vec<_>>());

  // and a second truncation archives again
  assert_eq!(wal.truncate(stream, 15).unwrap(), 1);
  assert!(archived.join(segment_filename(0)).exists());
}

#[test]
fn test_restore_without_an_archive_is_a_no_op() {
  let env = TestEnv::with_default();
  let stream = "no_archive";
  env.wal.append(stream, b"x").unwrap();
  assert_eq!(env.wal.restore_archived(stream).unwrap(), 0);
  assert_eq!(env.wal.get(stream, 0).unwrap().unwrap(), b"x");
}

#[test]
fn test_index_rebuild_after_truncate() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;

  let env = TestEnv::new(opts);
  let stream = "truncate_index";

  for _ in 0..30 {
    env.wal.append(stream, &[0u8; 100]).unwrap();
  }

  // Truncate
  env.wal.truncate(stream, 15).unwrap();

  // RESTART the WAL to verify index on disk is correct

  // Destructure env to drop the old WAL (releasing locks) BUT keep _dir alive.
  // If we just `drop(env)`, the TempDir is dropped and the files are deleted.
  let common::TestEnv {
    wal: old_wal,
    _dir,
    root,
  } = env;
  drop(old_wal);

  let wal = ironwal::Wal::new(ironwal::WalOptions::new(root)).unwrap();

  // We kept Seg 10 (ID 10..19) and Seg 20 (ID 20..29)
  // We should be able to read ID 20.
  let val = wal.get(stream, 20).unwrap();
  assert!(val.is_some());
}
