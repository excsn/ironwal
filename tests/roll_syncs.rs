mod common;
use common::TestEnv;
use ironwal::{SyncMode, Wal, WalOptions};
use std::fs;

fn rolling(sync_mode: SyncMode) -> WalOptions {
  WalOptions::default().sync_mode(sync_mode).max_segment_size(256).max_entries_per_segment(u64::MAX)
}

fn segments(env: &TestEnv, stream: &str) -> usize {
  fs::read_dir(env.root.join(stream)).unwrap().flatten().filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal")).count()
}

#[test]
fn syncs_counts_the_index_rewrite_a_truncate_makes() {
  let env = TestEnv::new(rolling(SyncMode::Async));
  let entry = vec![1u8; 100];
  for _ in 0..10 {
    env.wal.append_batch("trimmed", &[&entry]).unwrap();
  }
  assert!(env.wal.truncate("trimmed", 8).unwrap() > 0);
  assert!(env.wal.syncs() > 0, "the rewritten segments.idx is synced");
}

#[test]
fn async_mode_issues_no_fsync_across_segment_rolls() {
  let env = TestEnv::new(rolling(SyncMode::Async));
  let entry = vec![2u8; 100];
  for _ in 0..40 {
    env.wal.append_batch("relaxed", &[&entry]).unwrap();
  }
  assert!(segments(&env, "relaxed") >= 10);
  assert_eq!(env.wal.syncs(), 0);
}

#[test]
fn a_roll_in_batch_only_mode_costs_one_directory_fsync_beside_the_segment_syncs() {
  let env = TestEnv::new(rolling(SyncMode::BatchOnly));
  let entry = vec![3u8; 100];
  env.wal.append_batch("synced", &[&entry]).unwrap();
  let (created, syncs_before) = (segments(&env, "synced") as u64, env.wal.syncs());
  let batches = 40u64;
  for _ in 0..batches {
    env.wal.append_batch("synced", &[&entry]).unwrap();
  }
  let rolls = segments(&env, "synced") as u64 - created;
  assert!(rolls >= 10);
  assert_eq!(env.wal.syncs() - syncs_before, batches + 2 * rolls, "each batch syncs its segment and each roll syncs the segment it left and the stream directory");
}

#[test]
fn a_segment_missing_from_the_index_is_found_on_reopen() {
  let dir = tempfile::tempdir().unwrap();
  let options = rolling(SyncMode::Async).root_path(dir.path().to_path_buf());
  let entry = vec![4u8; 100];
  {
    let wal = Wal::new(options.clone()).unwrap();
    for _ in 0..6 {
      wal.append_batch("lost", &[&entry]).unwrap();
    }
  }
  let stream = dir.path().join("lost");
  let index = stream.join("segments.idx");
  let bytes = fs::read(&index).unwrap();
  assert!(bytes.len() >= 16);
  fs::write(&index, &bytes[..bytes.len() - 8]).unwrap();
  fs::remove_file(stream.join("head.state")).unwrap();

  let wal = Wal::new(options).unwrap();
  assert_eq!(wal.iter("lost", 0).unwrap().count(), 6);
  assert_eq!(wal.append("lost", &entry).unwrap(), 6);
  assert_eq!(wal.iter("lost", 0).unwrap().count(), 7);
}

#[test]
fn an_unreadable_head_state_is_ignored_on_reopen() {
  let dir = tempfile::tempdir().unwrap();
  let options = rolling(SyncMode::Async).root_path(dir.path().to_path_buf());
  let entry = vec![5u8; 100];
  {
    let wal = Wal::new(options.clone()).unwrap();
    for _ in 0..6 {
      wal.append_batch("torn", &[&entry]).unwrap();
    }
  }
  fs::write(dir.path().join("torn").join("head.state"), b"").unwrap();

  let wal = Wal::new(options).unwrap();
  assert_eq!(wal.iter("torn", 0).unwrap().count(), 6);
  assert_eq!(wal.append("torn", &entry).unwrap(), 6);
}
