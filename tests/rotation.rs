mod common;
use common::TestEnv;
use ironwal::{Wal, WalOptions};
use std::fs;

#[test]
fn test_segment_rotation_threshold() {
  let mut opts = WalOptions::default();
  // Set a tiny segment size limit to force frequent rotation.
  opts.max_segment_size = 1024;
  opts.max_entries_per_segment = u64::MAX; // Disable count limit

  let env = TestEnv::new(opts);
  let stream = "rotator";

  // Initial state
  let initial_seg = env.wal.current_segment_start_id(stream).unwrap_or(0);
  assert_eq!(initial_seg, 0);

  // Write until rotation is observed
  let payload = vec![0u8; 100];
  let mut rotated = false;
  let mut entries_written = 0;

  // We expect rotation within ~8 writes (1024 / 132 ~= 7.7).
  // Limit loop to 20 to prevent infinite loop on failure.
  for _ in 0..20 {
    env.wal.append(stream, &payload).unwrap();
    entries_written += 1;

    let current_seg = env.wal.current_segment_start_id(stream).unwrap();
    if current_seg > 0 {
      rotated = true;
      break;
    }
  }

  assert!(rotated, "WAL did not rotate segment despite exceeding size limit");

  // Verify we didn't rotate *too* early (e.g., after 1 write)
  assert!(entries_written > 2, "rotated too early: {}", entries_written);

  // Verify data integrity across segments
  // We read the last entry of the *previous* segment.
  // If we wrote `entries_written` items, IDs are 0 to `entries_written - 1`.
  // The rotation happened on the last write.
  let last_val = env
    .wal
    .get(stream, entries_written - 2)
    .unwrap()
    .expect("Missing pre-rotation entry");
  assert_eq!(last_val, payload);
}

#[test]
fn test_index_rebuild_on_missing_file() {
  let dir = tempfile::tempdir().unwrap();
  let root = dir.path().to_path_buf();
  let stream = "rebuild_test";

  // Phase 1: Create WAL, force rotation
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.max_segment_size = 500; // Tiny

    let wal = Wal::new(opts).unwrap();

    // Write enough to create at least 2 segments (0.wal and X.wal)
    wal.append(stream, &[1u8; 200]).unwrap(); // ID 0
    wal.append(stream, &[2u8; 200]).unwrap(); // ID 1
    wal.append(stream, &[3u8; 200]).unwrap(); // ID 2 -> Likely new segment
  }

  // Phase 2: Sabotage! Delete segments.idx
  let stream_dir = root.join(stream);
  let idx_path = stream_dir.join("segments.idx");
  assert!(idx_path.exists());
  fs::remove_file(&idx_path).unwrap();

  // Phase 3: Restart WAL and Verify Recovery
  {
    let mut opts = WalOptions::default();
    opts.root_path = root;

    // This constructor calls `recover_stream`, which detects missing index and calls `rebuild_index_from_scan`
    let wal = Wal::new(opts).unwrap();

    // Check if index was recreated
    assert!(idx_path.exists(), "segments.idx was not rebuilt");

    // Verify we can read data from BOTH segments (proving index is valid)
    let val0 = wal.get(stream, 0).unwrap().expect("Lost entry 0");
    assert_eq!(val0, vec![1u8; 200]);

    let val2 = wal.get(stream, 2).unwrap().expect("Lost entry 2");
    assert_eq!(val2, vec![3u8; 200]);

    // Verify we can write new data
    let id = wal.append(stream, b"new").unwrap();
    assert_eq!(id, 3);
  }
}
