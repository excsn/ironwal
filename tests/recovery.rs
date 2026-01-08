mod common;
use common::TestEnv;
use ironwal::{Error, SyncMode, WalOptions};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

#[test]
fn test_partial_write_recovery() {
  // 1. Setup a WAL with Strict sync to ensure data hits the disk before we sabotage it.
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  let env = TestEnv::new(opts);
  let stream = "power_plug";

  // 2. Write valid data (ID 0)
  env.wal.append(stream, b"valid_data").unwrap();

  // 3. Write the "victim" data (ID 1)
  env.wal.append(stream, b"victim_data").unwrap();

  // 4. "Pull the Plug"
  // Destructure env to drop the WAL (releasing locks) but KEEP the directory alive.
  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  // 5. Sabotage: Manually corrupt the file on disk.
  let stream_dir = root.join(stream);
  let segment_path = stream_dir.join("00000000000000000000.wal");

  let file = OpenOptions::new()
    .write(true)
    .open(&segment_path)
    .expect("Failed to open segment");
  let len = file.metadata().unwrap().len();

  // We chop off 10 bytes from the end.
  file.set_len(len - 10).expect("Failed to truncate file");
  drop(file);

  // 6. Restart the WAL (Recovery Mode)
  let wal = ironwal::Wal::new(WalOptions::new(&root)).expect("Failed to restart WAL");

  // 7. Verify Assertions

  // The first record must be intact.
  let rec0 = wal.get(stream, 0).unwrap();
  assert_eq!(
    rec0,
    Some(b"valid_data".to_vec()),
    "Data before corruption point was lost"
  );

  // The second record (victim) should be gone.
  let rec1 = wal.get(stream, 1).unwrap();
  assert!(
    rec1.is_none(),
    "Corrupted data was returned! {:?}. Should be None.",
    rec1
  );

  // 8. Verify the WAL is still writable
  let new_id = wal
    .append(stream, b"new_life")
    .expect("WAL is not writable after recovery");

  let rec_new = wal.get(stream, new_id).unwrap();
  assert_eq!(rec_new, Some(b"new_life".to_vec()));
}

#[test]
fn test_partial_header_recovery() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  let env = TestEnv::new(opts);
  let stream = "partial_header";

  env.wal.append(stream, b"item1").unwrap(); // ID 0
  env.wal.append(stream, b"item2").unwrap(); // ID 1

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);
  let segment_path = stream_dir.join("00000000000000000000.wal");

  let file = OpenOptions::new().write(true).open(&segment_path).unwrap();
  let len = file.metadata().unwrap().len();

  // Calculate size of last record: 32 (Header) + 5 (Payload) = 37 bytes.
  // We want to leave a partial header for the second record.
  // So we truncate to: (Total - 37) + 10 bytes of header.
  let len_item1 = 32 + 5;
  file.set_len(len_item1 + 10).unwrap();
  drop(file);

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Item 1 should be there
  assert!(wal.get(stream, 0).unwrap().is_some());
  // Item 2 should be gone (invalid header)
  assert!(wal.get(stream, 1).unwrap().is_none());

  // Append should work
  let id = wal.append(stream, b"item3").unwrap();
  // Since item 2 was effectively erased, the next ID might technically be 1 or 2
  // depending on if state was persisted.
  // In our current logic, `recover_scan` recalculates NextID based on valid frames.
  // So we expect next_id to reset to 1.
  assert_eq!(id, 1);
  assert_eq!(wal.get(stream, 1).unwrap(), Some(b"item3".to_vec()));
}

#[test]
fn test_zero_pad_recovery() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  let env = TestEnv::new(opts);
  let stream = "zero_pad";

  env.wal.append(stream, b"A").unwrap();

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);
  let segment_path = stream_dir.join("00000000000000000000.wal");

  let mut file = OpenOptions::new().write(true).append(true).open(&segment_path).unwrap();
  // Append 1KB of zeros (simulating pre-allocation or sparse file garbage)
  file.write_all(&[0u8; 1024]).unwrap();
  drop(file);

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Should read A
  assert_eq!(wal.get(stream, 0).unwrap(), Some(b"A".to_vec()));

  // Should identify zeros as invalid and truncate them, allowing write.
  let id = wal.append(stream, b"B").unwrap();
  assert_eq!(id, 1);
  assert_eq!(wal.get(stream, 1).unwrap(), Some(b"B".to_vec()));
}

#[test]
fn test_bit_rot_detection() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  // Ensure we rely on disk read, not cache
  opts.block_cache_size = None;
  let env = TestEnv::new(opts);
  let stream = "bit_rot";

  env.wal.append(stream, b"clean_data").unwrap();

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);
  let segment_path = stream_dir.join("00000000000000000000.wal");

  let mut file = OpenOptions::new().write(true).read(true).open(&segment_path).unwrap();

  // Seek past header (32 bytes) into payload
  file.seek(SeekFrom::Start(35)).unwrap();
  // Corrupt one byte
  file.write_all(&[0xFF]).unwrap();
  drop(file);

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Attempt read
  let result = wal.get(stream, 0);

  // Expect CRC Error
  match result {
    Err(Error::CrcMismatch { .. }) => (), // Pass
    Err(e) => panic!("Expected CrcMismatch, got {:?}", e),
    Ok(_) => panic!("Read succeeded despite bit rot!"),
  }
}

#[test]
fn test_zombie_index_recovery() {
  let mut opts = WalOptions::default();
  // Rotate every 2 items
  opts.max_entries_per_segment = 2;
  opts.max_segment_size = u64::MAX;
  let env = TestEnv::new(opts);
  let stream = "zombie_idx";

  // Create 3 segments:
  // Seg 0: IDs 0, 1
  // Seg 2: IDs 2, 3
  // Seg 4: IDs 4, 5
  for _ in 0..6 {
    env.wal.append(stream, &[0u8; 10]).unwrap();
  }

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);
  let idx_path = stream_dir.join("segments.idx");

  // SABOTAGE: Overwrite index to ONLY know about Segment 0.
  // We simulate a case where the index file wasn't flushed, but segments 2 and 4 exist on disk.
  let mut file = fs::File::create(&idx_path).unwrap();
  use byteorder::{LittleEndian, WriteBytesExt};
  file.write_u64::<LittleEndian>(0).unwrap();
  file.sync_all().unwrap();
  drop(file);

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Read from Segment 0 (Should work)
  assert!(wal.get(stream, 0).unwrap().is_some());

  // Read from Segment 4 (The active one, tracked by head.state usually)
  // Even if index is stale, recover_stream logic often trusts head.state for the active head.
  assert!(wal.get(stream, 4).unwrap().is_some());

  // Read from Segment 2 (The middle child)
  // This is the true test. If the index is canonical and stale, this will be None.
  // If the WAL is robust, it might self-heal?
  // *Current Implementation Expectation*: IronWal assumes index is truth if present.
  // This might fail or return None.
  // If it returns None, it implies we are NOT resilient to index desync.
  let res = wal.get(stream, 2).unwrap();

  // Note: If this fails, we know we need to implement "Index Reconstruction"
  assert!(res.is_some(), "Failed to read middle segment after index corruption");
}

#[test]
fn test_malformed_index_rebuild() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  let env = TestEnv::new(opts);
  let stream = "malformed_idx";

  // Create data to ensure a non-trivial index
  env.wal.append(stream, b"A").unwrap(); // Seg 0

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);
  let idx_path = stream_dir.join("segments.idx");

  // SABOTAGE 1: Truncate index to invalid alignment (3 bytes)
  let mut file = fs::OpenOptions::new().write(true).open(&idx_path).unwrap();
  file.set_len(3).unwrap();
  drop(file);

  // Restart 1: Should detect alignment error and rebuild
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();
  assert_eq!(wal.get(stream, 0).unwrap(), Some(b"A".to_vec()));

  // Close again
  drop(wal);

  // SABOTAGE 2: Valid alignment, but unsorted garbage
  let mut file = fs::OpenOptions::new()
    .write(true)
    .truncate(true)
    .open(&idx_path)
    .unwrap();
  use byteorder::{LittleEndian, WriteBytesExt};
  // Write [0, 100, 50] - Unsorted!
  file.write_u64::<LittleEndian>(0).unwrap();
  file.write_u64::<LittleEndian>(100).unwrap();
  file.write_u64::<LittleEndian>(50).unwrap();
  drop(file);

  // Restart 2:
  // NOTE: Currently ironwal checks alignment, but DOES IT check sort order?
  // If not, this might cause binary search weirdness.
  // A truly robust WAL should validate the index is sorted on load.
  // If this fails/panics, we know we need to add a sort-check validator.
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // If the index was accepted as-is, find_segment(0) might work or fail randomly.
  // If rebuilt, it will be just [0].
  assert_eq!(wal.get(stream, 0).unwrap(), Some(b"A".to_vec()));
}

#[test]
fn test_segment_gap_handling() {
  let mut opts = WalOptions::default();
  opts.max_entries_per_segment = 2; // Force small segments
  opts.max_segment_size = u64::MAX;
  let env = TestEnv::new(opts);
  let stream = "segment_gap";

  // Create segments: 0.wal (IDs 0,1), 2.wal (IDs 2,3), 4.wal (IDs 4,5)
  for _ in 0..6 {
    env.wal.append(stream, &[0u8]).unwrap();
  }

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);

  // SABOTAGE: Delete the middle segment (2.wal)
  // Index still thinks it exists.
  let seg2 = stream_dir.join("00000000000000000002.wal");
  fs::remove_file(&seg2).unwrap();

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Read Seg 0: Should work
  assert!(wal.get(stream, 0).unwrap().is_some());

  // Read Seg 4: Should work
  assert!(wal.get(stream, 4).unwrap().is_some());

  // Read Seg 2: Should NOT crash. Should return None.
  // The index says "Go to 2.wal". 2.wal is missing.
  // The SegmentReader::open should fail.
  // Wal::get should catch that and return None (or handle it).
  let res = wal.get(stream, 2);

  match res {
    Ok(None) => {} // Ideal: treated as missing data
    Ok(Some(_)) => panic!("Phantom data read!"),
    Err(e) => {
      // Acceptable: Returning an error saying "Segment Missing" is also valid
      // provided it's not a panic.
      println!("Got expected error for missing segment: {}", e);
    }
  }
}

#[test]
fn test_empty_segment_recovery() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::Strict;
  let env = TestEnv::new(opts);
  let stream = "empty_seg";

  // Create Seg 0
  env.wal.append(stream, b"A").unwrap();

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);

  // SABOTAGE: Manually create the NEXT segment (Seg 1) as a 0-byte file
  // This simulates a crash immediately after file creation.
  let seg1 = stream_dir.join("00000000000000000001.wal");
  fs::File::create(&seg1).unwrap(); // Creates empty file

  // Restart
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // 1. Should read old data
  assert_eq!(wal.get(stream, 0).unwrap(), Some(b"A".to_vec()));

  // 2. Should identify Seg 1 is the new active head (or handle the 0-byte file gracefully)
  // Logic check: recovering the stream might see Seg 1 in directory, add to index.
  // Then `recover_stream` checks it.

  // Write new data. It should go into ID 1.
  let id = wal.append(stream, b"B").unwrap();
  assert_eq!(id, 1);
  assert_eq!(wal.get(stream, 1).unwrap(), Some(b"B".to_vec()));
}

#[test]
fn test_missing_head_state_recovery() {
  let mut opts = WalOptions::default();
  opts.max_entries_per_segment = 2; // Force rotation
  opts.max_segment_size = u64::MAX;
  let env = TestEnv::new(opts);
  let stream = "headless";

  // Write 4 items. Segments: 0.wal (0,1), 2.wal (2,3) -> Active
  for _ in 0..4 {
    env.wal.append(stream, b"data").unwrap();
  }

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let stream_dir = root.join(stream);

  // SABOTAGE: Delete head.state
  // The WAL now doesn't know 2.wal is the active one.
  let head_path = stream_dir.join("head.state");
  fs::remove_file(&head_path).unwrap();

  // Restart
  // It should scan the directory/index, find 2.wal has the highest ID, and use it.
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // Verify we can read the latest data
  assert!(wal.get(stream, 3).unwrap().is_some());

  // Verify appending works and preserves sequence
  let id = wal.append(stream, b"new").unwrap();
  assert_eq!(id, 4);
}

#[test]
fn test_repair_on_restart_preserves_data() {
  let mut opts = WalOptions::default();
  opts.sync_mode = SyncMode::BatchOnly;
  let env = TestEnv::new(opts);
  let stream = "repair_verify";

  // 1. Write data
  env.wal.append(stream, &[]).unwrap(); // ID 0

  // 2. Restart
  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  // 3. Re-open
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();

  // 4. Trigger Repair by writing NEW data
  // This is the critical step. Opening the file for write triggers the repair logic.
  let id = wal.append(stream, b"next").unwrap();
  assert_eq!(id, 1);

  // 5. Verify OLD data still exists
  // If repair() fails to read the file due to permissions, it will truncate it to 0,
  // deleting this record.
  let val = wal.get(stream, 0).unwrap();
  assert_eq!(val, Some(vec![]), "Old data lost after restart + append");

  // 6. Verify NEW data exists
  assert_eq!(wal.get(stream, 1).unwrap(), Some(b"next".to_vec()));
}
