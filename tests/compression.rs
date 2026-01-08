mod common;
use common::TestEnv;
use ironwal::{CompressionType, WalOptions};
use std::fs;

fn segment_path(root: &std::path::Path, stream: &str) -> std::path::PathBuf {
  root.join(stream).join("00000000000000000000.wal")
}

#[test]
fn test_lz4_compression_effectiveness() {
  // Scenario: Write highly compressible data and verify disk usage is small.
  let mut opts = WalOptions::default();
  opts.compression = CompressionType::Lz4;
  opts.min_compression_size = 0; // Force compression for everything
  opts.max_segment_size = u64::MAX;

  let env = TestEnv::new(opts);
  let stream = "effective_comp";

  // Create a 10KB payload of repeated bytes (highly compressible)
  let payload = vec![b'A'; 10_000];

  env.wal.append(stream, &payload).unwrap();

  // Force flush by dropping WAL (or we could use SyncMode::Strict)
  // Destructure env to drop WAL but keep _dir alive so files aren't deleted
  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  // Check file size
  let path = segment_path(&root, stream);
  let file_size = fs::metadata(&path).unwrap().len();

  // 10KB payload + 32 byte header.
  // LZ4 should compress 10,000 'A's to ~50 bytes.
  // The file should be drastically smaller than the raw payload.
  assert!(
    file_size < 1000,
    "Compression failed! File size: {} bytes (Expected < 1000)",
    file_size
  );

  // Verify Data Integrity on Read
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();
  let read_back = wal.get(stream, 0).unwrap().unwrap();
  assert_eq!(read_back, payload, "Decompressed data mismatch");
}

#[test]
fn test_compression_threshold_skip() {
  // Scenario: Write a small payload that is BELOW the min_compression_size.
  // It should remain Raw (uncompressed).
  let mut opts = WalOptions::default();
  opts.compression = CompressionType::Lz4;
  opts.min_compression_size = 1000; // Threshold set high
  opts.max_segment_size = u64::MAX;

  let env = TestEnv::new(opts);
  let stream = "skip_comp";

  // 100 bytes is < 1000, so it should NOT be compressed.
  let payload = vec![b'B'; 100];
  env.wal.append(stream, &payload).unwrap();

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let path = segment_path(&root, stream);
  let file_size = fs::metadata(&path).unwrap().len();

  // Header (32) + Batch Length Prefix (4) + Payload (100) = 136 bytes.
  // If it WAS compressed, it would likely be different (LZ4 frame overhead).
  // Since we enforced Raw via threshold, it must be exactly the raw serialization size.
  assert_eq!(
    file_size, 136,
    "Threshold ignored! File compressed when it shouldn't have been."
  );

  // Verify Read
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();
  let read_back = wal.get(stream, 0).unwrap().unwrap();
  assert_eq!(read_back, payload);
}

#[test]
fn test_large_batch_compression() {
  // Scenario: Write a large batch of heterogeneous data.
  let mut opts = WalOptions::default();
  opts.compression = CompressionType::Lz4;
  opts.min_compression_size = 0;

  let env = TestEnv::new(opts);
  let stream = "batch_comp";

  // Create 100 items, each 1KB of zeros (highly compressible)
  let item = vec![0u8; 1024];
  let batch: Vec<&[u8]> = (0..100).map(|_| item.as_slice()).collect();

  // Raw size = 100 * 1KB = 100KB.
  env.wal.append_batch(stream, &batch).unwrap();

  let common::TestEnv { wal, _dir, root } = env;
  drop(wal);

  let path = segment_path(&root, stream);
  let file_size = fs::metadata(&path).unwrap().len();

  // 100KB of zeros should compress to almost nothing + headers.
  // Even with framing overhead, it should be well under 10KB.
  assert!(
    file_size < 10_000,
    "Batch compression ineffective. Size: {}",
    file_size
  );

  // Verify Read Iteration
  let wal = ironwal::Wal::new(WalOptions::new(&root)).unwrap();
  let mut iter = wal.iter(stream, 0).unwrap();
  let mut count = 0;
  while let Some(res) = iter.next() {
    assert_eq!(res.unwrap(), item);
    count += 1;
  }
  assert_eq!(count, 100);
}

#[test]
fn test_mixed_mode_read() {
  // Scenario: Can we read a stream that has both Compressed and Raw frames?
  // This verifies the reader checks the frame header type byte, not global config.

  let dir = tempfile::tempdir().unwrap();
  let root = dir.path().to_path_buf();
  let stream = "mixed_mode";

  // 1. Write Compressed Entry
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.compression = CompressionType::Lz4;
    opts.min_compression_size = 0; // Force compress
    let wal = ironwal::Wal::new(opts).unwrap();
    wal.append(stream, b"compressed_data").unwrap(); // ID 0
  }

  // 2. Write Raw Entry (Change options, same stream)
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.compression = CompressionType::None; // Force Raw
    let wal = ironwal::Wal::new(opts).unwrap();
    wal.append(stream, b"raw_data").unwrap(); // ID 1
  }

  // 3. Read Back
  {
    let opts = WalOptions::new(&root);
    let wal = ironwal::Wal::new(opts).unwrap();

    let val0 = wal.get(stream, 0).unwrap().unwrap();
    assert_eq!(val0, b"compressed_data");

    let val1 = wal.get(stream, 1).unwrap().unwrap();
    assert_eq!(val1, b"raw_data");
  }
}
