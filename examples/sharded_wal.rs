//! Example: Using ShardedWal for a distributed key-value store
//!
//! This example demonstrates:
//! - Writing key-value pairs with automatic sharding
//! - Creating checkpoints at logical boundaries
//! - Restoring from checkpoints
//! - Pruning old history

#[cfg(feature = "sharded")]
fn main() -> ironwal::Result<()> {
  use ironwal::WalOptions;
  use ironwal::sharded::ShardedWal;

  println!("=== IronWal Sharded Example ===\n");

  // 1. Create a sharded WAL with 16 shards
  let mut opts = WalOptions::default();
  opts.root_path = "./example_sharded_wal".into();

  let wal = ShardedWal::new(opts, 16)?;
  println!("✓ Created ShardedWal with 16 shards");

  // 2. Write some key-value pairs
  println!("\n--- Writing Data ---");
  for i in 0..100 {
    let key = format!("user_{}", i);
    let value = format!("profile_data_{}", i);

    let (shard_id, seq_id) = wal.append(key.as_bytes(), value.as_bytes())?;

    if i < 3 {
      println!("  {} -> shard {}, offset {}", key, shard_id, seq_id);
    }
  }
  println!("  ... (wrote 100 entries total)");

  // 3. Create a checkpoint
  println!("\n--- Creating Checkpoint ---");
  wal.create_checkpoint(b"checkpoint_v1")?;
  println!("✓ Created checkpoint 'checkpoint_v1'");

  // 4. Write more data
  println!("\n--- Writing More Data ---");
  for i in 100..200 {
    let key = format!("user_{}", i);
    let value = format!("profile_data_{}", i);
    wal.append(key.as_bytes(), value.as_bytes())?;
  }
  println!("  ... (wrote 100 more entries)");

  // 5. Create another checkpoint
  wal.create_checkpoint(b"checkpoint_v2")?;
  println!("✓ Created checkpoint 'checkpoint_v2'");

  // 6. Load and inspect checkpoints
  println!("\n--- Loading Checkpoints ---");
  let checkpoint_v1 = wal.load_checkpoint(b"checkpoint_v1")?;
  println!("Checkpoint v1 offsets: {:?}", checkpoint_v1.offsets);

  let checkpoint_v2 = wal.load_checkpoint(b"checkpoint_v2")?;
  println!("Checkpoint v2 offsets: {:?}", checkpoint_v2.offsets);

  // 7. Iterate from checkpoint
  println!("\n--- Reading from Checkpoint v1 ---");
  let mut total_read = 0;
  for shard_id in 0..16 {
    let offset = checkpoint_v1.offsets[shard_id as usize];
    let mut iter = wal.iter_shard(shard_id, offset)?;

    while let Some(entry) = iter.next() {
      let _data = entry?;
      total_read += 1;
    }
  }
  println!(
    "  Read {} entries from checkpoint v1 to current",
    total_read
  );

  // 8. Prune old data
  println!("\n--- Pruning Old Data ---");
  let stats = wal.prune_before_checkpoint(b"checkpoint_v2")?;
  println!(
    "✓ Pruned {} segments from {} shards",
    stats.segments_deleted, stats.shards_pruned
  );

  // 9. Checkpoint compaction
  println!("\n--- Compacting Checkpoints ---");
  wal.create_checkpoint(b"checkpoint_v3")?;
  wal.create_checkpoint(b"checkpoint_v4")?;

  let compact_stats = wal.compact_checkpoints(2)?;
  println!(
    "✓ Compacted {} -> {} checkpoints (reclaimed {} bytes)",
    compact_stats.checkpoints_before,
    compact_stats.checkpoints_after,
    compact_stats.bytes_reclaimed
  );

  // 10. Load latest checkpoint
  println!("\n--- Latest Checkpoint ---");
  let (latest_id, latest_data) = wal.load_latest_checkpoint()?;
  println!(
    "Latest checkpoint: {:?}",
    String::from_utf8_lossy(&latest_id)
  );
  println!(
    "Total entries across all shards: {}",
    latest_data.offsets.iter().sum::<u64>()
  );

  println!("\n✓ Example completed successfully!");
  println!("\nData stored in: ./example_sharded_wal");
  println!("To clean up: rm -rf ./example_sharded_wal");

  Ok(())
}

#[cfg(not(feature = "sharded"))]
fn main() {
  println!("This example requires the 'sharded' feature.");
  println!("Run with: cargo run --example sharded_wal --features sharded");
}
