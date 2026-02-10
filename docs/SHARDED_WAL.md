# ShardedWAL: Horizontal Sharding Extension

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Operational Guide](#operational-guide)
- [Design Rationale](#design-rationale)
- [Limitations](#limitations)
- [Migration Guide](#migration-guide)
- [Troubleshooting](#troubleshooting)
- [References](#references)

---

## Overview

`ShardedWal` is an optional horizontal sharding extension for IronWAL that provides:

- **Automatic key-based routing** - Writes are distributed across N independent shards via deterministic hashing
- **Consistent checkpointing** - Capture point-in-time snapshots across all shards atomically
- **User-addressable snapshots** - Reference checkpoints by meaningful IDs (Raft indices, transaction IDs, ULIDs)
- **Efficient pruning** - Delete historical data before checkpoint boundaries across all shards
- **Linear throughput scaling** - Add more shards to increase write bandwidth

### When to Use ShardedWAL

✅ **Use ShardedWAL when:**
- Write throughput exceeds ~100 MB/s on a single stream
- You need consistent snapshots across multiple independent streams
- Building distributed systems requiring coordinated checkpoints (Raft, Paxos, event sourcing)
- Parallelizing writes across multiple physical disks/volumes
- Managing millions of logical entities that can be independently sharded

❌ **Don't use ShardedWAL when:**
- Write volume is low (< 10 MB/s) - overhead isn't worth it
- You need cross-key transactions - shards commit independently
- You need to dynamically change shard count - requires offline migration
- Single-threaded sequential writes - no parallelism benefit

---

## Architecture

### Directory Structure
```
<root>/
├── HEAD                      # 2-266 bytes: latest checkpoint's user_id
├── checkpoints.log           # Append-only checkpoint history
├── shard_00/                 # Complete, independent IronWAL stream
│   ├── 00000000000000000000.wal
│   ├── 00000000000000001000.wal
│   ├── segments.idx
│   └── head.state
├── shard_01/
│   └── ...
├── shard_02/
│   └── ...
...
└── shard_15/
    └── ...
```

### Components

#### 1. Router
- **Purpose**: Deterministic key-to-shard mapping
- **Algorithm**: `shard_id = hash(key) % shard_count`
- **Hash Function**: SipHash (via Rust's `DefaultHasher`)
- **Properties**: 
  - Same key always routes to same shard
  - Uniform distribution across shards
  - No coordination required between threads

#### 2. Watermark Tracker
- **Purpose**: Track durable offsets per shard for checkpoint consistency
- **Behavior by SyncMode**:
  - `Strict`: Updated immediately after each `append()` (data is durable)
  - `BatchOnly`: Updated after `append_batch()` completion
  - `Async`: Requires manual tracking (not automatic)
- **Concurrency**: Lock-free using `AtomicU64` per shard

#### 3. Checkpoint Manager
- **Purpose**: Persist and retrieve consistent snapshots
- **Storage**: Append-only `checkpoints.log` file with binary format
- **Index**: In-memory `HashMap<user_id, CheckpointMetadata>` for O(1) lookup
- **Durability**: CRC32 checksums + atomic HEAD file updates

#### 4. HEAD File
- **Purpose**: Point to the latest checkpoint for O(1) access
- **Format**: `[user_id_len: u16][user_id: bytes]`
- **Atomicity**: Temp file + rename + directory fsync
- **Fallback**: If corrupt, scan `checkpoints.log` to find latest

### Checkpoint Binary Format
```
┌─────────────────────────────────────────────────────────────┐
│ MAGIC          │ 4 bytes  │ "CKPT" (0x43 0x4B 0x50 0x54)   │
├─────────────────────────────────────────────────────────────┤
│ CRC32          │ 4 bytes  │ Checksum of PAYLOAD            │
├─────────────────────────────────────────────────────────────┤
│ ENTRY_LENGTH   │ 4 bytes  │ Length of PAYLOAD (u32 LE)     │
├─────────────────────────────────────────────────────────────┤
│ PAYLOAD        │ N bytes  │ See payload structure below    │
└─────────────────────────────────────────────────────────────┘

Payload Structure:
┌─────────────────────────────────────────────────────────────┐
│ VERSION        │ 1 byte   │ Format version (0x01)          │
├─────────────────────────────────────────────────────────────┤
│ USER_ID_LEN    │ 2 bytes  │ Length of user_id (u16 LE)     │
├─────────────────────────────────────────────────────────────┤
│ USER_ID        │ N bytes  │ User-provided checkpoint ID     │
├─────────────────────────────────────────────────────────────┤
│ TIMESTAMP      │ 8 bytes  │ Unix timestamp in nanoseconds  │
├─────────────────────────────────────────────────────────────┤
│ SHARD_COUNT    │ 2 bytes  │ Number of shards (u16 LE)      │
├─────────────────────────────────────────────────────────────┤
│ OFFSETS        │ 8*N bytes│ One u64 per shard              │
└─────────────────────────────────────────────────────────────┘
```

**Design Notes**:
- MAGIC bytes enable corruption detection and forward/backward scanning
- CRC32 protects against bit rot and hardware errors
- ENTRY_LENGTH enables efficient seeking without parsing payload
- VERSION field allows format evolution
- TIMESTAMP supports retention policies and debugging
- SHARD_COUNT in payload ensures forward compatibility

---

## Operational Guide

### Shard Count Selection

**Rule of Thumb**: `shard_count` should be greater than or equal to the expected number of concurrent writer threads.

| Concurrent Writers | Recommended Shards | Recommended max_open_segments | Reasoning |
|--------------------|--------------------|-----------------------------|-----------|
| 1-4                | 4-8                | 16-32                       | Minimal overhead, allows for some parallelism. |
| 4-16               | 16-32              | 64-128                      | Excellent parallelism for typical server workloads. |
| 16-64              | 64-128             | 256-512                     | High-concurrency systems. Ensure `max_open_segments` is also increased. |
| 64+                | 256+               | 1024+                       | Extreme scale. Requires significant file descriptor resources. |

**Considerations**:
- **Too few shards**: Write contention, underutilized hardware
- **Too many shards**: Excessive open files, cache thrashing, coordination overhead
- **Sweet spot**: `shard_count >= concurrent_writers` is a good starting point.

### Checkpoint Frequency

| Use Case | Frequency | Rationale |
|----------|-----------|-----------|
| Raft/Consensus | Every 100-1000 entries | Snapshot stability |
| Database WAL | Every 1-10 seconds | Recovery time balance |
| Event Sourcing | Per aggregate commit | Domain transaction boundary |
| Stream Processing | Every 10,000-100,000 events | Checkpoint overhead vs recovery |

**Factors to Consider**:
- **Recovery time**: More frequent = faster recovery
- **Checkpoint overhead**: ~1-10 ms per checkpoint
- **Disk space**: Each checkpoint adds ~200 bytes to log
- **Pruning efficiency**: Need checkpoints to define safe pruning points

### Monitoring

**Key Metrics to Track**:
```rust
// Checkpoint metrics
let (latest_id, checkpoint) = wal.load_latest_checkpoint()?;
let checkpoint_count = wal.checkpoint_count();

// Per-shard metrics
for shard_id in 0..wal.shard_count() {
    let offset = checkpoint.offsets[shard_id as usize];
    // Track: write rate, lag, segment count per shard
}

// Checkpoint log size
let log_size = std::fs::metadata(root.join("checkpoints.log"))?.len();
```

**Alerts**:
- Checkpoint log size > 10 MB (time to compact)
- Checkpoint creation taking > 100 ms (I/O saturation)
- Shard offset skew > 10% (uneven key distribution)
- Checkpoint failure rate > 0.1% (investigate disk/permissions)

### Backup Strategy

**Option 1: Checkpoint-Based Backup**
```rust
// 1. Create checkpoint
wal.create_checkpoint(b"backup_point")?;

// 2. Flush all shards (ensure durability)
// (Already done by checkpoint creation in Strict/BatchOnly mode)

// 3. Copy shard directories + checkpoints.log + HEAD
for shard_id in 0..wal.shard_count() {
    let shard_dir = format!("shard_{:02}", shard_id);
    backup_directory(&shard_dir, &backup_dest)?;
}
backup_file("checkpoints.log", &backup_dest)?;
backup_file("HEAD", &backup_dest)?;
```

**Option 2: Continuous Backup**
```rust
// Use filesystem snapshots (LVM, ZFS, BTRFS)
// 1. Create checkpoint (for consistency marker)
wal.create_checkpoint(b"snapshot_marker")?;

// 2. Trigger filesystem snapshot
system("lvm snapshot /data/wal /backup/wal-snapshot")?;

// Restore: Just mount the snapshot
```

### Disaster Recovery

**Scenario 1: Corrupted Checkpoint Log**
```rust
// Checkpoint log is corrupt, but shard data is intact

// 1. Delete corrupt checkpoint log
std::fs::remove_file("checkpoints.log")?;
std::fs::remove_file("HEAD")?;

// 2. Restart WAL (will create empty checkpoint log)
let wal = ShardedWal::new(opts, 16)?;

// 3. Create new checkpoint from current state
wal.create_checkpoint(b"recovered")?;

// Note: Old checkpoints are lost, but data is intact
```

**Scenario 2: Missing Shards**
```rust
// Shard 5 is missing/corrupt

// Option A: Restore from backup (pseudo-code - implement based on your backup strategy)
// Example using filesystem copy:
// std::fs::copy("/backup/shard_05/segment.wal", "./shard_05/segment.wal")?;

// Option B: Accept data loss for that shard
// (Only works if application can tolerate missing data)
// No code needed - remaining shards continue working
```

**Note**: The actual backup restoration strategy depends on your backup system
(filesystem snapshots, object storage, tape archives, etc.). Implement according
to your infrastructure requirements.

**Scenario 3: Shard Count Mismatch**
```rust
// Opened with 16 shards, but data has 8 shards

// Error will occur:
// Error::ShardCountMismatch { expected: 16, found: 8 }

// Solution: Must match original shard count
let wal = ShardedWal::new(opts, 8)?; // Use correct count
```

---

## Design Rationale

### Why Append-Only Checkpoint Log?

**Alternatives Considered**:
1. **Single "checkpoint.json" file** - Rewritten on each checkpoint
   - ❌ High write amplification
   - ❌ Atomic updates are expensive (temp file + rename)
   
2. **Checkpoint per file** - `checkpoint_001.bin`, `checkpoint_002.bin`
   - ❌ Filesystem explosion (thousands of small files)
   - ❌ Expensive directory scans on startup
   
3. **Embedded in WAL stream** - Checkpoint metadata in special stream
   - ❌ Complex recovery (must scan entire stream)
   - ❌ Ties checkpoint lifecycle to WAL lifecycle

**Why Append-Only Wins**:
- ✅ Single sequential write per checkpoint (~1-5 ms)
- ✅ Natural versioning (all history preserved)
- ✅ Fast startup (single file scan, O(N) checkpoints)
- ✅ Simple compaction (rewrite keeping only recent N)
- ✅ No filesystem limits (one file, not thousands)

### Why Not Embedded Key-Value Index?

**Question**: Why use iterators instead of `get(key)` like core WAL?

**Answer**: Sharding is for **high-throughput sequential writes**, not random access:

1. **Key space explosion**: With millions of keys, maintaining an index per shard is expensive
2. **Write amplification**: Updating an index on every write hurts throughput
3. **Use case mismatch**: Systems using ShardedWal typically replay logs sequentially (Raft, event sourcing)

**Workaround**: Build application-level index if needed:
```rust
// Application maintains key -> (shard_id, seq_id) map
let mut index: HashMap<Vec<u8>, (u16, u64)> = HashMap::new();

// Populate during replay
for shard_id in 0..wal.shard_count() {
    let mut iter = wal.iter_shard(shard_id, 0)?;
    while let Some(entry) = iter.next() {
        let (key, value) = parse_entry(entry?);
        index.insert(key, (shard_id, current_offset));
    }
}

// Now you have fast lookups
let (shard_id, offset) = index.get(&key)?;
```

### Why Nanosecond Timestamps?

**Question**: Why nanoseconds instead of seconds for checkpoint timestamps?

**Answer**: Prevents collisions in high-frequency checkpointing:
```rust
// With seconds: Checkpoints created in same second have same timestamp
for i in 0..1000 {
    wal.create_checkpoint(format!("ckpt_{}", i).as_bytes())?;
}
// All 1000 have identical timestamps!

// With nanoseconds: Each checkpoint has unique timestamp
// Enables proper sorting by creation time
```

---

## Limitations

### 1. Fixed Shard Count

**Limitation**: Shard count cannot be changed after creation without migration.

**Impact**: 
- Cannot dynamically add shards to increase throughput
- Cannot reduce shards to save resources

**Workaround**: 
- Plan shard count for peak load
- Use offline migration if requirements change (see [Migration Guide](#migration-guide))

**Future Enhancement**: Virtual nodes / consistent hashing for dynamic resharding

### 2. No Cross-Shard Transactions

**Limitation**: `append_batch()` spanning multiple shards is not atomic across shards.

**Impact**:
```rust
let batch = vec![
    (b"user_1".to_vec(), b"data".to_vec()), // Routes to shard 3
    (b"user_2".to_vec(), b"data".to_vec()), // Routes to shard 7
];

// If crash occurs:
// - Shard 3 might commit
// - Shard 7 might not commit
// Result: Partial batch application
```

**Workaround**: 
- Use external 2PC coordinator if atomicity is required
- Design entities to be single-shard (all data for user_1 uses same key prefix)

### 3. No Built-in Rebalancing

**Limitation**: If keys are not uniformly distributed, some shards become hot.

**Impact**:
```rust
// Bad: All keys start with "user_"
for i in 0..10000 {
    wal.append(format!("user_{}", i).as_bytes(), b"data")?;
}
// Hash("user_X") might cluster, causing uneven distribution

// Good: Include unique ID in key
for i in 0..10000 {
    wal.append(format!("{}", i).as_bytes(), b"data")?;
}
// Hash(i) distributes uniformly
```

**Workaround**: 
- Design keys for uniform distribution
- Monitor per-shard metrics to detect skew
- Manually rebalance by creating new ShardedWal with different key scheme

### 4. Checkpoint Log Growth

**Limitation**: `checkpoints.log` grows unbounded without manual compaction.

**Impact**: 
- 1000 checkpoints ≈ 200 KB
- 10,000 checkpoints ≈ 2 MB
- Startup time increases linearly with checkpoint count

**Workaround**:
```rust
// Periodic compaction
if checkpoint_count > 1000 {
    wal.compact_checkpoints(100)?;
}
```

---

## Migration Guide

### From Single WAL to ShardedWAL

**Scenario**: Existing application uses core `Wal`, need to migrate to `ShardedWal`.

#### Option 1: Offline Migration (Downtime Required)
```rust
use ironwal::{Wal, WalOptions};
use ironwal::sharded::ShardedWal;

// 1. Open old WAL (read-only)
let old_wal = Wal::new(WalOptions::new("./old_data"))?;

// 2. Create new ShardedWAL
let sharded = ShardedWal::new(WalOptions::new("./new_data"), 16)?;

// 3. Replay all data with new keys
let mut iter = old_wal.iter("events", 0)?;
while let Some(entry) = iter.next() {
    let data = entry?;
    
    // Extract key from data (application-specific)
    let key = extract_key(&data)?;
    
    // Write to sharded WAL
    sharded.append(&key, &data)?;
}

// 4. Create checkpoint
sharded.create_checkpoint(b"migration_complete")?;

// 5. Update application to use new path
// 6. Delete old data after verification
```

**Downtime**: Proportional to data size (e.g., 100 GB @ 100 MB/s = ~17 minutes)

#### Option 2: Dual-Write Migration (Zero Downtime)
```rust
// Phase 1: Start dual-writing
let old_wal = Wal::new(WalOptions::new("./old"))?;
let new_wal = ShardedWal::new(WalOptions::new("./new"), 16)?;

fn write_event(old: &Wal, new: &ShardedWal, key: &[u8], data: &[u8]) -> Result<()> {
    old.append("events", data)?; // Keep writing to old
    new.append(key, data)?;      // Also write to new
    Ok(())
}

// Phase 2: Background migration of old data
std::thread::spawn(|| {
    let mut iter = old_wal.iter("events", 0)?;
    while let Some(entry) = iter.next() {
        let data = entry?;
        let key = extract_key(&data)?;
        new_wal.append(&key, &data)?;
    }
    new_wal.create_checkpoint(b"backfill_complete")?;
});

// Phase 3: Switch reads to new WAL once backfill completes
// Phase 4: Stop writing to old WAL
// Phase 5: Delete old WAL after grace period
```

**Downtime**: Zero (but increased write latency during dual-write phase)

### Changing Shard Count

**Scenario**: Need to increase from 16 to 64 shards.
```rust
// 1. Create new ShardedWAL with target shard count
let new_wal = ShardedWal::new(opts, 64)?;

// 2. Iterate all old shards
let old_wal = ShardedWal::new(old_opts, 16)?;

for old_shard_id in 0..16 {
    let mut iter = old_wal.iter_shard(old_shard_id, 0)?;
    
    while let Some(entry) = iter.next() {
        let data = entry?;
        let key = extract_key(&data)?; // Must derive key from data
        
        // Re-hash with new shard count
        new_wal.append(&key, &data)?;
    }
}

// 3. Create checkpoint
new_wal.create_checkpoint(b"resharding_complete")?;

// 4. Switch application to new WAL
```

**Critical Requirement**: Must be able to extract original keys from data. If keys are not stored in data, this migration is impossible.

---

## Troubleshooting

### Checkpoint Creation Failing

**Symptom**: `create_checkpoint()` returns error.

**Common Causes**:
1. **Disk full**: Check `df -h`
2. **Permissions**: Ensure write access to root directory
3. **Invalid user_id**: Check length < 65,535 bytes and non-empty

**Debug**:
```rust
match wal.create_checkpoint(user_id) {
    Err(Error::Io(e)) if e.kind() == ErrorKind::PermissionDenied => {
        println!("Permission denied - check directory ownership");
    }
    Err(Error::InvalidCheckpointId(msg)) => {
        println!("Invalid checkpoint ID: {}", msg);
    }
    Err(e) => println!("Unexpected error: {:?}", e),
    Ok(_) => {}
}
```

### Slow Checkpoint Creation

**Symptom**: `create_checkpoint()` takes > 100 ms.

**Causes**:
1. **Disk I/O saturation**: Use `iostat -x 1` to monitor
2. **Too many shards**: Each shard watermark read adds latency
3. **Network storage**: Latency amplified by fsync over network

**Solutions**:
- Use faster storage (NVMe > SATA SSD > HDD)
- Reduce checkpoint frequency
- Use `SyncMode::BatchOnly` instead of `Strict` for data writes

### Uneven Shard Distribution

**Symptom**: Some shards have 10x more data than others.

**Diagnosis**:
```rust
let checkpoint = wal.load_latest_checkpoint()?;
for (shard_id, offset) in checkpoint.offsets.iter().enumerate() {
    println!("Shard {}: {} entries", shard_id, offset);
}
// Output shows huge variance: Shard 0: 10000, Shard 1: 100, Shard 2: 50000
```

**Cause**: Non-uniform key distribution.

**Solution**: Redesign keys for better distribution:
```rust
// Bad: Sequential IDs
let key = format!("{}", user_id); // 1, 2, 3, 4...

// Good: Hash-based IDs
use uuid::Uuid;
let key = Uuid::new_v4().to_string();

// Better: Explicit random prefix
let key = format!("{}_{}", rand::random::<u64>(), user_id);
```

### High Memory Usage

**Symptom**: Process memory grows over time.

**Diagnosis**:
```rust
// Check checkpoint count
let checkpoint_count = wal.checkpoint_count();
println!("Checkpoint count: {}", checkpoint_count);
// If > 10,000, memory usage from index is significant

// Formula: ~300 bytes per checkpoint
let estimated_memory = checkpoint_count * 300;
println!("Estimated checkpoint index memory: {} MB", estimated_memory / 1_000_000);
```

**Solution**: Compact checkpoint log regularly:
```rust
if checkpoint_count > 1000 {
    wal.compact_checkpoints(100)?;
}
```

---

## References

- **Core WAL Documentation**: `../README.USAGE.md`
- **API Reference**: `../API_REFERENCE.md`
- **Examples**: `../examples/sharded_wal.rs`
- **Tests**: `../tests/sharded/`
- **Benchmarks**: `../benches/sharded_*.rs`
