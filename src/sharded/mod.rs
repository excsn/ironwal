//! # Sharded WAL Extension
//!
//! This module provides horizontal sharding capabilities for IronWAL through the
//! `ShardedWal` wrapper. It enables:
//!
//! - **Automatic key-based routing** to N independent WAL shards
//! - **Consistent checkpointing** across all shards for snapshot isolation
//! - **User-addressable checkpoint IDs** (Raft indices, transaction IDs, ULIDs, etc.)
//! - **Efficient pruning** of historical data before checkpoint boundaries
//!
//! ## Architecture
//!
//! `ShardedWal` wraps the core `Wal` and manages multiple independent WAL streams
//! (one per shard). Each shard is a complete, isolated WAL with its own segment files
//! and indices. Keys are hashed to determine shard assignment, ensuring uniform
//! distribution and deterministic routing.
//!
//! Checkpoints are stored in a separate append-only log (`checkpoints.log`) with
//! CRC protection and atomic updates via a `HEAD` pointer file.
//!
//! ## Example
//!
//! ```no_run
//! use ironwal::sharded::ShardedWal;
//! use ironwal::WalOptions;
//!
//! # fn main() -> ironwal::Result<()> {
//! // Create a sharded WAL with 16 shards
//! let opts = WalOptions::default();
//! let sharded = ShardedWal::new(opts, 16)?;
//!
//! // Writes are automatically routed by key hash
//! let (shard_id, seq_id) = sharded.append(b"user_123", b"profile_data")?;
//! println!("Wrote to shard {} at offset {}", shard_id, seq_id);
//!
//! // Create a consistent snapshot across all shards
//! sharded.create_checkpoint(b"raft_index_5000")?;
//!
//! // Later: restore from checkpoint
//! let (checkpoint_id, data) = sharded.load_latest_checkpoint()?;
//! for (shard_id, offset) in data.offsets.iter().enumerate() {
//!     println!("Shard {}: restore from offset {}", shard_id, offset);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Limitations
//!
//! - **Fixed shard count**: Cannot be changed after creation without migration
//! - **No cross-shard transactions**: Batches spanning multiple shards are not atomic
//! - **Manual checkpoint management**: User must decide when to create checkpoints
//!
//! ## Performance Characteristics
//!
//! - **Write latency**: ~60-510μs depending on `SyncMode` (same as core `Wal`)
//! - **Checkpoint creation**: ~1-10ms (dominated by fsync)
//! - **Startup time**: ~10-110ms for typical workloads (< 1000 checkpoints)
//! - **Memory overhead**: ~300 bytes per checkpoint

mod checkpoint;
mod checkpoint_log;
mod head;
mod index;
mod router;
mod wal;
mod watermark;

// Public API exports
pub use checkpoint::{CheckpointData, CompactionStats, PruneStats};
pub use wal::ShardedWal;