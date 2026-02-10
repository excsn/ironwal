//! # IronWal
//!
//! `ironwal` is a high-durability, deterministic Write-Ahead Log designed for
//! systems of record such as financial ledgers and game state synchronization.
//!
//! It prioritizes data integrity, precise replication control, and predictable
//! resource usage.
//!
//! ## Key Features
//!
//! * **Deterministic Storage**: Stream keys map to explicit directories.
//! * **Hybrid Compression**: Frames are compressed (LZ4) or raw based on batch size.
//! * **Integrity**: CRC32 checksums on every frame.
//! * **Resource Safety**: Strict LRU management of open file descriptors.
//! * **Manual Pruning**: Cursor-based truncation for safe replication.
//!
//! ## Example
//!
//! ```no_run
//! use ironwal::{Wal, WalOptions};
//!
//! # fn main() -> ironwal::Result<()> {
//! let wal = Wal::new(WalOptions::default())?;
//!
//! // Atomic append
//! let id = wal.append("user_ledger", b"transaction_data")?;
//!
//! // Batch append
//! let ids = wal.append_batch("user_ledger", &[b"tx_1", b"tx_2"])?;
//! # Ok(())
//! # }
//! ```

mod batch;
mod config;
mod error;
mod frame;
mod index;
mod iter;
mod segment;
mod state;
mod util;
mod wal;

// Sharded WAL extension (optional feature)
#[cfg(feature = "sharded")]
pub mod sharded;

// Re-exports for the flat public API
pub use batch::TxnWriter;
pub use config::{CompressionType, CorruptionPolicy, ReadStrategy, SyncMode, WalOptions};
pub use error::{Error, Result};
pub use wal::Wal;
