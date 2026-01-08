use crate::error::Result;
use crate::wal::Wal;

/// A handle for accumulating writes in memory before committing them to disk.
/// Useful for creating atomic batches from distributed logic.
pub struct TxnWriter {
  wal: Wal,
  stream: String,
  buffer: Vec<Vec<u8>>,
}

impl TxnWriter {
  pub(crate) fn new(wal: Wal, stream: String) -> Self {
    Self {
      wal,
      stream,
      buffer: Vec::new(),
    }
  }

  /// Adds an entry to the transaction buffer.
  /// Does not interact with the WAL or disk.
  pub fn add(&mut self, data: impl Into<Vec<u8>>) {
    self.buffer.push(data.into());
  }

  /// Commits the accumulated batch to the WAL atomically.
  /// This triggers a single `fsync` (if configured).
  pub fn commit(mut self) -> Result<std::ops::Range<u64>> {
    if self.buffer.is_empty() {
      // No-op commit
      return Ok(0..0);
    }

    let refs: Vec<&[u8]> = self.buffer.iter().map(|v| v.as_slice()).collect();
    let range = self.wal.append_batch(&self.stream, &refs)?;

    self.buffer.clear();
    Ok(range)
  }
}
