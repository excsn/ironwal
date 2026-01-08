use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("I/O Error: {0}")]
  Io(#[from] io::Error),

  #[error("Data Corruption: {0}")]
  Corruption(String),

  #[error("CRC32 Checksum Mismatch: expected {expected:#x}, got {actual:#x} at offset {offset}")]
  CrcMismatch { expected: u32, actual: u32, offset: u64 },

  #[error("Configuration Error: {0}")]
  Config(String),

  #[error("Segment not found for ID: {0}")]
  SegmentNotFound(u64),

  #[error("Stream not found: {0}")]
  StreamNotFound(String),

  #[error("Invalid filename format in WAL directory: {0}")]
  InvalidFilename(String),

  #[error("Serialization Error: {0}")]
  Serialization(String),
}
