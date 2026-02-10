//! Binary format serialization and deserialization for checkpoint entries.
//!
//! Each checkpoint entry is a self-contained, CRC-protected binary blob
//! stored in the append-only `checkpoints.log` file.
//!
//! ## Binary Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ MAGIC          │ 4 bytes  │ "CKPT" (0x43 0x4B 0x50 0x54)   │
//! ├─────────────────────────────────────────────────────────────┤
//! │ CRC32          │ 4 bytes  │ Checksum of PAYLOAD only       │
//! ├─────────────────────────────────────────────────────────────┤
//! │ ENTRY_LENGTH   │ 4 bytes  │ Length of PAYLOAD (u32 LE)     │
//! ├─────────────────────────────────────────────────────────────┤
//! │ PAYLOAD        │ N bytes  │ See payload structure          │
//! └─────────────────────────────────────────────────────────────┘
//!
//! Payload Structure:
//! ┌─────────────────────────────────────────────────────────────┐
//! │ VERSION        │ 1 byte   │ Format version (0x01)          │
//! ├─────────────────────────────────────────────────────────────┤
//! │ USER_ID_LEN    │ 2 bytes  │ Length of user_id (u16 LE)     │
//! ├─────────────────────────────────────────────────────────────┤
//! │ USER_ID        │ N bytes  │ User-provided checkpoint ID     │
//! ├─────────────────────────────────────────────────────────────┤
//! │ TIMESTAMP      │ 8 bytes  │ Unix timestamp in seconds      │
//! ├─────────────────────────────────────────────────────────────┤
//! │ SHARD_COUNT    │ 2 bytes  │ Number of shards (u16 LE)      │
//! ├─────────────────────────────────────────────────────────────┤
//! │ OFFSETS        │ 8*N bytes│ One u64 per shard              │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Error, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::io::{Cursor, Read, Write};

/// Magic bytes identifying a checkpoint entry: "CKPT"
pub const MAGIC: &[u8; 4] = b"CKPT";

/// Current checkpoint format version
pub const VERSION: u8 = 0x01;

/// Size of the entry header (MAGIC + CRC + LENGTH)
pub const HEADER_SIZE: usize = 4 + 4 + 4; // 12 bytes

/// A checkpoint entry representing a consistent snapshot across all shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointEntry {
  /// User-provided checkpoint identifier (e.g., Raft index, transaction ID)
  pub user_id: Vec<u8>,

  /// Unix timestamp when checkpoint was created
  pub timestamp: u64,

  /// Number of shards (for validation)
  pub shard_count: u16,

  /// Durable WAL offset for each shard (indexed by shard_id)
  pub offsets: Vec<u64>,
}

impl CheckpointEntry {
  /// Creates a new checkpoint entry.
  ///
  /// # Arguments
  ///
  /// * `user_id` - User-defined checkpoint identifier (must be non-empty, ≤ 65535 bytes)
  /// * `timestamp` - Unix timestamp in seconds
  /// * `offsets` - Durable offset for each shard (length must equal shard_count)
  ///
  /// # Errors
  ///
  /// Returns `Error::InvalidCheckpointId` if:
  /// - `user_id` is empty
  /// - `user_id` exceeds 65535 bytes
  /// - `offsets.len()` exceeds 65535 or is zero
  pub fn new(user_id: Vec<u8>, timestamp: u64, offsets: Vec<u64>) -> Result<Self> {
    if user_id.is_empty() {
      return Err(Error::InvalidCheckpointId("user_id cannot be empty".into()));
    }

    if user_id.len() > u16::MAX as usize {
      return Err(Error::InvalidCheckpointId(format!(
        "user_id too long: {} bytes (max: {})",
        user_id.len(),
        u16::MAX
      )));
    }

    if offsets.is_empty() {
      return Err(Error::InvalidCheckpointId("offsets cannot be empty".into()));
    }

    if offsets.len() > u16::MAX as usize {
      return Err(Error::InvalidCheckpointId(format!(
        "too many shards: {} (max: {})",
        offsets.len(),
        u16::MAX
      )));
    }

    Ok(Self {
      user_id,
      timestamp,
      shard_count: offsets.len() as u16,
      offsets,
    })
  }

  /// Serializes the checkpoint entry to binary format with framing.
  ///
  /// The output includes:
  /// - MAGIC bytes (4)
  /// - CRC32 checksum (4)
  /// - Payload length (4)
  /// - Payload (variable)
  ///
  /// # Returns
  ///
  /// A `Vec<u8>` containing the complete framed entry.
  pub fn serialize(&self) -> Result<Vec<u8>> {
    // 1. Build payload
    let payload = self.serialize_payload()?;

    // 2. Compute CRC over payload
    let crc = self.compute_crc(&payload);

    // 3. Build complete entry with framing
    let total_size = HEADER_SIZE + payload.len();
    let mut buf = Vec::with_capacity(total_size);

    buf.write_all(MAGIC)?;
    buf.write_u32::<LittleEndian>(crc)?;
    buf.write_u32::<LittleEndian>(payload.len() as u32)?;
    buf.write_all(&payload)?;

    Ok(buf)
  }

  /// Deserializes a checkpoint entry from a reader.
  ///
  /// The reader must be positioned at the start of a checkpoint entry
  /// (at the MAGIC bytes).
  ///
  /// # Arguments
  ///
  /// * `reader` - A reader positioned at the entry start
  /// * `file_offset` - The byte offset in the file where this entry starts
  ///   (used for error reporting)
  ///
  /// # Returns
  ///
  /// A tuple of `(CheckpointEntry, next_offset)` where `next_offset` is the
  /// file position immediately after this entry.
  ///
  /// # Errors
  ///
  /// Returns `Error::CheckpointCorrupted` if:
  /// - MAGIC bytes don't match
  /// - CRC validation fails
  /// - Payload is malformed
  /// - Reader encounters EOF
  pub fn deserialize<R: Read>(reader: &mut R, file_offset: u64) -> Result<(Self, u64)> {
    // 1. Read and validate MAGIC
    let mut magic = [0u8; 4];
    reader
      .read_exact(&mut magic)
      .map_err(|e| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Failed to read MAGIC: {}", e),
      })?;

    if &magic != MAGIC {
      return Err(Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Invalid MAGIC: expected {:?}, got {:?}", MAGIC, magic),
      });
    }

    // 2. Read CRC and length
    let stored_crc = reader
      .read_u32::<LittleEndian>()
      .map_err(|e| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Failed to read CRC: {}", e),
      })?;

    let payload_len = reader
      .read_u32::<LittleEndian>()
      .map_err(|e| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Failed to read length: {}", e),
      })? as usize;

    // 3. Read payload
    let mut payload = vec![0u8; payload_len];
    reader
      .read_exact(&mut payload)
      .map_err(|e| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Failed to read payload: {}", e),
      })?;

    // 4. Validate CRC
    let computed_crc = Self::compute_crc_static(&payload);
    if computed_crc != stored_crc {
      return Err(Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!(
          "CRC mismatch: expected {:#x}, got {:#x}",
          stored_crc, computed_crc
        ),
      });
    }

    // 5. Parse payload
    let entry = Self::deserialize_payload(&payload, file_offset)?;

    // 6. Calculate next offset
    let next_offset = file_offset + HEADER_SIZE as u64 + payload_len as u64;

    Ok((entry, next_offset))
  }

  /// Extracts just the user_id from a payload without full deserialization.
  ///
  /// This is useful during index rebuilding when we only need the key.
  ///
  /// # Arguments
  ///
  /// * `payload` - The payload bytes (without framing)
  ///
  /// # Returns
  ///
  /// The user_id as a `Vec<u8>`.
  pub fn extract_user_id(payload: &[u8]) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(payload);

    // Skip VERSION (1 byte)
    cursor
      .read_u8()
      .map_err(|_| Error::Corruption("Truncated payload: missing VERSION".into()))?;

    // Read USER_ID_LEN
    let user_id_len = cursor
      .read_u16::<LittleEndian>()
      .map_err(|_| Error::Corruption("Truncated payload: missing USER_ID_LEN".into()))?
      as usize;

    // Read USER_ID
    let mut user_id = vec![0u8; user_id_len];
    cursor
      .read_exact(&mut user_id)
      .map_err(|_| Error::Corruption("Truncated payload: incomplete USER_ID".into()))?;

    Ok(user_id)
  }

  // --- Private helpers ---

  fn serialize_payload(&self) -> Result<Vec<u8>> {
    let capacity = 1 // VERSION
            + 2 // USER_ID_LEN
            + self.user_id.len()
            + 8 // TIMESTAMP
            + 2 // SHARD_COUNT
            + (self.offsets.len() * 8); // OFFSETS

    let mut buf = Vec::with_capacity(capacity);

    buf.write_u8(VERSION)?;
    buf.write_u16::<LittleEndian>(self.user_id.len() as u16)?;
    buf.write_all(&self.user_id)?;
    buf.write_u64::<LittleEndian>(self.timestamp)?;
    buf.write_u16::<LittleEndian>(self.shard_count)?;

    for offset in &self.offsets {
      buf.write_u64::<LittleEndian>(*offset)?;
    }

    Ok(buf)
  }

  fn deserialize_payload(payload: &[u8], file_offset: u64) -> Result<Self> {
    let mut cursor = Cursor::new(payload);

    // VERSION
    let version = cursor.read_u8().map_err(|_| Error::CheckpointCorrupted {
      offset: file_offset,
      reason: "Missing VERSION".into(),
    })?;

    if version != VERSION {
      return Err(Error::CheckpointCorrupted {
        offset: file_offset,
        reason: format!("Unsupported version: {}", version),
      });
    }

    // USER_ID
    let user_id_len = cursor
      .read_u16::<LittleEndian>()
      .map_err(|_| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: "Missing USER_ID_LEN".into(),
      })? as usize;

    let mut user_id = vec![0u8; user_id_len];
    cursor
      .read_exact(&mut user_id)
      .map_err(|_| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: "Incomplete USER_ID".into(),
      })?;

    // TIMESTAMP
    let timestamp = cursor
      .read_u64::<LittleEndian>()
      .map_err(|_| Error::CheckpointCorrupted {
        offset: file_offset,
        reason: "Missing TIMESTAMP".into(),
      })?;

    // SHARD_COUNT
    let shard_count =
      cursor
        .read_u16::<LittleEndian>()
        .map_err(|_| Error::CheckpointCorrupted {
          offset: file_offset,
          reason: "Missing SHARD_COUNT".into(),
        })?;

    // OFFSETS
    let mut offsets = Vec::with_capacity(shard_count as usize);
    for _ in 0..shard_count {
      let offset = cursor
        .read_u64::<LittleEndian>()
        .map_err(|_| Error::CheckpointCorrupted {
          offset: file_offset,
          reason: "Incomplete OFFSETS".into(),
        })?;
      offsets.push(offset);
    }

    Ok(Self {
      user_id,
      timestamp,
      shard_count,
      offsets,
    })
  }

  fn compute_crc(&self, payload: &[u8]) -> u32 {
    Self::compute_crc_static(payload)
  }

  fn compute_crc_static(payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    hasher.finalize()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_new_validates_user_id() {
    // Empty user_id
    assert!(CheckpointEntry::new(vec![], 0, vec![0]).is_err());

    // Valid user_id
    assert!(CheckpointEntry::new(vec![1, 2, 3], 0, vec![0]).is_ok());
  }

  #[test]
  fn test_new_validates_offsets() {
    // Empty offsets
    assert!(CheckpointEntry::new(vec![1], 0, vec![]).is_err());

    // Valid offsets
    assert!(CheckpointEntry::new(vec![1], 0, vec![100, 200]).is_ok());
  }

  #[test]
  fn test_serialize_deserialize_roundtrip() {
    let entry = CheckpointEntry::new(
      b"checkpoint_123".to_vec(),
      1234567890,
      vec![100, 200, 300, 400],
    )
    .unwrap();

    let serialized = entry.serialize().unwrap();
    let mut cursor = Cursor::new(&serialized);
    let (deserialized, _) = CheckpointEntry::deserialize(&mut cursor, 0).unwrap();

    assert_eq!(entry, deserialized);
  }

  #[test]
  fn test_extract_user_id() {
    let entry = CheckpointEntry::new(b"my_checkpoint_id".to_vec(), 1000, vec![1, 2, 3]).unwrap();

    let serialized = entry.serialize().unwrap();
    // Skip header to get to payload
    let payload = &serialized[HEADER_SIZE..];

    let extracted = CheckpointEntry::extract_user_id(payload).unwrap();
    assert_eq!(extracted, b"my_checkpoint_id");
  }

  // Binary format serialization/deserialization tests
  #[test]
  fn test_roundtrip_minimal() {
    let entry = CheckpointEntry::new(b"x".to_vec(), 0, vec![0]).unwrap();

    let bytes = entry.serialize().unwrap();
    let mut cursor = Cursor::new(&bytes);
    let (decoded, _) = CheckpointEntry::deserialize(&mut cursor, 0).unwrap();

    assert_eq!(entry, decoded);
  }

  #[test]
  fn test_roundtrip_large() {
    let user_id = vec![0xAB; 1000]; // 1KB user_id
    let offsets = vec![123456789; 256]; // 256 shards

    let entry = CheckpointEntry::new(user_id.clone(), 9999999999, offsets.clone()).unwrap();

    let bytes = entry.serialize().unwrap();
    let mut cursor = Cursor::new(&bytes);
    let (decoded, _) = CheckpointEntry::deserialize(&mut cursor, 0).unwrap();

    assert_eq!(decoded.user_id, user_id);
    assert_eq!(decoded.offsets, offsets);
    assert_eq!(decoded.timestamp, 9999999999);
  }

  #[test]
  fn test_detects_corrupted_magic() {
    let entry = CheckpointEntry::new(b"test".to_vec(), 1000, vec![10]).unwrap();
    let mut bytes = entry.serialize().unwrap();

    // Corrupt MAGIC
    bytes[0] = 0xFF;

    let mut cursor = Cursor::new(&bytes);
    let result = CheckpointEntry::deserialize(&mut cursor, 0);

    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("Invalid MAGIC"));
  }

  #[test]
  fn test_detects_corrupted_crc() {
    let entry = CheckpointEntry::new(b"test".to_vec(), 1000, vec![10]).unwrap();
    let mut bytes = entry.serialize().unwrap();

    // Corrupt CRC (bytes 4-7)
    bytes[4] ^= 0xFF;

    let mut cursor = Cursor::new(&bytes);
    let result = CheckpointEntry::deserialize(&mut cursor, 0);

    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("CRC mismatch"));
  }

  #[test]
  fn test_detects_truncated_payload() {
    let entry = CheckpointEntry::new(b"test".to_vec(), 1000, vec![10, 20]).unwrap();
    let bytes = entry.serialize().unwrap();

    // Truncate 10 bytes from end
    let truncated = &bytes[..bytes.len() - 10];

    let mut cursor = Cursor::new(truncated);
    let result = CheckpointEntry::deserialize(&mut cursor, 0);

    assert!(result.is_err());
  }

  #[test]
  fn test_extract_user_id_fast_path() {
    let entry = CheckpointEntry::new(
      b"raft_term_5_index_1234567".to_vec(),
      5000,
      vec![1, 2, 3, 4, 5],
    )
    .unwrap();

    let bytes = entry.serialize().unwrap();
    let payload = &bytes[HEADER_SIZE..];

    let extracted = CheckpointEntry::extract_user_id(payload).unwrap();
    assert_eq!(extracted, b"raft_term_5_index_1234567");
  }

  #[test]
  fn test_serialized_structure() {
    let entry = CheckpointEntry::new(b"abc".to_vec(), 1000, vec![100]).unwrap();
    let bytes = entry.serialize().unwrap();

    // Verify structure
    assert_eq!(&bytes[0..4], MAGIC);

    // Total size = HEADER(12) + VERSION(1) + USER_ID_LEN(2) + USER_ID(3)
    //              + TIMESTAMP(8) + SHARD_COUNT(2) + OFFSETS(8)
    //            = 12 + 24 = 36
    assert_eq!(bytes.len(), 36);
  }
}
