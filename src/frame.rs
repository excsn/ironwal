use crate::error::{Error, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::io::{self, Cursor, Read, Write};

pub const FRAME_MAGIC: u32 = 0x4C57414C; // "LWAL"

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
  Raw = 0x00,
  Lz4 = 0x01,
}

impl TryFrom<u8> for FrameType {
  type Error = Error;
  fn try_from(v: u8) -> Result<Self> {
    match v {
      0x00 => Ok(FrameType::Raw),
      0x01 => Ok(FrameType::Lz4),
      _ => Err(Error::Corruption(format!("Unknown frame type: {}", v))),
    }
  }
}

/// The exact binary layout of a Frame Header on disk (32 bytes).
///
/// [Magic: 4]
/// [CRC32: 4]
/// [Start ID: 8]
/// [Count: 4]
/// [Type: 1]
/// [Disk Size: 4]
/// [Uncompressed Size: 4]
/// [Reserved: 3]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
  pub crc: u32,
  pub start_id: u64,
  pub entry_count: u32,
  pub frame_type: FrameType,
  pub disk_size: u32,
  pub uncompressed_size: u32,
}

impl FrameHeader {
  pub const SIZE: usize = 4 + 4 + 8 + 4 + 1 + 4 + 4 + 3; // 32 bytes

  pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_u32::<LittleEndian>(FRAME_MAGIC)?;
    writer.write_u32::<LittleEndian>(self.crc)?;
    writer.write_u64::<LittleEndian>(self.start_id)?;
    writer.write_u32::<LittleEndian>(self.entry_count)?;
    writer.write_u8(self.frame_type as u8)?;
    writer.write_u32::<LittleEndian>(self.disk_size)?;
    writer.write_u32::<LittleEndian>(self.uncompressed_size)?;
    writer.write_all(&[0u8; 3])?; // Padding
    Ok(())
  }

  pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
    let magic = reader.read_u32::<LittleEndian>()?;
    if magic != FRAME_MAGIC {
      return Err(Error::Corruption(format!("Invalid Frame Magic: {:#x}", magic)));
    }

    let crc = reader.read_u32::<LittleEndian>()?;
    let start_id = reader.read_u64::<LittleEndian>()?;
    let entry_count = reader.read_u32::<LittleEndian>()?;
    let type_byte = reader.read_u8()?;
    let frame_type = FrameType::try_from(type_byte)?;
    let disk_size = reader.read_u32::<LittleEndian>()?;
    let uncompressed_size = reader.read_u32::<LittleEndian>()?;

    // Skip padding
    let mut pad = [0u8; 3];
    reader.read_exact(&mut pad)?;

    Ok(Self {
      crc,
      start_id,
      entry_count,
      frame_type,
      disk_size,
      uncompressed_size,
    })
  }
}

/// Helper to serialize a batch of entries into a raw buffer.
/// Format per entry: [Len: 4][Data: N]
pub fn serialize_batch(entries: &[&[u8]]) -> io::Result<Vec<u8>> {
  // Pre-calculate size to avoid reallocations
  let total_len: usize = entries.iter().map(|e| 4 + e.len()).sum();
  let mut buffer = Vec::with_capacity(total_len);

  for entry in entries {
    buffer.write_u32::<LittleEndian>(entry.len() as u32)?;
    buffer.write_all(entry)?;
  }
  Ok(buffer)
}

/// Helper to deserialize a raw buffer back into individual entries.
pub fn deserialize_batch(buffer: &[u8]) -> Result<Vec<Vec<u8>>> {
  let mut cursor = Cursor::new(buffer);
  let mut entries = Vec::new();
  let len = buffer.len() as u64;

  while cursor.position() < len {
    let entry_len = cursor.read_u32::<LittleEndian>()? as usize;
    let mut entry = vec![0u8; entry_len];
    cursor.read_exact(&mut entry)?;
    entries.push(entry);
  }
  Ok(entries)
}

/// Calculates CRC32 for the frame content (excluding the Magic and CRC field itself).
pub fn calculate_checksum(start_id: u64, count: u32, frame_type: FrameType, payload: &[u8]) -> u32 {
  let mut hasher = Hasher::new();
  hasher.update(&start_id.to_le_bytes());
  hasher.update(&count.to_le_bytes());
  hasher.update(&[frame_type as u8]);
  hasher.update(&(payload.len() as u32).to_le_bytes());
  // Note: We don't hash uncompressed_size here for simplicity,
  // but practically the payload hash covers integrity.
  hasher.update(payload);
  hasher.finalize()
}
