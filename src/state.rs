use crate::error::{Error, Result};

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// In-memory representation of a stream's state.
#[derive(Debug, Clone)]
pub(crate) struct StreamState {
  /// The next sequence ID to be written.
  pub next_id: u64,
  /// The Start ID of the file we are currently appending to.
  pub active_segment_start_id: u64,
}

/// Metadata stored in the `head.state` file for each stream.
/// This acts as a pointer to the active segment file, enabling fast recovery.
#[derive(Debug, Clone)]
pub struct StreamStateFile {
  /// The filename of the current active segment (e.g., "0000000000001000.wal").
  pub active_segment_name: String,
  /// A version number for future format changes.
  pub version: u8, //TODO, we currently don't use this and we should use it.
}


impl StreamStateFile {
  /// The conventional name for the state file.
  const FILENAME: &'static str = "head.state";
  pub const VERSION: u8 = 1;

  /// Reads the state file from a given stream directory using a binary format.
  pub fn read_from(stream_dir: &Path) -> Result<Option<Self>> {
    let path = stream_dir.join(Self::FILENAME);
    if !path.exists() {
      return Ok(None);
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read Version
    let version = reader.read_u8()?;
    if version != Self::VERSION {
      return Err(Error::Corruption(format!(
        "Unsupported head.state version: expected {}, got {}",
        Self::VERSION,
        version
      )));
    }

    // Read Filename Length
    let name_len = reader.read_u8()? as usize;
    if name_len == 0 || name_len > 255 {
      return Err(Error::Corruption("Invalid filename length in head.state".into()));
    }

    // Read Filename
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf)?;
    let active_segment_name = String::from_utf8(name_buf)
      .map_err(|e| Error::Corruption(format!("Invalid UTF-8 in head.state filename: {}", e)))?;

    Ok(Some(Self {
      active_segment_name,
      version: version,
    }))
  }

  /// Writes the state file to a stream directory atomically using a binary format.
  pub fn write_to(&self, stream_dir: &Path) -> Result<()> {
    let temp_path = stream_dir.join(format!("{}.tmp", Self::FILENAME));
    let final_path = stream_dir.join(Self::FILENAME);

    // 1. Write to a temporary file
    let file = File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);

    // Write Version
    writer.write_u8(Self::VERSION)?;

    // Write Filename Length and Name
    let name_bytes = self.active_segment_name.as_bytes();
    if name_bytes.len() > 255 {
      return Err(Error::Serialization(
        "Segment filename is too long for state file".into(),
      ));
    }
    writer.write_u8(name_bytes.len() as u8)?;
    writer.write_all(name_bytes)?;

    writer.flush()?;
    writer.get_ref().sync_all()?;

    // 2. Atomically rename the temp file to the final name
    fs::rename(&temp_path, &final_path)?;

    // 3. fsync the parent directory to ensure the rename is persisted
    let parent_dir = File::open(stream_dir)?;
    parent_dir.sync_all()?;

    Ok(())
  }
}
