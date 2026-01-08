use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::config::{CompressionType, ReadStrategy, WalOptions};
use crate::error::{Error, Result};
use crate::frame::{FrameHeader, FrameType, calculate_checksum, deserialize_batch, serialize_batch};

use memmap2::Mmap;

#[cfg(feature = "compression")]
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

/// Represents the physical location and metadata of a frame on disk.
/// Used to decouple the "Scan" phase from the "Fetch" phase.
#[derive(Debug, Clone)]
pub struct FrameLocation {
  pub offset: u64,
  pub header: FrameHeader,
}

struct InnerSegment {
  file: BufWriter<File>,
  current_size: u64,
  /// Number of entries written *in this session* (or total if tracked)
  entry_count: u64,
}

/// Represents an active segment file opened for WRITING.
/// Uses internal mutability to allow concurrent access via Arc (cached).
pub struct ActiveSegment {
  path: PathBuf,
  // The internal state is protected to allow usage via Arc from the Cache
  inner: Mutex<InnerSegment>,
}

impl ActiveSegment {
  pub fn create(path: PathBuf, _start_id: u64, options: &WalOptions) -> Result<Self> {
    let file = OpenOptions::new().create(true).append(true).open(&path)?;

    let metadata = file.metadata()?;
    let current_size = metadata.len();

    let writer = BufWriter::with_capacity(options.write_buffer_size, file);

    Ok(Self {
      path,
      inner: Mutex::new(InnerSegment {
        file: writer,
        current_size,
        entry_count: 0,
      }),
    })
  }

  /// Appends a batch of entries to this segment.
  /// `batch_start_id` is provided by the WalState sequencer.
  pub fn append(&self, entries: &[&[u8]], batch_start_id: u64, options: &WalOptions) -> Result<std::ops::Range<u64>> {
    let count = entries.len() as u32;
    if count == 0 {
      return Ok(batch_start_id..batch_start_id);
    }

    let mut inner = self.inner.lock();

    // 1. Serialize entries into a raw buffer
    let raw_payload = serialize_batch(entries).map_err(Error::Io)?;
    let uncompressed_size = raw_payload.len() as u32;

    // 2. Decide compression
    let (frame_type, disk_payload) = self.compress_if_needed(&raw_payload, options)?;

    // 3. Prepare Header
    let crc = calculate_checksum(batch_start_id, count, frame_type, &disk_payload);

    let header = FrameHeader {
      crc,
      start_id: batch_start_id,
      entry_count: count,
      frame_type,
      disk_size: disk_payload.len() as u32,
      uncompressed_size,
    };

    // 4. Write Header + Payload
    header.write(&mut inner.file).map_err(Error::Io)?;
    inner.file.write_all(&disk_payload).map_err(Error::Io)?;

    // 5. Update stats
    let bytes_written = FrameHeader::SIZE as u64 + disk_payload.len() as u64;
    inner.current_size += bytes_written;
    inner.entry_count += count as u64;

    Ok(batch_start_id..batch_start_id + count as u64)
  }

  /// Returns current file size for rotation checks.
  pub fn size(&self) -> u64 {
    self.inner.lock().current_size
  }

  /// Returns entry count for rotation checks.
  pub fn count(&self) -> u64 {
    self.inner.lock().entry_count
  }

  pub fn flush(&self) -> Result<()> {
    let mut inner = self.inner.lock();
    inner.file.flush().map_err(Error::Io)?;
    inner.file.get_ref().sync_data().map_err(Error::Io)?;
    Ok(())
  }

  pub fn flush_buffer(&self) -> Result<()> {
    let mut inner = self.inner.lock();
    inner.file.flush().map_err(Error::Io)?;
    Ok(())
  }

  /// Scans the file to find the end of the last valid frame and truncates
  /// any corrupted tail data. Returns the number of bytes preserved.
  pub fn repair(&self) -> Result<u64> {
    // 1. Flush any pending writes to disk so the reader can see them
    {
      let mut inner = self.inner.lock();
      inner.file.flush().map_err(Error::Io)?;
    }

    // 2. Open a fresh read-only handle to inspect the file
    // We cannot use the inner.file handle because it is opened in 'append' mode,
    // which may not support reading/seeking on some platforms.
    let mut reader = File::open(&self.path)?;
    let mut valid_end = 0;

    loop {
      let header = match FrameHeader::read(&mut reader) {
        Ok(h) => h,
        Err(_) => break, // EOF or corrupt header, stop scanning
      };

      let frame_len = FrameHeader::SIZE as u64 + header.disk_size as u64;

      // Verify payload exists physically on disk
      if let Err(_) = reader.seek(SeekFrom::Current(header.disk_size as i64)) {
        break;
      }

      valid_end += frame_len;
    }

    // 3. Truncate using the write handle
    let mut inner = self.inner.lock();
    inner.file.get_ref().set_len(valid_end)?;
    inner.file.seek(SeekFrom::Start(valid_end))?;
    inner.current_size = valid_end;

    Ok(valid_end)
  }

  fn compress_if_needed<'a>(
    &self,
    raw: &'a [u8],
    options: &WalOptions,
  ) -> Result<(FrameType, std::borrow::Cow<'a, [u8]>)> {
    if options.compression == CompressionType::None || raw.len() < options.min_compression_size {
      return Ok((FrameType::Raw, std::borrow::Cow::Borrowed(raw)));
    }

    #[cfg(feature = "compression")]
    {
      if options.compression == CompressionType::Lz4 {
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(raw).map_err(Error::Io)?;

        let compressed = encoder
          .finish()
          .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        // Only use compression if we actually saved space
        if compressed.len() < raw.len() {
          return Ok((FrameType::Lz4, std::borrow::Cow::Owned(compressed)));
        }
      }
    }

    Ok((FrameType::Raw, std::borrow::Cow::Borrowed(raw)))
  }
}

/// Represents a segment opened for READING.
pub enum SegmentReader {
  Io(BufReader<File>),
  Mmap(Mmap, usize), // Added usize to track cursor position
}

impl SegmentReader {
  pub fn open(path: &Path, options: &WalOptions) -> Result<Self> {
    let file = File::open(path)?;

    if options.read_strategy == ReadStrategy::Mmap {
      // Safety: Caller must ensure file is not modified externally.
      let mmap = unsafe { Mmap::map(&file)? };
      return Ok(SegmentReader::Mmap(mmap, 0)); // Initialize cursor at 0
    }

    let reader = BufReader::with_capacity(options.read_buffer_size, file);
    Ok(SegmentReader::Io(reader))
  }

  /// Reads the NEXT batch from the current cursor position.
  /// Returns the Header AND the payload so the iterator knows the ID range.
  pub fn next_batch(&mut self) -> Result<Option<(FrameHeader, Vec<Vec<u8>>)>> {
    match self {
      Self::Io(reader) => {
        // We rely on FrameHeader::read to handle EOF
        let header = match FrameHeader::read(reader) {
          Ok(h) => h,
          Err(e) => {
            if let Error::Io(ref io_e) = e {
              if io_e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
              }
            }
            return Err(e);
          }
        };

        let mut payload = vec![0u8; header.disk_size as usize];
        reader.read_exact(&mut payload)?;

        let calc_crc = calculate_checksum(header.start_id, header.entry_count, header.frame_type, &payload);
        if calc_crc != header.crc {
          return Err(Error::CrcMismatch {
            expected: header.crc,
            actual: calc_crc,
            offset: 0, // We can't easily track offset in Io mode without tracking manually
          });
        }

        let final_data = Self::decompress(header.frame_type, &payload, header.uncompressed_size)?;
        Ok(Some((header, deserialize_batch(&final_data)?)))
      }
      Self::Mmap(mmap, cursor) => {
        if *cursor + FrameHeader::SIZE > mmap.len() {
          return Ok(None);
        }

        let mut header_slice = &mmap[*cursor..];
        let header = FrameHeader::read(&mut header_slice)?;

        let payload_start = *cursor + FrameHeader::SIZE;
        let payload_end = payload_start + header.disk_size as usize;

        if payload_end > mmap.len() {
          return Err(Error::Corruption("Frame payload truncated".into()));
        }

        let payload = &mmap[payload_start..payload_end];
        let calc_crc = calculate_checksum(header.start_id, header.entry_count, header.frame_type, payload);
        if calc_crc != header.crc {
          return Err(Error::CrcMismatch {
            expected: header.crc,
            actual: calc_crc,
            offset: *cursor as u64,
          });
        }

        // Advance cursor so the next call reads the next frame
        *cursor = payload_end;

        let final_data = Self::decompress(header.frame_type, payload, header.uncompressed_size)?;
        Ok(Some((header, deserialize_batch(&final_data)?)))
      }
    }
  }

  /// Efficiently scans headers to find the frame containing `target_id`.
  /// positions the cursor at that frame so `next_batch` will read it.
  pub fn seek_to_frame(&mut self, target_id: u64) -> Result<()> {
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(0))?;
        loop {
          // Peek at the header
          // We need to read it to know size, but if it's wrong, we skip payload.
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(e) if matches!(e, Error::Io(ref io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof) => {
              // EOF reached without finding target. Leave cursor at EOF.
              return Ok(());
            }
            Err(e) => return Err(e),
          };

          let frame_end_id = header.start_id + header.entry_count as u64;

          if target_id >= header.start_id && target_id < frame_end_id {
            // Found it!
            // Rewind back to the start of this header so next_batch reads it.
            reader.seek(SeekFrom::Current(-(FrameHeader::SIZE as i64)))?;
            return Ok(());
          }

          // Skip payload
          reader.seek(SeekFrom::Current(header.disk_size as i64))?;
        }
      }
      Self::Mmap(mmap, cursor) => {
        *cursor = 0;
        let len = mmap.len();
        while *cursor + FrameHeader::SIZE <= len {
          let current_pos = *cursor;
          let mut header_slice = &mmap[current_pos..];
          let header = FrameHeader::read(&mut header_slice)?;

          let frame_end_id = header.start_id + header.entry_count as u64;
          let next_pos = current_pos + FrameHeader::SIZE + header.disk_size as usize;

          if target_id >= header.start_id && target_id < frame_end_id {
            // Found it. Cursor is already at the start of this frame (current_pos).
            // Wait, we modified cursor in the loop? No, we used current_pos.
            *cursor = current_pos;
            return Ok(());
          }

          *cursor = next_pos;
        }
        Ok(())
      }
    }
  }

  /// Efficiently scans the segment headers to calculate the total number of entries.
  pub fn recover_scan(&mut self) -> Result<u64> {
    match self {
      Self::Io(reader) => {
        let mut total_entries = 0;
        reader.seek(SeekFrom::Start(0))?;
        loop {
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(e) => {
              // If we hit EOF, we are done.
              if let Error::Io(ref io_err) = e {
                if io_err.kind() == io::ErrorKind::UnexpectedEof {
                  break;
                }
              }
              // If we hit Invalid Magic (e.g. zeros or garbage), stop.
              // Do not error; allow recovery to proceed.
              // Ideally we check specifically for the "Invalid Frame Magic" string or type,
              // but for now, any corruption at the frame boundary ends the scan.
              break;
            }
          };
          total_entries += header.entry_count as u64;
          reader.seek(SeekFrom::Current(header.disk_size as i64))?;
        }
        Ok(total_entries)
      }
      Self::Mmap(mmap, _) => {
        let mut cursor = 0;
        let mut total_entries = 0;
        let len = mmap.len();
        while cursor + FrameHeader::SIZE <= len {
          let mut header_slice = &mmap[cursor..];
          // Same logic for Mmap: if header read fails (bad magic), we stop.
          let header = match FrameHeader::read(&mut header_slice) {
            Ok(h) => h,
            Err(_) => break,
          };

          let payload_end = cursor + FrameHeader::SIZE + header.disk_size as usize;
          if payload_end > len {
            break;
          }
          total_entries += header.entry_count as u64;
          cursor = payload_end;
        }
        Ok(total_entries)
      }
    }
  }

  /// Efficiently scans the segment headers to find the frame containing the target ID.
  /// Does NOT read or decompress the payload.
  pub fn find_frame(&mut self, target_id: u64) -> Result<Option<FrameLocation>> {
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(0))?;
        let mut offset = 0;
        loop {
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(e) => {
              if let Error::Io(ref io_err) = e {
                if io_err.kind() == io::ErrorKind::UnexpectedEof {
                  return Ok(None);
                }
              }
              return Err(e);
            }
          };

          let frame_end_id = header.start_id + header.entry_count as u64;

          if target_id >= header.start_id && target_id < frame_end_id {
            return Ok(Some(FrameLocation { offset, header }));
          }

          let increment = FrameHeader::SIZE as u64 + header.disk_size as u64;
          offset += increment;

          // Catch EOF during seek (payload truncation)
          if let Err(e) = reader.seek(SeekFrom::Current(header.disk_size as i64)) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
              return Ok(None);
            }
            return Err(Error::Io(e));
          }
        }
      }
      Self::Mmap(mmap, _) => {
        let mut cursor = 0;
        let len = mmap.len();
        while cursor + FrameHeader::SIZE <= len {
          let current_offset = cursor as u64;
          let mut header_slice = &mmap[cursor..];
          let header = FrameHeader::read(&mut header_slice)?;

          let frame_end_id = header.start_id + header.entry_count as u64;
          let payload_size = header.disk_size as usize;

          if target_id >= header.start_id && target_id < frame_end_id {
            return Ok(Some(FrameLocation {
              offset: current_offset,
              header,
            }));
          }

          cursor += FrameHeader::SIZE + payload_size;
        }
        Ok(None)
      }
    }
  }

  /// Reads, validates, and decompresses a frame at a specific location.
  pub fn read_at(&mut self, loc: &FrameLocation) -> Result<Vec<Vec<u8>>> {
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(loc.offset + FrameHeader::SIZE as u64))?;
        let mut payload = vec![0u8; loc.header.disk_size as usize];
        reader.read_exact(&mut payload)?;

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          &payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }

        let final_data = Self::decompress(loc.header.frame_type, &payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
      Self::Mmap(mmap, _) => {
        let start = loc.offset as usize + FrameHeader::SIZE;
        let end = start + loc.header.disk_size as usize;
        if end > mmap.len() {
          return Err(Error::Corruption("Frame payload truncated in mmap".into()));
        }
        let payload = &mmap[start..end];

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }

        let final_data = Self::decompress(loc.header.frame_type, payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
    }
  }

  fn decompress(ft: FrameType, data: &[u8], _size: u32) -> Result<std::borrow::Cow<'_, [u8]>> {
    match ft {
      FrameType::Raw => Ok(std::borrow::Cow::Borrowed(data)),
      FrameType::Lz4 => {
        #[cfg(feature = "compression")]
        {
          let mut decoder = FrameDecoder::new(data);
          let mut out = Vec::with_capacity(_size as usize);
          decoder.read_to_end(&mut out).map_err(Error::Io)?;
          Ok(std::borrow::Cow::Owned(out))
        }
        #[cfg(not(feature = "compression"))]
        {
          Err(Error::Config("LZ4 frame found but compression feature disabled".into()))
        }
      }
    }
  }
}
