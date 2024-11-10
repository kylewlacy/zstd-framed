pub mod futures;
pub mod tokio;

#[derive(Debug)]
pub struct ZstdFrameTable {
    frames: Vec<ZstdFrame>,
}

impl ZstdFrameTable {
    pub fn empty() -> Self {
        Self { frames: vec![] }
    }

    pub fn num_frames(&self) -> usize {
        self.frames.len()
    }

    pub fn frames(&self) -> impl Iterator<Item = ZstdFrame> + '_ {
        self.frames.iter().copied()
    }

    pub(crate) fn first_frame(&self) -> Option<ZstdFrame> {
        self.frames.first().copied()
    }

    pub(crate) fn last_frame(&self) -> Option<ZstdFrame> {
        self.frames.last().copied()
    }

    pub(crate) fn find_by_decompressed_pos(&self, pos: u64) -> Option<ZstdFrame> {
        let index = self
            .frames
            .binary_search_by(|frame| {
                if pos < frame.decompressed_pos {
                    std::cmp::Ordering::Greater
                } else if pos >= frame.decompressed_pos + frame.size.decompressed_size {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()?;
        let frame = self.frames[index];
        Some(frame)
    }

    pub(crate) fn get(&self, index: usize) -> Option<ZstdFrame> {
        self.frames.get(index).copied()
    }

    pub(crate) fn insert(&mut self, frame: ZstdFrame) {
        let next_index = self.frames.len();

        assert!(next_index >= frame.index);

        if frame.index == next_index {
            self.frames.push(frame);
        } else if frame.index + 1 == next_index {
            self.frames[frame.index] = frame;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ZstdFrame {
    pub(crate) index: usize,
    pub(crate) compressed_pos: u64,
    pub(crate) decompressed_pos: u64,
    pub(crate) size: ZstdFrameSize,
}

impl ZstdFrame {
    pub(crate) fn compressed_end(&self) -> u64 {
        self.compressed_pos + self.size.compressed_size
    }

    pub(crate) fn decompressed_end(&self) -> u64 {
        self.decompressed_pos + self.size.decompressed_size
    }

    pub fn compressed_size(&self) -> u64 {
        self.size.compressed_size
    }

    pub fn decompressed_size(&self) -> u64 {
        self.size.decompressed_size
    }

    pub fn compressed_range(&self) -> std::ops::Range<u64> {
        self.compressed_pos..self.compressed_end()
    }

    pub fn decompressed_range(&self) -> std::ops::Range<u64> {
        self.decompressed_pos..self.decompressed_end()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ZstdFrameSize {
    pub(crate) compressed_size: u64,
    pub(crate) decompressed_size: u64,
}

impl ZstdFrameSize {
    pub(crate) fn add_sizes(&mut self, compressed_size: usize, decompressed_size: usize) {
        let compressed_written: u64 = compressed_size
            .try_into()
            .expect("failed to convert written bytes to u64");
        let decompressed_written: u64 = decompressed_size
            .try_into()
            .expect("failed to convert written bytes to u64");

        let compressed_size = self
            .compressed_size
            .checked_add(compressed_written)
            .expect("adding to compressed size overflowed");
        let decompressed_size = self
            .decompressed_size
            .checked_add(decompressed_written)
            .expect("adding to decompressed size overflowed");

        self.compressed_size = compressed_size;
        self.decompressed_size = decompressed_size;
    }
}

pub fn read_seek_table<R>(reader: &mut R) -> std::io::Result<Option<ZstdFrameTable>>
where
    R: std::io::Read + std::io::Seek,
{
    // Seek to the start of the zstd seek table footer
    reader.seek(std::io::SeekFrom::End(-9))?;

    // Read the footer fields: number of frames (4 bytes),
    // table descriptor (1 byte), and the magic number (4 bytes)
    let mut num_frames_bytes = [0; 4];
    reader.read_exact(&mut num_frames_bytes)?;

    let mut seek_table_descriptor_bytes = [0; 1];
    reader.read_exact(&mut seek_table_descriptor_bytes)?;

    let mut seekable_magic_number_bytes = [0; 4];
    reader.read_exact(&mut seekable_magic_number_bytes)?;

    // Return if the magic number doesn't match
    if seekable_magic_number_bytes != crate::SEEKABLE_FOOTER_MAGIC_BYTES {
        return Ok(None);
    }

    // Parse the number of frames
    let num_frames = u32::from_le_bytes(num_frames_bytes);

    // Validate the seek table descriptor
    let [seek_table_descriptor] = seek_table_descriptor_bytes;
    let has_checksum = seek_table_descriptor & 0b1000_0000 != 0;
    let is_reserved_valid = seek_table_descriptor & 0b0111_1100 == 0;

    if !is_reserved_valid {
        return Err(std::io::Error::other(
            "zstd seek table has unsupported descriptor",
        ));
    }

    // Determine the table entry size (8 bytes, or 12 bytes with checksums)
    let table_entry_size: u32 = if has_checksum { 12 } else { 8 };

    // Calculate the full size of the skippable frame containing the
    // seek table. This can't overflow for a valid seek table, since the
    // frame size is part of the frame header.
    let table_frame_size = table_entry_size
        .checked_mul(num_frames)
        .and_then(|size| size.checked_add(9))
        .ok_or_else(|| std::io::Error::other("zstd seek table size overflowed"))?;

    // Seek to the start of the skippable frame containing the seek table
    reader.seek_relative(-i64::from(table_frame_size) - 8)?;

    // Read the skippable frame magic number header: the
    // magic number (4 bytes) and the frame size (4 bytes)
    let mut skippable_magic_number_bytes = [0; 4];
    reader.read_exact(&mut skippable_magic_number_bytes)?;

    let mut actual_table_frame_size_bytes = [0; 4];
    reader.read_exact(&mut actual_table_frame_size_bytes)?;

    // Validate the skippable frame magic number and frame size
    if skippable_magic_number_bytes != crate::SKIPPABLE_HEADER_MAGIC_BYTES {
        return Err(std::io::Error::other(
            "zstd seek table has unsupported skippable frame magic number",
        ));
    }

    let actual_table_frame_size = u32::from_le_bytes(actual_table_frame_size_bytes);
    if actual_table_frame_size != table_frame_size {
        return Err(std::io::Error::other("zstd seek table size did not match"));
    }

    // Read each table entry
    let mut table = ZstdFrameTable::empty();
    let mut compressed_pos = 0;
    let mut decompressed_pos = 0;
    for frame_index in 0..num_frames {
        let frame_index = usize::try_from(frame_index).unwrap();

        // Read the compressed size
        let mut compressed_size_bytes = [0; 4];
        reader.read_exact(&mut compressed_size_bytes)?;
        let compressed_size = u32::from_le_bytes(compressed_size_bytes);

        // Read the decompressed size
        let mut decompressed_size_bytes = [0; 4];
        reader.read_exact(&mut decompressed_size_bytes)?;
        let decompressed_size = u32::from_le_bytes(decompressed_size_bytes);

        // Skip the checksum if present
        if has_checksum {
            reader.seek_relative(4)?;
        }

        let frame = ZstdFrame {
            compressed_pos,
            decompressed_pos,
            index: frame_index,
            size: ZstdFrameSize {
                compressed_size: compressed_size.into(),
                decompressed_size: decompressed_size.into(),
            },
        };
        table.insert(frame);

        compressed_pos += u64::from(compressed_size);
        decompressed_pos += u64::from(decompressed_size);
    }

    Ok(Some(table))
}
