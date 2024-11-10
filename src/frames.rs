#[derive(Debug)]
pub struct ZstdFrameTable {
    frames: Vec<ZstdFrame>,
}

impl ZstdFrameTable {
    pub fn empty() -> Self {
        Self { frames: vec![] }
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
pub(crate) struct ZstdFrame {
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

    pub(crate) fn decompressed_range(&self) -> std::ops::Range<u64> {
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
