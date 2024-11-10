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
