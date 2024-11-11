use zstd::stream::raw::Operation as _;

use crate::{
    buffer::Buffer,
    table::{ZstdFrame, ZstdFrameSize, ZstdSeekTable},
};

pub struct ZstdFramedDecoder<'dict> {
    decoder: zstd::stream::raw::Decoder<'dict>,
    table: ZstdSeekTable,
    current_frame: ZstdFrame,
    decoded_size: ZstdFrameSize,
}

impl<'dict> ZstdFramedDecoder<'dict> {
    pub fn new(decoder: zstd::stream::raw::Decoder<'dict>, table: ZstdSeekTable) -> Self {
        let current_frame = table.first_frame().unwrap_or_else(|| ZstdFrame {
            compressed_pos: 0,
            decompressed_pos: 0,
            index: 0,
            size: ZstdFrameSize::default(),
        });

        Self {
            decoder,
            table,
            current_frame,
            decoded_size: ZstdFrameSize::default(),
        }
    }

    /// Decode some data, writing the decoded result to `buffer`. Returns
    /// `Complete(_)` with the length of data that was conusmed from `data`.
    pub fn decode(&mut self, data: &[u8], buffer: &mut impl Buffer) -> std::io::Result<usize> {
        // Decode the data
        let mut in_buffer = zstd::stream::raw::InBuffer::around(data);
        let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
            self.decoder.run(&mut in_buffer, out_buffer)
        });

        // Update the decoded size of the current frame
        self.decoded_size.add_sizes(in_buffer.pos(), written);

        // Update the current frame's size if we've exceeded the known
        // size for the current frame. This will be used to update the table
        // if we have a better idea of the frame's actual size
        if self.decoded_size.compressed_size >= self.current_frame.size.compressed_size {
            self.current_frame.size = self.decoded_size;
        }

        let hint = result?;
        if hint == 0 {
            // If zstd returned a hint of 0, that means it just finished
            // decoding the current frame.

            // We know for sure the current frame's size, so update it
            let mut finished_frame = self.current_frame;
            finished_frame.size = self.decoded_size;

            // Insert the finished frame in the table
            self.table.insert(finished_frame);

            // Start decoding a new frame
            self.current_frame = ZstdFrame {
                compressed_pos: finished_frame.compressed_end(),
                decompressed_pos: finished_frame.decompressed_end(),
                index: finished_frame.index + 1,
                size: ZstdFrameSize::default(),
            };
            self.decoded_size = ZstdFrameSize::default();
        }

        // Return however many bytes the underlying zstd decoder consumed
        Ok(in_buffer.pos())
    }

    /// Determine the best way to reach the deired position within the
    /// decompressed stream based on the current decoder state
    pub fn prepare_seek_to_decompressed_pos(&self, pos: u64) -> ZstdSeek {
        // Determine which frame we should be in to reach the decompressed
        // position
        let target_frame = if self.current_frame.decompressed_range().contains(&pos) {
            // The current frame contains the position, so we want to be
            // in the current frame
            self.current_frame
        } else if let Some(frame) = self.table.find_by_decompressed_pos(pos) {
            // If the table has a frame containing the position, return it
            frame
        } else {
            // Otherwise, we should just go to the last frame we know about

            let last_frame = self.table.last_frame();
            match last_frame {
                Some(frame) if frame.index > self.current_frame.index => {
                    // The last frame in the table is beyond our current frame,
                    // so use the last frame from the table
                    frame
                }
                _ => {
                    // The decoder's frame matches or is beyond the last
                    // frame from the table, so our frame is the last frame
                    // we know of
                    self.current_frame
                }
            }
        };
        let decompress_len = pos - target_frame.decompressed_pos;
        let decoded_decompressed_size = self.decoded_size.decompressed_size;

        if target_frame.index == self.current_frame.index
            && decoded_decompressed_size <= decompress_len
        {
            // The decoder is currently decoding the target frame, and
            // the target position is at or ahead of the decoder's position!
            // That means we don't need to seek, we just need to keep decoding
            // until we reach the target position
            ZstdSeek {
                seek_to_frame_start: None,
                decompress_len: decompress_len - decoded_decompressed_size,
            }
        } else {
            // We need to seek to start of the target frame, then decompress
            // until we reach the target position
            ZstdSeek {
                seek_to_frame_start: Some(target_frame),
                decompress_len,
            }
        }
    }

    /// Indicates that the caller has seeked the underlying stream to the
    /// start of `frame`. The underlying decoder will be reset.
    pub fn seeked_to_frame(&mut self, frame: ZstdFrame) -> std::io::Result<()> {
        self.decoder.reinit()?;
        self.current_frame = frame;
        self.decoded_size = ZstdFrameSize::default();
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ZstdSeek {
    pub seek_to_frame_start: Option<ZstdFrame>,
    pub decompress_len: u64,
}
