use zstd::stream::raw::Operation as _;

use crate::{
    buffer::Buffer,
    frames::{ZstdFrame, ZstdFrameSize, ZstdFrameTable},
};

pub struct ZstdFramedDecoder<'dict> {
    decoder: zstd::stream::raw::Decoder<'dict>,
    table: ZstdFrameTable,
    current_frame: ZstdFrame,
    decoded_size: ZstdFrameSize,
}

impl<'dict> ZstdFramedDecoder<'dict> {
    pub fn new(decoder: zstd::stream::raw::Decoder<'dict>, table: ZstdFrameTable) -> Self {
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

    pub fn decode(&mut self, data: &[u8], buffer: &mut impl Buffer) -> std::io::Result<usize> {
        let mut in_buffer = zstd::stream::raw::InBuffer::around(data);
        let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
            self.decoder.run(&mut in_buffer, out_buffer)
        });

        self.decoded_size.add_sizes(in_buffer.pos(), written);
        if self.decoded_size.compressed_size >= self.current_frame.size.compressed_size {
            self.current_frame.size = self.decoded_size;
        }

        let hint = result?;
        if hint == 0 {
            let mut finished_frame = self.current_frame;
            finished_frame.size = self.decoded_size;

            self.table.insert(finished_frame);
            self.current_frame = ZstdFrame {
                compressed_pos: finished_frame.compressed_end(),
                decompressed_pos: finished_frame.decompressed_end(),
                index: finished_frame.index + 1,
                size: ZstdFrameSize::default(),
            };
            self.decoded_size = ZstdFrameSize::default();
        }

        Ok(in_buffer.pos())
    }

    pub fn prepare_seek_to_decompressed_pos(&self, pos: u64) -> ZstdSeek {
        let target_frame = if self.current_frame.decompressed_range().contains(&pos) {
            self.current_frame
        } else if let Some(frame) = self.table.find_by_decompressed_pos(pos) {
            frame
        } else {
            let last_frame = self.table.last_frame();
            match last_frame {
                Some(frame) if frame.index > self.current_frame.index => frame,
                _ => self.current_frame,
            }
        };
        let decompress_len = pos - target_frame.decompressed_pos;
        let decoded_decompressed_size = self.decoded_size.decompressed_size;

        if target_frame.index == self.current_frame.index
            && decoded_decompressed_size <= decompress_len
        {
            ZstdSeek {
                seek_to_frame_start: None,
                decompress_len: decompress_len - decoded_decompressed_size,
            }
        } else {
            ZstdSeek {
                seek_to_frame_start: Some(target_frame),
                decompress_len,
            }
        }
    }

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
