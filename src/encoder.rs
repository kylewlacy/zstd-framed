use zstd::stream::raw::Operation as _;

use crate::{buffer::Buffer, frames::ZstdFrameSize, ZstdOutcome};

pub struct ZstdFramedEncoder<'dict> {
    encoder: zstd::stream::raw::Encoder<'dict>,
    frame_size: u32,
    finished_frames: Vec<ZstdFrameSize>,
    state: ZstdFramedEncoderState,
}

impl<'dict> ZstdFramedEncoder<'dict> {
    pub fn new(encoder: zstd::stream::raw::Encoder<'dict>, frame_size: u32) -> Self {
        Self {
            encoder,
            frame_size,
            finished_frames: vec![],
            state: ZstdFramedEncoderState::Encoding {
                current_frame: ZstdFrameSize::default(),
            },
        }
    }

    fn prepare_frame(
        &mut self,
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<ZstdFrameSize>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { current_frame } => {
                Ok(ZstdOutcome::Complete(current_frame))
            }
            ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                current_frame.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;

                if hint == 0 {
                    self.encoder.reinit()?;

                    self.finished_frames.push(current_frame);

                    let current_frame = ZstdFrameSize::default();
                    self.state = ZstdFramedEncoderState::Encoding { current_frame };
                    Ok(ZstdOutcome::Complete(current_frame))
                } else {
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame => {
                self.encoder.reinit()?;
                let current_frame = ZstdFrameSize::default();
                self.state = ZstdFramedEncoderState::Encoding { current_frame };
                Ok(ZstdOutcome::Complete(current_frame))
            }
            ZstdFramedEncoderState::WritingTable(_) => {
                panic!("called .prepare_frame() but encoder is writing table")
            }
        }
    }

    pub fn finish_frame(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { mut current_frame }
            | ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                current_frame.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let remaining_bytes = result?;
                if remaining_bytes == 0 {
                    self.finished_frames.push(current_frame);
                    self.state = ZstdFramedEncoderState::FinishedFrame;
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    Ok(ZstdOutcome::HasMore { remaining_bytes })
                }
            }
            ZstdFramedEncoderState::FinishedFrame | ZstdFramedEncoderState::WritingTable(_) => {
                Ok(ZstdOutcome::Complete(()))
            }
        }
    }

    pub fn encode(
        &mut self,
        data: &[u8],
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<usize>> {
        if let ZstdFramedEncoderState::WritingTable(_) = &self.state {
            return Ok(ZstdOutcome::Complete(0));
        }

        let mut current_frame = complete_ok!(self.prepare_frame(buffer)?);

        let frame_remaining_bytes =
            u64::from(self.frame_size).saturating_sub(current_frame.decompressed_size);
        let frame_remaining_bytes: usize = frame_remaining_bytes
            .try_into()
            .expect("could not cast remaining bytes to usize");

        if frame_remaining_bytes == 0 {
            complete_ok!(self.finish_frame(buffer)?);
            return Ok(ZstdOutcome::HasMore { remaining_bytes: 1 });
        }

        let data_len = data.len().min(frame_remaining_bytes);
        let data = &data[..data_len];

        let mut in_buffer = zstd::stream::raw::InBuffer::around(data);
        let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
            self.encoder.run(&mut in_buffer, out_buffer)
        });

        current_frame.add_sizes(written, in_buffer.pos());
        self.state = ZstdFramedEncoderState::Encoding { current_frame };

        let hint = result?;
        if hint == 0 {
            self.state = ZstdFramedEncoderState::FinishedFrame;
        }

        Ok(ZstdOutcome::Complete(in_buffer.pos()))
    }

    pub fn flush(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { mut current_frame } => {
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.flush(out_buffer)
                });

                current_frame.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::Encoding { current_frame };

                let hint = result?;
                if hint == 0 {
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                current_frame.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;
                if hint == 0 {
                    self.finished_frames.push(current_frame);
                    self.state = ZstdFramedEncoderState::FinishedFrame;
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame => Ok(ZstdOutcome::Complete(())),
            ZstdFramedEncoderState::WritingTable(ref mut writer) => {
                writer.write_table(&self.finished_frames, buffer)
            }
        }
    }

    pub fn shutdown(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        let writer = loop {
            match self.state {
                ZstdFramedEncoderState::Encoding { .. }
                | ZstdFramedEncoderState::FinishingFrame { .. } => {
                    complete_ok!(self.finish_frame(buffer)?);
                }
                ZstdFramedEncoderState::FinishedFrame => {
                    self.state =
                        ZstdFramedEncoderState::WritingTable(ZstdSeekableTableWriter::new());
                }
                ZstdFramedEncoderState::WritingTable(ref mut writer) => break writer,
            };
        };

        writer.write_table(&self.finished_frames, buffer)
    }
}

enum ZstdFramedEncoderState {
    Encoding { current_frame: ZstdFrameSize },
    FinishingFrame { current_frame: ZstdFrameSize },
    FinishedFrame,
    WritingTable(ZstdSeekableTableWriter),
}

struct ZstdSeekableTableWriter {
    buffer: crate::buffer::FixedBuffer<[u8; 12]>,
    state: ZstdSeekableTableWriterState,
}

impl ZstdSeekableTableWriter {
    fn new() -> Self {
        Self {
            state: ZstdSeekableTableWriterState::PrepareHeader,
            buffer: crate::buffer::FixedBuffer::new([0; 12]),
        }
    }

    fn write_table(
        &mut self,
        frames: &[ZstdFrameSize],
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<()>> {
        loop {
            complete_ok!(crate::buffer::move_buffer(&mut self.buffer, buffer));

            match self.state {
                ZstdSeekableTableWriterState::PrepareHeader => {
                    let magic_bytes = 0x184D2A5E_u32.to_le_bytes();

                    let entry_size = 8;
                    let frame_size = frames.len().checked_mul(entry_size);
                    let frame_size = frame_size.and_then(|frame_size| frame_size.checked_add(9));
                    let frame_size =
                        frame_size.and_then(|frame_size| u32::try_from(frame_size).ok());
                    let frame_size = frame_size.expect("failed to convert frame size to u32");
                    let frame_size_bytes = frame_size.to_le_bytes();

                    crate::buffer::write_all_to_buffer(&mut self.buffer, &magic_bytes[..]);
                    crate::buffer::write_all_to_buffer(&mut self.buffer, &frame_size_bytes[..]);

                    self.state = ZstdSeekableTableWriterState::WritingHeader;
                }
                ZstdSeekableTableWriterState::WritingHeader => {
                    self.state =
                        ZstdSeekableTableWriterState::PreparingNextFrame { num_written: 0 };
                }
                ZstdSeekableTableWriterState::PreparingNextFrame { num_written } => {
                    let frame = frames.get(num_written);
                    match frame {
                        Some(frame) => {
                            let compressed_size: u32 = frame
                                .compressed_size
                                .try_into()
                                .expect("could not convert frame compressed size to u32");
                            let decompressed_size: u32 = frame
                                .decompressed_size
                                .try_into()
                                .expect("could not convert frame decompressed size to u32");

                            let compressed_size_bytes = compressed_size.to_le_bytes();
                            let decompressed_size_bytes = decompressed_size.to_le_bytes();

                            crate::buffer::write_all_to_buffer(
                                &mut self.buffer,
                                &compressed_size_bytes[..],
                            );
                            crate::buffer::write_all_to_buffer(
                                &mut self.buffer,
                                &decompressed_size_bytes[..],
                            );

                            self.state = ZstdSeekableTableWriterState::WritingFrame {
                                frame_index: num_written,
                            };
                        }
                        None => {
                            self.state = ZstdSeekableTableWriterState::PreparingFooter;
                        }
                    }
                }
                ZstdSeekableTableWriterState::WritingFrame { frame_index } => {
                    self.state = ZstdSeekableTableWriterState::PreparingNextFrame {
                        num_written: frame_index + 1,
                    };
                }
                ZstdSeekableTableWriterState::PreparingFooter => {
                    let num_frames: u32 = frames
                        .len()
                        .try_into()
                        .expect("failed to convert number of frames to u32");
                    let num_frames_bytes = num_frames.to_le_bytes();
                    let seek_table_descriptor = 0u8;
                    let seekable_magic_number_bytes = 0x8F92EAB1_u32.to_le_bytes();

                    crate::buffer::write_all_to_buffer(&mut self.buffer, &num_frames_bytes[..]);
                    crate::buffer::write_all_to_buffer(&mut self.buffer, &[seek_table_descriptor]);
                    crate::buffer::write_all_to_buffer(
                        &mut self.buffer,
                        &seekable_magic_number_bytes[..],
                    );

                    self.state = ZstdSeekableTableWriterState::WritingFooter;
                }
                ZstdSeekableTableWriterState::WritingFooter => {
                    self.state = ZstdSeekableTableWriterState::Complete;
                }
                ZstdSeekableTableWriterState::Complete => return Ok(ZstdOutcome::Complete(())),
            }
        }
    }
}

#[derive(Debug)]
enum ZstdSeekableTableWriterState {
    PrepareHeader,
    WritingHeader,
    PreparingNextFrame { num_written: usize },
    WritingFrame { frame_index: usize },
    PreparingFooter,
    WritingFooter,
    Complete,
}
