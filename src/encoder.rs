use zstd::stream::raw::Operation as _;

use crate::{
    buffer::Buffer,
    table::{ZstdFrame, ZstdFrameSize, ZstdSeekTable},
    ZstdOutcome,
};

pub struct ZstdFramedEncoder<'dict> {
    encoder: zstd::stream::raw::Encoder<'dict>,
    max_frame_size: Option<u32>,
    table: ZstdSeekTable,
    state: ZstdFramedEncoderState,
    write_table: bool,
}

impl<'dict> ZstdFramedEncoder<'dict> {
    pub fn new(
        encoder: zstd::stream::raw::Encoder<'dict>,
        seek_table_config: Option<ZstdFramedEncoderSeekTableConfig>,
    ) -> Self {
        let write_table;
        let max_frame_size;
        match seek_table_config {
            Some(config) => {
                write_table = true;
                max_frame_size = Some(config.max_frame_size);
            }
            None => {
                write_table = false;
                max_frame_size = None;
            }
        }

        Self {
            encoder,
            table: ZstdSeekTable::empty(),
            max_frame_size,
            write_table,
            state: ZstdFramedEncoderState::Encoding {
                current_frame: ZstdFrame {
                    compressed_pos: 0,
                    decompressed_pos: 0,
                    index: 0,
                    size: ZstdFrameSize::default(),
                },
            },
        }
    }

    fn prepare_frame(
        &mut self,
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<ZstdFrame>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { current_frame } => {
                Ok(ZstdOutcome::Complete(current_frame))
            }
            ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;

                if hint == 0 {
                    self.encoder.reinit()?;

                    self.table.insert(current_frame);

                    let current_frame = ZstdFrame {
                        compressed_pos: current_frame.compressed_end(),
                        decompressed_pos: current_frame.decompressed_end(),
                        index: current_frame.index + 1,
                        size: ZstdFrameSize::default(),
                    };
                    self.state = ZstdFramedEncoderState::Encoding { current_frame };
                    Ok(ZstdOutcome::Complete(current_frame))
                } else {
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { finished_frame } => {
                let current_frame = ZstdFrame {
                    compressed_pos: finished_frame.compressed_end(),
                    decompressed_pos: finished_frame.decompressed_end(),
                    index: finished_frame.index + 1,
                    size: ZstdFrameSize::default(),
                };

                self.encoder.reinit()?;
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

                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let remaining_bytes = result?;
                if remaining_bytes == 0 {
                    self.table.insert(current_frame);
                    self.state = ZstdFramedEncoderState::FinishedFrame {
                        finished_frame: current_frame,
                    };
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    Ok(ZstdOutcome::HasMore { remaining_bytes })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { .. }
            | ZstdFramedEncoderState::WritingTable(_) => Ok(ZstdOutcome::Complete(())),
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

        let data = match self.max_frame_size {
            Some(max_frame_size) => {
                let frame_remaining_bytes =
                    u64::from(max_frame_size).saturating_sub(current_frame.size.decompressed_size);
                let frame_remaining_bytes: usize = frame_remaining_bytes
                    .try_into()
                    .expect("could not cast remaining bytes to usize");

                if frame_remaining_bytes == 0 {
                    complete_ok!(self.finish_frame(buffer)?);
                    return Ok(ZstdOutcome::HasMore { remaining_bytes: 1 });
                }

                let data_len = data.len().min(frame_remaining_bytes);
                &data[..data_len]
            }
            None => data,
        };

        let mut in_buffer = zstd::stream::raw::InBuffer::around(data);
        let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
            self.encoder.run(&mut in_buffer, out_buffer)
        });

        current_frame.size.add_sizes(written, in_buffer.pos());
        self.state = ZstdFramedEncoderState::Encoding { current_frame };

        let hint = result?;
        if hint == 0 {
            self.state = ZstdFramedEncoderState::FinishedFrame {
                finished_frame: current_frame,
            };
        }

        Ok(ZstdOutcome::Complete(in_buffer.pos()))
    }

    pub fn flush(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { mut current_frame } => {
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.flush(out_buffer)
                });

                current_frame.size.add_sizes(written, 0);
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

                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;
                if hint == 0 {
                    self.table.insert(current_frame);
                    self.state = ZstdFramedEncoderState::FinishedFrame {
                        finished_frame: current_frame,
                    };
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { .. } => Ok(ZstdOutcome::Complete(())),
            ZstdFramedEncoderState::WritingTable(ref mut writer) => {
                writer.write_table(&self.table, buffer)
            }
        }
    }

    pub fn shutdown(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        loop {
            match self.state {
                ZstdFramedEncoderState::Encoding { .. }
                | ZstdFramedEncoderState::FinishingFrame { .. } => {
                    complete_ok!(self.finish_frame(buffer)?);
                }
                ZstdFramedEncoderState::FinishedFrame { .. } => {
                    if self.write_table {
                        self.state =
                            ZstdFramedEncoderState::WritingTable(ZstdSeekTableWriter::new());
                    } else {
                        return Ok(ZstdOutcome::Complete(()));
                    }
                }
                ZstdFramedEncoderState::WritingTable(ref mut writer) => {
                    return writer.write_table(&self.table, buffer)
                }
            };
        }
    }
}

pub(crate) struct ZstdFramedEncoderSeekTableConfig {
    pub(crate) max_frame_size: u32,
}

enum ZstdFramedEncoderState {
    Encoding { current_frame: ZstdFrame },
    FinishingFrame { current_frame: ZstdFrame },
    FinishedFrame { finished_frame: ZstdFrame },
    WritingTable(ZstdSeekTableWriter),
}

struct ZstdSeekTableWriter {
    buffer: crate::buffer::FixedBuffer<[u8; 12]>,
    state: ZstdSeekTableWriterState,
}

impl ZstdSeekTableWriter {
    fn new() -> Self {
        Self {
            state: ZstdSeekTableWriterState::PrepareHeader,
            buffer: crate::buffer::FixedBuffer::new([0; 12]),
        }
    }

    fn write_table(
        &mut self,
        table: &ZstdSeekTable,
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<()>> {
        loop {
            complete_ok!(crate::buffer::move_buffer(&mut self.buffer, buffer));

            match self.state {
                ZstdSeekTableWriterState::PrepareHeader => {
                    let entry_size = 8;
                    let frame_size = table.num_frames().checked_mul(entry_size);
                    let frame_size = frame_size.and_then(|frame_size| frame_size.checked_add(9));
                    let frame_size =
                        frame_size.and_then(|frame_size| u32::try_from(frame_size).ok());
                    let frame_size = frame_size.expect("failed to convert frame size to u32");
                    let frame_size_bytes = frame_size.to_le_bytes();

                    crate::buffer::write_all_to_buffer(
                        &mut self.buffer,
                        &crate::SKIPPABLE_HEADER_MAGIC_BYTES[..],
                    );
                    crate::buffer::write_all_to_buffer(&mut self.buffer, &frame_size_bytes[..]);

                    self.state = ZstdSeekTableWriterState::WritingHeader;
                }
                ZstdSeekTableWriterState::WritingHeader => {
                    self.state = ZstdSeekTableWriterState::PreparingNextFrame { num_written: 0 };
                }
                ZstdSeekTableWriterState::PreparingNextFrame { num_written } => {
                    let frame = table.get(num_written);
                    match frame {
                        Some(frame) => {
                            let compressed_size: u32 = frame
                                .size
                                .compressed_size
                                .try_into()
                                .expect("could not convert frame compressed size to u32");
                            let decompressed_size: u32 = frame
                                .size
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

                            self.state = ZstdSeekTableWriterState::WritingFrame {
                                frame_index: num_written,
                            };
                        }
                        None => {
                            self.state = ZstdSeekTableWriterState::PreparingFooter;
                        }
                    }
                }
                ZstdSeekTableWriterState::WritingFrame { frame_index } => {
                    self.state = ZstdSeekTableWriterState::PreparingNextFrame {
                        num_written: frame_index + 1,
                    };
                }
                ZstdSeekTableWriterState::PreparingFooter => {
                    let num_frames: u32 = table
                        .num_frames()
                        .try_into()
                        .expect("failed to convert number of frames to u32");
                    let num_frames_bytes = num_frames.to_le_bytes();
                    let seek_table_descriptor = 0u8;

                    crate::buffer::write_all_to_buffer(&mut self.buffer, &num_frames_bytes[..]);
                    crate::buffer::write_all_to_buffer(&mut self.buffer, &[seek_table_descriptor]);
                    crate::buffer::write_all_to_buffer(
                        &mut self.buffer,
                        &crate::SEEKABLE_FOOTER_MAGIC_BYTES[..],
                    );

                    self.state = ZstdSeekTableWriterState::WritingFooter;
                }
                ZstdSeekTableWriterState::WritingFooter => {
                    self.state = ZstdSeekTableWriterState::Complete;
                }
                ZstdSeekTableWriterState::Complete => return Ok(ZstdOutcome::Complete(())),
            }
        }
    }
}

#[derive(Debug)]
enum ZstdSeekTableWriterState {
    PrepareHeader,
    WritingHeader,
    PreparingNextFrame { num_written: usize },
    WritingFrame { frame_index: usize },
    PreparingFooter,
    WritingFooter,
    Complete,
}
