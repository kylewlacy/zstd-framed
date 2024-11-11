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

    /// Ensure the encoder is in the [`Encoding`](ZstdFramedEncoderState::Encoding)
    /// state, and is ready to accept more data.
    fn prepare_frame(
        &mut self,
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<ZstdFrame>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { current_frame } => {
                // Already encoding, so we're all good

                Ok(ZstdOutcome::Complete(current_frame))
            }
            ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                // Currently finishing another frame

                // Call `.finish()` to flush more data for the frame to
                // finish
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                // Update the current frame size
                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;

                if hint == 0 {
                    // If zstd returned a hint of 0, that means it wrote
                    // all the data it needed to finish the frame

                    // Reset the encoder to start a new frame
                    self.encoder.reinit()?;

                    // Insert the newly-finished frame in the seek table
                    self.table.insert(current_frame);

                    // Start the next frame
                    let current_frame = ZstdFrame {
                        compressed_pos: current_frame.compressed_end(),
                        decompressed_pos: current_frame.decompressed_end(),
                        index: current_frame.index + 1,
                        size: ZstdFrameSize::default(),
                    };
                    self.state = ZstdFramedEncoderState::Encoding { current_frame };
                    Ok(ZstdOutcome::Complete(current_frame))
                } else {
                    // Otherwise, zstd has more data to write

                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { finished_frame } => {
                // We're between frames, so we need to start a new frame

                // Prepare the next frame
                let current_frame = ZstdFrame {
                    compressed_pos: finished_frame.compressed_end(),
                    decompressed_pos: finished_frame.decompressed_end(),
                    index: finished_frame.index + 1,
                    size: ZstdFrameSize::default(),
                };

                // Reset the encoder to start a new frame, then we're done
                self.encoder.reinit()?;
                self.state = ZstdFramedEncoderState::Encoding { current_frame };
                Ok(ZstdOutcome::Complete(current_frame))
            }
            ZstdFramedEncoderState::WritingTable(_) => {
                // We're currently writing the seek table, so we can't
                // start a new frame

                panic!("called .prepare_frame() but encoder is writing table")
            }
        }
    }

    /// Finish an in-progress frame. If a frame is currently being encoded,
    /// this will end the frame (which can either lead to a new frame
    /// or to the encoder shutting down cleanly).
    pub fn finish_frame(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { mut current_frame }
            | ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                // We're still writing a frame

                // Ensure we mark the current frame as "finishing" if it
                // was still encoding
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                // Call `.finish()` to flush more data for the frame to
                // finish
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                // Update the current frame size and encoder state
                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let remaining_bytes = result?;
                if remaining_bytes == 0 {
                    // If zstd returned a hint of 0, that means it wrote
                    // all the data it needed to finish the frame

                    // Insert the newly-finished frame in the seek table
                    self.table.insert(current_frame);

                    // Transition to indicate we're in between frames
                    self.state = ZstdFramedEncoderState::FinishedFrame {
                        finished_frame: current_frame,
                    };
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    // Otherwise, there's still more data before the frame
                    // is finished
                    Ok(ZstdOutcome::HasMore { remaining_bytes })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { .. }
            | ZstdFramedEncoderState::WritingTable(_) => {
                // We're not currently writing a frame!
                Ok(ZstdOutcome::Complete(()))
            }
        }
    }

    /// Encode some data, writing the encoded result to `buffer`. Returns
    /// `Complete(_)` with the length of data that was conusmed from `data`.
    pub fn encode(
        &mut self,
        data: &[u8],
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<usize>> {
        if let ZstdFramedEncoderState::WritingTable(_) = &self.state {
            // If we're writing the table, then we can't accept any more data
            return Ok(ZstdOutcome::Complete(0));
        }

        // Prepare the current frame
        let mut current_frame = complete_ok!(self.prepare_frame(buffer)?);

        // Slice data so that the current frame fits within the max frame size
        let data = match self.max_frame_size {
            Some(max_frame_size) => {
                let frame_remaining_bytes =
                    u64::from(max_frame_size).saturating_sub(current_frame.size.decompressed_size);
                let frame_remaining_bytes: usize = frame_remaining_bytes
                    .try_into()
                    .expect("could not cast remaining bytes to usize");

                if frame_remaining_bytes == 0 {
                    // No space left in the current frame! Finish the
                    // current frame instead
                    complete_ok!(self.finish_frame(buffer)?);

                    // Finishing the frame may have consumed some space
                    // from the buffer, so tell the caller that there's
                    // more work to do before we proceed with encoding
                    return Ok(ZstdOutcome::HasMore { remaining_bytes: 1 });
                }

                let data_len = data.len().min(frame_remaining_bytes);
                &data[..data_len]
            }
            None => data,
        };

        // Encode the data
        let mut in_buffer = zstd::stream::raw::InBuffer::around(data);
        let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
            self.encoder.run(&mut in_buffer, out_buffer)
        });

        // Update the current frame size and encoder state
        current_frame.size.add_sizes(written, in_buffer.pos());
        self.state = ZstdFramedEncoderState::Encoding { current_frame };

        let hint = result?;
        if hint == 0 {
            // If zstd returned a hint of 0, that means it just finished
            // a frame, so transition to the "finished frame" state

            self.state = ZstdFramedEncoderState::FinishedFrame {
                finished_frame: current_frame,
            };
        }

        // Return however many bytes the underlying zstd encoder consumed
        Ok(ZstdOutcome::Complete(in_buffer.pos()))
    }

    /// Flush any buffered data from the underlying encoder into `buffer`.
    pub fn flush(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        match self.state {
            ZstdFramedEncoderState::Encoding { mut current_frame } => {
                // We're in the middle of a frame

                // Flush the encoder
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.flush(out_buffer)
                });

                // Update the frame size and current state
                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::Encoding { current_frame };

                let hint = result?;
                if hint == 0 {
                    // No more data to flush, so flushing is finished
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    // There's still more data to flush
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishingFrame { mut current_frame } => {
                // We're finishing a frame. Finish encoding the frame to
                // flush

                // Call `.finish()` to flush more data for the frame to
                // finish
                let (result, written) = crate::buffer::with_zstd_out_buffer(buffer, |out_buffer| {
                    self.encoder.finish(out_buffer, false)
                });

                // Update the current frame size and encoder state
                current_frame.size.add_sizes(written, 0);
                self.state = ZstdFramedEncoderState::FinishingFrame { current_frame };

                let hint = result?;
                if hint == 0 {
                    // If zstd returned a hint of 0, that means it wrote
                    // all the data it needed to finish the frame

                    // Insert the newly-finished frame in the seek table
                    self.table.insert(current_frame);

                    // Transition to indicate we're in between frames
                    self.state = ZstdFramedEncoderState::FinishedFrame {
                        finished_frame: current_frame,
                    };
                    Ok(ZstdOutcome::Complete(()))
                } else {
                    // Otherwise, there's still more data before the frame
                    // is finished
                    Ok(ZstdOutcome::HasMore {
                        remaining_bytes: hint,
                    })
                }
            }
            ZstdFramedEncoderState::FinishedFrame { .. } => {
                // We're in between frames, so there's nothing to flush
                Ok(ZstdOutcome::Complete(()))
            }
            ZstdFramedEncoderState::WritingTable(ref mut writer) => {
                // We're in the process of writing the seek table, so
                // continue writing it
                writer.write_table(&self.table, buffer)
            }
        }
    }

    /// Indicate that the stream should shut down. This will finish any
    /// in-progress frames, then will write the seek table.
    pub fn shutdown(&mut self, buffer: &mut impl Buffer) -> std::io::Result<ZstdOutcome<()>> {
        loop {
            match self.state {
                ZstdFramedEncoderState::Encoding { .. }
                | ZstdFramedEncoderState::FinishingFrame { .. } => {
                    // Finish the current frame
                    complete_ok!(self.finish_frame(buffer)?);
                }
                ZstdFramedEncoderState::FinishedFrame { .. } => {
                    if self.write_table {
                        // If we should write the seek table, then
                        // switch to the "writing table" state
                        self.state =
                            ZstdFramedEncoderState::WritingTable(ZstdSeekTableWriter::new());
                    } else {
                        // Otherwise, we're done shutting down
                        return Ok(ZstdOutcome::Complete(()));
                    }
                }
                ZstdFramedEncoderState::WritingTable(ref mut writer) => {
                    // Continue writing the table
                    return writer.write_table(&self.table, buffer);
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

    /// Write the final zstd skippable frame containing the seek table,
    /// confirming to the [zstd seekable format].
    ///
    /// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
    fn write_table(
        &mut self,
        table: &ZstdSeekTable,
        buffer: &mut impl Buffer,
    ) -> std::io::Result<ZstdOutcome<()>> {
        // Each iteration will transfer data from our internal buffer into
        // `buffer`, then write another batch of data to our internal buffer.
        // This repeats until we've written the entire seek table, or until
        // we fill up `buffer`. This is effectively a hand-rolled coroutine
        loop {
            // Transfer any pending data from our internal buffer
            complete_ok!(crate::buffer::move_buffer(&mut self.buffer, buffer));

            match self.state {
                ZstdSeekTableWriterState::PrepareHeader => {
                    // Prepare the header by writing it to our internal buffer

                    let entry_size = 8;
                    let frame_size = table.num_frames().checked_mul(entry_size);
                    let frame_size = frame_size.and_then(|frame_size| frame_size.checked_add(9));
                    let frame_size =
                        frame_size.and_then(|frame_size| u32::try_from(frame_size).ok());
                    let frame_size = frame_size.expect("failed to convert frame size to u32");
                    let frame_size_bytes = frame_size.to_le_bytes();

                    // Write the skippable frame header magic (4 bytes)
                    // followed by the frame size (4 bytes)
                    crate::buffer::copy_all_from_slice(
                        &crate::SKIPPABLE_HEADER_MAGIC_BYTES[..],
                        &mut self.buffer,
                    );
                    crate::buffer::copy_all_from_slice(&frame_size_bytes[..], &mut self.buffer);

                    // The header has been written to the internal buffer
                    // so now we just wait until it's been flushed
                    self.state = ZstdSeekTableWriterState::WritingHeader;
                }
                ZstdSeekTableWriterState::WritingHeader => {
                    // Once the header's been flushed, prepare to write
                    // the first frame table entry
                    self.state = ZstdSeekTableWriterState::PreparingNextFrame { num_written: 0 };
                }
                ZstdSeekTableWriterState::PreparingNextFrame { num_written } => {
                    // We need to write the next frame to the internal buffer

                    // Get the frame to write
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

                            // Write the compressed size (4 bytes) followed
                            // by the decompressed size (4 bytes)
                            crate::buffer::copy_all_from_slice(
                                &compressed_size_bytes[..],
                                &mut self.buffer,
                            );
                            crate::buffer::copy_all_from_slice(
                                &decompressed_size_bytes[..],
                                &mut self.buffer,
                            );

                            // The frame entry has been written to the
                            // internal buffer, so wait until we flush it
                            // to start the next frame
                            self.state = ZstdSeekTableWriterState::WritingFrame {
                                frame_index: num_written,
                            };
                        }
                        None => {
                            // We've written all the frames, so get ready
                            // to write the footer
                            self.state = ZstdSeekTableWriterState::PreparingFooter;
                        }
                    }
                }
                ZstdSeekTableWriterState::WritingFrame { frame_index } => {
                    // Once a frame table entry has been flushed, prepare
                    // to write the the next frame table entry
                    self.state = ZstdSeekTableWriterState::PreparingNextFrame {
                        num_written: frame_index + 1,
                    };
                }
                ZstdSeekTableWriterState::PreparingFooter => {
                    // All frames have been flsuhed, so write the footer
                    // to the internal buffer

                    let num_frames: u32 = table
                        .num_frames()
                        .try_into()
                        .expect("failed to convert number of frames to u32");
                    let num_frames_bytes = num_frames.to_le_bytes();
                    let seek_table_descriptor = 0u8;

                    // Write the number of frames (4 bytes), followed by
                    // the seek table descriptor (1 byte), then the
                    // magic bytes (4 bytes)
                    crate::buffer::copy_all_from_slice(&num_frames_bytes[..], &mut self.buffer);
                    crate::buffer::copy_all_from_slice(&[seek_table_descriptor], &mut self.buffer);
                    crate::buffer::copy_all_from_slice(
                        &crate::SEEKABLE_FOOTER_MAGIC_BYTES[..],
                        &mut self.buffer,
                    );

                    // The footer has been written to the internal buffer,
                    // so now we just wait  until it's been flushed
                    self.state = ZstdSeekTableWriterState::WritingFooter;
                }
                ZstdSeekTableWriterState::WritingFooter => {
                    // The footer's been written, so we're done!
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
