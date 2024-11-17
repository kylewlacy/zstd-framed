use crate::{
    buffer::Buffer as _,
    encoder::{ZstdFramedEncoder, ZstdFramedEncoderSeekTableConfig},
    ZstdOutcome,
};

/// A writer that writes a compressed zstd stream to the underlying writer.
///
/// The underlying writer `W` must implement the following traits:
///
/// - [`std::io::Write`]
///
/// For async support, see [`crate::AsyncZstdWriter`].
///
/// ## Construction
///
/// Create a builder using [`ZstdWriter::builder`]. See [`ZstdWriterBuilder`]
/// for builder options. Call [`ZstdWriterBuilder::build`] to build the
/// [`ZstdWriter`] instance.
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let compressed_file = vec![];
/// let mut writer = zstd_framed::ZstdWriter::builder(compressed_file)
///     .with_compression_level(3) // Set custom compression level
///     .with_seek_table(1024 * 1024) // Write zstd seekable format table
///     .build()?;
///
/// // ...
///
/// writer.shutdown()?; // Optional, will shut down automatically on drop
/// # Ok(())
/// # }
/// ```
///
/// ## Writing multiple frames
///
/// To allow for efficient seeking (e.g. when using [`ZstdReaderBuilder::with_seek_table`](crate::reader::ZstdReaderBuilder::with_seek_table)),
/// you can write multiple zstd frames to the underlying writer. If the
/// [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table) option is
/// given during construction, multiple frames will be created automatically
/// to fit within the given `max_frame_size`.
///
/// Alternatively, you can use [`ZstdWriter::finish_frame()`] to explicitly
/// split the underlying stream into multiple frames. [`.finish_frame()`](ZstdWriter::finish_frame)
/// can be used even when not using the [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)
/// option (but note the seek table will only be written when using
/// [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)).
///
/// ## Clean shutdown
///
/// To ensure the writer shuts down cleanly (including flushing any in-memory
/// buffers and writing the seek table if enabled with [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)),
/// you can explicitly call the [`ZstdWriter::shutdown`] method. This
/// method will also be called automatically on drop, but errors will
/// be ignored.
pub struct ZstdWriter<'dict, W>
where
    W: std::io::Write,
{
    writer: W,
    encoder: ZstdFramedEncoder<'dict>,
    buffer: crate::buffer::FixedBuffer<Vec<u8>>,
}

impl<W> ZstdWriter<'_, W>
where
    W: std::io::Write,
{
    /// Create a new zstd writer that writes a compressed zstd stream
    /// to the underlying writer.
    pub fn builder(writer: W) -> ZstdWriterBuilder<W> {
        ZstdWriterBuilder::new(writer)
    }

    /// Explicitly finish the current zstd frame. If more data is written,
    /// a new frame will be started.
    ///
    /// When using [`ZstdWriterBuilder::with_seek_table`], the just-finished
    /// frame will be reflected in the resulting seek table.
    pub fn finish_frame(&mut self) -> std::io::Result<()> {
        self.encoder.finish_frame(&mut self.buffer)?;

        Ok(())
    }

    /// Cleanly shut down the zstd stream. This will flush internal buffers,
    /// finish writing any partially-written frames, and write the
    /// seek table when using [`ZstdWriterBuilder::with_seek_table`].
    ///
    /// This method will be called automatically on drop, although
    /// any errors will be ignored.
    pub fn shutdown(&mut self) -> std::io::Result<()> {
        loop {
            // Flush any uncommitted data
            self.flush_uncommitted()?;

            // Shut down the encoder
            let outcome = self.encoder.shutdown(&mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {
                    // Encoder still has more to write, so keep looping
                }
                ZstdOutcome::Complete(_) => {
                    // Encoder has nothing else to do, so we're done
                    break;
                }
            }
        }

        // Flush any final data from the encoder
        self.flush_uncommitted()?;

        // Flush the underlying writer for good measure
        self.writer.flush()?;

        Ok(())
    }

    /// Write all uncommitted buffered data to the underlying writer. After
    /// returning `Ok(_)`, `self.buffer` will be empty.
    fn flush_uncommitted(&mut self) -> std::io::Result<()> {
        loop {
            // Get the uncommitted data to write
            let uncommitted = self.buffer.uncommitted();
            if uncommitted.is_empty() {
                // If there's no uncommitted data, we're done
                return Ok(());
            }

            // Write the data to the underlying writer, and record it
            // as committed
            let committed = self.writer.write(uncommitted)?;
            self.buffer.commit(committed);

            if committed == 0 {
                // The underlying reader didn't accept any more of our data

                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                ));
            }
        }
    }
}

impl<W> std::io::Write for ZstdWriter<'_, W>
where
    W: std::io::Write,
{
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        loop {
            // Write all buffered data
            self.flush_uncommitted()?;

            // Encode the newly-written data
            let outcome = self.encoder.encode(data, &mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {
                    // The encoder has more to do before data can be encoded
                }
                ZstdOutcome::Complete(consumed) => {
                    // We've now encoded some data to the buffer, so we're done
                    return Ok(consumed);
                }
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            // Write all buffered data
            self.flush_uncommitted()?;

            // Flush any data from the encoder to the interal buffer
            let outcome = self.encoder.flush(&mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {
                    // zstd still has more data to flush, so loop again
                }
                ZstdOutcome::Complete(_) => {
                    // No more data from the encoder
                    break;
                }
            }
        }

        // Write any newly buffered data from the encoder
        self.flush_uncommitted()?;

        // Flush the underlying writer
        self.writer.flush()
    }
}

impl<W> Drop for ZstdWriter<'_, W>
where
    W: std::io::Write,
{
    fn drop(&mut self) {
        // Try to shut down the writer
        let _ = self.shutdown();
    }
}

/// A builder that builds a [`ZstdWriter`] from the provided writer.
pub struct ZstdWriterBuilder<W> {
    writer: W,
    compression_level: i32,
    seek_table_config: Option<ZstdFramedEncoderSeekTableConfig>,
}

impl<W> ZstdWriterBuilder<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            compression_level: 0,
            seek_table_config: None,
        }
    }

    /// Set the zstd compression level.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Write the stream using the [zstd seekable format].
    ///
    /// Once the current zstd frame reaches a decompressed size of
    /// `max_frame_size`, a new frame will automatically be started. When
    /// the writer is [shut down](ZstdWriter::shutdown), a final frame
    /// containing a seek table will be written to the end of the writer.
    /// This seek table can be used to efficiently seek through the file, such
    /// as by using [crate::table::read_seek_table] along with
    /// [`ZstdReaderBuilder::with_seek_table`](crate::reader::ZstdReaderBuilder::with_seek_table).
    ///
    /// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
    pub fn with_seek_table(mut self, max_frame_size: u32) -> Self {
        assert!(max_frame_size > 0, "max frame size must be greater than 0");

        self.seek_table_config = Some(ZstdFramedEncoderSeekTableConfig { max_frame_size });
        self
    }

    /// Build the writer.
    pub fn build(self) -> std::io::Result<ZstdWriter<'static, W>>
    where
        W: std::io::Write,
    {
        let zstd_encoder = zstd::stream::raw::Encoder::new(self.compression_level)?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::CCtx::out_size()]);
        let encoder = ZstdFramedEncoder::new(zstd_encoder, self.seek_table_config);

        Ok(ZstdWriter {
            writer: self.writer,
            encoder,
            buffer,
        })
    }
}
