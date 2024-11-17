use crate::encoder::{ZstdFramedEncoder, ZstdFramedEncoderSeekTableConfig};

pin_project_lite::pin_project! {
    /// A writer that writes a compressed zstd stream to the underlying writer. Works as either a `tokio` or `futures` writer if
    /// their respective features are enabled.
    ///
    /// The underlying writer `W` should implement the following traits:
    ///
    /// - `tokio`
    ///   - [`tokio::io::AsyncWrite`] (required for [`tokio::io::AsyncWrite`] impl)
    /// - `futures`
    ///   - [`futures::AsyncWrite`] (required for [`futures::AsyncWrite`] impl)
    ///
    /// For sync I/O support, see [`crate::ZstdWriter`].
    ///
    /// ## Construction
    ///
    /// Create a builder using [`AsyncZstdWriter::builder`]. See [`ZstdWriterBuilder`]
    /// for builder options. Call [`ZstdWriterBuilder::build`] to build the
    /// [`AsyncZstdWriter`] instance.
    ///
    /// ```
    /// # #[cfg(feature = "tokio")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use tokio::io::AsyncWriteExt as _;
    /// # let compressed_file = vec![];
    /// // Tokio example
    /// let mut writer = zstd_framed::AsyncZstdWriter::builder(compressed_file)
    ///     .with_compression_level(3) // Set custom compression level
    ///     .with_seek_table(1024 * 1024) // Write zstd seekable format table
    ///     .build()?;
    ///
    /// // ...
    ///
    /// writer.shutdown().await?; // Shut down the writer
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "tokio"))]
    /// # fn main() { }
    /// ```
    ///
    /// ```
    /// # #[cfg(feature = "futures")]
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use futures::io::AsyncWriteExt as _;
    /// # futures::executor::block_on(async {
    /// # let compressed_file = vec![];
    /// // futures example
    /// let mut writer = zstd_framed::AsyncZstdWriter::builder(compressed_file)
    ///     .with_compression_level(3) // Set custom compression level
    ///     .with_seek_table(1024 * 1024) // Write zstd seekable format table
    ///     .build()?;
    ///
    /// // ...
    ///
    /// writer.close().await?; // Close the writer
    /// # Ok(())
    /// # })
    /// # }
    /// # #[cfg(not(feature = "futures"))]
    /// # fn main() { }
    /// ```
    ///
    /// ## Writing multiple frames
    ///
    /// To allow for efficient seeking (e.g. when using [`ZstdReaderBuilder::with_seek_table`](crate::async_reader::ZstdReaderBuilder::with_seek_table)),
    /// you can write multiple zstd frames to the underlying writer. If the
    /// [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table) option is
    /// given during construction, multiple frames will be created automatically
    /// to fit within the given `max_frame_size`.
    ///
    /// Alternatively, you can use [`AsyncZstdWriter::finish_frame()`] to explicitly
    /// split the underlying stream into multiple frames. [`.finish_frame()`](ZstdWriter::finish_frame)
    /// can be used even when not using the [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)
    /// option (but note the seek table will only be written when using
    /// [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)).
    ///
    /// ## Clean shutdown
    ///
    /// To ensure the writer shuts down cleanly (including flushing any in-memory
    /// buffers and writing the seek table if enabled with [`.with_seek_table()`](ZstdWriterBuilder::with_seek_table)),
    /// make sure to call the Tokio [`.shutdown()`](tokio::io::AsyncWriteExt::shutdown)
    /// method or the or futures [`.close()`](`futures::io::AsyncWriteExt::close`) method!
    pub struct AsyncZstdWriter<'dict, W> {
        #[pin]
        writer: W,
        encoder: ZstdFramedEncoder<'dict>,
        buffer: crate::buffer::FixedBuffer<Vec<u8>> ,
    }
}

impl<W> AsyncZstdWriter<'_, W> {
    pub fn builder(writer: W) -> ZstdWriterBuilder<W> {
        ZstdWriterBuilder::new(writer)
    }

    pub fn finish_frame(&mut self) -> std::io::Result<()> {
        self.encoder.finish_frame(&mut self.buffer)?;

        Ok(())
    }

    /// Write all uncommitted buffered data to the underlying writer. After
    /// returning `Ok(_)``, `self.buffer` will be empty.
    #[cfg(feature = "tokio")]
    fn flush_uncommitted_tokio(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>>
    where
        W: tokio::io::AsyncWrite,
    {
        use crate::buffer::Buffer as _;

        let mut this = self.project();

        loop {
            // Get the uncommitted data to write
            let uncommitted = this.buffer.uncommitted();
            if uncommitted.is_empty() {
                // If there's no uncommitted data, we're done
                return std::task::Poll::Ready(Ok(()));
            }

            // Write the data to the underlying writer, and record it
            // as committed
            let committed = ready!(this.writer.as_mut().poll_write(cx, uncommitted))?;
            this.buffer.commit(committed);

            if committed == 0 {
                // The underlying reader didn't accept any more of our data

                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                )));
            }
        }
    }

    /// Write all uncommitted buffered data to the underlying writer. After
    /// returning `Ok(_)`, `self.buffer` will be empty.
    #[cfg(feature = "futures")]
    fn flush_uncommitted_futures(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>>
    where
        W: futures::AsyncWrite,
    {
        use crate::buffer::Buffer as _;

        let mut this = self.project();

        loop {
            // Get the uncommitted data to write
            let uncommitted = this.buffer.uncommitted();
            if uncommitted.is_empty() {
                // If there's no uncommitted data, we're done
                return std::task::Poll::Ready(Ok(()));
            }

            // Write the data to the underlying writer, and record it
            // as committed
            let committed = ready!(this.writer.as_mut().poll_write(cx, uncommitted))?;
            this.buffer.commit(committed);

            if committed == 0 {
                // The underlying reader didn't accept any more of our data

                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                )));
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<W> tokio::io::AsyncWrite for AsyncZstdWriter<'_, W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            // Write all buffered data
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            // Encode the newly-written data
            let outcome = this.encoder.encode(data, this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // The encoder has more to do before data can be encoded
                }
                crate::ZstdOutcome::Complete(consumed) => {
                    // We've now encoded some data to the buffer, so we're done
                    return std::task::Poll::Ready(Ok(consumed));
                }
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            // Write all buffered data
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            // Flush any data from the encoder to the interal buffer
            let outcome = this.encoder.flush(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // zstd still has more data to flush, so loop again
                }
                crate::ZstdOutcome::Complete(_) => {
                    // No more data from the encoder
                    break;
                }
            }
        }

        // Write any newly buffered data from the encoder
        ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

        // Flush the underlying writer
        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            // Flush any uncommitted data
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            // Shut down the encoder
            let outcome = this.encoder.shutdown(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // Encoder still has more to write, so keep looping
                }
                crate::ZstdOutcome::Complete(_) => {
                    // Encoder has nothing else to do, so we're done
                    break;
                }
            }
        }

        // Flush any final data from the encoder
        ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

        // Shut down the underlying writer
        let this = self.project();
        this.writer.poll_shutdown(cx)
    }
}

#[cfg(feature = "futures")]
impl<W> futures::AsyncWrite for AsyncZstdWriter<'_, W>
where
    W: futures::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            // Write all buffered data
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            // Encode the newly-written data
            let outcome = this.encoder.encode(data, this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // The encoder has more to do before data can be encoded
                }
                crate::ZstdOutcome::Complete(consumed) => {
                    // We've now encoded some data to the buffer, so we're done
                    return std::task::Poll::Ready(Ok(consumed));
                }
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            // Write all buffered data
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            // Flush any data from the encoder to the interal buffer
            let outcome = this.encoder.flush(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // zstd still has more data to flush, so loop again
                }
                crate::ZstdOutcome::Complete(_) => {
                    // No more data from the encoder
                    break;
                }
            }
        }

        // Write any newly buffered data from the encoder
        ready!(self.as_mut().flush_uncommitted_futures(cx))?;

        // Flush the underlying writer
        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            // Flush any uncommitted data
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            // Shut down the encoder
            let outcome = this.encoder.shutdown(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {
                    // Encoder still has more to write, so keep looping
                }
                crate::ZstdOutcome::Complete(_) => {
                    // Encoder has nothing else to do, so we're done
                    break;
                }
            }
        }

        // Flush any final data from the encoder
        ready!(self.as_mut().flush_uncommitted_futures(cx))?;

        // Close the underlying writer
        let this = self.project();
        this.writer.poll_close(cx)
    }
}

/// A builder that builds an [`AsyncZstdWriter`] from the provided writer.
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
    /// the writer is cleanly shut down, a final frame containing a seek
    /// table will be written to the end of the writer. This seek table can
    /// be used to efficiently seek through the file, such as by using
    /// [crate::table::read_seek_table] (or async equivalent) along with
    /// [`ZstdReaderBuilder::with_seek_table`](crate::async_reader::ZstdReaderBuilder::with_seek_table).
    ///
    /// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
    pub fn with_seek_table(mut self, max_frame_size: u32) -> Self {
        assert!(max_frame_size > 0, "max frame size must be greater than 0");

        self.seek_table_config = Some(ZstdFramedEncoderSeekTableConfig { max_frame_size });
        self
    }

    /// Build the writer.
    pub fn build(self) -> std::io::Result<AsyncZstdWriter<'static, W>> {
        let zstd_encoder = zstd::stream::raw::Encoder::new(self.compression_level)?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::CCtx::out_size()]);
        let encoder = ZstdFramedEncoder::new(zstd_encoder, self.seek_table_config);

        Ok(AsyncZstdWriter {
            writer: self.writer,
            encoder,
            buffer,
        })
    }
}
