use crate::{
    decoder::ZstdFramedDecoder,
    table::{ZstdFrame, ZstdSeekTable},
};

pin_project_lite::pin_project! {
    /// An reader that decompresses a zstd stream from an underlying
    /// async reader. Works as either a `tokio` or `futures` reader if
    /// their respective features are enabled.
    ///
    /// The underyling reader `R` should implement the following traits:
    ///
    /// - `tokio`
    ///   - [`tokio::io::AsyncBufRead`] (required for [`tokio::io::AsyncRead`] and [`tokio::io::AsyncBufRead`] impls)
    ///   - (Optional) [`tokio::io::AsyncSeek`] (used when calling [`.seekable()`](AsyncZstdReader::seekable))
    /// - `futures`
    ///   - [`futures::AsyncBufRead`] (required for [`futures::AsyncRead`] and [`futures::AsyncBufRead`] impls)
    ///   - (Optional) [`futures::AsyncSeek`] (used when calling [`.seekable()`](AsyncZstdReader::seekable))
    ///
    /// For sync I/O support, see [`crate::ZstdReader`].
    ///
    /// ## Construction
    ///
    /// Create a builder using [`AsyncZstdReader::builder_tokio`] (recommended
    /// for `tokio`) or [`AsyncZstdReader::builder_futures`] (recommended for
    /// `futures`); or use [`ZstdReader::builder_buffered`] to use a custom
    /// buffer for either. See [`ZstdReaderBuilder`] for build options. Call
    /// [`AsyncZstdReaderBuilder::build`] to build the
    /// [`AsyncZstdReader`] instance.
    ///
    /// ```
    /// # #[cfg(feature = "tokio")]
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let compressed_file: &[u8] = &[];
    /// // Tokio example
    /// let reader = zstd_framed::AsyncZstdReader::builder_tokio(compressed_file)
    ///     // .with_seek_table(table) // Provide a seek table if available
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "tokio"))]
    /// # fn main() { }
    /// ```
    ///
    /// ```
    /// # #[cfg(feature = "futures")]
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let compressed_file: &[u8] = &[];
    /// // futures example
    /// let reader = zstd_framed::AsyncZstdReader::builder_futures(compressed_file)
    ///     // .with_seek_table(table) // Provide a seek table if available
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "futures"))]
    /// # fn main() { }
    /// ```
    ///
    /// ## Buffering
    ///
    /// The decompressed zstd output is always buffered internally. Since the
    /// reader must also implement [`tokio::io::AsyncBufRead`] /
    /// [`futures::AsyncBufRead`], the compressed input must also be buffered.
    ///
    /// [`AsyncZstdReader::builder_tokio`] and
    /// [`AsyncZstdReader::builder_futures`] will wrap any reader implmenting
    /// [`tokio::io::AsyncRead`] or [`futures::AsyncRead`] (respectively)
    /// with a recommended buffer size for the input stream. For more control
    /// over how the input gets buffered, you can instead use
    /// [`AsyncZstdReader::builder_buffered`].
    ///
    /// ## Seeking
    ///
    /// If the underlying reader is seekable (i.e. it implements either
    /// [`tokio::io::AsyncSeek`] or [`futures::AsyncSeek`]), you can call
    /// [`.seekable()`](`AsyncZstdReader::seekable`) to convert it to a seekable reader. See
    /// [`AsyncZstdSeekableReader`] for notes and caveats about seeking.
    pub struct AsyncZstdReader<'dict, R> {
        #[pin]
        reader: R,
        decoder: ZstdFramedDecoder<'dict>,
        buffer: crate::buffer::FixedBuffer<Vec<u8>>,
        current_pos: u64,
    }
}

impl<'dict, R> AsyncZstdReader<'dict, R> {
    /// Create a new zstd reader that decompresses the zstd stream from
    /// the underlying Tokio reader. The provided reader will be wrapped
    /// with an appropriately-sized buffer.
    #[cfg(feature = "tokio")]
    pub fn builder_tokio(reader: R) -> ZstdReaderBuilder<tokio::io::BufReader<R>>
    where
        R: tokio::io::AsyncRead,
    {
        ZstdReaderBuilder::new_tokio(reader)
    }

    /// Create a new zstd reader that decompresses the zstd stream from
    /// the underlying `futures` reader. The provided reader will be
    /// wrapped with an appropriately-sized buffer.
    #[cfg(feature = "futures")]
    pub fn builder_futures(reader: R) -> ZstdReaderBuilder<futures::io::BufReader<R>>
    where
        R: futures::AsyncRead,
    {
        ZstdReaderBuilder::new_futures(reader)
    }

    /// Create a new zstd reader that decompresses the zstd stream from
    /// the underlying reader. The underlying reader must implement
    /// either [`tokio::io::AsyncBufRead`] or [`futures::AsyncBufRead`],
    /// and its buffer will be used directly. When in doubt, use
    /// one of the other builder methods to use an appropriate buffer size
    /// for decompressing a zstd stream.
    pub fn builder_buffered(reader: R) -> ZstdReaderBuilder<R> {
        ZstdReaderBuilder::with_buffered(reader)
    }

    /// Wrap the reader with [`AsyncZstdSeekableReader`], which adds support
    /// for seeking if the underlying reader supports seeking.
    pub fn seekable(self) -> AsyncZstdSeekableReader<'dict, R> {
        AsyncZstdSeekableReader {
            reader: self,
            pending_seek: None,
        }
    }

    /// Decode and consume the entire zstd stream until reaching the end.
    /// Stops when reaching EOF (i.e. the underlying reader had no more data)
    #[cfg(feature = "tokio")]
    fn poll_jump_to_end_tokio(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>
    where
        R: tokio::io::AsyncBufRead,
    {
        use tokio::io::AsyncBufRead as _;

        loop {
            // Decode some data from the underlying reader
            let decoded = ready!(self.as_mut().poll_fill_buf(cx))?;

            // If we didn't get any more data, we're done
            if decoded.is_empty() {
                break;
            }

            // Consume all the decoded data
            let decoded_len = decoded.len();
            self.as_mut().consume(decoded_len);
        }

        std::task::Poll::Ready(Ok(()))
    }

    /// Decode and consume the entire zstd stream until reaching the end.
    /// Stops when reaching EOF (i.e. the underlying reader had no more data)
    #[cfg(feature = "futures")]
    fn poll_jump_to_end_futures(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>
    where
        R: futures::AsyncBufRead,
    {
        use futures::AsyncBufRead as _;

        loop {
            // Decode some data from the underlying reader
            let decoded = ready!(self.as_mut().poll_fill_buf(cx))?;

            // If we didn't get any more data, we're done
            if decoded.is_empty() {
                break;
            }

            // Consume all the decoded data
            let decoded_len = decoded.len();
            self.as_mut().consume(decoded_len);
        }

        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl<'dict, R> tokio::io::AsyncBufRead for AsyncZstdReader<'dict, R>
where
    R: tokio::io::AsyncBufRead,
{
    fn poll_fill_buf(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        use crate::buffer::Buffer as _;

        loop {
            let mut this = self.as_mut().project();

            // Check if our buffer contais any data we can return
            if !this.buffer.uncommitted().is_empty() {
                // If it does, we're done
                break;
            }

            // Get some data from the underlying reader
            let decodable = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
            if decodable.is_empty() {
                // If the underlying reader doesn't have any more data,
                // then we're done
                break;
            }

            // Decode the data, and write it to `self.buffer`
            let consumed = this.decoder.decode(decodable, this.buffer)?;

            // Tell the underlying reader that we read the subset of
            // data we decoded
            this.reader.consume(consumed);
        }

        // Return all the data we have in `self.buffer`
        std::task::Poll::Ready(Ok(self.project().buffer.uncommitted()))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        use crate::buffer::Buffer as _;

        let this = self.project();

        // Tell the buffer that we've committed the data that was consumed
        this.buffer.commit(amt);

        // Advance the reader's position
        let amt_u64: u64 = amt.try_into().unwrap();
        *this.current_pos += amt_u64;
    }
}

#[cfg(feature = "tokio")]
impl<'dict, R> tokio::io::AsyncRead for AsyncZstdReader<'dict, R>
where
    R: tokio::io::AsyncBufRead,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use tokio::io::AsyncBufRead as _;

        // Decode some data from the underlying reader
        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;

        // Get some of the decoded data, capped to `buf`'s length
        let consumable = filled.len().min(buf.remaining());

        // Copy the decoded data to `buf`
        buf.put_slice(&filled[..consumable]);

        // Consume the copied data
        self.consume(consumable);

        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "futures")]
impl<'dict, R> futures::AsyncBufRead for AsyncZstdReader<'dict, R>
where
    R: futures::io::AsyncBufRead,
{
    fn poll_fill_buf(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        use crate::buffer::Buffer as _;

        loop {
            let mut this = self.as_mut().project();

            // Check if our buffer contais any data we can return
            if !this.buffer.uncommitted().is_empty() {
                // If it does, we're done
                break;
            }

            // Get some data from the underlying reader
            let decodable = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
            if decodable.is_empty() {
                // If the underlying reader doesn't have any more data,
                // then we're done
                break;
            }

            // Decode the data, and write it to `self.buffer`
            let consumed = this.decoder.decode(decodable, this.buffer)?;

            // Tell the underlying reader that we read the subset of
            // data we decoded
            this.reader.consume(consumed);
        }

        // Return all the data we have in `self.buffer`
        std::task::Poll::Ready(Ok(self.project().buffer.uncommitted()))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        use crate::buffer::Buffer as _;

        let this = self.project();

        // Tell the buffer that we've committed the data that was consumed
        this.buffer.commit(amt);

        // Advance the reader's position
        let amt_u64: u64 = amt.try_into().unwrap();
        *this.current_pos += amt_u64;
    }
}

#[cfg(feature = "futures")]
impl<'dict, R> futures::AsyncRead for AsyncZstdReader<'dict, R>
where
    R: futures::AsyncBufRead,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use futures::AsyncBufRead as _;

        if buf.is_empty() {
            return std::task::Poll::Ready(Ok(0));
        }

        // Decode some data from the underlying reader
        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;

        // Get some of the decoded data, capped to `buf`'s length
        let consumable = filled.len().min(buf.len());

        // Copy the decoded data to `buf`
        buf[..consumable].copy_from_slice(&filled[..consumable]);

        // Consume the copied data
        self.consume(consumable);

        std::task::Poll::Ready(Ok(consumable))
    }
}

pin_project_lite::pin_project! {
    /// A wrapper around [`AsyncZstdReader`] with extra support for seeking.
    /// Created via the method [`AsyncZstdReader::seekable`].
    ///
    /// The underlying reader `R` should implement the following traits:
    ///
    /// - `tokio`
    ///   - [`tokio::io::AsyncBufRead`] + [`tokio::io::AsyncSeek`] (required for [`tokio::io::AsyncRead`], [`tokio::io::AsyncBufRead`], and [`tokio::io::AsyncSeek`] impls)
    /// - `futures`
    ///   - [`futures::AsyncBufRead`] + [`futures::AsyncSeek`] (required for [`futures::AsyncRead`], [`futures::AsyncBufRead`], and [`tokio::io::AsyncSeek`] impls)
    ///
    /// **By default, seeking
    /// within the stream will linearly decompress
    /// until reaching the target!**
    ///
    /// Seeking can do a lot better when the underlying stream is broken up
    /// into multiple frames, such as a stream that uses the [zstd seekable format].
    /// You can create such a stream using [`ZstdWriterBuilder::with_seek_table`](crate::async_writer::ZstdWriterBuilder::with_seek_table).
    ///
    /// There are two situations where seeking can take advantage of a seek
    /// table:
    ///
    /// 1. When a seek table is provided up-front using [`ZstdReaderBuilder::with_seek_table`].
    ///    See [`crate::table::read_seek_table`] for reading a seek table
    ///    from a reader (there are also async-friendly functions available).
    /// 2. When rewinding to a previously-decompressed frame. Frame offsets are
    ///    automatically recorded during decompression.
    ///
    /// Even if a seek table is used, seeking will still need to rewind to
    /// the start of a frame, then decompress until reaching the target offset.
    ///
    /// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
    pub struct AsyncZstdSeekableReader<'dict, R> {
        #[pin]
        reader: AsyncZstdReader<'dict, R>,
        pending_seek: Option<PendingSeek>,
    }
}

impl<'dict, R> AsyncZstdSeekableReader<'dict, R> {
    /// If a seek operation was started with [`tokio::io::AsyncSeek::start_seek`]
    /// but wasn't polled to completion, "undo" the seek by seeking
    /// back to where we were in the zstd stream.
    #[cfg(feature = "tokio")]
    fn poll_cancel_seek_tokio(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>
    where
        R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek,
    {
        use crate::buffer::Buffer as _;
        use tokio::io::AsyncBufRead as _;

        let mut this = self.project();

        // Iterate until `self.pending_seek` is unset. Each iteration
        // should make some progress based on the current pending seek
        // state (or should return `Poll::Pending`)
        loop {
            let Some(pending_seek) = *this.pending_seek else {
                // No pending seek, which means we're done!
                return std::task::Poll::Ready(Ok(()));
            };

            match pending_seek.state {
                PendingSeekState::Starting => {
                    // Seek just started. There's nothing to undo, so
                    // just clear the pending seek
                    *this.pending_seek = None;
                }
                PendingSeekState::SeekingToLastFrame { .. }
                | PendingSeekState::JumpingToEnd { .. }
                | PendingSeekState::SeekingToTarget { .. }
                | PendingSeekState::SeekingToFrame { .. }
                | PendingSeekState::JumpingForward { .. } => {
                    // Seek is in progress

                    // Consume any leftover data in `self.buffer`. This ensures
                    // that the current position is in line with the
                    // underlying decoder
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    // Determine what we need to do to reach the target position
                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(pending_seek.initial_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        // We need to seek to the start of a frame

                        // Transition the state to indicate we're seeking
                        // to the start of a frame
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringSeekToFrame {
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });

                        // Submit a seek job to the underlying reader
                        let reader = this.reader.as_mut().project().reader;
                        let result =
                            reader.start_seek(std::io::SeekFrom::Start(frame.compressed_pos));

                        match result {
                            Ok(_) => {}
                            Err(error) => {
                                // Trying to seek the underlying reader
                                // failed, so clear the pending seek and bail
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(std::io::Error::other(
                                    format!("failed to cancel in-progress zstd seek: {error}"),
                                )));
                            }
                        }
                    } else {
                        // We just need to keep decompressing to reach the
                        // target position

                        // Transition to a state to indicate how many
                        // bytes we need to consume
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringJumpForward {
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    }
                }
                PendingSeekState::RestoringSeekToFrame {
                    frame,
                    decompress_len,
                } => {
                    // We're in the process of restoring to the previous
                    // seek position, and need to seek to the start of
                    // a frame

                    let reader = this.reader.as_mut().project();

                    // Poll until the seek completes
                    let result = ready!(reader.reader.poll_complete(cx));

                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed, so
                            // clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    // Update our internal position to align with the start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Error from the decoder, so clear the pending
                            // seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    // Update our state to decompress as much data as we
                    // need to reach the initial position again
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward { decompress_len },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringJumpForward { decompress_len: 0 } => {
                    // We finished getting back to the initial position!
                    // Clear the pending seek

                    assert_eq!(pending_seek.initial_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::RestoringJumpForward { decompress_len } => {
                    // We have to decompress some data to reach the initial
                    // position

                    // Try to decompress some data from the underlying
                    // reader
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            // Failed to decompress from the underlying
                            // reader, so clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    };

                    if filled.is_empty() {
                        // The underlying reader didn't give us any data, which
                        // means we hit EOF while trying to get back to the
                        // initial position. Clear the pending seek and bail

                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to cancel in-progress zstd seek",
                        )));
                    }

                    // Consume as much data as we can to try and reach
                    // the total data to decompress
                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

                    // Update the state based on how much more data
                    // we have left to decompress
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward {
                            decompress_len: decompress_len - jump_len,
                        },
                        ..pending_seek
                    });
                }
            }
        }
    }

    /// If a seek operation was started with [`futures::AsyncSeek::poll_seek`]
    /// but wasn't polled to completion, "undo" the seek by seeking
    /// back to where we were in the zstd stream.
    #[cfg(feature = "futures")]
    fn poll_cancel_seek_futures(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>
    where
        R: futures::AsyncBufRead + futures::AsyncSeek,
    {
        use crate::buffer::Buffer as _;
        use futures::AsyncBufRead as _;

        let mut this = self.project();

        // Iterate until `self.pending_seek` is unset. Each iteration
        // should make some progress based on the current pending seek
        // state (or should return `Poll::Pending`)
        loop {
            let Some(pending_seek) = *this.pending_seek else {
                // No pending seek, which means we're done!
                return std::task::Poll::Ready(Ok(()));
            };

            match pending_seek.state {
                PendingSeekState::Starting => {
                    // Seek just started. There's nothing to undo, so
                    // just clear the pending seek
                    *this.pending_seek = None;
                }
                PendingSeekState::SeekingToLastFrame { .. }
                | PendingSeekState::JumpingToEnd { .. }
                | PendingSeekState::SeekingToTarget { .. }
                | PendingSeekState::SeekingToFrame { .. }
                | PendingSeekState::JumpingForward { .. } => {
                    // Seek is in progress

                    // Consume any leftover data in `self.buffer`. This ensures
                    // that the current position is in line with the
                    // underlying decoder
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    // Determine what we need to do to reach the target position
                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(pending_seek.initial_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        // We need to seek to the start of a frame

                        // Transition the state to indicate we're seeking
                        // to the start of a frame
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringSeekToFrame {
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    } else {
                        // We just need to keep decompressing to reach the
                        // target position

                        // Transition to a state to indicate how many
                        // bytes we need to consume
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringJumpForward {
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    }
                }
                PendingSeekState::RestoringSeekToFrame {
                    frame,
                    decompress_len,
                } => {
                    // We're in the process of restoring to the previous
                    // seek position, and need to seek to the start of
                    // a frame

                    let reader = this.reader.as_mut().project();

                    // Poll until we finish seeking the underlying reader
                    let result = ready!(reader
                        .reader
                        .poll_seek(cx, std::io::SeekFrom::Start(frame.compressed_pos)));

                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed, so
                            // clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    // Update our internal position to align with the start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Error from the decoder, so clear the pending
                            // seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    // Update our state to decompress as much data as we
                    // need to reach the initial position again
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward { decompress_len },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringJumpForward { decompress_len: 0 } => {
                    // We finished getting back to the initial position!
                    // Clear the pending seek

                    assert_eq!(pending_seek.initial_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::RestoringJumpForward { decompress_len } => {
                    // We have to decompress some data to reach the initial
                    // position

                    // Try to decompress some data from the underlying
                    // reader
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            // Failed to decompress from the underlying
                            // reader, so clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    };

                    if filled.is_empty() {
                        // The underlying reader didn't give us any data, which
                        // means we hit EOF while trying to get back to the
                        // initial position. Clear the pending seek and bail

                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to cancel in-progress zstd seek",
                        )));
                    }

                    // Consume as much data as we can to try and reach
                    // the total data to decompress
                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

                    // Update the state based on how much more data
                    // we have left to decompress
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward {
                            decompress_len: decompress_len - jump_len,
                        },
                        ..pending_seek
                    });
                }
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<'dict, R> tokio::io::AsyncBufRead for AsyncZstdSeekableReader<'dict, R>
where
    R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek,
{
    fn poll_fill_buf(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        // Cancel any in-progress seeks
        ready!(self.as_mut().poll_cancel_seek_tokio(cx))?;

        // Defer to the underlying implementation
        let this = self.project();
        this.reader.poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.project();

        // Ensure we aren't trying to seek before removing any
        // data from the buffer
        assert!(
            this.pending_seek.is_none(),
            "tried to consume from buffer while seeking"
        );

        // Defer to the underlying implementation
        this.reader.consume(amt);
    }
}

#[cfg(feature = "tokio")]
impl<'dict, R> tokio::io::AsyncRead for AsyncZstdSeekableReader<'dict, R>
where
    R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use tokio::io::AsyncBufRead as _;

        // Decode some data from the underlying reader
        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;

        // Get some of the decoded data, capped to `buf`'s length
        let consumable = filled.len().min(buf.remaining());

        // Copy the decoded data to `buf`
        buf.put_slice(&filled[..consumable]);

        // Consume the copied data
        self.consume(consumable);

        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl<'dict, R> tokio::io::AsyncSeek for AsyncZstdSeekableReader<'dict, R>
where
    R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek,
{
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let mut this = self.project();

        // Ensure there isn't another seek in progress first
        if this.pending_seek.is_some() {
            return Err(std::io::Error::other("seek already in progress"));
        }

        // Transition to the "starting" seek state
        *this.pending_seek = Some(PendingSeek {
            initial_pos: this.reader.as_mut().current_pos,
            seek: position,
            state: PendingSeekState::Starting,
        });
        Ok(())
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        use crate::buffer::Buffer as _;
        use tokio::io::AsyncBufRead as _;

        // Iterate until `self.pending_seek` is unset. Each iteration
        // should make some progress based on the current pending seek
        // state (or should return `Poll::Pending`)
        loop {
            let mut this = self.as_mut().project();

            let Some(pending_seek) = *this.pending_seek else {
                return std::task::Poll::Ready(Ok(this.reader.current_pos));
            };

            match pending_seek.state {
                PendingSeekState::Starting => {
                    // Seek is starting. The first step is to determine
                    // the seek target relative to the start of the stream

                    match pending_seek.seek {
                        std::io::SeekFrom::Start(offset) => {
                            // The offset is already relatve to the start,
                            // so transition to the state to start seeking
                            *this.pending_seek = Some(PendingSeek {
                                state: PendingSeekState::SeekingToTarget { target_pos: offset },
                                ..pending_seek
                            });
                        }
                        std::io::SeekFrom::End(end_offset) => {
                            // To determine the offset relative to the start,
                            // we first need to reach the end of the stream.

                            // Determine the best way to reach the last
                            // position we know about in the stream.
                            let seek = this.reader.decoder.prepare_seek_to_last_known_pos();

                            if let Some(frame) = seek.seek_to_frame_start {
                                // We have a frame to seek to

                                // Start a job to seek to the start
                                // of the last known frame
                                let reader = this.reader.as_mut().project().reader;
                                let result = reader
                                    .start_seek(std::io::SeekFrom::Start(frame.compressed_pos));

                                match result {
                                    Ok(_) => {}
                                    Err(error) => {
                                        // Trying to seek the underlying reader
                                        // failed, so clear the pending seek and bail
                                        *this.pending_seek = None;
                                        return std::task::Poll::Ready(Err(std::io::Error::other(
                                            format!(
                                                "failed to cancel in-progress zstd seek: {error}"
                                            ),
                                        )));
                                    }
                                }

                                // Transition to the state to seek before
                                // jumping to the end of the stream
                                *this.pending_seek = Some(PendingSeek {
                                    state: PendingSeekState::SeekingToLastFrame {
                                        frame,
                                        end_offset,
                                    },
                                    ..pending_seek
                                })
                            } else {
                                // No need to seek, so transition to the
                                // state to jump to the end of the stream
                                *this.pending_seek = Some(PendingSeek {
                                    state: PendingSeekState::JumpingToEnd { end_offset },
                                    ..pending_seek
                                });
                            }
                        }
                        std::io::SeekFrom::Current(offset) => {
                            // Compute the offset relative to the current position
                            let offset = this.reader.current_pos.checked_add_signed(offset);
                            let offset = match offset {
                                Some(offset) => offset,
                                None => {
                                    // Offset overflowed, so clear the pending
                                    // seek and return an error
                                    *this.pending_seek = None;
                                    return std::task::Poll::Ready(Err(std::io::Error::other(
                                        "invalid seek offset",
                                    )));
                                }
                            };

                            // Transition to the state to start seeking based
                            // on the computed offset
                            *this.pending_seek = Some(PendingSeek {
                                state: PendingSeekState::SeekingToTarget { target_pos: offset },
                                ..pending_seek
                            });
                        }
                    }
                }
                PendingSeekState::SeekingToLastFrame { end_offset, frame } => {
                    // We're seeking to the last (known) frame in the stream
                    // before jumping to the end

                    let reader = this.reader.as_mut().project();

                    // Wait for the seek on the underlying reader to complete
                    let result = ready!(reader.reader.poll_complete(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed,
                            // so clear the in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // The decoder failed, so clear the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    // Update our internal position to align with the
                    // start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Clear the buffer
                    reader.buffer.clear();

                    // Seek complete, so transition states to jump to
                    // the end of the stream
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingToEnd { end_offset },
                        ..pending_seek
                    });
                }
                PendingSeekState::JumpingToEnd { end_offset } => {
                    // Seek target is relative to the end of the stream, so
                    // we need to jump to the end of the stream before
                    // determing the target position

                    // Try to jump to the end of stream
                    let result = ready!(this.reader.as_mut().poll_jump_to_end_tokio(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Jumping to the end failed, so cancel the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Now we're at the end of the stream, so we can now
                    // compute the target position
                    let target_pos = this.reader.current_pos.checked_add_signed(end_offset);
                    let target_pos = match target_pos {
                        Some(target_pos) => target_pos,
                        None => {
                            // Target position overflowed, so cancel the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(
                                "invalid seek offset",
                            )));
                        }
                    };

                    // Transition to the state to start seeking based
                    // on the computed offset
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::SeekingToTarget { target_pos },
                        ..pending_seek
                    });
                }
                PendingSeekState::SeekingToTarget { target_pos } => {
                    // We now know the relative position we're seeking to

                    // Consume any leftover data in `self.buffer`. This ensures
                    // that the current position is in line with the
                    // underlying decoder
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    // Determine what we need to do to reach the target position
                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(target_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        // We need to seek to the start of a frame

                        // Transition to the state so we poll until
                        // the seek completes
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToFrame {
                                target_pos,
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });

                        let reader = this.reader.as_mut().project().reader;

                        // Start a job to seek the underlying reader
                        let result =
                            reader.start_seek(std::io::SeekFrom::Start(frame.compressed_pos));

                        match result {
                            Ok(_) => {}
                            Err(error) => {
                                // Trying to seek the underlying reader
                                // failed, so clear the in-progress seek
                                // and bail
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(error));
                            }
                        }
                    } else {
                        // We need to keep decoding bytes to reach the
                        // target position

                        // Transition to the state so that we can keep
                        // decompressing until reaching the target position
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::JumpingForward {
                                target_pos,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    }
                }
                PendingSeekState::SeekingToFrame {
                    target_pos,
                    frame,
                    decompress_len,
                } => {
                    // We're seeking to the start of a frame

                    let reader = this.reader.as_mut().project();

                    // Poll until the underlying reader finishes seeking
                    let result = ready!(reader.reader.poll_complete(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed,
                            // so clear the in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // The decoder failed, so clear the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    // Update our internal position to align with the
                    // start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Seek complete, so transition states to decompress
                    // until reaching the target position
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingForward {
                            target_pos,
                            decompress_len,
                        },
                        ..pending_seek
                    });
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len: 0,
                } => {
                    // No more bytes to decompress, so at the target position!
                    // Clear the pending seek
                    assert_eq!(target_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len,
                } => {
                    // We have some bytes to decompress before reaching
                    // the target position

                    // Try to decompress some data from the underlying
                    // reader
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            // Failed to decompress from the underlying
                            // reader, so clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    if filled.is_empty() {
                        // The underlying reader didn't give us any data, which
                        // means we hit EOF while trying to seek. Clear the
                        // pending seek and bail
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to decode to offset",
                        )));
                    }

                    // Consume as much data as we can to try and reach
                    // the total data to decompress
                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

                    // Update the state based on how much more data
                    // we have left to decompress
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingForward {
                            target_pos,
                            decompress_len: decompress_len - jump_len,
                        },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringSeekToFrame { .. }
                | PendingSeekState::RestoringJumpForward { .. } => {
                    // The seek was cancelled, so poll until the
                    // cancellation finished then return an error
                    ready!(self.as_mut().poll_cancel_seek_tokio(cx))?;
                    return std::task::Poll::Ready(Err(std::io::Error::other("seek cancelled")));
                }
            }
        }
    }
}

#[cfg(feature = "futures")]
impl<'dict, R> futures::AsyncBufRead for AsyncZstdSeekableReader<'dict, R>
where
    R: futures::AsyncBufRead + futures::AsyncSeek,
{
    fn poll_fill_buf(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        // Cancel any in-progress seeks
        ready!(self.as_mut().poll_cancel_seek_futures(cx))?;

        // Defer to the underlying implementation
        let this = self.project();
        this.reader.poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.project();

        // Ensure we aren't trying to seek before removing any
        // data from the buffer
        assert!(
            this.pending_seek.is_none(),
            "tried to consume from buffer while seeking"
        );

        // Defer to the underlying implementation
        this.reader.consume(amt);
    }
}

#[cfg(feature = "futures")]
impl<'dict, R> futures::AsyncRead for AsyncZstdSeekableReader<'dict, R>
where
    R: futures::AsyncBufRead + futures::AsyncSeek,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use futures::AsyncBufRead as _;

        // Decode some data from the underlying reader
        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;

        // Get some of the decoded data, capped to `buf`'s length
        let consumable = filled.len().min(buf.len());

        // Copy the decoded data to `buf`
        buf[..consumable].copy_from_slice(&filled[..consumable]);

        // Consume the copied data
        self.consume(consumable);

        std::task::Poll::Ready(Ok(consumable))
    }
}

#[cfg(feature = "futures")]
impl<'dict, R> futures::AsyncSeek for AsyncZstdSeekableReader<'dict, R>
where
    R: futures::AsyncBufRead + futures::AsyncSeek,
{
    fn poll_seek(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        position: std::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        use crate::buffer::Buffer as _;
        use futures::io::AsyncBufRead as _;

        // Iterate until the seek is complete. Each iteration should
        // make some progress based on the current pending seek state
        // (or should return `Poll::Pending`)
        loop {
            let this = self.as_mut().project();

            let pending_seek = match *this.pending_seek {
                Some(pending_seek) if pending_seek.seek == position => {
                    // We're already seeking with the same position,
                    // so keep going from where we left off
                    pending_seek
                }
                _ => {
                    // If there's another seek operation in progress,
                    // cancel it first
                    ready!(self.as_mut().poll_cancel_seek_futures(cx))?;

                    let this = self.as_mut().project();

                    // Start a new seek operation
                    let pending_seek = PendingSeek {
                        initial_pos: this.reader.current_pos,
                        seek: position,
                        state: PendingSeekState::Starting,
                    };
                    *this.pending_seek = Some(pending_seek);
                    pending_seek
                }
            };

            let mut this = self.as_mut().project();

            match pending_seek.state {
                PendingSeekState::Starting => {
                    // Seek is starting. The first step is to determine
                    // the seek target relative to the start of the stream

                    match pending_seek.seek {
                        std::io::SeekFrom::Start(offset) => {
                            // The offset is already relatve to the start,
                            // so transition to the state to start seeking
                            *this.pending_seek = Some(PendingSeek {
                                state: PendingSeekState::SeekingToTarget { target_pos: offset },
                                ..pending_seek
                            });
                        }
                        std::io::SeekFrom::End(end_offset) => {
                            // To determine the offset relative to the start,
                            // we first need to reach the end of the stream.

                            // Determine the best way to reach the last
                            // position we know about in the stream.
                            let seek = this.reader.decoder.prepare_seek_to_last_known_pos();

                            if let Some(frame) = seek.seek_to_frame_start {
                                // We have a frame to seek to, so
                                // transition to the state to seek before
                                // jumping to the end of the stream
                                *this.pending_seek = Some(PendingSeek {
                                    state: PendingSeekState::SeekingToLastFrame {
                                        frame,
                                        end_offset,
                                    },
                                    ..pending_seek
                                })
                            } else {
                                // No need to seek, so transition to the
                                // state to jump to the end of the stream
                                *this.pending_seek = Some(PendingSeek {
                                    state: PendingSeekState::JumpingToEnd { end_offset },
                                    ..pending_seek
                                });
                            }
                        }
                        std::io::SeekFrom::Current(offset) => {
                            // Compute the offset relative to the current position
                            let offset = this.reader.current_pos.checked_add_signed(offset);
                            let offset = match offset {
                                Some(offset) => offset,
                                None => {
                                    // Offset overflowed, so clear the pending
                                    // seek and return an error
                                    *this.pending_seek = None;
                                    return std::task::Poll::Ready(Err(std::io::Error::other(
                                        "invalid seek offset",
                                    )));
                                }
                            };

                            // Transition to the state to start seeking based
                            // on the computed offset
                            *this.pending_seek = Some(PendingSeek {
                                state: PendingSeekState::SeekingToTarget { target_pos: offset },
                                ..pending_seek
                            });
                        }
                    }
                }
                PendingSeekState::SeekingToLastFrame { end_offset, frame } => {
                    // We're seeking to the last (known) frame in the stream
                    // before jumping to the end

                    let reader = this.reader.as_mut().project();

                    // Seek the underlying reader
                    let result = ready!(reader
                        .reader
                        .poll_seek(cx, std::io::SeekFrom::Start(frame.compressed_pos)));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed,
                            // so clear the in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // The decoder failed, so clear the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    // Update our internal position to align with the
                    // start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Clear the buffer
                    reader.buffer.clear();

                    // Seek complete, so transition states to jump to
                    // the end of the stream
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingToEnd { end_offset },
                        ..pending_seek
                    });
                }
                PendingSeekState::JumpingToEnd { end_offset } => {
                    // Seek target is relative to the end of the stream, so
                    // we need to jump to the end of the stream before
                    // determing the target position

                    // Try to jump to the end of stream
                    let result = ready!(this.reader.as_mut().poll_jump_to_end_futures(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Jumping to the end failed, so cancel the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Now we're at the end of the stream, so we can now
                    // compute the target position
                    let target_pos = this.reader.current_pos.checked_add_signed(end_offset);
                    let target_pos = match target_pos {
                        Some(target_pos) => target_pos,
                        None => {
                            // Target position overflowed, so cancel the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(
                                "invalid seek offset",
                            )));
                        }
                    };

                    // Transition to the state to start seeking based
                    // on the computed offset
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::SeekingToTarget { target_pos },
                        ..pending_seek
                    });
                }
                PendingSeekState::SeekingToTarget { target_pos } => {
                    // We now know the relative position we're seeking to

                    // Consume any leftover data in `self.buffer`. This ensures
                    // that the current position is in line with the
                    // underlying decoder
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    // Determine what we need to do to reach the target position
                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(target_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        // We need to seek to the start of a frame

                        // Transition to the state so we poll until
                        // the seek completes
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToFrame {
                                target_pos,
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    } else {
                        // We need to keep decoding bytes to reach the
                        // target position

                        // Transition to the state so that we can keep
                        // decompressing until reaching the target position
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::JumpingForward {
                                target_pos,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    }
                }
                PendingSeekState::SeekingToFrame {
                    target_pos,
                    frame,
                    decompress_len,
                } => {
                    // We're seeking to the start of a frame

                    let reader = this.reader.as_mut().project();

                    // Seek the underlying reader
                    let result = ready!(reader
                        .reader
                        .poll_seek(cx, std::io::SeekFrom::Start(frame.compressed_pos)));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // Seeking the underlying reader failed,
                            // so clear the in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    // Update the decoder based on what frame we're now at
                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            // The decoder failed, so clear the
                            // in-progress seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    // Update our internal position to align with the
                    // start of the frame
                    *reader.current_pos = frame.decompressed_pos;

                    // Seek complete, so transition states to decompress
                    // until reaching the target position
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingForward {
                            target_pos,
                            decompress_len,
                        },
                        ..pending_seek
                    });
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len: 0,
                } => {
                    // No more bytes to decompress, so at the target position!
                    // Clear the pending seek, then we're done
                    assert_eq!(target_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                    return std::task::Poll::Ready(Ok(this.reader.current_pos));
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len,
                } => {
                    // We have some bytes to decompress before reaching
                    // the target position

                    // Try to decompress some data from the underlying
                    // reader
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            // Failed to decompress from the underlying
                            // reader, so clear the pending seek and bail
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    if filled.is_empty() {
                        // The underlying reader didn't give us any data, which
                        // means we hit EOF while trying to seek. Clear the
                        // pending seek and bail
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to decode to offset",
                        )));
                    }

                    // Consume as much data as we can to try and reach
                    // the total data to decompress
                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

                    // Update the state based on how much more data
                    // we have left to decompress
                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::JumpingForward {
                            target_pos,
                            decompress_len: decompress_len - jump_len,
                        },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringSeekToFrame { .. }
                | PendingSeekState::RestoringJumpForward { .. } => {
                    // The seek was cancelled, so poll until the
                    // cancellation finished then return an error
                    ready!(self.as_mut().poll_cancel_seek_futures(cx))?;
                    return std::task::Poll::Ready(Err(std::io::Error::other("seek cancelled")));
                }
            }
        }
    }
}

/// A builder that builds an [`AsyncZstdReader`] from the provided reader.
pub struct ZstdReaderBuilder<R> {
    reader: R,
    table: ZstdSeekTable,
}

#[cfg(feature = "tokio")]
impl<R> ZstdReaderBuilder<tokio::io::BufReader<R>> {
    fn new_tokio(reader: R) -> Self
    where
        R: tokio::io::AsyncRead,
    {
        let reader = tokio::io::BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), reader);
        ZstdReaderBuilder::with_buffered(reader)
    }
}

#[cfg(feature = "futures")]
impl<R> ZstdReaderBuilder<futures::io::BufReader<R>> {
    fn new_futures(reader: R) -> Self
    where
        R: futures::AsyncRead,
    {
        let reader =
            futures::io::BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), reader);
        ZstdReaderBuilder::with_buffered(reader)
    }
}

impl<R> ZstdReaderBuilder<R> {
    fn with_buffered(reader: R) -> Self {
        ZstdReaderBuilder {
            reader,
            table: ZstdSeekTable::empty(),
        }
    }

    /// Use the given seek table when seeking the resulting reader. This can
    /// greatly speed up seek operations when using a zstd stream that
    /// uses the [zstd seekable format].
    ///
    /// See [`crate::table::read_seek_table`] for reading a seek table.
    ///
    /// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
    pub fn with_seek_table(mut self, table: ZstdSeekTable) -> Self {
        self.table = table;
        self
    }

    /// Build the reader.
    pub fn build(self) -> std::io::Result<AsyncZstdReader<'static, R>> {
        let zstd_decoder = zstd::stream::raw::Decoder::new()?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::DCtx::out_size()]);
        let decoder = ZstdFramedDecoder::new(zstd_decoder, self.table);

        Ok(AsyncZstdReader {
            reader: self.reader,
            decoder,
            buffer,
            current_pos: 0,
        })
    }
}

#[cfg_attr(not(any(feature = "tokio", feature = "futures")), expect(dead_code))]
#[derive(Debug, Clone, Copy)]
struct PendingSeek {
    initial_pos: u64,
    seek: std::io::SeekFrom,
    state: PendingSeekState,
}

#[cfg_attr(not(any(feature = "tokio", feature = "futures")), expect(dead_code))]
#[derive(Debug, Clone, Copy)]
enum PendingSeekState {
    Starting,
    SeekingToLastFrame {
        end_offset: i64,
        frame: ZstdFrame,
    },
    JumpingToEnd {
        end_offset: i64,
    },
    SeekingToTarget {
        target_pos: u64,
    },
    SeekingToFrame {
        target_pos: u64,
        frame: ZstdFrame,
        decompress_len: u64,
    },
    JumpingForward {
        target_pos: u64,
        decompress_len: u64,
    },
    RestoringSeekToFrame {
        frame: ZstdFrame,
        decompress_len: u64,
    },
    RestoringJumpForward {
        decompress_len: u64,
    },
}
