use crate::{
    decoder::ZstdFramedDecoder,
    table::{ZstdFrame, ZstdFrameTable},
};

pin_project_lite::pin_project! {
    pub struct AsyncZstdReader<'dict, R> {
        #[pin]
        reader: R,
        decoder: ZstdFramedDecoder<'dict>,
        buffer: crate::buffer::FixedBuffer<Vec<u8>>,
        current_pos: u64,
    }
}

impl<'dict, R> AsyncZstdReader<'dict, R> {
    #[cfg(feature = "tokio")]
    pub fn builder_tokio(reader: R) -> ZstdReaderBuilder<tokio::io::BufReader<R>>
    where
        R: tokio::io::AsyncRead,
    {
        ZstdReaderBuilder::new_tokio(reader)
    }

    #[cfg(feature = "futures")]
    pub fn builder_futures(reader: R) -> ZstdReaderBuilder<futures::io::BufReader<R>>
    where
        R: futures::AsyncRead,
    {
        ZstdReaderBuilder::new_futures(reader)
    }

    pub fn builder_buffered(reader: R) -> ZstdReaderBuilder<R> {
        ZstdReaderBuilder::with_buffered(reader)
    }

    pub fn seekable(self) -> AsyncZstdSeekableReader<'dict, R> {
        AsyncZstdSeekableReader {
            reader: self,
            pending_seek: None,
        }
    }

    #[cfg(feature = "tokio")]
    fn poll_jump_to_end_tokio(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>>
    where
        R: tokio::io::AsyncBufRead,
    {
        use tokio::io::AsyncBufRead as _;

        loop {
            let decoded = ready!(self.as_mut().poll_fill_buf(cx))?;
            if decoded.is_empty() {
                break;
            }

            let decoded_len = decoded.len();
            self.as_mut().consume(decoded_len);
        }

        std::task::Poll::Ready(Ok(self.current_pos))
    }

    #[cfg(feature = "futures")]
    fn poll_jump_to_end_futures(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>>
    where
        R: futures::AsyncBufRead,
    {
        use futures::AsyncBufRead as _;

        loop {
            let decoded = ready!(self.as_mut().poll_fill_buf(cx))?;
            if decoded.is_empty() {
                break;
            }

            let decoded_len = decoded.len();
            self.as_mut().consume(decoded_len);
        }

        std::task::Poll::Ready(Ok(self.current_pos))
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

            if !this.buffer.uncommitted().is_empty() {
                break;
            }

            let decodable = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
            if decodable.is_empty() {
                break;
            }

            let consumed = this.decoder.decode(decodable, this.buffer)?;
            this.reader.consume(consumed);
        }

        std::task::Poll::Ready(Ok(self.project().buffer.uncommitted()))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        use crate::buffer::Buffer as _;

        let this = self.project();

        this.buffer.commit(amt);

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

        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;
        let consumable = filled.len().min(buf.remaining());
        buf.put_slice(&filled[..consumable]);
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

            if !this.buffer.uncommitted().is_empty() {
                break;
            }

            let decodable = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
            if decodable.is_empty() {
                break;
            }

            let consumed = this.decoder.decode(decodable, this.buffer)?;
            this.reader.consume(consumed);
        }

        std::task::Poll::Ready(Ok(self.project().buffer.uncommitted()))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        use crate::buffer::Buffer as _;

        let this = self.project();

        this.buffer.commit(amt);

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

        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;
        let consumable = filled.len().min(buf.len());
        buf[..consumable].copy_from_slice(&filled[..consumable]);
        self.consume(consumable);
        std::task::Poll::Ready(Ok(consumable))
    }
}

pin_project_lite::pin_project! {
    pub struct AsyncZstdSeekableReader<'dict, R> {
        #[pin]
        reader: AsyncZstdReader<'dict, R>,
        pending_seek: Option<PendingSeek>,
    }
}

impl<'dict, R> AsyncZstdSeekableReader<'dict, R> {
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

        loop {
            let Some(pending_seek) = *this.pending_seek else {
                return std::task::Poll::Ready(Ok(()));
            };

            match pending_seek.state {
                PendingSeekState::Starting => {
                    *this.pending_seek = None;
                }
                PendingSeekState::JumpingToEnd { .. }
                | PendingSeekState::SeekingToTarget { .. }
                | PendingSeekState::SeekingToFrame { .. }
                | PendingSeekState::JumpingForward { .. } => {
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(pending_seek.initial_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringSeekToFrame {
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });

                        let reader = this.reader.as_mut().project().reader;
                        let result =
                            reader.start_seek(std::io::SeekFrom::Start(frame.compressed_pos));

                        match result {
                            Ok(_) => {}
                            Err(error) => {
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(std::io::Error::other(
                                    format!("failed to cancel in-progress zstd seek: {error}"),
                                )));
                            }
                        }
                    } else {
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
                    let reader = this.reader.as_mut().project();
                    let result = ready!(reader.reader.poll_complete(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    *reader.current_pos = frame.decompressed_pos;

                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward { decompress_len },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringJumpForward { decompress_len: 0 } => {
                    assert_eq!(pending_seek.initial_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::RestoringJumpForward { decompress_len } => {
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    };

                    if filled.is_empty() {
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to cancel in-progress zstd seek",
                        )));
                    }

                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

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

        loop {
            let Some(pending_seek) = *this.pending_seek else {
                return std::task::Poll::Ready(Ok(()));
            };

            match pending_seek.state {
                PendingSeekState::Starting => {
                    *this.pending_seek = None;
                }
                PendingSeekState::JumpingToEnd { .. }
                | PendingSeekState::SeekingToTarget { .. }
                | PendingSeekState::SeekingToFrame { .. }
                | PendingSeekState::JumpingForward { .. } => {
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(pending_seek.initial_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::RestoringSeekToFrame {
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    } else {
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
                    let reader = this.reader.as_mut().project();
                    let result = ready!(reader
                        .reader
                        .poll_seek(cx, std::io::SeekFrom::Start(frame.compressed_pos)));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    *reader.current_pos = frame.decompressed_pos;

                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    }

                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::RestoringJumpForward { decompress_len },
                        ..pending_seek
                    });
                }
                PendingSeekState::RestoringJumpForward { decompress_len: 0 } => {
                    assert_eq!(pending_seek.initial_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::RestoringJumpForward { decompress_len } => {
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(format!(
                                "failed to cancel in-progress zstd seek: {error}"
                            ))));
                        }
                    };

                    if filled.is_empty() {
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to cancel in-progress zstd seek",
                        )));
                    }

                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();
                    this.reader.as_mut().consume(jump_len_usize);

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
        ready!(self.as_mut().poll_cancel_seek_tokio(cx))?;

        let this = self.project();
        this.reader.poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.project();

        assert!(
            this.pending_seek.is_none(),
            "tried to consume from buffer while seeking"
        );

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

        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;
        let consumable = filled.len().min(buf.remaining());
        buf.put_slice(&filled[..consumable]);
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
        if this.pending_seek.is_some() {
            return Err(std::io::Error::other("seek already in progress"));
        }

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

        loop {
            let mut this = self.as_mut().project();

            let Some(pending_seek) = *this.pending_seek else {
                return std::task::Poll::Ready(Ok(this.reader.current_pos));
            };

            match pending_seek.state {
                PendingSeekState::Starting => match pending_seek.seek {
                    std::io::SeekFrom::Start(offset) => {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToTarget { target_pos: offset },
                            ..pending_seek
                        });
                    }
                    std::io::SeekFrom::End(end_offset) => {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::JumpingToEnd { end_offset },
                            ..pending_seek
                        });
                    }
                    std::io::SeekFrom::Current(offset) => {
                        let offset = this.reader.current_pos.checked_add_signed(offset);
                        let offset = match offset {
                            Some(offset) => offset,
                            None => {
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(std::io::Error::other(
                                    "invalid seek offset",
                                )));
                            }
                        };

                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToTarget { target_pos: offset },
                            ..pending_seek
                        });
                    }
                },
                PendingSeekState::JumpingToEnd { end_offset } => {
                    let result = ready!(this.reader.poll_jump_to_end_tokio(cx));
                    let end_pos = match result {
                        Ok(end_pos) => end_pos,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    let target_pos = end_pos.checked_add_signed(end_offset);
                    let target_pos = match target_pos {
                        Some(target_pos) => target_pos,
                        None => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(
                                "invalid seek offset",
                            )));
                        }
                    };

                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::SeekingToTarget { target_pos },
                        ..pending_seek
                    });
                }
                PendingSeekState::SeekingToTarget { target_pos } => {
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(target_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToFrame {
                                target_pos,
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });

                        let reader = this.reader.as_mut().project().reader;
                        let result =
                            reader.start_seek(std::io::SeekFrom::Start(frame.compressed_pos));

                        match result {
                            Ok(_) => {}
                            Err(error) => {
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(error));
                            }
                        }
                    } else {
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
                    let reader = this.reader.as_mut().project();
                    let result = ready!(reader.reader.poll_complete(cx));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    *reader.current_pos = frame.decompressed_pos;

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
                    assert_eq!(target_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len,
                } => {
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    if filled.is_empty() {
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to decode to offset",
                        )));
                    }

                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();

                    this.reader.as_mut().consume(jump_len_usize);

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
        ready!(self.as_mut().poll_cancel_seek_futures(cx))?;

        let this = self.project();
        this.reader.poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.project();

        assert!(
            this.pending_seek.is_none(),
            "tried to consume from buffer while seeking"
        );

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

        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;
        let consumable = filled.len().min(buf.len());
        buf[..consumable].copy_from_slice(&filled[..consumable]);
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

        loop {
            let this = self.as_mut().project();

            let pending_seek = match *this.pending_seek {
                Some(pending_seek) if pending_seek.seek == position => pending_seek,
                _ => {
                    ready!(self.as_mut().poll_cancel_seek_futures(cx))?;

                    let this = self.as_mut().project();

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
                PendingSeekState::Starting => match pending_seek.seek {
                    std::io::SeekFrom::Start(offset) => {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToTarget { target_pos: offset },
                            ..pending_seek
                        });
                    }
                    std::io::SeekFrom::End(end_offset) => {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::JumpingToEnd { end_offset },
                            ..pending_seek
                        });
                    }
                    std::io::SeekFrom::Current(offset) => {
                        let offset = this.reader.current_pos.checked_add_signed(offset);
                        let offset = match offset {
                            Some(offset) => offset,
                            None => {
                                *this.pending_seek = None;
                                return std::task::Poll::Ready(Err(std::io::Error::other(
                                    "invalid seek offset",
                                )));
                            }
                        };

                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToTarget { target_pos: offset },
                            ..pending_seek
                        });
                    }
                },
                PendingSeekState::JumpingToEnd { end_offset } => {
                    let result = ready!(this.reader.poll_jump_to_end_futures(cx));
                    let end_pos = match result {
                        Ok(end_pos) => end_pos,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    let target_pos = end_pos.checked_add_signed(end_offset);
                    let target_pos = match target_pos {
                        Some(target_pos) => target_pos,
                        None => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(std::io::Error::other(
                                "invalid seek offset",
                            )));
                        }
                    };

                    *this.pending_seek = Some(PendingSeek {
                        state: PendingSeekState::SeekingToTarget { target_pos },
                        ..pending_seek
                    });
                }
                PendingSeekState::SeekingToTarget { target_pos } => {
                    let consumable = this.reader.buffer.uncommitted().len();
                    this.reader.as_mut().consume(consumable);

                    let seek = this
                        .reader
                        .decoder
                        .prepare_seek_to_decompressed_pos(target_pos);

                    if let Some(frame) = seek.seek_to_frame_start {
                        *this.pending_seek = Some(PendingSeek {
                            state: PendingSeekState::SeekingToFrame {
                                target_pos,
                                frame,
                                decompress_len: seek.decompress_len,
                            },
                            ..pending_seek
                        });
                    } else {
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
                    let reader = this.reader.as_mut().project();
                    let result = ready!(reader
                        .reader
                        .poll_seek(cx, std::io::SeekFrom::Start(frame.compressed_pos)));
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    let result = reader.decoder.seeked_to_frame(frame);
                    match result {
                        Ok(_) => {}
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    }

                    *reader.current_pos = frame.decompressed_pos;

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
                    assert_eq!(target_pos, this.reader.current_pos);
                    *this.pending_seek = None;
                    return std::task::Poll::Ready(Ok(this.reader.current_pos));
                }
                PendingSeekState::JumpingForward {
                    target_pos,
                    decompress_len,
                } => {
                    let result = ready!(this.reader.as_mut().poll_fill_buf(cx));
                    let filled = match result {
                        Ok(filled) => filled,
                        Err(error) => {
                            *this.pending_seek = None;
                            return std::task::Poll::Ready(Err(error));
                        }
                    };

                    if filled.is_empty() {
                        *this.pending_seek = None;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "reached eof while trying to decode to offset",
                        )));
                    }

                    let filled_len_u64: u64 = filled.len().try_into().unwrap();
                    let jump_len = filled_len_u64.min(decompress_len);
                    let jump_len_usize: usize = jump_len.try_into().unwrap();

                    this.reader.as_mut().consume(jump_len_usize);

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
                    ready!(self.as_mut().poll_cancel_seek_futures(cx))?;
                    return std::task::Poll::Ready(Err(std::io::Error::other("seek cancelled")));
                }
            }
        }
    }
}

pub struct ZstdReaderBuilder<R> {
    reader: R,
    table: ZstdFrameTable,
}

#[cfg(feature = "tokio")]
impl<R> ZstdReaderBuilder<tokio::io::BufReader<R>> {
    pub fn new_tokio(reader: R) -> Self
    where
        R: tokio::io::AsyncRead,
    {
        let reader = tokio::io::BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), reader);
        ZstdReaderBuilder::with_buffered(reader)
    }
}

#[cfg(feature = "futures")]
impl<R> ZstdReaderBuilder<futures::io::BufReader<R>> {
    pub fn new_futures(reader: R) -> Self
    where
        R: futures::AsyncRead,
    {
        let reader =
            futures::io::BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), reader);
        ZstdReaderBuilder::with_buffered(reader)
    }
}

impl<R> ZstdReaderBuilder<R> {
    pub fn with_buffered(reader: R) -> Self {
        ZstdReaderBuilder {
            reader,
            table: ZstdFrameTable::empty(),
        }
    }

    pub fn with_seek_table(mut self, table: ZstdFrameTable) -> Self {
        self.table = table;
        self
    }

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
