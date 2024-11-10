use crate::encoder::{ZstdFramedEncoder, ZstdFramedEncoderSeekableTableConfig};

pin_project_lite::pin_project! {
    pub struct AsyncZstdWriter<'dict, W> {
        #[pin]
        writer: W,
        encoder: ZstdFramedEncoder<'dict>,
        buffer: crate::buffer::FixedBuffer<Vec<u8>> ,
    }
}

impl<'dict, W> AsyncZstdWriter<'dict, W> {
    pub fn builder(writer: W) -> ZstdWriterBuilder<W> {
        ZstdWriterBuilder::new(writer)
    }

    pub fn finish_frame(&mut self) -> std::io::Result<()> {
        self.encoder.finish_frame(&mut self.buffer)?;

        Ok(())
    }

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
            let uncommitted = this.buffer.uncommitted();
            if uncommitted.is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }

            let committed = ready!(this.writer.as_mut().poll_write(cx, uncommitted))?;
            this.buffer.commit(committed);

            if committed == 0 {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                )));
            }
        }
    }

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
            let uncommitted = this.buffer.uncommitted();
            if uncommitted.is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }

            let committed = ready!(this.writer.as_mut().poll_write(cx, uncommitted))?;
            this.buffer.commit(committed);

            if committed == 0 {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                )));
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<'dict, W> tokio::io::AsyncWrite for AsyncZstdWriter<'dict, W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.encode(data, this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(consumed) => {
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
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.flush(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(_) => {
                    break;
                }
            }
        }

        ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.shutdown(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(_) => {
                    ready!(self.as_mut().flush_uncommitted_tokio(cx))?;

                    break;
                }
            }
        }

        let this = self.project();
        this.writer.poll_shutdown(cx)
    }
}

#[cfg(feature = "futures")]
impl<'dict, W> futures::AsyncWrite for AsyncZstdWriter<'dict, W>
where
    W: futures::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.encode(data, this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(consumed) => {
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
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.flush(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(_) => {
                    break;
                }
            }
        }

        ready!(self.as_mut().flush_uncommitted_futures(cx))?;

        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            ready!(self.as_mut().flush_uncommitted_futures(cx))?;

            let this = self.as_mut().project();

            let outcome = this.encoder.shutdown(this.buffer)?;

            match outcome {
                crate::ZstdOutcome::HasMore { .. } => {}
                crate::ZstdOutcome::Complete(_) => {
                    ready!(self.as_mut().flush_uncommitted_futures(cx))?;

                    break;
                }
            }
        }

        let this = self.project();
        this.writer.poll_close(cx)
    }
}

pub struct ZstdWriterBuilder<W> {
    writer: W,
    compression_level: i32,
    seekable_table_config: Option<ZstdFramedEncoderSeekableTableConfig>,
}

impl<W> ZstdWriterBuilder<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            compression_level: 0,
            seekable_table_config: None,
        }
    }

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn with_seekable_table(mut self, max_frame_size: u32) -> Self {
        assert!(max_frame_size > 0, "max frame size must be greater than 0");

        self.seekable_table_config = Some(ZstdFramedEncoderSeekableTableConfig { max_frame_size });
        self
    }

    pub fn build(self) -> std::io::Result<AsyncZstdWriter<'static, W>>
    where
        W: std::io::Write,
    {
        let zstd_encoder = zstd::stream::raw::Encoder::new(self.compression_level)?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::CCtx::out_size()]);
        let encoder = ZstdFramedEncoder::new(zstd_encoder, self.seekable_table_config);

        Ok(AsyncZstdWriter {
            writer: self.writer,
            encoder,
            buffer,
        })
    }
}
