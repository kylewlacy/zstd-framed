use std::io::Write as _;

use crate::{
    buffer::Buffer as _,
    encoder::{ZstdFramedEncoder, ZstdFramedEncoderSeekTableConfig},
    ZstdOutcome,
};

pub struct ZstdWriter<'dict, W>
where
    W: std::io::Write,
{
    writer: W,
    encoder: ZstdFramedEncoder<'dict>,
    buffer: crate::buffer::FixedBuffer<Vec<u8>>,
}

impl<'dict, W> ZstdWriter<'dict, W>
where
    W: std::io::Write,
{
    pub fn builder(writer: W) -> ZstdWriterBuilder<W> {
        ZstdWriterBuilder::new(writer)
    }

    pub fn finish_frame(&mut self) -> std::io::Result<()> {
        self.encoder.finish_frame(&mut self.buffer)?;

        Ok(())
    }

    pub fn shutdown(&mut self) -> std::io::Result<()> {
        loop {
            self.flush_uncommitted()?;

            let outcome = self.encoder.shutdown(&mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {}
                ZstdOutcome::Complete(_) => {
                    break;
                }
            }
        }

        Ok(())
    }

    fn flush_uncommitted(&mut self) -> std::io::Result<()> {
        loop {
            let uncommitted = self.buffer.uncommitted();
            if uncommitted.is_empty() {
                return Ok(());
            }

            let committed = self.writer.write(uncommitted)?;
            self.buffer.commit(committed);

            if committed == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                ));
            }
        }
    }
}

impl<'dict, W> std::io::Write for ZstdWriter<'dict, W>
where
    W: std::io::Write,
{
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        loop {
            self.flush_uncommitted()?;

            let outcome = self.encoder.encode(data, &mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {}
                ZstdOutcome::Complete(consumed) => {
                    return Ok(consumed);
                }
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            self.flush_uncommitted()?;

            let outcome = self.encoder.flush(&mut self.buffer)?;

            match outcome {
                ZstdOutcome::HasMore { .. } => {}
                ZstdOutcome::Complete(_) => {
                    break;
                }
            }
        }

        self.writer.flush()
    }
}

impl<'dict, W> Drop for ZstdWriter<'dict, W>
where
    W: std::io::Write,
{
    fn drop(&mut self) {
        let _ = self.shutdown().and_then(|_| self.flush());
    }
}

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

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn with_seek_table(mut self, max_frame_size: u32) -> Self {
        assert!(max_frame_size > 0, "max frame size must be greater than 0");

        self.seek_table_config = Some(ZstdFramedEncoderSeekTableConfig { max_frame_size });
        self
    }

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
