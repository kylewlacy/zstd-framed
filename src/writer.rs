use std::io::Write as _;

use crate::{buffer::Buffer as _, encoder::ZstdFramedEncoder, ZstdOutcome};

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
    pub fn new(writer: W, level: i32, frame_size: u32) -> std::io::Result<Self> {
        assert!(frame_size > 0, "frame size must be greater than 0");

        let zstd_encoder = zstd::stream::raw::Encoder::new(level)?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::CCtx::out_size()]);
        let encoder = ZstdFramedEncoder::new(zstd_encoder, frame_size);
        Ok(Self {
            writer,
            encoder,
            buffer,
        })
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
