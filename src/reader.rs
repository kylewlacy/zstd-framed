use std::io::BufRead as _;

use crate::{buffer::Buffer, decoder::ZstdFramedDecoder, table::ZstdSeekTable};

pub struct ZstdReader<'dict, R> {
    reader: R,
    decoder: ZstdFramedDecoder<'dict>,
    buffer: crate::buffer::FixedBuffer<Vec<u8>>,
    current_pos: u64,
}

impl<'dict, R> ZstdReader<'dict, std::io::BufReader<R>> {
    pub fn builder(reader: R) -> ZstdReaderBuilder<std::io::BufReader<R>>
    where
        R: std::io::Read,
    {
        ZstdReaderBuilder::new(reader)
    }

    pub fn builder_buffered(reader: R) -> ZstdReaderBuilder<R> {
        ZstdReaderBuilder::with_buffered(reader)
    }
}

impl<'dict, R> ZstdReader<'dict, R> {
    fn jump_forward(&mut self, mut length: u64) -> std::io::Result<()>
    where
        R: std::io::BufRead,
    {
        while length > 0 {
            let decoded = self.fill_buf()?;
            if decoded.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "reached eof while trying to decode to offset",
                ));
            }

            let decoded_len: u64 = decoded.len().try_into().map_err(std::io::Error::other)?;
            let consumed_len = decoded_len.min(length);

            let consumed_len_usize: usize =
                consumed_len.try_into().map_err(std::io::Error::other)?;
            self.consume(consumed_len_usize);

            length -= consumed_len;
        }

        Ok(())
    }

    fn jump_to_end(&mut self) -> std::io::Result<u64>
    where
        R: std::io::BufRead,
    {
        loop {
            let decoded = self.fill_buf()?;
            if decoded.is_empty() {
                break;
            }

            let decoded_len = decoded.len();
            self.consume(decoded_len);
        }

        Ok(self.current_pos)
    }
}

impl<'dict, R> std::io::Read for ZstdReader<'dict, R>
where
    R: std::io::BufRead,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let filled = self.fill_buf()?;
        let consumable = filled.len().min(buf.len());
        buf[..consumable].copy_from_slice(&filled[..consumable]);
        self.consume(consumable);
        Ok(consumable)
    }
}

impl<'dict, R> std::io::BufRead for ZstdReader<'dict, R>
where
    R: std::io::BufRead,
{
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        loop {
            if !self.buffer.uncommitted().is_empty() {
                break;
            }

            let decodable = self.reader.fill_buf()?;
            if decodable.is_empty() {
                break;
            }

            let consumed = self.decoder.decode(decodable, &mut self.buffer)?;
            self.reader.consume(consumed);
        }

        Ok(self.buffer.uncommitted())
    }

    fn consume(&mut self, amt: usize) {
        self.buffer.commit(amt);

        let amt_u64: u64 = amt.try_into().unwrap();
        self.current_pos += amt_u64;
    }
}

impl<'dict, R> std::io::Seek for ZstdReader<'dict, R>
where
    R: std::io::BufRead + std::io::Seek,
{
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let target_pos = match pos {
            std::io::SeekFrom::Start(offset) => offset,
            std::io::SeekFrom::End(offset) => {
                let end_pos = self.jump_to_end()?;
                end_pos
                    .checked_add_signed(offset)
                    .ok_or_else(|| std::io::Error::other("invalid seek offset"))?
            }
            std::io::SeekFrom::Current(offset) => self
                .current_pos
                .checked_add_signed(offset)
                .ok_or_else(|| std::io::Error::other("invalid seek offset"))?,
        };

        self.consume(self.buffer.uncommitted().len());

        let seek = self.decoder.prepare_seek_to_decompressed_pos(target_pos);
        if let Some(frame) = seek.seek_to_frame_start {
            self.reader
                .seek(std::io::SeekFrom::Start(frame.compressed_pos))?;
            self.decoder.seeked_to_frame(frame)?;
            self.current_pos = frame.decompressed_pos;
        }

        self.jump_forward(seek.decompress_len)?;

        assert_eq!(self.current_pos, target_pos);
        Ok(self.current_pos)
    }
}

pub struct ZstdReaderBuilder<R> {
    reader: R,
    table: ZstdSeekTable,
}

impl<R> ZstdReaderBuilder<std::io::BufReader<R>> {
    fn new(reader: R) -> Self
    where
        R: std::io::Read,
    {
        let reader = std::io::BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), reader);
        ZstdReaderBuilder::with_buffered(reader)
    }
}

impl<R> ZstdReaderBuilder<R> {
    pub fn with_buffered(reader: R) -> Self {
        ZstdReaderBuilder {
            reader,
            table: ZstdSeekTable::empty(),
        }
    }

    pub fn with_seek_table(mut self, table: ZstdSeekTable) -> Self {
        self.table = table;
        self
    }

    pub fn build(self) -> std::io::Result<ZstdReader<'static, R>> {
        let zstd_decoder = zstd::stream::raw::Decoder::new()?;
        let buffer = crate::buffer::FixedBuffer::new(vec![0; zstd::zstd_safe::DCtx::out_size()]);
        let decoder = ZstdFramedDecoder::new(zstd_decoder, self.table);

        Ok(ZstdReader {
            reader: self.reader,
            decoder,
            buffer,
            current_pos: 0,
        })
    }
}
