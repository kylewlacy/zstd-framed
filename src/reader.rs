use std::io::BufRead as _;

use crate::{buffer::Buffer, decoder::ZstdFramedDecoder, table::ZstdSeekTable};

/// A reader that decompresses a zstd stream from an underlying reader.
///
/// The underyling reader `R` should implement the following traits:
///
/// - [`std::io::BufRead`] (required for [`std::io::Read`] and [`std::io::BufRead`] impls)
/// - (Optional) [`std::io::Seek`] (required for [`std::io::Seek`] impl)
///
/// For async support, see [`crate::AsyncZstdReader`].
///
/// ## Construction
///
/// Create a builder using either [`ZstdReader::builder`] (recommended) or
/// [`ZstdReader::builder_buffered`] (to use a custom buffer).
/// See [`ZstdReaderBuilder`] for build options. Call
/// [`ZstdReaderBuilder::build`] to build the [`ZstdReader`] instance.
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let compressed_file: &[u8] = &[];
/// let reader = zstd_framed::ZstdReader::builder(compressed_file)
///     // .with_seek_table(table) // Provide a seek table if available
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// ## Buffering
///
/// The decompressed zstd output is always buffered internally. Since the
/// reader must also implement [`std::io::BufRead`], the compressed input
/// must also be buffered.
///
/// [`ZstdReader::builder`] will wrap any reader implmenting [`std::io::Read`]
/// with a recommended buffer size for the input stream. For more control
/// over how the input gets buffered, you can instead use
/// [`ZstdReader::builder_buffered`].
///
/// ## Seeking
///
/// [`ZstdReader`] implements [`std::io::Seek`] as long as the underlying
/// reader implements [`std::io::Seek`]. **By default, seeking within the
/// stream will linearly decompress until reaching the target!**
///
/// Seeking can do a lot better when the underlying stream is broken up
/// into multiple frames, such as a stream that uses the [zstd seekable format].
/// You can create such a stream using [`ZstdWriterBuilder::with_seek_table`](crate::writer::ZstdWriterBuilder::with_seek_table).
///
/// There are two situations where seeking can take advantage of a seek
/// table:
///
/// 1. When a seek table is provided up-front using [`ZstdReaderBuilder::with_seek_table`].
///    See [`crate::table::read_seek_table`] for reading a seek table
///    from a reader.
/// 2. When rewinding to a previously-decompressed frame. Frame offsets are
///    automatically recorded during decompression.
///
/// Even if a seek table is used, seeking will still need to rewind to
/// the start of a frame, then decompress until reaching the target offset.
///
/// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
pub struct ZstdReader<'dict, R> {
    reader: R,
    decoder: ZstdFramedDecoder<'dict>,
    buffer: crate::buffer::FixedBuffer<Vec<u8>>,
    current_pos: u64,
}

impl<'dict, R> ZstdReader<'dict, std::io::BufReader<R>> {
    /// Create a new zstd reader that decompresses the zstd stream from
    /// the underlying reader. The provided reader will be wrapped with
    /// an appropriately-sized buffer.
    pub fn builder(reader: R) -> ZstdReaderBuilder<std::io::BufReader<R>>
    where
        R: std::io::Read,
    {
        ZstdReaderBuilder::new(reader)
    }

    /// Create a new zstd reader that decompresses the zstd stream from
    /// the underlying reader. The underlying reader must implement
    /// [`std::io::BufRead`], and its buffer will be used directly. When in
    /// doubt, use [`ZstdReader::builder`], which uses an appropriate
    /// buffer size for decompressing a zstd stream.
    pub fn builder_buffered(reader: R) -> ZstdReaderBuilder<R> {
        ZstdReaderBuilder::with_buffered(reader)
    }
}

impl<'dict, R> ZstdReader<'dict, R> {
    /// Jump forward, decoding and consuming `length` decompressed bytes
    /// from the zstd stream.
    fn jump_forward(&mut self, mut length: u64) -> std::io::Result<()>
    where
        R: std::io::BufRead,
    {
        while length > 0 {
            // Decode some data from the underyling reader
            let decoded = self.fill_buf()?;

            // Return an error if we don't have any more data to decode
            if decoded.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "reached eof while trying to decode to offset",
                ));
            }

            // Consume the data (up to the remaining length we should jump)
            let decoded_len = u64::try_from(decoded.len()).map_err(std::io::Error::other)?;
            let consumed_len = decoded_len.min(length);
            let consumed_len_usize =
                usize::try_from(consumed_len).map_err(std::io::Error::other)?;

            self.consume(consumed_len_usize);

            // Keep iterating until we've consumed enough to reach our target
            length -= consumed_len;
        }

        Ok(())
    }

    /// Decode and consume the entire zstd stream until reaching the end.
    /// Stops when reaching EOF (i.e. the underlying reader had no more data)
    fn jump_to_end(&mut self) -> std::io::Result<()>
    where
        R: std::io::BufRead,
    {
        loop {
            // Decode some data from the underlying reader
            let decoded = self.fill_buf()?;

            // If we didn't get any more data, we're done
            if decoded.is_empty() {
                break;
            }

            // Consume all the decoded data
            let decoded_len = decoded.len();
            self.consume(decoded_len);
        }

        Ok(())
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

        // Decode some data from the underlying reader
        let filled = self.fill_buf()?;

        // Get some of the decoded data, capped to `buf`'s length
        let consumable = filled.len().min(buf.len());

        // Copy the decoded data to `buf`
        buf[..consumable].copy_from_slice(&filled[..consumable]);

        // Consume the copied data
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
            // Check if our buffer contais any data we can return
            if !self.buffer.uncommitted().is_empty() {
                // If it does, we're done
                break;
            }

            // Get some data from the underlying reader
            let decodable = self.reader.fill_buf()?;
            if decodable.is_empty() {
                // If the underlying reader doesn't have any more data,
                // then we're done
                break;
            }

            // Decode the data, and write it to `self.buffer`
            let consumed = self.decoder.decode(decodable, &mut self.buffer)?;

            // Tell the underlying reader that we read the subset of
            // data we decoded
            self.reader.consume(consumed);
        }

        // Return all the data we have in `self.buffer`
        Ok(self.buffer.uncommitted())
    }

    fn consume(&mut self, amt: usize) {
        // Tell the buffer that we've committed the data that was consumed
        self.buffer.commit(amt);

        // Advance the reader's position
        let amt_u64 = u64::try_from(amt).unwrap();
        self.current_pos += amt_u64;
    }
}

impl<'dict, R> std::io::Seek for ZstdReader<'dict, R>
where
    R: std::io::BufRead + std::io::Seek,
{
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        // Get the target position relative to the stream start
        let target_pos = match pos {
            std::io::SeekFrom::Start(offset) => {
                // Position is already relative to the start
                offset
            }
            std::io::SeekFrom::End(offset) => {
                // Jump to the end of the stream
                self.jump_to_end()?;

                // The current position is now at the end, so
                // add the end offset
                self.current_pos
                    .checked_add_signed(offset)
                    .ok_or_else(|| std::io::Error::other("invalid seek offset"))?
            }
            std::io::SeekFrom::Current(offset) => {
                // Add the offset to the current position
                self.current_pos
                    .checked_add_signed(offset)
                    .ok_or_else(|| std::io::Error::other("invalid seek offset"))?
            }
        };

        // Consume any leftover data in `self.buffer`. This ensures that
        // the current position is in line with the underlying decoder
        self.consume(self.buffer.uncommitted().len());

        // Determine what we need to do to reach the target position
        let seek = self.decoder.prepare_seek_to_decompressed_pos(target_pos);

        if let Some(frame) = seek.seek_to_frame_start {
            // We need to seek to the start of a frame

            // Seek the underlying reader
            self.reader
                .seek(std::io::SeekFrom::Start(frame.compressed_pos))?;

            // Update the decoder based on what frame we're now at
            self.decoder.seeked_to_frame(frame)?;

            // Update our internal position to align with the start of the frame
            self.current_pos = frame.decompressed_pos;
        }

        // Seek the remaining distance (if any) to reach the target position
        self.jump_forward(seek.decompress_len)?;

        assert_eq!(self.current_pos, target_pos);
        Ok(self.current_pos)
    }
}

/// A builder that builds a [`ZstdReader`] from the provided reader.
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
