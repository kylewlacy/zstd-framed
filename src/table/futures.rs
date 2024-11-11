#![cfg(feature = "futures")]

use super::{ZstdFrame, ZstdFrameSize, ZstdSeekTable};

use futures::io::{AsyncReadExt as _, AsyncSeekExt as _};

/// Read the seek table from the end of a [zstd seekable format] stream.
///
/// Other implementations:
///
/// - sync I/O: [`crate::table::read_seek_table`]
/// - `tokio`: [`crate::table::tokio::read_seek_table`]
///
/// Returns `Ok(None)` if the stream doesn't apper to contain a seek table.
/// Otherwise, returns `Err(_)` if the seek table could not be parsed or
/// if an I/O error occurred while trying to read the seek table. If it
/// returns `Ok(_)`, it will also restore the reader to its original
/// stream position.
///
/// The seek table is returned as-is from the underlying reader. No attempt
/// is made to validate that the seek table lines up with the underlying
/// zstd stream. This means a malformed seek table could have out-of-bounds
/// offsets, could omit sections of the underyling stream, or could be
/// misaligned from frames of the underlying stream.
///
/// [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format
pub async fn read_seek_table<R>(mut reader: R) -> std::io::Result<Option<ZstdSeekTable>>
where
    R: Unpin + futures::AsyncRead + futures::AsyncSeek,
{
    // Get the stream position, so we can restore it later
    let initial_position = reader.stream_position().await?;

    // Read the seek table
    let seek_table_result = read_seek_table_inner(&mut reader).await;

    // Try to restore the seek position, even if reading
    // the seek table failed
    let seek_result = reader
        .seek(std::io::SeekFrom::Start(initial_position))
        .await;

    // If we got an error, return whichever we got first
    let seek_table = seek_table_result?;
    seek_result?;

    Ok(seek_table)
}

async fn read_seek_table_inner<R>(mut reader: R) -> std::io::Result<Option<ZstdSeekTable>>
where
    R: Unpin + futures::AsyncRead + futures::AsyncSeek,
{
    // Seek to the start of the zstd seek table footer
    reader.seek(std::io::SeekFrom::End(-9)).await?;

    // Read the footer fields: number of frames (4 bytes),
    // table descriptor (1 byte), and the magic number (4 bytes)
    let mut num_frames_bytes = [0; 4];
    reader.read_exact(&mut num_frames_bytes).await?;

    let mut seek_table_descriptor_bytes = [0; 1];
    reader.read_exact(&mut seek_table_descriptor_bytes).await?;

    let mut seekable_magic_number_bytes = [0; 4];
    reader.read_exact(&mut seekable_magic_number_bytes).await?;

    // Return if the magic number doesn't match
    if seekable_magic_number_bytes != crate::SEEKABLE_FOOTER_MAGIC_BYTES {
        return Ok(None);
    }

    // Parse the number of frames
    let num_frames = u32::from_le_bytes(num_frames_bytes);

    // Validate the seek table descriptor
    let [seek_table_descriptor] = seek_table_descriptor_bytes;
    let has_checksum = seek_table_descriptor & 0b1000_0000 != 0;
    let is_reserved_valid = seek_table_descriptor & 0b0111_1100 == 0;

    if !is_reserved_valid {
        return Err(std::io::Error::other(
            "zstd seek table has unsupported descriptor",
        ));
    }

    // Determine the table entry size (8 bytes, or 12 bytes with checksums)
    let table_entry_size: u32 = if has_checksum { 12 } else { 8 };

    // Calculate the full size of the skippable frame containing the
    // seek table. This can't overflow for a valid seek table, since the
    // frame size is part of the frame header.
    let table_frame_size = table_entry_size
        .checked_mul(num_frames)
        .and_then(|size| size.checked_add(9))
        .ok_or_else(|| std::io::Error::other("zstd seek table size overflowed"))?;

    // Seek to the start of the skippable frame containing the seek table
    reader
        .seek(std::io::SeekFrom::Current(-i64::from(table_frame_size) - 8))
        .await?;

    // Read the skippable frame magic number header: the
    // magic number (4 bytes) and the frame size (4 bytes)
    let mut skippable_magic_number_bytes = [0; 4];
    reader.read_exact(&mut skippable_magic_number_bytes).await?;

    let mut actual_table_frame_size_bytes = [0; 4];
    reader
        .read_exact(&mut actual_table_frame_size_bytes)
        .await?;

    // Validate the skippable frame magic number and frame size
    if skippable_magic_number_bytes != crate::SKIPPABLE_HEADER_MAGIC_BYTES {
        return Err(std::io::Error::other(
            "zstd seek table has unsupported skippable frame magic number",
        ));
    }

    let actual_table_frame_size = u32::from_le_bytes(actual_table_frame_size_bytes);
    if actual_table_frame_size != table_frame_size {
        return Err(std::io::Error::other("zstd seek table size did not match"));
    }

    // Read each table entry
    let mut table = ZstdSeekTable::empty();
    let mut compressed_pos = 0;
    let mut decompressed_pos = 0;
    for frame_index in 0..num_frames {
        let frame_index = usize::try_from(frame_index).unwrap();

        // Read the compressed size
        let mut compressed_size_bytes = [0; 4];
        reader.read_exact(&mut compressed_size_bytes).await?;
        let compressed_size = u32::from_le_bytes(compressed_size_bytes);

        // Read the decompressed size
        let mut decompressed_size_bytes = [0; 4];
        reader.read_exact(&mut decompressed_size_bytes).await?;
        let decompressed_size = u32::from_le_bytes(decompressed_size_bytes);

        // Skip the checksum if present
        if has_checksum {
            reader.seek(std::io::SeekFrom::Current(4)).await?;
        }

        let frame = ZstdFrame {
            compressed_pos,
            decompressed_pos,
            index: frame_index,
            size: ZstdFrameSize {
                compressed_size: compressed_size.into(),
                decompressed_size: decompressed_size.into(),
            },
        };
        table.insert(frame);

        compressed_pos += u64::from(compressed_size);
        decompressed_pos += u64::from(decompressed_size);
    }

    Ok(Some(table))
}
