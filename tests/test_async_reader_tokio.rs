#![cfg(feature = "tokio")]

use easy_hex::Hex;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use test_utils::ReaderAction;
use tokio::io::{AsyncReadExt as _, AsyncSeek as _, AsyncSeekExt as _, AsyncWriteExt as _};
use zstd_framed::{
    async_reader::AsyncZstdReader, async_writer::AsyncZstdWriter, table::tokio::read_seek_table,
};

mod test_utils;

proptest! {
    #[test]
    fn test_async_reader_tokio_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
    ) {
        let encoded = zstd::encode_all(&data[..], level).unwrap();

        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut decoder = AsyncZstdReader::builder_tokio(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(decoded), data);
        });
    }

    #[test]
    fn test_async_reader_tokio_decode_from_writer(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.shutdown().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_tokio(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(decoded), data);
        });
    }

    #[test]
    fn test_async_reader_tokio_decode_framed(
        frames in test_utils::arb_data_framed(),
        level in test_utils::arb_zstd_level(),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level).build().unwrap();

            for frame in &frames {
                encoder.write_all(&frame[..]).await.unwrap();
                encoder.finish_frame().unwrap();
            }
            encoder.shutdown().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_tokio(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            let data = frames.iter().flat_map(|frame| frame.iter()).copied().collect::<Vec<_>>();

            assert_eq!(Hex(decoded), Hex(data));
        });
    }

    #[test]
    fn test_async_reader_tokio_seek_from_start_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.shutdown().await.unwrap();

            let mut decoded = vec![];
            let mut decoder = AsyncZstdReader::builder_tokio(std::io::Cursor::new(&encoded[..])).build().unwrap().seekable();
            decoder.seek(std::io::SeekFrom::Start(pos.try_into().unwrap())).await.unwrap();
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(&decoded[..]), Hex(&data[pos..]));
        });
    }

    #[test]
    fn test_async_reader_tokio_seek_from_end_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.shutdown().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_tokio(std::io::Cursor::new(&encoded[..])).build().unwrap().seekable();
            let mut decoded = vec![];
            decoder.seek(std::io::SeekFrom::End(-i64::try_from(pos).unwrap())).await.unwrap();
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(&decoded[..]), Hex(&data[data.len() - pos..]));
        });
    }

    #[test]
    fn test_async_reader_tokio_seek_then_decode(
        (data, [pos_1, pos_2]) in test_utils::arb_data_with_positions(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
        seek_type in test_utils::arb_seek_type(),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.shutdown().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_tokio(std::io::Cursor::new(&encoded[..])).build().unwrap().seekable();

            let mut decoded_1 = vec![0; pos_1];
            decoder.read_exact(&mut decoded_1[..]).await.unwrap();
            assert_eq!(Hex(&decoded_1[..]), Hex(&data[..pos_1]));

            let mut decoded_2 = vec![];
            let seeked_pos = decoder.seek(seek_type.seek_from(pos_1, pos_2, data.len())).await.unwrap();
            assert_eq!(seeked_pos, pos_2 as u64);
            decoder.read_to_end(&mut decoded_2).await.unwrap();

            assert_eq!(Hex(&decoded_2[..]), Hex(&data[pos_2..]));
        });
    }

    #[test]
    fn test_async_reader_tokio_cancel_seek_then_decode(
        (data, [pos_1, pos_2]) in test_utils::arb_data_with_positions(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
        seek_type in test_utils::arb_seek_type(),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.shutdown().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_tokio(std::io::Cursor::new(&encoded[..])).build().unwrap().seekable();
            let mut decoder = std::pin::pin!(decoder);

            let mut decoded_1 = vec![0; pos_1];
            decoder.as_mut().read_exact(&mut decoded_1[..]).await.unwrap();
            assert_eq!(Hex(&decoded_1[..]), Hex(&data[..pos_1]));

            let mut decoded_2 = vec![];

            // Start a seek but don't poll it to completion. This
            // should cancel the seek, and the reader should return
            // to the previous position
            decoder.as_mut().start_seek(seek_type.seek_from(pos_1, pos_2, data.len())).unwrap();
            decoder.as_mut().read_to_end(&mut decoded_2).await.unwrap();

            assert_eq!(Hex(&decoded_2[..]), Hex(&data[pos_1..]));
        });
    }

    #[test]
    fn test_async_reader_tokio_seek_frame_boundary_without_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded)
                .with_compression_level(level)
                .build()
                .unwrap();

            for frame in &frames {
                encoder.write_all(&frame[..]).await.unwrap();
                encoder.finish_frame().unwrap();
            }
            encoder.shutdown().await.unwrap();

            let (reader, watcher_actions) = test_utils::ReaderWatcher::new(std::io::Cursor::new(&encoded[..]));
            let mut decoder = AsyncZstdReader::builder_tokio(reader).build().unwrap().seekable();

            let seeked_pos = decoder.seek(std::io::SeekFrom::Start(u64::try_from(pos).unwrap())).await.unwrap();
            assert_eq!(seeked_pos, pos as u64);

            let actions = std::mem::take(&mut *watcher_actions.write().unwrap());
            assert!(actions.iter().all(|action| matches!(action, ReaderAction::Read(_))));
        });
    }

    #[test]
    fn test_async_reader_tokio_seek_frame_boundary_with_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        let data_len = frames.iter().map(|frame| frame.len()).sum::<usize>();
        prop_assume!(pos > 0 && pos < data_len);

        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded)
                .with_compression_level(level)
                .with_seek_table(u32::MAX)
                .build()
                .unwrap();

            for frame in &frames {
                encoder.write_all(&frame[..]).await.unwrap();
                encoder.finish_frame().unwrap();
            }
            encoder.shutdown().await.unwrap();

            let table = read_seek_table(&mut std::io::Cursor::new(&encoded[..])).await.unwrap().unwrap();
            let frame = table.frames()
                .find(|frame| frame.decompressed_range().start == u64::try_from(pos).unwrap())
                .unwrap();

            let (reader, watcher_actions) = test_utils::ReaderWatcher::new(std::io::Cursor::new(&encoded[..]));
            let mut decoder = AsyncZstdReader::builder_tokio(reader).with_table(table).build().unwrap().seekable();

            let seeked_pos = decoder.seek(std::io::SeekFrom::Start(u64::try_from(pos).unwrap())).await.unwrap();
            assert_eq!(seeked_pos, pos as u64);

            let actions = std::mem::take(&mut *watcher_actions.write().unwrap());

            assert_eq!(
                actions,
                [
                    ReaderAction::Seek(std::io::SeekFrom::Start(frame.compressed_range().start))
                ],
            );
        });
    }
}
