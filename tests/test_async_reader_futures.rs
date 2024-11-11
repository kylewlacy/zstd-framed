#![cfg(feature = "futures")]

use easy_hex::Hex;
use futures::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use test_utils::ReaderAction;
use zstd_framed::{
    async_reader::AsyncZstdReader, async_writer::AsyncZstdWriter, table::futures::read_seek_table,
};

mod test_utils;

proptest! {
    #[test]
    fn test_async_reader_futures_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
    ) {
        let encoded = zstd::encode_all(&data[..], level).unwrap();

        futures::executor::block_on(async move {
            let mut decoder = AsyncZstdReader::builder_futures(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(decoded), data);
        });
    }

    #[test]
    fn test_async_reader_futures_decode_from_writer(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.close().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_futures(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(decoded), data);
        });
    }

    #[test]
    fn test_async_reader_futures_decode_framed(
        frames in test_utils::arb_data_framed(),
        level in test_utils::arb_zstd_level(),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];
            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level).build().unwrap();
            for frame in &frames {
                encoder.write_all(&frame[..]).await.unwrap();
                encoder.finish_frame().unwrap();
            }
            encoder.close().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_futures(&encoded[..]).build().unwrap();
            let mut decoded = vec![];
            decoder.read_to_end(&mut decoded).await.unwrap();

            let data = frames.iter().flat_map(|frame| frame.iter()).copied().collect::<Vec<_>>();

            assert_eq!(Hex(decoded), Hex(data));
        });
    }

    #[test]
    fn test_async_reader_futures_seek_from_start_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.close().await.unwrap();

            let mut decoded = vec![];
            let mut decoder = AsyncZstdReader::builder_futures(futures::io::Cursor::new(&encoded[..])).build().unwrap().seekable();
            decoder.seek(std::io::SeekFrom::Start(pos.try_into().unwrap())).await.unwrap();
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(&decoded[..]), Hex(&data[pos..]));
        });
    }

    #[test]
    fn test_async_reader_futures_seek_from_end_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.close().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_futures(futures::io::Cursor::new(&encoded[..])).build().unwrap().seekable();
            let mut decoded = vec![];
            decoder.seek(std::io::SeekFrom::End(-i64::try_from(pos).unwrap())).await.unwrap();
            decoder.read_to_end(&mut decoded).await.unwrap();

            assert_eq!(Hex(&decoded[..]), Hex(&data[data.len() - pos..]));
        });
    }

    #[test]
    fn test_async_reader_futures_seek_then_decode(
        (data, [pos_1, pos_2]) in test_utils::arb_data_with_positions(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
        seek_type in test_utils::arb_seek_type(),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.close().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_futures(futures::io::Cursor::new(&encoded[..])).build().unwrap().seekable();

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
    fn test_async_reader_futures_multiple_seeks(
        (data, [pos_1, pos_2, pos_3]) in test_utils::arb_data_with_positions(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
        [seek_type_1, seek_type_2, seek_type_3] in prop::array::uniform(test_utils::arb_seek_type()),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seek_table(frame_size);
            }
            let mut encoder = encoder.build().unwrap();

            encoder.write_all(&data[..]).await.unwrap();
            encoder.close().await.unwrap();

            let mut decoder = AsyncZstdReader::builder_futures(futures::io::Cursor::new(&encoded[..])).build().unwrap().seekable();

            let mut decoded = vec![];
            let seeked_pos = decoder.seek(seek_type_1.seek_from(0, pos_1, data.len())).await.unwrap();
            assert_eq!(seeked_pos, pos_1 as u64);
            decoder.read_to_end(&mut decoded).await.unwrap();
            assert_eq!(Hex(&decoded[..]), Hex(&data[pos_1..]));

            let mut decoded = vec![];
            decoder.seek(std::io::SeekFrom::Start(seeked_pos)).await.unwrap();
            let seeked_pos = decoder.seek(seek_type_2.seek_from(pos_1, pos_2, data.len())).await.unwrap();
            assert_eq!(seeked_pos, pos_2 as u64);
            decoder.read_to_end(&mut decoded).await.unwrap();
            assert_eq!(Hex(&decoded[..]), Hex(&data[pos_2..]));

            let mut decoded = vec![];
            decoder.seek(std::io::SeekFrom::Start(seeked_pos)).await.unwrap();
            let seeked_pos = decoder.seek(seek_type_3.seek_from(pos_2, pos_3, data.len())).await.unwrap();
            assert_eq!(seeked_pos, pos_3 as u64);
            decoder.read_to_end(&mut decoded).await.unwrap();
            assert_eq!(Hex(&decoded[..]), Hex(&data[pos_3..]));
        });
    }

    #[test]
    fn test_async_reader_futures_seek_frame_boundary_without_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        futures::executor::block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded)
                .with_compression_level(level)
                .build()
                .unwrap();

            for frame in &frames {
                encoder.write_all(&frame[..]).await.unwrap();
                encoder.finish_frame().unwrap();
            }
            encoder.close().await.unwrap();

            let (reader, watcher_actions) = test_utils::ReaderWatcher::new(futures::io::Cursor::new(&encoded[..]));
            let mut decoder = AsyncZstdReader::builder_futures(reader).build().unwrap().seekable();

            let seeked_pos = decoder.seek(std::io::SeekFrom::Start(u64::try_from(pos).unwrap())).await.unwrap();
            assert_eq!(seeked_pos, pos as u64);

            let actions = std::mem::take(&mut *watcher_actions.write().unwrap());
            assert!(actions.iter().all(|action| matches!(action, ReaderAction::Read(_))));
        });
    }

    #[test]
    fn test_async_reader_futures_seek_frame_boundary_with_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        let data_len = frames.iter().map(|frame| frame.len()).sum::<usize>();
        prop_assume!(pos > 0 && pos < data_len);

        futures::executor::block_on(async move {
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
            encoder.close().await.unwrap();

            let table = read_seek_table(&mut futures::io::Cursor::new(&encoded[..])).await.unwrap().unwrap();
            let frame = table.frames()
                .find(|frame| frame.decompressed_range().start == u64::try_from(pos).unwrap())
                .unwrap();

            let (reader, watcher_actions) = test_utils::ReaderWatcher::new(futures::io::Cursor::new(&encoded[..]));
            let mut decoder = AsyncZstdReader::builder_futures(reader)
                .with_seek_table(table)
                .build()
                .unwrap()
                .seekable();

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
