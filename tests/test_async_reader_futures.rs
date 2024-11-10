#![cfg(feature = "futures")]

use easy_hex::Hex;
use futures::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use zstd_framed::{async_reader::AsyncZstdReader, async_writer::AsyncZstdWriter};

mod test_utils;

proptest! {
    #[test]
    fn test_async_reader_futures_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
    ) {
        let encoded = zstd::encode_all(&data[..], level).unwrap();

        tokio::runtime::Runtime::new().unwrap().block_on(async move {
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
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seekable_table(frame_size);
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
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
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
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seekable_table(frame_size);
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
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seekable_table(frame_size);
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
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut encoded = vec![];

            let mut encoder = AsyncZstdWriter::builder(&mut encoded).with_compression_level(level);
            if let Some(frame_size) = frame_size {
                encoder = encoder.with_seekable_table(frame_size);
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
}
