#![cfg(feature = "tokio")]

use easy_hex::Hex;
use futures::AsyncWriteExt as _;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use zstd_framed::async_writer::AsyncZstdWriter;

mod test_utils;

proptest! {
    #[test]
    fn test_async_writer_tokio_encode_then_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in test_utils::arb_frame_size(),
    ) {
        let encoded = tokio::runtime::Runtime::new().unwrap().block_on({
            let data = data.clone();
            async move {
                let mut encoded = vec![];
                let mut writer = AsyncZstdWriter::new(&mut encoded, level, frame_size).unwrap();
                writer.write_all(&data[..]).await.unwrap();
                writer.close().await.unwrap();

                encoded
            }
        });

        let decoded = zstd::decode_all(&encoded[..]).unwrap();
        assert_eq!(Hex(decoded), data);
    }

    #[test]
    fn test_async_writer_tokio_encode_with_split_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in test_utils::arb_frame_size(),
    ) {
        let encoded = futures::executor::block_on({
            let data = data.clone();
            async move {
                let (first, second) = data.split_at(pos);

                let mut encoded = vec![];
                let mut writer = AsyncZstdWriter::new(&mut encoded, level, frame_size).unwrap();
                writer.write_all(first).await.unwrap();
                writer.finish_frame().unwrap();
                writer.write_all(second).await.unwrap();
                writer.close().await.unwrap();

                encoded
            }
        });

        let decoded = zstd::decode_all(&encoded[..]).unwrap();
        assert_eq!(Hex(decoded), data);
    }
}
