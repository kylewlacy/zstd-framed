use std::io::Write as _;

use easy_hex::Hex;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use zstd_framed::writer::ZstdWriter;

mod test_utils;

proptest! {
    #[test]
    fn test_writer_encode_then_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        let mut encoded = vec![];

        let mut writer = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            writer = writer.with_seekable_table(frame_size);
        }
        let mut writer = writer.build().unwrap();

        writer.write_all(&data[..]).unwrap();
        drop(writer);

        let decoded = zstd::decode_all(&encoded[..]).unwrap();
        assert_eq!(Hex(decoded), data);
    }

    #[test]
    fn test_writer_encode_with_split_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        let (first, second) = data.split_at(pos);

        let mut encoded = vec![];

        let mut writer = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            writer = writer.with_seekable_table(frame_size);
        }
        let mut writer = writer.build().unwrap();

        writer.write_all(first).unwrap();
        writer.finish_frame().unwrap();
        writer.write_all(second).unwrap();
        drop(writer);

        let decoded = zstd::decode_all(&encoded[..]).unwrap();
        assert_eq!(Hex(decoded), data);
    }
}
