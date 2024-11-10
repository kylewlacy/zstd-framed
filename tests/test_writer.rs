use std::io::Write as _;

use assert_matches::assert_matches;
use easy_hex::Hex;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use zstd_framed::{table::read_seek_table, writer::ZstdWriter};

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
            writer = writer.with_seek_table(frame_size);
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
            writer = writer.with_seek_table(frame_size);
        }
        let mut writer = writer.build().unwrap();

        writer.write_all(first).unwrap();
        writer.finish_frame().unwrap();
        writer.write_all(second).unwrap();
        drop(writer);

        let decoded = zstd::decode_all(&encoded[..]).unwrap();
        assert_eq!(Hex(decoded), data);
    }

    #[test]
    fn test_writer_encode_with_table(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in test_utils::arb_frame_size(),
    ) {
        let mut encoded = vec![];

        let mut writer = ZstdWriter::builder(&mut encoded)
            .with_compression_level(level)
            .with_seek_table(frame_size)
            .build()
            .unwrap();

        writer.write_all(&data[..]).unwrap();
        drop(writer);

        let table = read_seek_table(&mut std::io::Cursor::new(&encoded[..])).unwrap().unwrap();
        let frames = table.frames().collect::<Vec<_>>();

        // Ensure every frame except the last matches the frame size
        for frame in frames.iter().rev().skip(1) {
            assert_eq!(frame.decompressed_size(), u64::from(frame_size));
        }
    }

    #[test]
    fn test_writer_encode_frames_with_table(
        frames in test_utils::arb_data_framed(),
        level in test_utils::arb_zstd_level(),
    ) {
        let mut encoded = vec![];

        let mut writer = ZstdWriter::builder(&mut encoded)
            .with_compression_level(level)
            .with_seek_table(u32::MAX)
            .build()
            .unwrap();

        for frame in &frames {
            writer.write_all(&frame[..]).unwrap();
            writer.finish_frame().unwrap();
        }

        drop(writer);

        let table = read_seek_table(&mut std::io::Cursor::new(&encoded[..])).unwrap().unwrap();

        let non_empty_frames: Vec<_> = frames.iter().filter(|frame| !frame.is_empty()).collect();
        let non_empty_table_frames: Vec<_> = table.frames().filter(|frame| frame.decompressed_size() > 0).collect();

        // Validate each (non-empty) frame has the right size. Empty frames
        // are skipped because they may or may not be written to the
        // table depending on when we tried to add it.
        assert_eq!(non_empty_table_frames.len(), non_empty_frames.len());
        for (table_frame, frame) in non_empty_table_frames.iter().zip(&non_empty_frames) {
            assert_eq!(table_frame.decompressed_size(), u64::try_from(frame.len()).unwrap());
        }
    }

    #[test]
    fn test_writer_encode_without_table(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
    ) {
        let mut encoded = vec![];

        let mut writer = ZstdWriter::builder(&mut encoded)
            .with_compression_level(level)
            .build()
            .unwrap();

        writer.write_all(&data[..]).unwrap();
        drop(writer);

        let table = read_seek_table(&mut std::io::Cursor::new(&encoded[..])).unwrap();

        // Ensure the table didn't get written
        // (TODO: Could this test fail if the compressed data happens
        // to look like a valid table?)
        assert_matches!(table, None);
    }
}
