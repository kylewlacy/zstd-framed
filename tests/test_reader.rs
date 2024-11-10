use std::io::{Read as _, Seek as _, Write as _};

use easy_hex::Hex;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use test_utils::ReaderAction;
use zstd_framed::{reader::ZstdReader, table::read_seek_table, writer::ZstdWriter};

mod test_utils;

proptest! {
    #[test]
    fn test_reader_decode(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
    ) {
        let encoded = zstd::encode_all(&data[..], level).unwrap();

        let mut decoder = ZstdReader::builder(&encoded[..]).build().unwrap();
        let mut decoded = vec![];
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(Hex(decoded), data);
    }

    #[test]
    fn test_reader_decode_from_writer(
        data in test_utils::arb_data(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            encoder = encoder.with_seekable_table(frame_size);
        }
        let mut encoder = encoder.build().unwrap();

        encoder.write_all(&data[..]).unwrap();
        drop(encoder);

        let mut decoder = ZstdReader::builder(&encoded[..]).build().unwrap();
        let mut decoded = vec![];
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(Hex(decoded), data);
    }

    #[test]
    fn test_reader_decode_framed(
        frames in test_utils::arb_data_framed(),
        level in test_utils::arb_zstd_level(),
    ) {
        let mut encoded = vec![];
        let mut encoder = ZstdWriter::builder(&mut encoded).with_compression_level(level).build().unwrap();
        for frame in &frames {
            encoder.write_all(&frame[..]).unwrap();
            encoder.finish_frame().unwrap();
        }
        drop(encoder);

        let data = frames.iter().flat_map(|frame| frame.iter()).copied().collect::<Vec<_>>();

        let mut decoder = ZstdReader::builder(&encoded[..]).build().unwrap();
        let mut decoded = vec![];
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(Hex(decoded), Hex(data));
    }

    #[test]
    fn test_reader_seek_from_start_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            encoder = encoder.with_seekable_table(frame_size);
        }
        let mut encoder = encoder.build().unwrap();

        encoder.write_all(&data[..]).unwrap();
        drop(encoder);

        let mut decoded = vec![];
        let mut decoder = ZstdReader::builder(std::io::Cursor::new(&encoded[..])).build().unwrap();
        decoder.seek(std::io::SeekFrom::Start(pos.try_into().unwrap())).unwrap();
        decoder.read_to_end(&mut decoded).unwrap();

        assert_eq!(Hex(&decoded[..]), Hex(&data[pos..]));
    }

    #[test]
    fn test_reader_seek_from_end_then_decode(
        (data, pos) in test_utils::arb_data_with_pos(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
    ) {
        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            encoder = encoder.with_seekable_table(frame_size);
        }
        let mut encoder = encoder.build().unwrap();

        encoder.write_all(&data[..]).unwrap();
        drop(encoder);

        let mut decoder = ZstdReader::builder(std::io::Cursor::new(&encoded[..])).build().unwrap();
        let mut decoded = vec![];
        decoder.seek(std::io::SeekFrom::End(-i64::try_from(pos).unwrap())).unwrap();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(Hex(&decoded[..]), Hex(&data[data.len() - pos..]));
    }

    #[test]
    fn test_reader_seek_then_decode(
        (data, [pos_1, pos_2]) in test_utils::arb_data_with_positions(),
        level in test_utils::arb_zstd_level(),
        frame_size in prop::option::of(test_utils::arb_frame_size()),
        seek_type in test_utils::arb_seek_type(),
    ) {
        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded).with_compression_level(level);
        if let Some(frame_size) = frame_size {
            encoder = encoder.with_seekable_table(frame_size);
        }
        let mut encoder = encoder.build().unwrap();

        encoder.write_all(&data[..]).unwrap();
        drop(encoder);

        let mut decoder = ZstdReader::builder(std::io::Cursor::new(&encoded[..])).build().unwrap();

        let mut decoded_1 = vec![0; pos_1];
        decoder.read_exact(&mut decoded_1[..]).unwrap();
        assert_eq!(Hex(&decoded_1[..]), Hex(&data[..pos_1]));

        let mut decoded_2 = vec![];
        let seeked_pos = decoder.seek(seek_type.seek_from(pos_1, pos_2, data.len())).unwrap();
        assert_eq!(seeked_pos, pos_2 as u64);
        decoder.read_to_end(&mut decoded_2).unwrap();

        assert_eq!(Hex(&decoded_2[..]), Hex(&data[pos_2..]));
    }

    #[test]
    fn test_reader_seek_frame_boundary_without_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded)
            .with_compression_level(level)
            .build()
            .unwrap();

        for frame in &frames {
            encoder.write_all(&frame[..]).unwrap();
            encoder.finish_frame().unwrap();
        }
        drop(encoder);

        let (reader, watcher_actions) = test_utils::ReaderWatcher::new(std::io::Cursor::new(&encoded[..]));
        let mut decoder = ZstdReader::builder(reader).build().unwrap();

        let seeked_pos = decoder.seek(std::io::SeekFrom::Start(u64::try_from(pos).unwrap())).unwrap();
        assert_eq!(seeked_pos, pos as u64);

        let actions = std::mem::take(&mut *watcher_actions.write().unwrap());
        assert!(actions.iter().all(|action| matches!(action, ReaderAction::Read(_))));
    }

    #[test]
    fn test_reader_seek_frame_boundary_with_table(
        (frames, pos) in test_utils::arb_data_framed_with_frame_boundary_pos(),
        level in test_utils::arb_zstd_level(),
    ) {
        let data_len = frames.iter().map(|frame| frame.len()).sum::<usize>();
        prop_assume!(pos > 0 && pos < data_len);

        let mut encoded = vec![];

        let mut encoder = ZstdWriter::builder(&mut encoded)
            .with_compression_level(level)
            .with_seekable_table(u32::MAX)
            .build()
            .unwrap();

        for frame in &frames {
            encoder.write_all(&frame[..]).unwrap();
            encoder.finish_frame().unwrap();
        }
        drop(encoder);

        let table = read_seek_table(&mut std::io::Cursor::new(&encoded[..])).unwrap().unwrap();
        let frame = table.frames()
            .find(|frame| frame.decompressed_range().start == u64::try_from(pos).unwrap())
            .unwrap();

        let (reader, watcher_actions) = test_utils::ReaderWatcher::new(std::io::Cursor::new(&encoded[..]));
        let mut decoder = ZstdReader::builder(reader).with_table(table).build().unwrap();

        let seeked_pos = decoder.seek(std::io::SeekFrom::Start(u64::try_from(pos).unwrap())).unwrap();
        assert_eq!(seeked_pos, pos as u64);

        let actions = std::mem::take(&mut *watcher_actions.write().unwrap());

        assert_eq!(
            actions,
            [
                ReaderAction::Seek(std::io::SeekFrom::Start(frame.compressed_range().start))
            ],
        );
    }
}
