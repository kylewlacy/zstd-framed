use std::io::{Read as _, Seek as _, Write as _};

use easy_hex::Hex;
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use zstd_framed::{reader::ZstdReader, writer::ZstdWriter};

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
        frame_size in test_utils::arb_frame_size(),
    ) {
        let mut encoded = vec![];
        let mut encoder = ZstdWriter::new(&mut encoded, level, frame_size).unwrap();
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
        let mut encoder = ZstdWriter::new(&mut encoded, level, 99999).unwrap();
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
        frame_size in test_utils::arb_frame_size(),
    ) {
        let mut encoded = vec![];
        let mut encoder = ZstdWriter::new(&mut encoded, level, frame_size).unwrap();
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
        frame_size in test_utils::arb_frame_size(),
    ) {
        let mut encoded = vec![];
        let mut encoder = ZstdWriter::new(&mut encoded, level, frame_size).unwrap();
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
        frame_size in test_utils::arb_frame_size(),
        seek_type in test_utils::arb_seek_type(),
    ) {
        let mut encoded = vec![];
        let mut encoder = ZstdWriter::new(&mut encoded, level, frame_size).unwrap();
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
}
