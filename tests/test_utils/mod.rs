#![allow(unused)]

use proptest::prelude::*;

pub type Data = easy_hex::Hex<Vec<u8>>;

pub const MAX_DATA_LENGTH: usize = 1000;
pub const MIN_ZSTD_LEVEL: i32 = -3;
pub const MAX_ZSTD_LEVEL: i32 = 15;

pub fn arb_data() -> impl Strategy<Value = Data> {
    prop::collection::vec(any::<u8>(), 0..=MAX_DATA_LENGTH).prop_map(easy_hex::Hex)
}

pub fn arb_data_with_pos() -> impl Strategy<Value = (Data, usize)> {
    arb_data().prop_flat_map(|data| {
        let len = data.len();
        (Just(data), 0..=len)
    })
}

pub fn arb_data_with_positions<const N: usize>() -> impl Strategy<Value = (Data, [usize; N])> {
    arb_data().prop_flat_map(|data| {
        let len = data.len();
        (Just(data), prop::array::uniform(0..=len))
    })
}

pub fn arb_zstd_level() -> impl Strategy<Value = i32> {
    MIN_ZSTD_LEVEL..=MAX_ZSTD_LEVEL
}

pub fn arb_frame_size() -> impl Strategy<Value = u32> {
    let max_data_length: u32 = MAX_DATA_LENGTH.try_into().unwrap();
    1..=max_data_length
}

pub fn arb_data_framed() -> impl Strategy<Value = Vec<Data>> {
    prop::collection::vec(arb_data(), 0..=MAX_DATA_LENGTH / 10)
}

pub fn arb_data_framed_with_pos() -> impl Strategy<Value = (Vec<Data>, usize)> {
    arb_data_framed().prop_flat_map(|frames| {
        let len = frames.iter().map(|frame| frame.len()).sum::<usize>();
        (Just(frames), 0..=len)
    })
}

pub fn arb_data_framed_with_positions<const N: usize>(
) -> impl Strategy<Value = (Vec<Data>, [usize; N])> {
    arb_data_framed().prop_flat_map(|frames| {
        let len = frames.iter().map(|frame| frame.len()).sum::<usize>();
        (Just(frames), prop::array::uniform(0..=len))
    })
}

#[derive(Debug, Clone, Copy)]
pub enum SeekType {
    Start,
    End,
    Current,
}

impl SeekType {
    pub fn seek_from(&self, current: usize, target: usize, end: usize) -> std::io::SeekFrom {
        match self {
            Self::Start => std::io::SeekFrom::Start(u64::try_from(target).unwrap()),
            Self::End => {
                std::io::SeekFrom::End(i64::try_from(target).unwrap() - i64::try_from(end).unwrap())
            }
            Self::Current => std::io::SeekFrom::Current(
                i64::try_from(target).unwrap() - i64::try_from(current).unwrap(),
            ),
        }
    }
}

pub fn arb_seek_type() -> impl Strategy<Value = SeekType> {
    prop_oneof![
        Just(SeekType::Start),
        Just(SeekType::End),
        Just(SeekType::Current)
    ]
}
