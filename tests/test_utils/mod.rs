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

pub fn arb_zstd_level() -> impl Strategy<Value = i32> {
    MIN_ZSTD_LEVEL..=MAX_ZSTD_LEVEL
}

pub fn arb_frame_size() -> impl Strategy<Value = u32> {
    let max_data_length: u32 = MAX_DATA_LENGTH.try_into().unwrap();
    1..=max_data_length
}
