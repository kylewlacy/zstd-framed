#[macro_use]
mod macros;

pub mod async_reader;
pub mod async_writer;
mod buffer;
mod decoder;
mod encoder;
pub mod reader;
pub mod table;
pub mod writer;

const SKIPPABLE_HEADER_MAGIC_BYTES: [u8; 4] = 0x184D2A5E_u32.to_le_bytes();
const SEEKABLE_FOOTER_MAGIC_BYTES: [u8; 4] = 0x8F92EAB1_u32.to_le_bytes();

#[derive(Debug, Clone, Copy)]
enum ZstdOutcome<T> {
    Complete(T),
    HasMore { remaining_bytes: usize },
}
