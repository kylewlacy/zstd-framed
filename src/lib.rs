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

#[derive(Debug, Clone, Copy)]
enum ZstdOutcome<T> {
    Complete(T),
    HasMore { remaining_bytes: usize },
}
