#[macro_use]
mod macros;

pub mod async_writer;
mod buffer;
mod encoder;
pub(crate) mod frames;
pub mod writer;

#[derive(Debug, Clone, Copy)]
enum ZstdOutcome<T> {
    Complete(T),
    HasMore { remaining_bytes: usize },
}
