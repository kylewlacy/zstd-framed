//! Seekable reader types and writer types for use with zstd-compressed
//! streams made up of multiple frames, especially those that use the [zstd seekable format]. Works with both sync I/O and async I/O via the `tokio` and `futures` features.
//!
//! By compressing a file into multiple frames, we can handle seeking
//! relatively efficiently: we first jump to the start of the last frame
//! before the target seek position, then decompress until we reach the
//! target. As long as the file was created with a reasonable frame size,
//! this can be more efficient than decompressing the entire file.
//!
//! This crate uses the [`zstd`] crate for zstd compression and decompression,
//! but includes a custom Rust implementation for the [zstd seekable format]
//! parser and serializer.
//!
//! ## When not to use this crate
//!
//! **This crate exposes a lot of options and can be easily mis-used if
//! you don't review your configuration carefully!** This is not meant to
//! be a general-purpose tool for compression and decompression, but one
//! that gives a lot of flexibility, such as when dealing with
//! partially-written files, or when you need precise control on how a file
//! gets broken up into multiple frames.
//!
//! For more general use, consider one of these alternative crates:
//!
//! - [`zstd`] for zstd compression and decompression (with synchronous I/O)
//! - [`async-compression`](https://crates.io/crates/async-compression) for
//!   compression and decompression with zstd and many other algorithms (with
//!   asynchronous I/O)
//! - [`zstd-seekable`](https://crates.io/crates/zstd-seekable) for compression
//!   and decompression using the [zstd seekable format] (with synchronous I/O).
//!   It also uses the official upstream zstd implementation for the seekable
//!   format.
//!
//! ## Getting started
//!
//! - Sync I/O
//!     - Reading a seek table: [`crate::table::read_seek_table`]
//!     - Reading a compressed stream: [`crate::ZstdReader`]
//!     - Writing a compressed stream [`crate::ZstdWriter`]
//! - Tokio
//!     - Reading a seek table: [`crate::table::tokio::read_seek_table`]
//!     - Reading a compressed stream: [`crate::AsyncZstdReader`]
//!     - Writing a compressed stream: [`crate::AsyncZstdWriter`]
//! - `futures-rs`
//!     - Reading a seek table: [`crate::table::futures::read_seek_table`]
//!     - Reading a compressed stream: [`crate::AsyncZstdReader`]
//!     - Writing a compressed stream: [`crate::AsyncZstdWriter`]
//!
//! ## Examples
//!
//! ```
//! # use std::io::{Read as _, Seek as _, Write as _};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // -----------------------------
//! // - Write a compressed stream -
//! // -----------------------------
//! let mut compressed_stream = vec![];
//!
//! let mut writer = zstd_framed::ZstdWriter::builder(&mut compressed_stream)
//!     .with_compression_level(3) // Set zstd compression level
//!     .with_seek_table(1000) // Add a seek table with a frame size of 1000
//!     .build()?;
//!
//! // Compress data by writing it to the writer
//! writer.write_all(b"hello world!");
//!
//! // Finish writing the stream
//! writer.shutdown()?;
//! drop(writer);
//!
//! // -------------------------------------
//! // - Read the seek table from a stream -
//! // -------------------------------------
//! let mut compressed_stream = std::io::Cursor::new(compressed_stream);
//! let seek_table = zstd_framed::table::read_seek_table(&mut compressed_stream)?
//!     .ok_or("expected stream to contain a seek table")?;
//! compressed_stream.seek(std::io::SeekFrom::Start(0))?;
//!
//! // --------------------------------------------------
//! // - Read and decompress a stream with a seek table -
//! // --------------------------------------------------
//! let mut reader = zstd_framed::ZstdReader::builder(&mut compressed_stream)
//!     .with_seek_table(seek_table)
//!     .build()?;
//!
//! // Seek past the word "hello", then read the rest of the stream
//! let mut decompressed_part = String::new();
//! reader.seek(std::io::SeekFrom::Start(6))?;
//! reader.read_to_string(&mut decompressed_part)?;
//!
//! assert_eq!(decompressed_part, "world!");
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature flags
//!
//! - `tokio`: Enable Tokio support, which enables the [`AsyncZstdReader`]
//!   and [`AsyncZstdWriter`] types to be used with the Tokio I/O traits
//!   ([`tokio::io::AsyncRead`], [`tokio::io::AsyncWrite`], etc.)
//! - `futures`: Enable support for the `futures-rs` crate, which enables the [`AsyncZstdReader`] and [`AsyncZstdWriter`] types to be used with the `futures-rs` traits ([`futures::AsyncRead`], [`futures::AsyncWrite`], etc.)
//!
//! [zstd seekable format]: https://github.com/facebook/zstd/tree/51eb7daf39c8e8a7c338ba214a9d4e2a6a086826/contrib/seekable_format

pub use async_reader::{AsyncZstdReader, AsyncZstdSeekableReader};
pub use async_writer::AsyncZstdWriter;
pub use reader::ZstdReader;
pub use writer::ZstdWriter;

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
