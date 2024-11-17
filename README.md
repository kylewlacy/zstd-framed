# zstd-framed

[![Crate](https://img.shields.io/crates/v/zstd-framed)](https://crates.io/crates/zstd-framed)
[![Docs](https://docs.rs/zstd-framed/badge.svg)](https://docs.rs/zstd-framed)
[![Tests](https://github.com/kylewlacy/zstd-framed/workflows/Tests/badge.svg)](https://github.com/kylewlacy/zstd-framed/actions)

Compress and decompress zstd streams broken up into multiple frames, especially those that use the [zstd seekable format]. Includes both sync I/O support, and optional async I/O when using the `tokio` or `futures` features. Also supports seeking through zstd streams (but when used incorrectly, this can be very inefficient!)

This crate uses the [`zstd`](https://crates.io/crates/zstd) crate to handle the underlying compression and decompression, then adds on a custom layer that handles multiple frames. Support for the [zstd seekable format] does not use the official zstd implementation, and instead includes a custom Rust-native implementation (this was done due to difficulties adapting the official implementation for use with async I/O).

## Usage

```rust
// -----------------------------
// - Write a compressed stream -
// -----------------------------
let mut compressed_stream = vec![];

let mut writer = zstd_framed::ZstdWriter::builder(&mut compressed_stream)
    .with_compression_level(3) // Set zstd compression level
    .with_seek_table(1000) // Add a seek table with a frame size of 1000
    .build()?;

// Compress data by writing it to the writer
writer.write_all(b"hello world!");

// Finish writing the stream
writer.shutdown()?;
drop(writer);

// -------------------------------------
// - Read the seek table from a stream -
// -------------------------------------
let mut compressed_stream = std::io::Cursor::new(compressed_stream);
let seek_table = zstd_framed::table::read_seek_table(&mut compressed_stream)?
    .ok_or("expected stream to contain a seek table")?;

// --------------------------------------------------
// - Read and decompress a stream with a seek table -
// --------------------------------------------------
let mut reader = zstd_framed::ZstdReader::builder(&mut compressed_stream)
    .with_seek_table(seek_table)
    .build()?;

// Seek past the word "hello", then read the rest of the stream
let mut decompressed_part = String::new();
reader.seek(std::io::SeekFrom::Start(6))?;
reader.read_to_string(&mut decompressed_part)?;

assert_eq!(decompressed_part, "world!");
```

## Seeking

The main idea behind this crate is to break up a zstd stream into multiple independent frames. This will lead to a lower overall decompression ratio (usually by a small amount), but will allow decompressing from the start of any frame. The easiest way to do this is to use the `.with_seek_table()` method when constructing a `ZstdWriter` or an `AsyncZstdWriter` type.

Then, the reader types `ZstdReader` and `AsyncZstdSeekableReader` can load the written table using the corresponding `.with_seek_table()` method. Then, when seeking, the reader will use the table to determine which frame to seek to, then decompress the stream from the start of that frame until reaching the target seek position.

Note though that the reader types support seeking _even if a seek table is not provided_. Here are a few notes when seeking:

- Seeking backward restarts decompression of the current frame.
- Seeking forward within a frame will just decompress until reaching the target position.
- **Seeking won't provide any benefit if a zstd stream only contains one frame!** Be sure to choose an appropriate size when using the `.with_seek_table()` option on the writer, or use the `.finish_frame()` method regularly.
- Seeking without a seek table is allowed, but has quite a few performance considerations:
    - **Seeking from the end of a stream without a seek table will decompress the entire remainder of the stream.**
    - **Seeking forward without a seek table will decompress until the target position is reached, even if the stream is made up of multiple frames.**
    - Seeking backward without a seek table will use a partial in-memory table to determine which frame to seek to.

## Minimum Supported Rust Version (MSRV)

`zstd-framed` currently supports Rust **v1.81** and above. Changes to the MSRV are tracked in the [Changelog](./CHANGELOG.md).

## Alternatives

This crate fills a very specific niche, and it exposes a lot of options that could easily be mis-used! For more general use, consider one of these alternatives:

- [`zstd`](https://crates.io/crates/zstd) for zstd compression and decompression (with synchronous I/O)
- [`async-compression`](https://crates.io/crates/async-compression) for
  compression and decompression with zstd and many other algorithms (with
  asynchronous I/O)
- [`zstd-seekable`](https://crates.io/crates/zstd-seekable) for compression
  and decompression using the [zstd seekable format] (with synchronous I/O).
  It also uses the official upstream zstd implementation for the seekable
  format.

## License

Licensed under either the [MIT license](./LICENSE-MIT.md) or [the Unlicense](./UNLICENSE.md) (licensee's choice).

[zstd seekable format]: https://github.com/facebook/zstd/tree/b2c5bc16d90e15735b0dad051c6d7cd654b97cc6/contrib/seekable_format
