use crate::ZstdOutcome;

/// A trait used to buffer data. Conceptually, a buffer starts empty,
/// has some data written to it. The buffer holds that data while it's
/// uncommitted, then something else commits that data e.g. by writing it
/// to the outside world. Once all data has been committed, then the buffer
/// is again empty.
///
/// See [FixedBuffer] for a minimal buffer implementation.
pub trait Buffer {
    /// Get the writable slice of the buffer.
    fn writable(&mut self) -> &mut [u8];

    /// Mark the first `len` bytes as having been written to the writable
    /// part of the buffer.
    ///
    /// ## Panics
    ///
    /// Implementations may panic if `len > self.writable().len()`.
    fn written(&mut self, len: usize);

    /// Get the uncommitted slice of the buffer. This is data that has
    /// been written to the buffer but not yet committed.
    fn uncommitted(&self) -> &[u8];

    /// Mark the first `len` bytes as having been committed to the uncommitted
    /// part of the buffer, e.g. because it was flushed to the outside world.
    /// Implementations may then free the committed space to allow for
    /// writing again.
    ///
    /// ## Panics
    ///
    /// Implementations may panic if `len > self.uncommitted().len()`.
    fn commit(&mut self, len: usize);

    /// Clear any uncommitted data in the buffer. This is conceptually
    /// equivalent to committing all the uncommitted data.
    fn clear(&mut self);
}

/// A [Buffer] that wraps some fixed-size array-like type, which uses two
/// indices to track the writable, uncommitted, and commmitted parts of the
/// buffer.
///
/// The type parameter `T` should implement [`AsRef<u8>`](core::convert::AsRef)
/// and [`AsMut<u8>`](core::convert::AsMut)
///
/// The buffer will look like this internally:
///
/// ```plain
/// |--------------------- buffer ---------------------|
/// |-- (committed) --|-- uncommitted --|-- writable --|
///                 ^ head            ^ tail
/// ```
///
/// - When data is written to the buffer, `tail` is bumped forward
/// - When data is committed from the buffer, `head` is bumped forward
/// - Once all data is committed, then `head` and `tail` are reset
pub struct FixedBuffer<T> {
    buffer: T,
    head: usize,
    tail: usize,
}

impl<T> FixedBuffer<T> {
    pub fn new(buffer: T) -> Self {
        Self {
            buffer,
            head: 0,
            tail: 0,
        }
    }
}

impl<T> Buffer for FixedBuffer<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn writable(&mut self) -> &mut [u8] {
        let buffer = self.buffer.as_mut();
        &mut buffer[self.tail..]
    }

    fn written(&mut self, len: usize) {
        self.tail += len;
        assert!(self.tail <= self.buffer.as_ref().len());
    }

    fn uncommitted(&self) -> &[u8] {
        let buffer = self.buffer.as_ref();
        &buffer[self.head..self.tail]
    }

    fn commit(&mut self, len: usize) {
        self.head += len;
        assert!(self.head <= self.tail);

        if self.head == self.tail {
            self.head = 0;
            self.tail = 0;
        }
    }

    fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
    }
}

/// Copy data from `src` into the `dst` buffer, capped to the buffer's
/// available write capacity. Returns the number of bytes copied from `src`.
pub fn copy_from_slice(src: &[u8], dst: &mut impl Buffer) -> usize {
    let writable = dst.writable();
    let write_len = writable.len().min(src.len());

    writable[..write_len].copy_from_slice(&src[..write_len]);
    dst.written(write_len);

    write_len
}

/// Write all data from `src` into the `dst` buffer. Panics if the buffer
/// doesn't have enough capacity to copy all bytes.
pub fn copy_all_from_slice(src: &[u8], dst: &mut impl Buffer) {
    let written = copy_from_slice(src, dst);

    assert_eq!(src.len(), written, "could not write all data to buffer");
}

/// Move all uncommitted data from the `src` buffer into the `dst` buffer.
/// Returns [`Complete(())`](ZstdOutcome::Complete) if all data was
/// written, or [`HasMore(_)`](ZstdOutcome::HasMore) with the number of
/// uncommitted bytes left in `src`.
pub fn move_buffer(src: &mut impl Buffer, dst: &mut impl Buffer) -> ZstdOutcome<()> {
    let src_uncommitted = src.uncommitted();
    let dst_writable = dst.writable();

    let movable = src_uncommitted.len().min(dst_writable.len());
    let unmovable = src_uncommitted.len() - movable;

    dst_writable[..movable].copy_from_slice(&src_uncommitted[..movable]);
    src.commit(movable);
    dst.written(movable);

    if unmovable == 0 {
        ZstdOutcome::Complete(())
    } else {
        ZstdOutcome::HasMore {
            remaining_bytes: unmovable,
        }
    }
}

/// Call a function that uses a [`zstd::stream::raw::OutBuffer`] by wrapping
/// a [`Buffer`]. Returns a tuple containing the function's output and the
/// total uncommitted bytes written to the buffer during the function call.
pub fn with_zstd_out_buffer<R>(
    buffer: &mut impl Buffer,
    f: impl FnOnce(&mut zstd::stream::raw::OutBuffer<'_, [u8]>) -> R,
) -> (R, usize) {
    let mut out_buffer = zstd::stream::raw::OutBuffer::around(buffer.writable());
    let result = f(&mut out_buffer);
    let written = out_buffer.pos();
    buffer.written(written);
    (result, written)
}
