use crate::ZstdOutcome;

pub trait Buffer {
    fn writable(&mut self) -> &mut [u8];
    fn written(&mut self, len: usize);
    fn uncommitted(&self) -> &[u8];
    fn commit(&mut self, len: usize);
}

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
}

pub fn write_to_buffer(buffer: &mut impl Buffer, data: &[u8]) -> usize {
    let writable = buffer.writable();
    let write_len = writable.len().min(data.len());

    writable[..write_len].copy_from_slice(&data[..write_len]);
    buffer.written(write_len);

    write_len
}

pub fn write_all_to_buffer(buffer: &mut impl Buffer, data: &[u8]) {
    let written = write_to_buffer(buffer, data);

    assert_eq!(data.len(), written, "could not write all data to buffer");
}

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
