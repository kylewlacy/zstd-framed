#![allow(unused)]

use std::sync::{Arc, RwLock};

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

pub fn arb_data_framed_with_frame_boundary_pos() -> impl Strategy<Value = (Vec<Data>, usize)> {
    arb_data_framed().prop_flat_map(|frames| {
        let frame_pos = 0..=frames.len();
        frame_pos.prop_flat_map(move |frame_pos| {
            let pos = frames[..frame_pos]
                .iter()
                .map(|frame| frame.len())
                .sum::<usize>();
            (Just(frames.clone()), Just(pos))
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderAction {
    Read(usize),
    Seek(std::io::SeekFrom),
}

pin_project_lite::pin_project! {
    pub struct ReaderWatcher<R> {
        #[pin]
        reader: R,
        actions: Arc<RwLock<Vec<ReaderAction>>>,
    }
}

impl<R> ReaderWatcher<R> {
    pub fn new(reader: R) -> (Self, Arc<RwLock<Vec<ReaderAction>>>) {
        let actions = Arc::new(RwLock::new(Vec::<ReaderAction>::new()));
        (
            Self {
                reader,
                actions: actions.clone(),
            },
            actions,
        )
    }
}

impl<R> std::io::Read for ReaderWatcher<R>
where
    R: std::io::Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let length = self.reader.read(buf)?;
        self.actions
            .write()
            .unwrap()
            .push(ReaderAction::Read(length));
        Ok(length)
    }
}

impl<R> std::io::Seek for ReaderWatcher<R>
where
    R: std::io::Seek,
{
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.actions.write().unwrap().push(ReaderAction::Seek(pos));
        self.reader.seek(pos)
    }
}

impl<R> tokio::io::AsyncRead for ReaderWatcher<R>
where
    R: tokio::io::AsyncRead,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();

        let buf_remaining = buf.remaining();
        futures::ready!(this.reader.poll_read(cx, buf))?;
        let buf_filled = buf_remaining.saturating_sub(buf.remaining());
        this.actions
            .write()
            .unwrap()
            .push(ReaderAction::Read(buf_filled));
        std::task::Poll::Ready(Ok(()))
    }
}

impl<R> tokio::io::AsyncSeek for ReaderWatcher<R>
where
    R: tokio::io::AsyncSeek,
{
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let this = self.project();
        this.actions
            .write()
            .unwrap()
            .push(ReaderAction::Seek(position));
        this.reader.start_seek(position)
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        self.project().reader.poll_complete(cx)
    }
}

impl<R> futures::AsyncRead for ReaderWatcher<R>
where
    R: futures::AsyncRead,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.project();

        let length = futures::ready!(this.reader.poll_read(cx, buf))?;
        this.actions
            .write()
            .unwrap()
            .push(ReaderAction::Read(length));
        std::task::Poll::Ready(Ok(length))
    }
}

impl<R> futures::AsyncSeek for ReaderWatcher<R>
where
    R: futures::AsyncSeek,
{
    fn poll_seek(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        let this = self.project();

        let seek_pos = futures::ready!(this.reader.poll_seek(cx, pos))?;
        this.actions.write().unwrap().push(ReaderAction::Seek(pos));
        std::task::Poll::Ready(Ok(seek_pos))
    }
}
