#![cfg_attr(
    not(any(feature = "tokio", feature = "futures")),
    expect(unused_macros)
)]

macro_rules! complete_ok {
    ($e:expr) => {
        match $e {
            crate::ZstdOutcome::HasMore { remaining_bytes } => {
                return ::std::result::Result::Ok(crate::ZstdOutcome::HasMore { remaining_bytes })
            }
            crate::ZstdOutcome::Complete(value) => value,
        }
    };
}

macro_rules! ready {
    ($e:expr) => {
        match $e {
            std::task::Poll::Pending => {
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(value) => value,
        }
    };
}
