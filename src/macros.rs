#![cfg_attr(
    not(any(feature = "tokio", feature = "futures")),
    expect(unused_macros)
)]

/// Unwrap a [`ZstdOutcome`](crate::ZstdOutcome) value, returning the value
/// from the [`Complete`](crate::ZstdOutcome::Complete) variant, or
/// propagating the [`HasMore`](crate::ZstdOutcome::HasMore) variant to the
/// caller (wrapped with `Ok(_)`).
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

/// Unwrap a [`Poll`](std::task::Poll) value, returning the value from
/// the [`Ready`](std::task::Poll::Ready) variant, or propagating the
/// [`Pending`](std::task::Poll::Pending) variant to the caller.
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
