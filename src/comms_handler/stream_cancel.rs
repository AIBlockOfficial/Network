use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

/// Stream stopping when until become ready.
#[derive(Clone, Debug)]
pub struct TakeUntil<S, F> {
    stream: S,
    until: F,
}

/// Trait implementing canceling on future ready.
pub trait StreamCancel: Stream {
    /// Stream also complete if until complete.
    fn take_until<F>(self, until: F) -> TakeUntil<Self, F>
    where
        F: Future<Output = ()>,
        Self: Sized,
    {
        TakeUntil {
            stream: self,
            until,
        }
    }
}

impl<S> StreamCancel for S where S: Stream {}

impl<S, F> Stream for TakeUntil<S, F>
where
    S: Stream + std::marker::Unpin,
    F: Future<Output = ()> + std::marker::Unpin,
{
    type Item = S::Item;

    /// Complete If `until` ready, otherwise process inner stream.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if Pin::new(&mut self.until).poll(cx).is_ready() {
            Poll::Ready(None)
        } else {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}
