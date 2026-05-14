use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use strum::{EnumCount, VariantArray};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, VariantArray, EnumCount)]
pub(crate) enum Timer {
    /// When to send an ack-eliciting probe packet or declare unacked packets
    /// lost
    LossDetection  = 0,
    /// When to close the connection after no activity
    Idle           = 1,
    /// When the close timer expires, the connection has been gracefully
    /// terminated.
    Close          = 2,
    /// When keys are discarded because they should not be needed anymore
    KeyDiscard     = 3,
    /// When to give up on validating a new path to the peer
    PathValidation = 4,
    /// When to send a `PING` frame to keep the connection alive
    KeepAlive      = 5,
    /// When pacing will allow us to send a packet
    Pacing         = 6,
    /// When to invalidate old CID and proactively push new one via
    /// NEW_CONNECTION_ID frame
    PushNewCid     = 7,
    /// When to send an immediate ACK if there are unacked ack-eliciting packets
    /// of the peer
    MaxAckDelay    = 8,
}

/// A table of data associated with each distinct kind of `Timer`
#[derive(Debug, Clone, Default)]
pub(crate) struct TimerTable {
    data: [Option<Instant>; Timer::COUNT],
    waker: Option<Waker>,
}

impl TimerTable {
    /// Call this after change any of the timer
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        };
    }

    pub(super) fn set(&mut self, timer: Timer, time: Instant) {
        self.data[timer as usize] = Some(time);
        self.wake();
    }

    pub(super) fn stop(&mut self, timer: Timer) {
        self.data[timer as usize] = None;
        self.wake();
    }
}

impl Future for TimerTable {
    type Output = Timer;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.waker = Some(cx.waker().clone());

        let now = Instant::now();
        let Some((timer_id, earlist)) = self
            .data
            .iter()
            .enumerate()
            .filter_map(|(timer_id, time)| time.map(|time| (timer_id, time)))
            .min_by_key(|x| x.1)
        else {
            return Poll::Pending;
        };

        if now >= earlist {
            self.data[timer_id] = None;
            debug!(
                "Timer {:?} should be triggered {:?} ago.",
                Timer::VARIANTS[timer_id],
                earlist.elapsed()
            );
            return Poll::Ready(Timer::VARIANTS[timer_id]);
        }
        let waker = cx.waker().clone();

        tokio::spawn(async move {
            sleep_until(earlist).await;
            waker.wake();
        });

        Poll::Pending
    }
}
