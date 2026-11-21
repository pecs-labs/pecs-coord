use crate::{CoordError, CoordResult};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Monotonic local validity guard for lease-backed locks.
///
/// A confirmation cannot revive a locally expired owner. Validity is computed from the
/// request *send* time (not response arrival time), so delayed or buffered replies
/// cannot extend an expired lease.
#[cfg_attr(not(feature = "etcd"), allow(dead_code))]
pub(crate) struct LeaseValidity {
    deadline: std::sync::Mutex<tokio::time::Instant>,
    lost: CancellationToken,
}

#[cfg_attr(not(feature = "etcd"), allow(dead_code))]
impl LeaseValidity {
    pub(crate) fn new(sent: tokio::time::Instant, ttl: i64) -> CoordResult<Self> {
        Ok(Self {
            deadline: std::sync::Mutex::new(Self::expires_at(sent, ttl)?),
            lost: CancellationToken::new(),
        })
    }

    pub(crate) fn expires_at(
        sent: tokio::time::Instant,
        ttl: i64,
    ) -> CoordResult<tokio::time::Instant> {
        if ttl <= 0 {
            return Err(CoordError::lost("lock lease expired"));
        }
        sent.checked_add(Duration::from_secs(
            (ttl.saturating_mul(2) / 3).max(1) as u64
        ))
        .ok_or_else(|| CoordError::config("lock TTL exceeds supported clock range"))
    }

    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        *self.deadline.lock().unwrap()
    }

    /// 返回当前本地单调时钟下的剩余有效时长（已失锁或过期时返回 `Duration::ZERO`）。
    pub(crate) fn remaining(&self) -> Duration {
        if self.is_lost() {
            return Duration::ZERO;
        }
        self.deadline()
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(Duration::ZERO)
    }

    pub(crate) fn cancel(&self) {
        self.lost.cancel();
    }

    pub(crate) fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.lost.cancelled()
    }

    pub(crate) fn is_lost(&self) -> bool {
        if tokio::time::Instant::now() >= self.deadline() {
            self.lost.cancel();
        }
        self.lost.is_cancelled()
    }

    pub(crate) fn confirm(&self, sent: tokio::time::Instant, ttl: i64) -> CoordResult<()> {
        let next = Self::expires_at(sent, ttl)?;
        if self.is_lost() || tokio::time::Instant::now() >= next {
            self.lost.cancel();
            return Err(CoordError::lost("late lock renewal acknowledgement"));
        }
        *self.deadline.lock().unwrap() = next;
        Ok(())
    }

    pub(crate) async fn wait_lost(&self) {
        loop {
            if self.is_lost() {
                return;
            }
            let deadline = self.deadline();
            tokio::select! {
                _ = self.lost.cancelled() => return,
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn delayed_confirmation_cannot_revive_expired_owner() {
        let sent = tokio::time::Instant::now();
        let validity = LeaseValidity::new(sent, 30).unwrap();
        tokio::time::advance(Duration::from_secs(21)).await;
        assert!(validity.confirm(sent, 30).is_err());
        assert!(validity.is_lost());
        assert!(validity.confirm(tokio::time::Instant::now(), 30).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_extends_monotonic_deadline() {
        let t0 = tokio::time::Instant::now();
        let validity = LeaseValidity::new(t0, 30).unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        let t1 = tokio::time::Instant::now();
        validity.confirm(t1, 30).unwrap();
        tokio::time::advance(Duration::from_secs(15)).await;
        assert!(!validity.is_lost());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(validity.is_lost());
    }
}
