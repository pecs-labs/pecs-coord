use crate::CoordResult;
use async_trait::async_trait;
use std::time::Duration;

/// Active ownership handle for a distributed mutual-exclusion lock.
///
/// Implementations are **not `Clone`**: one acquisition owns exactly one lease/permit.
/// Dropping the handle schedules best-effort cleanup; calling [`LockHandle::release`]
/// awaits server confirmation.
#[async_trait]
pub trait LockHandle: Send + Sync {
    /// Strictly increasing fencing token assigned when ownership was granted.
    ///
    /// Downstream storage layers must atomically reject writes carrying a stale token.
    fn fencing_token(&self) -> i64;

    /// Returns `true` once lease renewal fails or local monotonic validity expires.
    /// A lost handle can never be revived.
    fn is_lost(&self) -> bool;

    /// Resolves as soon as ownership is lost (renewal failure or local deadline expiry).
    async fn lost(&self);

    /// Explicitly release ownership and await backend confirmation.
    async fn release(self: Box<Self>) -> CoordResult<()>;
}

/// Pluggable SPI for lease-backed distributed mutual exclusion.
#[async_trait]
pub trait DistributedLock: Send + Sync {
    /// Acquire a lock using the service's default wait budget.
    async fn acquire(&self, resource: &str) -> CoordResult<Box<dyn LockHandle>>;

    /// Acquire a lock with an explicit bounded wait budget.
    async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> CoordResult<Box<dyn LockHandle>>;
}

/// Pluggable SPI for atomic, bounded-retention occurrence deduplication.
///
/// Unlike a mutual-exclusion lock, a claim records an *attempt* (not completion) for
/// `retention_secs` and does not renew or release when work finishes.
#[async_trait]
pub trait OccurrenceClaim: Send + Sync {
    /// Atomically claim `resource` for `retention_secs`.
    ///
    /// - `Ok(true)`: this call won the claim and may proceed.
    /// - `Ok(false)`: already claimed within the retention window.
    /// - `Err(_)`: ambiguous or unavailable backend; callers must **not** proceed.
    async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool>;
}
