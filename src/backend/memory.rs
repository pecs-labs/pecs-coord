//! In-memory coordination backend for unit tests and local examples.
//!
//! Never use this backend as a silent fallback for multi-replica production locks.

use crate::config::LockConfig;
use crate::traits::{DistributedLock, LockHandle, OccurrenceClaim};
use crate::{CoordError, CoordResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Inner {
    locks: Mutex<HashMap<String, Arc<Semaphore>>>,
    claims: Mutex<HashMap<String, Instant>>,
    revision: AtomicI64,
}

/// Process-local coordination service implementing [`DistributedLock`] and [`OccurrenceClaim`].
///
/// Generates strictly monotonic fencing tokens and honors bounded wait timeouts,
/// resource validation, and claim retention windows.
#[derive(Clone)]
pub struct MemoryLockService {
    inner: Arc<Inner>,
    config: LockConfig,
}

impl std::fmt::Debug for MemoryLockService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLockService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl MemoryLockService {
    /// Construct a new in-memory coordination service with explicit config validation.
    pub fn new(config: LockConfig) -> CoordResult<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner::default()),
            config,
        })
    }

    /// Convenience constructor for tests with `enabled = true` and default prefix/timeouts.
    pub fn enabled() -> Self {
        Self::new(LockConfig {
            enabled: true,
            ..LockConfig::default()
        })
        .expect("default enabled LockConfig is valid")
    }

    /// Atomically claim a named occurrence for `retention_secs`.
    pub async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool> {
        if retention_secs <= 0 {
            return Err(CoordError::config("claim retention must be positive"));
        }
        let key = self.config.resource_key(resource)?;
        let now = Instant::now();
        let expires_at = now
            .checked_add(Duration::from_secs(retention_secs as u64))
            .ok_or_else(|| CoordError::config("claim retention exceeds supported clock range"))?;
        let mut claims = self.inner.claims.lock().unwrap();
        claims.retain(|_, exp| *exp > now);
        if claims.contains_key(&key) {
            return Ok(false);
        }
        claims.insert(key, expires_at);
        Ok(true)
    }

    /// Acquire a lock using the configured default wait budget.
    pub async fn acquire(&self, resource: &str) -> CoordResult<MemoryLockGuard> {
        self.acquire_with_timeout(
            resource,
            Duration::from_millis(self.config.acquire_timeout_ms),
        )
        .await
    }

    /// Acquire a lock with an explicit bounded wait budget.
    pub async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> CoordResult<MemoryLockGuard> {
        if wait_timeout.is_zero() {
            return Err(CoordError::config("lock wait timeout must be positive"));
        }
        let key = self.config.resource_key(resource)?;
        let sem = {
            let mut map = self.inner.locks.lock().unwrap();
            map.entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        let permit = tokio::time::timeout(wait_timeout, sem.acquire_owned())
            .await
            .map_err(|_| CoordError::timeout("lock acquisition"))?
            .map_err(|_| CoordError::driver("memory semaphore closed"))?;
        let token = self.inner.revision.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(MemoryLockGuard {
            _permit: Some(permit),
            lost: CancellationToken::new(),
            token,
        })
    }
}

/// Active ownership guard for [`MemoryLockService`].
#[must_use = "dropping the guard releases the lock"]
pub struct MemoryLockGuard {
    _permit: Option<OwnedSemaphorePermit>,
    lost: CancellationToken,
    token: i64,
}

impl std::fmt::Debug for MemoryLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLockGuard")
            .field("fencing_token", &self.token)
            .field("is_lost", &self.is_lost())
            .finish()
    }
}

impl MemoryLockGuard {
    pub fn fencing_token(&self) -> i64 {
        self.token
    }

    pub fn is_lost(&self) -> bool {
        self.lost.is_cancelled()
    }

    pub async fn lost(&self) {
        self.lost.cancelled().await;
    }

    /// Simulate external lease revocation in unit tests.
    pub fn force_lose(&mut self) {
        self._permit.take();
        self.lost.cancel();
    }

    pub async fn release(mut self) -> CoordResult<()> {
        self._permit.take();
        self.lost.cancel();
        Ok(())
    }
}

#[async_trait]
impl LockHandle for MemoryLockGuard {
    fn fencing_token(&self) -> i64 {
        MemoryLockGuard::fencing_token(self)
    }

    fn is_lost(&self) -> bool {
        MemoryLockGuard::is_lost(self)
    }

    async fn lost(&self) {
        MemoryLockGuard::lost(self).await;
    }

    async fn release(self: Box<Self>) -> CoordResult<()> {
        MemoryLockGuard::release(*self).await
    }
}

#[async_trait]
impl DistributedLock for MemoryLockService {
    async fn acquire(&self, resource: &str) -> CoordResult<Box<dyn LockHandle>> {
        let guard = MemoryLockService::acquire(self, resource).await?;
        Ok(Box::new(guard))
    }

    async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> CoordResult<Box<dyn LockHandle>> {
        let guard = MemoryLockService::acquire_with_timeout(self, resource, wait_timeout).await?;
        Ok(Box::new(guard))
    }
}

#[async_trait]
impl OccurrenceClaim for MemoryLockService {
    async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool> {
        MemoryLockService::claim_once(self, resource, retention_secs).await
    }
}
