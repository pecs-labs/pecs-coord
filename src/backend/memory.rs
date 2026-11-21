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
    /// 构建进程内内存协调服务实例（用于单元测试与本地示例，构造时执行 `config.validate()` 校验）。
    ///
    /// # 参数说明 (Arguments)
    /// - `config`: 分布式锁参数配置（开关 `enabled`、全局前缀 `prefix`、租约寿命 `lease_ttl_secs`、默认抢锁等待 `acquire_timeout_ms`、RPC 超时 `rpc_timeout_ms`）。
    pub fn new(config: LockConfig) -> CoordResult<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner::default()),
            config,
        })
    }

    /// 便捷构造函数：创建默认已开启 (`enabled = true`) 的内存协调服务实例（专用于单元测试与示例）。
    pub fn enabled() -> Self {
        Self::new(LockConfig {
            enabled: true,
            ..LockConfig::default()
        })
        .expect("default enabled LockConfig is valid")
    }

    /// 原子认领指定资源 `resource`，并在 `retention_secs` 秒内保留该认领记录（用于定时任务时间桶去重或一次性事件防重）。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 认领记录的逻辑资源路径（如 `"job-runs/pecs-core/sync_data/29934120"`），**必须与互斥锁使用不同的资源命名空间**。
    /// - `retention_secs`: 认领记录在内存中的保留秒数（必须 `> 0`，过期前再次认领返回 `Ok(false)`）。
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

    /// 使用配置的默认排队等待时间 (`LockConfig::acquire_timeout_ms`) 获取指定资源的互斥锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"inventory/product/42"`），禁止包含空段、`.` 或 `..`。
    pub async fn acquire(&self, resource: &str) -> CoordResult<MemoryLockGuard> {
        self.acquire_with_timeout(
            resource,
            Duration::from_millis(self.config.acquire_timeout_ms),
        )
        .await
    }

    /// 使用本次调用显式指定的排队等待预算获取互斥锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"jobs/pecs-core/sync_data"`），禁止包含空段、`.` 或 `..`。
    /// - `wait_timeout`: **排队等待获取锁的最大时长**（必须 `> 0`，超时仍未抢到锁则返回 `CoordError::Timeout`）。
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
            // 自动清理无人持有且无排队者的空闲信号量，防止海量动态资源 ID 导致内存增长
            if map.len() >= 1024 && !map.contains_key(&key) {
                map.retain(|_, s| Arc::strong_count(s) > 1 || s.available_permits() == 0);
            }
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        let permit = tokio::time::timeout(wait_timeout, sem.acquire_owned())
            .await
            .map_err(|_| CoordError::timeout("lock acquisition"))?
            .map_err(|_| CoordError::driver("memory semaphore closed"))?;
        let token = self.inner.revision.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(MemoryLockGuard {
            resource: key,
            permit: Mutex::new(Some(permit)),
            lost: CancellationToken::new(),
            token,
        })
    }

    /// 获取当前锁服务的配置引用。
    pub fn config(&self) -> &LockConfig {
        &self.config
    }

    /// 当前锁服务是否处于启用状态 (`config.enabled`)。
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// 一站式执行带锁临界区（无需显式导入 [`crate::Coordinator`] Trait 即可直接调用）。
    pub async fn run_locked<F, Fut, T>(
        &self,
        resource: &str,
        acquire_timeout: Duration,
        exec_timeout: Duration,
        work: F,
    ) -> CoordResult<T>
    where
        F: FnOnce(i64) -> Fut + Send,
        Fut: std::future::Future<Output = T> + Send,
        T: Send,
    {
        crate::runner::Coordinator::run_locked(
            self,
            resource,
            acquire_timeout,
            exec_timeout,
            work,
        )
        .await
    }

    /// 执行分布式定时任务时间槽（无需显式导入 [`crate::Coordinator`] Trait 即可直接调用）。
    pub async fn run_slot<F, T>(
        &self,
        slot: &crate::runner::TaskSlot,
        work: F,
    ) -> CoordResult<Option<T>>
    where
        F: std::future::Future<Output = T> + Send,
        T: Send,
    {
        crate::runner::Coordinator::run_slot(self, slot, work).await
    }
}

/// Active ownership guard for [`MemoryLockService`].
#[must_use = "dropping the guard releases the lock"]
pub struct MemoryLockGuard {
    resource: String,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    lost: CancellationToken,
    token: i64,
}

impl std::fmt::Debug for MemoryLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLockGuard")
            .field("resource", &self.resource)
            .field("fencing_token", &self.token)
            .field("is_lost", &self.is_lost())
            .finish()
    }
}

impl MemoryLockGuard {
    /// 当前锁保护的完整规范化资源路径（`{prefix}/{resource}`）。
    pub fn resource(&self) -> &str {
        &self.resource
    }

    pub fn fencing_token(&self) -> i64 {
        self.token
    }

    pub fn is_lost(&self) -> bool {
        self.lost.is_cancelled()
    }

    pub async fn lost(&self) {
        self.lost.cancelled().await;
    }

    /// 在持有当前锁期间运行异步任务 `work`，并实时监听 [`Self::lost`]（一旦失锁立即中断任务）。
    ///
    /// # 参数说明 (Arguments)
    /// - `work`: 需要在锁保护下执行的业务异步任务 (`Future`)。
    pub async fn run<F, T>(&self, work: F) -> CoordResult<T>
    where
        F: std::future::Future<Output = T>,
    {
        crate::runner::eval_guarded(self, None, work).await
    }

    /// 在持有当前锁期间运行异步任务 `work`，同时受实时失锁监听 ([`Self::lost`]) 与最大执行时长 (`exec_timeout`) 双重保护。
    ///
    /// # 参数说明 (Arguments)
    /// - `exec_timeout`: 业务代码允许运行的最大超时时长（必须 `> 0`）。
    /// - `work`: 需要在锁保护下执行的业务异步任务 (`Future`)。
    pub async fn run_timeout<F, T>(&self, exec_timeout: Duration, work: F) -> CoordResult<T>
    where
        F: std::future::Future<Output = T>,
    {
        crate::runner::eval_guarded(self, Some(exec_timeout), work).await
    }

    /// 在单测中模拟外部租约失效（仅需 `&self`，支持在 `guard.run(...)` 运行中途从外部触发失锁）。
    pub fn force_lose(&self) {
        self.permit.lock().unwrap().take();
        self.lost.cancel();
    }

    pub async fn release(self) -> CoordResult<()> {
        self.permit.lock().unwrap().take();
        self.lost.cancel();
        Ok(())
    }
}

#[async_trait]
impl LockHandle for MemoryLockGuard {
    fn resource(&self) -> &str {
        MemoryLockGuard::resource(self)
    }

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
