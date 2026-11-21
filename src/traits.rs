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
    /// 当前锁保护的完整规范化资源路径（`{prefix}/{resource}`）。
    fn resource(&self) -> &str;

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
    /// 使用配置的默认等待时间 (`LockConfig::acquire_timeout_ms`) 获取指定资源的分布式互斥锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"inventory/product/42"`），底层自动拼接 `LockConfig::prefix`；禁止包含空段、`.` 或 `..`。
    async fn acquire(&self, resource: &str) -> CoordResult<Box<dyn LockHandle>>;

    /// 使用本次调用显式指定的排队等待预算获取分布式互斥锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"jobs/pecs-core/sync_data"`），禁止包含空段、`.` 或 `..`。
    /// - `wait_timeout`: 排队等待获取锁的最大时长（必须 `> 0`；注意这是**等锁超时**，不是拿到锁后的业务执行超时，也不是租约 TTL）。
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
    /// 原子认领指定资源 `resource`，并在 `retention_secs` 秒内保留该认领记录（用于定时任务时间桶去重或一次性事件防重）。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 认领记录的逻辑资源路径（如 `"job-runs/pecs-core/sync_data/29934120"`），**必须与互斥锁使用不同的命名空间**。
    /// - `retention_secs`: 认领记录在后端保留的秒数（必须 `> 0`）；在此期间任何副本再次认领同一 `resource` 均返回 `Ok(false)`，不随业务锁释放而提前删除。
    ///
    /// # 返回值 (Returns)
    /// - `Ok(true)`: 当前调用首次认领成功，可以继续执行业务。
    /// - `Ok(false)`: 在保留期内已被认领过，当前调用应直接跳过。
    /// - `Err(_)`: 后端不可用或 RPC 结果不确定（服务端可能已写入，调用方**严禁**继续执行业务）。
    async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool>;
}
