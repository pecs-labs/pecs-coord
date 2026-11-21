//! 协调执行器 (Runner)：构建在底层 SPI (`DistributedLock` + `OccurrenceClaim`) 之上的高阶受控执行编排层。
//!
//! 职责边界：
//! - `traits.rs` 仅定义底层驱动必须实现的纯 SPI 契约；
//! - `runner.rs` 封装面向业务调用方的三类核心执行模型：
//!   1. 已持锁句柄的失锁/超时受控执行 (`guard.run` / `guard.run_timeout`)；
//!   2. 一站式临界区闭包执行 (`Coordinator::run_locked`)；
//!   3. 周期任务时间桶双层防重执行 (`TaskSlot` + `Coordinator::run_slot`)。

use crate::traits::{DistributedLock, LockHandle, OccurrenceClaim};
use crate::{CoordError, CoordResult};
use async_trait::async_trait;
use std::future::Future;
use std::time::Duration;

/// 在持有锁 `guard` 期间执行 `work`，实时监听 `guard.lost()`，并可选绑定业务执行超时 `exec_timeout`。
pub(crate) async fn eval_guarded<G, F, T>(
    guard: &G,
    exec_timeout: Option<Duration>,
    work: F,
) -> CoordResult<T>
where
    G: LockHandle + ?Sized,
    F: Future<Output = T>,
{
    if let Some(t) = exec_timeout {
        if t.is_zero() {
            return Err(CoordError::config("execution timeout must be positive"));
        }
    }
    if guard.is_lost() {
        return Err(CoordError::lost("lock ownership lost"));
    }
    match exec_timeout {
        None => tokio::select! {
            biased;
            _ = guard.lost() => Err(CoordError::lost("lock ownership lost")),
            out = work => if guard.is_lost() {
                Err(CoordError::lost("lock ownership lost"))
            } else {
                Ok(out)
            },
        },
        Some(timeout) => tokio::select! {
            biased;
            _ = guard.lost() => Err(CoordError::lost("lock ownership lost")),
            res = tokio::time::timeout(timeout, work) => match res {
                Ok(out) if !guard.is_lost() => Ok(out),
                Ok(_) => Err(CoordError::lost("lock ownership lost")),
                Err(_) => Err(CoordError::timeout("locked execution timed out")),
            },
        },
    }
}

/// 分布式定时任务时间槽 (TaskSlot)
///
/// 统一封装“互斥执行锁 + 时间桶轮次认领 (`claim_once`) + 失锁/超时受控执行”所需的参数。
#[derive(Clone, Debug)]
pub struct TaskSlot {
    /// 互斥执行锁资源路径（如 `"jobs/pecs-core/sync_data"`）
    pub lock_key: String,
    /// 时间桶轮次认领资源路径（如 `"job-runs/pecs-core/sync_data/29934120"`）
    pub claim_key: String,
    /// 对齐后的时间桶轮次编号
    pub slot: i64,
    /// 排队抢互斥锁的最大等待时长
    pub acquire_timeout: Duration,
    /// 抢到锁后业务代码的最大执行时长
    pub exec_timeout: Duration,
    /// 本轮认领记录在后端保留的秒数（`exec_timeout + 2 * cadence_secs`）
    pub retention_secs: i64,
}

impl TaskSlot {
    /// 根据触发时刻 Unix 秒级时间戳 `unix_secs` 与固定周期 `cadence_secs` 计算时间桶轮次编号。
    #[inline]
    pub fn slot_of(unix_secs: i64, cadence_secs: u64) -> CoordResult<i64> {
        if cadence_secs == 0 {
            return Err(CoordError::config("scheduled cadence_secs must be positive"));
        }
        Ok(unix_secs.div_euclid(cadence_secs as i64))
    }

    /// 根据业务最大执行时长 `exec_timeout` 与固定周期 `cadence_secs` 计算认领保留秒数。
    #[inline]
    pub fn retention_of(exec_timeout: Duration, cadence_secs: u64) -> i64 {
        exec_timeout
            .as_secs()
            .saturating_add(cadence_secs.saturating_mul(2))
            .max(1) as i64
    }

    /// 按标准微服务规范 (`jobs/{service}/{task}` 与 `job-runs/{service}/{task}/{slot}`) 构建时间槽。
    ///
    /// # 参数说明 (Arguments)
    /// - `service`: 微服务名（如 `"pecs-core"`），用于隔离不同服务下的同名任务。
    /// - `task`: 稳定的定时任务名（如 `"sync_data"`）。
    /// - `unix_secs`: 本次触发时刻的 Unix 秒级时间戳（在排队抢锁前采样）。
    /// - `cadence_secs`: 固定调度周期秒数（时间桶宽度，必须 `> 0`）。
    /// - `acquire_timeout`: 排队抢锁的最大等待时间（如 `Duration::from_secs(2)`）。
    /// - `exec_timeout`: 抢到锁后业务代码的最大运行超时（如 `Duration::from_secs(3600)`）。
    pub fn new(
        service: &str,
        task: &str,
        unix_secs: i64,
        cadence_secs: u64,
        acquire_timeout: Duration,
        exec_timeout: Duration,
    ) -> CoordResult<Self> {
        let slot = Self::slot_of(unix_secs, cadence_secs)?;
        let retention_secs = Self::retention_of(exec_timeout, cadence_secs);
        Ok(Self {
            lock_key: format!("jobs/{service}/{task}"),
            claim_key: format!("job-runs/{service}/{task}/{slot}"),
            slot,
            acquire_timeout,
            exec_timeout,
            retention_secs,
        })
    }
}

/// 统一协调器特质 (`DistributedLock + OccurrenceClaim`)，内置带锁受控执行 (`run_locked`) 与定时时间槽执行 (`run_slot`)。
#[async_trait]
pub trait Coordinator: DistributedLock + OccurrenceClaim {
    /// 一站式执行带锁临界区：限时抢锁 -> 传入 `fencing_token` 运行业务 -> 实时监听失锁与执行超时 -> 自动确认释放锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"hotlists/collect"`）。
    /// - `acquire_timeout`: **排队抢锁的最大等待时间**（必须 `> 0`）。
    /// - `exec_timeout`: **抢到锁后业务代码的最大执行时长**（必须 `> 0`）。
    /// - `work`: 接收 `fencing_token: i64` 并返回业务 `Future` 的闭包。
    async fn run_locked<F, Fut, T>(
        &self,
        resource: &str,
        acquire_timeout: Duration,
        exec_timeout: Duration,
        work: F,
    ) -> CoordResult<T>
    where
        Self: Sized,
        F: FnOnce(i64) -> Fut + Send,
        Fut: Future<Output = T> + Send,
        T: Send,
    {
        let guard = self.acquire_with_timeout(resource, acquire_timeout).await?;
        let token = guard.fencing_token();
        let exec_res = eval_guarded(guard.as_ref(), Some(exec_timeout), work(token)).await;
        if let Err(err) = guard.release().await {
            tracing::warn!(resource, error = %err, "Distributed lock release failed");
        }
        exec_res
    }

    /// 执行分布式定时任务时间槽 (`TaskSlot`)：限时抢互斥锁 -> `claim_once` 原子认领本轮时间桶 -> 失锁/超时受控执行 -> 自动释放锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `slot`: 本轮定时任务的时间槽参数（参见 [`TaskSlot`]）。
    /// - `work`: 仅在“抢到互斥锁 **且** 首次认领本轮时间桶成功”时才运行的业务 `Future`。
    ///
    /// # 返回值 (Returns)
    /// - `Ok(Some(T))`: 本副本赢得本轮执行权并顺利完成 `work`。
    /// - `Ok(None)`: 本轮时间桶已被其他副本认领，当前副本安全跳过。
    /// - `Err(CoordError)`: 抢锁超时、认领异常、执行途中失锁或业务执行超时。
    async fn run_slot<F, T>(&self, slot: &TaskSlot, work: F) -> CoordResult<Option<T>>
    where
        Self: Sized,
        F: Future<Output = T> + Send,
        T: Send,
    {
        let guard = self
            .acquire_with_timeout(&slot.lock_key, slot.acquire_timeout)
            .await?;
        let claimed = async {
            match self.claim_once(&slot.claim_key, slot.retention_secs).await {
                Ok(true) => Ok(Some(work.await)),
                Ok(false) => Ok(None),
                Err(err) => Err(err),
            }
        };
        let res = eval_guarded(guard.as_ref(), Some(slot.exec_timeout), claimed).await;
        if let Err(err) = guard.release().await {
            tracing::warn!(lock_key = %slot.lock_key, error = %err, "Task lock release failed");
        }
        res?
    }
}

impl<T: DistributedLock + OccurrenceClaim + ?Sized> Coordinator for T {}

impl dyn LockHandle + '_ {
    /// 在持有当前动态锁句柄期间运行异步任务 `work`，并实时监听 [`LockHandle::lost`]（一旦失锁立即中断任务）。
    pub async fn run<F, T>(&self, work: F) -> CoordResult<T>
    where
        F: Future<Output = T>,
    {
        eval_guarded(self, None, work).await
    }

    /// 在持有当前动态锁句柄期间运行异步任务 `work`，同时受实时失锁监听与最大执行时长 (`exec_timeout`) 双重保护。
    pub async fn run_timeout<F, T>(&self, exec_timeout: Duration, work: F) -> CoordResult<T>
    where
        F: Future<Output = T>,
    {
        eval_guarded(self, Some(exec_timeout), work).await
    }
}
