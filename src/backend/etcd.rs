//! Production etcd lease-backed distributed lock and occurrence claim backend.
//!
//! Guarantees:
//! - Mutual exclusion via etcd `v3lockpb.Lock/Lock` bound to a dedicated lease;
//! - Monotonic fencing token derived from the lock key's `create_revision`;
//! - Local monotonic deadline (`LeaseValidity`) preventing stale-owner resurrection;
//! - Automatic auth token refresh on `UNAUTHENTICATED` (gRPC code 16);
//! - Authenticated `unlock` before best-effort `lease_revoke` (works cleanly on auth-enabled
//!   etcd clusters even with `etcd-client 0.19` whose Lease interceptor omits the token).

use crate::config::{connect_etcd, EtcdConfig, LockConfig};
use crate::lease::LeaseValidity;
use crate::traits::{DistributedLock, LockHandle, OccurrenceClaim};
use crate::{CoordError, CoordResult};
use async_trait::async_trait;
use etcd_client::{Client, Compare, GetOptions, LockOptions, PutOptions, Txn, TxnOp};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Type alias for ergonomic usage when etcd is the primary coordination backend.
pub type LockService = EtcdLockService;
/// Type alias for ergonomic usage when etcd is the primary coordination backend.
pub type LockGuard = EtcdLockGuard;

/// Lease-backed distributed lock and occurrence claim service backed by etcd v3.
#[derive(Clone)]
pub struct EtcdLockService {
    client: Arc<tokio::sync::Mutex<Option<Client>>>,
    auth_refresh: Arc<tokio::sync::Mutex<()>>,
    auth_epoch: Arc<AtomicU64>,
    connection: EtcdConfig,
    config: LockConfig,
}

impl std::fmt::Debug for EtcdLockService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdLockService")
            .field("connection", &self.connection)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl EtcdLockService {
    /// 构建基于 etcd v3 的分布式锁与认领服务实例（构造时校验 `config` 不变量，首次调用时惰性建立连接）。
    ///
    /// # 参数说明 (Arguments)
    /// - `connection`: etcd 连接配置（集群地址 `host`、端口 `port`、TLS 证书与账号密码）。
    /// - `config`: 分布式锁参数配置（开关 `enabled`、全局前缀 `prefix`、租约寿命 `lease_ttl_secs`、默认抢锁等待 `acquire_timeout_ms`、单次 RPC 超时 `rpc_timeout_ms`）。
    pub fn new(connection: EtcdConfig, config: LockConfig) -> CoordResult<Self> {
        config.validate()?;
        Ok(Self {
            client: Default::default(),
            auth_refresh: Default::default(),
            auth_epoch: Default::default(),
            connection,
            config,
        })
    }

    fn is_auth_expired(error: &etcd_client::Error) -> bool {
        // gRPC UNAUTHENTICATED = 16. PermissionDenied is not a token expiry.
        matches!(error, etcd_client::Error::GRpcStatus(status) if status.code() as i32 == 16)
    }

    async fn connected_client(&self) -> CoordResult<Client> {
        let mut connection = self.connection.clone();
        connection.timeout = (self.config.rpc_timeout_ms / 1000).max(1);
        let mut cached = self.client.lock().await;
        if cached.is_none() {
            *cached = Some(connect_etcd(&connection).await?);
        }
        Ok(cached.as_ref().unwrap().clone())
    }

    async fn refresh_auth(&self, failed_epoch: u64, rpc: Duration) -> CoordResult<()> {
        let _refresh = self.auth_refresh.lock().await;
        if self.auth_epoch.load(Ordering::Acquire) != failed_epoch {
            return Ok(());
        }
        let client = self.connected_client().await?;
        let res = tokio::time::timeout(rpc, client.refresh_token())
            .await
            .map_err(|_| CoordError::timeout("etcd authentication refresh timeout"))
            .and_then(|r| {
                r.map_err(|error| {
                    CoordError::connection(format!("etcd authentication refresh failed: {error}"))
                })
            });
        if res.is_err() {
            // 若旧连接或会话已彻底失效，清空缓存客户端以便下次调用重新建立连接与认证
            *self.client.lock().await = None;
        }
        res?;
        self.auth_epoch.fetch_add(1, Ordering::Release);
        tracing::info!("etcd coordination authentication refreshed");
        Ok(())
    }

    async fn authenticated<T, F, Fut>(
        &self,
        rpc: Duration,
        timeout_label: &'static str,
        mut operation: F,
    ) -> CoordResult<T>
    where
        F: FnMut(Client) -> Fut,
        Fut: std::future::Future<Output = Result<T, etcd_client::Error>>,
    {
        let epoch = self.auth_epoch.load(Ordering::Acquire);
        for attempt in 0..2 {
            let client = self.connected_client().await?;
            match tokio::time::timeout(rpc, operation(client)).await {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(error))
                    if attempt == 0
                        && Self::is_auth_expired(&error)
                        && self.connection.has_auth_credentials() =>
                {
                    self.refresh_auth(epoch, rpc).await?;
                }
                Ok(Err(error)) => return Err(CoordError::driver(error.to_string())),
                Err(_) => return Err(CoordError::timeout(timeout_label)),
            }
        }
        unreachable!("the second attempt always returns")
    }

    /// 原子认领指定资源 `resource`，并在 `retention_secs` 秒内保留该认领记录（用于定时任务时间桶去重或一次性事件防重）。
    ///
    /// 记录的是“尝试认领”而非“业务执行成功”：即使认领后业务失败或进程崩溃，在租约到期前也不会允许第二次认领。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 认领记录的逻辑资源路径（如 `"job-runs/pecs-core/sync_data/29934120"`），**必须与互斥锁使用不同的资源命名空间**。
    /// - `retention_secs`: 认领记录在 etcd 中的独立租约保留秒数（必须 `> 0`，不自动续租，也不随业务锁释放而清除）。
    pub async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool> {
        if retention_secs <= 0 {
            return Err(CoordError::config("claim retention must be positive"));
        }
        let key = self.config.resource_key(resource)?;
        let rpc = Duration::from_millis(self.config.rpc_timeout_ms);
        let mut client = self.connected_client().await?;
        let lease = self
            .authenticated(rpc, "claim lease grant", |mut c| async move {
                c.lease_grant(retention_secs, None).await
            })
            .await?;
        let lease_id = lease.id();
        let response = self
            .authenticated(rpc, "claim transaction", |mut client| {
                let key = key.clone();
                async move {
                    let txn = Txn::new()
                        .when(vec![Compare::version(
                            key.clone(),
                            etcd_client::CompareOp::Equal,
                            0,
                        )])
                        .and_then(vec![TxnOp::put(
                            key,
                            "claimed",
                            Some(PutOptions::new().with_lease(lease_id)),
                        )]);
                    client.txn(txn).await
                }
            })
            .await?;
        if !response.succeeded() {
            // A definite rejected transaction never attached this lease to a key.
            let _ = tokio::time::timeout(rpc, client.lease_revoke(lease_id)).await;
        }
        Ok(response.succeeded())
    }

    /// 使用配置的默认排队等待时间 (`LockConfig::acquire_timeout_ms`) 获取指定资源的分布式互斥锁。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"inventory/product/42"`），底层自动拼接 `LockConfig::prefix`；禁止包含空段、`.` 或 `..`。
    pub async fn acquire(&self, resource: &str) -> CoordResult<EtcdLockGuard> {
        self.acquire_with_timeout(
            resource,
            Duration::from_millis(self.config.acquire_timeout_ms),
        )
        .await
    }

    /// 使用本次调用显式指定的排队等待预算获取分布式互斥锁（有界排队等待，取消时自动清理排队 key 与租约）。
    ///
    /// # 参数说明 (Arguments)
    /// - `resource`: 逻辑锁资源路径（如 `"jobs/pecs-core/sync_data"`），禁止包含空段、`.` 或 `..`。
    /// - `wait_timeout`: **排队等待获取锁的最大时长**（必须 `> 0`，覆盖 etcd `Lock` 竞争与所有权校验；连接与租约申请受 `rpc_timeout_ms` 独立控制）。
    pub async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> CoordResult<EtcdLockGuard> {
        if wait_timeout.is_zero() {
            return Err(CoordError::config("lock wait timeout must be positive"));
        }
        let name = self.config.resource_key(resource)?;
        let rpc = Duration::from_millis(self.config.rpc_timeout_ms);
        let ttl_secs = self.config.lease_ttl_secs;
        let grant_started = tokio::time::Instant::now();
        let lease = self
            .authenticated(rpc, "lock lease grant", |mut c| async move {
                c.lease_grant(ttl_secs, None).await
            })
            .await?;
        let mut client = self.connected_client().await?;
        let lease_id = lease.id();
        let lock_key = Arc::new(std::sync::Mutex::new(
            format!("{name}/{lease_id:x}").into_bytes(),
        ));
        let stop = CancellationToken::new();
        let validity = Arc::new(LeaseValidity::new(grant_started, lease.ttl())?);
        let task = spawn_lease(
            self.clone(),
            client.clone(),
            lease_id,
            lease.ttl(),
            rpc,
            stop.clone(),
            validity.clone(),
            lock_key.clone(),
        );
        // Guard exists before the first cancellable await following lease allocation.
        let mut guard = EtcdLockGuard {
            resource: name.clone(),
            lease_id,
            stop,
            validity,
            task: Some(task),
            token: 0,
        };
        let acquire = async {
            let epoch = self.auth_epoch.load(Ordering::Acquire);
            let response = match client
                .lock(name.clone(), Some(LockOptions::new().with_lease(lease_id)))
                .await
            {
                Ok(resp) => resp,
                Err(error)
                    if Self::is_auth_expired(&error) && self.connection.has_auth_credentials() =>
                {
                    self.refresh_auth(epoch, rpc).await?;
                    let mut refreshed = self.connected_client().await?;
                    refreshed
                        .lock(name, Some(LockOptions::new().with_lease(lease_id)))
                        .await
                        .map_err(|e| CoordError::driver(e.to_string()))?
                }
                Err(error) => return Err(CoordError::driver(error.to_string())),
            };
            let key = response.key().to_vec();
            *lock_key.lock().unwrap() = key.clone();
            let result = self
                .authenticated(rpc, "lock ownership read", |mut client| {
                    let key = key.clone();
                    async move { client.get(key, Some(GetOptions::new())).await }
                })
                .await?;
            let kv = result
                .kvs()
                .first()
                .ok_or_else(|| CoordError::lost("lock ownership lost"))?;
            if kv.lease() != lease_id {
                return Err(CoordError::lost("lock lease mismatch"));
            }
            Ok(kv.create_revision())
        };
        guard.token = tokio::select! {
            biased;
            _ = guard.lost() => return Err(CoordError::lost("lock lease lost")),
            result = tokio::time::timeout(wait_timeout, acquire) =>
                result.map_err(|_| CoordError::timeout("lock acquisition"))??,
        };
        if guard.is_lost() {
            return Err(CoordError::lost("lock lease lost"));
        }
        Ok(guard)
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

/// Active ownership guard for [`EtcdLockService`].
///
/// Not `Clone`: one acquisition owns exactly one lease. Drop schedules best-effort cleanup;
/// [`EtcdLockGuard::release`] awaits server confirmation. Lost guards can never be revived.
#[must_use = "dropping the guard releases the distributed lock"]
pub struct EtcdLockGuard {
    resource: String,
    lease_id: i64,
    stop: CancellationToken,
    validity: Arc<LeaseValidity>,
    task: Option<JoinHandle<CoordResult<()>>>,
    token: i64,
}

impl EtcdLockGuard {
    /// 当前锁保护的完整规范化资源路径（`{prefix}/{resource}`）。
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// 底层 etcd 租约 ID (`lease_id`)。
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }

    /// 当前本地单调时钟下的剩余租约确认有效时长（已失锁时返回 `Duration::ZERO`）。
    pub fn remaining_validity(&self) -> Duration {
        self.validity.remaining()
    }

    pub fn fencing_token(&self) -> i64 {
        self.token
    }

    /// Resolves on renewal failure or local monotonic expiry, including after a pause.
    pub async fn lost(&self) {
        self.validity.wait_lost().await;
    }

    pub fn is_lost(&self) -> bool {
        self.validity.is_lost()
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

    pub async fn release(mut self) -> CoordResult<()> {
        self.stop.cancel();
        self.task
            .take()
            .unwrap()
            .await
            .map_err(|e| CoordError::other(e.to_string()))?
    }
}

impl Drop for EtcdLockGuard {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl std::fmt::Debug for EtcdLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdLockGuard")
            .field("resource", &self.resource)
            .field("lease_id", &self.lease_id)
            .field("fencing_token", &self.token)
            .field("is_lost", &self.is_lost())
            .finish()
    }
}

#[async_trait]
impl LockHandle for EtcdLockGuard {
    fn resource(&self) -> &str {
        EtcdLockGuard::resource(self)
    }

    fn fencing_token(&self) -> i64 {
        EtcdLockGuard::fencing_token(self)
    }

    fn is_lost(&self) -> bool {
        EtcdLockGuard::is_lost(self)
    }

    async fn lost(&self) {
        EtcdLockGuard::lost(self).await;
    }

    async fn release(self: Box<Self>) -> CoordResult<()> {
        EtcdLockGuard::release(*self).await
    }
}

#[async_trait]
impl DistributedLock for EtcdLockService {
    async fn acquire(&self, resource: &str) -> CoordResult<Box<dyn LockHandle>> {
        let guard = EtcdLockService::acquire(self, resource).await?;
        Ok(Box::new(guard))
    }

    async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> CoordResult<Box<dyn LockHandle>> {
        let guard = EtcdLockService::acquire_with_timeout(self, resource, wait_timeout).await?;
        Ok(Box::new(guard))
    }
}

#[async_trait]
impl OccurrenceClaim for EtcdLockService {
    async fn claim_once(&self, resource: &str, retention_secs: i64) -> CoordResult<bool> {
        EtcdLockService::claim_once(self, resource, retention_secs).await
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_lease(
    service: EtcdLockService,
    mut client: Client,
    id: i64,
    ttl: i64,
    rpc: Duration,
    stop: CancellationToken,
    validity: Arc<LeaseValidity>,
    lock_key: Arc<std::sync::Mutex<Vec<u8>>>,
) -> JoinHandle<CoordResult<()>> {
    tokio::spawn(async move {
        let renew = async {
            let setup_sent = tokio::time::Instant::now();
            let setup_deadline = validity.deadline().min(setup_sent + rpc);
            let (mut keeper, mut stream) =
                tokio::time::timeout_at(setup_deadline, client.lease_keep_alive(id))
                    .await
                    .map_err(|_| CoordError::timeout("lock keepalive setup"))?
                    .map_err(|e| CoordError::driver(e.to_string()))?;
            // etcd-client consumes the first KeepAlive confirmation during setup.
            validity.confirm(setup_sent, ttl)?;
            let mut interval = tokio::time::interval(Duration::from_secs((ttl / 3).max(1) as u64));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending = None;
            loop {
                if validity.is_lost() {
                    return Err(CoordError::lost("lock confirmation expired"));
                }
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(validity.deadline()) => {
                        return Err::<(), _>(CoordError::timeout("lock renewal acknowledgement"));
                    }
                    reply = stream.message() => {
                        let reply = reply
                            .map_err(|e| CoordError::driver(e.to_string()))?
                            .filter(|r| r.id() == id && r.ttl() > 0)
                            .ok_or_else(|| CoordError::lost("lock lease expired"))?;
                        let sent = pending
                            .take()
                            .ok_or_else(|| CoordError::driver("unexpected lock renewal acknowledgement"))?;
                        validity.confirm(sent, reply.ttl())?;
                    }
                    _ = interval.tick(), if pending.is_none() => {
                        let sent = tokio::time::Instant::now();
                        tokio::time::timeout_at(
                            validity.deadline().min(sent + rpc),
                            keeper.keep_alive(),
                        )
                        .await
                        .map_err(|_| CoordError::timeout("lock renewal"))?
                        .map_err(|e| CoordError::driver(e.to_string()))?;
                        pending = Some(sent);
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = stop.cancelled() => {},
            _ = validity.cancelled() => {},
            result = renew => {
                if let Err(error) = result {
                    tracing::warn!(%error, "Distributed lock lost");
                }
            }
        }
        validity.cancel();
        // Unlock carries authentication even on etcd-client versions whose Lease RPCs
        // omit the auth token (e.g. etcd-client 0.19). Removing the lock key first
        // immediately releases ownership for contenders; best-effort lease_revoke then
        // reclaims the empty lease (or lets it expire harmlessly by TTL).
        let key = lock_key.lock().unwrap().clone();
        let unlock_res = service
            .authenticated(rpc, "lock unlock", |mut client| {
                let key = key.clone();
                async move { client.unlock(key).await }
            })
            .await;

        let revoke_res = tokio::time::timeout(rpc, client.lease_revoke(id))
            .await
            .map_err(|_| CoordError::timeout("lock release"))
            .and_then(|result| result.map_err(|e| CoordError::driver(e.to_string())));

        match (unlock_res, revoke_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (Ok(_), Err(error)) => {
                tracing::debug!(
                    %error,
                    lease_id = id,
                    "Lock key unlocked; empty lease will expire by TTL"
                );
                Ok(())
            }
            (Err(error), Ok(_)) => {
                tracing::debug!(
                    %error,
                    lease_id = id,
                    "Lock unlock failed but lease revoked cleanly"
                );
                Ok(())
            }
            (Err(unlock_err), Err(revoke_err)) => {
                tracing::warn!(
                    unlock_error = %unlock_err,
                    revoke_error = %revoke_err,
                    lease_id = id,
                    "Lock cleanup failed; waiting for lease expiry"
                );
                Err(unlock_err)
            }
        }
    })
}
