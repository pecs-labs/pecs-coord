# pecs-coord 设计规范 (Design Specification)

本文档定义 `pecs-coord` 的核心架构约束、安全不变量与后端扩展 SPI 规范。任何新增协调原语或后端实现（如 Redis、PostgreSQL、选主、信号量）都必须遵守本规范。

---

## 1. 定位与边界

`pecs-coord` 是面向 Rust 异步微服务的**分布式协调原语库**，与服务注册发现（`pecs-registry`）完全解耦：

- **负责**：基于租约的互斥锁（`DistributedLock` / `LockHandle`）、有界保留期的一次性认领（`OccurrenceClaim`），以及后续可扩展的 Leader 选举、分布式信号量等协调原语。
- **不负责**：服务注册/发现、负载均衡、配置中心或持久化状态存储。
- **零隐式降级红线**：生产后端（如 `etcd`）关闭或不可用时必须显式返回 `Err(CoordError::...)`，**严禁**静默回退到进程内 `MemoryLockService`。

---

## 2. 核心安全不变量 (Safety Invariants)

### 2.1 单调递增 Fencing Token
- 每次成功获取锁都必须返回一个全局单调递增的 `fencing_token: i64`（etcd 后端使用锁 key 的 `create_revision`；memory 后端使用 `AtomicI64` 递增序列）。
- 锁本身只能降低并发冲突概率，**无法阻止进程 GC/暂停后的迟到写请求**；下游存储层必须结合 `fencing_token` 做原子条件更新（如 `WHERE version < $token`）或幂等约束。

### 2.2 本地单调时钟有效期 (`LeaseValidity`)
- 租约有效期必须基于 `tokio::time::Instant`（单调时钟），从**续租请求发出时刻**起算 `max(1, ttl * 2 / 3)`，而非响应到达时刻，防止缓冲/迟到响应“复活”已过期持有者。
- 一旦本地截止时间到达或续租失败，`is_lost()` 永久变为 `true` 且 `lost()` 立即唤醒，已失锁的 Guard **永不可恢复**。
- 配置校验强制约束：`lease_ttl_secs >= 3` 且 `rpc_timeout_ms < lease_ttl_secs * 1000 / 3`。

### 2.3 所有权不可克隆与确定性释放
- `LockHandle` / `EtcdLockGuard` / `MemoryLockGuard` 均**不实现 `Clone`**，保证单次获取唯一对应一个租约与清理任务。
- `Drop` 仅触发异步尽力清理（无法等待确认）；需要服务端确认释放时，调用方必须显式 `await guard.release()`。

### 2.4 `OccurrenceClaim`（一次性认领）与锁的语义隔离
- `claim_once(resource, retention_secs)` 记录的是**“尝试认领”**而非“执行成功”：
  - 使用独立租约，不自动续租，也不随业务锁释放而清除；
  - 必须与业务互斥锁使用不同的 `resource` 命名空间；
  - RPC 超时或不确定错误时，服务端可能已提交，调用方必须跳过执行，绝不能当作“未认领”继续跑。

---

## 3. etcd 后端协议规范 (`src/backend/etcd.rs`)

1. **锁获取与验证**：
   - 先 `lease_grant` 并立即启动后台 `spawn_lease` 保活任务（在第一个可取消的 `.await` 之前创建 Guard，避免取消泄漏租约）；
   - 通过 `v3lockpb.Lock/Lock` 竞争互斥锁，再用 `KV/Range` 校验 `kv.lease() == lease_id` 并提取 `create_revision` 作为 fencing token。
2. **认证 Token 自动续签**：
   - 仅当 `user` 非空白且 `password` 非空时启用 etcd 密码认证（`EtcdConfig::has_auth_credentials()`）；
   - 遇到 gRPC `UNAUTHENTICATED (code 16)` 时通过 `auth_epoch` 串行防风暴刷新一次 token 并重试（`PermissionDenied` 不触发刷新）。
3. **释放顺序（`unlock` 优先于 `lease_revoke`）**：
   - `etcd-client 0.19` 的拦截器会跳过 `etcdserverpb.Lease/*` 的认证 token 注入，而 `v3lockpb.Lock/Unlock` 携带完整 token；
   - 因此释放阶段必须**先执行带认证的 `client.unlock(key)`** 立即删除锁 key（对竞争者零延迟释放），再尽力执行 `client.lease_revoke(id)` 回收空租约；
   - 锁 key 在创建租约时即预置为 `{prefix}/{resource}/{lease_id:x}`，拿到 `Lock` 响应后更新为服务端返回的 `response.key()`，确保取消等待中的 waiter 也能准确清理。

---

## 4. SPI 扩展规范与路线图

### 新增后端须满足
- 实现 `DistributedLock`、`LockHandle` 与 `OccurrenceClaim`（位于 `src/traits.rs`）；
- 通过独立 Cargo feature 门控（如 `redis`、`postgres`），不向默认特性引入无关重依赖；
- 必须复用 `LockConfig::validate()` 与 `validate_resource_name()`，禁止接受空段、`.`、`..` 等非法资源路径；
- 若后端无法提供真正的线性一致 fencing token（例如普通单节点 Redis 锁），必须在文档与类型注释中明确标注弱一致性边界。

### 演进路线
- **v0.1**（当前）：`DistributedLock` + `OccurrenceClaim`，内置 `etcd`（生产）与 `memory`（测试/示例）。
- **v0.2+**：按需扩展 `LeaderElection`、`Semaphore`、`redis` 后端。
