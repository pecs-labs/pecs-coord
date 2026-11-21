# pecs-coord

[English](./README.en.md) | [设计规范 (DESIGN.md)](./DESIGN.md)

面向 Rust 异步微服务的生产级**分布式协调原语库**，提供：

- **租约互斥锁 (`DistributedLock` / `LockHandle`)**：单调递增 Fencing Token、后台自动续租、本地单调时钟失锁检测（`lost()` / `is_lost()`）、确定性释放（`release()`）；
- **原子一次性认领 (`OccurrenceClaim`)**：基于有界保留期的原子去重（适用于分布式定时任务时间桶认领、幂等触发防重）；
- **可插拔 SPI 架构**：统一异步 Trait，内置 `etcd`（生产级，含认证 Token 自动续签与 `unlock` 优先释放）与 `memory`（零依赖，仅用于单测与本地示例）。

> **安全红线**：`pecs-coord` 绝不将关闭或不可用的生产锁静默降级为进程内本地锁。

## 快速开始

在 `Cargo.toml` 中引入：

```toml
[dependencies]
pecs-coord = { git = "https://github.com/pecs-labs/pecs-coord.git", branch = "main" }
```

### 1. etcd 分布式锁（生产推荐）

```rust
use pecs_coord::{EtcdConfig, LockConfig, LockService};
use std::time::Duration;

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let coord = LockService::new(
    EtcdConfig::new("127.0.0.1:2379"),
    LockConfig::enabled("/myapp/prod/locks"),
)?;

let guard = coord.acquire("inventory/product/42").await?;
let fencing_token = guard.fencing_token();

// 长任务配合 lost() 监听失锁，并将 fencing_token 透传给下游存储做条件更新
let work = tokio::select! {
    biased;
    _ = guard.lost() => Err("lock ownership lost"),
    res = tokio::time::timeout(Duration::from_secs(60), async move {
        let _ = fencing_token;
        Ok::<(), &str>(())
    }) => res.map_err(|_| "business timeout").and_then(|r| r),
};

let released = guard.release().await;
work?;
released?;
# Ok(())
# }
```

### 2. 原子一次性认领 (`claim_once`)

```rust
# use pecs_coord::MemoryLockService;
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let coord = MemoryLockService::enabled();

// 同一时间桶在 retention_secs 内最多只会被一个副本认领成功
if coord.claim_once("runs/daily-report/2026-11-21", 300).await? {
    // 执行本轮任务
}
# Ok(())
# }
```

## 特性开关 (Feature Flags)

| Feature | 默认开启 | 说明 |
| --- | :---: | --- |
| `etcd` | 是 | 基于 etcd v3 的生产级分布式锁与 `claim_once` |
| `memory` | 是 | 进程内内存实现，仅用于单元测试与本地示例 |

## 运行测试与示例

```bash
# 运行零依赖快速开始示例
cargo run --example quickstart

# 运行 etcd 示例
ETCD_ENDPOINT=http://127.0.0.1:2379 cargo run --example etcd

# 运行本地单元测试（无需外部 etcd）
cargo test

# 运行真实 etcd 集成测试（需要隔离测试 etcd）
ETCD_TEST_ENDPOINT=http://127.0.0.1:2379 cargo test --test etcd_coord_test -- --ignored
```

## 许可证

双许可：[MIT](./LICENSE-MIT) OR [Apache-2.0](./LICENSE-APACHE)。
