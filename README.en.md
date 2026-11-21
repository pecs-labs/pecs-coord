# pecs-coord

[中文](./README.md) | [Design Spec (DESIGN.md)](./DESIGN.md)

Production-grade **distributed coordination primitives** for async Rust microservices:

- **Lease-backed Mutual Exclusion (`DistributedLock` / `LockHandle`)**: strictly monotonic fencing tokens, automatic lease keepalive, monotonic local expiry detection (`lost()` / `is_lost()`), and server-confirmed `release()`.
- **Atomic Occurrence Claims (`OccurrenceClaim`)**: bounded-retention deduplication (`claim_once`) for scheduled job buckets and one-shot distributed triggers.
- **Pluggable Backend SPI**: clean async traits with built-in `etcd` (production, including automatic auth token refresh and authenticated `unlock`-before-`lease_revoke`) and `memory` (zero-dependency, for unit tests and local examples).

> **Safety Rule**: `pecs-coord` never silently degrades a disabled or unreachable production lock into an in-process local lock.

## Quick Start

```toml
[dependencies]
pecs-coord = { git = "https://github.com/pecs-labs/pecs-coord.git", branch = "main" }
```

### 1. Production etcd Lock

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

### 2. Atomic Occurrence Claim

```rust
# use pecs_coord::MemoryLockService;
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let coord = MemoryLockService::enabled();
if coord.claim_once("runs/daily-report/2026-11-21", 300).await? {
    // execute once per retention window
}
# Ok(())
# }
```

## Feature Flags

| Feature | Default | Description |
| --- | :---: | --- |
| `etcd` | yes | Production etcd v3 lease-backed locks and `claim_once` |
| `memory` | yes | In-process backend for unit tests and local examples |

## License

Dual-licensed under [MIT](./LICENSE-MIT) or [Apache-2.0](./LICENSE-APACHE).
