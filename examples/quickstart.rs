//! Zero-dependency quickstart demonstrating the core coordination primitives
//! (`OccurrenceClaim` and `DistributedLock`) using the in-memory test backend.
//!
//! Run with: `cargo run --example quickstart`

use pecs_coord::MemoryLockService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = MemoryLockService::enabled();

    // 1. Atomic occurrence claim (e.g., deduplicate a scheduled job bucket)
    if coord
        .claim_once("runs/daily-report/2026-11-21", 300)
        .await?
    {
        println!("claimed daily-report bucket");
    }

    // 2. Mutual exclusion lock with monotonic fencing token
    let guard = coord.acquire("inventory/product/42").await?;
    println!("acquired lock with fencing_token={}", guard.fencing_token());

    // Pass guard.fencing_token() to downstream storage for atomic stale-write rejection.
    guard.release().await?;
    println!("lock released cleanly");
    Ok(())
}
