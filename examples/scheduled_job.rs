//! Two-layer distributed scheduled job coordination example (`execute_scheduled_slot`)
//! and lease-loss-aware critical section execution (`run_locked` / `run_until_lost`).
//!
//! Run with: `cargo run -p pecs-coord --example scheduled_job`

use pecs_coord::{MemoryLockService, TaskSlot};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = Arc::new(MemoryLockService::enabled());
    let executions = Arc::new(AtomicUsize::new(0));

    // 1. Two replicas trigger the same 60-second scheduled job in the same time bucket (t=125s & t=130s)
    let slot_a = TaskSlot::new(
        "pecs-core",
        "sync_data",
        125,
        60,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    let slot_b = TaskSlot::new(
        "pecs-core",
        "sync_data",
        130,
        60,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;

    let out_a = coord
        .run_slot(&slot_a, async {
            executions.fetch_add(1, Ordering::SeqCst) + 1
        })
        .await?;
    let out_b = coord
        .run_slot(&slot_b, async {
            executions.fetch_add(1, Ordering::SeqCst) + 1
        })
        .await?;
    println!(
        "🕒 Time slot {}: Replica A={out_a:?}, Replica B={out_b:?}",
        slot_a.slot
    );

    // 2. Lease-guarded fenced execution (`coord.run_locked`)
    let token = coord
        .run_locked(
            "hotlists/collect",
            Duration::from_secs(1),
            Duration::from_secs(5),
            |fencing_token| async move { fencing_token },
        )
        .await?;
    println!("🔒 Executed fenced critical section with token={token}");

    Ok(())
}
