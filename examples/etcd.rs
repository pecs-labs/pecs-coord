//! Production-style etcd distributed lock example with `lost()` monitoring.
//!
//! Run with: `ETCD_ENDPOINT=http://127.0.0.1:2379 cargo run --example etcd`

use pecs_coord::{EtcdConfig, LockConfig, LockService};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let coord = LockService::new(
        EtcdConfig::new(host),
        LockConfig::enabled("/coord/examples/locks"),
    )?;

    let guard = coord.acquire("jobs/catalog-refresh").await?;
    let token = guard.fencing_token();
    println!("acquired etcd lock with fencing_token={token}");

    let work = tokio::select! {
        biased;
        _ = guard.lost() => Err("lock ownership lost"),
        _ = tokio::time::sleep(Duration::from_millis(200)) => Ok(()),
    };

    let released = guard.release().await;
    work?;
    released?;
    println!("released etcd lock cleanly");
    Ok(())
}
