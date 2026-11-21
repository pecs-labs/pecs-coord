#![cfg(feature = "memory")]

use pecs_coord::{CoordError, DistributedLock, LockConfig, MemoryLockService, OccurrenceClaim};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn memory_lock_enforces_mutual_exclusion_timeout_and_monotonic_fencing() {
    let locks = MemoryLockService::enabled();

    let first = locks.acquire("inventory/sku-42").await.unwrap();
    let t1 = first.fencing_token();
    assert!(t1 > 0);

    // Contender times out while first holds ownership
    let err = locks
        .acquire_with_timeout("inventory/sku-42", Duration::from_millis(30))
        .await
        .expect_err("expected timeout");
    assert!(matches!(err, CoordError::Timeout(_)));

    // Explicit release hands off ownership with strictly greater fencing token
    first.release().await.unwrap();

    let second = locks.acquire("inventory/sku-42").await.unwrap();
    assert!(second.fencing_token() > t1);
    drop(second);

    // Drop also releases ownership
    let third = locks.acquire("inventory/sku-42").await.unwrap();
    assert!(third.fencing_token() > t1 + 1);
    third.release().await.unwrap();
}

#[tokio::test]
async fn memory_lock_works_through_trait_objects() {
    let backend: Arc<dyn DistributedLock> = Arc::new(MemoryLockService::enabled());
    let guard = backend.acquire("jobs/nightly").await.unwrap();
    assert!(guard.fencing_token() > 0);
    assert!(!guard.is_lost());
    guard.release().await.unwrap();

    let claims: Arc<dyn OccurrenceClaim> = Arc::new(MemoryLockService::enabled());
    assert!(claims
        .claim_once("runs/nightly/20261121", 60)
        .await
        .unwrap());
    assert!(!claims
        .claim_once("runs/nightly/20261121", 60)
        .await
        .unwrap());
}

#[tokio::test(start_paused = true)]
async fn memory_claim_once_deduplicates_within_retention_and_expires() {
    let service = MemoryLockService::enabled();

    assert!(service.claim_once("runs/task/bucket-1", 5).await.unwrap());
    assert!(!service.claim_once("runs/task/bucket-1", 5).await.unwrap());
    assert!(service.claim_once("runs/task/bucket-2", 5).await.unwrap());

    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(service.claim_once("runs/task/bucket-1", 5).await.unwrap());
}

#[tokio::test]
async fn disabled_memory_service_fails_fast() {
    let disabled = MemoryLockService::new(LockConfig::default()).unwrap();
    assert!(matches!(
        disabled.acquire("a/b").await,
        Err(CoordError::Config(_))
    ));
    assert!(matches!(
        disabled.claim_once("a/b", 10).await,
        Err(CoordError::Config(_))
    ));
}

#[tokio::test]
async fn scheduled_slot_and_fenced_execution_helpers_enforce_invariants() {
    use pecs_coord::TaskSlot;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let coord = Arc::new(MemoryLockService::enabled());
    let calls = Arc::new(AtomicUsize::new(0));

    let slot1 = TaskSlot::new(
        "pecs-core",
        "sync_data",
        125,
        60,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )
    .unwrap();
    let slot1_dup = TaskSlot::new(
        "pecs-core",
        "sync_data",
        179,
        60,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(slot1.slot, slot1_dup.slot);

    let r1 = coord
        .run_slot(&slot1, async { calls.fetch_add(1, Ordering::SeqCst) })
        .await
        .unwrap();
    let r2 = coord
        .run_slot(&slot1_dup, async { calls.fetch_add(1, Ordering::SeqCst) })
        .await
        .unwrap();
    assert_eq!(r1, Some(0));
    assert_eq!(r2, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let token = coord
        .run_locked(
            "hotlists/collect",
            Duration::from_millis(200),
            Duration::from_secs(1),
            |t| async move { t },
        )
        .await
        .unwrap();
    assert!(token > 0);
}

#[tokio::test]
async fn guard_run_aborts_immediately_on_mid_flight_lease_loss_and_timeout() {
    let locks = MemoryLockService::enabled();
    assert!(locks.is_enabled());

    let guard = Arc::new(locks.acquire("jobs/long-task").await.unwrap());
    assert_eq!(guard.resource(), "/coord/locks/jobs/long-task");

    // 1. Mid-flight lease loss aborts `guard.run(...)` immediately
    let guard_lose = guard.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        guard_lose.force_lose();
    });
    let err = guard
        .run(std::future::pending::<()>())
        .await
        .expect_err("must abort on lease loss");
    assert!(err.is_lost());

    // 2. Execution timeout aborts `guard.run_timeout(...)` and works on `dyn LockHandle`
    let dyn_locks: Arc<dyn DistributedLock> = Arc::new(locks);
    let dyn_guard = dyn_locks.acquire("jobs/long-task").await.unwrap();
    let timeout_err = dyn_guard
        .run_timeout(Duration::from_millis(20), std::future::pending::<()>())
        .await
        .expect_err("must time out");
    assert!(timeout_err.is_timeout());
    dyn_guard.release().await.unwrap();
}
