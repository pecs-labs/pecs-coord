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
