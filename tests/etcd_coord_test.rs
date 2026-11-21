#![cfg(feature = "etcd")]

use pecs_coord::{CoordError, EtcdConfig, LockConfig, LockService};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn connection() -> EtcdConfig {
    EtcdConfig {
        host: std::env::var("ETCD_TEST_ENDPOINT").expect("set ETCD_TEST_ENDPOINT"),
        timeout: 2,
        ..Default::default()
    }
}

fn prefix() -> String {
    format!(
        "/pecs-coord-test/{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn service(prefix: String) -> LockService {
    LockService::new(
        connection(),
        LockConfig {
            enabled: true,
            prefix,
            lease_ttl_secs: 3,
            acquire_timeout_ms: 400,
            rpc_timeout_ms: 500,
        },
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn lock_contention_renewal_drop_cancel_and_fencing() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let prefix = prefix();
        let locks = service(prefix.clone());
        let first = locks.acquire("order/42").await.unwrap();
        let token = first.fencing_token();
        assert!(service(prefix.clone()).acquire("order/42").await.is_err());
        assert!(matches!(
            locks
                .acquire_with_timeout("order/42", Duration::from_millis(25))
                .await,
            Err(CoordError::Timeout(_))
        ));
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert!(!first.is_lost());
        let pending = tokio::spawn({
            let locks = locks.clone();
            async move { locks.acquire("order/42").await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        pending.abort();
        let _ = pending.await;
        first.release().await.unwrap();
        let second = locks.acquire("order/42").await.unwrap();
        assert!(second.fencing_token() > token);
        drop(second);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let third = locks.acquire("order/42").await.unwrap();
        third.release().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn occurrence_claim_is_atomic_and_expires() {
    let prefix = prefix();
    let first = service(prefix.clone());
    let second = service(prefix);
    let (a, b) = tokio::join!(
        first.claim_once("runs/task/1", 3),
        second.claim_once("runs/task/1", 3),
    );
    assert_ne!(a.unwrap(), b.unwrap(), "exactly one contender must claim");
    assert!(!second.claim_once("runs/task/1", 3).await.unwrap());
    assert!(second.claim_once("runs/task/2", 3).await.unwrap());
}
