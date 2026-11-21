use pecs_coord::{CoordError, EtcdConfig, LockConfig};

#[test]
fn endpoints_accept_single_urls_and_clusters() {
    for (host, expected) in [
        ("localhost", vec!["http://localhost:2379"]),
        ("https://etcd:1234", vec!["https://etcd:1234"]),
        ("etcd:1234", vec!["http://etcd:1234"]),
        ("[::1]", vec!["http://[::1]:2379"]),
        (
            " a , https://b:1234 ",
            vec!["http://a:2379", "https://b:1234"],
        ),
    ] {
        let config = EtcdConfig {
            host: host.into(),
            ..Default::default()
        };
        assert_eq!(config.endpoints(), expected);
        assert_eq!(config.endpoint(), expected[0]);
    }
}

#[test]
fn has_auth_credentials_requires_non_empty_user_and_password() {
    let mut cfg = EtcdConfig::default();
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("root".into());
    cfg.password = Some(String::new());
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("   ".into());
    cfg.password = Some("secret".into());
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("root".into());
    cfg.password = Some("secret".into());
    assert!(cfg.has_auth_credentials());
}

#[test]
fn lock_config_validates_invariants() {
    assert!(LockConfig::default().validate().is_ok());

    let bad_ttl = LockConfig {
        lease_ttl_secs: 2,
        ..Default::default()
    };
    assert!(matches!(bad_ttl.validate(), Err(CoordError::Config(_))));

    let bad_ratio = LockConfig {
        lease_ttl_secs: 6,
        rpc_timeout_ms: 2000,
        ..Default::default()
    };
    assert!(matches!(bad_ratio.validate(), Err(CoordError::Config(_))));

    let bad_prefix = LockConfig {
        prefix: "///".into(),
        ..Default::default()
    };
    assert!(matches!(bad_prefix.validate(), Err(CoordError::Config(_))));
}

#[cfg(feature = "etcd")]
#[tokio::test]
async fn disabled_etcd_lock_fails_without_local_fallback() {
    let locks = pecs_coord::LockService::new(Default::default(), Default::default()).unwrap();
    assert!(locks.acquire("order/42").await.is_err());
    assert!(matches!(
        locks
            .acquire_with_timeout("order/42", std::time::Duration::ZERO)
            .await,
        Err(CoordError::Config(_))
    ));
}

#[cfg(feature = "etcd")]
#[tokio::test]
async fn etcd_claims_reject_disabled_invalid_resources_and_non_positive_retention() {
    let disabled = pecs_coord::LockService::new(Default::default(), Default::default()).unwrap();
    assert!(matches!(
        disabled.claim_once("runs/1", 60).await,
        Err(CoordError::Config(_))
    ));

    let enabled = pecs_coord::LockService::new(
        Default::default(),
        LockConfig {
            enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    for resource in ["", "a//b", "../a", "a/./b"] {
        assert!(matches!(
            enabled.claim_once(resource, 60).await,
            Err(CoordError::Config(_))
        ));
    }
    assert!(matches!(
        enabled.claim_once("runs/1", 0).await,
        Err(CoordError::Config(_))
    ));
}

#[test]
fn debug_output_redacts_secrets() {
    let cfg = EtcdConfig::new("127.0.0.1:2379").with_credentials("root", "super-secret-pass");
    let mut cfg = cfg;
    cfg.tls = Some(pecs_coord::EtcdTlsConfig {
        domain_name: Some("etcd.internal".into()),
        ca_pem: Some("CA-PEM-BODY".into()),
        client_cert_pem: Some("CERT-PEM-BODY".into()),
        client_key_pem: Some("PRIVATE-KEY-SECRET".into()),
    });
    let rendered = format!("{cfg:?}");
    assert!(!rendered.contains("super-secret-pass"));
    assert!(!rendered.contains("PRIVATE-KEY-SECRET"));
    assert!(!rendered.contains("CA-PEM-BODY"));
    assert!(!rendered.contains("CERT-PEM-BODY"));
    assert!(rendered.contains("[REDACTED]"));
}
