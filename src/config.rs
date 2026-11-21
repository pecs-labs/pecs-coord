use crate::{CoordError, CoordResult};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for lease-backed distributed locks and occurrence claims.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LockConfig {
    /// Explicit opt-in switch (`enable` is accepted as a YAML/JSON alias).
    /// Disabled services fail fast and never silently degrade to local locks.
    #[serde(alias = "enable")]
    pub enabled: bool,

    /// Shared keyspace prefix (must be identical across replicas in the same environment
    /// and isolated across environments).
    pub prefix: String,

    /// Lease TTL in seconds (must be >= 3 and > 3 * rpc_timeout_ms).
    pub lease_ttl_secs: i64,

    /// Default wait budget for acquiring a lock, in milliseconds.
    pub acquire_timeout_ms: u64,

    /// Per-RPC timeout budget, in milliseconds.
    pub rpc_timeout_ms: u64,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix: "/coord/locks".into(),
            lease_ttl_secs: 30,
            acquire_timeout_ms: 3000,
            rpc_timeout_ms: 3000,
        }
    }
}

impl LockConfig {
    /// Create an enabled configuration for the given keyspace prefix with safe defaults.
    pub fn enabled(prefix: impl Into<String>) -> Self {
        Self {
            enabled: true,
            prefix: prefix.into(),
            ..Self::default()
        }
    }

    pub fn with_lease_ttl_secs(mut self, secs: i64) -> Self {
        self.lease_ttl_secs = secs;
        self
    }

    pub fn with_acquire_timeout_ms(mut self, ms: u64) -> Self {
        self.acquire_timeout_ms = ms;
        self
    }

    pub fn with_rpc_timeout_ms(mut self, ms: u64) -> Self {
        self.rpc_timeout_ms = ms;
        self
    }

    /// Validate invariants required for safe lease renewal and prefix isolation.
    pub fn validate(&self) -> CoordResult<()> {
        if self.lease_ttl_secs < 3
            || self.rpc_timeout_ms == 0
            || self.acquire_timeout_ms == 0
            || self.rpc_timeout_ms >= (self.lease_ttl_secs as u64).saturating_mul(1000) / 3
            || self.prefix.trim_matches('/').is_empty()
        {
            return Err(CoordError::config("invalid distributed lock configuration"));
        }
        Ok(())
    }

    /// Validate a logical resource path and build its normalized full key.
    pub fn resource_key(&self, resource: &str) -> CoordResult<String> {
        if !self.enabled {
            return Err(CoordError::config("distributed lock disabled"));
        }
        validate_resource_name(resource)?;
        Ok(format!(
            "{}/{}",
            self.prefix.trim_end_matches('/'),
            resource
        ))
    }
}

/// Validate a logical coordination resource path (rejects empty segments and `.` / `..`).
pub fn validate_resource_name(resource: &str) -> CoordResult<()> {
    if resource.is_empty()
        || resource
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return Err(CoordError::config("invalid lock resource"));
    }
    Ok(())
}

/// PEM-encoded TLS credentials for etcd connections.
/// Keep private keys out of committed configuration files.
#[derive(Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct EtcdTlsConfig {
    pub domain_name: Option<String>,
    pub ca_pem: Option<String>,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
}

impl fmt::Debug for EtcdTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdTlsConfig")
            .field("domain_name", &self.domain_name)
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "[REDACTED]"))
            .field(
                "client_cert_pem",
                &self.client_cert_pem.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Connection configuration for the etcd coordination backend.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct EtcdConfig {
    /// Single host or comma-separated cluster list (e.g. `"10.0.0.1:2379,10.0.0.2:2379"`).
    pub host: String,
    pub port: u16,
    pub tls: Option<EtcdTlsConfig>,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Connection / default operation timeout in seconds.
    pub timeout: u64,
}

impl fmt::Debug for EtcdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 2379,
            tls: None,
            user: None,
            password: None,
            timeout: 10,
        }
    }
}

impl EtcdConfig {
    /// Create a connection config targeting the given host or endpoint URL.
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            ..Self::default()
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_credentials(
        mut self,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout = secs;
        self
    }

    /// Returns `true` only when both a non-blank username and a non-empty password are set.
    pub fn has_auth_credentials(&self) -> bool {
        self.user.as_ref().is_some_and(|u| !u.trim().is_empty())
            && self.password.as_ref().is_some_and(|p| !p.is_empty())
    }

    /// Resolve the first endpoint URL.
    pub fn endpoint(&self) -> String {
        self.endpoints().into_iter().next().unwrap_or_default()
    }

    /// Parse comma-separated hosts into normalized `http://` / `https://` endpoint URLs.
    pub fn endpoints(&self) -> Vec<String> {
        self.host
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|addr| {
                if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr.to_string()
                } else if addr.starts_with('[') && addr.ends_with(']') {
                    format!("http://{addr}:{}", self.port)
                } else if addr.contains(':') {
                    format!("http://{addr}")
                } else {
                    format!("http://{addr}:{}", self.port)
                }
            })
            .collect()
    }
}

#[cfg(feature = "etcd")]
pub(crate) async fn connect_etcd(config: &EtcdConfig) -> CoordResult<etcd_client::Client> {
    let timeout = std::time::Duration::from_secs(config.timeout.max(1));
    let mut options = etcd_client::ConnectOptions::new().with_connect_timeout(timeout);
    if let Some(tls) = &config.tls {
        let mut options_tls = etcd_client::TlsOptions::new().with_native_roots();
        if let Some(domain) = &tls.domain_name {
            options_tls = options_tls.domain_name(domain);
        }
        if let Some(ca) = &tls.ca_pem {
            options_tls = options_tls.ca_certificate(etcd_client::Certificate::from_pem(ca));
        }
        match (&tls.client_cert_pem, &tls.client_key_pem) {
            (Some(cert), Some(key)) => {
                options_tls = options_tls.identity(etcd_client::Identity::from_pem(cert, key));
            }
            (None, None) => {}
            _ => {
                return Err(CoordError::config(
                    "TLS client certificate and key must be supplied together",
                ));
            }
        }
        options = options.with_tls(options_tls);
    }
    if config.has_auth_credentials() {
        if let (Some(user), Some(password)) = (&config.user, &config.password) {
            options = options.with_user(user, password);
        }
    }
    tokio::time::timeout(
        timeout,
        etcd_client::Client::connect(config.endpoints(), Some(options)),
    )
    .await
    .map_err(|_| CoordError::timeout("etcd connect"))?
    .map_err(|e| CoordError::connection(e.to_string()))
}
