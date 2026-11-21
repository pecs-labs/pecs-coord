use thiserror::Error;

/// Unified error type for distributed coordination operations.
#[derive(Debug, Error)]
pub enum CoordError {
    /// Invalid configuration, disabled service, or malformed resource path.
    #[error("coordination config error: {0}")]
    Config(String),

    /// Failed to establish or authenticate a backend connection.
    #[error("coordination connection error: {0}")]
    Connection(String),

    /// Operation exceeded its configured RPC or wait timeout budget.
    #[error("coordination timeout: {0}")]
    Timeout(String),

    /// Backend driver / protocol error.
    #[error("coordination driver error: {0}")]
    Driver(String),

    /// Lock ownership was lost or never confirmed before expiry.
    #[error("coordination ownership lost: {0}")]
    Lost(String),

    /// Other internal or runtime errors.
    #[error("coordination error: {0}")]
    Other(String),
}

impl CoordError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }

    pub fn driver(msg: impl Into<String>) -> Self {
        Self::Driver(msg.into())
    }

    pub fn lost(msg: impl Into<String>) -> Self {
        Self::Lost(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    /// 是否为超时错误 (`CoordError::Timeout`)
    #[inline]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// 是否为锁所有权丢失错误 (`CoordError::Lost`)
    #[inline]
    pub fn is_lost(&self) -> bool {
        matches!(self, Self::Lost(_))
    }

    /// 是否为配置或资源名非法错误 (`CoordError::Config`)
    #[inline]
    pub fn is_config(&self) -> bool {
        matches!(self, Self::Config(_))
    }

    /// 是否为后端连接/认证错误 (`CoordError::Connection`)
    #[inline]
    pub fn is_connection(&self) -> bool {
        matches!(self, Self::Connection(_))
    }
}

pub type CoordResult<T> = Result<T, CoordError>;
