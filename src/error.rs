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
}

pub type CoordResult<T> = Result<T, CoordError>;
