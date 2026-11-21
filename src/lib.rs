//! # pecs-coord
//!
//! Production-grade distributed coordination primitives for Rust:
//! - Lease-backed mutual exclusion locks with strictly monotonic fencing tokens;
//! - Atomic, bounded-retention occurrence claims (`claim_once`);
//! - Pluggable backend SPI (`DistributedLock`, `LockHandle`, `OccurrenceClaim`);
//! - Built-in [`backend::etcd`] (production) and [`backend::memory`] (unit tests & examples).
//!
//! When the `etcd` feature is enabled, [`LockService`] and [`LockGuard`] are re-exported
//! as type aliases for [`EtcdLockService`] and [`EtcdLockGuard`]. They are intentionally
//! omitted when `etcd` is disabled so production code can never silently fall back to an
//! in-process lock.

pub mod backend;
pub mod config;
pub mod error;
#[cfg(any(feature = "etcd", test))]
pub(crate) mod lease;
pub mod runner;
pub mod traits;

pub use config::{validate_resource_name, EtcdConfig, EtcdTlsConfig, LockConfig};
pub use error::{CoordError, CoordResult};
pub use runner::{Coordinator, TaskSlot};
pub use traits::{DistributedLock, LockHandle, OccurrenceClaim};

#[cfg(feature = "etcd")]
pub use backend::etcd::{EtcdLockGuard, EtcdLockService, LockGuard, LockService};
#[cfg(feature = "memory")]
pub use backend::memory::{MemoryLockGuard, MemoryLockService};

/// Convenient re-exports for common coordination types and traits.
pub mod prelude {
    pub use crate::config::{EtcdConfig, EtcdTlsConfig, LockConfig};
    pub use crate::error::{CoordError, CoordResult};
    pub use crate::runner::{Coordinator, TaskSlot};
    pub use crate::traits::{DistributedLock, LockHandle, OccurrenceClaim};

    #[cfg(feature = "etcd")]
    pub use crate::backend::etcd::{EtcdLockGuard, EtcdLockService, LockGuard, LockService};
    #[cfg(feature = "memory")]
    pub use crate::backend::memory::{MemoryLockGuard, MemoryLockService};
}
