//! Pluggable coordination backends.

#[cfg(feature = "etcd")]
pub mod etcd;
#[cfg(feature = "memory")]
pub mod memory;
