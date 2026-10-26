//! Containerd runtime integration.
//!
//! Provides [`ContainerdRuntime`], a real [`Runtime`] implementation backed
//! by a containerd daemon. The runtime communicates with containerd over
//! its gRPC Unix socket.
//!
//! # Modules
//!
//! - [`proto`] — Protobuf message types (wire-compatible with containerd v1.7)
//! - [`oci`] — OCI runtime spec generation from [`minga_state::types::TaskSpec`]
//! - [`client`] — Low-level gRPC client
//! - [`runtime`] — [`ContainerdRuntime`] implementing the [`Runtime`] trait
//!
//! [`Runtime`]: crate::runtime::Runtime

pub mod client;
pub mod oci;
pub mod proto;
pub mod runtime;

pub use client::{ClientError, ContainerdClient, DEFAULT_SOCKET};
pub use runtime::ContainerdRuntime;
