//! `minga-ctl` — companion CLI for Minga.
//!
//! The crate is a library plus a thin binary: the command tree lives in
//! `main.rs`, the reusable parts live in the modules below so the
//! integration tests can drive them directly.

pub mod api;
pub mod cmd;
pub mod config;
pub mod error;
pub mod output;
pub mod transport;
pub mod view;

pub use error::CtlError;
