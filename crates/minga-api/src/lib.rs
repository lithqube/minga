//! Docker Engine API subset (REST) for Minga.
//!
//! Implements enough of the Docker Engine API (v1.41) for `docker stack
//! deploy`, `docker service ls`, `docker service inspect`, `docker node ls`,
//! `docker stack ps`, and related commands to work against a Minga cluster.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/_ping` | Health check |
//! | GET | `/{version}/version` | Engine version |
//! | GET | `/{version}/info` | System info (Swarm status) |
//! | GET | `/{version}/nodes` | List cluster nodes |
//! | GET | `/{version}/nodes/{id}` | Inspect a node |
//! | GET | `/{version}/services` | List services |
//! | POST | `/{version}/services/create` | Create a service |
//! | GET | `/{version}/services/{id}` | Inspect a service |
//! | DELETE | `/{version}/services/{id}` | Remove a service |
//! | POST | `/{version}/services/{id}/update` | Update a service |
//! | GET | `/{version}/tasks` | List tasks |
//! | GET | `/{version}/tasks/{id}` | Inspect a task |
//! | GET | `/{version}/networks` | List networks |
//! | POST | `/{version}/networks/create` | Create a network |
//! | GET | `/{version}/networks/{id}` | Inspect a network |
//! | DELETE | `/{version}/networks/{id}` | Remove a network |
//! | GET | `/{version}/secrets` | List secrets |
//! | POST | `/{version}/secrets/create` | Create a secret |
//! | GET | `/{version}/secrets/{id}` | Inspect a secret |
//! | DELETE | `/{version}/secrets/{id}` | Remove a secret |
//! | GET | `/{version}/configs` | List configs |
//! | POST | `/{version}/configs/create` | Create a config |
//! | GET | `/{version}/configs/{id}` | Inspect a config |
//! | DELETE | `/{version}/configs/{id}` | Remove a config |
//!
//! # Architecture
//!
//! - **Models** ([`models`]): Docker API request/response types (serde).
//! - **Translation** ([`translate`]): bidirectional mapping between Docker
//!   API types and Minga state types.
//! - **Handlers** ([`handlers`]): axum route handlers that read/write
//!   cluster state via [`StateStore`](minga_state::StateStore).
//! - **Server** ([`server`]): router construction and server lifecycle.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use minga_api::ApiServer;
//! use minga_state::StateStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(StateStore::open(1, "10.0.0.1:50051", "/var/lib/minga", Default::default()).await?);
//! let server = ApiServer::start(store, "0.0.0.0:2375").await?;
//! println!("API server listening on {}", server.addr());
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod handlers;
pub mod models;
pub mod server;
pub mod translate;

pub use error::{ApiError, ApiResult};
pub use server::{build_router, ApiServer, ApiServerError};
