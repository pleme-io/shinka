//! Shinka (進化) - GitOps-native database migration operator for Kubernetes
//!
//! A Rust-based Kubernetes operator that automatically runs database migrations
//! when deployment images change, integrated with CNPG health checks and full observability.
//!
//! # Architecture
//!
//! Shinka provides three integrated components:
//!
//! 1. **Kubernetes Operator**: Watches for DatabaseMigration CRDs and manages migration Jobs
//! 2. **API Layer**: REST, GraphQL, and gRPC APIs for programmatic control
//! 3. **Health & Metrics**: Prometheus metrics and health endpoints
//!
//! # API Access
//!
//! Shinka exposes a unified API accessible via multiple protocols:
//!
//! - **REST**: RESTful HTTP endpoints at `/api/v1/*`
//! - **GraphQL**: Full schema with queries, mutations, subscriptions at `/graphql`
//! - **gRPC**: High-performance RPC services on port 50051
//!
//! See the [`api`] module for detailed documentation.
//!
//! # Example
//!
//! ```rust,ignore
//! use shinka::{api::MigrationApiService, Config};
//! use kube::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::try_default().await?;
//!     let config = Config::from_env();
//!
//!     // Create API service
//!     let api_service = MigrationApiService::new(client.clone()).await?;
//!
//!     // Query migration status
//!     let migration = api_service.get_migration("default", "my-app").await?;
//!     println!("Migration phase: {}", migration.status.phase);
//!
//!     Ok(())
//! }
//! ```

pub mod api;
pub mod circuit_breaker;
pub mod config;
pub mod controller;
pub mod crd;
pub mod database;
pub mod error;
pub mod health;
pub mod leader;
pub mod metrics;
pub mod migrator;
pub mod redact;
pub mod util;
pub mod webhook;

pub use api::MigrationApiService;
pub use config::Config;
pub use crd::DatabaseMigration;
pub use error::{Error, Result};
