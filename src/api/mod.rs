//! Shinka API Layer
//!
//! Provides a unified API layer accessible via multiple protocols:
//! - REST: RESTful HTTP endpoints with JSON
//! - GraphQL: Full GraphQL schema with queries, mutations, and subscriptions
//! - gRPC: High-performance RPC with streaming support
//!
//! All protocols share the same core service layer, ensuring consistent behavior
//! and business logic across all access methods.
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────────┐
//! │                        Protocol Adapters                          │
//! ├─────────────────┬──────────────────┬─────────────────────────────┤
//! │   REST (axum)   │ GraphQL (async-  │      gRPC (tonic)           │
//! │                 │  graphql)        │                             │
//! └────────┬────────┴────────┬─────────┴──────────────┬──────────────┘
//!          │                 │                        │
//!          ▼                 ▼                        ▼
//! ┌───────────────────────────────────────────────────────────────────┐
//! │                    Core Service Layer                              │
//! │                  (MigrationApiService)                             │
//! │                                                                    │
//! │  • Get/List/Watch migrations                                       │
//! │  • Retry/Cancel operations                                         │
//! │  • Database health checks                                          │
//! │  • Queue management                                                │
//! └───────────────────────────────────────────────────────────────────┘
//!          │
//!          ▼
//! ┌───────────────────────────────────────────────────────────────────┐
//! │                    Kubernetes API                                  │
//! │              (DatabaseMigration CRD, Jobs, Events)                 │
//! └───────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ## REST API
//!
//! ```rust,ignore
//! use shinka::api::{rest, MigrationApiService};
//! use std::sync::Arc;
//!
//! let service = Arc::new(MigrationApiService::new(client).await?);
//! let router = rest::create_router(service);
//!
//! // Serve on port 8080
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, router).await?;
//! ```
//!
//! ## GraphQL API
//!
//! ```rust,ignore
//! use shinka::api::{graphql, MigrationApiService};
//! use async_graphql_axum::{GraphQL, GraphQLSubscription};
//!
//! let service = Arc::new(MigrationApiService::new(client).await?);
//! let schema = graphql::create_schema(service);
//!
//! let app = Router::new()
//!     .route("/graphql", post(GraphQL::new(schema.clone())))
//!     .route("/graphql/ws", GraphQLSubscription::new(schema));
//! ```
//!
//! ## gRPC API
//!
//! ```rust,ignore
//! use shinka::api::{grpc, MigrationApiService};
//! use tonic::transport::Server;
//!
//! let service = Arc::new(MigrationApiService::new(client).await?);
//! let (migration_svc, database_svc, queue_svc) = grpc::create_grpc_services(service);
//!
//! Server::builder()
//!     .add_service(migration_svc)
//!     .add_service(database_svc)
//!     .add_service(queue_svc)
//!     .serve("[::]:50051".parse()?).await?;
//! ```
//!
//! # Endpoints Overview
//!
//! | Operation | REST | GraphQL | gRPC |
//! |-----------|------|---------|------|
//! | Get migration | `GET /api/v1/namespaces/:ns/migrations/:name` | `migration(ns, name)` | `GetMigration` |
//! | List migrations | `GET /api/v1/migrations` | `migrations(filter)` | `ListMigrations` |
//! | Watch migrations | `GET /api/v1/migrations/watch` (SSE) | `subscription { migrations }` | `WatchMigrations` (stream) |
//! | Retry migration | `POST .../retry` | `retryMigration(...)` | `RetryMigration` |
//! | Cancel migration | `POST .../cancel` | `cancelMigration(...)` | `CancelMigration` |
//! | Check DB ready | `GET .../clusters/:cluster/ready` | `databaseReady(...)` | `CheckDatabaseReady` |
//! | Cluster health | `GET .../clusters/:cluster/health` | `clusterHealth(...)` | `GetClusterHealth` |
//! | Queue status | `GET /api/v1/queue/status` | `queueStatus` | `GetQueueStatus` |
//! | Pause queue | `POST /api/v1/queue/pause` | `pauseQueue(...)` | `PauseQueue` |
//! | Resume queue | `POST /api/v1/queue/resume` | `resumeQueue` | `ResumeQueue` |

pub mod graphql;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod rest;
pub mod service;
pub mod types;

// Re-export commonly used items
pub use graphql::{create_schema, ShinkaSchemA};
#[cfg(feature = "grpc")]
pub use grpc::create_grpc_services;
pub use rest::create_router;
pub use service::MigrationApiService;
pub use types::{
    ApiError, ClusterHealth, DatabaseInfo, DatabaseReadiness, EventType, MigrationEvent,
    MigrationFilter, MigrationHistoryEntry, MigrationList, MigrationResource, QueueItem,
    QueueStatus,
};
