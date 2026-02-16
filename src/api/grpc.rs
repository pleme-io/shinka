//! gRPC API implementation for Shinka
//!
//! Implements the MigrationService, DatabaseService, and QueueService
//! gRPC services defined in proto/shinka.proto.

use chrono::Utc;
use futures::StreamExt;
use prost_types::{Duration as ProstDuration, Timestamp};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use super::service::MigrationApiService;
use super::types::{
    ApiError, ClusterHealth, DatabaseInfo, DatabaseReadiness, MigrationFilter,
    MigrationHistoryEntry, MigrationResource, QueueItem, QueueStatus,
};

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("shinka.v1");
}

use proto::{
    database_service_server::{DatabaseService, DatabaseServiceServer},
    migration_service_server::{MigrationService, MigrationServiceServer},
    queue_service_server::{QueueService, QueueServiceServer},
    *,
};

// =============================================================================
// Type Conversions
// =============================================================================

fn to_timestamp(dt: chrono::DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn to_duration_proto(seconds: f64) -> Option<ProstDuration> {
    Some(ProstDuration {
        seconds: seconds as i64,
        nanos: ((seconds.fract()) * 1_000_000_000.0) as i32,
    })
}

fn api_error_to_status(e: ApiError) -> Status {
    match e.code.as_str() {
        "NOT_FOUND" => Status::not_found(e.message),
        "INVALID_ARGUMENT" => Status::invalid_argument(e.message),
        "PERMISSION_DENIED" => Status::permission_denied(e.message),
        "FAILED_PRECONDITION" => Status::failed_precondition(e.message),
        "ALREADY_EXISTS" => Status::already_exists(e.message),
        _ => Status::internal(e.message),
    }
}

impl From<MigrationResource> for Migration {
    fn from(m: MigrationResource) -> Self {
        Self {
            name: m.name,
            namespace: m.namespace,
            uid: m.uid,
            created_at: Some(to_timestamp(m.created_at)),
            spec: Some(MigrationSpec {
                cnpg_cluster: m.spec.cnpg_cluster,
                database: m.spec.database.unwrap_or_default(),
                migrator_type: m.spec.migrator_type,
                deployment_ref: m.spec.deployment_ref,
                command: m.spec.command,
                require_healthy_cluster: m.spec.require_healthy_cluster,
                max_retries: m.spec.max_retries as i32,
                migration_timeout: Some(ProstDuration {
                    seconds: m.spec.migration_timeout_seconds as i64,
                    nanos: 0,
                }),
            }),
            status: Some(proto::MigrationStatus {
                phase: m.status.phase,
                last_migration: m.status.last_migration.map(|lm| LastMigration {
                    image_tag: lm.image_tag,
                    success: lm.success,
                    duration: lm.duration_seconds.and_then(to_duration_proto),
                    completed_at: lm.completed_at.map(to_timestamp),
                    error: lm.error.unwrap_or_default(),
                }),
                retry_count: m.status.retry_count as i32,
                current_job: m.status.current_job.unwrap_or_default(),
                observed_generation: m.status.observed_generation,
            }),
            conditions: m
                .conditions
                .into_iter()
                .map(|c| Condition {
                    r#type: c.condition_type,
                    status: c.status,
                    reason: c.reason.unwrap_or_default(),
                    message: c.message.unwrap_or_default(),
                    last_transition_time: c.last_transition_time.map(to_timestamp),
                    observed_generation: 0,
                })
                .collect(),
        }
    }
}

impl From<MigrationHistoryEntry> for MigrationHistoryEntry_proto {
    fn from(h: MigrationHistoryEntry) -> Self {
        MigrationHistoryEntry_proto {
            image_tag: h.image_tag,
            success: h.success,
            started_at: Some(to_timestamp(h.started_at)),
            completed_at: h.completed_at.map(to_timestamp),
            duration: h.duration_seconds.and_then(to_duration_proto),
            error: h.error.unwrap_or_default(),
            retry_count: h.retry_count as i32,
            job_name: h.job_name,
        }
    }
}

type MigrationHistoryEntry_proto = proto::MigrationHistoryEntry;

impl From<DatabaseReadiness> for DatabaseReadyResponse {
    fn from(d: DatabaseReadiness) -> Self {
        Self {
            ready: d.ready,
            cluster_healthy: d.cluster_healthy,
            migrations_complete: d.migrations_complete,
            message: d.message,
            pending_migrations: d.pending_migrations,
            active_migrations: d.active_migrations,
        }
    }
}

impl From<ClusterHealth> for ClusterHealthResponse {
    fn from(c: ClusterHealth) -> Self {
        Self {
            healthy: c.healthy,
            phase: c.phase,
            ready_replicas: c.ready_replicas,
            total_replicas: c.total_replicas,
            primary_pod: c.primary_pod.unwrap_or_default(),
            last_check: to_timestamp(c.last_check),
            message: c.message,
        }
    }
}

impl From<DatabaseInfo> for proto::DatabaseInfo {
    fn from(d: DatabaseInfo) -> Self {
        Self {
            namespace: d.namespace,
            cluster_name: d.cluster_name,
            database: d.database.unwrap_or_default(),
            healthy: d.healthy,
            pending_migrations: d.pending_migrations,
            completed_migrations: d.completed_migrations,
            last_migration: d.last_migration.and_then(to_timestamp),
        }
    }
}

impl From<QueueStatus> for QueueStatusResponse {
    fn from(q: QueueStatus) -> Self {
        Self {
            paused: q.paused,
            pending: q.pending,
            active: q.active,
            completed: q.completed,
            failed: q.failed,
            max_concurrent: q.max_concurrent,
            strategy: q.strategy,
        }
    }
}

impl From<QueueItem> for proto::QueueItem {
    fn from(q: QueueItem) -> Self {
        Self {
            namespace: q.namespace,
            name: q.name,
            status: q.status,
            priority: q.priority,
            enqueued_at: to_timestamp(q.enqueued_at),
            started_at: q.started_at.and_then(to_timestamp),
            database: q.database,
        }
    }
}

// =============================================================================
// Migration Service Implementation
// =============================================================================

/// gRPC Migration Service
pub struct MigrationServiceImpl {
    api: Arc<MigrationApiService>,
}

impl MigrationServiceImpl {
    pub fn new(api: Arc<MigrationApiService>) -> Self {
        Self { api }
    }
}

#[tonic::async_trait]
impl MigrationService for MigrationServiceImpl {
    #[instrument(skip(self))]
    async fn get_migration(
        &self,
        request: Request<GetMigrationRequest>,
    ) -> Result<Response<Migration>, Status> {
        let req = request.into_inner();
        let result = self
            .api
            .get_migration(&req.namespace, &req.name)
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn list_migrations(
        &self,
        request: Request<ListMigrationsRequest>,
    ) -> Result<Response<ListMigrationsResponse>, Status> {
        let req = request.into_inner();
        let filter = MigrationFilter {
            namespace: if req.namespace.is_empty() {
                None
            } else {
                Some(req.namespace)
            },
            phases: req.phases,
            limit: if req.limit > 0 { Some(req.limit) } else { None },
            continue_token: if req.continue_token.is_empty() {
                None
            } else {
                Some(req.continue_token)
            },
            ..Default::default()
        };

        let result = self
            .api
            .list_migrations(filter)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(ListMigrationsResponse {
            migrations: result.migrations.into_iter().map(Into::into).collect(),
            continue_token: result.continue_token.unwrap_or_default(),
            total_count: result.total_count.unwrap_or(0),
        }))
    }

    type WatchMigrationsStream =
        Pin<Box<dyn futures::Stream<Item = Result<MigrationEvent, Status>> + Send>>;

    #[instrument(skip(self))]
    async fn watch_migrations(
        &self,
        request: Request<WatchMigrationsRequest>,
    ) -> Result<Response<Self::WatchMigrationsStream>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            None
        } else {
            Some(req.namespace)
        };
        let name = if req.name.is_empty() {
            None
        } else {
            Some(req.name)
        };

        let stream = self.api.watch_migrations(namespace, name, req.phases).await;

        let mapped = stream.map(|event| {
            Ok(MigrationEvent {
                event_type: event.event_type.to_string(),
                migration: Some(event.migration.into()),
                timestamp: to_timestamp(event.timestamp),
            })
        });

        Ok(Response::new(Box::pin(mapped)))
    }

    #[instrument(skip(self))]
    async fn get_migration_history(
        &self,
        request: Request<GetMigrationHistoryRequest>,
    ) -> Result<Response<MigrationHistoryResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit > 0 { Some(req.limit) } else { None };

        let result = self
            .api
            .get_migration_history(&req.namespace, &req.name, limit)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(MigrationHistoryResponse {
            entries: result.into_iter().map(Into::into).collect(),
        }))
    }

    #[instrument(skip(self))]
    async fn retry_migration(
        &self,
        request: Request<RetryMigrationRequest>,
    ) -> Result<Response<Migration>, Status> {
        let req = request.into_inner();
        let result = self
            .api
            .retry_migration(&req.namespace, &req.name, req.reset_retry_count)
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn cancel_migration(
        &self,
        request: Request<CancelMigrationRequest>,
    ) -> Result<Response<Migration>, Status> {
        let req = request.into_inner();
        let reason = if req.reason.is_empty() {
            None
        } else {
            Some(req.reason)
        };

        let result = self
            .api
            .cancel_migration(&req.namespace, &req.name, reason)
            .await
            .map_err(api_error_to_status)?;
        Ok(Response::new(result.into()))
    }
}

// =============================================================================
// Database Service Implementation
// =============================================================================

/// gRPC Database Service
pub struct DatabaseServiceImpl {
    api: Arc<MigrationApiService>,
}

impl DatabaseServiceImpl {
    pub fn new(api: Arc<MigrationApiService>) -> Self {
        Self { api }
    }
}

#[tonic::async_trait]
impl DatabaseService for DatabaseServiceImpl {
    #[instrument(skip(self))]
    async fn check_database_ready(
        &self,
        request: Request<CheckDatabaseReadyRequest>,
    ) -> Result<Response<DatabaseReadyResponse>, Status> {
        let req = request.into_inner();
        let database = if req.database.is_empty() {
            None
        } else {
            Some(req.database)
        };

        let result = self
            .api
            .check_database_ready(&req.namespace, &req.cluster_name, database)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn get_cluster_health(
        &self,
        request: Request<GetClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .api
            .get_cluster_health(&req.namespace, &req.cluster_name)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            None
        } else {
            Some(req.namespace)
        };

        let result = self
            .api
            .list_databases(namespace)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(ListDatabasesResponse {
            databases: result.into_iter().map(Into::into).collect(),
        }))
    }
}

// =============================================================================
// Queue Service Implementation
// =============================================================================

/// gRPC Queue Service
pub struct QueueServiceImpl {
    api: Arc<MigrationApiService>,
}

impl QueueServiceImpl {
    pub fn new(api: Arc<MigrationApiService>) -> Self {
        Self { api }
    }
}

#[tonic::async_trait]
impl QueueService for QueueServiceImpl {
    #[instrument(skip(self))]
    async fn get_queue_status(
        &self,
        _request: Request<GetQueueStatusRequest>,
    ) -> Result<Response<QueueStatusResponse>, Status> {
        let result = self.api.get_queue_status().await;
        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn list_queue_items(
        &self,
        request: Request<ListQueueItemsRequest>,
    ) -> Result<Response<ListQueueItemsResponse>, Status> {
        let req = request.into_inner();
        let status = if req.status.is_empty() {
            None
        } else {
            Some(req.status)
        };
        let limit = if req.limit > 0 { Some(req.limit) } else { None };

        let result = self
            .api
            .list_queue_items(status, limit)
            .await
            .map_err(api_error_to_status)?;

        Ok(Response::new(ListQueueItemsResponse {
            items: result.into_iter().map(Into::into).collect(),
        }))
    }

    #[instrument(skip(self))]
    async fn pause_queue(
        &self,
        request: Request<PauseQueueRequest>,
    ) -> Result<Response<QueueStatusResponse>, Status> {
        let req = request.into_inner();
        let reason = if req.reason.is_empty() {
            None
        } else {
            Some(req.reason)
        };

        let result = self.api.pause_queue(reason).await;
        Ok(Response::new(result.into()))
    }

    #[instrument(skip(self))]
    async fn resume_queue(
        &self,
        _request: Request<ResumeQueueRequest>,
    ) -> Result<Response<QueueStatusResponse>, Status> {
        let result = self.api.resume_queue().await;
        Ok(Response::new(result.into()))
    }
}

// =============================================================================
// Server Builder
// =============================================================================

/// Create gRPC service servers
pub fn create_grpc_services(
    api: Arc<MigrationApiService>,
) -> (
    MigrationServiceServer<MigrationServiceImpl>,
    DatabaseServiceServer<DatabaseServiceImpl>,
    QueueServiceServer<QueueServiceImpl>,
) {
    (
        MigrationServiceServer::new(MigrationServiceImpl::new(api.clone())),
        DatabaseServiceServer::new(DatabaseServiceImpl::new(api.clone())),
        QueueServiceServer::new(QueueServiceImpl::new(api)),
    )
}
