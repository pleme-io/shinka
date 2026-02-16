//! GraphQL API for Shinka
//!
//! Provides a GraphQL schema for migration management, database health,
//! and queue operations with subscriptions for real-time updates.

use async_graphql::{
    Context, Enum, InputObject, Object, Result, Schema, SimpleObject, Subscription,
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tracing::instrument;

use super::service::MigrationApiService;
use super::types::{
    ClusterHealth, DatabaseInfo, DatabaseReadiness, EventType, MigrationFilter,
    MigrationHistoryEntry, MigrationResource, QueueItem, QueueStatus,
};

// =============================================================================
// GraphQL Types
// =============================================================================

/// Migration resource
#[derive(SimpleObject, Clone)]
pub struct Migration {
    /// Resource name
    pub name: String,
    /// Kubernetes namespace
    pub namespace: String,
    /// Unique identifier
    pub uid: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Migration spec
    pub spec: MigrationSpec,
    /// Current status
    pub status: MigrationStatus,
    /// Conditions
    pub conditions: Vec<Condition>,
}

/// Migration spec
#[derive(SimpleObject, Clone)]
pub struct MigrationSpec {
    /// CNPG cluster name
    pub cnpg_cluster: String,
    /// Database name
    pub database: Option<String>,
    /// Migrator type
    pub migrator_type: String,
    /// Referenced deployment
    pub deployment_ref: String,
    /// Migration command
    pub command: Vec<String>,
    /// Whether healthy cluster is required
    pub require_healthy_cluster: bool,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Migration timeout in seconds
    pub migration_timeout_seconds: u64,
}

/// Migration status
#[derive(SimpleObject, Clone)]
pub struct MigrationStatus {
    /// Current phase
    pub phase: String,
    /// Last migration details
    pub last_migration: Option<LastMigration>,
    /// Current retry count
    pub retry_count: u32,
    /// Current job name
    pub current_job: Option<String>,
    /// Observed generation
    pub observed_generation: i64,
}

/// Last migration info
#[derive(SimpleObject, Clone)]
pub struct LastMigration {
    /// Image tag
    pub image_tag: String,
    /// Whether it succeeded
    pub success: bool,
    /// Duration in seconds
    pub duration_seconds: Option<f64>,
    /// Completion time
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message
    pub error: Option<String>,
}

/// Condition
#[derive(SimpleObject, Clone)]
pub struct Condition {
    /// Condition type
    pub condition_type: String,
    /// Status
    pub status: String,
    /// Reason
    pub reason: Option<String>,
    /// Message
    pub message: Option<String>,
    /// Last transition time
    pub last_transition_time: Option<DateTime<Utc>>,
}

/// Database readiness
#[derive(SimpleObject, Clone)]
pub struct DatabaseReady {
    /// Overall readiness
    pub ready: bool,
    /// Cluster health
    pub cluster_healthy: bool,
    /// Migrations complete
    pub migrations_complete: bool,
    /// Message
    pub message: String,
    /// Pending migrations
    pub pending_migrations: Vec<String>,
    /// Active migrations
    pub active_migrations: Vec<String>,
}

/// Cluster health
#[derive(SimpleObject, Clone)]
pub struct ClusterHealthGql {
    /// Healthy
    pub healthy: bool,
    /// Phase
    pub phase: String,
    /// Ready replicas
    pub ready_replicas: i32,
    /// Total replicas
    pub total_replicas: i32,
    /// Primary pod
    pub primary_pod: Option<String>,
    /// Last check
    pub last_check: DateTime<Utc>,
    /// Message
    pub message: String,
}

/// Database info
#[derive(SimpleObject, Clone)]
pub struct Database {
    /// Namespace
    pub namespace: String,
    /// Cluster name
    pub cluster_name: String,
    /// Database name
    pub database: Option<String>,
    /// Healthy
    pub healthy: bool,
    /// Pending migrations
    pub pending_migrations: i32,
    /// Completed migrations
    pub completed_migrations: i32,
    /// Last migration time
    pub last_migration: Option<DateTime<Utc>>,
}

/// Queue status
#[derive(SimpleObject, Clone)]
pub struct Queue {
    /// Paused
    pub paused: bool,
    /// Pending count
    pub pending: i32,
    /// Active count
    pub active: i32,
    /// Completed count
    pub completed: i32,
    /// Failed count
    pub failed: i32,
    /// Max concurrent
    pub max_concurrent: i32,
    /// Strategy
    pub strategy: String,
}

/// Queue item
#[derive(SimpleObject, Clone)]
pub struct QueueItemGql {
    /// Namespace
    pub namespace: String,
    /// Name
    pub name: String,
    /// Status
    pub status: String,
    /// Priority
    pub priority: i32,
    /// Enqueued at
    pub enqueued_at: DateTime<Utc>,
    /// Started at
    pub started_at: Option<DateTime<Utc>>,
    /// Database
    pub database: String,
}

/// Migration history entry
#[derive(SimpleObject, Clone)]
pub struct HistoryEntry {
    /// Image tag
    pub image_tag: String,
    /// Success
    pub success: bool,
    /// Started at
    pub started_at: DateTime<Utc>,
    /// Completed at
    pub completed_at: Option<DateTime<Utc>>,
    /// Duration seconds
    pub duration_seconds: Option<f64>,
    /// Error
    pub error: Option<String>,
    /// Retry count
    pub retry_count: u32,
    /// Job name
    pub job_name: String,
}

/// Migration event for subscriptions
#[derive(SimpleObject, Clone)]
pub struct MigrationEventGql {
    /// Event type
    pub event_type: MigrationEventType,
    /// Migration
    pub migration: Migration,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Event type enum
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum MigrationEventType {
    Added,
    Modified,
    Deleted,
}

// =============================================================================
// Input Types
// =============================================================================

/// Filter for listing migrations
#[derive(InputObject, Default, Debug)]
pub struct MigrationFilterInput {
    /// Namespace filter
    pub namespace: Option<String>,
    /// Phase filter
    pub phases: Option<Vec<String>>,
    /// Limit
    pub limit: Option<i32>,
}

/// Watch filter
#[derive(InputObject, Default, Debug)]
pub struct WatchFilterInput {
    /// Namespace
    pub namespace: Option<String>,
    /// Name
    pub name: Option<String>,
    /// Phases
    pub phases: Option<Vec<String>>,
}

// =============================================================================
// Conversions
// =============================================================================

impl From<MigrationResource> for Migration {
    fn from(m: MigrationResource) -> Self {
        Self {
            name: m.name,
            namespace: m.namespace,
            uid: m.uid,
            created_at: m.created_at,
            spec: MigrationSpec {
                cnpg_cluster: m.spec.cnpg_cluster,
                database: m.spec.database,
                migrator_type: m.spec.migrator_type,
                deployment_ref: m.spec.deployment_ref,
                command: m.spec.command,
                require_healthy_cluster: m.spec.require_healthy_cluster,
                max_retries: m.spec.max_retries,
                migration_timeout_seconds: m.spec.migration_timeout_seconds,
            },
            status: MigrationStatus {
                phase: m.status.phase,
                last_migration: m.status.last_migration.map(|lm| LastMigration {
                    image_tag: lm.image_tag,
                    success: lm.success,
                    duration_seconds: lm.duration_seconds,
                    completed_at: lm.completed_at,
                    error: lm.error,
                }),
                retry_count: m.status.retry_count,
                current_job: m.status.current_job,
                observed_generation: m.status.observed_generation,
            },
            conditions: m
                .conditions
                .into_iter()
                .map(|c| Condition {
                    condition_type: c.condition_type,
                    status: c.status,
                    reason: c.reason,
                    message: c.message,
                    last_transition_time: c.last_transition_time,
                })
                .collect(),
        }
    }
}

impl From<DatabaseReadiness> for DatabaseReady {
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

impl From<ClusterHealth> for ClusterHealthGql {
    fn from(c: ClusterHealth) -> Self {
        Self {
            healthy: c.healthy,
            phase: c.phase,
            ready_replicas: c.ready_replicas,
            total_replicas: c.total_replicas,
            primary_pod: c.primary_pod,
            last_check: c.last_check,
            message: c.message,
        }
    }
}

impl From<DatabaseInfo> for Database {
    fn from(d: DatabaseInfo) -> Self {
        Self {
            namespace: d.namespace,
            cluster_name: d.cluster_name,
            database: d.database,
            healthy: d.healthy,
            pending_migrations: d.pending_migrations,
            completed_migrations: d.completed_migrations,
            last_migration: d.last_migration,
        }
    }
}

impl From<QueueStatus> for Queue {
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

impl From<QueueItem> for QueueItemGql {
    fn from(q: QueueItem) -> Self {
        Self {
            namespace: q.namespace,
            name: q.name,
            status: q.status,
            priority: q.priority,
            enqueued_at: q.enqueued_at,
            started_at: q.started_at,
            database: q.database,
        }
    }
}

impl From<MigrationHistoryEntry> for HistoryEntry {
    fn from(h: MigrationHistoryEntry) -> Self {
        Self {
            image_tag: h.image_tag,
            success: h.success,
            started_at: h.started_at,
            completed_at: h.completed_at,
            duration_seconds: h.duration_seconds,
            error: h.error,
            retry_count: h.retry_count,
            job_name: h.job_name,
        }
    }
}

impl From<EventType> for MigrationEventType {
    fn from(e: EventType) -> Self {
        match e {
            EventType::Added => MigrationEventType::Added,
            EventType::Modified => MigrationEventType::Modified,
            EventType::Deleted => MigrationEventType::Deleted,
        }
    }
}

// =============================================================================
// Query Root
// =============================================================================

/// GraphQL Query root
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a specific migration
    #[instrument(skip(self, ctx))]
    async fn migration(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
    ) -> Result<Migration> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.get_migration(&namespace, &name).await?;
        Ok(result.into())
    }

    /// List migrations with optional filters
    #[instrument(skip(self, ctx))]
    async fn migrations(
        &self,
        ctx: &Context<'_>,
        filter: Option<MigrationFilterInput>,
    ) -> Result<Vec<Migration>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let filter = filter.unwrap_or_default();

        let api_filter = MigrationFilter {
            namespace: filter.namespace,
            phases: filter.phases.unwrap_or_default(),
            limit: filter.limit,
            ..Default::default()
        };

        let result = service.list_migrations(api_filter).await?;
        Ok(result.migrations.into_iter().map(Into::into).collect())
    }

    /// Get migration history
    #[instrument(skip(self, ctx))]
    async fn migration_history(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
        limit: Option<i32>,
    ) -> Result<Vec<HistoryEntry>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service
            .get_migration_history(&namespace, &name, limit)
            .await?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// Check if a database is ready
    #[instrument(skip(self, ctx))]
    async fn database_ready(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        cluster: String,
        database: Option<String>,
    ) -> Result<DatabaseReady> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service
            .check_database_ready(&namespace, &cluster, database)
            .await?;
        Ok(result.into())
    }

    /// Get cluster health
    #[instrument(skip(self, ctx))]
    async fn cluster_health(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        cluster: String,
    ) -> Result<ClusterHealthGql> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.get_cluster_health(&namespace, &cluster).await?;
        Ok(result.into())
    }

    /// List databases
    #[instrument(skip(self, ctx))]
    async fn databases(
        &self,
        ctx: &Context<'_>,
        namespace: Option<String>,
    ) -> Result<Vec<Database>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.list_databases(namespace).await?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// Get queue status
    #[instrument(skip(self, ctx))]
    async fn queue_status(&self, ctx: &Context<'_>) -> Result<Queue> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.get_queue_status().await;
        Ok(result.into())
    }

    /// List queue items
    #[instrument(skip(self, ctx))]
    async fn queue_items(
        &self,
        ctx: &Context<'_>,
        status: Option<String>,
        limit: Option<i32>,
    ) -> Result<Vec<QueueItemGql>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.list_queue_items(status, limit).await?;
        Ok(result.into_iter().map(Into::into).collect())
    }
}

// =============================================================================
// Mutation Root
// =============================================================================

/// GraphQL Mutation root
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Retry a failed migration
    #[instrument(skip(self, ctx))]
    async fn retry_migration(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
        reset_retry_count: Option<bool>,
    ) -> Result<Migration> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service
            .retry_migration(&namespace, &name, reset_retry_count.unwrap_or(false))
            .await?;
        Ok(result.into())
    }

    /// Cancel a running migration
    #[instrument(skip(self, ctx))]
    async fn cancel_migration(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
        reason: Option<String>,
    ) -> Result<Migration> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.cancel_migration(&namespace, &name, reason).await?;
        Ok(result.into())
    }

    /// Pause the migration queue
    #[instrument(skip(self, ctx))]
    async fn pause_queue(&self, ctx: &Context<'_>, reason: Option<String>) -> Result<Queue> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.pause_queue(reason).await;
        Ok(result.into())
    }

    /// Resume the migration queue
    #[instrument(skip(self, ctx))]
    async fn resume_queue(&self, ctx: &Context<'_>) -> Result<Queue> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let result = service.resume_queue().await;
        Ok(result.into())
    }
}

// =============================================================================
// Subscription Root
// =============================================================================

/// GraphQL Subscription root
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Watch for migration changes
    #[instrument(skip(self, ctx))]
    async fn migrations(
        &self,
        ctx: &Context<'_>,
        filter: Option<WatchFilterInput>,
    ) -> Result<impl Stream<Item = MigrationEventGql>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let filter = filter.unwrap_or_default();

        let stream = service
            .watch_migrations(
                filter.namespace,
                filter.name,
                filter.phases.unwrap_or_default(),
            )
            .await;

        Ok(stream.map(|event| MigrationEventGql {
            event_type: event.event_type.into(),
            migration: event.migration.into(),
            timestamp: event.timestamp,
        }))
    }
}

// =============================================================================
// Schema
// =============================================================================

pub type ShinkaSchemA = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// Create the GraphQL schema
pub fn create_schema(service: Arc<MigrationApiService>) -> ShinkaSchemA {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(service)
        .finish()
}
