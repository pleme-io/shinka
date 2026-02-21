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
// GraphQL-Only Types (not shared with REST/gRPC)
// =============================================================================

/// Migration event for subscriptions
#[derive(SimpleObject, Clone)]
pub struct MigrationEventGql {
    /// Event type
    pub event_type: MigrationEventType,
    /// Migration
    pub migration: MigrationResource,
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
    ) -> Result<MigrationResource> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.get_migration(&namespace, &name).await?)
    }

    /// List migrations with optional filters
    #[instrument(skip(self, ctx))]
    async fn migrations(
        &self,
        ctx: &Context<'_>,
        filter: Option<MigrationFilterInput>,
    ) -> Result<Vec<MigrationResource>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        let filter = filter.unwrap_or_default();

        let api_filter = MigrationFilter {
            namespace: filter.namespace,
            phases: filter.phases.unwrap_or_default(),
            limit: filter.limit,
            ..Default::default()
        };

        let result = service.list_migrations(api_filter).await?;
        Ok(result.migrations)
    }

    /// Get migration history
    #[instrument(skip(self, ctx))]
    async fn migration_history(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
        limit: Option<i32>,
    ) -> Result<Vec<MigrationHistoryEntry>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service
            .get_migration_history(&namespace, &name, limit)
            .await?)
    }

    /// Check if a database is ready
    #[instrument(skip(self, ctx))]
    async fn database_ready(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        cluster: String,
        database: Option<String>,
    ) -> Result<DatabaseReadiness> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service
            .check_database_ready(&namespace, &cluster, database)
            .await?)
    }

    /// Get cluster health
    #[instrument(skip(self, ctx))]
    async fn cluster_health(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        cluster: String,
    ) -> Result<ClusterHealth> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.get_cluster_health(&namespace, &cluster).await?)
    }

    /// List databases
    #[instrument(skip(self, ctx))]
    async fn databases(
        &self,
        ctx: &Context<'_>,
        namespace: Option<String>,
    ) -> Result<Vec<DatabaseInfo>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.list_databases(namespace).await?)
    }

    /// Get queue status
    #[instrument(skip(self, ctx))]
    async fn queue_status(&self, ctx: &Context<'_>) -> Result<QueueStatus> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.get_queue_status().await)
    }

    /// List queue items
    #[instrument(skip(self, ctx))]
    async fn queue_items(
        &self,
        ctx: &Context<'_>,
        status: Option<String>,
        limit: Option<i32>,
    ) -> Result<Vec<QueueItem>> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.list_queue_items(status, limit).await?)
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
    ) -> Result<MigrationResource> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service
            .retry_migration(&namespace, &name, reset_retry_count.unwrap_or(false))
            .await?)
    }

    /// Cancel a running migration
    #[instrument(skip(self, ctx))]
    async fn cancel_migration(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
        reason: Option<String>,
    ) -> Result<MigrationResource> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service
            .cancel_migration(&namespace, &name, reason)
            .await?)
    }

    /// Pause the migration queue
    #[instrument(skip(self, ctx))]
    async fn pause_queue(&self, ctx: &Context<'_>, reason: Option<String>) -> Result<QueueStatus> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.pause_queue(reason).await)
    }

    /// Resume the migration queue
    #[instrument(skip(self, ctx))]
    async fn resume_queue(&self, ctx: &Context<'_>) -> Result<QueueStatus> {
        let service = ctx.data::<Arc<MigrationApiService>>()?;
        Ok(service.resume_queue().await)
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
            migration: event.migration,
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
