//! Core API Service Layer
//!
//! This module contains the shared business logic for all API protocols (REST, GraphQL, gRPC).
//! Each protocol adapter translates requests/responses to/from these common service methods.

use chrono::Utc;
use futures::StreamExt;
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    runtime::watcher::{self, Event},
    Client, ResourceExt,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, instrument, warn};

use crate::crd::{DatabaseMigration, MigrationPhase};
use crate::database;

use super::types::{
    ApiError, ClusterHealth, DatabaseInfo, DatabaseReadiness, EventType, MigrationEvent,
    MigrationFilter, MigrationHistoryEntry, MigrationList, MigrationResource, QueueItem,
    QueueStatus,
};

/// Core API service providing migration management operations
///
/// This service is shared across all API protocols and contains
/// the actual business logic for migration operations.
#[derive(Clone)]
pub struct MigrationApiService {
    /// Kubernetes client
    client: Client,

    /// Shared queue state (for queue operations)
    queue_state: Arc<RwLock<QueueState>>,

    /// Event broadcaster for watch operations
    event_sender: tokio::sync::broadcast::Sender<MigrationEvent>,
}

/// Internal queue state tracking
#[derive(Default)]
struct QueueState {
    paused: bool,
    pause_reason: Option<String>,
}

impl MigrationApiService {
    /// Create a new migration API service
    pub fn new(client: Client) -> Self {
        let (event_sender, _) = tokio::sync::broadcast::channel(1024);

        Self {
            client,
            queue_state: Arc::new(RwLock::new(QueueState::default())),
            event_sender,
        }
    }

    /// Get the Kubernetes client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Subscribe to migration events
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<MigrationEvent> {
        self.event_sender.subscribe()
    }

    // =========================================================================
    // Migration Operations
    // =========================================================================

    /// Get a specific migration by namespace and name
    #[instrument(skip(self), err)]
    pub async fn get_migration(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<MigrationResource, ApiError> {
        let api: Api<DatabaseMigration> = Api::namespaced(self.client.clone(), namespace);

        match api.get(name).await {
            Ok(dm) => Ok(MigrationResource::from(&dm)),
            Err(kube::Error::Api(e)) if e.code == 404 => {
                Err(ApiError::not_found("DatabaseMigration", name))
            }
            Err(e) => {
                error!(error = %e, namespace, name, "Failed to get migration");
                Err(ApiError::internal(format!(
                    "Failed to get migration: {}",
                    e
                )))
            }
        }
    }

    /// List migrations with optional filters
    #[instrument(skip(self), err)]
    pub async fn list_migrations(
        &self,
        filter: MigrationFilter,
    ) -> Result<MigrationList, ApiError> {
        let mut list_params = ListParams::default();

        // Apply label selector
        if !filter.label_selector.is_empty() {
            let selector = filter
                .label_selector
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            list_params = list_params.labels(&selector);
        }

        // Apply limit
        if let Some(limit) = filter.limit {
            list_params = list_params.limit(limit as u32);
        }

        // Apply continue token
        if let Some(ref token) = filter.continue_token {
            list_params = list_params.continue_token(token);
        }

        // Get migrations
        let migrations: Vec<MigrationResource> = if let Some(ref ns) = filter.namespace {
            let api: Api<DatabaseMigration> = Api::namespaced(self.client.clone(), ns);
            let list = api.list(&list_params).await.map_err(|e| {
                error!(error = %e, "Failed to list migrations");
                ApiError::internal(format!("Failed to list migrations: {}", e))
            })?;

            list.items.iter().map(MigrationResource::from).collect()
        } else {
            let api: Api<DatabaseMigration> = Api::all(self.client.clone());
            let list = api.list(&list_params).await.map_err(|e| {
                error!(error = %e, "Failed to list migrations");
                ApiError::internal(format!("Failed to list migrations: {}", e))
            })?;

            list.items.iter().map(MigrationResource::from).collect()
        };

        // Filter by phase if specified
        let migrations = if filter.phases.is_empty() {
            migrations
        } else {
            migrations
                .into_iter()
                .filter(|m| filter.phases.contains(&m.status.phase))
                .collect()
        };

        Ok(MigrationList {
            migrations,
            continue_token: None, // Pagination not supported yet
            total_count: None,
        })
    }

    /// Start watching migrations for changes
    #[instrument(skip(self))]
    pub async fn watch_migrations(
        &self,
        namespace: Option<String>,
        name: Option<String>,
        phases: Vec<String>,
    ) -> impl futures::Stream<Item = MigrationEvent> {
        let client = self.client.clone();
        let event_sender = self.event_sender.clone();

        // Create the watcher
        let api: Api<DatabaseMigration> = if let Some(ref ns) = namespace {
            Api::namespaced(client.clone(), ns)
        } else {
            Api::all(client.clone())
        };

        let watcher_config = watcher::Config::default();
        let stream = watcher::watcher(api, watcher_config);

        // Process events
        let name_filter = name.clone();
        let phases_filter = phases.clone();

        stream.filter_map(move |event| {
            let event_sender = event_sender.clone();
            let name_filter = name_filter.clone();
            let phases_filter = phases_filter.clone();

            async move {
                match event {
                    Ok(Event::Apply(dm)) => {
                        // Apply name filter
                        if let Some(ref n) = name_filter {
                            if dm.name_any() != *n {
                                return None;
                            }
                        }

                        let resource = MigrationResource::from(&dm);

                        // Apply phase filter
                        if !phases_filter.is_empty()
                            && !phases_filter.contains(&resource.status.phase)
                        {
                            return None;
                        }

                        let event = MigrationEvent {
                            event_type: EventType::Modified,
                            migration: resource,
                            timestamp: Utc::now(),
                        };

                        // Broadcast to other subscribers
                        let _ = event_sender.send(event.clone());

                        Some(event)
                    }
                    Ok(Event::Delete(dm)) => {
                        if let Some(ref n) = name_filter {
                            if dm.name_any() != *n {
                                return None;
                            }
                        }

                        let resource = MigrationResource::from(&dm);
                        let event = MigrationEvent {
                            event_type: EventType::Deleted,
                            migration: resource,
                            timestamp: Utc::now(),
                        };

                        let _ = event_sender.send(event.clone());
                        Some(event)
                    }
                    Ok(Event::Init) | Ok(Event::InitApply(_)) | Ok(Event::InitDone) => None,
                    Err(e) => {
                        warn!(error = %e, "Watch error");
                        None
                    }
                }
            }
        })
    }

    /// Get migration history for a specific migration
    #[instrument(skip(self), err)]
    pub async fn get_migration_history(
        &self,
        namespace: &str,
        name: &str,
        limit: Option<i32>,
    ) -> Result<Vec<MigrationHistoryEntry>, ApiError> {
        // Note: Full history requires persistent storage (Redis, DB)
        // For MVP, return only the last migration from CRD status
        let migration = self.get_migration(namespace, name).await?;

        let mut entries = Vec::new();

        if let Some(last) = migration.status.last_migration {
            entries.push(MigrationHistoryEntry {
                image_tag: last.image_tag,
                success: last.success,
                started_at: last
                    .completed_at
                    .unwrap_or_else(Utc::now)
                    .checked_sub_signed(chrono::Duration::seconds(
                        last.duration_seconds.map(|d| d as i64).unwrap_or(0),
                    ))
                    .unwrap_or_else(Utc::now),
                completed_at: last.completed_at,
                duration_seconds: last.duration_seconds,
                error: last.error,
                retry_count: migration.status.retry_count,
                job_name: migration.status.current_job.unwrap_or_default(),
            });
        }

        // Apply limit
        if let Some(l) = limit {
            entries.truncate(l as usize);
        }

        Ok(entries)
    }

    /// Retry a failed migration
    #[instrument(skip(self), err)]
    pub async fn retry_migration(
        &self,
        namespace: &str,
        name: &str,
        reset_retry_count: bool,
    ) -> Result<MigrationResource, ApiError> {
        let api: Api<DatabaseMigration> = Api::namespaced(self.client.clone(), namespace);

        // Get current migration
        let dm = api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ref api_err) = e {
                if api_err.code == 404 {
                    return ApiError::not_found("DatabaseMigration", name);
                }
            }
            ApiError::internal(format!("Failed to get migration: {}", e))
        })?;

        // Check if it's in a failed state
        let phase = dm
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .cloned()
            .unwrap_or(MigrationPhase::Pending);

        if phase != MigrationPhase::Failed {
            return Err(ApiError::failed_precondition(format!(
                "Migration is not in Failed state, current phase: {}",
                phase
            )));
        }

        // Patch to trigger retry
        let retry_count = if reset_retry_count {
            0
        } else {
            dm.status.as_ref().and_then(|s| s.retry_count).unwrap_or(0)
        };

        let patch = serde_json::json!({
            "status": {
                "phase": "Pending",
                "retryCount": retry_count
            }
        });

        let patched = api
            .patch_status(
                name,
                &PatchParams::apply("shinka-api"),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(|e| ApiError::internal(format!("Failed to patch migration: {}", e)))?;

        info!(namespace, name, "Migration retry triggered via API");

        Ok(MigrationResource::from(&patched))
    }

    /// Cancel a running migration
    #[instrument(skip(self), err)]
    pub async fn cancel_migration(
        &self,
        namespace: &str,
        name: &str,
        reason: Option<String>,
    ) -> Result<MigrationResource, ApiError> {
        let api: Api<DatabaseMigration> = Api::namespaced(self.client.clone(), namespace);

        // Get current migration
        let dm = api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ref api_err) = e {
                if api_err.code == 404 {
                    return ApiError::not_found("DatabaseMigration", name);
                }
            }
            ApiError::internal(format!("Failed to get migration: {}", e))
        })?;

        // Check if it's in a cancellable state
        let phase = dm
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .cloned()
            .unwrap_or(MigrationPhase::Pending);

        if phase == MigrationPhase::Ready || phase == MigrationPhase::Failed {
            return Err(ApiError::failed_precondition(format!(
                "Migration cannot be cancelled in {} state",
                phase
            )));
        }

        // Patch to mark as failed with cancellation reason
        let error_msg = reason.unwrap_or_else(|| "Cancelled via API".to_string());
        let patch = serde_json::json!({
            "status": {
                "phase": "Failed",
                "lastMigration": {
                    "success": false,
                    "error": error_msg
                }
            }
        });

        let patched = api
            .patch_status(
                name,
                &PatchParams::apply("shinka-api"),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(|e| ApiError::internal(format!("Failed to cancel migration: {}", e)))?;

        info!(namespace, name, "Migration cancelled via API");

        Ok(MigrationResource::from(&patched))
    }

    // =========================================================================
    // Database Operations
    // =========================================================================

    /// Check if a database is ready (healthy + migrations complete)
    #[instrument(skip(self), err)]
    pub async fn check_database_ready(
        &self,
        namespace: &str,
        cluster_name: &str,
        database: Option<String>,
    ) -> Result<DatabaseReadiness, ApiError> {
        // Check cluster health
        let cluster_healthy =
            database::check_cluster_health(self.client.clone(), namespace, cluster_name)
                .await
                .map(|h| h.healthy)
                .unwrap_or(false);

        // Get migrations for this database
        let filter = MigrationFilter {
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        let migrations = self.list_migrations(filter).await?;

        // Filter to this cluster/database
        let relevant_migrations: Vec<_> = migrations
            .migrations
            .into_iter()
            .filter(|m| {
                m.spec.cnpg_cluster == cluster_name
                    && (database.is_none() || m.spec.database == database)
            })
            .collect();

        // Categorize migrations
        let mut pending = Vec::new();
        let mut active = Vec::new();
        let mut all_complete = true;

        for m in &relevant_migrations {
            match m.status.phase.as_str() {
                "Ready" => {}
                "Failed" => all_complete = false,
                "Migrating" | "CheckingHealth" | "WaitingForDatabase" => {
                    active.push(m.name.clone());
                    all_complete = false;
                }
                _ => {
                    pending.push(m.name.clone());
                    all_complete = false;
                }
            }
        }

        let migrations_complete = all_complete && pending.is_empty() && active.is_empty();
        let ready = cluster_healthy && migrations_complete;

        let message = if !cluster_healthy {
            "CNPG cluster is not healthy".to_string()
        } else if !active.is_empty() {
            format!("Migrations in progress: {}", active.join(", "))
        } else if !pending.is_empty() {
            format!("Migrations pending: {}", pending.join(", "))
        } else if !migrations_complete {
            "Some migrations have failed".to_string()
        } else {
            "Database is ready".to_string()
        };

        Ok(DatabaseReadiness {
            ready,
            cluster_healthy,
            migrations_complete,
            message,
            pending_migrations: pending,
            active_migrations: active,
        })
    }

    /// Get CNPG cluster health
    #[instrument(skip(self), err)]
    pub async fn get_cluster_health(
        &self,
        namespace: &str,
        cluster_name: &str,
    ) -> Result<ClusterHealth, ApiError> {
        let health = database::check_cluster_health(self.client.clone(), namespace, cluster_name)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to get cluster health: {}", e)))?;

        Ok(ClusterHealth {
            healthy: health.healthy,
            phase: health.phase,
            ready_replicas: health.ready_instances,
            total_replicas: health.instances,
            primary_pod: None, // Not available from CNPG health check
            last_check: Utc::now(),
            message: health.message,
        })
    }

    /// List all monitored databases
    #[instrument(skip(self), err)]
    pub async fn list_databases(
        &self,
        namespace: Option<String>,
    ) -> Result<Vec<DatabaseInfo>, ApiError> {
        let filter = MigrationFilter {
            namespace,
            ..Default::default()
        };
        let migrations = self.list_migrations(filter).await?;

        // Group by cluster/database
        let mut databases: std::collections::HashMap<
            (String, String, Option<String>),
            DatabaseInfo,
        > = std::collections::HashMap::new();

        for m in migrations.migrations {
            let key = (
                m.namespace.clone(),
                m.spec.cnpg_cluster.clone(),
                m.spec.database.clone(),
            );

            let entry = databases
                .entry(key.clone())
                .or_insert_with(|| DatabaseInfo {
                    namespace: key.0.clone(),
                    cluster_name: key.1.clone(),
                    database: key.2.clone(),
                    healthy: true,
                    pending_migrations: 0,
                    completed_migrations: 0,
                    last_migration: None,
                });

            match m.status.phase.as_str() {
                "Ready" => {
                    entry.completed_migrations += 1;
                    if let Some(last) = m.status.last_migration {
                        if entry.last_migration.is_none()
                            || last.completed_at > entry.last_migration
                        {
                            entry.last_migration = last.completed_at;
                        }
                    }
                }
                "Failed" => {
                    entry.healthy = false;
                    entry.pending_migrations += 1;
                }
                _ => {
                    entry.pending_migrations += 1;
                }
            }
        }

        Ok(databases.into_values().collect())
    }

    // =========================================================================
    // Queue Operations
    // =========================================================================

    /// Get queue status
    #[instrument(skip(self))]
    pub async fn get_queue_status(&self) -> QueueStatus {
        let state = self.queue_state.read().await;

        // Get migrations in various states
        let filter = MigrationFilter::default();
        let migrations = self
            .list_migrations(filter)
            .await
            .unwrap_or_else(|_| MigrationList {
                migrations: vec![],
                continue_token: None,
                total_count: None,
            });

        let mut pending = 0;
        let mut active = 0;
        let mut completed = 0;
        let mut failed = 0;

        for m in &migrations.migrations {
            match m.status.phase.as_str() {
                "Pending" => pending += 1,
                "CheckingHealth" | "WaitingForDatabase" | "Migrating" => active += 1,
                "Ready" => completed += 1,
                "Failed" => failed += 1,
                _ => {}
            }
        }

        QueueStatus {
            paused: state.paused,
            pending,
            active,
            completed,
            failed,
            max_concurrent: 10, // Default max concurrent migrations
            strategy: "Fair".to_string(),
        }
    }

    /// List queue items
    #[instrument(skip(self), err)]
    pub async fn list_queue_items(
        &self,
        status_filter: Option<String>,
        limit: Option<i32>,
    ) -> Result<Vec<QueueItem>, ApiError> {
        let filter = MigrationFilter::default();
        let migrations = self.list_migrations(filter).await?;

        let mut items: Vec<QueueItem> = migrations
            .migrations
            .into_iter()
            .filter(|m| {
                if let Some(ref s) = status_filter {
                    let phase = &m.status.phase;
                    match s.as_str() {
                        "pending" => phase == "Pending",
                        "active" => {
                            phase == "CheckingHealth"
                                || phase == "WaitingForDatabase"
                                || phase == "Migrating"
                        }
                        "completed" => phase == "Ready",
                        "failed" => phase == "Failed",
                        _ => true,
                    }
                } else {
                    true
                }
            })
            .map(|m| QueueItem {
                namespace: m.namespace.clone(),
                name: m.name.clone(),
                status: m.status.phase.clone(),
                priority: 0, // Default priority (FIFO ordering)
                enqueued_at: m.created_at,
                started_at: None, // Not tracked in current implementation
                database: format!(
                    "{}/{}",
                    m.spec.cnpg_cluster,
                    m.spec.database.as_deref().unwrap_or("default")
                ),
            })
            .collect();

        if let Some(l) = limit {
            items.truncate(l as usize);
        }

        Ok(items)
    }

    /// Pause the migration queue
    #[instrument(skip(self))]
    pub async fn pause_queue(&self, reason: Option<String>) -> QueueStatus {
        let mut state = self.queue_state.write().await;
        state.paused = true;
        state.pause_reason = reason;

        info!("Migration queue paused via API");

        drop(state);
        self.get_queue_status().await
    }

    /// Resume the migration queue
    #[instrument(skip(self))]
    pub async fn resume_queue(&self) -> QueueStatus {
        let mut state = self.queue_state.write().await;
        state.paused = false;
        state.pause_reason = None;

        info!("Migration queue resumed via API");

        drop(state);
        self.get_queue_status().await
    }

    // =========================================================================
    // Release Await Operations
    // =========================================================================

    /// Block until a migration reaches Ready or Failed for the expected tag
    ///
    /// Uses kube-rs watcher API internally for event-driven waiting.
    /// Max timeout capped at 900s.
    #[instrument(skip(self), err)]
    pub async fn await_migration(
        &self,
        namespace: &str,
        name: &str,
        expected_tag: &str,
        timeout_secs: u64,
    ) -> Result<super::types::AwaitResponse, ApiError> {
        use super::types::AwaitResponse;

        let timeout_secs = timeout_secs.min(900);
        let start = std::time::Instant::now();

        info!(
            namespace = %namespace,
            name = %name,
            expected_tag = %expected_tag,
            timeout_secs = %timeout_secs,
            "Awaiting migration completion"
        );

        let api: Api<DatabaseMigration> = Api::namespaced(self.client.clone(), namespace);

        // First, check current state immediately
        match api.get(name).await {
            Ok(dm) => {
                if let Some(resp) = check_migration_done(&dm, expected_tag, start.elapsed().as_secs_f64()) {
                    return Ok(resp);
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                return Err(ApiError::not_found("DatabaseMigration", name));
            }
            Err(e) => {
                return Err(ApiError::internal(format!("Failed to get migration: {}", e)));
            }
        }

        // Poll with kube-rs watch (event-driven with timeout)
        let timeout = tokio::time::Duration::from_secs(timeout_secs);
        let poll_interval = tokio::time::Duration::from_secs(2);

        let result = tokio::time::timeout(timeout, async {
            loop {
                tokio::time::sleep(poll_interval).await;

                match api.get(name).await {
                    Ok(dm) => {
                        if let Some(resp) = check_migration_done(&dm, expected_tag, start.elapsed().as_secs_f64()) {
                            return Ok(resp);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Error polling migration during await");
                    }
                }
            }
        })
        .await;

        match result {
            Ok(resp) => resp,
            Err(_) => {
                // Timeout
                Ok(AwaitResponse {
                    success: false,
                    phase: "Timeout".to_string(),
                    image_tag: None,
                    waited_secs: start.elapsed().as_secs_f64(),
                    error: Some(format!(
                        "Timeout after {}s waiting for tag '{}'",
                        timeout_secs, expected_tag
                    )),
                    migrator_results: Vec::new(),
                })
            }
        }
    }
}

/// Check if a DatabaseMigration has reached a terminal state for the expected tag
fn check_migration_done(
    dm: &DatabaseMigration,
    expected_tag: &str,
    waited_secs: f64,
) -> Option<super::types::AwaitResponse> {
    use super::types::{AwaitResponse, MigratorResultSummary};

    let status = dm.status.as_ref()?;
    let phase = status.phase.as_ref()?;
    let last_migration = status.last_migration.as_ref()?;

    if last_migration.image_tag != expected_tag {
        return None;
    }

    let migrator_results = status
        .migrator_results
        .as_ref()
        .map(|results| {
            results
                .iter()
                .map(|r| MigratorResultSummary {
                    name: r.name.clone(),
                    success: r.success,
                    duration: r.duration.clone(),
                    error: r.error.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    match phase {
        MigrationPhase::Ready => Some(AwaitResponse {
            success: true,
            phase: "Ready".to_string(),
            image_tag: Some(last_migration.image_tag.clone()),
            waited_secs,
            error: None,
            migrator_results,
        }),
        MigrationPhase::Failed => Some(AwaitResponse {
            success: false,
            phase: "Failed".to_string(),
            image_tag: Some(last_migration.image_tag.clone()),
            waited_secs,
            error: last_migration.error.clone(),
            migrator_results,
        }),
        _ => None,
    }
}
