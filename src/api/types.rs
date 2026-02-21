//! Common API types shared across REST, GraphQL, and gRPC interfaces
//!
//! These types represent the canonical API representations that are translated
//! to/from protocol-specific formats by each API layer.

use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::crd::{
    Condition as CrdCondition, DatabaseMigration, DatabaseMigrationStatus, LastMigration,
};

// =============================================================================
// Core API Types
// =============================================================================

/// Migration resource as exposed via API
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "Migration")]
pub struct MigrationResource {
    /// Resource name
    pub name: String,

    /// Kubernetes namespace
    pub namespace: String,

    /// Unique identifier
    pub uid: String,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Migration spec summary
    pub spec: MigrationSpecSummary,

    /// Current status
    pub status: MigrationStatusSummary,

    /// Current conditions
    pub conditions: Vec<ConditionInfo>,
}

/// Summary of migration spec for API responses
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "MigrationSpec")]
pub struct MigrationSpecSummary {
    /// CNPG cluster name
    pub cnpg_cluster: String,

    /// Database name
    pub database: Option<String>,

    /// Migrator type (sqlx, goose, etc.)
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

/// Summary of migration status for API responses
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "MigrationStatus")]
pub struct MigrationStatusSummary {
    /// Current phase
    pub phase: String,

    /// Last migration details
    pub last_migration: Option<LastMigrationInfo>,

    /// Current retry count
    pub retry_count: u32,

    /// Current job name
    pub current_job: Option<String>,

    /// Observed generation
    pub observed_generation: i64,
}

/// Last migration information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "LastMigration")]
pub struct LastMigrationInfo {
    /// Image tag that was migrated
    pub image_tag: String,

    /// Whether the migration succeeded
    pub success: bool,

    /// Duration in seconds
    pub duration_seconds: Option<f64>,

    /// Completion time
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if failed
    pub error: Option<String>,
}

/// Condition information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "Condition")]
pub struct ConditionInfo {
    /// Condition type
    pub condition_type: String,

    /// Status (True, False, Unknown)
    pub status: String,

    /// Reason code
    pub reason: Option<String>,

    /// Human-readable message
    pub message: Option<String>,

    /// Last transition time
    pub last_transition_time: Option<DateTime<Utc>>,
}

// =============================================================================
// Database Types
// =============================================================================

/// Database readiness information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "DatabaseReady")]
pub struct DatabaseReadiness {
    /// Overall readiness
    pub ready: bool,

    /// Cluster health status
    pub cluster_healthy: bool,

    /// Whether all migrations are complete
    pub migrations_complete: bool,

    /// Human-readable message
    pub message: String,

    /// Pending migrations for this database
    pub pending_migrations: Vec<String>,

    /// Currently running migrations
    pub active_migrations: Vec<String>,
}

/// CNPG cluster health information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "ClusterHealth")]
pub struct ClusterHealth {
    /// Overall health status
    pub healthy: bool,

    /// CNPG cluster phase
    pub phase: String,

    /// Ready replica count
    pub ready_replicas: i32,

    /// Total replica count
    pub total_replicas: i32,

    /// Primary pod name
    pub primary_pod: Option<String>,

    /// Last health check time
    pub last_check: DateTime<Utc>,

    /// Status message
    pub message: String,
}

/// Database summary information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "Database")]
pub struct DatabaseInfo {
    /// Kubernetes namespace
    pub namespace: String,

    /// CNPG cluster name
    pub cluster_name: String,

    /// Database name
    pub database: Option<String>,

    /// Whether the database is healthy
    pub healthy: bool,

    /// Number of pending migrations
    pub pending_migrations: i32,

    /// Number of completed migrations
    pub completed_migrations: i32,

    /// Last migration timestamp
    pub last_migration: Option<DateTime<Utc>>,
}

// =============================================================================
// Queue Types
// =============================================================================

/// Migration queue status
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "Queue")]
pub struct QueueStatus {
    /// Whether the queue is paused
    pub paused: bool,

    /// Number of pending migrations
    pub pending: i32,

    /// Number of active migrations
    pub active: i32,

    /// Number of completed migrations
    pub completed: i32,

    /// Number of failed migrations
    pub failed: i32,

    /// Maximum concurrent migrations
    pub max_concurrent: i32,

    /// Queue strategy
    pub strategy: String,
}

/// Queue item information
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "QueueItem")]
pub struct QueueItem {
    /// Migration namespace
    pub namespace: String,

    /// Migration name
    pub name: String,

    /// Queue status
    pub status: String,

    /// Priority
    pub priority: i32,

    /// When the item was enqueued
    pub enqueued_at: DateTime<Utc>,

    /// When the item started processing
    pub started_at: Option<DateTime<Utc>>,

    /// Target database
    pub database: String,
}

/// Migration history entry
#[derive(Clone, Debug, SimpleObject, Serialize, Deserialize)]
#[graphql(name = "HistoryEntry")]
pub struct MigrationHistoryEntry {
    /// Image tag
    pub image_tag: String,

    /// Whether it succeeded
    pub success: bool,

    /// Start time
    pub started_at: DateTime<Utc>,

    /// Completion time
    pub completed_at: Option<DateTime<Utc>>,

    /// Duration in seconds
    pub duration_seconds: Option<f64>,

    /// Error message
    pub error: Option<String>,

    /// Retry count at completion
    pub retry_count: u32,

    /// Job name
    pub job_name: String,
}

// =============================================================================
// Request/Response Types
// =============================================================================

/// Filter options for listing migrations
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MigrationFilter {
    /// Optional namespace filter
    pub namespace: Option<String>,

    /// Label selector
    pub label_selector: BTreeMap<String, String>,

    /// Phase filter
    pub phases: Vec<String>,

    /// Result limit
    pub limit: Option<i32>,

    /// Pagination token
    pub continue_token: Option<String>,
}

/// Paginated list response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationList {
    /// List of migrations
    pub migrations: Vec<MigrationResource>,

    /// Continuation token for pagination
    pub continue_token: Option<String>,

    /// Total count (if available)
    pub total_count: Option<i32>,
}

/// Migration event for watch streams
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationEvent {
    /// Event type: Added, Modified, Deleted
    pub event_type: EventType,

    /// The migration
    pub migration: MigrationResource,

    /// Event timestamp
    pub timestamp: DateTime<Utc>,
}

/// Event type for watch streams
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Added,
    Modified,
    Deleted,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Added => write!(f, "ADDED"),
            EventType::Modified => write!(f, "MODIFIED"),
            EventType::Deleted => write!(f, "DELETED"),
        }
    }
}

// =============================================================================
// Release Await Types
// =============================================================================

/// Query parameters for the await endpoint
#[derive(Clone, Debug, Deserialize)]
pub struct AwaitQuery {
    /// Expected image tag to wait for (e.g., "amd64-abc1234")
    pub tag: String,

    /// Maximum timeout in seconds (default: 600, max: 900)
    #[serde(default = "default_await_timeout")]
    pub timeout: u64,
}

fn default_await_timeout() -> u64 {
    600
}

/// Response from the await endpoint
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AwaitResponse {
    /// Whether the migration completed successfully
    pub success: bool,

    /// Final phase of the migration
    pub phase: String,

    /// Image tag that was migrated
    pub image_tag: Option<String>,

    /// Seconds waited before completion
    pub waited_secs: f64,

    /// Error message if failed or timed out
    pub error: Option<String>,

    /// Individual migrator results
    pub migrator_results: Vec<MigratorResultSummary>,
}

/// Summary of a single migrator's result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigratorResultSummary {
    /// Migrator name
    pub name: String,

    /// Whether this migrator succeeded
    pub success: bool,

    /// Duration string
    pub duration: Option<String>,

    /// Error message if failed
    pub error: Option<String>,
}

// =============================================================================
// API Error Types
// =============================================================================

/// API error type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiError {
    /// Error code
    pub code: String,

    /// HTTP status code equivalent
    pub status: u16,

    /// Human-readable message
    pub message: String,

    /// Additional details
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn not_found(resource: &str, name: &str) -> Self {
        Self {
            code: "NOT_FOUND".to_string(),
            status: 404,
            message: format!("{} '{}' not found", resource, name),
            details: None,
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_ARGUMENT".to_string(),
            status: 400,
            message: message.into(),
            details: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: "INTERNAL".to_string(),
            status: 500,
            message: message.into(),
            details: None,
        }
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            code: "PERMISSION_DENIED".to_string(),
            status: 403,
            message: message.into(),
            details: None,
        }
    }

    pub fn already_exists(resource: &str, name: &str) -> Self {
        Self {
            code: "ALREADY_EXISTS".to_string(),
            status: 409,
            message: format!("{} '{}' already exists", resource, name),
            details: None,
        }
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self {
            code: "FAILED_PRECONDITION".to_string(),
            status: 412,
            message: message.into(),
            details: None,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

impl From<crate::Error> for ApiError {
    fn from(err: crate::Error) -> Self {
        match &err {
            crate::Error::ClusterNotFound { name, namespace } => {
                Self::not_found("Cluster", &format!("{}/{}", namespace, name))
            }
            crate::Error::ResourceNotFound {
                kind,
                name,
                namespace,
            } => Self::not_found(kind, &format!("{}/{}", namespace, name)),
            crate::Error::DeploymentNotFound { name, namespace } => {
                Self::not_found("Deployment", &format!("{}/{}", namespace, name))
            }
            crate::Error::Configuration(msg) => Self::invalid_argument(msg.clone()),
            crate::Error::InvalidConfig(msg) => Self::invalid_argument(msg.clone()),
            crate::Error::MissingField(field) => {
                Self::invalid_argument(format!("Missing required field: {}", field))
            }
            crate::Error::InvalidTimeout(msg) => Self::invalid_argument(msg.clone()),
            crate::Error::MigrationFailed { name, reason } => {
                Self::failed_precondition(format!("Migration {} failed: {}", name, reason))
            }
            _ => Self::internal(err.to_string()),
        }
    }
}

// =============================================================================
// Conversion Implementations
// =============================================================================

impl From<&DatabaseMigration> for MigrationResource {
    fn from(dm: &DatabaseMigration) -> Self {
        let metadata = &dm.metadata;
        let spec = &dm.spec;
        let status = dm.status.as_ref();

        // Parse migration timeout
        let timeout_str = &spec.timeouts.migration;
        let timeout_seconds = humantime::parse_duration(timeout_str)
            .map(|d| d.as_secs())
            .unwrap_or(300);

        Self {
            name: metadata.name.clone().unwrap_or_default(),
            namespace: metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            uid: metadata.uid.clone().unwrap_or_default(),
            created_at: metadata
                .creation_timestamp
                .as_ref()
                .map(|t| t.0)
                .unwrap_or_else(Utc::now),
            spec: {
                // Get the first migrator for summary (or use defaults)
                let first_migrator = dm.get_migrators().first().copied();
                MigrationSpecSummary {
                    cnpg_cluster: spec.database.cnpg_cluster_ref.name.clone(),
                    database: spec.database.cnpg_cluster_ref.database.clone(),
                    migrator_type: first_migrator
                        .map(|m| m.migrator_type.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    deployment_ref: first_migrator
                        .map(|m| m.deployment_ref.name.clone())
                        .unwrap_or_else(|| "unknown".to_string()),
                    command: first_migrator
                        .map(|m| m.effective_command())
                        .unwrap_or_default(),
                    require_healthy_cluster: spec.safety.require_healthy_cluster,
                    max_retries: spec.safety.max_retries,
                    migration_timeout_seconds: timeout_seconds,
                }
            },
            status: status.map(MigrationStatusSummary::from).unwrap_or_else(|| {
                MigrationStatusSummary {
                    phase: "Unknown".to_string(),
                    last_migration: None,
                    retry_count: 0,
                    current_job: None,
                    observed_generation: 0,
                }
            }),
            conditions: status
                .and_then(|s| s.conditions.as_ref())
                .map(|conds| conds.iter().map(ConditionInfo::from).collect())
                .unwrap_or_default(),
        }
    }
}

impl From<&DatabaseMigrationStatus> for MigrationStatusSummary {
    fn from(status: &DatabaseMigrationStatus) -> Self {
        Self {
            phase: status
                .phase
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "Unknown".to_string()),
            last_migration: status.last_migration.as_ref().map(LastMigrationInfo::from),
            retry_count: status.retry_count.unwrap_or(0),
            current_job: status.current_job.clone(),
            observed_generation: status.observed_generation.unwrap_or(0),
        }
    }
}

impl From<&LastMigration> for LastMigrationInfo {
    fn from(lm: &LastMigration) -> Self {
        let duration_seconds = lm.duration.as_ref().and_then(|d| {
            humantime::parse_duration(d)
                .ok()
                .map(|dur| dur.as_secs_f64())
        });

        Self {
            image_tag: lm.image_tag.clone(),
            success: lm.success,
            duration_seconds,
            completed_at: lm.completed_at,
            error: lm.error.clone(),
        }
    }
}

impl From<&CrdCondition> for ConditionInfo {
    fn from(cond: &CrdCondition) -> Self {
        Self {
            condition_type: cond.condition_type.clone(),
            status: cond.status.clone(),
            reason: cond.reason.clone(),
            message: cond.message.clone(),
            last_transition_time: cond.last_transition_time,
        }
    }
}
