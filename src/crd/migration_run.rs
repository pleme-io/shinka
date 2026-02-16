//! MigrationRun CRD - Tracks individual migration execution attempts
//!
//! Each MigrationRun represents a single execution of a database migration.
//! This provides a historical record for debugging, auditing, and analytics.
//!
//! ## Usage
//!
//! MigrationRun resources are created automatically by the Shinka operator
//! when a migration job is started. They are owned by the parent DatabaseMigration
//! and will be garbage collected when the parent is deleted.
//!
//! ## Example
//!
//! ```yaml
//! apiVersion: shinka.pleme.io/v1alpha1
//! kind: MigrationRun
//! metadata:
//!   name: lilitu-backend-run-abc123
//!   namespace: lilitu-staging
//!   ownerReferences:
//!     - apiVersion: shinka.pleme.io/v1alpha1
//!       kind: DatabaseMigration
//!       name: lilitu-backend
//!       uid: xxx-xxx-xxx
//! spec:
//!   migrationRef:
//!     name: lilitu-backend
//!   imageTag: "sha256:abc123def456"
//!   attempt: 1
//! status:
//!   phase: Succeeded
//!   startedAt: "2026-01-17T10:00:00Z"
//!   completedAt: "2026-01-17T10:00:30Z"
//!   duration: "30s"
//!   jobName: "lilitu-backend-migration-abc12345"
//!   message: "Migration completed successfully"
//! ```

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// MigrationRun tracks a single execution of a database migration
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "shinka.pleme.io",
    version = "v1alpha1",
    kind = "MigrationRun",
    plural = "migrationruns",
    shortname = "mr",
    namespaced,
    status = "MigrationRunStatus",
    printcolumn = r#"{"name":"Migration","type":"string","jsonPath":".spec.migrationRef.name"}"#,
    printcolumn = r#"{"name":"Attempt","type":"integer","jsonPath":".spec.attempt"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Duration","type":"string","jsonPath":".status.duration"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MigrationRunSpec {
    /// Reference to the parent DatabaseMigration
    pub migration_ref: MigrationRef,

    /// Image tag that triggered this migration run
    pub image_tag: String,

    /// Attempt number (1-based)
    pub attempt: u32,

    /// Migrator type used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrator_type: Option<String>,

    /// Command executed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    /// Whether this was an auto-retry after failure
    #[serde(default)]
    pub is_retry: bool,

    /// Previous image tag (for auto-retry cases)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_image_tag: Option<String>,

    /// Trace context for distributed tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
}

/// Reference to a DatabaseMigration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrationRef {
    /// Name of the DatabaseMigration
    pub name: String,
}

/// Status of a MigrationRun
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct MigrationRunStatus {
    /// Current phase of the migration run
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<MigrationRunPhase>,

    /// When the migration job was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// When the migration job completed (success or failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,

    /// Human-readable duration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Duration in seconds (for metrics/sorting)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,

    /// Name of the Kubernetes Job
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,

    /// Human-readable message about the current state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Whether checksum reconciliation was performed
    #[serde(default)]
    pub checksum_reconciled: bool,

    /// Version that was reconciled (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciled_version: Option<i64>,

    /// Database cluster health at the time of migration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_health: Option<DatabaseHealthSnapshot>,
}

/// Phase of a migration run
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum MigrationRunPhase {
    /// Job has been created, waiting for pod to start
    #[default]
    Pending,
    /// Migration job is running
    Running,
    /// Migration completed successfully
    Succeeded,
    /// Migration failed
    Failed,
    /// Migration was cancelled
    Cancelled,
}

impl std::fmt::Display for MigrationRunPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationRunPhase::Pending => write!(f, "Pending"),
            MigrationRunPhase::Running => write!(f, "Running"),
            MigrationRunPhase::Succeeded => write!(f, "Succeeded"),
            MigrationRunPhase::Failed => write!(f, "Failed"),
            MigrationRunPhase::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Snapshot of database health at migration time
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseHealthSnapshot {
    /// CNPG cluster phase
    pub cluster_phase: String,

    /// Whether the cluster was healthy
    pub healthy: bool,

    /// Number of ready replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    /// Primary instance name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_instance: Option<String>,
}

impl MigrationRun {
    /// Get the namespace or default
    pub fn namespace_or_default(&self) -> String {
        self.metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Get the name or default
    pub fn name_or_default(&self) -> String {
        self.metadata
            .name
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Check if the run is complete (succeeded or failed)
    pub fn is_complete(&self) -> bool {
        matches!(
            self.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(MigrationRunPhase::Succeeded) | Some(MigrationRunPhase::Failed) | Some(MigrationRunPhase::Cancelled)
        )
    }

    /// Check if the run succeeded
    pub fn is_success(&self) -> bool {
        matches!(
            self.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(MigrationRunPhase::Succeeded)
        )
    }

    /// Get duration in seconds if available
    pub fn duration_seconds(&self) -> Option<u64> {
        self.status.as_ref().and_then(|s| s.duration_seconds)
    }
}

/// Helper to format duration as human-readable string
pub fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        let mins = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{}m", mins)
        } else {
            format!("{}m {}s", mins, secs)
        }
    } else {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h 0m");
        assert_eq!(format_duration(3720), "1h 2m");
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(format!("{}", MigrationRunPhase::Pending), "Pending");
        assert_eq!(format!("{}", MigrationRunPhase::Succeeded), "Succeeded");
        assert_eq!(format!("{}", MigrationRunPhase::Failed), "Failed");
    }
}
