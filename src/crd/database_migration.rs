//! DatabaseMigration CRD definition
//!
//! This CRD defines a database migration that runs automatically when
//! the referenced deployment's image changes.
//!
//! Supports multiple migration tools:
//! - Rust: sqlx (default), refinery, diesel, seaorm
//! - Go: goose, golang-migrate, atlas, dbmate
//! - Java: flyway, liquibase
//! - Custom: user-defined commands
//!
//! ## Multiple Migrators
//!
//! You can define multiple migrators that run in sequence. This is useful for
//! transitioning between migration tools (e.g., SQLx to SeaORM) or running
//! migrations from multiple sources.
//!
//! ```yaml
//! spec:
//!   migrators:
//!     - name: sqlx-migrations
//!       type: sqlx
//!       deploymentRef:
//!         name: backend
//!     - name: seaorm-migrations
//!       type: seaorm
//!       deploymentRef:
//!         name: backend
//!   safety:
//!     continueOnFailure: false  # Stop if any migration fails (default)
//! ```

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::migrator::{MigratorType, ToolSpecificConfig};

/// Finalizer name for Shinka-managed resources
#[allow(dead_code)]
pub const FINALIZER: &str = "shinka.pleme.io/finalizer";

/// Annotation key to trigger a manual retry of a failed migration
///
/// When a migration is in the Failed state and this annotation is set to "true",
/// Shinka will reset the migration to Pending and retry. The annotation is
/// automatically removed after triggering the retry.
///
/// Usage:
///   kubectl annotate databasemigration my-migration shinka.pleme.io/retry=true
pub const RETRY_ANNOTATION: &str = "shinka.pleme.io/retry";

/// Annotation key set by nexus-deploy to signal an expected release tag
///
/// When set, Shinka will:
/// 1. Invalidate its deployment image cache immediately
/// 2. Fast-requeue at 1s instead of 60s until the expected tag is detected
/// 3. Clear the annotation once the expected tag is found on the deployment
///
/// This eliminates up to 90s of wasted time (30s image cache + 60s requeue)
/// during a release deployment.
///
/// Usage:
///   kubectl annotate databasemigration my-migration \
///     release.shinka.pleme.io/expected-tag=amd64-abc1234 --overwrite
pub const EXPECTED_TAG_ANNOTATION: &str = "release.shinka.pleme.io/expected-tag";

/// DatabaseMigration is the Schema for the databasemigrations API
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "shinka.pleme.io",
    version = "v1alpha1",
    kind = "DatabaseMigration",
    status = "DatabaseMigrationStatus",
    namespaced
)]
#[kube(printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#)]
#[kube(
    printcolumn = r#"{"name":"Last Image","type":"string","jsonPath":".status.lastMigration.imageTag"}"#
)]
#[kube(
    printcolumn = r#"{"name":"Success","type":"boolean","jsonPath":".status.lastMigration.success"}"#
)]
#[kube(printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#)]
pub struct DatabaseMigrationSpec {
    /// Database configuration
    pub database: DatabaseSpec,

    /// Single migrator configuration (for backward compatibility)
    ///
    /// Use `migrators` field instead for multiple ordered migrations.
    /// If both `migrator` and `migrators` are specified, `migrators` takes precedence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrator: Option<MigratorSpec>,

    /// Ordered list of migrators to run sequentially
    ///
    /// Each migrator runs in order. By default, if any migrator fails,
    /// subsequent migrators are not executed. Set `safety.continueOnFailure`
    /// to `true` to continue despite failures.
    ///
    /// Example:
    /// ```yaml
    /// migrators:
    ///   - name: sqlx-migrations
    ///     type: sqlx
    ///     deploymentRef:
    ///       name: backend
    ///   - name: seaorm-migrations
    ///     type: seaorm
    ///     deploymentRef:
    ///       name: backend
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrators: Option<Vec<MigratorSpec>>,

    /// Safety settings
    #[serde(default)]
    pub safety: SafetySpec,

    /// Timeout settings
    #[serde(default)]
    pub timeouts: TimeoutSpec,
}

/// Database configuration referencing a CNPG cluster
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DatabaseSpec {
    /// Reference to the CNPG cluster
    #[serde(rename = "cnpgClusterRef")]
    pub cnpg_cluster_ref: CnpgClusterRef,
}

/// Reference to a CNPG Cluster resource
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct CnpgClusterRef {
    /// Name of the CNPG Cluster
    pub name: String,

    /// Database name within the cluster
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

/// Migrator configuration - which deployment and tool to use for migrations
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MigratorSpec {
    /// Optional name for this migrator (used for identification in logs and status)
    ///
    /// When using multiple migrators, providing a name helps identify which
    /// migrator is running or failed. If not provided, defaults to the
    /// migrator type name (e.g., "sqlx", "seaorm").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Type of migration tool to use (defaults to sqlx)
    ///
    /// Supported types:
    /// - Rust: sqlx (default), refinery, diesel, seaorm
    /// - Go: goose, golang-migrate, atlas, dbmate
    /// - Java: flyway, liquibase
    /// - custom: user-defined command
    #[serde(default, rename = "type")]
    pub migrator_type: MigratorType,

    /// Reference to the deployment whose image will run migrations
    #[serde(rename = "deploymentRef")]
    pub deployment_ref: DeploymentRef,

    /// Override the container image directly, bypassing deploymentRef lookup.
    ///
    /// When set, Shinka uses this image instead of resolving it from the
    /// referenced deployment. Useful for ephemeral environments where the
    /// deployment doesn't exist yet (e.g., Kenshi ephemeral test environments
    /// run migrations before deploying the application).
    #[serde(skip_serializing_if = "Option::is_none", rename = "imageOverride")]
    pub image_override: Option<String>,

    /// Command to run for migration (e.g., ["./backend", "--migrate"])
    /// If not specified, uses the default command for the migrator type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    /// Optional arguments to append to the command
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    /// Working directory for migration execution
    #[serde(skip_serializing_if = "Option::is_none", rename = "workingDir")]
    pub working_dir: Option<String>,

    /// Path to migrations directory (relative to workingDir or absolute)
    #[serde(skip_serializing_if = "Option::is_none", rename = "migrationsPath")]
    pub migrations_path: Option<String>,

    /// Additional environment variables for the migration job
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,

    /// Tool-specific configuration options
    #[serde(skip_serializing_if = "Option::is_none", rename = "toolConfig")]
    pub tool_config: Option<ToolSpecificConfig>,

    /// Secrets to mount for database credentials
    #[serde(skip_serializing_if = "Option::is_none", rename = "secretRefs")]
    pub secret_refs: Option<Vec<SecretRef>>,

    /// Environment variables from ConfigMaps/Secrets
    #[serde(skip_serializing_if = "Option::is_none", rename = "envFrom")]
    pub env_from: Option<Vec<EnvFromSource>>,

    /// Resource requirements for migration jobs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Service account to use for migration jobs
    #[serde(skip_serializing_if = "Option::is_none", rename = "serviceAccountName")]
    pub service_account_name: Option<String>,
}

/// Reference to a Deployment
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeploymentRef {
    /// Name of the deployment
    pub name: String,

    /// Container name to get the image from (defaults to first container)
    #[serde(skip_serializing_if = "Option::is_none", rename = "containerName")]
    pub container_name: Option<String>,
}

/// Reference to a Secret
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SecretRef {
    /// Name of the secret
    pub name: String,
}

/// Source for environment variables
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EnvFromSource {
    /// Reference to a ConfigMap
    #[serde(skip_serializing_if = "Option::is_none", rename = "configMapRef")]
    pub config_map_ref: Option<ConfigMapRef>,

    /// Reference to a Secret
    #[serde(skip_serializing_if = "Option::is_none", rename = "secretRef")]
    pub secret_ref: Option<SecretRef>,
}

/// Reference to a ConfigMap
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ConfigMapRef {
    /// Name of the configmap
    pub name: String,
}

/// Resource requirements for migration jobs
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct ResourceRequirements {
    /// Memory limit (e.g., "256Mi")
    #[serde(skip_serializing_if = "Option::is_none", rename = "memoryLimit")]
    pub memory_limit: Option<String>,

    /// CPU limit (e.g., "500m")
    #[serde(skip_serializing_if = "Option::is_none", rename = "cpuLimit")]
    pub cpu_limit: Option<String>,

    /// Memory request (e.g., "128Mi")
    #[serde(skip_serializing_if = "Option::is_none", rename = "memoryRequest")]
    pub memory_request: Option<String>,

    /// CPU request (e.g., "100m")
    #[serde(skip_serializing_if = "Option::is_none", rename = "cpuRequest")]
    pub cpu_request: Option<String>,
}

/// How to handle checksum mismatches when a migration file has been modified
/// after being applied to the database.
///
/// Since migrations should be idempotent, checksum mismatches typically indicate
/// that a migration file was edited after it was already applied. The best practice
/// is to NEVER modify applied migrations - always create new migrations instead.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ChecksumMode {
    /// Strict mode: Fail immediately on any checksum mismatch.
    /// No auto-reconciliation - the migration will stay in Failed state until
    /// the issue is resolved manually (either revert the file change or create
    /// a new corrective migration).
    ///
    /// Recommended for production environments where migration file integrity
    /// is critical and modifications should be caught and reviewed.
    Strict,

    /// Auto-reconcile mode (default): Automatically fix checksum mismatches
    /// by removing the migration record and allowing re-application.
    ///
    /// This is safe because:
    /// 1. Migrations should be idempotent (use IF NOT EXISTS, ON CONFLICT, etc.)
    /// 2. The schema is already in the correct state from the original application
    /// 3. Re-running the migration will succeed and record the new checksum
    ///
    /// Metrics and logs are emitted for observability.
    #[default]
    AutoReconcile,

    /// Pre-flight mode: Validate ALL migration checksums before running ANY migrations.
    /// If any checksum mismatch is detected, the entire migration is aborted before
    /// any schema changes are made.
    ///
    /// This catches issues early in CI/CD pipelines and prevents partial application
    /// of migrations when one has been modified.
    PreFlight,
}

impl std::fmt::Display for ChecksumMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksumMode::Strict => write!(f, "strict"),
            ChecksumMode::AutoReconcile => write!(f, "auto-reconcile"),
            ChecksumMode::PreFlight => write!(f, "pre-flight"),
        }
    }
}

/// Safety settings for migration execution
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SafetySpec {
    /// Require CNPG cluster to be healthy before migration
    #[serde(default = "default_true", rename = "requireHealthyCluster")]
    pub require_healthy_cluster: bool,

    /// Maximum number of retry attempts for failed migrations
    #[serde(default = "default_max_retries", rename = "maxRetries")]
    pub max_retries: u32,

    /// How to handle checksum mismatches when migration files have been modified
    /// after being applied.
    ///
    /// Options:
    /// - `strict`: Fail on any mismatch, no auto-reconciliation (recommended for production)
    /// - `auto-reconcile`: Automatically fix mismatches (default, safe for idempotent migrations)
    /// - `pre-flight`: Validate all checksums before running any migrations
    #[serde(default, rename = "checksumMode")]
    pub checksum_mode: ChecksumMode,

    /// Continue running subsequent migrators even if one fails
    ///
    /// When using multiple migrators (`spec.migrators`), this controls what
    /// happens when a migrator fails:
    /// - `false` (default): Stop immediately on the first failure. The overall
    ///   migration is marked as Failed and subsequent migrators are not run.
    /// - `true`: Continue running remaining migrators. The overall migration is
    ///   marked as Failed if any migrator failed, but all migrators will execute.
    ///
    /// Recommended to keep as `false` for production to ensure data consistency.
    #[serde(default, rename = "continueOnFailure")]
    pub continue_on_failure: bool,
}

impl Default for SafetySpec {
    fn default() -> Self {
        Self {
            require_healthy_cluster: true,
            max_retries: 3,
            checksum_mode: ChecksumMode::AutoReconcile,
            continue_on_failure: false,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_max_retries() -> u32 {
    3
}

/// Timeout settings for various operations
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TimeoutSpec {
    /// Migration job timeout (e.g., "5m")
    #[serde(default = "default_migration_timeout")]
    pub migration: String,
}

impl Default for TimeoutSpec {
    fn default() -> Self {
        Self {
            migration: "5m".to_string(),
        }
    }
}

fn default_migration_timeout() -> String {
    "5m".to_string()
}

// =============================================================================
// Status Types
// =============================================================================

/// Status of the DatabaseMigration
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct DatabaseMigrationStatus {
    /// Current phase of the migration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<MigrationPhase>,

    /// Information about the last migration run
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastMigration")]
    pub last_migration: Option<LastMigration>,

    /// Current retry count
    #[serde(skip_serializing_if = "Option::is_none", rename = "retryCount")]
    pub retry_count: Option<u32>,

    /// Name of the currently running or last migration job
    #[serde(skip_serializing_if = "Option::is_none", rename = "currentJob")]
    pub current_job: Option<String>,

    /// When the current job was started (for timeout tracking)
    #[serde(skip_serializing_if = "Option::is_none", rename = "jobStartedAt")]
    pub job_started_at: Option<DateTime<Utc>>,

    /// Conditions represent the latest available observations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,

    /// Observed generation (for detecting spec changes)
    #[serde(skip_serializing_if = "Option::is_none", rename = "observedGeneration")]
    pub observed_generation: Option<i64>,

    // ==========================================================================
    // Multi-Migrator Status Fields
    // ==========================================================================
    /// Index of the currently running migrator (0-based)
    ///
    /// When using multiple migrators, this tracks which one is currently
    /// executing. Starts at 0 for the first migrator.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "currentMigratorIndex"
    )]
    pub current_migrator_index: Option<u32>,

    /// Name of the currently running migrator
    ///
    /// Either the user-provided name or the migrator type name.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "currentMigratorName"
    )]
    pub current_migrator_name: Option<String>,

    /// Total number of migrators to run
    #[serde(skip_serializing_if = "Option::is_none", rename = "totalMigrators")]
    pub total_migrators: Option<u32>,

    /// Results of each migrator execution
    ///
    /// Contains the outcome of each migrator that has completed (or failed).
    /// Useful for debugging and understanding which migrator caused issues.
    #[serde(skip_serializing_if = "Option::is_none", rename = "migratorResults")]
    pub migrator_results: Option<Vec<MigratorResult>>,
}

/// Result of a single migrator execution
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigratorResult {
    /// Index of this migrator (0-based)
    pub index: u32,

    /// Name of the migrator
    pub name: String,

    /// Type of migrator used
    pub migrator_type: String,

    /// Whether this migrator succeeded
    pub success: bool,

    /// Duration of this migrator's execution
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// When this migrator completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if this migrator failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Job name for this migrator
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
}

/// Phase of the migration lifecycle
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Waiting for initial reconciliation
    Pending,
    /// Checking CNPG cluster health
    CheckingHealth,
    /// Waiting for database to become healthy
    WaitingForDatabase,
    /// Running migration job
    Migrating,
    /// Migration completed successfully
    Ready,
    /// Migration failed (retries exhausted or permanent error)
    Failed,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationPhase::Pending => write!(f, "Pending"),
            MigrationPhase::CheckingHealth => write!(f, "CheckingHealth"),
            MigrationPhase::WaitingForDatabase => write!(f, "WaitingForDatabase"),
            MigrationPhase::Migrating => write!(f, "Migrating"),
            MigrationPhase::Ready => write!(f, "Ready"),
            MigrationPhase::Failed => write!(f, "Failed"),
        }
    }
}

/// Information about the last migration run
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LastMigration {
    /// Image tag that was migrated
    #[serde(rename = "imageTag")]
    pub image_tag: String,

    /// Whether the migration succeeded
    pub success: bool,

    /// Duration of the migration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// When the migration completed
    #[serde(skip_serializing_if = "Option::is_none", rename = "completedAt")]
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if migration failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Condition represents an observation of an object's state
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Condition {
    /// Type of condition (e.g., "Ready", "DatabaseHealthy", "MigrationSucceeded")
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition ("True", "False", "Unknown")
    pub status: String,

    /// Observed generation when condition was set
    #[serde(skip_serializing_if = "Option::is_none", rename = "observedGeneration")]
    pub observed_generation: Option<i64>,

    /// Machine-readable reason for the condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Time when the condition last transitioned
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastTransitionTime")]
    pub last_transition_time: Option<DateTime<Utc>>,
}

impl Condition {
    /// Create a new Ready=True condition
    pub fn ready(generation: i64, reason: &str, message: &str) -> Self {
        Self {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            observed_generation: Some(generation),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
            last_transition_time: Some(Utc::now()),
        }
    }

    /// Create a new Ready=False condition
    pub fn not_ready(generation: i64, reason: &str, message: &str) -> Self {
        Self {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            observed_generation: Some(generation),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
            last_transition_time: Some(Utc::now()),
        }
    }
}

// =============================================================================
// Helper Methods
// =============================================================================

impl DatabaseMigration {
    /// Get the namespace, defaulting to "default" if not set
    pub fn namespace_or_default(&self) -> String {
        self.metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Get the name
    pub fn name_or_default(&self) -> String {
        self.metadata
            .name
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Check if we need to run a migration for the given image tag
    pub fn needs_migration(&self, current_image_tag: &str) -> bool {
        match &self.status {
            Some(status) => match &status.last_migration {
                Some(last) => last.image_tag != current_image_tag || !last.success,
                None => true,
            },
            None => true,
        }
    }

    /// Get the current retry count
    pub fn retry_count(&self) -> u32 {
        self.status
            .as_ref()
            .and_then(|s| s.retry_count)
            .unwrap_or(0)
    }

    /// Check if retries are exhausted
    pub fn retries_exhausted(&self) -> bool {
        self.retry_count() >= self.spec.safety.max_retries
    }

    /// Get the last migration image tag (if any)
    pub fn last_image_tag(&self) -> Option<String> {
        self.status
            .as_ref()
            .and_then(|s| s.last_migration.as_ref())
            .map(|m| m.image_tag.clone())
    }

    /// Check if the retry annotation is set to "true"
    ///
    /// Used to trigger a manual retry of a failed migration.
    pub fn has_retry_annotation(&self) -> bool {
        self.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(RETRY_ANNOTATION))
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Get the expected release tag from the annotation, if set
    ///
    /// Returns `Some("amd64-abc1234")` if nexus-deploy set the expected-tag annotation.
    pub fn expected_release_tag(&self) -> Option<&str> {
        self.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(EXPECTED_TAG_ANNOTATION))
            .map(|v| v.as_str())
            .filter(|v| !v.is_empty())
    }

    // =========================================================================
    // Multi-Migrator Helper Methods
    // =========================================================================

    /// Get the ordered list of migrators to execute
    ///
    /// Returns migrators from the `migrators` field if present, otherwise
    /// wraps the single `migrator` field in a vec for backward compatibility.
    pub fn get_migrators(&self) -> Vec<&MigratorSpec> {
        if let Some(ref migrators) = self.spec.migrators {
            migrators.iter().collect()
        } else if let Some(ref migrator) = self.spec.migrator {
            vec![migrator]
        } else {
            vec![]
        }
    }

    /// Get the number of migrators configured
    pub fn migrator_count(&self) -> usize {
        self.get_migrators().len()
    }

    /// Check if this migration uses multiple migrators
    pub fn is_multi_migrator(&self) -> bool {
        self.spec
            .migrators
            .as_ref()
            .map(|m| m.len() > 1)
            .unwrap_or(false)
    }

    /// Get the current migrator index (0 if not set or single migrator)
    pub fn current_migrator_index(&self) -> u32 {
        self.status
            .as_ref()
            .and_then(|s| s.current_migrator_index)
            .unwrap_or(0)
    }

    /// Get the migrator at a specific index
    pub fn get_migrator_at(&self, index: usize) -> Option<&MigratorSpec> {
        self.get_migrators().get(index).copied()
    }

    /// Get the current migrator based on status
    pub fn current_migrator(&self) -> Option<&MigratorSpec> {
        let index = self.current_migrator_index() as usize;
        self.get_migrator_at(index)
    }

    /// Check if all migrators have completed (for multi-migrator scenarios)
    pub fn all_migrators_complete(&self) -> bool {
        let total = self.migrator_count();
        if total == 0 {
            return true;
        }

        let completed = self
            .status
            .as_ref()
            .and_then(|s| s.migrator_results.as_ref())
            .map(|r| r.len())
            .unwrap_or(0);

        completed >= total
    }

    /// Get completed migrator results
    pub fn migrator_results(&self) -> Vec<&MigratorResult> {
        self.status
            .as_ref()
            .and_then(|s| s.migrator_results.as_ref())
            .map(|r| r.iter().collect())
            .unwrap_or_default()
    }

    /// Check if any migrator has failed
    pub fn any_migrator_failed(&self) -> bool {
        self.migrator_results().iter().any(|r| !r.success)
    }

    /// Get the first migrator deployment ref (for backward compatibility)
    ///
    /// Used when we need to determine the deployment image for migration checks.
    /// Returns the deployment ref from the first migrator in the list.
    pub fn primary_deployment_ref(&self) -> Option<&DeploymentRef> {
        self.get_migrators().first().map(|m| &m.deployment_ref)
    }
}

impl MigratorSpec {
    /// Check if this migrator has an image override set
    pub fn has_image_override(&self) -> bool {
        self.image_override.is_some()
    }

    /// Get the display name for this migrator
    ///
    /// Returns the user-provided name if set, otherwise returns the migrator
    /// type name (e.g., "sqlx", "seaorm").
    pub fn display_name(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| self.migrator_type.to_string())
    }

    /// Convert to a MigratorDefinition for command building
    pub fn to_definition(&self) -> crate::migrator::MigratorDefinition {
        crate::migrator::MigratorDefinition {
            migrator_type: self.migrator_type.clone(),
            config: crate::migrator::MigratorConfig {
                command: self.command.clone(),
                args: self.args.clone(),
                working_dir: self.working_dir.clone(),
                migrations_path: self.migrations_path.clone(),
                env: self.env.clone(),
                tool_config: self.tool_config.clone(),
            },
        }
    }

    /// Get the effective command for this migrator spec
    pub fn effective_command(&self) -> Vec<String> {
        self.to_definition().effective_command()
    }

    /// Build the complete command with database URL
    pub fn build_command(&self, database_url: &str) -> Vec<String> {
        self.to_definition().build_command(database_url)
    }

    /// Get the database environment variable name for this migrator type
    pub fn database_env_var(&self) -> &'static str {
        self.migrator_type.database_env_var()
    }
}
