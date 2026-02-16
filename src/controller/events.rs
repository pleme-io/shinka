//! Kubernetes Event recorder for DatabaseMigration resources
//!
//! Emits native Kubernetes Events for key migration lifecycle moments.
//! Events are visible via `kubectl describe databasemigration <name>`.
//!
//! ## Structured Event Format
//!
//! Events include structured metadata in the message for parseability:
//! ```text
//! Human-readable message [key1=value1 key2=value2]
//! ```
//!
//! This allows log aggregators to extract metrics and build dashboards.

use kube::{
    runtime::events::{Event, EventType, Recorder, Reporter},
    Client, Resource,
};

use crate::crd::DatabaseMigration;

/// Kubernetes Event recorder for Shinka operator
#[derive(Clone)]
pub struct EventRecorder {
    client: Client,
    reporter: Reporter,
}

/// Event reasons emitted by Shinka
pub mod reasons {
    /// Migration job has been created
    pub const MIGRATION_STARTED: &str = "MigrationStarted";
    /// Migration completed successfully
    pub const MIGRATION_SUCCEEDED: &str = "MigrationSucceeded";
    /// Migration job failed
    pub const MIGRATION_FAILED: &str = "MigrationFailed";
    /// Auto-retrying migration after image change
    pub const MIGRATION_RETRYING: &str = "MigrationRetrying";
    /// Database health check passed
    pub const DATABASE_HEALTHY: &str = "DatabaseHealthy";
    /// Database health check failed
    pub const DATABASE_UNHEALTHY: &str = "DatabaseUnhealthy";
    /// Checksum mismatch detected
    pub const CHECKSUM_MISMATCH: &str = "ChecksumMismatch";
    /// Pre-flight validation failed
    pub const PREFLIGHT_FAILED: &str = "PreFlightFailed";
    /// Checksum was auto-reconciled
    pub const CHECKSUM_RECONCILED: &str = "ChecksumReconciled";
    /// Migration phase changed
    pub const PHASE_CHANGED: &str = "PhaseChanged";
    /// Manual retry annotation detected
    pub const MANUAL_RETRY: &str = "ManualRetry";
    /// Waiting for database to become healthy
    pub const WAITING_FOR_DATABASE: &str = "WaitingForDatabase";
    /// Schema validation performed
    pub const SCHEMA_VALIDATED: &str = "SchemaValidated";
    /// Job timeout reached
    pub const JOB_TIMEOUT: &str = "JobTimeout";
    /// Maximum retries exhausted
    pub const RETRIES_EXHAUSTED: &str = "RetriesExhausted";
}

/// Structured labels for events (key-value pairs appended to messages)
#[derive(Default)]
pub struct EventLabels {
    pairs: Vec<(String, String)>,
}

impl EventLabels {
    /// Create new empty labels
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a label
    pub fn add(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.pairs.push((key.into(), value.into()));
        self
    }

    /// Add optional label (only if Some)
    pub fn add_opt(mut self, key: impl Into<String>, value: Option<impl Into<String>>) -> Self {
        if let Some(v) = value {
            self.pairs.push((key.into(), v.into()));
        }
        self
    }

    /// Format labels as a bracketed string
    fn format(&self) -> String {
        if self.pairs.is_empty() {
            return String::new();
        }
        let pairs: Vec<String> = self
            .pairs
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        format!(" [{}]", pairs.join(" "))
    }
}

/// Helper to extract common labels from a DatabaseMigration
fn common_labels(migration: &DatabaseMigration) -> EventLabels {
    let namespace = migration.metadata.namespace.as_deref().unwrap_or("default");
    let name = migration.metadata.name.as_deref().unwrap_or("unknown");
    // Get the current migrator type (or first one if none running)
    let migrator_type = migration
        .current_migrator()
        .map(|m| m.migrator_type.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let cluster = &migration.spec.database.cnpg_cluster_ref.name;
    let database = migration
        .spec
        .database
        .cnpg_cluster_ref
        .database
        .as_deref()
        .unwrap_or("default");

    EventLabels::new()
        .add("namespace", namespace)
        .add("migration", name)
        .add("migrator", migrator_type)
        .add("cluster", cluster)
        .add("database", database)
}

impl EventRecorder {
    /// Create a new EventRecorder
    pub fn new(client: Client) -> Self {
        let reporter = Reporter {
            controller: "shinka".into(),
            instance: std::env::var("POD_NAME").ok(),
        };
        Self { client, reporter }
    }

    /// Emit a Normal event
    pub async fn normal(&self, migration: &DatabaseMigration, reason: &str, message: &str) {
        self.emit(migration, EventType::Normal, reason, message)
            .await;
    }

    /// Emit a Warning event
    pub async fn warning(&self, migration: &DatabaseMigration, reason: &str, message: &str) {
        self.emit(migration, EventType::Warning, reason, message)
            .await;
    }

    /// Internal emit implementation
    async fn emit(
        &self,
        migration: &DatabaseMigration,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let object_ref = migration.object_ref(&());

        // Create a recorder for this specific object
        let recorder = Recorder::new(self.client.clone(), self.reporter.clone(), object_ref);

        if let Err(e) = recorder
            .publish(Event {
                type_: event_type,
                reason: reason.into(),
                note: Some(message.into()),
                action: reason.into(),
                secondary: None,
            })
            .await
        {
            // Log but don't fail - events are best-effort
            tracing::debug!(
                error = %e,
                reason = %reason,
                "Failed to publish Kubernetes event"
            );
        }
    }

    // Convenience methods for common events with structured labels

    /// Migration job created
    pub async fn migration_started(&self, migration: &DatabaseMigration, image_tag: &str) {
        let labels = common_labels(migration).add("image_tag", image_tag);
        self.normal(
            migration,
            reasons::MIGRATION_STARTED,
            &format!(
                "Starting migration job for image tag {}{}",
                image_tag,
                labels.format()
            ),
        )
        .await;
    }

    /// Migration completed successfully
    pub async fn migration_succeeded(
        &self,
        migration: &DatabaseMigration,
        image_tag: &str,
        duration_secs: u64,
    ) {
        let labels = common_labels(migration)
            .add("image_tag", image_tag)
            .add("duration_secs", duration_secs.to_string())
            .add("status", "success");
        self.normal(
            migration,
            reasons::MIGRATION_SUCCEEDED,
            &format!(
                "Migration completed successfully in {}s for image tag {}{}",
                duration_secs,
                image_tag,
                labels.format()
            ),
        )
        .await;
    }

    /// Migration failed
    pub async fn migration_failed(
        &self,
        migration: &DatabaseMigration,
        error: &str,
        retry_count: u32,
        max_retries: u32,
    ) {
        let labels = common_labels(migration)
            .add("retry_count", retry_count.to_string())
            .add("max_retries", max_retries.to_string())
            .add("status", "failed");
        self.warning(
            migration,
            reasons::MIGRATION_FAILED,
            &format!(
                "Migration failed (retry {}/{}): {}{}",
                retry_count,
                max_retries,
                error,
                labels.format()
            ),
        )
        .await;
    }

    /// Auto-retrying after image change
    pub async fn migration_retrying(
        &self,
        migration: &DatabaseMigration,
        old_tag: Option<&str>,
        new_tag: &str,
    ) {
        let labels = common_labels(migration)
            .add("new_tag", new_tag)
            .add_opt("old_tag", old_tag.map(|s| s.to_string()));
        let message = match old_tag {
            Some(old) => format!(
                "Auto-retrying after image change: {} -> {}{}",
                old,
                new_tag,
                labels.format()
            ),
            None => format!(
                "Auto-retrying migration for image tag {}{}",
                new_tag,
                labels.format()
            ),
        };
        self.normal(migration, reasons::MIGRATION_RETRYING, &message)
            .await;
    }

    /// Database health check passed
    pub async fn database_healthy(&self, migration: &DatabaseMigration, cluster_name: &str) {
        let labels = common_labels(migration).add("health_status", "healthy");
        self.normal(
            migration,
            reasons::DATABASE_HEALTHY,
            &format!(
                "Database cluster {} is healthy{}",
                cluster_name,
                labels.format()
            ),
        )
        .await;
    }

    /// Database health check failed
    pub async fn database_unhealthy(
        &self,
        migration: &DatabaseMigration,
        cluster_name: &str,
        message: &str,
    ) {
        let labels = common_labels(migration).add("health_status", "unhealthy");
        self.warning(
            migration,
            reasons::DATABASE_UNHEALTHY,
            &format!(
                "Database cluster {} is unhealthy: {}{}",
                cluster_name,
                message,
                labels.format()
            ),
        )
        .await;
    }

    /// Waiting for database to become healthy
    pub async fn waiting_for_database(&self, migration: &DatabaseMigration, cluster_name: &str) {
        let labels = common_labels(migration).add("health_status", "waiting");
        self.normal(
            migration,
            reasons::WAITING_FOR_DATABASE,
            &format!(
                "Waiting for database cluster {} to become healthy{}",
                cluster_name,
                labels.format()
            ),
        )
        .await;
    }

    /// Checksum mismatch detected
    pub async fn checksum_mismatch(
        &self,
        migration: &DatabaseMigration,
        version: i64,
        mode: &str,
    ) {
        let labels = common_labels(migration)
            .add("version", version.to_string())
            .add("checksum_mode", mode);
        self.warning(
            migration,
            reasons::CHECKSUM_MISMATCH,
            &format!(
                "Checksum mismatch detected for migration v{}, mode: {}{}",
                version,
                mode,
                labels.format()
            ),
        )
        .await;
    }

    /// Checksum was auto-reconciled
    pub async fn checksum_reconciled(&self, migration: &DatabaseMigration, version: i64) {
        let labels = common_labels(migration).add("version", version.to_string());
        self.normal(
            migration,
            reasons::CHECKSUM_RECONCILED,
            &format!(
                "Checksum auto-reconciled for migration v{}{}",
                version,
                labels.format()
            ),
        )
        .await;
    }

    /// Pre-flight validation failed
    pub async fn preflight_failed(&self, migration: &DatabaseMigration, mismatch_count: usize) {
        let labels = common_labels(migration).add("mismatch_count", mismatch_count.to_string());
        self.warning(
            migration,
            reasons::PREFLIGHT_FAILED,
            &format!(
                "Pre-flight validation failed: {} checksum mismatch(es){}",
                mismatch_count,
                labels.format()
            ),
        )
        .await;
    }

    /// Phase changed
    pub async fn phase_changed(&self, migration: &DatabaseMigration, from: &str, to: &str) {
        let labels = common_labels(migration)
            .add("from_phase", from)
            .add("to_phase", to);
        self.normal(
            migration,
            reasons::PHASE_CHANGED,
            &format!("Phase changed: {} -> {}{}", from, to, labels.format()),
        )
        .await;
    }

    /// Manual retry annotation detected
    pub async fn manual_retry(&self, migration: &DatabaseMigration) {
        let labels = common_labels(migration).add("trigger", "annotation");
        self.normal(
            migration,
            reasons::MANUAL_RETRY,
            &format!("Manual retry triggered via annotation{}", labels.format()),
        )
        .await;
    }

    /// Schema validation performed
    pub async fn schema_validated(
        &self,
        migration: &DatabaseMigration,
        safe_to_retry: bool,
        existing_tables: usize,
    ) {
        let labels = common_labels(migration)
            .add("safe_to_retry", safe_to_retry.to_string())
            .add("existing_tables", existing_tables.to_string());
        if safe_to_retry {
            self.normal(
                migration,
                reasons::SCHEMA_VALIDATED,
                &format!(
                    "Schema validation passed: {} existing table(s) found{}",
                    existing_tables,
                    labels.format()
                ),
            )
            .await;
        } else {
            self.warning(
                migration,
                reasons::SCHEMA_VALIDATED,
                &format!(
                    "Schema validation: retry may not be safe, {} existing table(s){}",
                    existing_tables,
                    labels.format()
                ),
            )
            .await;
        }
    }

    /// Job timeout reached
    pub async fn job_timeout(&self, migration: &DatabaseMigration, timeout_secs: u64) {
        let labels = common_labels(migration).add("timeout_secs", timeout_secs.to_string());
        self.warning(
            migration,
            reasons::JOB_TIMEOUT,
            &format!(
                "Migration job timed out after {}s{}",
                timeout_secs,
                labels.format()
            ),
        )
        .await;
    }

    /// Maximum retries exhausted
    pub async fn retries_exhausted(
        &self,
        migration: &DatabaseMigration,
        retry_count: u32,
        last_error: &str,
    ) {
        let labels = common_labels(migration)
            .add("retry_count", retry_count.to_string())
            .add("status", "exhausted");
        self.warning(
            migration,
            reasons::RETRIES_EXHAUSTED,
            &format!(
                "Maximum retries ({}) exhausted. Last error: {}{}",
                retry_count,
                last_error,
                labels.format()
            ),
        )
        .await;
    }
}

/// Optional event recorder (no-op if not configured)
#[derive(Clone)]
pub struct OptionalEventRecorder {
    inner: Option<EventRecorder>,
}

impl OptionalEventRecorder {
    /// Create from client
    pub fn new(client: Client) -> Self {
        Self {
            inner: Some(EventRecorder::new(client)),
        }
    }

    /// Create disabled recorder
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Get inner recorder if available
    pub fn recorder(&self) -> Option<&EventRecorder> {
        self.inner.as_ref()
    }

    // Delegate methods

    pub async fn migration_started(&self, migration: &DatabaseMigration, image_tag: &str) {
        if let Some(r) = &self.inner {
            r.migration_started(migration, image_tag).await;
        }
    }

    pub async fn migration_succeeded(
        &self,
        migration: &DatabaseMigration,
        image_tag: &str,
        duration_secs: u64,
    ) {
        if let Some(r) = &self.inner {
            r.migration_succeeded(migration, image_tag, duration_secs)
                .await;
        }
    }

    pub async fn migration_failed(
        &self,
        migration: &DatabaseMigration,
        error: &str,
        retry_count: u32,
        max_retries: u32,
    ) {
        if let Some(r) = &self.inner {
            r.migration_failed(migration, error, retry_count, max_retries)
                .await;
        }
    }

    pub async fn migration_retrying(
        &self,
        migration: &DatabaseMigration,
        old_tag: Option<&str>,
        new_tag: &str,
    ) {
        if let Some(r) = &self.inner {
            r.migration_retrying(migration, old_tag, new_tag).await;
        }
    }

    pub async fn database_healthy(&self, migration: &DatabaseMigration, cluster_name: &str) {
        if let Some(r) = &self.inner {
            r.database_healthy(migration, cluster_name).await;
        }
    }

    pub async fn database_unhealthy(
        &self,
        migration: &DatabaseMigration,
        cluster_name: &str,
        message: &str,
    ) {
        if let Some(r) = &self.inner {
            r.database_unhealthy(migration, cluster_name, message).await;
        }
    }

    pub async fn waiting_for_database(&self, migration: &DatabaseMigration, cluster_name: &str) {
        if let Some(r) = &self.inner {
            r.waiting_for_database(migration, cluster_name).await;
        }
    }

    pub async fn checksum_mismatch(
        &self,
        migration: &DatabaseMigration,
        version: i64,
        mode: &str,
    ) {
        if let Some(r) = &self.inner {
            r.checksum_mismatch(migration, version, mode).await;
        }
    }

    pub async fn checksum_reconciled(&self, migration: &DatabaseMigration, version: i64) {
        if let Some(r) = &self.inner {
            r.checksum_reconciled(migration, version).await;
        }
    }

    pub async fn preflight_failed(&self, migration: &DatabaseMigration, mismatch_count: usize) {
        if let Some(r) = &self.inner {
            r.preflight_failed(migration, mismatch_count).await;
        }
    }

    pub async fn phase_changed(&self, migration: &DatabaseMigration, from: &str, to: &str) {
        if let Some(r) = &self.inner {
            r.phase_changed(migration, from, to).await;
        }
    }

    pub async fn manual_retry(&self, migration: &DatabaseMigration) {
        if let Some(r) = &self.inner {
            r.manual_retry(migration).await;
        }
    }

    pub async fn schema_validated(
        &self,
        migration: &DatabaseMigration,
        safe_to_retry: bool,
        existing_tables: usize,
    ) {
        if let Some(r) = &self.inner {
            r.schema_validated(migration, safe_to_retry, existing_tables)
                .await;
        }
    }

    pub async fn job_timeout(&self, migration: &DatabaseMigration, timeout_secs: u64) {
        if let Some(r) = &self.inner {
            r.job_timeout(migration, timeout_secs).await;
        }
    }

    pub async fn retries_exhausted(
        &self,
        migration: &DatabaseMigration,
        retry_count: u32,
        last_error: &str,
    ) {
        if let Some(r) = &self.inner {
            r.retries_exhausted(migration, retry_count, last_error)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_labels_format() {
        let labels = EventLabels::new()
            .add("key1", "value1")
            .add("key2", "value2");
        assert_eq!(labels.format(), " [key1=value1 key2=value2]");
    }

    #[test]
    fn test_event_labels_empty() {
        let labels = EventLabels::new();
        assert_eq!(labels.format(), "");
    }

    #[test]
    fn test_event_labels_add_opt_some() {
        let labels = EventLabels::new()
            .add("key1", "value1")
            .add_opt("key2", Some("value2".to_string()));
        assert_eq!(labels.format(), " [key1=value1 key2=value2]");
    }

    #[test]
    fn test_event_labels_add_opt_none() {
        let labels = EventLabels::new()
            .add("key1", "value1")
            .add_opt("key2", None::<String>);
        assert_eq!(labels.format(), " [key1=value1]");
    }
}
