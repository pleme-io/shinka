//! Prometheus metrics for Shinka operator
//!
//! Uses OnceLock pattern for lazy metric initialization.
//! All metrics are registered once at startup.

use once_cell::sync::Lazy;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramVec,
    IntCounterVec, IntGaugeVec, TextEncoder,
};

/// Total migrations by status
pub static MIGRATIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_migrations_total",
        "Total number of migrations by status",
        &["name", "namespace", "status"]
    )
    .expect("metric can be created")
});

/// Migration duration in seconds
pub static MIGRATION_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "shinka_migration_duration_seconds",
        "Duration of migrations in seconds",
        &["name", "namespace"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
    )
    .expect("metric can be created")
});

/// Migrations currently in progress
pub static MIGRATIONS_IN_FLIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "shinka_migrations_in_flight",
        "Number of migrations currently in progress",
        &["namespace"]
    )
    .expect("metric can be created")
});

/// Database health status (1=healthy, 0=unhealthy)
pub static DATABASE_HEALTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "shinka_database_health",
        "Health status of CNPG cluster (1=healthy, 0=unhealthy)",
        &["cluster", "namespace"]
    )
    .expect("metric can be created")
});

/// Total reconciliations by result
pub static RECONCILIATIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_reconciliations_total",
        "Total reconciliation attempts by result",
        &["result"]
    )
    .expect("metric can be created")
});

/// Total errors by category
pub static ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_errors_total",
        "Total errors by name, namespace, and category",
        &["name", "namespace", "category"]
    )
    .expect("metric can be created")
});

/// Retry attempts by migration
pub static RETRY_ATTEMPTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_retry_attempts_total",
        "Total retry attempts by migration",
        &["name", "namespace"]
    )
    .expect("metric can be created")
});

/// Auto-retries triggered by image change (recovery from Failed state)
pub static MIGRATION_AUTO_RETRIES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_migration_auto_retries_total",
        "Total automatic retries triggered by image change in Failed state",
        &["namespace", "name"]
    )
    .expect("metric can be created")
});

/// Current phase of each migration
pub static MIGRATION_PHASE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "shinka_migration_phase",
        "Current phase of migration (0=Pending, 1=CheckingHealth, 2=WaitingForDatabase, 3=Migrating, 4=Ready, 5=Failed)",
        &["name", "namespace"]
    )
    .expect("metric can be created")
});

// =============================================================================
// Checksum Safety Metrics
// =============================================================================

/// Total checksum reconciliations performed
///
/// Tracks when Shinka automatically fixes checksum mismatches by removing
/// the migration record and allowing re-application (since migrations are idempotent).
pub static CHECKSUM_RECONCILIATIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_checksum_reconciliations_total",
        "Total checksum reconciliations by migration",
        &["namespace", "name", "version"]
    )
    .expect("metric can be created")
});

/// Total checksum mismatches detected
///
/// Tracks when a migration file's checksum doesn't match what was recorded
/// in the database. Outcome indicates how it was handled:
/// - "reconciled": Auto-fixed by removing and re-applying
/// - "strict_rejected": Blocked in strict mode
/// - "preflight_failed": Caught during pre-flight validation
pub static CHECKSUM_MISMATCHES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_checksum_mismatches_total",
        "Total checksum mismatches detected",
        &["namespace", "name", "version", "outcome"]
    )
    .expect("metric can be created")
});

/// Migration transaction rollbacks
///
/// Tracks when a migration transaction was rolled back due to errors.
/// Reason indicates the cause:
/// - "sql_error": SQL execution failed
/// - "timeout": Migration exceeded timeout
/// - "cancelled": Migration was cancelled
/// - "connection_lost": Database connection lost
pub static MIGRATION_ROLLBACKS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "shinka_migration_rollbacks_total",
        "Total migration transaction rollbacks",
        &["namespace", "name", "reason"]
    )
    .expect("metric can be created")
});

/// Current checksum mode per migration
///
/// Tracks the configured checksum mode:
/// - 0: auto-reconcile (default)
/// - 1: strict
/// - 2: pre-flight
pub static CHECKSUM_MODE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "shinka_checksum_mode",
        "Configured checksum mode (0=auto-reconcile, 1=strict, 2=pre-flight)",
        &["namespace", "name"]
    )
    .expect("metric can be created")
});

// =============================================================================
// Leader Election Metrics
// =============================================================================

/// Leader election status (1=leader, 0=follower)
pub static LEADER_STATUS: Lazy<prometheus::IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "shinka_leader_status",
        "Whether this instance is the leader (1=leader, 0=follower)"
    )
    .expect("metric can be created")
});

/// Initialize all metrics (force lazy initialization)
pub fn init() {
    // Force initialization by accessing each metric
    let _ = &*MIGRATIONS_TOTAL;
    let _ = &*MIGRATION_DURATION_SECONDS;
    let _ = &*MIGRATIONS_IN_FLIGHT;
    let _ = &*DATABASE_HEALTH;
    let _ = &*RECONCILIATIONS_TOTAL;
    let _ = &*ERRORS_TOTAL;
    let _ = &*RETRY_ATTEMPTS;
    let _ = &*MIGRATION_AUTO_RETRIES_TOTAL;
    let _ = &*MIGRATION_PHASE;
    // Checksum safety metrics
    let _ = &*CHECKSUM_RECONCILIATIONS_TOTAL;
    let _ = &*CHECKSUM_MISMATCHES_TOTAL;
    let _ = &*MIGRATION_ROLLBACKS_TOTAL;
    let _ = &*CHECKSUM_MODE;
    // Leader election metrics
    let _ = &*LEADER_STATUS;

    tracing::info!("Shinka metrics initialized");
}

/// Set leader election status
pub fn set_leader_status(is_leader: bool) {
    LEADER_STATUS.set(if is_leader { 1 } else { 0 });
}

/// Gather all metrics as Prometheus text format
pub fn gather() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder
        .encode_to_string(&metric_families)
        .unwrap_or_else(|e| format!("# Error encoding metrics: {e}"))
}

/// Record a successful migration
pub fn record_migration_success(name: &str, namespace: &str, duration_secs: f64) {
    MIGRATIONS_TOTAL
        .with_label_values(&[name, namespace, "success"])
        .inc();
    MIGRATION_DURATION_SECONDS
        .with_label_values(&[name, namespace])
        .observe(duration_secs);
    MIGRATIONS_IN_FLIGHT.with_label_values(&[namespace]).dec();
    set_migration_phase(name, namespace, "Ready");
}

/// Record a failed migration
pub fn record_migration_failure(name: &str, namespace: &str, category: &str) {
    MIGRATIONS_TOTAL
        .with_label_values(&[name, namespace, "failure"])
        .inc();
    ERRORS_TOTAL
        .with_label_values(&[name, namespace, category])
        .inc();
    MIGRATIONS_IN_FLIGHT.with_label_values(&[namespace]).dec();
}

/// Record migration start
pub fn record_migration_start(name: &str, namespace: &str) {
    MIGRATIONS_IN_FLIGHT.with_label_values(&[namespace]).inc();
    set_migration_phase(name, namespace, "Migrating");
}

/// Record a retry attempt
pub fn record_retry(name: &str, namespace: &str) {
    RETRY_ATTEMPTS.with_label_values(&[name, namespace]).inc();
}

/// Record an auto-retry triggered by image change in Failed state
pub fn record_auto_retry(name: &str, namespace: &str) {
    MIGRATION_AUTO_RETRIES_TOTAL
        .with_label_values(&[namespace, name])
        .inc();
}

/// Record database health
pub fn record_database_health(cluster: &str, namespace: &str, healthy: bool) {
    DATABASE_HEALTH
        .with_label_values(&[cluster, namespace])
        .set(if healthy { 1 } else { 0 });
}

/// Record reconciliation result
pub fn record_reconciliation(result: &str) {
    RECONCILIATIONS_TOTAL.with_label_values(&[result]).inc();
}

/// Set current migration phase
pub fn set_migration_phase(name: &str, namespace: &str, phase: &str) {
    let phase_value = match phase {
        "Pending" => 0,
        "CheckingHealth" => 1,
        "WaitingForDatabase" => 2,
        "Migrating" => 3,
        "Ready" => 4,
        "Failed" => 5,
        _ => -1,
    };
    MIGRATION_PHASE
        .with_label_values(&[name, namespace])
        .set(phase_value);
}

// =============================================================================
// Checksum Safety Recording Functions
// =============================================================================

/// Record a checksum reconciliation event
///
/// Called when Shinka automatically fixes a checksum mismatch by removing
/// the migration record and allowing re-application.
pub fn record_checksum_reconciliation(namespace: &str, name: &str, version: i64) {
    CHECKSUM_RECONCILIATIONS_TOTAL
        .with_label_values(&[namespace, name, &version.to_string()])
        .inc();

    tracing::info!(
        event = "checksum_reconciled",
        namespace = %namespace,
        migration = %name,
        version = %version,
        "Checksum reconciliation recorded"
    );
}

/// Record a checksum mismatch detection
///
/// Called when a migration file's checksum doesn't match the database record.
/// Outcome should be one of: "reconciled", "strict_rejected", "preflight_failed"
pub fn record_checksum_mismatch(namespace: &str, name: &str, version: i64, outcome: &str) {
    CHECKSUM_MISMATCHES_TOTAL
        .with_label_values(&[namespace, name, &version.to_string(), outcome])
        .inc();

    tracing::warn!(
        event = "checksum_mismatch_detected",
        namespace = %namespace,
        migration = %name,
        version = %version,
        outcome = %outcome,
        "Checksum mismatch detected"
    );
}

/// Record a migration rollback
///
/// Called when a migration transaction is rolled back due to an error.
/// Reason should be one of: "sql_error", "timeout", "cancelled", "connection_lost"
pub fn record_migration_rollback(namespace: &str, name: &str, reason: &str) {
    MIGRATION_ROLLBACKS_TOTAL
        .with_label_values(&[namespace, name, reason])
        .inc();

    tracing::warn!(
        event = "migration_rollback",
        namespace = %namespace,
        migration = %name,
        reason = %reason,
        "Migration transaction rolled back"
    );
}

/// Set the checksum mode for a migration
///
/// Mode values:
/// - 0: auto-reconcile (default)
/// - 1: strict
/// - 2: pre-flight
pub fn set_checksum_mode(namespace: &str, name: &str, mode: &str) {
    let mode_value = match mode {
        "auto-reconcile" | "AutoReconcile" => 0,
        "strict" | "Strict" => 1,
        "pre-flight" | "PreFlight" => 2,
        _ => 0,
    };
    CHECKSUM_MODE
        .with_label_values(&[namespace, name])
        .set(mode_value);
}
