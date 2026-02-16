//! Migrator module for creating and watching migration jobs
//!
//! Supports multiple migration tools including:
//! - SQLx, Refinery, Diesel (Rust)
//! - Goose, golang-migrate, Atlas, Dbmate (Go)
//! - Flyway, Liquibase (Java)
//! - Custom user-defined commands
//!
//! ## Checksum Reconciliation
//!
//! When sqlx migrations fail due to checksum mismatch (VersionMismatch error),
//! the checksum_reconciler module can automatically fix this by:
//! 1. Detecting the error in job logs
//! 2. Removing the conflicting migration record (since migrations are idempotent)
//! 3. Retrying the migration job
//!
//! This ensures Shinka always reaches a convergent state without manual intervention.
//!
//! ## Pre-Flight Validation
//!
//! For strict environments, the preflight module can validate all migration
//! checksums BEFORE running any migrations. This catches modified migrations
//! early, preventing partial migration states.

// Re-export the checksum_reconciler module for direct access to utility functions
pub mod checksum_reconciler;

mod job_builder;
mod job_watcher;
mod preflight;
mod types;

pub use self::checksum_reconciler::{
    detect_version_mismatch, get_job_pod_logs, ChecksumReconciler, SchemaValidationResult,
    VersionMismatchInfo,
};
pub use job_builder::{build_migration_job, MigrationJobConfig, PodScheduling};
pub use job_watcher::{check_job_status, wait_for_job_completion, JobResult, JobStatus};
pub use preflight::{PreFlightMismatch, PreFlightResult, PreFlightValidator};
pub use types::{MigratorConfig, MigratorDefinition, MigratorType, ToolSpecificConfig};
