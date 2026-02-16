//! Custom Resource Definitions for Shinka operator

mod database_migration;
mod migration_run;

pub use database_migration::{
    ChecksumMode, Condition, ConfigMapRef, DatabaseMigration, DatabaseMigrationSpec,
    DatabaseMigrationStatus, DatabaseSpec, DeploymentRef, EnvFromSource, LastMigration,
    MigrationPhase, MigratorResult, MigratorSpec, ResourceRequirements, SafetySpec, SecretRef,
    TimeoutSpec, EXPECTED_TAG_ANNOTATION, RETRY_ANNOTATION,
};

pub use migration_run::{
    DatabaseHealthSnapshot, MigrationRef, MigrationRun, MigrationRunPhase, MigrationRunSpec,
    MigrationRunStatus, format_duration,
};
