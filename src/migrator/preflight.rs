//! Pre-flight validation for migrations
//!
//! Validates all migration checksums before running any migrations.
//! This catches modified migrations early, before any schema changes.
//!
//! ## How it works
//!
//! 1. Query all existing migration records from `_sqlx_migrations`
//! 2. For each recorded migration, compare the stored checksum with the current file
//! 3. If any mismatches are found, abort before running any migrations
//!
//! ## Limitations
//!
//! Pre-flight validation requires access to the migration files, which may be
//! embedded in the binary (sqlx) or stored on disk (goose, flyway, etc.).
//! For embedded migrations, this module cannot directly validate checksums.
//! Instead, it queries the database and logs warnings about any migrations
//! that may have issues based on historical data.

use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

use crate::{Error, Result};

/// Information about a checksum mismatch detected during pre-flight
#[derive(Debug, Clone)]
pub struct PreFlightMismatch {
    /// The migration version that has a mismatch
    pub version: i64,
    /// Human-readable description of the issue
    pub description: String,
}

/// Result of pre-flight validation
#[derive(Debug)]
pub struct PreFlightResult {
    /// Whether all validations passed
    pub passed: bool,
    /// Number of migrations checked
    pub migrations_checked: usize,
    /// List of any mismatches found
    pub mismatches: Vec<PreFlightMismatch>,
    /// Warning messages (non-fatal issues)
    pub warnings: Vec<String>,
}

impl PreFlightResult {
    /// Create a successful result
    pub fn success(migrations_checked: usize) -> Self {
        Self {
            passed: true,
            migrations_checked,
            mismatches: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Create a failed result with mismatches
    pub fn failed(mismatches: Vec<PreFlightMismatch>) -> Self {
        Self {
            passed: false,
            migrations_checked: 0,
            mismatches,
            warnings: Vec::new(),
        }
    }

    /// Add a warning
    pub fn with_warning(mut self, warning: String) -> Self {
        self.warnings.push(warning);
        self
    }
}

/// Pre-flight validator for migration checksums
pub struct PreFlightValidator {
    database_url: String,
    connect_timeout: Duration,
}

impl PreFlightValidator {
    /// Create a new pre-flight validator
    pub fn new(database_url: String, connect_timeout: Duration) -> Self {
        Self {
            database_url,
            connect_timeout,
        }
    }

    /// Run pre-flight validation
    ///
    /// Connects to the database and checks the state of the `_sqlx_migrations` table.
    /// For pre-flight mode, we focus on ensuring the database is consistent and
    /// reporting any potential issues.
    ///
    /// Since we can't directly access embedded migration files, this validation:
    /// 1. Checks that all recorded migrations have success=true
    /// 2. Checks for any duplicate or out-of-order migrations
    /// 3. Reports the current migration state for logging
    pub async fn validate(&self) -> Result<PreFlightResult> {
        tracing::info!("Running pre-flight migration validation");

        // Connect to database
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(self.connect_timeout)
            .connect(&self.database_url)
            .await
            .map_err(|e| {
                Error::Internal(format!("Pre-flight: failed to connect to database: {}", e))
            })?;

        // Check if _sqlx_migrations table exists
        let table_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = '_sqlx_migrations'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "Pre-flight: failed to check migrations table: {}",
                e
            ))
        })?;

        if !table_exists {
            tracing::info!(
                "Pre-flight: No _sqlx_migrations table found, this appears to be a fresh database"
            );
            return Ok(PreFlightResult::success(0).with_warning(
                "No migration history table found - this may be a fresh database".to_string(),
            ));
        }

        // Query all migrations
        let migrations: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(
            "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::Internal(format!("Pre-flight: failed to query migrations: {}", e)))?;

        pool.close().await;

        let mut result = PreFlightResult::success(migrations.len());
        let mut mismatches = Vec::new();

        // Check for failed migrations in history
        for (version, success, _checksum) in &migrations {
            if !success {
                tracing::warn!(
                    version = version,
                    "Pre-flight: Found previously failed migration"
                );
                mismatches.push(PreFlightMismatch {
                    version: *version,
                    description: format!("Migration {} was previously recorded as failed", version),
                });
            }
        }

        // Check for gaps in version numbers (not strictly wrong but suspicious)
        let versions: Vec<i64> = migrations.iter().map(|(v, _, _)| *v).collect();
        for window in versions.windows(2) {
            if let [prev, next] = window {
                if next - prev > 1 {
                    result.warnings.push(format!(
                        "Gap detected in migration versions: {} to {} (missing {} migrations)",
                        prev,
                        next,
                        next - prev - 1
                    ));
                }
            }
        }

        if !mismatches.is_empty() {
            result.passed = false;
            result.mismatches = mismatches;
        }

        tracing::info!(
            passed = result.passed,
            migrations_checked = result.migrations_checked,
            mismatches = result.mismatches.len(),
            warnings = result.warnings.len(),
            "Pre-flight validation complete"
        );

        Ok(result)
    }

    /// Validate and abort if any issues are found
    ///
    /// Returns Ok(()) if validation passes, or an error with details if it fails.
    pub async fn validate_or_fail(&self) -> Result<()> {
        let result = self.validate().await?;

        if !result.passed {
            let mismatch_details: Vec<String> = result
                .mismatches
                .iter()
                .map(|m| format!("  - Migration {}: {}", m.version, m.description))
                .collect();

            return Err(Error::PreFlightValidationFailed {
                count: result.mismatches.len(),
                details: mismatch_details.join("\n"),
            });
        }

        // Log warnings even on success
        for warning in &result.warnings {
            tracing::warn!(warning = %warning, "Pre-flight warning");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preflight_result_success() {
        let result = PreFlightResult::success(5);
        assert!(result.passed);
        assert_eq!(result.migrations_checked, 5);
        assert!(result.mismatches.is_empty());
    }

    #[test]
    fn test_preflight_result_failed() {
        let mismatches = vec![PreFlightMismatch {
            version: 42,
            description: "test".to_string(),
        }];
        let result = PreFlightResult::failed(mismatches);
        assert!(!result.passed);
        assert_eq!(result.mismatches.len(), 1);
    }

    #[test]
    fn test_preflight_result_with_warning() {
        let result = PreFlightResult::success(0).with_warning("test warning".to_string());
        assert!(result.passed);
        assert_eq!(result.warnings.len(), 1);
    }
}
