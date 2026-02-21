//! Checksum mismatch detection, reconciliation, and pre-flight validation.

use kube::runtime::controller::Action;
use std::time::Duration;

use crate::{
    crd::{ChecksumMode, DatabaseMigration},
    metrics,
    migrator::{
        self,
        checksum_reconciler::{extract_secret_names, get_database_url_from_secrets},
        ChecksumReconciler, PreFlightValidator, VersionMismatchInfo,
    },
    Error,
};

use super::{status, state_migrating, Context};

/// Detect checksum mismatch from job logs
///
/// Parses the pod logs for a migration job to detect VersionMismatch errors.
/// Returns the version number if a mismatch is detected.
pub(super) async fn detect_checksum_mismatch_from_job(
    ctx: &Context,
    namespace: &str,
    job_name: &str,
) -> Option<VersionMismatchInfo> {
    // Try to get job logs
    match migrator::get_job_pod_logs(ctx.client.clone(), namespace, job_name).await {
        Ok(logs) => migrator::detect_version_mismatch(&logs),
        Err(e) => {
            tracing::debug!(
                namespace = %namespace,
                job = %job_name,
                error = %e,
                "Could not get job logs for checksum mismatch detection"
            );
            None
        }
    }
}

/// Handle checksum mismatch based on configured mode
///
/// Returns Ok(Some(action)) if the mismatch was handled and we should return early,
/// Ok(None) if we should continue with normal retry logic,
/// Err if an error occurred.
pub(super) async fn handle_checksum_mismatch(
    migration: &DatabaseMigration,
    ctx: &Context,
    mismatch: &VersionMismatchInfo,
    job_name: &str,
    image_tag: &str,
) -> crate::Result<Option<Action>> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();
    let checksum_mode = &migration.spec.safety.checksum_mode;

    tracing::warn!(
        event = "checksum_mismatch_detected",
        name = %name,
        namespace = %namespace,
        version = mismatch.version,
        checksum_mode = %checksum_mode,
        "Checksum mismatch detected"
    );

    // Emit K8s checksum mismatch event
    ctx.event_recorder
        .checksum_mismatch(migration, mismatch.version, &checksum_mode.to_string())
        .await;

    match checksum_mode {
        ChecksumMode::Strict => {
            // Strict mode: Fail immediately, no auto-reconciliation
            metrics::record_checksum_mismatch(
                &namespace,
                &name,
                mismatch.version,
                "strict_rejected",
            );

            tracing::error!(
                name = %name,
                namespace = %namespace,
                version = mismatch.version,
                event = "checksum_strict_mode_rejection",
                "Migration blocked by strict checksum mode. The migration file was modified after being applied. Create a new migration instead of modifying applied migrations."
            );

            let error_msg = format!(
                "Checksum mismatch for migration {} in strict mode. Migration file was modified after being applied. Create a new corrective migration instead.",
                mismatch.version
            );
            metrics::record_migration_failure(&name, &namespace, "checksum_strict");
            status::update_status_failed(ctx, migration, image_tag, &error_msg).await?;

            Ok(Some(Action::requeue(Duration::from_secs(300))))
        }
        ChecksumMode::AutoReconcile => {
            // Auto-reconcile mode: Attempt to fix the checksum
            let current_migrator = match migration.current_migrator() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        name = %name,
                        namespace = %namespace,
                        "No current migrator for checksum reconciliation"
                    );
                    return Ok(None);
                }
            };

            let reconciler = ChecksumReconciler::new(
                ctx.client.clone(),
                ctx.config.database_connect_timeout,
            );

            match reconciler
                .try_reconcile(&namespace, job_name, current_migrator)
                .await
            {
                Ok(true) => {
                    tracing::info!(
                        event = "checksum_reconciled",
                        name = %name,
                        namespace = %namespace,
                        version = mismatch.version,
                        "Checksum reconciliation successful, retrying migration"
                    );

                    // Emit K8s event for checksum reconciliation
                    ctx.event_recorder
                        .checksum_reconciled(migration, mismatch.version)
                        .await;

                    metrics::record_checksum_mismatch(
                        &namespace,
                        &name,
                        mismatch.version,
                        "reconciled",
                    );
                    metrics::record_checksum_reconciliation(&namespace, &name, mismatch.version);
                    metrics::record_reconciliation("checksum_reconciled");

                    // Reset retry count since this is a reconciliation, not a retry
                    status::reset_retry_count(ctx, migration).await?;
                    state_migrating::start_migration(migration, ctx).await?;

                    Ok(Some(Action::requeue(Duration::from_secs(5))))
                }
                Ok(false) => {
                    tracing::debug!(
                        name = %name,
                        namespace = %namespace,
                        "Checksum reconciliation not needed or not possible"
                    );
                    Ok(None)
                }
                Err(e) => {
                    tracing::warn!(
                        name = %name,
                        namespace = %namespace,
                        error = %e,
                        "Checksum reconciliation failed, proceeding with normal retry logic"
                    );
                    Ok(None)
                }
            }
        }
        ChecksumMode::PreFlight => {
            // Pre-flight mode: This shouldn't happen as pre-flight catches issues earlier
            metrics::record_checksum_mismatch(
                &namespace,
                &name,
                mismatch.version,
                "preflight_missed",
            );

            tracing::error!(
                name = %name,
                namespace = %namespace,
                version = mismatch.version,
                "Checksum mismatch in pre-flight mode (should have been caught earlier)"
            );

            let error_msg = format!(
                "Checksum mismatch for migration {}. Pre-flight validation should have caught this.",
                mismatch.version
            );
            metrics::record_migration_failure(&name, &namespace, "checksum_preflight");
            status::update_status_failed(ctx, migration, image_tag, &error_msg).await?;

            Ok(Some(Action::requeue(Duration::from_secs(300))))
        }
    }
}

/// Run pre-flight validation before starting migration
///
/// Validates all migration checksums against the database to catch modified
/// migrations before they run. Only runs when checksum_mode is PreFlight.
pub(super) async fn run_preflight_validation(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> crate::Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    tracing::info!(
        name = %name,
        namespace = %namespace,
        event = "preflight_validation_starting",
        "Running pre-flight validation before migration"
    );

    // Get the current migrator for env_from configuration
    let current_migrator = match migration.current_migrator() {
        Some(m) => m,
        None => {
            tracing::debug!(
                name = %name,
                namespace = %namespace,
                "No migrator configured, skipping pre-flight validation"
            );
            return Ok(());
        }
    };

    // Get database URL from secrets
    let secret_names = extract_secret_names(&current_migrator.env_from);
    if secret_names.is_empty() {
        tracing::debug!(
            name = %name,
            namespace = %namespace,
            "No secrets configured, skipping pre-flight validation"
        );
        return Ok(());
    }

    let database_url = match get_database_url_from_secrets(
        ctx.client.clone(),
        &namespace,
        &secret_names,
    )
    .await
    {
        Ok(url) => url,
        Err(e) => {
            tracing::warn!(
                name = %name,
                namespace = %namespace,
                error = %e,
                "Could not get database URL for pre-flight validation (continuing with migration)"
            );
            return Ok(());
        }
    };

    let validator = PreFlightValidator::new(database_url, ctx.config.database_connect_timeout);

    match validator.validate().await {
        Ok(result) => {
            // Log warnings
            for warning in &result.warnings {
                tracing::warn!(
                    name = %name,
                    namespace = %namespace,
                    warning = %warning,
                    "Pre-flight validation warning"
                );
            }

            if !result.passed {
                tracing::error!(
                    event = "preflight_validation_failed",
                    name = %name,
                    namespace = %namespace,
                    mismatches = result.mismatches.len(),
                    "Pre-flight validation failed"
                );

                // Emit K8s event for pre-flight failure
                ctx.event_recorder
                    .preflight_failed(migration, result.mismatches.len())
                    .await;

                // Record metrics for each mismatch
                for mismatch in &result.mismatches {
                    metrics::record_checksum_mismatch(
                        &namespace,
                        &name,
                        mismatch.version,
                        "preflight_blocked",
                    );
                }

                // Record rollback/prevention metric
                metrics::record_migration_rollback(
                    &namespace,
                    &name,
                    "preflight_validation_failed",
                );

                return Err(Error::PreFlightValidationFailed {
                    count: result.mismatches.len(),
                    details: result
                        .mismatches
                        .iter()
                        .map(|m| format!("  - Migration {}: {}", m.version, m.description))
                        .collect::<Vec<_>>()
                        .join("\n"),
                });
            }

            tracing::info!(
                name = %name,
                namespace = %namespace,
                migrations_checked = result.migrations_checked,
                event = "preflight_validation_passed",
                "Pre-flight validation passed"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                name = %name,
                namespace = %namespace,
                error = %e,
                "Pre-flight validation error (continuing with migration)"
            );
            Ok(())
        }
    }
}
