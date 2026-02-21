//! Migration notification helpers.
//!
//! Consolidates the three-way notification calls (event_recorder + webhook_client
//! + discord_client) that appear at multiple call sites.

use crate::{
    crd::DatabaseMigration,
    migrator::JobResult,
};

use super::Context;

/// Send success notifications to all configured channels
///
/// Notifies Release Tracker, Discord, and Kubernetes events about successful migration.
pub(super) async fn notify_migration_success(
    migration: &DatabaseMigration,
    ctx: &Context,
    image_tag: &str,
    duration_secs: u64,
) {
    let namespace = migration.namespace_or_default();
    let name = migration.name_or_default();
    let deployment_name = migration
        .current_migrator()
        .map(|m| m.deployment_ref.name.as_str())
        .unwrap_or("unknown");

    // Emit K8s event
    ctx.event_recorder
        .migration_succeeded(migration, image_tag, duration_secs)
        .await;

    // Emit phase change event
    ctx.event_recorder
        .phase_changed(migration, "Migrating", "Ready")
        .await;

    // Notify Release Tracker (fire-and-forget)
    ctx.webhook_client
        .notify_succeeded(&namespace, &name, deployment_name, image_tag, duration_secs)
        .await;

    // Notify Discord (fire-and-forget)
    ctx.discord_client
        .notify_succeeded(&namespace, &name, deployment_name, image_tag, duration_secs)
        .await;
}

/// Send failure notifications to all configured channels
///
/// Notifies Release Tracker, Discord, and Kubernetes events about failed migration.
pub(super) async fn notify_migration_failure(
    migration: &DatabaseMigration,
    ctx: &Context,
    image_tag: &str,
    error_msg: &str,
    retry_count: u32,
    max_retries: u32,
    is_permanent: bool,
) {
    let namespace = migration.namespace_or_default();
    let name = migration.name_or_default();
    let deployment_name = migration
        .current_migrator()
        .map(|m| m.deployment_ref.name.as_str())
        .unwrap_or("unknown");

    // Emit K8s failure event
    ctx.event_recorder
        .migration_failed(migration, error_msg, retry_count, max_retries)
        .await;

    if is_permanent {
        // Emit phase change event only for permanent failures
        ctx.event_recorder
            .phase_changed(migration, "Migrating", "Failed")
            .await;

        // Emit retries exhausted event
        ctx.event_recorder
            .retries_exhausted(migration, retry_count, error_msg)
            .await;

        // Notify Release Tracker about permanent failure
        ctx.webhook_client
            .notify_failed(&namespace, &name, deployment_name, image_tag, error_msg, retry_count)
            .await;

        // Notify Discord about permanent failure
        ctx.discord_client
            .notify_failed(&namespace, &name, deployment_name, image_tag, error_msg, retry_count)
            .await;
    }
}

/// Build a detailed error message from job result
///
/// Combines the job condition error with pod logs error for comprehensive debugging.
pub(super) fn build_detailed_error_message(result: &JobResult) -> String {
    let job_condition_error = result
        .error
        .clone()
        .unwrap_or_else(|| "Unknown error".to_string());

    // Use pod logs error for the detailed message (actual migration failure)
    if let Some(ref pod_error) = result.pod_logs_error {
        format!(
            "{}\n\n**Pod Logs Error:**\n{}",
            job_condition_error, pod_error
        )
    } else {
        job_condition_error
    }
}
