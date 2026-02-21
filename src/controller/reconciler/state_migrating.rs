//! Migrating phase handler — monitors running migration jobs.

use chrono::Utc;
use k8s_openapi::api::batch::v1::Job;
use kube::{
    api::{Api, Patch, PatchParams, PostParams},
    runtime::controller::Action,
};
use serde_json::json;
use std::time::Duration;

use crate::{
    crd::{
        ChecksumMode, Condition, DatabaseMigration, DatabaseMigrationStatus, MigrationPhase,
    },
    metrics,
    migrator::{self, JobResult, JobStatus, MigrationJobConfig},
    util::extract_image_tag,
    Error,
};

use super::{checksum, image, notify, status, Context, ReconcileResult};

/// Handle Migrating phase - check job status (non-blocking)
///
/// Uses the non-blocking `check_job_status` pattern instead of blocking
/// on job completion. This allows the reconciler to process other migrations
/// while jobs are running.
pub(super) async fn handle_migrating(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    metrics::set_migration_phase(&name, &namespace, "Migrating");

    // Get the current job name and start time from status
    let status_ref = migration.status.as_ref();
    let job_name = match status_ref.and_then(|s| s.current_job.clone()) {
        Some(j) => j,
        None => {
            tracing::error!(
                name = %name,
                namespace = %namespace,
                "No current job found in Migrating phase"
            );
            return Err(Error::Internal(
                "No current job in Migrating phase".to_string(),
            ));
        }
    };

    let job_started_at = status_ref.and_then(|s| s.job_started_at);

    // Parse timeout
    let timeout = humantime::parse_duration(&migration.spec.timeouts.migration)
        .unwrap_or(ctx.config.default_migration_timeout);

    // Calculate job duration so far
    let job_duration = job_started_at
        .map(|start| {
            let elapsed = Utc::now() - start;
            Duration::from_secs(elapsed.num_seconds().max(0) as u64)
        })
        .unwrap_or(Duration::ZERO);

    // Non-blocking job status check — always check completion BEFORE timeout.
    // A job that finished successfully after the timeout window should still
    // be treated as success (migrations are idempotent, the work is done).
    match migrator::check_job_status(ctx.client.clone(), &namespace, &job_name, job_duration).await
    {
        Ok(JobStatus::Completed(job_result)) => {
            if job_duration > timeout {
                tracing::info!(
                    name = %name,
                    namespace = %namespace,
                    job = %job_name,
                    elapsed_secs = %job_duration.as_secs(),
                    timeout_secs = %timeout.as_secs(),
                    "Job completed after timeout window — accepting result"
                );
            }
            handle_job_result(migration, ctx, &job_result).await
        }
        Ok(JobStatus::Running) => {
            // Job still running — now check timeout
            if job_duration > timeout {
                tracing::warn!(
                    name = %name,
                    namespace = %namespace,
                    job = %job_name,
                    elapsed_secs = %job_duration.as_secs(),
                    timeout_secs = %timeout.as_secs(),
                    "Job exceeded timeout while still running"
                );
                return handle_job_timeout(migration, ctx).await;
            }

            tracing::debug!(
                name = %name,
                namespace = %namespace,
                job = %job_name,
                elapsed_secs = %job_duration.as_secs(),
                "Job still running, will check again"
            );
            // Check again in 5 seconds
            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Ok(JobStatus::NotFound) => {
            // Job disappeared - this shouldn't happen normally
            tracing::error!(
                name = %name,
                namespace = %namespace,
                job = %job_name,
                "Migration job not found"
            );
            Err(Error::MigrationFailed {
                name: name.clone(),
                reason: format!("Migration job '{}' not found", job_name),
            })
        }
        Err(e) => Err(e),
    }
}

/// Handle a completed job result
async fn handle_job_result(
    migration: &DatabaseMigration,
    ctx: &Context,
    result: &JobResult,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    // Get the current migrator that was running
    let current_migrator = match migration.current_migrator() {
        Some(m) => m,
        None => {
            return Err(Error::Internal(
                "No current migrator found when handling job result".to_string(),
            ));
        }
    };

    // Get current image for recording: imageOverride takes precedence
    let image_str = if let Some(ref override_image) = current_migrator.image_override {
        override_image.clone()
    } else {
        ctx.get_deployment_image_cached(
            &namespace,
            &current_migrator.deployment_ref.name,
            current_migrator.deployment_ref.container_name.as_deref(),
        )
        .await?
    };
    let image_tag = extract_image_tag(&image_str);

    if result.success {
        tracing::info!(
            event = "migration_succeeded",
            name = %name,
            namespace = %namespace,
            duration_secs = %result.duration.as_secs(),
            image_tag = %image_tag,
            "Migration succeeded"
        );

        // Record success metrics
        metrics::record_migration_success(&name, &namespace, result.duration.as_secs_f64());

        // Send notifications
        notify::notify_migration_success(migration, ctx, &image_tag, result.duration.as_secs())
            .await;

        // Update status to Ready
        status::update_status_success(ctx, migration, &image_tag, result.duration).await?;

        return Ok(Action::requeue(ctx.config.requeue_interval));
    }

    // Handle failure case
    let error_msg = notify::build_detailed_error_message(result);

    // Log with both error sources for debugging
    tracing::warn!(
        name = %name,
        namespace = %namespace,
        job_error = ?result.error,
        pod_logs_error = ?result.pod_logs_error,
        pod_logs_tail_lines = ?result.pod_logs_tail.as_ref().map(|s| s.lines().count()),
        "Migration failed"
    );

    // Try checksum reconciliation for VersionMismatch errors
    let job_name = migration
        .status
        .as_ref()
        .and_then(|s| s.current_job.clone());

    let checksum_mode = &migration.spec.safety.checksum_mode;
    metrics::set_checksum_mode(&namespace, &name, &checksum_mode.to_string());

    // Check for checksum mismatch and handle it
    if let Some(job_name) = job_name {
        if let Some(mismatch) =
            checksum::detect_checksum_mismatch_from_job(ctx, &namespace, &job_name).await
        {
            if let Some(action) =
                checksum::handle_checksum_mismatch(migration, ctx, &mismatch, &job_name, &image_tag)
                    .await?
            {
                return Ok(action);
            }
        }
    }

    // Check retry count
    if migration.retries_exhausted() {
        tracing::error!(
            event = "migration_failed_permanently",
            name = %name,
            namespace = %namespace,
            retries = migration.retry_count(),
            max_retries = migration.spec.safety.max_retries,
            error = %error_msg,
            "Max retries exceeded, migration failed permanently"
        );

        metrics::record_migration_failure(&name, &namespace, "max_retries");

        // Send failure notifications (permanent)
        notify::notify_migration_failure(
            migration,
            ctx,
            &image_tag,
            &error_msg,
            migration.retry_count(),
            migration.spec.safety.max_retries,
            true,
        )
        .await;

        status::update_status_failed(ctx, migration, &image_tag, &error_msg).await?;

        Ok(Action::requeue(Duration::from_secs(300)))
    } else {
        // Retry
        metrics::record_retry(&name, &namespace);

        let retry_count = migration.retry_count() + 1;
        tracing::info!(
            name = %name,
            namespace = %namespace,
            retry_count = retry_count,
            "Retrying migration"
        );

        // Increment retry count and restart
        status::increment_retry_count(ctx, migration).await?;
        start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Handle job timeout
async fn handle_job_timeout(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    tracing::warn!(
        name = %name,
        namespace = %namespace,
        "Migration job timed out"
    );

    // Get current migrator's deployment ref
    let current_migrator = match migration.current_migrator() {
        Some(m) => m,
        None => {
            return Err(Error::Internal(
                "No current migrator found when handling job timeout".to_string(),
            ));
        }
    };

    if migration.retries_exhausted() {
        let image_str = if let Some(ref override_image) = current_migrator.image_override {
            override_image.clone()
        } else {
            ctx.get_deployment_image_cached(
                &namespace,
                &current_migrator.deployment_ref.name,
                current_migrator.deployment_ref.container_name.as_deref(),
            )
            .await?
        };
        let image_tag = extract_image_tag(&image_str);

        metrics::record_migration_failure(&name, &namespace, "timeout");
        status::update_status_failed(ctx, migration, &image_tag, "Migration timed out").await?;

        Ok(Action::requeue(Duration::from_secs(300)))
    } else {
        metrics::record_retry(&name, &namespace);
        status::increment_retry_count(ctx, migration).await?;
        start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Start a migration job
pub(super) async fn start_migration(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> crate::Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    // Get the current migrator to run
    let migrator_spec = match migration.current_migrator() {
        Some(m) => m,
        None => {
            return Err(Error::Internal("No migrator configured".to_string()));
        }
    };

    let migrator_index = migration.current_migrator_index();
    let migrator_name = migrator_spec.display_name();

    // Resolve the container image: imageOverride takes precedence over deployment lookup
    let image_str = if let Some(ref override_image) = migrator_spec.image_override {
        tracing::info!(
            name = %name,
            namespace = %namespace,
            image_override = %override_image,
            "Using imageOverride for migration job"
        );
        override_image.clone()
    } else {
        ctx.get_deployment_image_cached(
            &namespace,
            &migrator_spec.deployment_ref.name,
            migrator_spec.deployment_ref.container_name.as_deref(),
        )
        .await?
    };

    let image_tag = extract_image_tag(&image_str);

    // Pre-flight validation for PreFlight mode
    let checksum_mode = &migration.spec.safety.checksum_mode;
    metrics::set_checksum_mode(&namespace, &name, &checksum_mode.to_string());

    if matches!(checksum_mode, ChecksumMode::PreFlight) {
        checksum::run_preflight_validation(migration, ctx).await?;
    }

    tracing::info!(
        event = "migration_starting",
        name = %name,
        namespace = %namespace,
        image = %image_str,
        image_tag = %image_tag,
        migrator_type = ?migrator_spec.migrator_type,
        migrator_name = %migrator_name,
        migrator_index = migrator_index,
        total_migrators = migration.migrator_count(),
        "Starting migration job"
    );

    // Record migration start
    metrics::record_migration_start(&name, &namespace);

    // Emit K8s event for migration started
    ctx.event_recorder
        .migration_started(migration, &image_tag)
        .await;

    // Emit phase change event
    ctx.event_recorder
        .phase_changed(migration, "CheckingHealth", "Migrating")
        .await;

    // Notify Release Tracker (fire-and-forget)
    let deployment_name = &migrator_spec.deployment_ref.name;
    ctx.webhook_client
        .notify_started(&namespace, &name, deployment_name, &image_tag)
        .await;

    // Notify Discord (fire-and-forget)
    ctx.discord_client
        .notify_started(&namespace, &name, deployment_name, &image_tag)
        .await;

    // Inherit scheduling constraints from the deployment so migration jobs
    // run on the same node(s) (required for node taints, affinity, etc.)
    let scheduling = image::get_deployment_scheduling(
        ctx.client.clone(),
        &namespace,
        &migrator_spec.deployment_ref.name,
    )
    .await;

    // Build the job using the configured migrator
    let config = MigrationJobConfig {
        image: image_str,
        image_tag: image_tag.clone(),
        command: migrator_spec.effective_command(),
        args: migrator_spec.args.clone(),
        working_dir: migrator_spec.working_dir.clone(),
        resources: migrator_spec.resources.clone(),
        env_from: migrator_spec.env_from.clone(),
        env: migrator_spec.env.clone(),
        service_account_name: migrator_spec.service_account_name.clone(),
        migrator_type: migrator_spec.migrator_type.clone(),
        // TODO: Implement trace context extraction from current span when tracing is configured
        trace_context: None,
        scheduling,
        job_ttl_seconds: ctx.config.job_ttl_seconds,
        disable_mesh_sidecar: ctx.config.disable_mesh_sidecar,
        mesh_sidecar_annotation_key: ctx.config.mesh_sidecar_annotation_key.clone(),
        mesh_sidecar_annotation_value: ctx.config.mesh_sidecar_annotation_value.clone(),
    };

    let job = migrator::build_migration_job(migration, config)?;
    let job_name = job.metadata.name.clone().unwrap_or_default();

    // Create the job
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
    jobs.create(&PostParams::default(), &job)
        .await
        .map_err(Error::KubeApi)?;

    // Update status with job start time for non-blocking timeout tracking
    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);
    let total_migrators = migration.migrator_count() as u32;
    let new_status = DatabaseMigrationStatus {
        phase: Some(MigrationPhase::Migrating),
        current_job: Some(job_name),
        job_started_at: Some(Utc::now()),
        retry_count: migration.status.as_ref().and_then(|s| s.retry_count),
        conditions: Some(vec![Condition::not_ready(
            migration.metadata.generation.unwrap_or(0),
            "MigrationRunning",
            &format!(
                "Running migrator {} ({}/{})",
                migrator_name,
                migrator_index + 1,
                total_migrators
            ),
        )]),
        observed_generation: migration.metadata.generation,
        // Multi-migrator status fields
        current_migrator_index: Some(migrator_index),
        current_migrator_name: Some(migrator_name),
        total_migrators: Some(total_migrators),
        // Preserve existing migrator results
        migrator_results: migration
            .status
            .as_ref()
            .and_then(|s| s.migrator_results.clone()),
        ..Default::default()
    };

    let patch = Patch::Merge(json!({ "status": new_status }));
    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}
