//! Failed phase handler — comprehensive retry strategy.
//!
//! Implements smart migration recovery with multiple retry triggers:
//!
//! 1. **Image change**: When deployment image changes, auto-retry (migrations are idempotent)
//! 2. **Manual retry annotation**: `shinka.pleme.io/retry=true` triggers immediate retry
//! 3. **Database recovery**: If DB was unhealthy and is now healthy, auto-retry
//! 4. **Cooldown retry**: After 1 hour in Failed state, check DB health and retry if healthy
//! 5. **Spec generation change**: If spec is updated (e.g., different timeout), auto-retry

use chrono::Utc;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use serde_json::json;
use std::time::Duration;

use crate::{
    crd::{
        Condition, DatabaseMigration, DatabaseMigrationStatus, MigrationPhase,
        RETRY_ANNOTATION,
    },
    database, metrics,
    util::extract_image_tag,
    Error,
};

use super::{state_pending, Context, ReconcileResult};

/// Handle Failed phase - comprehensive retry strategy
pub(super) async fn handle_failed(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    // Get the primary migrator for image checking
    let primary_migrator = match migration.get_migrators().first() {
        Some(m) => *m,
        None => {
            tracing::error!(
                name = %name,
                namespace = %namespace,
                "No migrator configured in Failed state"
            );
            return Ok(Action::requeue(Duration::from_secs(300)));
        }
    };

    let deployment_ref = &primary_migrator.deployment_ref;
    let last_tag = migration.last_image_tag();

    // If imageOverride is set, there's no deployment to watch for image changes.
    // Only retry on manual annotation or spec generation change.
    if let Some(ref image_override) = primary_migrator.image_override {
        // Check for manual retry annotation
        if migration.has_retry_annotation() {
            tracing::info!(
                event = "manual_retry_triggered",
                name = %name,
                namespace = %namespace,
                "Retry annotation detected (imageOverride), triggering manual retry"
            );
            ctx.event_recorder.manual_retry(migration).await;
            remove_retry_annotation(ctx, migration).await?;
            let override_tag = extract_image_tag(image_override);
            metrics::record_auto_retry(&name, &namespace);
            reset_and_retry(ctx, migration, &override_tag).await?;
            return Ok(Action::requeue(Duration::from_secs(5)));
        }

        // Check for spec generation change
        let observed_gen = migration
            .status
            .as_ref()
            .and_then(|s| s.observed_generation);
        let current_gen = migration.metadata.generation;
        if let (Some(observed), Some(current)) = (observed_gen, current_gen) {
            if current > observed {
                let override_tag = extract_image_tag(image_override);
                metrics::record_auto_retry(&name, &namespace);
                reset_and_retry(ctx, migration, &override_tag).await?;
                return Ok(Action::requeue(Duration::from_secs(5)));
            }
        }

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Strategy 0: Check expected-tag annotation (release in progress)
    let expected_tag = migration.expected_release_tag().map(|s| s.to_string());
    if let Some(ref expected) = expected_tag {
        let tag_changed = last_tag.as_ref().map(|t| t != expected).unwrap_or(true);
        if tag_changed {
            let previous_error = migration
                .status
                .as_ref()
                .and_then(|s| s.last_migration.as_ref())
                .and_then(|lm| lm.error.as_deref());

            tracing::info!(
                event = "auto_retry_expected_tag",
                name = %name,
                namespace = %namespace,
                old_tag = ?last_tag,
                expected_tag = %expected,
                previous_error = ?previous_error,
                "Expected-tag annotation detected, automatically retrying failed migration"
            );

            metrics::record_auto_retry(&name, &namespace);

            // Emit K8s event for expected-tag retry
            ctx.event_recorder
                .migration_retrying(migration, last_tag.as_deref(), expected)
                .await;

            let deployment_name = &deployment_ref.name;
            ctx.discord_client
                .notify_auto_retried(
                    &namespace,
                    &name,
                    deployment_name,
                    expected,
                    last_tag.as_deref(),
                    previous_error,
                )
                .await;

            // Clear the expected-tag annotation to avoid re-triggering on the same tag
            state_pending::clear_expected_tag_annotation(ctx, migration).await;

            reset_and_retry(ctx, migration, expected).await?;
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
    }

    // Get the current image from the deployment (cached)
    let image = match ctx
        .get_deployment_image_cached(
            &namespace,
            &deployment_ref.name,
            deployment_ref.container_name.as_deref(),
        )
        .await
    {
        Ok(img) => img,
        Err(e) => {
            tracing::warn!(
                name = %name,
                namespace = %namespace,
                error = %e,
                "Failed to get deployment image in Failed state, staying failed"
            );
            return Ok(Action::requeue(Duration::from_secs(300)));
        }
    };

    let image_tag = extract_image_tag(&image);

    // Strategy 1: Check if image changed
    let image_changed = last_tag.as_ref().map(|t| t != &image_tag).unwrap_or(false);
    if image_changed {
        let previous_error = migration
            .status
            .as_ref()
            .and_then(|s| s.last_migration.as_ref())
            .and_then(|lm| lm.error.as_deref());

        tracing::info!(
            event = "auto_retry_image_changed",
            name = %name,
            namespace = %namespace,
            old_tag = ?last_tag,
            new_tag = %image_tag,
            previous_error = ?previous_error,
            "Image changed, automatically retrying failed migration"
        );

        metrics::record_auto_retry(&name, &namespace);

        // Emit K8s event for auto-retry
        ctx.event_recorder
            .migration_retrying(migration, last_tag.as_deref(), &image_tag)
            .await;

        let deployment_name = &deployment_ref.name;
        ctx.webhook_client
            .notify_auto_retried(
                &namespace,
                &name,
                deployment_name,
                &image_tag,
                last_tag.as_deref(),
            )
            .await;

        // Notify Discord about auto-retry with previous error details
        ctx.discord_client
            .notify_auto_retried(
                &namespace,
                &name,
                deployment_name,
                &image_tag,
                last_tag.as_deref(),
                previous_error,
            )
            .await;

        reset_and_retry(ctx, migration, &image_tag).await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    // Strategy 2: Check for manual retry annotation
    if migration.has_retry_annotation() {
        tracing::info!(
            event = "manual_retry_triggered",
            name = %name,
            namespace = %namespace,
            "Retry annotation detected, triggering manual retry"
        );

        // Emit K8s event for manual retry
        ctx.event_recorder.manual_retry(migration).await;

        // Remove the annotation to prevent infinite retry loops
        remove_retry_annotation(ctx, migration).await?;

        metrics::record_auto_retry(&name, &namespace);
        reset_and_retry(ctx, migration, &image_tag).await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    // Strategy 3: Check for spec generation change (user updated the spec)
    let observed_gen = migration
        .status
        .as_ref()
        .and_then(|s| s.observed_generation);
    let current_gen = migration.metadata.generation;
    if let (Some(observed), Some(current)) = (observed_gen, current_gen) {
        if current > observed {
            tracing::info!(
                event = "auto_retry_spec_changed",
                name = %name,
                namespace = %namespace,
                old_generation = observed,
                new_generation = current,
                "Spec generation changed, automatically retrying failed migration"
            );

            // Emit K8s event for spec change retry
            ctx.event_recorder
                .migration_retrying(migration, None, &image_tag)
                .await;

            metrics::record_auto_retry(&name, &namespace);
            reset_and_retry(ctx, migration, &image_tag).await?;
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
    }

    // Strategy 4 & 5: Cooldown retry with database health check
    let failed_at = migration
        .status
        .as_ref()
        .and_then(|s| s.last_migration.as_ref())
        .and_then(|m| m.completed_at);

    let cooldown_elapsed = failed_at
        .map(|t| {
            let elapsed = Utc::now().signed_duration_since(t);
            elapsed.num_seconds() > 3600 // 1 hour cooldown
        })
        .unwrap_or(false);

    if cooldown_elapsed {
        // Check database health before retrying
        let cluster_name = &migration.spec.database.cnpg_cluster_ref.name;
        let health =
            database::check_cluster_health(ctx.client.clone(), &namespace, cluster_name).await;

        match health {
            Ok(h) if h.healthy => {
                tracing::info!(
                    event = "auto_retry_cooldown_healthy",
                    name = %name,
                    namespace = %namespace,
                    cluster = %cluster_name,
                    "Cooldown elapsed and database is healthy, automatically retrying"
                );

                // Emit K8s event for cooldown retry
                ctx.event_recorder
                    .migration_retrying(migration, None, &image_tag)
                    .await;

                metrics::record_auto_retry(&name, &namespace);
                reset_and_retry(ctx, migration, &image_tag).await?;
                return Ok(Action::requeue(Duration::from_secs(5)));
            }
            Ok(h) => {
                tracing::debug!(
                    event = "cooldown_db_unhealthy",
                    name = %name,
                    namespace = %namespace,
                    cluster = %cluster_name,
                    db_message = %h.message,
                    "Cooldown elapsed but database still unhealthy, waiting"
                );
            }
            Err(e) => {
                tracing::debug!(
                    event = "cooldown_db_check_failed",
                    name = %name,
                    namespace = %namespace,
                    cluster = %cluster_name,
                    error = %e,
                    "Could not check database health during cooldown check"
                );
            }
        }
    }

    // No retry condition met, stay in Failed state
    let requeue_secs = if failed_at.is_some() && !cooldown_elapsed {
        let time_until_cooldown = failed_at
            .map(|t| {
                let elapsed = Utc::now().signed_duration_since(t);
                let remaining = 3600 - elapsed.num_seconds();
                remaining.max(60) as u64
            })
            .unwrap_or(300);
        time_until_cooldown.min(300)
    } else {
        300
    };

    tracing::debug!(
        event = "failed_state_check",
        name = %name,
        namespace = %namespace,
        image_tag = %image_tag,
        image_changed = %image_changed,
        has_retry_annotation = migration.has_retry_annotation(),
        cooldown_elapsed = %cooldown_elapsed,
        requeue_secs = requeue_secs,
        "No retry conditions met, staying in Failed state"
    );

    Ok(Action::requeue(Duration::from_secs(requeue_secs)))
}

/// Reset retry count and transition migration back to Pending for auto-retry
async fn reset_and_retry(
    ctx: &Context,
    migration: &DatabaseMigration,
    new_image_tag: &str,
) -> crate::Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let status = DatabaseMigrationStatus {
        phase: Some(MigrationPhase::Pending),
        retry_count: Some(0),
        current_job: None,
        job_started_at: None,
        conditions: Some(vec![Condition::not_ready(
            migration.metadata.generation.unwrap_or(0),
            "AutoRetryTriggered",
            &format!(
                "Auto-retrying migration after image changed to {}",
                new_image_tag
            ),
        )]),
        observed_generation: migration.metadata.generation,
        // Preserve last_migration info for debugging
        last_migration: migration
            .status
            .as_ref()
            .and_then(|s| s.last_migration.clone()),
        // Reset multi-migrator tracking on retry
        current_migrator_index: Some(0),
        current_migrator_name: None,
        total_migrators: Some(migration.migrator_count() as u32),
        migrator_results: None,
    };

    let patch = Patch::Merge(json!({ "status": status }));
    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    tracing::info!(
        name = %name,
        namespace = %namespace,
        new_image_tag = %new_image_tag,
        "Reset migration to Pending for auto-retry"
    );

    Ok(())
}

/// Remove the retry annotation from a migration
///
/// Called after processing a manual retry trigger to prevent infinite loops.
async fn remove_retry_annotation(
    ctx: &Context,
    migration: &DatabaseMigration,
) -> crate::Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    // Use Merge patch to set the annotation to null (removes it)
    let patch = Patch::Merge(json!({
        "metadata": {
            "annotations": {
                RETRY_ANNOTATION: serde_json::Value::Null
            }
        }
    }));

    match migrations
        .patch(&name, &PatchParams::default(), &patch)
        .await
    {
        Ok(_) => {
            tracing::debug!(
                name = %name,
                namespace = %namespace,
                "Removed retry annotation"
            );
            Ok(())
        }
        Err(kube::Error::Api(kube::error::ErrorResponse { code: 422, .. })) => {
            // Annotation might already be removed or path doesn't exist - that's fine
            tracing::debug!(
                name = %name,
                namespace = %namespace,
                "Retry annotation already removed or path not found"
            );
            Ok(())
        }
        Err(e) => Err(Error::KubeApi(e)),
    }
}
