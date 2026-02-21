//! Pending / Ready phase handler — detects deployment image changes and triggers migrations.

use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use serde_json::json;
use std::time::Duration;

use crate::{
    crd::{DatabaseMigration, MigrationPhase, EXPECTED_TAG_ANNOTATION},
    util::extract_image_tag,
    Error,
};

use super::{status, Context, ReconcileResult};

/// Handle Pending or Ready phase - check if migration is needed
pub(super) async fn handle_pending_or_ready(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    // Get the primary migrator to check for imageOverride
    let primary_migrator = match migration.get_migrators().first() {
        Some(m) => *m,
        None => {
            tracing::error!(
                name = %name,
                namespace = %namespace,
                "No migrator configured in DatabaseMigration"
            );
            return Err(Error::Internal("No migrator configured".to_string()));
        }
    };

    // If imageOverride is set, bypass deployment lookup entirely.
    // This is used by ephemeral environments where the deployment doesn't exist yet.
    if let Some(ref override_image) = primary_migrator.image_override {
        let image_tag = extract_image_tag(override_image);

        // For imageOverride, always run migration on first reconcile (Pending).
        // In Ready state, only re-run if the override image changed.
        let current_phase = migration
            .status
            .as_ref()
            .and_then(|s| s.phase.clone())
            .unwrap_or(MigrationPhase::Pending);

        if current_phase == MigrationPhase::Ready && !migration.needs_migration(&image_tag) {
            tracing::debug!(
                event = "migration_skipped",
                reason = "image_override_unchanged",
                name = %name,
                namespace = %namespace,
                image_override = %override_image,
                "No migration needed: imageOverride unchanged"
            );
            return Ok(Action::requeue(ctx.config.requeue_interval));
        }

        tracing::info!(
            event = "migration_needed",
            name = %name,
            namespace = %namespace,
            image_override = %override_image,
            image_tag = %image_tag,
            "Migration needed (imageOverride set), checking database health"
        );

        ctx.event_recorder
            .phase_changed(migration, "Pending", "CheckingHealth")
            .await;
        status::update_phase(ctx, migration, MigrationPhase::CheckingHealth).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Standard path: resolve image from deployment
    let deployment_ref = migration.primary_deployment_ref().unwrap();

    // Check for expected-tag annotation (release in progress)
    // If present, invalidate the image cache to pick up new deployments immediately
    let expected_tag = migration.expected_release_tag().map(|s| s.to_string());
    if let Some(ref tag) = expected_tag {
        tracing::info!(
            name = %name,
            namespace = %namespace,
            expected_tag = %tag,
            "Release annotation detected, invalidating image cache"
        );
        ctx.invalidate_image_cache(&namespace, &deployment_ref.name);

        // Notify Discord about release detection (fire-and-forget)
        // Only notify once per (namespace, name, tag) to avoid spamming during
        // the 1s fast-requeue polling loop while awaiting deployment rollout.
        let notify_key = format!("{}/{}:{}", namespace, name, tag);
        let already_notified = ctx
            .release_notified
            .read()
            .unwrap()
            .contains(&notify_key);

        if !already_notified {
            ctx.release_notified
                .write()
                .unwrap()
                .insert(notify_key);

            let deployment_name = deployment_ref.name.as_str();
            ctx.discord_client
                .notify(
                    crate::controller::webhook::MigrationEventType::ReleaseDetected,
                    &namespace,
                    &name,
                    deployment_name,
                    tag,
                    Some(&format!("Awaiting deployment image tag: {}", tag)),
                    None,
                    None,
                    None,
                )
                .await;
        }
    }

    // Get the current image from the deployment (cached for 30s, but cache
    // was just invalidated if expected-tag annotation is present)
    let image = ctx
        .get_deployment_image_cached(
            &namespace,
            &deployment_ref.name,
            deployment_ref.container_name.as_deref(),
        )
        .await?;

    let image_tag = extract_image_tag(&image);

    // If expected-tag annotation is set and the deployment now has the expected tag,
    // clear the annotation (self-cleaning) and remove the notification dedup entry
    if let Some(ref tag) = expected_tag {
        if image_tag == *tag {
            tracing::info!(
                name = %name,
                namespace = %namespace,
                expected_tag = %tag,
                "Expected tag detected on deployment, clearing annotation"
            );
            clear_expected_tag_annotation(ctx, migration).await;
            let notify_key = format!("{}/{}:{}", namespace, name, tag);
            ctx.release_notified.write().unwrap().remove(&notify_key);
        }
    }

    // Check if migration is needed
    let last_tag = migration.last_image_tag();
    if !migration.needs_migration(&image_tag) {
        // If expected-tag annotation is set but deployment hasn't updated yet,
        // fast-requeue at 1s to detect the new image quickly
        if let Some(ref tag) = expected_tag {
            if image_tag != *tag {
                tracing::debug!(
                    name = %name,
                    namespace = %namespace,
                    current_tag = %image_tag,
                    expected_tag = %tag,
                    "Expected tag not yet on deployment, fast-requeue"
                );
                return Ok(Action::requeue(Duration::from_secs(1)));
            }
        }

        tracing::debug!(
            event = "migration_skipped",
            reason = "image_unchanged",
            name = %name,
            namespace = %namespace,
            current_tag = %image_tag,
            last_tag = ?last_tag,
            "No migration needed: image tag unchanged"
        );
        return Ok(Action::requeue(ctx.config.requeue_interval));
    }

    tracing::info!(
        event = "migration_needed",
        name = %name,
        namespace = %namespace,
        current_tag = %image_tag,
        last_tag = ?last_tag,
        "Migration needed, checking database health"
    );

    // Emit phase change event
    ctx.event_recorder
        .phase_changed(migration, "Pending", "CheckingHealth")
        .await;

    // Transition to CheckingHealth
    status::update_phase(ctx, migration, MigrationPhase::CheckingHealth).await?;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Clear the expected-tag annotation from a DatabaseMigration
pub(super) async fn clear_expected_tag_annotation(ctx: &Context, migration: &DatabaseMigration) {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();
    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let patch = json!({
        "metadata": {
            "annotations": {
                EXPECTED_TAG_ANNOTATION: null
            }
        }
    });

    match migrations
        .patch(
            &name,
            &PatchParams::apply("shinka-controller"),
            &Patch::Merge(&patch),
        )
        .await
    {
        Ok(_) => {
            tracing::debug!(
                name = %name,
                namespace = %namespace,
                "Cleared expected-tag annotation"
            );
        }
        Err(e) => {
            tracing::warn!(
                name = %name,
                namespace = %namespace,
                error = %e,
                "Failed to clear expected-tag annotation (non-fatal)"
            );
        }
    }
}
