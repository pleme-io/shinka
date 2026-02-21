//! Status update operations for DatabaseMigration resources.

use chrono::Utc;
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;
use std::time::Duration;

use crate::{
    crd::{
        Condition, DatabaseMigration, DatabaseMigrationStatus, LastMigration, MigrationPhase,
    },
    Error, Result,
};

use super::Context;

/// Update phase in status
pub(super) async fn update_phase(
    ctx: &Context,
    migration: &DatabaseMigration,
    phase: MigrationPhase,
) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);
    let patch = Patch::Merge(json!({
        "status": {
            "phase": phase,
            "observedGeneration": migration.metadata.generation,
        }
    }));

    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}

/// Update status on success
pub(super) async fn update_status_success(
    ctx: &Context,
    migration: &DatabaseMigration,
    image_tag: &str,
    duration: Duration,
) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let status = DatabaseMigrationStatus {
        phase: Some(MigrationPhase::Ready),
        last_migration: Some(LastMigration {
            image_tag: image_tag.to_string(),
            success: true,
            duration: Some(format!("{}s", duration.as_secs())),
            completed_at: Some(Utc::now()),
            error: None,
        }),
        retry_count: Some(0),
        current_job: None,
        job_started_at: None,
        conditions: Some(vec![Condition::ready(
            migration.metadata.generation.unwrap_or(0),
            "MigrationSucceeded",
            &format!("Migration completed successfully for image {}", image_tag),
        )]),
        observed_generation: migration.metadata.generation,
        // Clear multi-migrator status on success
        current_migrator_index: None,
        current_migrator_name: None,
        total_migrators: None,
        migrator_results: None,
    };

    let patch = Patch::Merge(json!({ "status": status }));
    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}

/// Update status on failure
pub(super) async fn update_status_failed(
    ctx: &Context,
    migration: &DatabaseMigration,
    image_tag: &str,
    error: &str,
) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let status = DatabaseMigrationStatus {
        phase: Some(MigrationPhase::Failed),
        last_migration: Some(LastMigration {
            image_tag: image_tag.to_string(),
            success: false,
            duration: None,
            completed_at: Some(Utc::now()),
            error: Some(error.to_string()),
        }),
        retry_count: migration.status.as_ref().and_then(|s| s.retry_count),
        current_job: None,
        job_started_at: None,
        conditions: Some(vec![Condition::not_ready(
            migration.metadata.generation.unwrap_or(0),
            "MigrationFailed",
            error,
        )]),
        observed_generation: migration.metadata.generation,
        // Preserve multi-migrator status for debugging which migrator failed
        current_migrator_index: migration.status.as_ref().and_then(|s| s.current_migrator_index),
        current_migrator_name: migration.status.as_ref().and_then(|s| s.current_migrator_name.clone()),
        total_migrators: migration.status.as_ref().and_then(|s| s.total_migrators),
        migrator_results: migration.status.as_ref().and_then(|s| s.migrator_results.clone()),
    };

    let patch = Patch::Merge(json!({ "status": status }));
    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}

/// Increment retry count
pub(super) async fn increment_retry_count(
    ctx: &Context,
    migration: &DatabaseMigration,
) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let current = migration.retry_count();
    let patch = Patch::Merge(json!({
        "status": {
            "retryCount": current + 1,
        }
    }));

    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}

/// Reset retry count (used after successful checksum reconciliation)
pub(super) async fn reset_retry_count(
    ctx: &Context,
    migration: &DatabaseMigration,
) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    let patch = Patch::Merge(json!({
        "status": {
            "retryCount": 0,
        }
    }));

    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    tracing::debug!(
        name = %name,
        namespace = %namespace,
        "Reset retry count after checksum reconciliation"
    );

    Ok(())
}
