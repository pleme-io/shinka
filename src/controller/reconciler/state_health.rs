//! CheckingHealth / WaitingForDatabase phase handlers.

use kube::runtime::controller::Action;
use std::time::Duration;

use crate::{
    crd::{DatabaseMigration, MigrationPhase},
    database, metrics,
};

use super::{status, state_migrating, Context, ReconcileResult};

/// Handle CheckingHealth phase - verify CNPG cluster is healthy
pub(super) async fn handle_checking_health(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();
    let cluster_name = &migration.spec.database.cnpg_cluster_ref.name;

    metrics::set_migration_phase(&name, &namespace, "CheckingHealth");

    // Check cluster health
    let health =
        database::check_cluster_health(ctx.client.clone(), &namespace, cluster_name).await?;

    if health.healthy {
        tracing::info!(
            event = "database_healthy",
            name = %name,
            namespace = %namespace,
            cluster = %cluster_name,
            "Database healthy, starting migration"
        );

        // Emit database healthy event
        ctx.event_recorder
            .database_healthy(migration, cluster_name)
            .await;

        // Start migration
        state_migrating::start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    } else if migration.spec.safety.require_healthy_cluster {
        tracing::warn!(
            event = "database_unhealthy",
            name = %name,
            namespace = %namespace,
            cluster = %cluster_name,
            message = %health.message,
            require_healthy = true,
            "Database not healthy, waiting"
        );

        // Emit database unhealthy and phase change events
        ctx.event_recorder
            .database_unhealthy(migration, cluster_name, &health.message)
            .await;
        ctx.event_recorder
            .phase_changed(migration, "CheckingHealth", "WaitingForDatabase")
            .await;

        // Transition to WaitingForDatabase
        status::update_phase(ctx, migration, MigrationPhase::WaitingForDatabase).await?;

        Ok(Action::requeue(Duration::from_secs(30)))
    } else {
        // Proceed despite unhealthy cluster (safety.requireHealthyCluster = false)
        tracing::warn!(
            event = "database_unhealthy_proceeding",
            name = %name,
            namespace = %namespace,
            cluster = %cluster_name,
            message = %health.message,
            require_healthy = false,
            "Proceeding with migration despite unhealthy cluster (requireHealthyCluster=false)"
        );

        state_migrating::start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Handle WaitingForDatabase phase - wait for cluster to become healthy
pub(super) async fn handle_waiting_for_database(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();
    let cluster_name = &migration.spec.database.cnpg_cluster_ref.name;

    metrics::set_migration_phase(&name, &namespace, "WaitingForDatabase");

    // Re-check cluster health
    let health =
        database::check_cluster_health(ctx.client.clone(), &namespace, cluster_name).await?;

    if health.healthy {
        tracing::info!(
            event = "database_recovered",
            name = %name,
            namespace = %namespace,
            cluster = %cluster_name,
            "Database became healthy, starting migration"
        );

        // Emit database healthy event
        ctx.event_recorder
            .database_healthy(migration, cluster_name)
            .await;

        state_migrating::start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    } else {
        tracing::debug!(
            event = "waiting_for_database",
            name = %name,
            namespace = %namespace,
            cluster = %cluster_name,
            message = %health.message,
            "Still waiting for database to become healthy"
        );

        // Emit waiting event (periodically)
        ctx.event_recorder
            .waiting_for_database(migration, cluster_name)
            .await;

        Ok(Action::requeue(Duration::from_secs(30)))
    }
}
