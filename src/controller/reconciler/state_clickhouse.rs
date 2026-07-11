//! ClickHouse typed-model source phase handler.
//!
//! A `clickhouseRef` migration does not run a Kubernetes Job against a CNPG
//! cluster; it converges a typed `analitico` table model (create-only) against
//! a Keeper-backed ClickHouse (`ON CLUSTER`, `ReplicatedMergeTree`). This
//! handler is the controller-side wiring of that branch: it drives
//! [`crate::clickhouse::plan_and_apply_clickhouse`] (health-skip → render model
//! → read live schema → converge → apply) and maps the typed outcome onto the
//! migration's phase FSM — `Ready` on success, `Failed` (with a typed error
//! message) on failure. The converge logic itself lives in
//! [`crate::clickhouse`] and is unit tested against a mock environment; this
//! handler owns only the status + event + requeue effects.

use std::time::{Duration, Instant};

use kube::runtime::controller::Action;

use crate::{
    clickhouse::{plan_and_apply_clickhouse, ClickHouseReconcileOutcome, KubeClickHouseEnv},
    crd::{DatabaseMigration, MigrationPhase},
    metrics,
};

use super::{status, Context, ReconcileResult};

/// Handle a ClickHouse typed-model migration (all phases route here — the
/// ClickHouse branch is a single in-process converge, not a multi-phase Job
/// lifecycle).
pub(super) async fn handle_clickhouse(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    metrics::set_migration_phase(&name, &namespace, "Migrating");

    let env = KubeClickHouseEnv::new(ctx.client.clone());
    let started = Instant::now();

    match plan_and_apply_clickhouse(migration, &env).await {
        Ok(ClickHouseReconcileOutcome::AlreadyReady { version }) => {
            // Nothing to do — ensure the CR reads Ready (idempotent).
            let current_phase = migration.status.as_ref().and_then(|s| s.phase.clone());
            if current_phase != Some(MigrationPhase::Ready) {
                status::update_phase(ctx, migration, MigrationPhase::Ready).await?;
            }
            tracing::debug!(
                event = "migration_skipped",
                reason = "model_present",
                name = %name,
                namespace = %namespace,
                version = %version,
                "ClickHouse model already present since last converge"
            );
            Ok(Action::requeue(ctx.config.requeue_interval))
        }
        Ok(ClickHouseReconcileOutcome::Applied { version, created }) => {
            let duration = started.elapsed();
            tracing::info!(
                event = "migration_succeeded",
                name = %name,
                namespace = %namespace,
                version = %version,
                created = ?created,
                duration_secs = duration.as_secs(),
                "ClickHouse model converged"
            );

            metrics::record_migration_success(&name, &namespace, duration.as_secs_f64());

            ctx.event_recorder
                .migration_started(migration, &version)
                .await;
            status::update_status_success(ctx, migration, &version, duration).await?;
            ctx.event_recorder
                .migration_succeeded(migration, &version, duration.as_secs())
                .await;

            Ok(Action::requeue(ctx.config.requeue_interval))
        }
        Err(ch_err) => {
            // Surface the typed error onto the CR (phase=Failed) and requeue for
            // retry — CREATE IF NOT EXISTS is idempotent, so a transient failure
            // (ClickHouse still coming up) heals on the next reconcile.
            let error_msg = ch_err.to_string();
            tracing::warn!(
                event = "migration_failed",
                name = %name,
                namespace = %namespace,
                error = %error_msg,
                "ClickHouse converge failed"
            );
            metrics::record_migration_failure(&name, &namespace, "clickhouse_apply");

            ctx.event_recorder
                .migration_failed(
                    migration,
                    &error_msg,
                    migration.retry_count(),
                    migration.spec.safety.max_retries,
                )
                .await;
            status::update_status_failed(
                ctx,
                migration,
                &migration.spec.database.display_target(),
                &error_msg,
            )
            .await?;

            Ok(Action::requeue(Duration::from_secs(30)))
        }
    }
}
