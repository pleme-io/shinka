//! Direct (non-CNPG) source phase handler.
//!
//! A `directRef` migration does not run a Kubernetes Job against a CNPG cluster;
//! it applies additive DDL in-process against an already-running MySQL/Postgres
//! engine. This handler is the controller-side wiring of that branch: it drives
//! [`crate::direct::plan_and_apply_direct`] (health-skip → load SQL → synthesise
//! connection → per-op apply) and maps the typed outcome onto the migration's
//! phase FSM — `Ready` on success, `Failed` (with a typed error message) on
//! failure. The reconcile logic itself lives in [`crate::direct`] and is unit
//! tested against a mock environment; this handler owns only the status +
//! event + requeue effects.

use std::time::{Duration, Instant};

use kube::runtime::controller::Action;

use crate::{
    crd::{DatabaseMigration, MigrationPhase},
    direct::{plan_and_apply_direct, DirectReconcileOutcome, KubeDirectEnv},
    metrics,
};

use super::{status, Context, ReconcileResult};

/// Handle a direct-source migration (all phases route here — the direct branch
/// is a single in-process apply, not a multi-phase Job lifecycle).
pub(super) async fn handle_direct(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    metrics::set_migration_phase(&name, &namespace, "Migrating");

    let env = KubeDirectEnv::new(ctx.client.clone(), ctx.config.database_connect_timeout);
    let started = Instant::now();

    match plan_and_apply_direct(migration, &env).await {
        Ok(DirectReconcileOutcome::AlreadyReady { version }) => {
            // Nothing to do — ensure the CR reads Ready (idempotent).
            let current_phase = migration
                .status
                .as_ref()
                .and_then(|s| s.phase.clone());
            if current_phase != Some(MigrationPhase::Ready) {
                status::update_phase(ctx, migration, MigrationPhase::Ready).await?;
            }
            tracing::debug!(
                event = "migration_skipped",
                reason = "sql_unchanged",
                name = %name,
                namespace = %namespace,
                version = %version,
                "Direct SQL unchanged since last apply"
            );
            Ok(Action::requeue(ctx.config.requeue_interval))
        }
        Ok(DirectReconcileOutcome::Applied { version, report }) => {
            let duration = started.elapsed();
            tracing::info!(
                event = "migration_succeeded",
                name = %name,
                namespace = %namespace,
                version = %version,
                ops = report.ops,
                statements = report.statements,
                duration_secs = duration.as_secs(),
                "Direct migration applied"
            );

            metrics::record_migration_success(&name, &namespace, duration.as_secs_f64());

            // Emit lifecycle events (best-effort) and advance to Ready.
            ctx.event_recorder
                .migration_started(migration, &version)
                .await;
            status::update_status_success(ctx, migration, &version, duration).await?;
            ctx.event_recorder
                .migration_succeeded(migration, &version, duration.as_secs())
                .await;

            Ok(Action::requeue(ctx.config.requeue_interval))
        }
        Err(direct_err) => {
            // Surface the typed error onto the CR (phase=Failed) and requeue for
            // retry — additive DDL is idempotent, so a transient failure (engine
            // still coming up) heals on the next reconcile.
            let error_msg = direct_err.to_string();
            tracing::warn!(
                event = "migration_failed",
                name = %name,
                namespace = %namespace,
                error = %error_msg,
                "Direct migration failed"
            );
            metrics::record_migration_failure(&name, &namespace, "direct_apply");

            ctx.event_recorder
                .migration_failed(migration, &error_msg, migration.retry_count(), migration.spec.safety.max_retries)
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
