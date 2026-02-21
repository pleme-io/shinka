//! Reconciler for DatabaseMigration resources
//!
//! State machine:
//! ```text
//! Pending → CheckingHealth → [healthy] → Migrating → Ready
//!                         ↓ [unhealthy]
//!                    WaitingForDatabase (requeue 30s)
//!
//! Migrating → [job failed] → Retry (if < maxRetries) → Migrating
//!          → [retries exhausted] → Failed
//! ```

mod checksum;
mod image;
mod notify;
mod state_failed;
mod state_health;
mod state_migrating;
mod state_pending;
mod status;

use kube::{
    runtime::{
        controller::Action,
        finalizer::{finalizer, Event as FinalizerEvent},
    },
    Client,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Instant,
};

use crate::{
    config::Config,
    controller::{
        discord::OptionalDiscordClient,
        events::OptionalEventRecorder,
        finalizers,
        webhook::OptionalReleaseTrackerClient,
    },
    crd::{DatabaseMigration, MigrationPhase},
    metrics,
    Error,
};

use image::ImageCacheKey;

/// Result type for reconciliation
pub type ReconcileResult = std::result::Result<Action, Error>;

/// Context shared across all reconcile calls
pub struct Context {
    pub client: Client,
    pub config: Config,
    /// Optional Release Tracker webhook client for migration event notifications
    pub webhook_client: OptionalReleaseTrackerClient,
    /// Optional Discord webhook client for beautiful notifications
    pub discord_client: OptionalDiscordClient,
    /// Kubernetes Event recorder for native events
    pub event_recorder: OptionalEventRecorder,
    /// Cache for deployment images (TTL: 30 seconds)
    image_cache: RwLock<HashMap<ImageCacheKey, image::CachedImage>>,
    /// Tracks release tags we've already sent Discord notifications for.
    /// Key: "namespace/name:tag". Prevents duplicate "Release Detected" messages
    /// during the fast-requeue polling loop.
    release_notified: RwLock<HashSet<String>>,
}

impl Context {
    /// Create a new Context
    pub fn new(
        client: Client,
        config: Config,
        webhook_client: OptionalReleaseTrackerClient,
        discord_client: OptionalDiscordClient,
        event_recorder: OptionalEventRecorder,
    ) -> Self {
        Self {
            client,
            config,
            webhook_client,
            discord_client,
            event_recorder,
            image_cache: RwLock::new(HashMap::new()),
            release_notified: RwLock::new(HashSet::new()),
        }
    }
}

/// Main reconciliation function
pub async fn reconcile(migration: Arc<DatabaseMigration>, ctx: Arc<Context>) -> ReconcileResult {
    let _start_time = Instant::now();
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    tracing::info!(
        name = %name,
        namespace = %namespace,
        "Reconciling DatabaseMigration"
    );

    // Handle finalizer
    let migrations: kube::api::Api<DatabaseMigration> =
        kube::api::Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(
        &migrations,
        finalizers::FINALIZER,
        migration.clone(),
        |event| async {
            match event {
                FinalizerEvent::Apply(migration) => {
                    reconcile_migration(migration, ctx.clone()).await
                }
                FinalizerEvent::Cleanup(migration) => {
                    finalizers::cleanup(
                        ctx.client.clone(),
                        &migration.namespace_or_default(),
                        &migration.name_or_default(),
                    )
                    .await?;
                    Ok(Action::await_change())
                }
            }
        },
    )
    .await
    .map_err(Error::from)
}

/// Reconcile a DatabaseMigration resource
async fn reconcile_migration(
    migration: Arc<DatabaseMigration>,
    ctx: Arc<Context>,
) -> ReconcileResult {
    let _start_time = Instant::now();
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    // Get current phase
    let current_phase = migration
        .status
        .as_ref()
        .and_then(|s| s.phase.clone())
        .unwrap_or(MigrationPhase::Pending);

    tracing::debug!(
        name = %name,
        namespace = %namespace,
        phase = %current_phase,
        "Current phase"
    );

    // State machine
    let result = match current_phase {
        MigrationPhase::Pending | MigrationPhase::Ready => {
            state_pending::handle_pending_or_ready(&migration, &ctx).await
        }
        MigrationPhase::CheckingHealth => {
            state_health::handle_checking_health(&migration, &ctx).await
        }
        MigrationPhase::WaitingForDatabase => {
            state_health::handle_waiting_for_database(&migration, &ctx).await
        }
        MigrationPhase::Migrating => {
            state_migrating::handle_migrating(&migration, &ctx).await
        }
        MigrationPhase::Failed => {
            state_failed::handle_failed(&migration, &ctx).await
        }
    };

    // Record metrics
    let _duration = _start_time.elapsed().as_secs_f64();
    match &result {
        Ok(_) => metrics::record_reconciliation("success"),
        Err(e) => {
            metrics::record_reconciliation("error");
            metrics::ERRORS_TOTAL
                .with_label_values(&[&name, &namespace, e.category()])
                .inc();
        }
    }

    result
}

/// Error policy - determines how to handle reconciliation errors
pub fn error_policy(
    migration: Arc<DatabaseMigration>,
    error: &Error,
    _ctx: Arc<Context>,
) -> Action {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    tracing::error!(
        name = %name,
        namespace = %namespace,
        error = %error,
        "Reconciliation error"
    );

    // Record error metric
    metrics::ERRORS_TOTAL
        .with_label_values(&[&name, &namespace, error.category()])
        .inc();

    // Requeue based on error type
    Action::requeue(error.requeue_duration())
}
