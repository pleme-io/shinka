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

use chrono::Utc;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use kube::{
    api::{Api, Patch, PatchParams, PostParams},
    runtime::{
        controller::Action,
        finalizer::{finalizer, Event as FinalizerEvent},
    },
    Client,
};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use crate::{
    config::Config,
    controller::{
        discord::OptionalDiscordClient,
        events::OptionalEventRecorder,
        finalizers,
        webhook::OptionalReleaseTrackerClient,
    },
    crd::{
        ChecksumMode, Condition, DatabaseMigration, DatabaseMigrationStatus, LastMigration,
        MigrationPhase, EXPECTED_TAG_ANNOTATION, RETRY_ANNOTATION,
    },
    database, metrics,
    migrator::{
        self,
        checksum_reconciler::{extract_secret_names, get_database_url_from_secrets},
        ChecksumReconciler, JobResult, JobStatus, MigrationJobConfig, PodScheduling,
        PreFlightValidator,
        VersionMismatchInfo,
    },
    Error, Result,
};

/// Result type for reconciliation
pub type ReconcileResult = std::result::Result<Action, Error>;

/// Cached deployment image with expiry time
#[derive(Clone)]
struct CachedImage {
    image: String,
    expires_at: Instant,
}

/// Cache key for deployment images
#[derive(Clone, Hash, Eq, PartialEq)]
struct ImageCacheKey {
    namespace: String,
    deployment: String,
    container: Option<String>,
}

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
    image_cache: RwLock<HashMap<ImageCacheKey, CachedImage>>,
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

    /// Get a cached image or fetch it from the API
    async fn get_deployment_image_cached(
        &self,
        namespace: &str,
        deployment_name: &str,
        container_name: Option<&str>,
    ) -> Result<String> {
        let cache_key = ImageCacheKey {
            namespace: namespace.to_string(),
            deployment: deployment_name.to_string(),
            container: container_name.map(|s| s.to_string()),
        };

        // Try to get from cache
        // Note: We use unwrap_or_else to handle lock poisoning gracefully
        // since the cache is not critical - we can always refetch from the API
        {
            let cache = self
                .image_cache
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(cached) = cache.get(&cache_key) {
                if cached.expires_at > Instant::now() {
                    tracing::trace!(
                        namespace = %namespace,
                        deployment = %deployment_name,
                        "Using cached deployment image"
                    );
                    return Ok(cached.image.clone());
                }
            }
        }

        // Fetch from API
        let image =
            get_deployment_image(self.client.clone(), namespace, deployment_name, container_name)
                .await?;

        // Store in cache with 30-second TTL
        {
            let mut cache = self
                .image_cache
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.insert(
                cache_key,
                CachedImage {
                    image: image.clone(),
                    expires_at: Instant::now() + Duration::from_secs(30),
                },
            );

            // Prune expired entries if cache is getting large
            if cache.len() > 100 {
                let now = Instant::now();
                cache.retain(|_, v| v.expires_at > now);
            }
        }

        Ok(image)
    }

    /// Invalidate cached image for a deployment
    ///
    /// Called when the expected-tag annotation is detected, to force a fresh
    /// image lookup instead of waiting for the 30s cache TTL to expire.
    pub fn invalidate_image_cache(&self, namespace: &str, deployment_name: &str) {
        let mut cache = self
            .image_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.retain(|k, _| !(k.namespace == namespace && k.deployment == deployment_name));
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
    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

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
            // Check if we need to run a migration
            handle_pending_or_ready(&migration, &ctx).await
        }
        MigrationPhase::CheckingHealth => handle_checking_health(&migration, &ctx).await,
        MigrationPhase::WaitingForDatabase => handle_waiting_for_database(&migration, &ctx).await,
        MigrationPhase::Migrating => handle_migrating(&migration, &ctx).await,
        MigrationPhase::Failed => {
            // Check if image changed - if so, auto-retry (migrations are idempotent)
            handle_failed(&migration, &ctx).await
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

/// Handle Pending or Ready phase - check if migration is needed
async fn handle_pending_or_ready(migration: &DatabaseMigration, ctx: &Context) -> ReconcileResult {
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
        update_phase(ctx, migration, MigrationPhase::CheckingHealth).await?;
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
                    super::webhook::MigrationEventType::ReleaseDetected,
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
    update_phase(ctx, migration, MigrationPhase::CheckingHealth).await?;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Clear the expected-tag annotation from a DatabaseMigration
async fn clear_expected_tag_annotation(ctx: &Context, migration: &DatabaseMigration) {
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

/// Handle CheckingHealth phase - verify CNPG cluster is healthy
async fn handle_checking_health(migration: &DatabaseMigration, ctx: &Context) -> ReconcileResult {
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
        start_migration(migration, ctx).await?;

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
        update_phase(ctx, migration, MigrationPhase::WaitingForDatabase).await?;

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

        start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Handle WaitingForDatabase phase - wait for cluster to become healthy
async fn handle_waiting_for_database(
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

        start_migration(migration, ctx).await?;

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

/// Handle Migrating phase - check job status (non-blocking)
///
/// Uses the non-blocking `check_job_status` pattern instead of blocking
/// on job completion. This allows the reconciler to process other migrations
/// while jobs are running.
async fn handle_migrating(migration: &DatabaseMigration, ctx: &Context) -> ReconcileResult {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    metrics::set_migration_phase(&name, &namespace, "Migrating");

    // Get the current job name and start time from status
    let status = migration.status.as_ref();
    let job_name = match status.and_then(|s| s.current_job.clone()) {
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

    let job_started_at = status.and_then(|s| s.job_started_at);

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
    let image = if let Some(ref override_image) = current_migrator.image_override {
        override_image.clone()
    } else {
        ctx.get_deployment_image_cached(
            &namespace,
            &current_migrator.deployment_ref.name,
            current_migrator.deployment_ref.container_name.as_deref(),
        )
        .await?
    };
    let image_tag = extract_image_tag(&image);

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
        notify_migration_success(migration, ctx, &image_tag, result.duration.as_secs()).await;

        // Update status to Ready
        update_status_success(ctx, migration, &image_tag, result.duration).await?;

        return Ok(Action::requeue(ctx.config.requeue_interval));
    }

    // Handle failure case
    let error_msg = build_detailed_error_message(result);

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
        if let Some(mismatch) = detect_checksum_mismatch_from_job(ctx, &namespace, &job_name).await
        {
            if let Some(action) =
                handle_checksum_mismatch(migration, ctx, &mismatch, &job_name, &image_tag).await?
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
        notify_migration_failure(
            migration,
            ctx,
            &image_tag,
            &error_msg,
            migration.retry_count(),
            migration.spec.safety.max_retries,
            true,
        )
        .await;

        update_status_failed(ctx, migration, &image_tag, &error_msg).await?;

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
        increment_retry_count(ctx, migration).await?;
        start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Handle job timeout
async fn handle_job_timeout(migration: &DatabaseMigration, ctx: &Context) -> ReconcileResult {
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
        let image = if let Some(ref override_image) = current_migrator.image_override {
            override_image.clone()
        } else {
            ctx.get_deployment_image_cached(
                &namespace,
                &current_migrator.deployment_ref.name,
                current_migrator.deployment_ref.container_name.as_deref(),
            )
            .await?
        };
        let image_tag = extract_image_tag(&image);

        metrics::record_migration_failure(&name, &namespace, "timeout");
        update_status_failed(ctx, migration, &image_tag, "Migration timed out").await?;

        Ok(Action::requeue(Duration::from_secs(300)))
    } else {
        metrics::record_retry(&name, &namespace);
        increment_retry_count(ctx, migration).await?;
        start_migration(migration, ctx).await?;

        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

/// Handle Failed phase - comprehensive retry strategy
///
/// This implements smart migration recovery with multiple retry triggers:
///
/// 1. **Image change**: When deployment image changes, auto-retry (migrations are idempotent)
/// 2. **Manual retry annotation**: `shinka.pleme.io/retry=true` triggers immediate retry
/// 3. **Database recovery**: If DB was unhealthy and is now healthy, auto-retry
/// 4. **Cooldown retry**: After 1 hour in Failed state, check DB health and retry if healthy
/// 5. **Spec generation change**: If spec is updated (e.g., different timeout), auto-retry
///
/// These strategies work together - any condition that's satisfied will trigger a retry.
async fn handle_failed(migration: &DatabaseMigration, ctx: &Context) -> ReconcileResult {
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
    if primary_migrator.image_override.is_some() {
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
            let override_tag = extract_image_tag(primary_migrator.image_override.as_ref().unwrap());
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
                let override_tag = extract_image_tag(primary_migrator.image_override.as_ref().unwrap());
                metrics::record_auto_retry(&name, &namespace);
                reset_and_retry(ctx, migration, &override_tag).await?;
                return Ok(Action::requeue(Duration::from_secs(5)));
            }
        }

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Strategy 0: Check expected-tag annotation (release in progress)
    // This breaks the deadlock where nexus-deploy sets the annotation before
    // FluxCD updates the deployment image. Without this, Shinka would check the
    // deployment image (still old), see no change, and stay Failed.
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
            clear_expected_tag_annotation(ctx, migration).await;

            // Reset to Pending with the expected tag. The Pending handler will
            // then pick up the deployment image (which may or may not have the
            // expected tag yet — if not, it will fast-requeue until it does).
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
        // Get previous error from last migration status for Discord notification
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
    // After 1 hour in Failed state, check if database is healthy and retry
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
    // Requeue frequently to catch annotation changes and cooldown
    let requeue_secs = if failed_at.is_some() && !cooldown_elapsed {
        // If we have a failure time and cooldown hasn't elapsed, calculate time until cooldown
        let time_until_cooldown = failed_at
            .map(|t| {
                let elapsed = Utc::now().signed_duration_since(t);
                let remaining = 3600 - elapsed.num_seconds();
                remaining.max(60) as u64 // At least 1 minute, at most until cooldown
            })
            .unwrap_or(300);
        time_until_cooldown.min(300) // Cap at 5 minutes for responsiveness
    } else {
        300 // Default 5 minute requeue
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
) -> Result<()> {
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
async fn remove_retry_annotation(ctx: &Context, migration: &DatabaseMigration) -> Result<()> {
    let name = migration.name_or_default();
    let namespace = migration.namespace_or_default();

    let migrations: Api<DatabaseMigration> = Api::namespaced(ctx.client.clone(), &namespace);

    // Use Merge patch to set the annotation to null (removes it)
    // The annotation key with "/" needs to be in a nested structure for Merge patch
    let patch = Patch::Merge(json!({
        "metadata": {
            "annotations": {
                RETRY_ANNOTATION: serde_json::Value::Null
            }
        }
    }));

    match migrations.patch(&name, &PatchParams::default(), &patch).await {
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

/// Start a migration job
async fn start_migration(migration: &DatabaseMigration, ctx: &Context) -> Result<()> {
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
    let image = if let Some(ref override_image) = migrator_spec.image_override {
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

    let image_tag = extract_image_tag(&image);

    // Pre-flight validation for PreFlight mode
    let checksum_mode = &migration.spec.safety.checksum_mode;
    metrics::set_checksum_mode(&namespace, &name, &checksum_mode.to_string());

    if matches!(checksum_mode, ChecksumMode::PreFlight) {
        run_preflight_validation(migration, ctx).await?;
    }

    tracing::info!(
        event = "migration_starting",
        name = %name,
        namespace = %namespace,
        image = %image,
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
    // When imageOverride is set, the deployment may not exist yet, so we
    // gracefully fall back to default scheduling (get_deployment_scheduling
    // already handles missing deployments with a warning + default).
    let scheduling = get_deployment_scheduling(
        ctx.client.clone(),
        &namespace,
        &migrator_spec.deployment_ref.name,
    )
    .await;

    // Build the job using the configured migrator
    let config = MigrationJobConfig {
        image: image.clone(),
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
    let status = DatabaseMigrationStatus {
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

    let patch = Patch::Merge(json!({ "status": status }));
    migrations
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(Error::KubeApi)?;

    Ok(())
}

/// Update phase in status
async fn update_phase(
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
async fn update_status_success(
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
async fn update_status_failed(
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
async fn increment_retry_count(ctx: &Context, migration: &DatabaseMigration) -> Result<()> {
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
async fn reset_retry_count(ctx: &Context, migration: &DatabaseMigration) -> Result<()> {
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

/// Get the container image from a deployment
async fn get_deployment_image(
    client: Client,
    namespace: &str,
    deployment_name: &str,
    container_name: Option<&str>,
) -> Result<String> {
    let deployments: Api<Deployment> = Api::namespaced(client, namespace);

    let deployment = deployments.get(deployment_name).await.map_err(|e| {
        if matches!(
            e,
            kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })
        ) {
            Error::DeploymentNotFound {
                name: deployment_name.to_string(),
                namespace: namespace.to_string(),
            }
        } else {
            Error::KubeApi(e)
        }
    })?;

    let containers = deployment
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(|s| &s.containers)
        .ok_or_else(|| Error::NoContainerImage {
            name: deployment_name.to_string(),
        })?;

    let container = match container_name {
        Some(name) => containers.iter().find(|c| c.name == name),
        None => containers.first(),
    };

    container
        .and_then(|c| c.image.clone())
        .ok_or_else(|| Error::NoContainerImage {
            name: deployment_name.to_string(),
        })
}

/// Get pod scheduling constraints (tolerations, nodeSelector, affinity) from a deployment.
/// Migration jobs must run on the same node(s) as the application to ensure
/// network connectivity to the database and other dependencies.
async fn get_deployment_scheduling(
    client: Client,
    namespace: &str,
    deployment_name: &str,
) -> PodScheduling {
    let deployments: Api<Deployment> = Api::namespaced(client, namespace);

    match deployments.get(deployment_name).await {
        Ok(deployment) => {
            let pod_spec = deployment
                .spec
                .as_ref()
                .and_then(|s| s.template.spec.as_ref());

            match pod_spec {
                Some(spec) => PodScheduling {
                    node_selector: spec.node_selector.clone(),
                    tolerations: spec.tolerations.clone(),
                    affinity: spec.affinity.clone(),
                },
                None => PodScheduling::default(),
            }
        }
        Err(e) => {
            tracing::warn!(
                deployment = %deployment_name,
                namespace = %namespace,
                error = %e,
                "Failed to get deployment scheduling constraints, using defaults"
            );
            PodScheduling::default()
        }
    }
}

/// Extract tag from image string (e.g., "image:tag" -> "tag")
fn extract_image_tag(image: &str) -> String {
    if let Some(at_pos) = image.rfind('@') {
        // Image uses digest: image@sha256:abc123
        image[at_pos + 1..].to_string()
    } else if let Some(colon_pos) = image.rfind(':') {
        // Image uses tag: image:tag
        // But need to handle port numbers like registry:5000/image:tag
        let after_colon = &image[colon_pos + 1..];
        if after_colon.contains('/') {
            // This colon was a port, no tag
            "latest".to_string()
        } else {
            after_colon.to_string()
        }
    } else {
        "latest".to_string()
    }
}

/// Detect checksum mismatch from job logs
///
/// Parses the pod logs for a migration job to detect VersionMismatch errors.
/// Returns the version number if a mismatch is detected.
async fn detect_checksum_mismatch_from_job(
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

// =============================================================================
// EXTRACTED HELPER FUNCTIONS
// =============================================================================
// These functions are extracted from larger functions to improve readability
// and testability. Each handles a distinct concern.
// =============================================================================

/// Build a detailed error message from job result
///
/// Combines the job condition error with pod logs error for comprehensive debugging.
fn build_detailed_error_message(result: &JobResult) -> String {
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

/// Run pre-flight validation before starting migration
///
/// Validates all migration checksums against the database to catch modified
/// migrations before they run. Only runs when checksum_mode is PreFlight.
async fn run_preflight_validation(
    migration: &DatabaseMigration,
    ctx: &Context,
) -> Result<()> {
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

/// Send success notifications to all configured channels
///
/// Notifies Release Tracker, Discord, and Kubernetes events about successful migration.
async fn notify_migration_success(
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
async fn notify_migration_failure(
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

/// Handle checksum mismatch based on configured mode
///
/// Returns Ok(Some(action)) if the mismatch was handled and we should return early,
/// Ok(None) if we should continue with normal retry logic,
/// Err if an error occurred.
async fn handle_checksum_mismatch(
    migration: &DatabaseMigration,
    ctx: &Context,
    mismatch: &VersionMismatchInfo,
    job_name: &str,
    image_tag: &str,
) -> Result<Option<Action>> {
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
            update_status_failed(ctx, migration, image_tag, &error_msg).await?;

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
                    reset_retry_count(ctx, migration).await?;
                    start_migration(migration, ctx).await?;

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
            update_status_failed(ctx, migration, image_tag, &error_msg).await?;

            Ok(Some(Action::requeue(Duration::from_secs(300))))
        }
    }
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
