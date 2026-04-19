//! Shinka (進化) - GitOps-native database migration operator for Kubernetes
//!
//! A Rust-based Kubernetes operator that automatically runs database migrations
//! when deployment images change, integrated with CNPG health checks and full observability.
//!
//! # Run Modes
//!
//! Set `RUN_MODE` environment variable:
//! - `operator` (default): Run as Kubernetes operator watching DatabaseMigration CRDs
//! - `wait`: Run as init container, waiting for migrations to complete before exiting

mod wait;

use shinka::{
    config::{Config, LogFormat},
    controller::{
        error_policy, reconcile, Context, OperatorEventType, OptionalDiscordClient,
        OptionalEventRecorder, OptionalReleaseTrackerClient,
    },
    crd::DatabaseMigration,
    health::{self, HealthState},
    leader::{LeaderElectionConfig, LeaderElector},
    metrics,
    webhook::{self, WebhookConfig},
};

use futures::StreamExt;
use kube::{
    runtime::{controller::Controller, watcher::Config as WatcherConfig},
    Api, Client,
};
use std::{env, sync::Arc};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check RUN_MODE first - determines which mode to run
    let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "operator".to_string());

    match run_mode.as_str() {
        "wait" => {
            // Run as init container - wait for migrations then exit
            let exit_code = wait::run().await;
            std::process::exit(exit_code);
        }
        _ => {
            // Run as operator (default)
            run_operator().await
        }
    }
}

async fn run_operator() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment
    let config = Config::from_env();
    let leader_config = LeaderElectionConfig::from_env();
    let webhook_config = WebhookConfig::from_env();

    // Initialize tracing (structured JSON logging)
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{},shinka=debug", config.log_level)));

    match config.log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(fmt::layer().json())
                .with(filter)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(fmt::layer().pretty())
                .with(filter)
                .init();
        }
    }

    // Get version from environment or build-time
    let version = option_env!("GIT_SHA").unwrap_or(env!("CARGO_PKG_VERSION"));
    tracing::info!(
        version = %version,
        "Starting Shinka operator"
    );
    tracing::info!("GitOps-native database migration operator for Kubernetes");

    // Initialize Prometheus metrics
    metrics::init();
    tracing::info!("Metrics initialized");

    // Log configuration
    tracing::info!(
        health_addr = %config.health_addr,
        metrics_port = %config.metrics_port,
        watch_namespace = ?config.watch_namespace,
        default_timeout_secs = %config.default_migration_timeout.as_secs(),
        default_max_retries = %config.default_max_retries,
        leader_election_enabled = %leader_config.enabled,
        leader_identity = %leader_config.identity,
        "Configuration loaded"
    );

    // Create Kubernetes client
    let client = Client::try_default().await?;
    tracing::info!("Connected to Kubernetes API");

    // Create Release Tracker webhook client (if configured)
    let webhook_client = OptionalReleaseTrackerClient::from_url(
        config.release_tracker_url.clone(),
        config.webhook_timeout,
    );
    if webhook_client.is_configured() {
        tracing::info!(
            url = %config.release_tracker_url.as_deref().unwrap_or(""),
            "Release Tracker webhook notifications enabled"
        );
    }

    // Create Discord webhook client (if configured)
    let discord_client =
        OptionalDiscordClient::from_config(config.discord.clone(), config.webhook_timeout);
    if discord_client.is_configured() {
        tracing::info!(
            cluster = %config.discord.cluster_name,
            environment = %config.discord.environment,
            "Discord webhook notifications enabled"
        );
    }

    // Create Kubernetes event recorder
    let event_recorder = OptionalEventRecorder::new(client.clone());
    tracing::info!("Kubernetes event recorder enabled");

    // Create shared context for reconciler
    let context = Arc::new(Context::new(
        client.clone(),
        config.clone(),
        webhook_client,
        discord_client.clone(),
        event_recorder,
    ));

    // Send startup announcement to Discord
    discord_client
        .notify_operator_event(OperatorEventType::Started, version)
        .await;

    // Create health state with GraphQL API enabled by default
    let health_state = HealthState::with_api(client.clone());
    tracing::info!("GraphQL API is the default protocol");

    // Start health/metrics HTTP server
    let health_addr = config.health_addr.clone();
    let health_state_clone = health_state.clone();
    let health_server = tokio::spawn(async move {
        if let Err(e) = health::serve(&health_addr, health_state_clone).await {
            tracing::error!(error = %e, "Health server error");
        }
    });

    // Start webhook server (if enabled)
    let webhook_client = client.clone();
    let webhook_task = tokio::spawn(async move {
        if let Err(e) = webhook::serve(webhook_config, webhook_client).await {
            tracing::error!(error = %e, "Webhook server error");
        }
    });

    // Create API handle for DatabaseMigration resources
    let migrations: Api<DatabaseMigration> = match &config.watch_namespace {
        Some(ns) => {
            tracing::info!(namespace = %ns, "Watching namespace");
            Api::namespaced(client.clone(), ns)
        }
        None => {
            tracing::info!("Watching all namespaces");
            Api::all(client.clone())
        }
    };

    // Set up leader election
    let leader_elector = LeaderElector::new(client.clone(), leader_config.clone());
    let leader_handle = leader_elector.leader_handle();

    // Start leader election in background
    let leader_task = tokio::spawn({
        let elector = leader_elector;
        async move {
            if let Err(e) = elector.run().await {
                tracing::error!(error = %e, "Leader election error");
            }
        }
    });

    // Wait for leadership if leader election is enabled
    if leader_config.enabled {
        tracing::info!("Waiting for leadership...");
        while !leader_handle.is_leader() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        tracing::info!("Acquired leadership, starting controller");
    }

    tracing::info!("Starting controller loop");

    // Mark as ready
    health_state.set_ready(true).await;

    // Run the controller with graceful shutdown
    // Only process reconciliations when we're the leader
    let leader_handle_clone = leader_handle.clone();
    let controller = Controller::new(migrations, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(move |result| {
            let leader_handle = leader_handle_clone.clone();
            async move {
                // Skip reconciliation if we lost leadership
                if leader_config.enabled && !leader_handle.is_leader() {
                    tracing::debug!("Skipping reconciliation - not leader");
                    return;
                }

                match result {
                    Ok((obj, _action)) => {
                        tracing::debug!(
                            namespace = obj.namespace.as_deref().unwrap_or("default"),
                            name = %obj.name,
                            "Reconciliation successful"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            error = ?err,
                            "Reconciliation error"
                        );
                    }
                }
            }
        });

    // Wait for drain signal or controller end, whichever comes first.
    // tsunagu installs SIGTERM/SIGINT handlers once and hands out tokens.
    let drain = tsunagu::ShutdownController::install();
    tokio::select! {
        _ = controller => {
            tracing::info!("Controller loop ended");
        }
        _ = drain.token().wait() => {
            tracing::info!("Drain signal received from tsunagu");
        }
    }

    // Mark as not ready during shutdown
    health_state.set_ready(false).await;

    // Send shutdown announcement to Discord
    discord_client
        .notify_operator_event(OperatorEventType::ShuttingDown, version)
        .await;

    tracing::info!("Shinka operator shutting down gracefully");

    // Give health checks time to fail before terminating
    tokio::time::sleep(config.graceful_shutdown_delay).await;

    // Abort background tasks
    health_server.abort();
    leader_task.abort();
    webhook_task.abort();

    tracing::info!("Shutdown complete");

    Ok(())
}

