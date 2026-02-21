//! shinka-wait - Init container for waiting on database migrations
//!
//! This binary is designed to be used as a Kubernetes init container.
//! It polls the Shinka API until migrations are confirmed complete,
//! ensuring the application only starts after the database schema is ready.
//!
//! # Check Modes
//!
//! Two check modes are available:
//!
//! 1. **Migration Mode** (default): Wait for a specific DatabaseMigration CR
//! 2. **Database Mode**: Wait for cluster health + all migrations complete
//!
//! # Environment Variables
//!
//! **Common:**
//! - `SHINKA_URL` - Shinka service URL (default: http://shinka.shinka-system.svc.cluster.local:8080)
//! - `NAMESPACE` or `MIGRATION_NAMESPACE` - Namespace to check (required)
//! - `TIMEOUT_SECONDS` - Maximum time to wait (default: 300)
//! - `RETRY_INTERVAL_SECONDS` - Time between retries (default: 5)
//! - `LOG_LEVEL` - Logging verbosity (default: info)
//! - `CHECK_MODE` - `migration` (default) or `database`
//!
//! **Migration Mode:**
//! - `MIGRATION_NAME` - Name of the DatabaseMigration resource to wait for (required)
//!
//! **Database Mode:**
//! - `CLUSTER_NAME` - Name of the CNPG cluster to check (required)
//! - `DATABASE` - Specific database to check (optional, checks all if not set)
//!
//! # Exit Codes
//!
//! - 0: Ready, safe to start application
//! - 1: Timeout waiting for readiness
//! - 2: Configuration error
//! - 3: API error (Shinka unreachable)

use serde::Deserialize;
use std::env;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Check mode - which endpoint to use for readiness checks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckMode {
    /// Check a specific migration by name
    Migration,
    /// Check database cluster health + all migrations
    Database,
}

impl CheckMode {
    fn from_env() -> Self {
        match env::var("CHECK_MODE").as_deref() {
            Ok("database") => Self::Database,
            _ => Self::Migration,
        }
    }
}

/// Response from Shinka's database ready endpoint
#[derive(Debug, Deserialize)]
struct DatabaseReadyResponse {
    ready: bool,
    cluster_healthy: bool,
    migrations_complete: bool,
    message: String,
    #[serde(default)]
    pending_migrations: u32,
    #[serde(default)]
    active_migrations: u32,
}

/// Response from Shinka's migration status endpoint
#[derive(Debug, Deserialize)]
struct MigrationStatus {
    phase: String,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    current_job: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Migration {
    status: Option<MigrationStatus>,
}

/// Configuration parsed from environment
struct Config {
    shinka_url: String,
    namespace: String,
    timeout: Duration,
    retry_interval: Duration,
    check_mode: CheckMode,
    /// Migration name (required for migration mode)
    migration_name: Option<String>,
    /// Cluster name (required for database mode)
    cluster_name: Option<String>,
    /// Database name (optional for database mode)
    database: Option<String>,
    /// Per-request HTTP timeout
    http_request_timeout: Duration,
    /// Global HTTP client timeout
    http_client_timeout: Duration,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let shinka_url = env::var("SHINKA_URL")
            .unwrap_or_else(|_| "http://shinka.shinka-system.svc.cluster.local:8080".to_string());

        let namespace = env::var("MIGRATION_NAMESPACE")
            .or_else(|_| env::var("NAMESPACE"))
            .map_err(|_| "MIGRATION_NAMESPACE or NAMESPACE environment variable is required")?;

        let timeout_secs: u64 = env::var("TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "300".to_string())
            .parse()
            .map_err(|_| "TIMEOUT_SECONDS must be a valid number")?;

        let retry_interval_secs: u64 = env::var("RETRY_INTERVAL_SECONDS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .map_err(|_| "RETRY_INTERVAL_SECONDS must be a valid number")?;

        let check_mode = CheckMode::from_env();

        // Validate mode-specific config
        let migration_name = env::var("MIGRATION_NAME").ok();
        let cluster_name = env::var("CLUSTER_NAME").ok();
        let database = env::var("DATABASE").ok();

        match check_mode {
            CheckMode::Migration if migration_name.is_none() => {
                return Err(
                    "MIGRATION_NAME is required when CHECK_MODE=migration (default)".to_string(),
                );
            }
            CheckMode::Database if cluster_name.is_none() => {
                return Err("CLUSTER_NAME is required when CHECK_MODE=database".to_string());
            }
            _ => {}
        }

        let http_request_timeout_secs: u64 = env::var("HTTP_REQUEST_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .map_err(|_| "HTTP_REQUEST_TIMEOUT_SECONDS must be a valid number")?;

        let http_client_timeout_secs: u64 = env::var("HTTP_CLIENT_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .map_err(|_| "HTTP_CLIENT_TIMEOUT_SECONDS must be a valid number")?;

        Ok(Self {
            shinka_url,
            namespace,
            timeout: Duration::from_secs(timeout_secs),
            retry_interval: Duration::from_secs(retry_interval_secs),
            check_mode,
            migration_name,
            cluster_name,
            database,
            http_request_timeout: Duration::from_secs(http_request_timeout_secs),
            http_client_timeout: Duration::from_secs(http_client_timeout_secs),
        })
    }
}

/// Check if a specific migration is in Ready phase
///
/// # Safety
/// `config.migration_name` is validated as `Some` by `Config::from_env()` when
/// `check_mode` is `Migration`. This function is only called in that mode.
async fn check_migration_ready(
    client: &reqwest::Client,
    config: &Config,
) -> Result<bool, reqwest::Error> {
    let Some(migration_name) = config.migration_name.as_ref() else {
        // Should never happen due to Config::from_env() validation
        error!("migration_name missing despite validation");
        return Ok(false);
    };
    let url = format!(
        "{}/api/v1/namespaces/{}/migrations/{}",
        config.shinka_url, config.namespace, migration_name
    );

    let response = client
        .get(&url)
        .timeout(config.http_request_timeout)
        .send()
        .await?;

    if !response.status().is_success() {
        // Migration might not exist yet, or Shinka is still starting
        return Ok(false);
    }

    let migration: Migration = response.json().await?;

    match migration.status {
        Some(status) => {
            info!(
                phase = %status.phase,
                retry_count = status.retry_count,
                current_job = ?status.current_job,
                "Migration status"
            );
            Ok(status.phase == "Ready")
        }
        None => Ok(false),
    }
}

/// Check database cluster health + all migrations complete
///
/// This is a more thorough check that ensures:
/// 1. The CNPG cluster is healthy (primary + replicas)
/// 2. All migrations for the database are complete
/// 3. No migrations are currently running or pending
///
/// # Safety
/// `config.cluster_name` is validated as `Some` by `Config::from_env()` when
/// `check_mode` is `Database`. This function is only called in that mode.
async fn check_database_ready(
    client: &reqwest::Client,
    config: &Config,
) -> Result<bool, reqwest::Error> {
    let Some(cluster_name) = config.cluster_name.as_ref() else {
        // Should never happen due to Config::from_env() validation
        error!("cluster_name missing despite validation");
        return Ok(false);
    };
    let mut url = format!(
        "{}/api/v1/namespaces/{}/clusters/{}/ready",
        config.shinka_url, config.namespace, cluster_name
    );

    if let Some(db) = &config.database {
        url.push_str(&format!("?database={}", db));
    }

    let response = client
        .get(&url)
        .timeout(config.http_request_timeout)
        .send()
        .await?;

    if !response.status().is_success() {
        warn!(
            status = %response.status(),
            "Database readiness check returned non-success status"
        );
        return Ok(false);
    }

    let ready: DatabaseReadyResponse = response.json().await?;

    info!(
        ready = ready.ready,
        cluster_healthy = ready.cluster_healthy,
        migrations_complete = ready.migrations_complete,
        pending = ready.pending_migrations,
        active = ready.active_migrations,
        message = %ready.message,
        "Database readiness check"
    );

    // All conditions must be met
    if !ready.cluster_healthy {
        warn!("Database cluster is not healthy");
    }
    if !ready.migrations_complete {
        warn!(
            pending = ready.pending_migrations,
            active = ready.active_migrations,
            "Migrations not complete"
        );
    }

    Ok(ready.ready)
}

/// Perform the appropriate readiness check based on config
async fn check_ready(client: &reqwest::Client, config: &Config) -> Result<bool, reqwest::Error> {
    match config.check_mode {
        CheckMode::Migration => check_migration_ready(client, config).await,
        CheckMode::Database => check_database_ready(client, config).await,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    // Initialize logging
    let filter = EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    info!("shinka-wait: Waiting for database migrations to complete");

    // Parse configuration
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Configuration error");
            return ExitCode::from(2);
        }
    };

    // Log configuration based on mode
    // Note: validation in Config::from_env() guarantees these are Some for their respective modes
    match config.check_mode {
        CheckMode::Migration => {
            let migration = config.migration_name.as_deref().unwrap_or("<unknown>");
            info!(
                mode = "migration",
                migration = %migration,
                namespace = %config.namespace,
                shinka_url = %config.shinka_url,
                timeout_secs = config.timeout.as_secs(),
                retry_interval_secs = config.retry_interval.as_secs(),
                "Configuration loaded"
            );
        }
        CheckMode::Database => {
            let cluster = config.cluster_name.as_deref().unwrap_or("<unknown>");
            info!(
                mode = "database",
                cluster = %cluster,
                database = ?config.database,
                namespace = %config.namespace,
                shinka_url = %config.shinka_url,
                timeout_secs = config.timeout.as_secs(),
                retry_interval_secs = config.retry_interval.as_secs(),
                "Configuration loaded"
            );
        }
    }

    // Create HTTP client
    let client = match reqwest::Client::builder()
        .timeout(config.http_client_timeout)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to create HTTP client");
            return ExitCode::from(3);
        }
    };

    let start = Instant::now();
    let mut attempts = 0u32;

    // Poll until ready or timeout
    loop {
        attempts += 1;
        let elapsed = start.elapsed();

        if elapsed >= config.timeout {
            error!(
                attempts = attempts,
                elapsed_secs = elapsed.as_secs(),
                timeout_secs = config.timeout.as_secs(),
                "Timeout waiting for database to be ready"
            );
            return ExitCode::from(1);
        }

        match check_ready(&client, &config).await {
            Ok(true) => {
                info!(
                    attempts = attempts,
                    elapsed_secs = elapsed.as_secs(),
                    "Database ready! Application can start."
                );
                return ExitCode::SUCCESS;
            }
            Ok(false) => {
                info!(
                    attempts = attempts,
                    elapsed_secs = elapsed.as_secs(),
                    remaining_secs = (config.timeout - elapsed).as_secs(),
                    "Not ready yet, waiting..."
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    attempts = attempts,
                    "Failed to check status (Shinka may be starting)"
                );
            }
        }

        sleep(config.retry_interval).await;
    }
}
