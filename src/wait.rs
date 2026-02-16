//! Wait mode - Init container for waiting on database migrations
//!
//! When `RUN_MODE=wait`, Shinka acts as an init container that polls
//! the Shinka API until migrations are confirmed complete.
//!
//! # Environment Variables
//!
//! - `SHINKA_URL` - Shinka service URL (default: http://shinka.shinka-system.svc.cluster.local:8080)
//! - `MIGRATION_NAME` - Name of the DatabaseMigration resource to wait for
//! - `MIGRATION_NAMESPACE` - Namespace of the DatabaseMigration
//! - `TIMEOUT_SECONDS` - Maximum time to wait (default: 300)
//! - `RETRY_INTERVAL_SECONDS` - Time between retries (default: 5)
//!
//! # Exit Codes
//!
//! - 0: Migrations complete, safe to start application
//! - 1: Timeout waiting for migrations
//! - 2: Configuration error
//! - 3: API error (Shinka unreachable)

use serde::Deserialize;
use std::env;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// API response wrapper matching Shinka's REST API format
#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    data: T,
}

/// Migration resource from the API
#[derive(Debug, Deserialize)]
struct MigrationResource {
    status: MigrationStatus,
}

/// Status summary from the API
#[derive(Debug, Deserialize)]
struct MigrationStatus {
    phase: String,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    current_job: Option<String>,
}

/// Configuration for wait mode
struct WaitConfig {
    shinka_url: String,
    migration_name: String,
    migration_namespace: String,
    timeout: Duration,
    retry_interval: Duration,
}

impl WaitConfig {
    fn from_env() -> Result<Self, String> {
        let shinka_url = env::var("SHINKA_URL")
            .unwrap_or_else(|_| "http://shinka.shinka-system.svc.cluster.local:8080".to_string());

        let migration_name = env::var("MIGRATION_NAME")
            .map_err(|_| "MIGRATION_NAME environment variable is required")?;

        let migration_namespace = env::var("MIGRATION_NAMESPACE")
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

        Ok(Self {
            shinka_url,
            migration_name,
            migration_namespace,
            timeout: Duration::from_secs(timeout_secs),
            retry_interval: Duration::from_secs(retry_interval_secs),
        })
    }
}

/// Check if the migration is in Ready phase
async fn check_migration_ready(
    client: &reqwest::Client,
    config: &WaitConfig,
) -> Result<bool, reqwest::Error> {
    let url = format!(
        "{}/api/v1/namespaces/{}/migrations/{}",
        config.shinka_url, config.migration_namespace, config.migration_name
    );

    let response = client
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        return Ok(false);
    }

    // Parse the API response (wrapped in {"data": {...}})
    let api_response: ApiResponse<MigrationResource> = response.json().await?;
    let status = &api_response.data.status;

    info!(
        phase = %status.phase,
        retry_count = status.retry_count,
        current_job = ?status.current_job,
        "Migration status"
    );

    Ok(status.phase == "Ready")
}

/// Run wait mode - returns exit code
pub async fn run() -> i32 {
    // Initialize logging
    let filter = EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    info!("Shinka wait mode: Waiting for database migrations to complete");

    // Parse configuration
    let config = match WaitConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Configuration error");
            return 2;
        }
    };

    info!(
        migration = %config.migration_name,
        namespace = %config.migration_namespace,
        shinka_url = %config.shinka_url,
        timeout_secs = config.timeout.as_secs(),
        retry_interval_secs = config.retry_interval.as_secs(),
        "Configuration loaded"
    );

    // Create HTTP client
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to create HTTP client");
            return 3;
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
                "Timeout waiting for migrations to complete"
            );
            return 1;
        }

        match check_migration_ready(&client, &config).await {
            Ok(true) => {
                info!(
                    attempts = attempts,
                    elapsed_secs = elapsed.as_secs(),
                    "Migrations complete! Application can start."
                );
                return 0;
            }
            Ok(false) => {
                info!(
                    attempts = attempts,
                    elapsed_secs = elapsed.as_secs(),
                    remaining_secs = (config.timeout - elapsed).as_secs(),
                    "Migrations not ready yet, waiting..."
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    attempts = attempts,
                    "Failed to check migration status (Shinka may be starting)"
                );
            }
        }

        sleep(config.retry_interval).await;
    }
}
