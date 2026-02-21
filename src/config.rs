//! Configuration for Shinka operator
//!
//! Configuration can be loaded from:
//! 1. Environment variables (twelve-factor app methodology)
//! 2. YAML configuration file (for complex migrator defaults)
//!
//! YAML config takes precedence for migrator defaults, env vars for runtime settings.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use crate::controller::discord::DiscordConfig;
use crate::migrator::{MigratorConfig, MigratorType, ToolSpecificConfig};
use crate::Result;

/// Operator configuration loaded from environment and optional YAML file
#[derive(Debug, Clone)]
pub struct Config {
    /// Namespace to watch (empty = all namespaces)
    pub watch_namespace: Option<String>,

    /// Health server listen address
    pub health_addr: String,

    /// Metrics server port
    pub metrics_port: u16,

    /// Default migration timeout
    pub default_migration_timeout: Duration,

    /// Default max retries for migrations
    pub default_max_retries: u32,

    /// Reconciliation requeue interval when idle
    pub requeue_interval: Duration,

    /// Log level
    pub log_level: String,

    /// Log format (json or pretty)
    pub log_format: LogFormat,

    /// Path to YAML config file (optional)
    pub config_file: Option<String>,

    /// Parallelization settings for concurrent migrations
    pub parallelization: ParallelizationConfig,

    /// Migrator configurations loaded from YAML
    pub migrator_defaults: MigratorDefaults,

    /// Release Tracker service URL for webhook notifications (optional)
    /// When set, Shinka sends migration lifecycle events (started, succeeded, failed, auto-retried)
    /// to the Release Tracker for observability and coordination.
    pub release_tracker_url: Option<String>,

    /// Webhook HTTP client timeout
    pub webhook_timeout: Duration,

    /// Database connection acquire timeout (for checksum reconciliation)
    pub database_connect_timeout: Duration,

    /// Graceful shutdown delay
    pub graceful_shutdown_delay: Duration,

    /// Discord webhook configuration for beautiful notifications
    pub discord: DiscordConfig,

    /// TTL in seconds for completed migration Jobs (Kubernetes garbage collection)
    /// Set to 0 to disable automatic cleanup (manual deletion required)
    pub job_ttl_seconds: i32,

    /// Whether to inject an annotation disabling service mesh sidecar on migration pods
    /// Prevents race conditions where migration starts before mesh proxy is ready
    pub disable_mesh_sidecar: bool,

    /// Annotation key for disabling mesh sidecar injection (e.g., "sidecar.istio.io/inject")
    pub mesh_sidecar_annotation_key: String,

    /// Annotation value for disabling mesh sidecar injection (e.g., "false")
    pub mesh_sidecar_annotation_value: String,
}

/// Configuration for parallel migration execution
///
/// Shinka automatically analyzes the environment to determine:
/// - Which migrations can run in parallel (different databases)
/// - Which migrations must run serially (same database)
/// - Optimal concurrency based on cluster resources
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParallelizationConfig {
    /// Enable parallel migrations for independent databases
    #[serde(default = "crate::util::default_true")]
    pub enabled: bool,

    /// Maximum concurrent migrations across all databases
    /// Set to 1 to disable parallelization entirely
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,

    /// Maximum concurrent migrations per database
    /// Migrations to the same database are always serialized
    #[serde(default = "default_max_per_database")]
    pub max_per_database: u32,

    /// Queue strategy for pending migrations
    #[serde(default)]
    pub queue_strategy: QueueStrategy,

    /// Priority weights by namespace (higher = more priority)
    #[serde(default)]
    pub namespace_priorities: BTreeMap<String, u32>,

    /// Enable automatic concurrency adjustment based on load
    #[serde(default)]
    pub auto_scale: bool,
}

fn default_max_concurrent() -> u32 {
    10
}

fn default_max_per_database() -> u32 {
    1
}

/// Strategy for ordering pending migrations
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueStrategy {
    /// First-in, first-out ordering
    #[default]
    Fifo,

    /// Priority-based ordering (by namespace or explicit priority)
    Priority,

    /// Fair scheduling across namespaces
    Fair,

    /// Shortest migration first (based on historical data)
    ShortestFirst,
}

impl Default for ParallelizationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent: 10,
            max_per_database: 1, // Serialize same-database migrations
            queue_strategy: QueueStrategy::Fair,
            namespace_priorities: BTreeMap::new(),
            auto_scale: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

/// Default configurations for each migrator type
///
/// Loaded from YAML configuration file, allows customizing
/// defaults per-migrator-type without modifying CRDs.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigratorDefaults {
    /// Default migrator type if not specified in CRD
    #[serde(default)]
    pub default_type: MigratorType,

    /// Per-type default configurations
    #[serde(default)]
    pub types: BTreeMap<String, MigratorConfig>,

    /// Global environment variables applied to all migrators
    #[serde(default)]
    pub global_env: BTreeMap<String, String>,

    /// Default resource requests/limits
    #[serde(default)]
    pub default_resources: Option<ResourceDefaults>,
}

/// Default resource requirements for migration jobs
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDefaults {
    pub memory_limit: Option<String>,
    pub cpu_limit: Option<String>,
    pub memory_request: Option<String>,
    pub cpu_request: Option<String>,
}

/// YAML configuration file structure
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct YamlConfig {
    /// API version for future compatibility
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Operator settings
    #[serde(default)]
    pub operator: OperatorSettings,

    /// Parallelization settings
    #[serde(default)]
    pub parallelization: ParallelizationConfig,

    /// Migrator defaults
    #[serde(default)]
    pub migrators: MigratorDefaults,
}

fn default_api_version() -> String {
    "shinka.pleme.io/v1alpha1".to_string()
}

/// Operator-level settings from YAML
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorSettings {
    /// Watch namespace override
    pub watch_namespace: Option<String>,

    /// Health server address override
    pub health_addr: Option<String>,

    /// Metrics port override
    pub metrics_port: Option<u16>,

    /// Default migration timeout in seconds
    pub default_migration_timeout_seconds: Option<u64>,

    /// Default max retries
    pub default_max_retries: Option<u32>,

    /// Requeue interval in seconds
    pub requeue_interval_seconds: Option<u64>,

    /// Log level
    pub log_level: Option<String>,

    /// Log format
    pub log_format: Option<String>,

    /// Release Tracker URL for webhook notifications
    pub release_tracker_url: Option<String>,

    /// Webhook HTTP client timeout in seconds
    pub webhook_timeout_seconds: Option<u64>,

    /// Database connection acquire timeout in seconds
    pub database_connect_timeout_seconds: Option<u64>,

    /// Graceful shutdown delay in seconds
    pub graceful_shutdown_delay_seconds: Option<u64>,

    /// Discord webhook configuration
    #[serde(default)]
    pub discord: DiscordConfig,

    /// TTL in seconds for completed migration Jobs
    pub job_ttl_seconds: Option<i32>,

    /// Whether to inject mesh sidecar disable annotation
    pub disable_mesh_sidecar: Option<bool>,

    /// Annotation key for disabling mesh sidecar
    pub mesh_sidecar_annotation_key: Option<String>,

    /// Annotation value for disabling mesh sidecar
    pub mesh_sidecar_annotation_value: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            watch_namespace: None,
            health_addr: "0.0.0.0:8080".to_string(),
            metrics_port: 9090,
            default_migration_timeout: Duration::from_secs(300), // 5 minutes
            default_max_retries: 3,
            requeue_interval: Duration::from_secs(60),
            log_level: "info".to_string(),
            log_format: LogFormat::Json,
            config_file: None,
            parallelization: ParallelizationConfig::default(),
            migrator_defaults: MigratorDefaults::default(),
            release_tracker_url: None,
            webhook_timeout: Duration::from_secs(5),
            database_connect_timeout: Duration::from_secs(10),
            graceful_shutdown_delay: Duration::from_secs(2),
            discord: DiscordConfig::default(),
            job_ttl_seconds: 3600,
            disable_mesh_sidecar: true,
            mesh_sidecar_annotation_key: "sidecar.istio.io/inject".to_string(),
            mesh_sidecar_annotation_value: "false".to_string(),
        }
    }
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(ns) = std::env::var("WATCH_NAMESPACE") {
            if !ns.is_empty() {
                config.watch_namespace = Some(ns);
            }
        }

        if let Ok(addr) = std::env::var("HEALTH_ADDR") {
            config.health_addr = addr;
        }

        if let Ok(port) = std::env::var("METRICS_PORT") {
            if let Ok(p) = port.parse() {
                config.metrics_port = p;
            }
        }

        if let Ok(timeout) = std::env::var("DEFAULT_MIGRATION_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.default_migration_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(retries) = std::env::var("DEFAULT_MAX_RETRIES") {
            if let Ok(r) = retries.parse() {
                config.default_max_retries = r;
            }
        }

        if let Ok(interval) = std::env::var("REQUEUE_INTERVAL") {
            if let Ok(secs) = interval.parse::<u64>() {
                config.requeue_interval = Duration::from_secs(secs);
            }
        }

        if let Ok(level) = std::env::var("LOG_LEVEL") {
            config.log_level = level;
        }

        if let Ok(format) = std::env::var("LOG_FORMAT") {
            config.log_format = match format.to_lowercase().as_str() {
                "pretty" => LogFormat::Pretty,
                _ => LogFormat::Json,
            };
        }

        // Job TTL for completed migrations
        if let Ok(ttl) = std::env::var("JOB_TTL_SECONDS") {
            if let Ok(t) = ttl.parse::<i32>() {
                config.job_ttl_seconds = t;
            }
        }

        // Mesh sidecar injection control
        if let Ok(val) = std::env::var("DISABLE_MESH_SIDECAR") {
            config.disable_mesh_sidecar = val == "true" || val == "1";
        }
        if let Ok(key) = std::env::var("MESH_SIDECAR_ANNOTATION_KEY") {
            config.mesh_sidecar_annotation_key = key;
        }
        if let Ok(val) = std::env::var("MESH_SIDECAR_ANNOTATION_VALUE") {
            config.mesh_sidecar_annotation_value = val;
        }

        // Check for YAML config file
        if let Ok(path) = std::env::var("SHINKA_CONFIG") {
            config.config_file = Some(path);
        }

        // Release Tracker URL for webhook notifications
        if let Ok(url) = std::env::var("RELEASE_TRACKER_URL") {
            if !url.is_empty() {
                config.release_tracker_url = Some(url);
            }
        }

        // Webhook HTTP client timeout
        if let Ok(timeout) = std::env::var("WEBHOOK_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.webhook_timeout = Duration::from_secs(secs);
            }
        }

        // Database connection timeout
        if let Ok(timeout) = std::env::var("DATABASE_CONNECT_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.database_connect_timeout = Duration::from_secs(secs);
            }
        }

        // Graceful shutdown delay
        if let Ok(delay) = std::env::var("GRACEFUL_SHUTDOWN_DELAY") {
            if let Ok(secs) = delay.parse::<u64>() {
                config.graceful_shutdown_delay = Duration::from_secs(secs);
            }
        }

        // Discord webhook configuration
        if let Ok(url) = std::env::var("DISCORD_WEBHOOK_URL") {
            if !url.is_empty() {
                config.discord.webhook_url = Some(url);
            }
        }
        if let Ok(username) = std::env::var("DISCORD_USERNAME") {
            config.discord.username = username;
        }
        if let Ok(avatar) = std::env::var("DISCORD_AVATAR_URL") {
            config.discord.avatar_url = Some(avatar);
        }
        if let Ok(cluster) = std::env::var("DISCORD_CLUSTER_NAME") {
            config.discord.cluster_name = cluster;
        }
        if let Ok(env) = std::env::var("DISCORD_ENVIRONMENT") {
            config.discord.environment = env;
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_SUCCESS") {
            config.discord.notify_on_success = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_FAILURE") {
            config.discord.notify_on_failure = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_STARTED") {
            config.discord.notify_on_started = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_RETRY") {
            config.discord.notify_on_retry = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_HEALTH") {
            config.discord.notify_on_health = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("DISCORD_NOTIFY_ON_CHECKSUM") {
            config.discord.notify_on_checksum = val == "true" || val == "1";
        }
        if let Ok(role) = std::env::var("DISCORD_FAILURE_MENTION_ROLE") {
            if !role.is_empty() {
                config.discord.failure_mention_role = Some(role);
            }
        }
        if let Ok(users) = std::env::var("DISCORD_FAILURE_MENTION_USERS") {
            config.discord.failure_mention_users = users
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        config
    }

    /// Load and merge YAML configuration if specified
    pub fn load_yaml_config(&mut self) -> Result<()> {
        let path = match &self.config_file {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        self.load_yaml_from_path(&path)
    }

    /// Load YAML configuration from a specific path
    pub fn load_yaml_from_path(&mut self, path: &str) -> Result<()> {
        let path = Path::new(path);
        if !path.exists() {
            tracing::warn!(path = %path.display(), "Config file not found, using defaults");
            return Ok(());
        }

        let content = std::fs::read_to_string(path).map_err(|e| {
            crate::Error::Configuration(format!("Failed to read config file: {}", e))
        })?;

        let yaml_config: YamlConfig = serde_yaml::from_str(&content).map_err(|e| {
            crate::Error::Configuration(format!("Failed to parse config file: {}", e))
        })?;

        // Merge operator settings (YAML overrides env vars only if explicitly set)
        if let Some(ns) = yaml_config.operator.watch_namespace {
            self.watch_namespace = Some(ns);
        }
        if let Some(addr) = yaml_config.operator.health_addr {
            self.health_addr = addr;
        }
        if let Some(port) = yaml_config.operator.metrics_port {
            self.metrics_port = port;
        }
        if let Some(timeout) = yaml_config.operator.default_migration_timeout_seconds {
            self.default_migration_timeout = Duration::from_secs(timeout);
        }
        if let Some(retries) = yaml_config.operator.default_max_retries {
            self.default_max_retries = retries;
        }
        if let Some(interval) = yaml_config.operator.requeue_interval_seconds {
            self.requeue_interval = Duration::from_secs(interval);
        }
        if let Some(level) = yaml_config.operator.log_level {
            self.log_level = level;
        }
        if let Some(format) = yaml_config.operator.log_format {
            self.log_format = match format.to_lowercase().as_str() {
                "pretty" => LogFormat::Pretty,
                _ => LogFormat::Json,
            };
        }
        if let Some(url) = yaml_config.operator.release_tracker_url {
            self.release_tracker_url = Some(url);
        }
        if let Some(timeout) = yaml_config.operator.webhook_timeout_seconds {
            self.webhook_timeout = Duration::from_secs(timeout);
        }
        if let Some(timeout) = yaml_config.operator.database_connect_timeout_seconds {
            self.database_connect_timeout = Duration::from_secs(timeout);
        }
        if let Some(delay) = yaml_config.operator.graceful_shutdown_delay_seconds {
            self.graceful_shutdown_delay = Duration::from_secs(delay);
        }

        // Merge job/mesh config from YAML
        if let Some(ttl) = yaml_config.operator.job_ttl_seconds {
            self.job_ttl_seconds = ttl;
        }
        if let Some(disable) = yaml_config.operator.disable_mesh_sidecar {
            self.disable_mesh_sidecar = disable;
        }
        if let Some(key) = yaml_config.operator.mesh_sidecar_annotation_key {
            self.mesh_sidecar_annotation_key = key;
        }
        if let Some(val) = yaml_config.operator.mesh_sidecar_annotation_value {
            self.mesh_sidecar_annotation_value = val;
        }

        // Merge Discord config from YAML (YAML overrides env vars)
        let discord_yaml = &yaml_config.operator.discord;
        if discord_yaml.webhook_url.is_some() {
            self.discord.webhook_url = discord_yaml.webhook_url.clone();
        }
        if !discord_yaml.username.is_empty() && discord_yaml.username != "Shinka" {
            self.discord.username = discord_yaml.username.clone();
        }
        if discord_yaml.avatar_url.is_some() {
            self.discord.avatar_url = discord_yaml.avatar_url.clone();
        }
        if !discord_yaml.cluster_name.is_empty() && discord_yaml.cluster_name != "kubernetes" {
            self.discord.cluster_name = discord_yaml.cluster_name.clone();
        }
        if !discord_yaml.environment.is_empty() && discord_yaml.environment != "unknown" {
            self.discord.environment = discord_yaml.environment.clone();
        }
        // Boolean flags are always taken from YAML when present
        self.discord.notify_on_success = discord_yaml.notify_on_success;
        self.discord.notify_on_failure = discord_yaml.notify_on_failure;
        self.discord.notify_on_started = discord_yaml.notify_on_started;
        self.discord.notify_on_retry = discord_yaml.notify_on_retry;
        self.discord.notify_on_health = discord_yaml.notify_on_health;
        self.discord.notify_on_checksum = discord_yaml.notify_on_checksum;
        if discord_yaml.failure_mention_role.is_some() {
            self.discord.failure_mention_role = discord_yaml.failure_mention_role.clone();
        }
        if !discord_yaml.failure_mention_users.is_empty() {
            self.discord.failure_mention_users = discord_yaml.failure_mention_users.clone();
        }

        // Use parallelization and migrator defaults from YAML
        self.parallelization = yaml_config.parallelization;
        self.migrator_defaults = yaml_config.migrators;

        tracing::info!(path = %path.display(), "Loaded YAML configuration");
        Ok(())
    }

    /// Get the default configuration for a migrator type
    pub fn get_migrator_config(&self, migrator_type: &MigratorType) -> MigratorConfig {
        let type_key = migrator_type.to_string();
        self.migrator_defaults
            .types
            .get(&type_key)
            .cloned()
            .unwrap_or_default()
    }
}

impl MigratorDefaults {
    /// Create example configuration for documentation
    pub fn example() -> Self {
        let mut types = BTreeMap::new();

        // SQLx example - Rust compile-time verified migrations
        types.insert(
            "sqlx".to_string(),
            MigratorConfig {
                command: Some(vec!["./backend".into(), "--migrate".into()]),
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    sqlx_embedded: Some(true),
                    sqlx_source: Some("migrations".into()),
                    sqlx_ignore_missing: Some(false),
                    sqlx_dry_run: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // SeaORM example - Rust async ORM migrations
        types.insert(
            "seaorm".to_string(),
            MigratorConfig {
                command: None, // Uses default: sea-orm-cli migrate up
                migrations_path: Some("migration".into()),
                tool_config: Some(ToolSpecificConfig {
                    seaorm_migration_dir: Some("migration".into()),
                    seaorm_schema: Some("public".into()),
                    seaorm_steps: None, // Apply all pending
                    seaorm_verbose: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Diesel example - Rust ORM migrations
        types.insert(
            "diesel".to_string(),
            MigratorConfig {
                command: None, // Uses default: diesel migration run
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    diesel_migration_dir: Some("migrations".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Refinery example - Rust flexible migrations
        types.insert(
            "refinery".to_string(),
            MigratorConfig {
                command: None, // Uses default: refinery migrate
                migrations_path: Some("migrations".into()),
                tool_config: None,
                ..Default::default()
            },
        );

        // Goose example - Go migrations
        types.insert(
            "goose".to_string(),
            MigratorConfig {
                migrations_path: Some("db/migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    goose_driver: Some("postgres".into()),
                    goose_table: Some("schema_migrations".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // golang-migrate example
        types.insert(
            "golang-migrate".to_string(),
            MigratorConfig {
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    migrate_lock_timeout: Some(30),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Atlas example - declarative migrations
        types.insert(
            "atlas".to_string(),
            MigratorConfig {
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    atlas_schema: Some("public".into()),
                    atlas_revision_schema: Some("atlas_schema_revisions".into()),
                    atlas_baseline: None,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Dbmate example
        types.insert(
            "dbmate".to_string(),
            MigratorConfig {
                migrations_path: Some("db/migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    dbmate_migrations_table: Some("schema_migrations".into()),
                    dbmate_no_dump_schema: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Flyway example - Java enterprise migrations
        types.insert(
            "flyway".to_string(),
            MigratorConfig {
                migrations_path: Some("sql".into()),
                tool_config: Some(ToolSpecificConfig {
                    flyway_schemas: Some(vec!["public".into()]),
                    flyway_baseline_version: Some("1".into()),
                    flyway_clean_disabled: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        // Liquibase example - Java changelog migrations
        types.insert(
            "liquibase".to_string(),
            MigratorConfig {
                migrations_path: Some("changelog".into()),
                tool_config: Some(ToolSpecificConfig {
                    liquibase_changelog_file: Some("changelog/db.changelog-master.xml".into()),
                    liquibase_default_schema: Some("public".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Self {
            default_type: MigratorType::Sqlx,
            types,
            global_env: BTreeMap::new(),
            default_resources: Some(ResourceDefaults {
                memory_limit: Some("256Mi".into()),
                cpu_limit: Some("500m".into()),
                memory_request: Some("128Mi".into()),
                cpu_request: Some("100m".into()),
            }),
        }
    }

    /// Create a minimal configuration for SQLx (default)
    pub fn sqlx_default() -> Self {
        let mut types = BTreeMap::new();
        types.insert(
            "sqlx".to_string(),
            MigratorConfig {
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    sqlx_source: Some("migrations".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Self {
            default_type: MigratorType::Sqlx,
            types,
            global_env: BTreeMap::new(),
            default_resources: None,
        }
    }

    /// Create a minimal configuration for SeaORM
    pub fn seaorm_default() -> Self {
        let mut types = BTreeMap::new();
        types.insert(
            "seaorm".to_string(),
            MigratorConfig {
                migrations_path: Some("migration".into()),
                tool_config: Some(ToolSpecificConfig {
                    seaorm_migration_dir: Some("migration".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Self {
            default_type: MigratorType::SeaOrm,
            types,
            global_env: BTreeMap::new(),
            default_resources: None,
        }
    }

    /// Create a configuration that runs both SQLx and SeaORM migrations
    /// Useful for transitioning from SQLx to SeaORM
    pub fn sqlx_and_seaorm() -> Self {
        let mut types = BTreeMap::new();

        types.insert(
            "sqlx".to_string(),
            MigratorConfig {
                migrations_path: Some("migrations".into()),
                tool_config: Some(ToolSpecificConfig {
                    sqlx_source: Some("migrations".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        types.insert(
            "seaorm".to_string(),
            MigratorConfig {
                migrations_path: Some("migration".into()),
                tool_config: Some(ToolSpecificConfig {
                    seaorm_migration_dir: Some("migration".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Self {
            default_type: MigratorType::Sqlx, // SQLx runs first by default
            types,
            global_env: BTreeMap::new(),
            default_resources: None,
        }
    }

    /// Check if a specific migrator type is configured
    pub fn has_type(&self, migrator_type: &MigratorType) -> bool {
        self.types.contains_key(&migrator_type.to_string())
    }

    /// Get configuration for a specific migrator type
    pub fn get_type_config(&self, migrator_type: &MigratorType) -> Option<&MigratorConfig> {
        self.types.get(&migrator_type.to_string())
    }

    /// Get mutable configuration for a specific migrator type
    pub fn get_type_config_mut(
        &mut self,
        migrator_type: &MigratorType,
    ) -> Option<&mut MigratorConfig> {
        self.types.get_mut(&migrator_type.to_string())
    }

    /// Add or update configuration for a migrator type
    pub fn set_type_config(&mut self, migrator_type: MigratorType, config: MigratorConfig) {
        self.types.insert(migrator_type.to_string(), config);
    }

    /// Remove configuration for a migrator type
    pub fn remove_type_config(&mut self, migrator_type: &MigratorType) -> Option<MigratorConfig> {
        self.types.remove(&migrator_type.to_string())
    }

    /// List all configured migrator types
    pub fn configured_types(&self) -> Vec<MigratorType> {
        self.types
            .keys()
            .filter_map(|key| match key.as_str() {
                "sqlx" => Some(MigratorType::Sqlx),
                "seaorm" => Some(MigratorType::SeaOrm),
                "diesel" => Some(MigratorType::Diesel),
                "refinery" => Some(MigratorType::Refinery),
                "goose" => Some(MigratorType::Goose),
                "golang-migrate" => Some(MigratorType::GolangMigrate),
                "atlas" => Some(MigratorType::Atlas),
                "dbmate" => Some(MigratorType::Dbmate),
                "flyway" => Some(MigratorType::Flyway),
                "liquibase" => Some(MigratorType::Liquibase),
                "custom" => Some(MigratorType::Custom),
                _ => None,
            })
            .collect()
    }

    /// Generate example YAML configuration
    pub fn to_example_yaml() -> String {
        let config = YamlConfig {
            api_version: "shinka.pleme.io/v1alpha1".to_string(),
            operator: OperatorSettings {
                watch_namespace: None,
                health_addr: Some("0.0.0.0:8080".into()),
                metrics_port: Some(9090),
                default_migration_timeout_seconds: Some(300),
                default_max_retries: Some(3),
                requeue_interval_seconds: Some(60),
                log_level: Some("info".into()),
                log_format: Some("json".into()),
                release_tracker_url: None,
                webhook_timeout_seconds: Some(5),
                database_connect_timeout_seconds: Some(10),
                graceful_shutdown_delay_seconds: Some(2),
                discord: DiscordConfig::default(),
                job_ttl_seconds: Some(3600),
                disable_mesh_sidecar: None,
                mesh_sidecar_annotation_key: None,
                mesh_sidecar_annotation_value: None,
            },
            parallelization: ParallelizationConfig::default(),
            migrators: Self::example(),
        };

        serde_yaml::to_string(&config).unwrap_or_else(|_| "# Error generating example".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.health_addr, "0.0.0.0:8080");
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.default_max_retries, 3);
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
apiVersion: shinka.pleme.io/v1alpha1
operator:
  healthAddr: "0.0.0.0:9000"
  metricsPort: 8080
migrators:
  defaultType: goose
  types:
    goose:
      migrationsPath: "db/migrations"
      toolConfig:
        gooseDriver: "postgres"
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        assert_eq!(config.operator.health_addr, Some("0.0.0.0:9000".into()));
        assert_eq!(config.operator.metrics_port, Some(8080));
        assert_eq!(config.migrators.default_type, MigratorType::Goose);
    }

    #[test]
    fn test_example_yaml_generation() {
        let yaml = MigratorDefaults::to_example_yaml();
        assert!(yaml.contains("apiVersion: shinka.pleme.io/v1alpha1"));
        assert!(yaml.contains("sqlx"));
        assert!(yaml.contains("goose"));
        assert!(yaml.contains("seaorm"));
    }

    // =========================================================================
    // MigratorDefaults Helper Method Tests
    // =========================================================================

    #[test]
    fn test_sqlx_default_configuration() {
        let defaults = MigratorDefaults::sqlx_default();
        assert_eq!(defaults.default_type, MigratorType::Sqlx);
        assert!(defaults.has_type(&MigratorType::Sqlx));
        assert!(!defaults.has_type(&MigratorType::SeaOrm));

        let sqlx_config = defaults.get_type_config(&MigratorType::Sqlx);
        assert!(sqlx_config.is_some());
        assert_eq!(
            sqlx_config.unwrap().migrations_path,
            Some("migrations".to_string())
        );
    }

    #[test]
    fn test_seaorm_default_configuration() {
        let defaults = MigratorDefaults::seaorm_default();
        assert_eq!(defaults.default_type, MigratorType::SeaOrm);
        assert!(defaults.has_type(&MigratorType::SeaOrm));
        assert!(!defaults.has_type(&MigratorType::Sqlx));

        let seaorm_config = defaults.get_type_config(&MigratorType::SeaOrm);
        assert!(seaorm_config.is_some());
        assert_eq!(
            seaorm_config.unwrap().migrations_path,
            Some("migration".to_string())
        );
    }

    #[test]
    fn test_sqlx_and_seaorm_configuration() {
        let defaults = MigratorDefaults::sqlx_and_seaorm();
        assert_eq!(defaults.default_type, MigratorType::Sqlx);
        assert!(defaults.has_type(&MigratorType::Sqlx));
        assert!(defaults.has_type(&MigratorType::SeaOrm));

        let configured = defaults.configured_types();
        assert!(configured.contains(&MigratorType::Sqlx));
        assert!(configured.contains(&MigratorType::SeaOrm));
    }

    #[test]
    fn test_set_and_remove_type_config() {
        let mut defaults = MigratorDefaults::default();

        // Add SeaORM config
        let seaorm_config = MigratorConfig {
            migrations_path: Some("custom/migration".to_string()),
            ..Default::default()
        };
        defaults.set_type_config(MigratorType::SeaOrm, seaorm_config);

        assert!(defaults.has_type(&MigratorType::SeaOrm));
        let config = defaults.get_type_config(&MigratorType::SeaOrm).unwrap();
        assert_eq!(config.migrations_path, Some("custom/migration".to_string()));

        // Remove it
        let removed = defaults.remove_type_config(&MigratorType::SeaOrm);
        assert!(removed.is_some());
        assert!(!defaults.has_type(&MigratorType::SeaOrm));
    }

    #[test]
    fn test_configured_types_returns_all() {
        let defaults = MigratorDefaults::example();
        let types = defaults.configured_types();

        assert!(types.contains(&MigratorType::Sqlx));
        assert!(types.contains(&MigratorType::SeaOrm));
        assert!(types.contains(&MigratorType::Diesel));
        assert!(types.contains(&MigratorType::Goose));
        assert!(types.contains(&MigratorType::Flyway));
    }

    // =========================================================================
    // YAML Configuration Tests for SQLx and SeaORM
    // =========================================================================

    #[test]
    fn test_yaml_sqlx_configuration() {
        let yaml = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  defaultType: sqlx
  types:
    sqlx:
      command: ["./backend", "--migrate"]
      migrationsPath: "db/migrations"
      toolConfig:
        sqlxEmbedded: true
        sqlxSource: "db/migrations"
        sqlxIgnoreMissing: false
        sqlxDryRun: false
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        assert_eq!(config.migrators.default_type, MigratorType::Sqlx);

        let sqlx_config = config.migrators.types.get("sqlx").unwrap();
        assert_eq!(
            sqlx_config.command,
            Some(vec!["./backend".to_string(), "--migrate".to_string()])
        );
        assert_eq!(
            sqlx_config.migrations_path,
            Some("db/migrations".to_string())
        );

        let tool_config = sqlx_config.tool_config.as_ref().unwrap();
        assert_eq!(tool_config.sqlx_embedded, Some(true));
        assert_eq!(tool_config.sqlx_source, Some("db/migrations".to_string()));
        assert_eq!(tool_config.sqlx_ignore_missing, Some(false));
        assert_eq!(tool_config.sqlx_dry_run, Some(false));
    }

    #[test]
    fn test_yaml_seaorm_configuration() {
        let yaml = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  defaultType: seaorm
  types:
    seaorm:
      migrationsPath: "migration"
      toolConfig:
        seaormMigrationDir: "migration"
        seaormSchema: "custom_schema"
        seaormSteps: 5
        seaormVerbose: true
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        assert_eq!(config.migrators.default_type, MigratorType::SeaOrm);

        let seaorm_config = config.migrators.types.get("seaorm").unwrap();
        assert_eq!(seaorm_config.migrations_path, Some("migration".to_string()));

        let tool_config = seaorm_config.tool_config.as_ref().unwrap();
        assert_eq!(
            tool_config.seaorm_migration_dir,
            Some("migration".to_string())
        );
        assert_eq!(tool_config.seaorm_schema, Some("custom_schema".to_string()));
        assert_eq!(tool_config.seaorm_steps, Some(5));
        assert_eq!(tool_config.seaorm_verbose, Some(true));
    }

    #[test]
    fn test_yaml_switching_between_sqlx_and_seaorm() {
        // Configuration with SQLx as default
        let yaml_sqlx = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  defaultType: sqlx
  types:
    sqlx:
      migrationsPath: "migrations"
"#;

        // Configuration with SeaORM as default
        let yaml_seaorm = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  defaultType: seaorm
  types:
    seaorm:
      migrationsPath: "migration"
"#;

        let config_sqlx: YamlConfig =
            serde_yaml::from_str(yaml_sqlx).expect("Failed to parse SQLx YAML");
        let config_seaorm: YamlConfig =
            serde_yaml::from_str(yaml_seaorm).expect("Failed to parse SeaORM YAML");

        assert_eq!(config_sqlx.migrators.default_type, MigratorType::Sqlx);
        assert_eq!(config_seaorm.migrators.default_type, MigratorType::SeaOrm);
    }

    #[test]
    fn test_yaml_custom_command_override() {
        let yaml = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  types:
    sqlx:
      command: ["/custom/path/sqlx-cli", "database", "migrate", "run"]
      workingDir: "/app"
      args: ["--verbose"]
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        let sqlx_config = config.migrators.types.get("sqlx").unwrap();

        assert_eq!(
            sqlx_config.command,
            Some(vec![
                "/custom/path/sqlx-cli".to_string(),
                "database".to_string(),
                "migrate".to_string(),
                "run".to_string()
            ])
        );
        assert_eq!(sqlx_config.working_dir, Some("/app".to_string()));
        assert_eq!(sqlx_config.args, Some(vec!["--verbose".to_string()]));
    }

    #[test]
    fn test_yaml_global_env_variables() {
        let yaml = r#"
apiVersion: shinka.pleme.io/v1alpha1
migrators:
  globalEnv:
    DATABASE_URL: "postgres://localhost/test"
    RUST_LOG: "debug"
  types:
    sqlx:
      env:
        SQLX_OFFLINE: "true"
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");

        assert_eq!(
            config.migrators.global_env.get("DATABASE_URL"),
            Some(&"postgres://localhost/test".to_string())
        );
        assert_eq!(
            config.migrators.global_env.get("RUST_LOG"),
            Some(&"debug".to_string())
        );

        let sqlx_config = config.migrators.types.get("sqlx").unwrap();
        let env = sqlx_config.env.as_ref().unwrap();
        assert_eq!(env.get("SQLX_OFFLINE"), Some(&"true".to_string()));
    }

    #[test]
    fn test_config_get_migrator_config() {
        let mut config = Config::default();
        config.migrator_defaults = MigratorDefaults::example();

        let sqlx_config = config.get_migrator_config(&MigratorType::Sqlx);
        assert!(sqlx_config.command.is_some());

        let seaorm_config = config.get_migrator_config(&MigratorType::SeaOrm);
        assert!(seaorm_config.tool_config.is_some());

        // Non-configured type should return default
        let custom_config = config.get_migrator_config(&MigratorType::Custom);
        assert!(custom_config.command.is_none());
    }
}
