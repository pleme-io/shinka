//! Migrator types and configurations
//!
//! Defines supported migration tools and their configurations.
//! Shinka supports multiple migration utilities for PostgreSQL.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Supported migration tool types
///
/// Each tool has specific command patterns and environment requirements.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MigratorType {
    // =========================================================================
    // Rust-based migration tools
    // =========================================================================
    /// SQLx migrations (Rust) - Primary recommended tool
    /// Command: `sqlx migrate run` or embedded in binary with `--migrate` flag
    /// Env: DATABASE_URL
    #[default]
    Sqlx,

    /// Refinery migrations (Rust)
    /// Command: `refinery migrate` or embedded in binary
    /// Env: DATABASE_URL
    Refinery,

    /// Diesel migrations (Rust)
    /// Command: `diesel migration run`
    /// Env: DATABASE_URL
    Diesel,

    /// SeaORM migrations (Rust)
    /// Command: `sea-orm-cli migrate up`
    /// Env: DATABASE_URL
    SeaOrm,

    // =========================================================================
    // Go-based migration tools
    // =========================================================================
    /// Goose migrations (Go)
    /// Command: `goose -dir <migrations> postgres <database_url> up`
    /// Supports SQL and Go-based migrations
    Goose,

    /// golang-migrate (Go)
    /// Command: `migrate -path <migrations> -database <database_url> up`
    /// Language-agnostic, supports many databases
    #[serde(rename = "golang-migrate")]
    GolangMigrate,

    /// Atlas migrations (Go)
    /// Command: `atlas migrate apply --url <database_url>`
    /// Declarative and versioned migrations
    Atlas,

    /// Dbmate migrations (Go)
    /// Command: `dbmate -d <migrations> up`
    /// Env: DATABASE_URL
    Dbmate,

    // =========================================================================
    // Java-based migration tools (run via container)
    // =========================================================================
    /// Flyway migrations (Java)
    /// Command: `flyway -url=<jdbc_url> migrate`
    /// Enterprise standard, requires JDBC URL format
    Flyway,

    /// Liquibase migrations (Java)
    /// Command: `liquibase --url=<jdbc_url> update`
    /// Enterprise standard, supports XML/YAML/JSON/SQL changelogs
    Liquibase,

    // =========================================================================
    // Custom migration tool
    // =========================================================================
    /// Custom migration command
    /// User defines the complete command and arguments
    Custom,
}

impl MigratorType {
    /// Get the default command for this migrator type
    ///
    /// Returns None for Custom type (must be user-defined)
    pub fn default_command(&self) -> Option<Vec<String>> {
        match self {
            Self::Sqlx => Some(vec!["sqlx".into(), "migrate".into(), "run".into()]),
            Self::Refinery => Some(vec!["refinery".into(), "migrate".into()]),
            Self::Diesel => Some(vec!["diesel".into(), "migration".into(), "run".into()]),
            Self::SeaOrm => Some(vec!["sea-orm-cli".into(), "migrate".into(), "up".into()]),
            Self::Goose => Some(vec!["goose".into(), "up".into()]),
            Self::GolangMigrate => Some(vec!["migrate".into(), "up".into()]),
            Self::Atlas => Some(vec!["atlas".into(), "migrate".into(), "apply".into()]),
            Self::Dbmate => Some(vec!["dbmate".into(), "up".into()]),
            Self::Flyway => Some(vec!["flyway".into(), "migrate".into()]),
            Self::Liquibase => Some(vec!["liquibase".into(), "update".into()]),
            Self::Custom => None,
        }
    }

    /// Get the primary environment variable for database connection
    pub fn database_env_var(&self) -> &'static str {
        match self {
            Self::Sqlx | Self::Refinery | Self::Diesel | Self::SeaOrm | Self::Dbmate => {
                "DATABASE_URL"
            }
            Self::Goose => "GOOSE_DBSTRING",
            Self::GolangMigrate => "DATABASE_URL",
            Self::Atlas => "DATABASE_URL",
            Self::Flyway => "FLYWAY_URL",
            Self::Liquibase => "LIQUIBASE_COMMAND_URL",
            Self::Custom => "DATABASE_URL",
        }
    }

    /// Whether this tool uses JDBC URL format (Java tools)
    pub fn uses_jdbc_url(&self) -> bool {
        matches!(self, Self::Flyway | Self::Liquibase)
    }

    /// Get the default migrations directory name
    pub fn default_migrations_dir(&self) -> &'static str {
        match self {
            Self::Sqlx => "migrations",
            Self::Refinery => "migrations",
            Self::Diesel => "migrations",
            Self::SeaOrm => "migration",
            Self::Goose => "migrations",
            Self::GolangMigrate => "migrations",
            Self::Atlas => "migrations",
            Self::Dbmate => "db/migrations",
            Self::Flyway => "sql",
            Self::Liquibase => "changelog",
            Self::Custom => "migrations",
        }
    }

    /// Human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            Self::Sqlx => "SQLx (Rust) - compile-time verified SQL migrations",
            Self::Refinery => "Refinery (Rust) - flexible SQL/Rust migrations",
            Self::Diesel => "Diesel (Rust) - ORM with migration support",
            Self::SeaOrm => "SeaORM (Rust) - async ORM with migration support",
            Self::Goose => "Goose (Go) - SQL and Go-based migrations",
            Self::GolangMigrate => "golang-migrate (Go) - language-agnostic migrations",
            Self::Atlas => "Atlas (Go) - declarative schema migrations",
            Self::Dbmate => "Dbmate (Go) - simple multi-database migrations",
            Self::Flyway => "Flyway (Java) - enterprise migration tool",
            Self::Liquibase => "Liquibase (Java) - enterprise changelog migrations",
            Self::Custom => "Custom - user-defined migration command",
        }
    }
}

impl std::fmt::Display for MigratorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx => write!(f, "sqlx"),
            Self::Refinery => write!(f, "refinery"),
            Self::Diesel => write!(f, "diesel"),
            Self::SeaOrm => write!(f, "seaorm"),
            Self::Goose => write!(f, "goose"),
            Self::GolangMigrate => write!(f, "golang-migrate"),
            Self::Atlas => write!(f, "atlas"),
            Self::Dbmate => write!(f, "dbmate"),
            Self::Flyway => write!(f, "flyway"),
            Self::Liquibase => write!(f, "liquibase"),
            Self::Custom => write!(f, "custom"),
        }
    }
}

/// Configuration specific to a migrator type
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigratorConfig {
    /// Override the default command for this migrator
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    /// Additional arguments to append to the command
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    /// Working directory for migration execution
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    /// Path to migrations directory (relative to working_dir or absolute)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrations_path: Option<String>,

    /// Additional environment variables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,

    /// Tool-specific configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_config: Option<ToolSpecificConfig>,
}

/// Tool-specific configuration options
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToolSpecificConfig {
    // =========================================================================
    // SQLx specific
    // =========================================================================
    /// For sqlx: use embedded migrations (binary flag like --migrate)
    /// When true, runs the binary with --migrate flag instead of sqlx CLI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlx_embedded: Option<bool>,

    /// For sqlx: migrations source directory (defaults to "migrations")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlx_source: Option<String>,

    /// For sqlx: ignore missing migrations in the database
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlx_ignore_missing: Option<bool>,

    /// For sqlx: run in dry-run mode (show what would be applied)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlx_dry_run: Option<bool>,

    // =========================================================================
    // Goose specific
    // =========================================================================
    /// For goose: driver name (postgres, mysql, sqlite3, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goose_driver: Option<String>,

    /// For goose: table name for tracking migrations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goose_table: Option<String>,

    // =========================================================================
    // Atlas specific
    // =========================================================================
    /// For atlas: schema name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atlas_schema: Option<String>,

    /// For atlas: revision schema name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atlas_revision_schema: Option<String>,

    /// For atlas: baseline version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atlas_baseline: Option<String>,

    // =========================================================================
    // Flyway specific
    // =========================================================================
    /// For flyway: schemas to manage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flyway_schemas: Option<Vec<String>>,

    /// For flyway: baseline version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flyway_baseline_version: Option<String>,

    /// For flyway: clean disabled (safety)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flyway_clean_disabled: Option<bool>,

    // =========================================================================
    // Liquibase specific
    // =========================================================================
    /// For liquibase: changelog file path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquibase_changelog_file: Option<String>,

    /// For liquibase: default schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquibase_default_schema: Option<String>,

    // =========================================================================
    // Diesel specific
    // =========================================================================
    /// For diesel: migration directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diesel_migration_dir: Option<String>,

    // =========================================================================
    // SeaORM specific
    // =========================================================================
    /// For seaorm: migration directory (defaults to "migration")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seaorm_migration_dir: Option<String>,

    /// For seaorm: database schema (if not using default 'public')
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seaorm_schema: Option<String>,

    /// For seaorm: number of migrations to apply (default: all pending)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seaorm_steps: Option<u32>,

    /// For seaorm: verbose output
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seaorm_verbose: Option<bool>,

    // =========================================================================
    // Dbmate specific
    // =========================================================================
    /// For dbmate: migrations table name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbmate_migrations_table: Option<String>,

    /// For dbmate: no dump schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbmate_no_dump_schema: Option<bool>,

    // =========================================================================
    // golang-migrate specific
    // =========================================================================
    /// For golang-migrate: lock timeout
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrate_lock_timeout: Option<u32>,
}

/// Complete migrator specification combining type and config
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigratorDefinition {
    /// Type of migrator to use
    #[serde(rename = "type")]
    pub migrator_type: MigratorType,

    /// Configuration for the migrator
    #[serde(flatten)]
    pub config: MigratorConfig,
}

impl Default for MigratorDefinition {
    fn default() -> Self {
        Self {
            migrator_type: MigratorType::Sqlx,
            config: MigratorConfig::default(),
        }
    }
}

impl MigratorDefinition {
    /// Create a new migrator definition with the specified type
    pub fn new(migrator_type: MigratorType) -> Self {
        Self {
            migrator_type,
            config: MigratorConfig::default(),
        }
    }

    /// Get the effective command for this migrator
    pub fn effective_command(&self) -> Vec<String> {
        self.config
            .command
            .clone()
            .or_else(|| self.migrator_type.default_command())
            .unwrap_or_else(|| vec!["echo".into(), "No command configured".into()])
    }

    /// Get the effective migrations path
    pub fn effective_migrations_path(&self) -> String {
        self.config
            .migrations_path
            .clone()
            .unwrap_or_else(|| self.migrator_type.default_migrations_dir().to_string())
    }

    /// Build the complete command with all arguments
    pub fn build_command(&self, database_url: &str) -> Vec<String> {
        let mut cmd = self.effective_command();

        // Add tool-specific arguments based on migrator type
        match self.migrator_type {
            MigratorType::Goose => {
                // goose -dir <migrations> postgres <database_url> up
                let driver = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.goose_driver.clone())
                    .unwrap_or_else(|| "postgres".to_string());

                // Insert args before "up" if command ends with "up"
                if cmd.last().map(|s| s.as_str()) == Some("up") {
                    cmd.pop(); // Remove "up"
                    cmd.extend([
                        "-dir".to_string(),
                        self.effective_migrations_path(),
                        driver,
                        database_url.to_string(),
                        "up".to_string(),
                    ]);
                }

                if let Some(table) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.goose_table.clone())
                {
                    cmd.extend(["-table".to_string(), table]);
                }
            }

            MigratorType::GolangMigrate => {
                // migrate -path <migrations> -database <database_url> up
                if cmd.last().map(|s| s.as_str()) == Some("up") {
                    cmd.pop();
                    cmd.extend([
                        "-path".to_string(),
                        self.effective_migrations_path(),
                        "-database".to_string(),
                        database_url.to_string(),
                        "up".to_string(),
                    ]);
                }

                if let Some(timeout) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.migrate_lock_timeout)
                {
                    cmd.extend(["-lock-timeout".to_string(), format!("{}s", timeout)]);
                }
            }

            MigratorType::Atlas => {
                // atlas migrate apply --url <database_url>
                cmd.extend(["--url".to_string(), database_url.to_string()]);

                if let Some(ref dir) = self.config.migrations_path {
                    cmd.extend(["--dir".to_string(), format!("file://{}", dir)]);
                }

                if let Some(schema) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.atlas_schema.clone())
                {
                    cmd.extend(["--schema".to_string(), schema]);
                }

                if let Some(baseline) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.atlas_baseline.clone())
                {
                    cmd.extend(["--baseline".to_string(), baseline]);
                }
            }

            MigratorType::Flyway => {
                // flyway -url=<jdbc_url> migrate
                let jdbc_url = to_jdbc_url(database_url);
                cmd.extend([format!("-url={}", jdbc_url)]);

                if let Some(schemas) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.flyway_schemas.clone())
                {
                    cmd.extend([format!("-schemas={}", schemas.join(","))]);
                }

                if let Some(baseline) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.flyway_baseline_version.clone())
                {
                    cmd.extend([format!("-baselineVersion={}", baseline)]);
                }

                if self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.flyway_clean_disabled)
                    .unwrap_or(true)
                {
                    cmd.push("-cleanDisabled=true".to_string());
                }
            }

            MigratorType::Liquibase => {
                // liquibase --url=<jdbc_url> update
                let jdbc_url = to_jdbc_url(database_url);
                cmd.extend([format!("--url={}", jdbc_url)]);

                if let Some(changelog) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.liquibase_changelog_file.clone())
                {
                    cmd.extend([format!("--changelog-file={}", changelog)]);
                }

                if let Some(schema) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.liquibase_default_schema.clone())
                {
                    cmd.extend([format!("--default-schema-name={}", schema)]);
                }
            }

            MigratorType::Dbmate => {
                // dbmate -d <migrations> up
                // DATABASE_URL is read from environment
                if let Some(ref dir) = self.config.migrations_path {
                    // Insert -d flag before "up"
                    if let Some(pos) = cmd.iter().position(|s| s == "up") {
                        cmd.insert(pos, dir.clone());
                        cmd.insert(pos, "-d".to_string());
                    }
                }

                if let Some(table) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.dbmate_migrations_table.clone())
                {
                    if let Some(pos) = cmd.iter().position(|s| s == "up") {
                        cmd.insert(pos, table);
                        cmd.insert(pos, "--migrations-table".to_string());
                    }
                }
            }

            MigratorType::Diesel => {
                // diesel migration run
                // DATABASE_URL is read from environment
                if let Some(dir) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.diesel_migration_dir.clone())
                    .or_else(|| self.config.migrations_path.clone())
                {
                    cmd.extend(["--migration-dir".to_string(), dir]);
                }
            }

            MigratorType::SeaOrm => {
                // sea-orm-cli migrate up -d <migration_dir>
                // DATABASE_URL is read from environment
                if let Some(dir) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.seaorm_migration_dir.clone())
                    .or_else(|| self.config.migrations_path.clone())
                {
                    cmd.extend(["-d".to_string(), dir]);
                }

                // Schema option
                if let Some(schema) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.seaorm_schema.clone())
                {
                    cmd.extend(["-s".to_string(), schema]);
                }

                // Number of steps to apply
                if let Some(steps) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.seaorm_steps)
                {
                    cmd.extend(["-n".to_string(), steps.to_string()]);
                }

                // Verbose output
                if self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.seaorm_verbose)
                    .unwrap_or(false)
                {
                    cmd.push("-v".to_string());
                }
            }

            MigratorType::Sqlx => {
                // sqlx migrate run --source <migrations>
                // DATABASE_URL is read from environment
                if let Some(source) = self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.sqlx_source.clone())
                    .or_else(|| self.config.migrations_path.clone())
                {
                    cmd.extend(["--source".to_string(), source]);
                }

                // Ignore missing migrations
                if self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.sqlx_ignore_missing)
                    .unwrap_or(false)
                {
                    cmd.push("--ignore-missing".to_string());
                }

                // Dry run mode
                if self
                    .config
                    .tool_config
                    .as_ref()
                    .and_then(|tc| tc.sqlx_dry_run)
                    .unwrap_or(false)
                {
                    cmd.push("--dry-run".to_string());
                }
            }

            MigratorType::Refinery => {
                // Refinery typically reads DATABASE_URL from environment
                // No additional args needed for basic usage
            }

            MigratorType::Custom => {
                // Custom command - user is responsible for all args
            }
        }

        // Append any additional user-specified args
        if let Some(extra_args) = &self.config.args {
            cmd.extend(extra_args.iter().cloned());
        }

        cmd
    }
}

/// Convert a postgres:// URL to JDBC format for Java tools
fn to_jdbc_url(database_url: &str) -> String {
    if database_url.starts_with("jdbc:") {
        return database_url.to_string();
    }

    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        // postgres://user:pass@host:port/db -> jdbc:postgresql://host:port/db?user=user&password=pass
        // For simplicity, just prepend jdbc: and adjust protocol
        let url = database_url.replace("postgres://", "postgresql://");
        return format!("jdbc:{}", url);
    }

    // Fallback: just prepend jdbc:postgresql://
    format!("jdbc:postgresql://{}", database_url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrator_type_default_command() {
        assert_eq!(
            MigratorType::Sqlx.default_command(),
            Some(vec!["sqlx".into(), "migrate".into(), "run".into()])
        );
        assert_eq!(MigratorType::Custom.default_command(), None);
    }

    #[test]
    fn test_migrator_type_display() {
        assert_eq!(MigratorType::Sqlx.to_string(), "sqlx");
        assert_eq!(MigratorType::GolangMigrate.to_string(), "golang-migrate");
    }

    #[test]
    fn test_jdbc_url_conversion() {
        assert_eq!(
            to_jdbc_url("postgres://user:pass@localhost:5432/db"),
            "jdbc:postgresql://user:pass@localhost:5432/db"
        );
        assert_eq!(
            to_jdbc_url("jdbc:postgresql://localhost:5432/db"),
            "jdbc:postgresql://localhost:5432/db"
        );
    }

    #[test]
    fn test_build_command_goose() {
        let def = MigratorDefinition::new(MigratorType::Goose);
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"postgres://localhost/test".to_string()));
        assert!(cmd.contains(&"postgres".to_string()));
        assert!(cmd.contains(&"up".to_string()));
    }

    // =========================================================================
    // SQLx Configuration Tests
    // =========================================================================

    #[test]
    fn test_sqlx_default_command() {
        let def = MigratorDefinition::new(MigratorType::Sqlx);
        let cmd = def.build_command("postgres://localhost/test");
        assert_eq!(cmd[0], "sqlx");
        assert_eq!(cmd[1], "migrate");
        assert_eq!(cmd[2], "run");
    }

    #[test]
    fn test_sqlx_with_source_directory() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.tool_config = Some(ToolSpecificConfig {
            sqlx_source: Some("db/migrations".to_string()),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"--source".to_string()));
        assert!(cmd.contains(&"db/migrations".to_string()));
    }

    #[test]
    fn test_sqlx_with_migrations_path_fallback() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.migrations_path = Some("custom/migrations".to_string());
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"--source".to_string()));
        assert!(cmd.contains(&"custom/migrations".to_string()));
    }

    #[test]
    fn test_sqlx_with_ignore_missing() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.tool_config = Some(ToolSpecificConfig {
            sqlx_ignore_missing: Some(true),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"--ignore-missing".to_string()));
    }

    #[test]
    fn test_sqlx_with_dry_run() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.tool_config = Some(ToolSpecificConfig {
            sqlx_dry_run: Some(true),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"--dry-run".to_string()));
    }

    #[test]
    fn test_sqlx_with_all_options() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.tool_config = Some(ToolSpecificConfig {
            sqlx_source: Some("db/migrations".to_string()),
            sqlx_ignore_missing: Some(true),
            sqlx_dry_run: Some(true),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"--source".to_string()));
        assert!(cmd.contains(&"db/migrations".to_string()));
        assert!(cmd.contains(&"--ignore-missing".to_string()));
        assert!(cmd.contains(&"--dry-run".to_string()));
    }

    // =========================================================================
    // SeaORM Configuration Tests
    // =========================================================================

    #[test]
    fn test_seaorm_default_command() {
        let def = MigratorDefinition::new(MigratorType::SeaOrm);
        let cmd = def.build_command("postgres://localhost/test");
        assert_eq!(cmd[0], "sea-orm-cli");
        assert_eq!(cmd[1], "migrate");
        assert_eq!(cmd[2], "up");
    }

    #[test]
    fn test_seaorm_default_migrations_dir() {
        assert_eq!(MigratorType::SeaOrm.default_migrations_dir(), "migration");
    }

    #[test]
    fn test_seaorm_with_migration_dir() {
        let mut def = MigratorDefinition::new(MigratorType::SeaOrm);
        def.config.tool_config = Some(ToolSpecificConfig {
            seaorm_migration_dir: Some("db/seaorm_migrations".to_string()),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"-d".to_string()));
        assert!(cmd.contains(&"db/seaorm_migrations".to_string()));
    }

    #[test]
    fn test_seaorm_with_schema() {
        let mut def = MigratorDefinition::new(MigratorType::SeaOrm);
        def.config.tool_config = Some(ToolSpecificConfig {
            seaorm_schema: Some("custom_schema".to_string()),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"-s".to_string()));
        assert!(cmd.contains(&"custom_schema".to_string()));
    }

    #[test]
    fn test_seaorm_with_steps() {
        let mut def = MigratorDefinition::new(MigratorType::SeaOrm);
        def.config.tool_config = Some(ToolSpecificConfig {
            seaorm_steps: Some(5),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"-n".to_string()));
        assert!(cmd.contains(&"5".to_string()));
    }

    #[test]
    fn test_seaorm_with_verbose() {
        let mut def = MigratorDefinition::new(MigratorType::SeaOrm);
        def.config.tool_config = Some(ToolSpecificConfig {
            seaorm_verbose: Some(true),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"-v".to_string()));
    }

    #[test]
    fn test_seaorm_with_all_options() {
        let mut def = MigratorDefinition::new(MigratorType::SeaOrm);
        def.config.tool_config = Some(ToolSpecificConfig {
            seaorm_migration_dir: Some("migration".to_string()),
            seaorm_schema: Some("public".to_string()),
            seaorm_steps: Some(3),
            seaorm_verbose: Some(true),
            ..Default::default()
        });
        let cmd = def.build_command("postgres://localhost/test");
        assert!(cmd.contains(&"-d".to_string()));
        assert!(cmd.contains(&"migration".to_string()));
        assert!(cmd.contains(&"-s".to_string()));
        assert!(cmd.contains(&"public".to_string()));
        assert!(cmd.contains(&"-n".to_string()));
        assert!(cmd.contains(&"3".to_string()));
        assert!(cmd.contains(&"-v".to_string()));
    }

    // =========================================================================
    // Migration Type Switching Tests
    // =========================================================================

    #[test]
    fn test_switch_sqlx_to_seaorm() {
        // SQLx config
        let mut sqlx_def = MigratorDefinition::new(MigratorType::Sqlx);
        sqlx_def.config.migrations_path = Some("migrations".to_string());

        // SeaORM config
        let mut seaorm_def = MigratorDefinition::new(MigratorType::SeaOrm);
        seaorm_def.config.migrations_path = Some("migration".to_string());

        // Verify different default directories
        assert_eq!(MigratorType::Sqlx.default_migrations_dir(), "migrations");
        assert_eq!(MigratorType::SeaOrm.default_migrations_dir(), "migration");

        // Verify different commands
        let sqlx_cmd = sqlx_def.build_command("postgres://localhost/test");
        let seaorm_cmd = seaorm_def.build_command("postgres://localhost/test");

        assert_eq!(sqlx_cmd[0], "sqlx");
        assert_eq!(seaorm_cmd[0], "sea-orm-cli");
    }

    #[test]
    fn test_switch_seaorm_to_sqlx() {
        // Start with SeaORM
        let seaorm_def = MigratorDefinition::new(MigratorType::SeaOrm);
        assert_eq!(seaorm_def.migrator_type, MigratorType::SeaOrm);

        // Switch to SQLx
        let sqlx_def = MigratorDefinition::new(MigratorType::Sqlx);
        assert_eq!(sqlx_def.migrator_type, MigratorType::Sqlx);

        // Verify environment variable consistency
        assert_eq!(MigratorType::Sqlx.database_env_var(), "DATABASE_URL");
        assert_eq!(MigratorType::SeaOrm.database_env_var(), "DATABASE_URL");
    }

    #[test]
    fn test_migrator_type_equality() {
        assert_eq!(MigratorType::Sqlx, MigratorType::Sqlx);
        assert_eq!(MigratorType::SeaOrm, MigratorType::SeaOrm);
        assert_ne!(MigratorType::Sqlx, MigratorType::SeaOrm);
    }

    #[test]
    fn test_default_migrator_is_sqlx() {
        let default_def = MigratorDefinition::default();
        assert_eq!(default_def.migrator_type, MigratorType::Sqlx);
    }

    #[test]
    fn test_migrator_type_serialization_round_trip() {
        // Test SQLx
        let sqlx = MigratorType::Sqlx;
        let serialized = serde_json::to_string(&sqlx).unwrap();
        let deserialized: MigratorType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(sqlx, deserialized);

        // Test SeaOrm
        let seaorm = MigratorType::SeaOrm;
        let serialized = serde_json::to_string(&seaorm).unwrap();
        let deserialized: MigratorType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(seaorm, deserialized);
    }

    #[test]
    fn test_tool_specific_config_serialization() {
        let config = ToolSpecificConfig {
            sqlx_source: Some("migrations".to_string()),
            sqlx_dry_run: Some(true),
            seaorm_migration_dir: Some("migration".to_string()),
            seaorm_verbose: Some(true),
            ..Default::default()
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: ToolSpecificConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.sqlx_source, deserialized.sqlx_source);
        assert_eq!(config.sqlx_dry_run, deserialized.sqlx_dry_run);
        assert_eq!(
            config.seaorm_migration_dir,
            deserialized.seaorm_migration_dir
        );
        assert_eq!(config.seaorm_verbose, deserialized.seaorm_verbose);
    }

    #[test]
    fn test_extra_args_appended() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.args = Some(vec![
            "--verbose".to_string(),
            "--connect-timeout".to_string(),
            "30".to_string(),
        ]);

        let cmd = def.build_command("postgres://localhost/test");

        // Extra args should be at the end
        assert!(cmd.contains(&"--verbose".to_string()));
        assert!(cmd.contains(&"--connect-timeout".to_string()));
        assert!(cmd.contains(&"30".to_string()));
    }

    #[test]
    fn test_custom_command_override() {
        let mut def = MigratorDefinition::new(MigratorType::Sqlx);
        def.config.command = Some(vec!["/app/migrate".to_string(), "--run".to_string()]);

        let cmd = def.build_command("postgres://localhost/test");

        // Should use custom command, not default
        assert_eq!(cmd[0], "/app/migrate");
        assert_eq!(cmd[1], "--run");
    }
}
