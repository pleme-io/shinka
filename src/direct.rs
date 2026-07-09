//! Direct (non-CNPG) database-source reconcile: the applier seam + the
//! plan-and-apply decision for a `directRef` migration.
//!
//! A CNPG migration runs a Kubernetes Job built from a deployment image against
//! a CloudNativePG Postgres cluster, gated on cluster health. A **direct**
//! migration has neither a deployment image nor a CNPG cluster: it addresses an
//! already-running engine (MySQL or Postgres) by host + credentials Secret and
//! applies additive DDL read from a ConfigMap, **in-process**. This module owns
//! that path end to end:
//!
//! 1. **directRef health-skip** — resolving the source through
//!    [`crate::crd::DatabaseSpec::require_direct_ref`] *is* the health-skip: a
//!    direct source has no CNPG cluster, so the CNPG health precondition is
//!    never reached.
//! 2. **connection synthesis** — [`DirectConnParams::from_direct`] builds the
//!    typed connection parameters from the `directRef` plus the password read
//!    from `credentialsSecretRef`; the real applier feeds them into a typed
//!    sqlx `*ConnectOptions` builder (never a hand-formatted URL string).
//! 3. **/sql load** — [`DirectMigrationEnv::load_sql`] reads the migration SQL
//!    from the referenced ConfigMap as an ordered list of [`SqlOp`].
//! 4. **per-op apply** — [`DirectMigrationEnv::apply`] applies each op's DDL
//!    against the direct source; [`plan_and_apply_direct`] advances the CR to
//!    `phase=Ready` on success and returns a typed [`DirectError`] on failure.
//!
//! The three side effects — read a Secret key, load the SQL ConfigMap, execute
//! the DDL — sit behind the [`DirectMigrationEnv`] trait so the reconcile logic
//! is unit-testable with **no live MySQL and no live API server** (the
//! TYPED-SPEC-TRIPLET testability contract). [`KubeDirectEnv`] is the real impl
//! (kube reads + sqlx execute); tests drive a mock.
//!
//! ## Tier honesty
//!
//! The reconcile logic, connection synthesis, SQL statement splitting, content
//! addressing, needs-apply decision, per-op apply loop, and phase advance are
//! **real code**, exercised by the mock-env tests below. The one part that
//! genuinely needs a running engine to prove is the raw socket execution in
//! [`KubeDirectEnv::apply`] — that is a `LiveTODO(direct-mysql-apply)`
//! end-to-end verification, not a stub: the code path is real and compiled, it
//! is the *observation against a live MySQL* that is deferred.

use std::time::Duration;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{api::Api, Client};
use sha2::{Digest, Sha256};

use crate::crd::{DatabaseEngine, DatabaseMigration, DatabaseSourceError, DirectDatabaseRef};

/// A single migration operation: one keyed SQL document from the ConfigMap.
///
/// Each entry in the `sqlConfigMapRef` ConfigMap's `data` is one op; ops are
/// applied in lexical key order so the apply sequence is deterministic and
/// operator-controllable via key naming (e.g. `00-databases.sql` before
/// `10-authdb.sql`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlOp {
    /// The ConfigMap key this SQL came from (used for logs + error context).
    pub name: String,
    /// The raw SQL document.
    pub sql: String,
}

/// Typed connection parameters for a direct source.
///
/// Engine-agnostic; the engine selects which typed sqlx `*ConnectOptions`
/// builder consumes these (see [`KubeDirectEnv::apply`]). The password is held
/// but never rendered by [`Display`] (which is the redacted, log-safe form).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectConnParams {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

impl DirectConnParams {
    /// Synthesise connection parameters from a resolved `directRef` and the
    /// password read from its `credentialsSecretRef`.
    pub fn from_direct(direct: &DirectDatabaseRef, password: String) -> Self {
        Self {
            host: direct.host.clone(),
            port: direct.effective_port(),
            username: direct.effective_username().to_string(),
            password,
            database: direct.database.clone(),
        }
    }
}

impl std::fmt::Display for DirectConnParams {
    /// Redacted, log-safe rendering — the password is never shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}:{}", self.username, self.host, self.port)?;
        if let Some(db) = &self.database {
            write!(f, "/{db}")?;
        }
        Ok(())
    }
}

/// Report of a completed per-op apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedReport {
    /// Number of ops (ConfigMap keys) applied.
    pub ops: usize,
    /// Total number of individual SQL statements executed across all ops.
    pub statements: usize,
}

/// Typed errors for the direct-source reconcile branch.
#[derive(Debug, thiserror::Error)]
pub enum DirectError {
    /// The credentials Secret was not found.
    #[error("credentials secret {secret} not found in namespace {namespace}")]
    SecretNotFound { namespace: String, secret: String },

    /// The password key was missing from the credentials Secret.
    #[error("password key {key} not found in secret {secret} (namespace {namespace})")]
    SecretKeyMissing {
        namespace: String,
        secret: String,
        key: String,
    },

    /// The password bytes were not valid UTF-8.
    #[error("password key {key} in secret {secret} is not valid UTF-8")]
    InvalidUtf8 { secret: String, key: String },

    /// The SQL ConfigMap was not found.
    #[error("SQL configmap {configmap} not found in namespace {namespace}")]
    ConfigMapNotFound { namespace: String, configmap: String },

    /// The SQL ConfigMap contained no usable SQL data.
    #[error("SQL configmap {configmap} in namespace {namespace} has no SQL entries")]
    EmptySql { namespace: String, configmap: String },

    /// A Kubernetes API call failed.
    #[error("kubernetes API error reading {what}: {message}")]
    Kube { what: String, message: String },

    /// Connecting to the direct database failed.
    #[error("connecting to {target}: {message}")]
    Connect { target: String, message: String },

    /// Applying a specific op/statement failed.
    #[error("applying op {op} (statement {stmt_index}): {message}")]
    Apply {
        op: String,
        stmt_index: usize,
        message: String,
    },

    /// The migration source could not be resolved as a direct source.
    #[error("{0}")]
    Source(#[from] DatabaseSourceError),
}

impl From<DirectError> for crate::Error {
    fn from(e: DirectError) -> Self {
        match &e {
            // Spec / configuration problems: permanent until the spec or the
            // referenced Secret/ConfigMap is fixed.
            DirectError::SecretNotFound { .. }
            | DirectError::SecretKeyMissing { .. }
            | DirectError::InvalidUtf8 { .. }
            | DirectError::ConfigMapNotFound { .. }
            | DirectError::EmptySql { .. }
            | DirectError::Source(_) => crate::Error::Configuration(e.to_string()),
            // Runtime apply/connection problems: surfaced as a migration
            // failure (retried on the next reconcile — additive DDL is
            // idempotent).
            DirectError::Kube { .. }
            | DirectError::Connect { .. }
            | DirectError::Apply { .. } => crate::Error::MigrationFailed {
                name: "direct".to_string(),
                reason: e.to_string(),
            },
        }
    }
}

/// The side-effecting environment a direct-source reconcile depends on.
///
/// Every method that touches the cluster or a real database lives here so the
/// [`plan_and_apply_direct`] decision is unit-testable against a mock. This is
/// the TYPED-SPEC-TRIPLET Environment trait: real implementations satisfy it,
/// tests mock it.
#[async_trait::async_trait]
pub trait DirectMigrationEnv: Send + Sync {
    /// Read a single string value (the password) from a Secret.
    async fn read_secret_key(
        &self,
        namespace: &str,
        secret: &str,
        key: &str,
    ) -> Result<String, DirectError>;

    /// Load the ordered SQL ops from a ConfigMap.
    async fn load_sql(
        &self,
        namespace: &str,
        configmap: &str,
    ) -> Result<Vec<SqlOp>, DirectError>;

    /// Apply every op against the direct source and report what was applied.
    async fn apply(
        &self,
        params: &DirectConnParams,
        engine: DatabaseEngine,
        ops: &[SqlOp],
    ) -> Result<AppliedReport, DirectError>;
}

/// The outcome of a direct-source plan-and-apply cycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirectReconcileOutcome {
    /// The SQL content is unchanged since the last successful apply; the CR is
    /// already (and stays) `Ready`. Carries the content-address version.
    AlreadyReady { version: String },
    /// The SQL was applied this cycle; the CR advances to `Ready`.
    Applied {
        version: String,
        report: AppliedReport,
    },
}

impl DirectReconcileOutcome {
    /// The content-address version this outcome recorded.
    pub fn version(&self) -> &str {
        match self {
            DirectReconcileOutcome::AlreadyReady { version }
            | DirectReconcileOutcome::Applied { version, .. } => version,
        }
    }
}

/// Plan and apply a direct-source migration.
///
/// The whole direct reconcile decision, health-skipped by construction:
///
/// 1. resolve the direct ref (health-skip — no CNPG cluster to poll);
/// 2. load the SQL ops from the ConfigMap;
/// 3. content-address the ops → a version;
/// 4. if unchanged since the last success → [`DirectReconcileOutcome::AlreadyReady`];
/// 5. else read the password, synthesise the connection, apply each op, and
///    return [`DirectReconcileOutcome::Applied`].
///
/// Returns a typed [`DirectError`] on any failure — never a silent success.
pub async fn plan_and_apply_direct<E: DirectMigrationEnv + ?Sized>(
    migration: &DatabaseMigration,
    env: &E,
) -> Result<DirectReconcileOutcome, DirectError> {
    let namespace = migration.namespace_or_default();

    // 1. directRef health-skip: resolve the direct source. A CNPG source fails
    //    typed here (NotDirect) rather than being mis-handled; a direct source
    //    has no cluster to health-check, so there is no health precondition.
    let direct = migration.spec.database.require_direct_ref()?;

    // 3 (load). Read the SQL ops from the ConfigMap.
    let ops = env
        .load_sql(&namespace, &direct.sql_config_map_ref.name)
        .await?;

    // 4. Content-address the ops so a ConfigMap SQL change re-applies (GitOps).
    let version = content_version(&ops);
    if !migration.needs_migration(&version) {
        return Ok(DirectReconcileOutcome::AlreadyReady { version });
    }

    // 2 (synthesis). Read the admin password and build the typed connection.
    let password = env
        .read_secret_key(
            &namespace,
            &direct.credentials_secret_ref.name,
            &direct.password_key,
        )
        .await?;
    let params = DirectConnParams::from_direct(direct, password);

    // 4 (apply). Apply every op against the direct source.
    let report = env.apply(&params, direct.engine, &ops).await?;

    Ok(DirectReconcileOutcome::Applied { version, report })
}

/// Content-address a set of ops into a stable version marker.
///
/// A length-framed SHA-256 over each op's name + SQL, rendered as a short hex
/// digest with a `sql-` prefix so it reads as a SQL content-address (not an
/// image tag) when stored in `status.lastMigration.imageTag`. Changing any op's
/// name or body changes the version, which re-triggers an apply.
pub fn content_version(ops: &[SqlOp]) -> String {
    let mut hasher = Sha256::new();
    for op in ops {
        hasher.update((op.name.len() as u64).to_le_bytes());
        hasher.update(op.name.as_bytes());
        hasher.update((op.sql.len() as u64).to_le_bytes());
        hasher.update(op.sql.as_bytes());
    }
    let digest = hasher.finalize();
    // 8 bytes → 16 hex chars is ample for change detection.
    let hex = HexDigest(&digest[..8]).to_string();
    let mut version = String::with_capacity(4 + hex.len());
    version.push_str("sql-");
    version.push_str(&hex);
    version
}

/// Hex renderer for a byte slice via a typed `Display` surface (no `format!`).
struct HexDigest<'a>(&'a [u8]);

impl std::fmt::Display for HexDigest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Split a SQL document into individual top-level statements.
///
/// A small character state machine: `;` terminates a statement only when it is
/// not inside a single-quoted / double-quoted / backtick-quoted string or a
/// line (`--`, `#`) or block (`/* */`) comment. Handles doubled (`''`) and
/// backslash-escaped quotes. Trailing/whitespace-only fragments are dropped.
///
/// Sufficient for the additive DDL these migrations carry (CREATE DATABASE /
/// TABLE, ALTER, INSERT, USE). `DELIMITER`-based stored-procedure bodies are a
/// known limitation — see `LiveTODO(direct-mysql-apply)`.
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum St {
        Normal,
        Single,
        Double,
        Backtick,
        LineComment,
        BlockComment,
    }

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut state = St::Normal;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match state {
            St::Normal => match c {
                ';' => {
                    push_statement(&mut statements, &current);
                    current.clear();
                }
                '\'' => {
                    state = St::Single;
                    current.push(c);
                }
                '"' => {
                    state = St::Double;
                    current.push(c);
                }
                '`' => {
                    state = St::Backtick;
                    current.push(c);
                }
                '-' if chars.peek() == Some(&'-') => {
                    state = St::LineComment;
                    current.push(c);
                    current.push(chars.next().unwrap());
                }
                '#' => {
                    state = St::LineComment;
                    current.push(c);
                }
                '/' if chars.peek() == Some(&'*') => {
                    state = St::BlockComment;
                    current.push(c);
                    current.push(chars.next().unwrap());
                }
                _ => current.push(c),
            },
            St::Single | St::Double | St::Backtick => {
                current.push(c);
                let quote = match state {
                    St::Single => '\'',
                    St::Double => '"',
                    _ => '`',
                };
                if c == '\\' {
                    // Escaped next char (MySQL backslash escapes); consume it.
                    if let Some(n) = chars.next() {
                        current.push(n);
                    }
                } else if c == quote {
                    if chars.peek() == Some(&quote) {
                        // Doubled quote — an escaped quote, stay in-string.
                        current.push(chars.next().unwrap());
                    } else {
                        state = St::Normal;
                    }
                }
            }
            St::LineComment => {
                current.push(c);
                if c == '\n' {
                    state = St::Normal;
                }
            }
            St::BlockComment => {
                current.push(c);
                if c == '*' && chars.peek() == Some(&'/') {
                    current.push(chars.next().unwrap());
                    state = St::Normal;
                }
            }
        }
    }

    // Trailing statement without a terminating semicolon.
    push_statement(&mut statements, &current);
    statements
}

/// Push `stmt` onto `out` if it contains any non-whitespace, non-comment SQL.
fn push_statement(out: &mut Vec<String>, stmt: &str) {
    let trimmed = stmt.trim();
    if trimmed.is_empty() {
        return;
    }
    // Drop fragments that are only comments (a `-- ...` tail before EOF).
    if statement_is_only_comments(trimmed) {
        return;
    }
    out.push(trimmed.to_string());
}

/// True when the fragment has no executable SQL — only whitespace + comments.
fn statement_is_only_comments(stmt: &str) -> bool {
    for line in stmt.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with('#') {
            continue;
        }
        // A block comment on its own line, or any other content, we treat as
        // executable (conservative — better to send it than silently drop it).
        if line.starts_with("/*") && line.ends_with("*/") {
            continue;
        }
        return false;
    }
    true
}

/// Real, cluster-backed [`DirectMigrationEnv`]: kube reads + sqlx execute.
pub struct KubeDirectEnv {
    client: Client,
    connect_timeout: Duration,
}

impl KubeDirectEnv {
    /// Create a real environment bound to a kube client + connection timeout.
    pub fn new(client: Client, connect_timeout: Duration) -> Self {
        Self {
            client,
            connect_timeout,
        }
    }
}

#[async_trait::async_trait]
impl DirectMigrationEnv for KubeDirectEnv {
    async fn read_secret_key(
        &self,
        namespace: &str,
        secret: &str,
        key: &str,
    ) -> Result<String, DirectError> {
        let secrets: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let obj = secrets.get(secret).await.map_err(|e| {
            if is_not_found(&e) {
                DirectError::SecretNotFound {
                    namespace: namespace.to_string(),
                    secret: secret.to_string(),
                }
            } else {
                DirectError::Kube {
                    what: format!("secret {secret}"),
                    message: e.to_string(),
                }
            }
        })?;

        let bytes = obj
            .data
            .as_ref()
            .and_then(|d| d.get(key))
            .ok_or_else(|| DirectError::SecretKeyMissing {
                namespace: namespace.to_string(),
                secret: secret.to_string(),
                key: key.to_string(),
            })?;

        String::from_utf8(bytes.0.clone()).map_err(|_| DirectError::InvalidUtf8 {
            secret: secret.to_string(),
            key: key.to_string(),
        })
    }

    async fn load_sql(
        &self,
        namespace: &str,
        configmap: &str,
    ) -> Result<Vec<SqlOp>, DirectError> {
        let cms: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let obj = cms.get(configmap).await.map_err(|e| {
            if is_not_found(&e) {
                DirectError::ConfigMapNotFound {
                    namespace: namespace.to_string(),
                    configmap: configmap.to_string(),
                }
            } else {
                DirectError::Kube {
                    what: format!("configmap {configmap}"),
                    message: e.to_string(),
                }
            }
        })?;

        // `data` is a sorted BTreeMap; lexical key order = deterministic apply
        // order (operator-controllable via key naming).
        let ops: Vec<SqlOp> = obj
            .data
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, sql)| !sql.trim().is_empty())
            .map(|(name, sql)| SqlOp { name, sql })
            .collect();

        if ops.is_empty() {
            return Err(DirectError::EmptySql {
                namespace: namespace.to_string(),
                configmap: configmap.to_string(),
            });
        }
        Ok(ops)
    }

    async fn apply(
        &self,
        params: &DirectConnParams,
        engine: DatabaseEngine,
        ops: &[SqlOp],
    ) -> Result<AppliedReport, DirectError> {
        // LiveTODO(direct-mysql-apply): the code path below is real (compiled,
        // typed), but end-to-end apply against a live MySQL is verified in the
        // cluster, not in unit tests (which mock this method).
        match engine {
            DatabaseEngine::Mysql => apply_mysql(params, ops, self.connect_timeout).await,
            DatabaseEngine::Postgres => apply_postgres(params, ops, self.connect_timeout).await,
        }
    }
}

/// Apply ops against a MySQL source via the typed `MySqlConnectOptions` builder.
async fn apply_mysql(
    params: &DirectConnParams,
    ops: &[SqlOp],
    connect_timeout: Duration,
) -> Result<AppliedReport, DirectError> {
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};

    let mut opts = MySqlConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.username)
        .password(&params.password);
    if let Some(db) = &params.database {
        opts = opts.database(db);
    }

    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(connect_timeout)
        .connect_with(opts)
        .await
        .map_err(|e| DirectError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    let mut total = 0usize;
    for op in ops {
        for (idx, stmt) in split_sql_statements(&op.sql).into_iter().enumerate() {
            sqlx::query(stmt.as_str())
                .execute(&pool)
                .await
                .map_err(|e| DirectError::Apply {
                    op: op.name.clone(),
                    stmt_index: idx,
                    message: e.to_string(),
                })?;
            total += 1;
        }
    }
    pool.close().await;

    Ok(AppliedReport {
        ops: ops.len(),
        statements: total,
    })
}

/// Apply ops against a Postgres source via the typed `PgConnectOptions` builder.
async fn apply_postgres(
    params: &DirectConnParams,
    ops: &[SqlOp],
    connect_timeout: Duration,
) -> Result<AppliedReport, DirectError> {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let mut opts = PgConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.username)
        .password(&params.password);
    if let Some(db) = &params.database {
        opts = opts.database(db);
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(connect_timeout)
        .connect_with(opts)
        .await
        .map_err(|e| DirectError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    let mut total = 0usize;
    for op in ops {
        for (idx, stmt) in split_sql_statements(&op.sql).into_iter().enumerate() {
            sqlx::query(stmt.as_str())
                .execute(&pool)
                .await
                .map_err(|e| DirectError::Apply {
                    op: op.name.clone(),
                    stmt_index: idx,
                    message: e.to_string(),
                })?;
            total += 1;
        }
    }
    pool.close().await;

    Ok(AppliedReport {
        ops: ops.len(),
        statements: total,
    })
}

/// True if a kube error is a 404 NotFound.
fn is_not_found(err: &kube::Error) -> bool {
    matches!(
        err,
        kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        CnpgClusterRef, ConfigMapRef, DatabaseMigrationSpec, DatabaseSpec, DatabaseMigrationStatus,
        DirectDatabaseRef, LastMigration, MigrationPhase, SafetySpec, SecretRef, TimeoutSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::sync::Mutex;

    // ---- Mock environment -------------------------------------------------

    /// In-memory [`DirectMigrationEnv`] — no cluster, no database.
    struct MockDirectEnv {
        /// Password returned by `read_secret_key`, or `None` to force a
        /// SecretKeyMissing error.
        password: Option<String>,
        /// Ops returned by `load_sql` (empty → EmptySql).
        ops: Vec<SqlOp>,
        /// When true, `apply` returns a typed Apply error.
        fail_apply: bool,
        /// Records the op names actually applied (proves per-op apply).
        applied: Mutex<Vec<String>>,
    }

    impl MockDirectEnv {
        fn healthy(ops: Vec<SqlOp>) -> Self {
            Self {
                password: Some("s3cr3t".to_string()),
                ops,
                fail_apply: false,
                applied: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl DirectMigrationEnv for MockDirectEnv {
        async fn read_secret_key(
            &self,
            namespace: &str,
            secret: &str,
            key: &str,
        ) -> Result<String, DirectError> {
            self.password
                .clone()
                .ok_or_else(|| DirectError::SecretKeyMissing {
                    namespace: namespace.to_string(),
                    secret: secret.to_string(),
                    key: key.to_string(),
                })
        }

        async fn load_sql(
            &self,
            namespace: &str,
            configmap: &str,
        ) -> Result<Vec<SqlOp>, DirectError> {
            if self.ops.is_empty() {
                return Err(DirectError::EmptySql {
                    namespace: namespace.to_string(),
                    configmap: configmap.to_string(),
                });
            }
            Ok(self.ops.clone())
        }

        async fn apply(
            &self,
            _params: &DirectConnParams,
            _engine: DatabaseEngine,
            ops: &[SqlOp],
        ) -> Result<AppliedReport, DirectError> {
            if self.fail_apply {
                return Err(DirectError::Apply {
                    op: ops.first().map(|o| o.name.clone()).unwrap_or_default(),
                    stmt_index: 0,
                    message: "mock apply failure".to_string(),
                });
            }
            let mut statements = 0usize;
            {
                let mut applied = self.applied.lock().unwrap();
                for op in ops {
                    applied.push(op.name.clone());
                    statements += split_sql_statements(&op.sql).len();
                }
            }
            Ok(AppliedReport {
                ops: ops.len(),
                statements,
            })
        }
    }

    // ---- Fixtures ---------------------------------------------------------

    fn direct_ref() -> DirectDatabaseRef {
        DirectDatabaseRef {
            engine: DatabaseEngine::Mysql,
            host: "akeyless-saas-akeyless-mysql".to_string(),
            port: None,
            database: Some("authdb".to_string()),
            username: None,
            credentials_secret_ref: SecretRef {
                name: "akeyless-mysql-root".to_string(),
            },
            password_key: "root-password".to_string(),
            sql_config_map_ref: ConfigMapRef {
                name: "akeyless-schema-apply-sql".to_string(),
            },
        }
    }

    fn direct_migration(status: Option<DatabaseMigrationStatus>) -> DatabaseMigration {
        DatabaseMigration {
            metadata: ObjectMeta {
                name: Some("authdb-schema".to_string()),
                namespace: Some("camelot".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: DatabaseMigrationSpec {
                database: DatabaseSpec {
                    cnpg_cluster_ref: None,
                    direct_ref: Some(direct_ref()),
                    clickhouse_ref: None,
                },
                migrator: None,
                migrators: None,
                safety: SafetySpec::default(),
                timeouts: TimeoutSpec::default(),
            },
            status,
        }
    }

    fn two_ops() -> Vec<SqlOp> {
        vec![
            SqlOp {
                name: "00-databases.sql".to_string(),
                sql: "CREATE DATABASE IF NOT EXISTS authdb;".to_string(),
            },
            SqlOp {
                name: "10-authdb.sql".to_string(),
                sql: "USE authdb;\nCREATE TABLE IF NOT EXISTS t (id INT);".to_string(),
            },
        ]
    }

    // ---- Reconcile-branch tests ------------------------------------------

    #[tokio::test]
    async fn direct_reconcile_reaches_ready_and_applies_all_ops() {
        // First reconcile (no status): the direct branch loads SQL, synthesises
        // the connection from the mock secret, applies every op, and yields an
        // Applied outcome the controller maps to phase=Ready.
        let m = direct_migration(None);
        let env = MockDirectEnv::healthy(two_ops());

        let outcome = plan_and_apply_direct(&m, &env)
            .await
            .expect("direct reconcile applies");

        match &outcome {
            DirectReconcileOutcome::Applied { version, report } => {
                assert!(version.starts_with("sql-"), "content-addressed version");
                assert_eq!(report.ops, 2, "both ops applied");
                assert_eq!(report.statements, 3, "1 + 2 statements executed");
            }
            other => panic!("expected Applied, got {other:?}"),
        }

        // Per-op apply actually ran, in deterministic key order.
        let applied = env.applied.lock().unwrap().clone();
        assert_eq!(applied, vec!["00-databases.sql", "10-authdb.sql"]);
    }

    #[tokio::test]
    async fn direct_reconcile_skips_when_already_applied() {
        // Status records a success at the current content version → no re-apply.
        let ops = two_ops();
        let version = content_version(&ops);
        let m = direct_migration(Some(DatabaseMigrationStatus {
            phase: Some(MigrationPhase::Ready),
            last_migration: Some(LastMigration {
                image_tag: version.clone(),
                success: true,
                duration: None,
                completed_at: None,
                error: None,
            }),
            ..Default::default()
        }));
        let env = MockDirectEnv::healthy(ops);

        let outcome = plan_and_apply_direct(&m, &env).await.unwrap();
        assert_eq!(outcome, DirectReconcileOutcome::AlreadyReady { version });
        assert!(
            env.applied.lock().unwrap().is_empty(),
            "no re-apply when content unchanged"
        );
    }

    #[tokio::test]
    async fn direct_reconcile_reapplies_when_sql_changes() {
        // Status records a success at an OLD version → the new content re-applies.
        let m = direct_migration(Some(DatabaseMigrationStatus {
            phase: Some(MigrationPhase::Ready),
            last_migration: Some(LastMigration {
                image_tag: "sql-deadbeefdeadbeef".to_string(),
                success: true,
                duration: None,
                completed_at: None,
                error: None,
            }),
            ..Default::default()
        }));
        let env = MockDirectEnv::healthy(two_ops());

        let outcome = plan_and_apply_direct(&m, &env).await.unwrap();
        assert!(matches!(outcome, DirectReconcileOutcome::Applied { .. }));
        assert_eq!(env.applied.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn direct_reconcile_typed_error_on_apply_failure() {
        let m = direct_migration(None);
        let env = MockDirectEnv {
            password: Some("pw".to_string()),
            ops: two_ops(),
            fail_apply: true,
            applied: Mutex::new(Vec::new()),
        };

        let err = plan_and_apply_direct(&m, &env)
            .await
            .expect_err("apply failure is a typed error, not a silent success");
        assert!(matches!(err, DirectError::Apply { .. }));
        // The typed DirectError maps to a migration-category shinka error.
        let shinka_err: crate::Error = err.into();
        assert_eq!(shinka_err.category(), "migration");
    }

    #[tokio::test]
    async fn direct_reconcile_typed_error_on_missing_password() {
        let m = direct_migration(None);
        let env = MockDirectEnv {
            password: None,
            ops: two_ops(),
            fail_apply: false,
            applied: Mutex::new(Vec::new()),
        };

        let err = plan_and_apply_direct(&m, &env).await.expect_err("missing password");
        assert!(matches!(err, DirectError::SecretKeyMissing { .. }));
        // Missing credentials is a permanent configuration error.
        let shinka_err: crate::Error = err.into();
        assert_eq!(shinka_err.category(), "configuration");
        assert!(shinka_err.is_permanent());
    }

    #[tokio::test]
    async fn direct_reconcile_typed_error_on_empty_sql() {
        let m = direct_migration(None);
        let env = MockDirectEnv::healthy(vec![]); // no ops → EmptySql
        let err = plan_and_apply_direct(&m, &env).await.expect_err("empty sql");
        assert!(matches!(err, DirectError::EmptySql { .. }));
    }

    #[tokio::test]
    async fn direct_reconcile_rejects_cnpg_source_typed() {
        // Health-skip boundary: a CNPG migration handed to the direct branch
        // fails typed (NotDirect), never touching a CNPG health check.
        let mut m = direct_migration(None);
        m.spec.database = DatabaseSpec {
            cnpg_cluster_ref: Some(CnpgClusterRef {
                name: "pg".to_string(),
                database: None,
            }),
            direct_ref: None,
            clickhouse_ref: None,
        };
        let env = MockDirectEnv::healthy(two_ops());
        let err = plan_and_apply_direct(&m, &env).await.expect_err("cnpg rejected");
        assert!(matches!(
            err,
            DirectError::Source(DatabaseSourceError::NotDirect)
        ));
    }

    // ---- Pure-helper tests -----------------------------------------------

    #[test]
    fn conn_params_synthesis_and_redaction() {
        let d = direct_ref();
        let params = DirectConnParams::from_direct(&d, "hunter2".to_string());
        assert_eq!(params.host, "akeyless-saas-akeyless-mysql");
        assert_eq!(params.port, 3306); // mysql default
        assert_eq!(params.username, "root"); // mysql default admin
        assert_eq!(params.database.as_deref(), Some("authdb"));
        assert_eq!(params.password, "hunter2");
        // Display is redacted — the password never appears.
        let shown = params.to_string();
        assert_eq!(shown, "root@akeyless-saas-akeyless-mysql:3306/authdb");
        assert!(!shown.contains("hunter2"));
    }

    #[test]
    fn conn_params_honor_explicit_port_user_and_engine_defaults() {
        let mut d = direct_ref();
        d.engine = DatabaseEngine::Postgres;
        d.port = Some(15432);
        d.username = Some("admin".to_string());
        d.database = None;
        let params = DirectConnParams::from_direct(&d, "pw".to_string());
        assert_eq!(params.port, 15432);
        assert_eq!(params.username, "admin");
        assert_eq!(params.database, None);
        assert_eq!(params.to_string(), "admin@akeyless-saas-akeyless-mysql:15432");
    }

    #[test]
    fn content_version_is_stable_and_change_sensitive() {
        let a = content_version(&two_ops());
        let b = content_version(&two_ops());
        assert_eq!(a, b, "same content → same version");
        assert!(a.starts_with("sql-"));

        let mut changed = two_ops();
        changed[1].sql.push_str("\nALTER TABLE t ADD COLUMN n INT;");
        assert_ne!(content_version(&changed), a, "changed SQL → new version");

        // Same bytes, different op names → different version (name is framed in).
        let mut renamed = two_ops();
        renamed[0].name = "01-databases.sql".to_string();
        assert_ne!(content_version(&renamed), a);
    }

    #[test]
    fn split_sql_basic_multi_statement() {
        let sql = "CREATE DATABASE authdb;\nUSE authdb;\nCREATE TABLE t (id INT);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "CREATE DATABASE authdb");
        assert_eq!(stmts[1], "USE authdb");
        assert_eq!(stmts[2], "CREATE TABLE t (id INT)");
    }

    #[test]
    fn split_sql_ignores_semicolons_in_strings() {
        let sql = "INSERT INTO t (v) VALUES ('a;b'); INSERT INTO t (v) VALUES (\"c;d\");";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'a;b'"));
        assert!(stmts[1].contains("\"c;d\""));
    }

    #[test]
    fn split_sql_ignores_semicolons_in_comments() {
        let sql = "-- a comment; with a semicolon\nSELECT 1;\n/* block; comment */ SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("SELECT 1"));
        assert!(stmts[1].contains("SELECT 2"));
    }

    #[test]
    fn split_sql_handles_escaped_and_doubled_quotes() {
        // A doubled '' quote and a backslash-escaped \' both stay in-string, so
        // the trailing ; is the only statement terminator.
        let sql = "INSERT INTO t VALUES ('it''s a; test'); INSERT INTO t VALUES ('x\\'; y');";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "quotes swallowed the inner semicolons");
    }

    #[test]
    fn split_sql_drops_trailing_comment_and_whitespace_fragments() {
        let sql = "SELECT 1;\n\n-- trailing comment only\n   ";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    #[test]
    fn direct_error_maps_to_shinka_error_categories() {
        let cfg: crate::Error = DirectError::ConfigMapNotFound {
            namespace: "ns".to_string(),
            configmap: "cm".to_string(),
        }
        .into();
        assert_eq!(cfg.category(), "configuration");
        assert!(cfg.is_permanent());

        let run: crate::Error = DirectError::Connect {
            target: "root@h:3306".to_string(),
            message: "refused".to_string(),
        }
        .into();
        assert_eq!(run.category(), "migration");
    }
}
