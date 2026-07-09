//! ClickHouse typed-model converge: the executor seam + the create-only
//! plan-and-converge decision for a `clickhouseRef` migration.
//!
//! This is the direct-source pattern ([`crate::direct`]) applied to a *typed
//! model* target instead of operator-authored SQL. A CNPG migration runs a
//! Kubernetes Job against a CloudNativePG cluster; a **direct** migration
//! applies additive SQL from a ConfigMap; a **ClickHouse** migration carries no
//! SQL at all — it names a typed [`analitico`] table model and *converges* it,
//! create-only, against a Keeper-backed ClickHouse (`ON CLUSTER`,
//! `ReplicatedMergeTree`). This module owns that path end to end:
//!
//! 1. **health-skip** — resolving the source through
//!    [`crate::crd::DatabaseSpec::require_clickhouse_ref`] *is* the health-skip:
//!    a ClickHouse source has no CNPG cluster, so the CNPG health precondition
//!    is never reached.
//! 2. **typed model** — [`build_model`] renders the [`crate::crd::ClickHouseModel`]
//!    to an [`analitico::Ddl`] value (typed emission; no `format!` of DDL).
//! 3. **live schema** — [`ClickHouseMigrationEnv::read_live_schema`] reads the
//!    tables present in the target database (`SELECT name FROM system.tables`).
//! 4. **converge** — [`plan_and_converge_clickhouse`] is a *pure* function: M0
//!    is create-if-absent, so an absent table yields one `CREATE TABLE` op and a
//!    present table yields none.
//! 5. **apply** — [`ClickHouseMigrationEnv::apply`] executes each op's DDL.
//!
//! The three side effects — read a Secret key, read the live schema, execute the
//! DDL — sit behind the [`ClickHouseMigrationEnv`] trait so the converge logic
//! is unit-testable with **no live ClickHouse and no live API server** (the
//! TYPED-SPEC-TRIPLET testability contract). [`KubeClickHouseEnv`] is the real
//! impl (kube reads + the `clickhouse` HTTP crate); tests drive a mock.
//!
//! ## Tier honesty
//!
//! The model rendering, content addressing, live-schema converge decision, and
//! plan-and-apply loop are **real code**, exercised by the mock-env tests below
//! and proven end-to-end against the real Keeper-backed camelot ClickHouse (see
//! `examples/clickhouse_m0.rs`). M0 is deliberately **create-only**: an
//! existing table is never altered or dropped — schema *evolution* (add-column,
//! type-widen) is a named follow-up, not a stub.

use std::collections::BTreeSet;

use analitico::Ddl;
use k8s_openapi::api::core::v1::Secret;
use kube::{api::Api, Client};
use sha2::{Digest, Sha256};

use crate::crd::{ClickHouseModel, ClickHouseModelRef, DatabaseMigration, DatabaseSourceError};

/// Typed connection parameters for a ClickHouse source.
///
/// The password is held but never rendered by [`Display`] (which is the
/// redacted, log-safe form).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClickHouseConnParams {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

impl ClickHouseConnParams {
    /// Synthesise connection parameters from a resolved `clickhouseRef` and the
    /// password read from its `credentialsSecretRef`.
    pub fn from_ref(ch: &ClickHouseModelRef, password: String) -> Self {
        Self {
            host: ch.host.clone(),
            port: ch.effective_port(),
            username: ch.effective_username().to_string(),
            password,
            database: ch.database.clone(),
        }
    }

    /// The base HTTP URL the `clickhouse` client connects to (`http://host:port`).
    pub fn http_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for ClickHouseConnParams {
    /// Redacted, log-safe rendering — the password is never shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}/{}",
            self.username, self.host, self.port, self.database
        )
    }
}

/// The set of tables present in the target database (the observed live schema).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveSchema {
    /// Unqualified table names present in the target database.
    pub tables: BTreeSet<String>,
}

impl LiveSchema {
    /// True if a table of this (unqualified) name already exists.
    pub fn present(&self, table: &str) -> bool {
        self.tables.contains(table)
    }
}

/// A single converge operation.
///
/// M0 emits only [`DdlOp::CreateTable`] (create-if-absent). Additional op kinds
/// (alter, add-column) are a named follow-up, not a stub — the enum is the
/// extension point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DdlOp {
    /// Create a table from the rendered typed model DDL.
    CreateTable {
        /// The (unqualified) table name being created.
        table: String,
        /// The rendered `CREATE TABLE ... ON CLUSTER ... ReplicatedMergeTree` DDL.
        ddl: String,
    },
}

/// Typed errors for the ClickHouse converge branch.
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseError {
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

    /// A Kubernetes API call failed.
    #[error("kubernetes API error reading {what}: {message}")]
    Kube { what: String, message: String },

    /// Reading the live schema (system.tables) failed.
    #[error("reading live schema from {target}: {message}")]
    ReadSchema { target: String, message: String },

    /// Applying a specific CREATE TABLE op failed.
    #[error("applying create-table for {table}: {message}")]
    Apply { table: String, message: String },

    /// The migration source could not be resolved as a ClickHouse source.
    #[error("{0}")]
    Source(#[from] DatabaseSourceError),
}

impl From<ClickHouseError> for crate::Error {
    fn from(e: ClickHouseError) -> Self {
        match &e {
            // Spec / configuration problems: permanent until the spec or the
            // referenced Secret is fixed.
            ClickHouseError::SecretNotFound { .. }
            | ClickHouseError::SecretKeyMissing { .. }
            | ClickHouseError::InvalidUtf8 { .. }
            | ClickHouseError::Source(_) => crate::Error::Configuration(e.to_string()),
            // Runtime read/apply problems: surfaced as a migration failure
            // (retried on the next reconcile — CREATE IF NOT EXISTS is idempotent).
            ClickHouseError::Kube { .. }
            | ClickHouseError::ReadSchema { .. }
            | ClickHouseError::Apply { .. } => crate::Error::MigrationFailed {
                name: "clickhouse".to_string(),
                reason: e.to_string(),
            },
        }
    }
}

/// The side-effecting environment a ClickHouse converge depends on.
///
/// Every method that touches the cluster or a real ClickHouse lives here so the
/// [`plan_and_apply_clickhouse`] decision is unit-testable against a mock. This
/// is the TYPED-SPEC-TRIPLET Environment trait: real implementations satisfy it,
/// tests mock it.
#[async_trait::async_trait]
pub trait ClickHouseMigrationEnv: Send + Sync {
    /// Read a single string value (the password) from a Secret.
    async fn read_secret_key(
        &self,
        namespace: &str,
        secret: &str,
        key: &str,
    ) -> Result<String, ClickHouseError>;

    /// Read the tables present in the target database (the live schema).
    async fn read_live_schema(
        &self,
        params: &ClickHouseConnParams,
    ) -> Result<LiveSchema, ClickHouseError>;

    /// Apply one converge op against the ClickHouse source.
    async fn apply(
        &self,
        params: &ClickHouseConnParams,
        op: &DdlOp,
    ) -> Result<(), ClickHouseError>;
}

/// Render the typed [`analitico`] model for a ClickHouse source.
///
/// The model is a typed [`Ddl`] value — its Display is the DDL the executor
/// applies and the bytes the content-address hashes. No `format!` composes DDL.
pub fn build_model(ch: &ClickHouseModelRef) -> Ddl {
    match ch.model {
        ClickHouseModel::Events => Ddl::events(&ch.database, &ch.effective_table(), &ch.cluster),
    }
}

/// Content-address a rendered typed model into a stable version marker.
///
/// A length-framed SHA-256 over the model's rendered DDL, rendered as a short
/// hex digest with a `chmodel-` prefix so it reads as a ClickHouse model
/// content-address when stored in `status.lastMigration.imageTag`. Changing any
/// column, type, engine, or clause changes the model's Display, hence the
/// version, which re-triggers a converge.
pub fn content_version(model: &Ddl) -> String {
    let rendered = model.to_string();
    let mut hasher = Sha256::new();
    hasher.update((rendered.len() as u64).to_le_bytes());
    hasher.update(rendered.as_bytes());
    let digest = hasher.finalize();
    // 8 bytes → 16 hex chars is ample for change detection.
    let hex = HexDigest(&digest[..8]).to_string();
    let mut version = String::with_capacity(8 + hex.len());
    version.push_str("chmodel-");
    version.push_str(&hex);
    version
}

/// Hex renderer for a byte slice via a typed `Display` surface.
struct HexDigest<'a>(&'a [u8]);

impl std::fmt::Display for HexDigest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Converge a typed model against the observed live schema (M0: create-only).
///
/// Pure: no I/O. An absent table yields one [`DdlOp::CreateTable`] carrying the
/// rendered model DDL; a present table yields no ops (idempotent). `table` is
/// the unqualified name to look up in the live schema.
pub fn plan_and_converge_clickhouse(model: &Ddl, live: &LiveSchema, table: &str) -> Vec<DdlOp> {
    if live.present(table) {
        Vec::new()
    } else {
        vec![DdlOp::CreateTable {
            table: table.to_string(),
            ddl: model.to_string(),
        }]
    }
}

/// The outcome of a ClickHouse plan-and-apply cycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClickHouseReconcileOutcome {
    /// The model is already present (converge no-op); the CR is (and stays)
    /// `Ready`. Carries the content-address version.
    AlreadyReady { version: String },
    /// The model was created this cycle; the CR advances to `Ready`.
    Applied {
        version: String,
        /// The (unqualified) tables created this cycle.
        created: Vec<String>,
    },
}

impl ClickHouseReconcileOutcome {
    /// The content-address version this outcome recorded.
    pub fn version(&self) -> &str {
        match self {
            ClickHouseReconcileOutcome::AlreadyReady { version }
            | ClickHouseReconcileOutcome::Applied { version, .. } => version,
        }
    }
}

/// Plan and apply a ClickHouse typed-model converge.
///
/// The whole ClickHouse reconcile decision, health-skipped by construction:
///
/// 1. resolve the ClickHouse ref (health-skip — no CNPG cluster to poll);
/// 2. render the typed model + content-address it → a version;
/// 3. if the content is unchanged since the last success →
///    [`ClickHouseReconcileOutcome::AlreadyReady`];
/// 4. else read the password, synthesise the connection, read the live schema,
///    converge (create-only), and apply each op.
///
/// Returns a typed [`ClickHouseError`] on any failure — never a silent success.
pub async fn plan_and_apply_clickhouse<E: ClickHouseMigrationEnv + ?Sized>(
    migration: &DatabaseMigration,
    env: &E,
) -> Result<ClickHouseReconcileOutcome, ClickHouseError> {
    let namespace = migration.namespace_or_default();

    // 1. health-skip: resolve the ClickHouse source. A CNPG/direct source fails
    //    typed here (NotClickHouse); a ClickHouse source has no cluster to
    //    health-check, so there is no health precondition.
    let ch = migration.spec.database.require_clickhouse_ref()?;

    // 2. Render the typed model + content-address it.
    let model = build_model(ch);
    let table = ch.effective_table();
    let version = content_version(&model);
    if !migration.needs_migration(&version) {
        return Ok(ClickHouseReconcileOutcome::AlreadyReady { version });
    }

    // 4. Read the password + synthesise the connection.
    let password = env
        .read_secret_key(
            &namespace,
            &ch.credentials_secret_ref.name,
            &ch.password_key,
        )
        .await?;
    let params = ClickHouseConnParams::from_ref(ch, password);

    // Read the live schema and converge (create-only).
    let live = env.read_live_schema(&params).await?;
    let ops = plan_and_converge_clickhouse(&model, &live, &table);
    if ops.is_empty() {
        // The table is already present in the live schema (converge no-op) even
        // though the content-address moved — record Ready without a re-create.
        return Ok(ClickHouseReconcileOutcome::AlreadyReady { version });
    }

    let mut created = Vec::with_capacity(ops.len());
    for op in &ops {
        env.apply(&params, op).await?;
        let DdlOp::CreateTable { table, .. } = op;
        created.push(table.clone());
    }

    Ok(ClickHouseReconcileOutcome::Applied { version, created })
}

/// Real, cluster-backed [`ClickHouseMigrationEnv`]: kube Secret reads + the
/// `clickhouse` HTTP crate (port 8123) for schema reads and DDL apply.
pub struct KubeClickHouseEnv {
    client: Client,
}

impl KubeClickHouseEnv {
    /// Create a real environment bound to a kube client (for Secret reads).
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Build a `clickhouse` HTTP client for a set of connection params.
    fn ch_client(params: &ClickHouseConnParams) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(params.http_url())
            .with_user(&params.username)
            .with_password(&params.password)
            .with_database(&params.database)
    }
}

/// One row of `SELECT name FROM system.tables`.
#[derive(clickhouse::Row, serde::Deserialize)]
struct TableRow {
    name: String,
}

#[async_trait::async_trait]
impl ClickHouseMigrationEnv for KubeClickHouseEnv {
    async fn read_secret_key(
        &self,
        namespace: &str,
        secret: &str,
        key: &str,
    ) -> Result<String, ClickHouseError> {
        let secrets: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let obj = secrets.get(secret).await.map_err(|e| {
            if is_not_found(&e) {
                ClickHouseError::SecretNotFound {
                    namespace: namespace.to_string(),
                    secret: secret.to_string(),
                }
            } else {
                ClickHouseError::Kube {
                    what: format!("secret {secret}"),
                    message: e.to_string(),
                }
            }
        })?;

        let bytes = obj
            .data
            .as_ref()
            .and_then(|d| d.get(key))
            .ok_or_else(|| ClickHouseError::SecretKeyMissing {
                namespace: namespace.to_string(),
                secret: secret.to_string(),
                key: key.to_string(),
            })?;

        String::from_utf8(bytes.0.clone()).map_err(|_| ClickHouseError::InvalidUtf8 {
            secret: secret.to_string(),
            key: key.to_string(),
        })
    }

    async fn read_live_schema(
        &self,
        params: &ClickHouseConnParams,
    ) -> Result<LiveSchema, ClickHouseError> {
        let client = Self::ch_client(params);
        let rows: Vec<TableRow> = client
            .query("SELECT name FROM system.tables WHERE database = ?")
            .bind(params.database.as_str())
            .fetch_all()
            .await
            .map_err(|e| ClickHouseError::ReadSchema {
                target: params.to_string(),
                message: e.to_string(),
            })?;
        Ok(LiveSchema {
            tables: rows.into_iter().map(|r| r.name).collect(),
        })
    }

    async fn apply(
        &self,
        params: &ClickHouseConnParams,
        op: &DdlOp,
    ) -> Result<(), ClickHouseError> {
        let client = Self::ch_client(params);
        let DdlOp::CreateTable { table, ddl } = op;
        client
            .query(ddl)
            .execute()
            .await
            .map_err(|e| ClickHouseError::Apply {
                table: table.clone(),
                message: e.to_string(),
            })
    }
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
        ClickHouseModel, ClickHouseModelRef, CnpgClusterRef, DatabaseMigrationSpec, DatabaseMigrationStatus,
        DatabaseSpec, LastMigration, MigrationPhase, SafetySpec, SecretRef, TimeoutSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::sync::Mutex;

    // ---- Mock environment -------------------------------------------------

    /// In-memory [`ClickHouseMigrationEnv`] — no cluster, no ClickHouse.
    struct MockClickHouseEnv {
        password: Option<String>,
        /// Tables the mock reports as already present in the live schema.
        present_tables: BTreeSet<String>,
        /// When true, `read_live_schema` returns a typed error.
        fail_read: bool,
        /// Records the tables actually applied (proves per-op apply).
        applied: Mutex<Vec<String>>,
    }

    impl MockClickHouseEnv {
        /// A healthy env with an EMPTY live schema (nothing present).
        fn empty() -> Self {
            Self {
                password: Some("tendril".to_string()),
                present_tables: BTreeSet::new(),
                fail_read: false,
                applied: Mutex::new(Vec::new()),
            }
        }

        /// A healthy env whose live schema already holds `table`.
        fn with_present(table: &str) -> Self {
            let mut present = BTreeSet::new();
            present.insert(table.to_string());
            Self {
                password: Some("tendril".to_string()),
                present_tables: present,
                fail_read: false,
                applied: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ClickHouseMigrationEnv for MockClickHouseEnv {
        async fn read_secret_key(
            &self,
            namespace: &str,
            secret: &str,
            key: &str,
        ) -> Result<String, ClickHouseError> {
            self.password
                .clone()
                .ok_or_else(|| ClickHouseError::SecretKeyMissing {
                    namespace: namespace.to_string(),
                    secret: secret.to_string(),
                    key: key.to_string(),
                })
        }

        async fn read_live_schema(
            &self,
            params: &ClickHouseConnParams,
        ) -> Result<LiveSchema, ClickHouseError> {
            if self.fail_read {
                return Err(ClickHouseError::ReadSchema {
                    target: params.to_string(),
                    message: "mock read failure".to_string(),
                });
            }
            Ok(LiveSchema {
                tables: self.present_tables.clone(),
            })
        }

        async fn apply(
            &self,
            _params: &ClickHouseConnParams,
            op: &DdlOp,
        ) -> Result<(), ClickHouseError> {
            let DdlOp::CreateTable { table, .. } = op;
            self.applied.lock().unwrap().push(table.clone());
            Ok(())
        }
    }

    // ---- Fixtures ---------------------------------------------------------

    fn clickhouse_ref() -> ClickHouseModelRef {
        ClickHouseModelRef {
            model: ClickHouseModel::Events,
            host: "clickhouse.monitoring.svc".to_string(),
            port: None,
            database: "tendril".to_string(),
            cluster: "tendril".to_string(),
            table: Some("events_m0".to_string()),
            username: Some("tendril".to_string()),
            credentials_secret_ref: SecretRef {
                name: "clickhouse-auth".to_string(),
            },
            password_key: "clickhouse-password".to_string(),
        }
    }

    fn clickhouse_migration(status: Option<DatabaseMigrationStatus>) -> DatabaseMigration {
        DatabaseMigration {
            metadata: ObjectMeta {
                name: Some("events-model".to_string()),
                namespace: Some("monitoring".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: DatabaseMigrationSpec {
                database: DatabaseSpec {
                    cnpg_cluster_ref: None,
                    direct_ref: None,
                    clickhouse_ref: Some(clickhouse_ref()),
                },
                migrator: None,
                migrators: None,
                safety: SafetySpec::default(),
                timeouts: TimeoutSpec::default(),
            },
            status,
        }
    }

    // ---- Pure converge tests ---------------------------------------------

    #[test]
    fn converge_emits_create_when_table_absent() {
        let model = build_model(&clickhouse_ref());
        let live = LiveSchema::default(); // empty
        let ops = plan_and_converge_clickhouse(&model, &live, "events_m0");
        assert_eq!(ops.len(), 1);
        let DdlOp::CreateTable { table, ddl } = &ops[0];
        assert_eq!(table, "events_m0");
        assert!(ddl.contains("ON CLUSTER tendril"));
        assert!(ddl.contains("ReplicatedMergeTree('/clickhouse/tables/{shard}/events_m0', '{replica}')"));
        assert!(ddl.contains("timestamp DateTime64(3)"));
    }

    #[test]
    fn converge_is_noop_when_table_present() {
        let model = build_model(&clickhouse_ref());
        let mut live = LiveSchema::default();
        live.tables.insert("events_m0".to_string());
        let ops = plan_and_converge_clickhouse(&model, &live, "events_m0");
        assert!(ops.is_empty(), "present table → create-only no-op");
    }

    #[test]
    fn content_version_is_stable_and_change_sensitive() {
        let model = build_model(&clickhouse_ref());
        let a = content_version(&model);
        let b = content_version(&model);
        assert_eq!(a, b, "same model → same version");
        assert!(a.starts_with("chmodel-"), "content-addressed with chmodel- prefix");

        // A different table name changes the rendered DDL → a different version.
        let mut other_ref = clickhouse_ref();
        other_ref.table = Some("events_other".to_string());
        let other = content_version(&build_model(&other_ref));
        assert_ne!(other, a, "changed model → new version");
    }

    // ---- Reconcile-branch tests ------------------------------------------

    #[tokio::test]
    async fn clickhouse_reconcile_applies_when_absent() {
        // First reconcile (no status): empty live schema → converge creates the
        // table, yielding an Applied outcome the controller maps to Ready.
        let m = clickhouse_migration(None);
        let env = MockClickHouseEnv::empty();

        let outcome = plan_and_apply_clickhouse(&m, &env)
            .await
            .expect("clickhouse converge applies");

        match &outcome {
            ClickHouseReconcileOutcome::Applied { version, created } => {
                assert!(version.starts_with("chmodel-"), "content-addressed version");
                assert_eq!(created, &vec!["events_m0".to_string()]);
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(env.applied.lock().unwrap().clone(), vec!["events_m0"]);
    }

    #[tokio::test]
    async fn clickhouse_reconcile_noop_when_table_present() {
        // The live schema already holds the table → converge is a no-op even on
        // a fresh (statusless) migration; nothing is applied.
        let m = clickhouse_migration(None);
        let env = MockClickHouseEnv::with_present("events_m0");

        let outcome = plan_and_apply_clickhouse(&m, &env).await.unwrap();
        assert!(matches!(
            outcome,
            ClickHouseReconcileOutcome::AlreadyReady { .. }
        ));
        assert!(
            env.applied.lock().unwrap().is_empty(),
            "no create when table already present"
        );
    }

    #[tokio::test]
    async fn clickhouse_reconcile_skips_on_matching_content_address() {
        // Status records a success at the current model content version → the
        // content-address short-circuit skips before any live-schema read.
        let model = build_model(&clickhouse_ref());
        let version = content_version(&model);
        let m = clickhouse_migration(Some(DatabaseMigrationStatus {
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
        // fail_read would fire if we reached read_live_schema — we must not.
        let env = MockClickHouseEnv {
            password: Some("tendril".to_string()),
            present_tables: BTreeSet::new(),
            fail_read: true,
            applied: Mutex::new(Vec::new()),
        };

        let outcome = plan_and_apply_clickhouse(&m, &env).await.unwrap();
        assert_eq!(
            outcome,
            ClickHouseReconcileOutcome::AlreadyReady { version }
        );
        assert!(env.applied.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn clickhouse_reconcile_typed_error_on_missing_password() {
        let m = clickhouse_migration(None);
        let env = MockClickHouseEnv {
            password: None,
            present_tables: BTreeSet::new(),
            fail_read: false,
            applied: Mutex::new(Vec::new()),
        };
        let err = plan_and_apply_clickhouse(&m, &env)
            .await
            .expect_err("missing password is a typed error");
        assert!(matches!(err, ClickHouseError::SecretKeyMissing { .. }));
        let shinka_err: crate::Error = err.into();
        assert_eq!(shinka_err.category(), "configuration");
        assert!(shinka_err.is_permanent());
    }

    #[tokio::test]
    async fn clickhouse_reconcile_typed_error_on_schema_read_failure() {
        let m = clickhouse_migration(None);
        let env = MockClickHouseEnv {
            password: Some("tendril".to_string()),
            present_tables: BTreeSet::new(),
            fail_read: true,
            applied: Mutex::new(Vec::new()),
        };
        let err = plan_and_apply_clickhouse(&m, &env)
            .await
            .expect_err("schema read failure is a typed error");
        assert!(matches!(err, ClickHouseError::ReadSchema { .. }));
        let shinka_err: crate::Error = err.into();
        assert_eq!(shinka_err.category(), "migration");
    }

    #[tokio::test]
    async fn clickhouse_reconcile_rejects_non_clickhouse_source_typed() {
        // Health-skip boundary: a CNPG migration handed to the ClickHouse branch
        // fails typed (NotClickHouse), never touching a CNPG health check.
        let mut m = clickhouse_migration(None);
        m.spec.database = DatabaseSpec {
            cnpg_cluster_ref: Some(CnpgClusterRef {
                name: "pg".to_string(),
                database: None,
            }),
            direct_ref: None,
            clickhouse_ref: None,
        };
        let env = MockClickHouseEnv::empty();
        let err = plan_and_apply_clickhouse(&m, &env)
            .await
            .expect_err("cnpg source rejected");
        assert!(matches!(
            err,
            ClickHouseError::Source(DatabaseSourceError::NotClickHouse)
        ));
    }

    // ---- Pure-helper tests -----------------------------------------------

    #[test]
    fn conn_params_synthesis_and_redaction() {
        let ch = clickhouse_ref();
        let params = ClickHouseConnParams::from_ref(&ch, "s3cr3t".to_string());
        assert_eq!(params.host, "clickhouse.monitoring.svc");
        assert_eq!(params.port, 8123);
        assert_eq!(params.username, "tendril");
        assert_eq!(params.database, "tendril");
        assert_eq!(params.http_url(), "http://clickhouse.monitoring.svc:8123");
        // Display is redacted — the password never appears.
        let shown = params.to_string();
        assert_eq!(shown, "tendril@clickhouse.monitoring.svc:8123/tendril");
        assert!(!shown.contains("s3cr3t"));
    }

    #[test]
    fn build_model_renders_the_events_table() {
        let ddl = build_model(&clickhouse_ref()).to_string();
        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS tendril.events_m0 ON CLUSTER tendril"));
        assert!(ddl.contains("PARTITION BY toYYYYMMDD(timestamp)"));
        assert!(ddl.contains("ORDER BY (source, timestamp)"));
        assert!(ddl.contains("TTL toDateTime(timestamp) + INTERVAL 30 DAY"));
    }
}
