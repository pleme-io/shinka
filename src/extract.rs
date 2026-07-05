//! Schema extraction — the `extract` leg of `/copy-model`.
//!
//! `direct.rs` **applies** an ordered set of [`SqlOp`]s to a database (write).
//! This module is its **inverse**: it **reads** a live source database's schema
//! *into* a typed [`SchemaModel`], then renders that model back **into** ordered
//! [`SqlOp`]s (the additive base). Together they make shinka *bidirectional* over
//! its own model format:
//!
//! ```text
//!   direct.rs :  Vec<SqlOp>  ──apply──▶   live DB      (write / mold)
//!   extract.rs:  live DB      ──read──▶   SchemaModel  ──render──▶  Vec<SqlOp>   (extract → base)
//! ```
//!
//! ## Why this is the shinka-aware morphism
//!
//! Shinka *imposes the model*: a `directRef` migration's model is exactly a
//! keyed, lexically-ordered set of [`SqlOp`]s (a `sqlConfigMapRef` ConfigMap)
//! that shinka content-addresses and applies additively. `/copy-model` needs to
//! feed shinka a *real extracted base model* in that exact format. The morphism
//! is:
//!
//! ```text
//!   introspect(source)  →  SchemaModel  →  render_base_ops(model)  →  Vec<SqlOp>
//! ```
//!
//! The output is shinka-native by construction: it IS a `Vec<SqlOp>`, the same
//! type `direct.rs` applies. A `render_base_configmap` turns it into a
//! `sqlConfigMapRef`-shaped ConfigMap — commit it, point a `directRef`
//! `DatabaseMigration` at it, and shinka absorbs the extracted schema as the
//! base, then molds it forward with additional keyed ops.
//!
//! ## The Environment seam (TYPED-SPEC-TRIPLET testability contract)
//!
//! Every side effect that touches a live database — enumerate schemas, read
//! `information_schema` columns — sits behind the [`SchemaSource`] trait so the
//! whole pipeline ([`extract_base`]) is unit-tested against an in-memory mock
//! with **no live MySQL/Postgres and no cluster**. [`SqlxSchemaSource`] is the
//! real impl (sqlx introspection against the direct source, reusing
//! [`crate::direct::DirectConnParams`]); tests drive [`tests::MockSchemaSource`].
//!
//! ## Tier honesty
//!
//! - **Shipped + unit-tested (real code, mock-driven):** the `SchemaSource`
//!   seam, model assembly ([`build_schema_model`], deterministic ordering),
//!   additive DDL rendering (typed `Display` surfaces — no `format!()` of SQL),
//!   base-op emission ([`render_base_ops`]), the ConfigMap projection, and
//!   content addressing (reuses [`crate::direct::content_version`], so a source
//!   schema drift is detectable against the committed base).
//! - **`LiveTODO(extract-live-introspect)`** — the raw `information_schema`
//!   query execution in [`SqlxSchemaSource`] is real, compiled, typed code whose
//!   *observation against a live engine* is verified in the cluster, not in
//!   units (which mock the source). The exact mirror of `direct.rs`'s
//!   `LiveTODO(direct-mysql-apply)`.
//! - **Scope (M0):** columns + per-table primary keys + database grouping are
//!   modelled and rendered. **Secondary indexes, foreign keys, and full
//!   engine-specific type fidelity beyond the reported column type are a named
//!   `LiveTODO(extract-constraints)`** — the model carries the fields so adding
//!   them is additive, not a reshape.

use std::time::Duration;

use crate::crd::DatabaseEngine;
use crate::direct::{DirectConnParams, SqlOp};

// =============================================================================
// Typed schema model
// =============================================================================

/// A whole extracted schema: the engine it came from + its databases.
///
/// This is the typed data the extract leg produces and the render leg consumes.
/// Deterministically ordered (databases by name, tables by name, columns by
/// source ordinal) so a re-extract of an unchanged source produces byte-identical
/// output — the property `render_base_ops` relies on for drift detection.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaModel {
    /// The wire protocol the model was extracted from (selects DDL dialect).
    pub engine: DatabaseEngine,
    /// The databases (MySQL schemas / Postgres databases-or-schemas) captured.
    #[serde(default)]
    pub databases: Vec<DatabaseModel>,
}

impl SchemaModel {
    /// Total number of tables across all databases (for reports / summaries).
    pub fn table_count(&self) -> usize {
        self.databases.iter().map(|d| d.tables.len()).sum()
    }

    /// Total number of columns across all tables (for reports / summaries).
    pub fn column_count(&self) -> usize {
        self.databases
            .iter()
            .flat_map(|d| d.tables.iter())
            .map(|t| t.columns.len())
            .sum()
    }
}

/// One database (MySQL schema) and its tables, ordered by table name.
///
/// Also the authoring shape of a `/copy-model` `createDatabase` mold evolution
/// (the mold file reuses this type via serde — one model, no fork).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseModel {
    pub name: String,
    #[serde(default)]
    pub tables: Vec<TableModel>,
}

/// One table and its columns, ordered by source ordinal position.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableModel {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<ColumnModel>,
    /// Ordered primary-key column names (empty when the table has no PK).
    #[serde(default)]
    pub primary_key: Vec<String>,
}

/// One column: enough to render an additive, faithful `CREATE TABLE`.
///
/// `data_type` is the engine's *reported column type string* (MySQL
/// `column_type`, e.g. `varchar(255)` / `int unsigned`; Postgres `data_type`
/// with length folded in) so the rendered DDL round-trips the source type
/// without a lossy type-name mapping.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnModel {
    pub name: String,
    pub data_type: String,
    /// Whether the column admits NULL. Defaults to SQL-standard nullable when a
    /// mold author omits it.
    #[serde(default = "nullable_default")]
    pub nullable: bool,
    /// Column default expression as reported by the engine, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// True for `AUTO_INCREMENT` (MySQL) / identity (Postgres) columns.
    #[serde(default)]
    pub auto_increment: bool,
}

/// serde default for [`ColumnModel::nullable`] — SQL-standard columns are
/// nullable unless declared `NOT NULL`.
fn nullable_default() -> bool {
    true
}

// =============================================================================
// Raw introspection rows (the SchemaSource output before assembly)
// =============================================================================

/// One raw `information_schema.columns` row, engine-normalised.
///
/// The [`SchemaSource`] produces these; [`build_schema_model`] assembles them
/// into the typed [`SchemaModel`]. Keeping raw rows separate from the model
/// keeps the *assembly* logic pure and unit-testable independent of the query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawColumn {
    pub database: String,
    pub table: String,
    pub column: String,
    /// 1-based ordinal position within the table (source order).
    pub ordinal: u32,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub auto_increment: bool,
    /// True when this column participates in the table's primary key.
    pub primary_key: bool,
}

// =============================================================================
// Errors
// =============================================================================

/// Typed errors for the extract leg (mirrors `direct::DirectError`'s shape).
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    /// Connecting to the source database failed.
    #[error("connecting to {target}: {message}")]
    Connect { target: String, message: String },

    /// An introspection query failed.
    #[error("introspecting {what}: {message}")]
    Introspect { what: String, message: String },

    /// The source exposed no user databases / tables to extract.
    #[error("source {target} exposed no extractable schema (after filtering)")]
    EmptySchema { target: String },
}

// =============================================================================
// The Environment seam
// =============================================================================

/// The side-effecting source a schema extraction reads from.
///
/// Every live-database read lives here so [`extract_base`] and
/// [`build_schema_model`] are unit-testable against a mock. This is the
/// TYPED-SPEC-TRIPLET Environment trait: [`SqlxSchemaSource`] satisfies it for
/// real; [`tests::MockSchemaSource`] mocks it.
#[async_trait::async_trait]
pub trait SchemaSource: Send + Sync {
    /// Enumerate the user databases (schemas) available to extract, already
    /// excluding the engine's system schemas.
    async fn list_databases(&self) -> Result<Vec<String>, ExtractError>;

    /// Read every column of every table in `database` as raw rows.
    async fn list_columns(&self, database: &str) -> Result<Vec<RawColumn>, ExtractError>;
}

// =============================================================================
// The pure pipeline: introspect → assemble → render
// =============================================================================

/// Which databases to include in an extraction.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum DatabaseFilter {
    /// Every user database the source reports (the default).
    #[default]
    All,
    /// Only the named databases (in the source's own order, filtered).
    Only(Vec<String>),
}

impl DatabaseFilter {
    /// Apply the filter to the source's reported database list, preserving the
    /// filter's order for `Only` (so `/copy-model` controls apply order) and the
    /// source's sorted order for `All`.
    fn select(&self, available: &[String]) -> Vec<String> {
        match self {
            DatabaseFilter::All => {
                let mut v = available.to_vec();
                v.sort();
                v.dedup();
                v
            }
            DatabaseFilter::Only(wanted) => wanted
                .iter()
                .filter(|w| available.iter().any(|a| a == *w))
                .cloned()
                .collect(),
        }
    }
}

/// Assemble raw rows into the deterministic typed [`SchemaModel`].
///
/// Pure — no I/O. Groups columns by `(database, table)`, orders databases by
/// name, tables by name, and columns by source ordinal, and lifts the per-row
/// `primary_key` flags into each table's ordered `primary_key` vec (in column
/// order). Deterministic: same rows in any order → same model.
pub fn build_schema_model(
    engine: DatabaseEngine,
    databases: Vec<String>,
    mut columns: Vec<RawColumn>,
) -> SchemaModel {
    // Stable, deterministic ordering independent of the source's row order.
    columns.sort_by(|a, b| {
        a.database
            .cmp(&b.database)
            .then(a.table.cmp(&b.table))
            .then(a.ordinal.cmp(&b.ordinal))
            .then(a.column.cmp(&b.column))
    });

    let mut db_models: Vec<DatabaseModel> = Vec::new();
    for db_name in databases {
        let mut tables: Vec<TableModel> = Vec::new();
        let db_cols: Vec<&RawColumn> = columns.iter().filter(|c| c.database == db_name).collect();

        // Distinct table names in sorted order.
        let mut table_names: Vec<&String> = db_cols.iter().map(|c| &c.table).collect();
        table_names.sort();
        table_names.dedup();

        for table_name in table_names {
            let cols: Vec<ColumnModel> = db_cols
                .iter()
                .filter(|c| &c.table == table_name)
                .map(|c| ColumnModel {
                    name: c.column.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    default: c.default.clone(),
                    auto_increment: c.auto_increment,
                })
                .collect();
            let primary_key: Vec<String> = db_cols
                .iter()
                .filter(|c| &c.table == table_name && c.primary_key)
                .map(|c| c.column.clone())
                .collect();
            tables.push(TableModel {
                name: table_name.clone(),
                columns: cols,
                primary_key,
            });
        }

        db_models.push(DatabaseModel {
            name: db_name,
            tables,
        });
    }

    SchemaModel {
        engine,
        databases: db_models,
    }
}

/// Render a [`SchemaModel`] into shinka's additive base [`SqlOp`]s.
///
/// One op per database, keyed `NN-<db>.sql` (zero-padded index → lexical apply
/// order matches extraction order — the same convention `direct.rs` documents).
/// Every statement is additive (`CREATE DATABASE IF NOT EXISTS` /
/// `CREATE TABLE IF NOT EXISTS`) so applying the base against a fresh OR a
/// partially-populated target is safe and idempotent — the `safe` + `additive`
/// properties of the shinka model-evolution framework, true by construction.
pub fn render_base_ops(model: &SchemaModel) -> Vec<SqlOp> {
    model
        .databases
        .iter()
        .enumerate()
        .map(|(idx, db)| SqlOp {
            name: op_key(idx, &db.name),
            sql: DatabaseDdl {
                engine: model.engine,
                db,
            }
            .to_string(),
        })
        .collect()
}

/// Render one database's additive DDL (create-database + every table) as SQL.
///
/// The shared database-DDL surface: [`render_base_ops`] uses it per base op, and
/// the `/copy-model` **mold** overlay ([`crate::copy_model`]) reuses it to render
/// a `CreateDatabase` evolution — one DDL renderer, two consumers (Op-Principle
/// #1: extend the primitive, never duplicate the render). Additive + engine-aware
/// by construction (`CREATE DATABASE/TABLE IF NOT EXISTS`).
pub fn render_database_ddl(engine: DatabaseEngine, db: &DatabaseModel) -> String {
    DatabaseDdl { engine, db }.to_string()
}

/// The lexically-sortable ConfigMap key for the `idx`-th database's base op.
fn op_key(idx: usize, db: &str) -> String {
    // Two zero-padded digits keep <100 databases in stable lexical order; the
    // key is authored, not user-input, so this is sufficient and deterministic.
    let mut key = String::with_capacity(3 + db.len() + 4);
    key.push_str(&PaddedIndex(idx).to_string());
    key.push('-');
    key.push_str(db);
    key.push_str(".sql");
    key
}

/// Zero-pad an index to two digits via a typed `Display` surface (no `format!`).
struct PaddedIndex(usize);
impl std::fmt::Display for PaddedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02}", self.0)
    }
}

/// The full extract pipeline: introspect a source, assemble the model, render
/// the additive base ops. The one call `/copy-model` M0 drives.
///
/// Returns [`ExtractError::EmptySchema`] when the (filtered) source has nothing
/// to extract — never a silent empty success.
pub async fn extract_base<S: SchemaSource + ?Sized>(
    engine: DatabaseEngine,
    source: &S,
    filter: &DatabaseFilter,
    target_label: &str,
) -> Result<(SchemaModel, Vec<SqlOp>), ExtractError> {
    let available = source.list_databases().await?;
    let selected = filter.select(&available);
    if selected.is_empty() {
        return Err(ExtractError::EmptySchema {
            target: target_label.to_string(),
        });
    }

    let mut all_columns: Vec<RawColumn> = Vec::new();
    for db in &selected {
        all_columns.extend(source.list_columns(db).await?);
    }
    if all_columns.is_empty() {
        return Err(ExtractError::EmptySchema {
            target: target_label.to_string(),
        });
    }

    let model = build_schema_model(engine, selected, all_columns);
    let ops = render_base_ops(&model);
    Ok((model, ops))
}

// =============================================================================
// Additive DDL rendering — typed Display surfaces (TYPED EMISSION)
// =============================================================================

/// Renders a whole database's additive DDL (create-database + every table).
struct DatabaseDdl<'a> {
    engine: DatabaseEngine,
    db: &'a DatabaseModel,
}

impl std::fmt::Display for DatabaseDdl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "-- shinka base schema for `{}` (extracted by /copy-model)",
            self.db.name
        )?;
        writeln!(
            f,
            "CREATE DATABASE IF NOT EXISTS {};",
            Ident(self.engine, &self.db.name)
        )?;
        writeln!(f, "USE {};", Ident(self.engine, &self.db.name))?;
        for table in &self.db.tables {
            writeln!(f)?;
            write!(
                f,
                "{}",
                TableDdl {
                    engine: self.engine,
                    table
                }
            )?;
        }
        Ok(())
    }
}

/// Renders one additive `CREATE TABLE IF NOT EXISTS` for a table.
struct TableDdl<'a> {
    engine: DatabaseEngine,
    table: &'a TableModel,
}

impl std::fmt::Display for TableDdl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "CREATE TABLE IF NOT EXISTS {} (",
            Ident(self.engine, &self.table.name)
        )?;
        let last = self.table.columns.len().saturating_sub(1);
        for (i, col) in self.table.columns.iter().enumerate() {
            let comma = if i < last || !self.table.primary_key.is_empty() {
                ","
            } else {
                ""
            };
            writeln!(
                f,
                "  {}{}",
                ColumnDdl {
                    engine: self.engine,
                    col
                },
                comma
            )?;
        }
        if !self.table.primary_key.is_empty() {
            let mut cols = String::new();
            for (i, pk) in self.table.primary_key.iter().enumerate() {
                if i > 0 {
                    cols.push_str(", ");
                }
                cols.push_str(&Ident(self.engine, pk).to_string());
            }
            writeln!(f, "  PRIMARY KEY ({cols})")?;
        }
        writeln!(f, ");")?;
        Ok(())
    }
}

/// Renders one column definition.
struct ColumnDdl<'a> {
    engine: DatabaseEngine,
    col: &'a ColumnModel,
}

impl std::fmt::Display for ColumnDdl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Ident(self.engine, &self.col.name),
            self.col.data_type
        )?;
        if self.col.nullable {
            write!(f, " NULL")?;
        } else {
            write!(f, " NOT NULL")?;
        }
        if let Some(def) = &self.col.default {
            write!(f, " DEFAULT {def}")?;
        }
        if self.col.auto_increment {
            match self.engine {
                // Postgres identity is expressed in the type; MySQL uses the
                // AUTO_INCREMENT column attribute.
                DatabaseEngine::Mysql => write!(f, " AUTO_INCREMENT")?,
                DatabaseEngine::Postgres => {}
            }
        }
        Ok(())
    }
}

/// A quoted identifier for the engine's dialect (backtick for MySQL, double
/// quote for Postgres). Rendered through a typed `Display` — never `format!`.
struct Ident<'a>(DatabaseEngine, &'a str);

impl std::fmt::Display for Ident<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DatabaseEngine::Mysql => write!(f, "`{}`", self.1.replace('`', "``")),
            DatabaseEngine::Postgres => write!(f, "\"{}\"", self.1.replace('"', "\"\"")),
        }
    }
}

// =============================================================================
// The real, sqlx-backed SchemaSource
// =============================================================================

/// The real [`SchemaSource`]: sqlx `information_schema` introspection against a
/// direct source, reusing [`DirectConnParams`] (the same typed connection
/// `direct.rs` applies through) so extract and apply address the source
/// identically.
pub struct SqlxSchemaSource {
    params: DirectConnParams,
    engine: DatabaseEngine,
    connect_timeout: Duration,
}

impl SqlxSchemaSource {
    /// Bind a real source to typed connection params + an engine + a timeout.
    pub fn new(params: DirectConnParams, engine: DatabaseEngine, connect_timeout: Duration) -> Self {
        Self {
            params,
            engine,
            connect_timeout,
        }
    }
}

#[async_trait::async_trait]
impl SchemaSource for SqlxSchemaSource {
    async fn list_databases(&self) -> Result<Vec<String>, ExtractError> {
        // LiveTODO(extract-live-introspect): real, compiled query paths; the
        // observation against a live engine is verified in-cluster, not in units.
        match self.engine {
            DatabaseEngine::Mysql => mysql_list_databases(&self.params, self.connect_timeout).await,
            DatabaseEngine::Postgres => {
                pg_list_databases(&self.params, self.connect_timeout).await
            }
        }
    }

    async fn list_columns(&self, database: &str) -> Result<Vec<RawColumn>, ExtractError> {
        match self.engine {
            DatabaseEngine::Mysql => {
                mysql_list_columns(&self.params, database, self.connect_timeout).await
            }
            DatabaseEngine::Postgres => {
                pg_list_columns(&self.params, database, self.connect_timeout).await
            }
        }
    }
}

/// MySQL system schemas excluded from extraction.
const MYSQL_SYSTEM_SCHEMAS: &[&str] =
    &["mysql", "information_schema", "performance_schema", "sys"];

async fn mysql_list_databases(
    params: &DirectConnParams,
    connect_timeout: Duration,
) -> Result<Vec<String>, ExtractError> {
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
    use sqlx::Row;

    let opts = MySqlConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.username)
        .password(&params.password);
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(connect_timeout)
        .connect_with(opts)
        .await
        .map_err(|e| ExtractError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    let rows = sqlx::query(
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| ExtractError::Introspect {
        what: "information_schema.schemata".to_string(),
        message: e.to_string(),
    })?;
    pool.close().await;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<String, _>("schema_name").ok())
        .filter(|s| !MYSQL_SYSTEM_SCHEMAS.contains(&s.as_str()))
        .collect())
}

async fn mysql_list_columns(
    params: &DirectConnParams,
    database: &str,
    connect_timeout: Duration,
) -> Result<Vec<RawColumn>, ExtractError> {
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
    use sqlx::Row;

    let opts = MySqlConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.username)
        .password(&params.password);
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(connect_timeout)
        .connect_with(opts)
        .await
        .map_err(|e| ExtractError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    // column_type carries the full MySQL type (e.g. `varchar(255)`, `int
    // unsigned`); column_key='PRI' marks primary-key membership.
    let rows = sqlx::query(
        "SELECT table_name, column_name, ordinal_position, column_type, is_nullable, \
         column_default, extra, column_key \
         FROM information_schema.columns WHERE table_schema = ? \
         ORDER BY table_name, ordinal_position",
    )
    .bind(database)
    .fetch_all(&pool)
    .await
    .map_err(|e| ExtractError::Introspect {
        what: "information_schema.columns".to_string(),
        message: e.to_string(),
    })?;
    pool.close().await;

    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let table: String = r.try_get("table_name").ok()?;
            let column: String = r.try_get("column_name").ok()?;
            let ordinal: u32 = r
                .try_get::<u32, _>("ordinal_position")
                .or_else(|_| r.try_get::<i64, _>("ordinal_position").map(|v| v as u32))
                .unwrap_or(0);
            let data_type: String = r.try_get("column_type").ok()?;
            let is_nullable: String = r.try_get("is_nullable").unwrap_or_default();
            let default: Option<String> = r.try_get("column_default").ok();
            let extra: String = r.try_get("extra").unwrap_or_default();
            let column_key: String = r.try_get("column_key").unwrap_or_default();
            Some(RawColumn {
                database: database.to_string(),
                table,
                column,
                ordinal,
                data_type,
                nullable: is_nullable.eq_ignore_ascii_case("YES"),
                default,
                auto_increment: extra.to_ascii_lowercase().contains("auto_increment"),
                primary_key: column_key.eq_ignore_ascii_case("PRI"),
            })
        })
        .collect())
}

/// Postgres schemas excluded from extraction.
const PG_SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema", "pg_toast"];

async fn pg_list_databases(
    params: &DirectConnParams,
    connect_timeout: Duration,
) -> Result<Vec<String>, ExtractError> {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::Row;

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
        .map_err(|e| ExtractError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    // Postgres "databases" for our model = user schemas of the connected DB.
    let rows = sqlx::query(
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| ExtractError::Introspect {
        what: "information_schema.schemata".to_string(),
        message: e.to_string(),
    })?;
    pool.close().await;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<String, _>("schema_name").ok())
        .filter(|s| !PG_SYSTEM_SCHEMAS.contains(&s.as_str()))
        .collect())
}

async fn pg_list_columns(
    params: &DirectConnParams,
    schema: &str,
    connect_timeout: Duration,
) -> Result<Vec<RawColumn>, ExtractError> {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::Row;

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
        .map_err(|e| ExtractError::Connect {
            target: params.to_string(),
            message: e.to_string(),
        })?;

    // LiveTODO(extract-constraints): postgres type fidelity here uses data_type;
    // folding character_maximum_length / numeric precision + identity detection
    // is the additive next step. Primary-key detection via a joined
    // key_column_usage query is likewise deferred.
    let rows = sqlx::query(
        "SELECT table_name, column_name, ordinal_position, data_type, is_nullable, \
         column_default, is_identity \
         FROM information_schema.columns WHERE table_schema = $1 \
         ORDER BY table_name, ordinal_position",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await
    .map_err(|e| ExtractError::Introspect {
        what: "information_schema.columns".to_string(),
        message: e.to_string(),
    })?;
    pool.close().await;

    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let table: String = r.try_get("table_name").ok()?;
            let column: String = r.try_get("column_name").ok()?;
            let ordinal: i32 = r.try_get("ordinal_position").unwrap_or(0);
            let data_type: String = r.try_get("data_type").ok()?;
            let is_nullable: String = r.try_get("is_nullable").unwrap_or_default();
            let default: Option<String> = r.try_get("column_default").ok();
            let is_identity: String = r.try_get("is_identity").unwrap_or_default();
            Some(RawColumn {
                database: schema.to_string(),
                table,
                column,
                ordinal: ordinal.max(0) as u32,
                data_type,
                nullable: is_nullable.eq_ignore_ascii_case("YES"),
                default,
                auto_increment: is_identity.eq_ignore_ascii_case("YES"),
                primary_key: false,
            })
        })
        .collect())
}

// =============================================================================
// Feed-to-shinka artifact: the typed base ConfigMap
// =============================================================================

/// Project the extracted base ops into a `sqlConfigMapRef`-shaped ConfigMap.
///
/// This IS "feed to shinka as base": each op becomes one `data` key (its
/// lexically-ordered `NN-<db>.sql` name), so a `directRef` `DatabaseMigration`
/// pointing its `sqlConfigMapRef` at this ConfigMap loads exactly these ops (via
/// [`crate::direct::KubeDirectEnv::load_sql`]) and applies them as the base.
/// Commit it; shinka absorbs it.
pub fn render_base_configmap(
    name: &str,
    namespace: &str,
    ops: &[SqlOp],
) -> k8s_openapi::api::core::v1::ConfigMap {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    let data: BTreeMap<String, String> =
        ops.iter().map(|op| (op.name.clone(), op.sql.clone())).collect();
    let labels = BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "shinka".to_string(),
        ),
        ("shinka.pleme.io/copy-model".to_string(), "base".to_string()),
    ]);
    k8s_openapi::api::core::v1::ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::direct::content_version;

    // ---- Mock source ------------------------------------------------------

    /// In-memory [`SchemaSource`] — no cluster, no database.
    struct MockSchemaSource {
        databases: Vec<String>,
        columns: Vec<RawColumn>,
    }

    #[async_trait::async_trait]
    impl SchemaSource for MockSchemaSource {
        async fn list_databases(&self) -> Result<Vec<String>, ExtractError> {
            Ok(self.databases.clone())
        }
        async fn list_columns(&self, database: &str) -> Result<Vec<RawColumn>, ExtractError> {
            Ok(self
                .columns
                .iter()
                .filter(|c| c.database == database)
                .cloned()
                .collect())
        }
    }

    fn col(db: &str, table: &str, ord: u32, name: &str, ty: &str, pk: bool) -> RawColumn {
        RawColumn {
            database: db.to_string(),
            table: table.to_string(),
            column: name.to_string(),
            ordinal: ord,
            data_type: ty.to_string(),
            nullable: !pk,
            default: None,
            auto_increment: pk && ty.starts_with("int"),
            primary_key: pk,
        }
    }

    /// An authdb-shaped source (the camelot fixture, mirroring direct.rs).
    fn authdb_source() -> MockSchemaSource {
        MockSchemaSource {
            databases: vec!["authdb".to_string()],
            columns: vec![
                col("authdb", "accesses", 1, "id", "int unsigned", true),
                col("authdb", "accesses", 2, "access_id", "varchar(255)", false),
                col("authdb", "accesses", 3, "rules_type", "varchar(64)", false),
                col("authdb", "tokens", 1, "id", "int unsigned", true),
                col("authdb", "tokens", 2, "value", "text", false),
            ],
        }
    }

    // ---- Pipeline tests ---------------------------------------------------

    #[tokio::test]
    async fn extract_builds_model_and_renders_additive_base() {
        let src = authdb_source();
        let (model, ops) = extract_base(
            DatabaseEngine::Mysql,
            &src,
            &DatabaseFilter::All,
            "root@mte-staging:3306",
        )
        .await
        .expect("extract succeeds");

        // Model shape.
        assert_eq!(model.databases.len(), 1);
        assert_eq!(model.table_count(), 2);
        assert_eq!(model.column_count(), 5);
        let authdb = &model.databases[0];
        assert_eq!(authdb.name, "authdb");
        // Tables sorted by name: accesses before tokens.
        assert_eq!(authdb.tables[0].name, "accesses");
        assert_eq!(authdb.tables[1].name, "tokens");
        // Primary key lifted.
        assert_eq!(authdb.tables[0].primary_key, vec!["id".to_string()]);

        // One base op, keyed for lexical apply order.
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].name, "00-authdb.sql");

        // Additive DDL — safe to apply against a fresh or partial target.
        let sql = &ops[0].sql;
        assert!(sql.contains("CREATE DATABASE IF NOT EXISTS `authdb`;"));
        assert!(sql.contains("USE `authdb`;"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `accesses` ("));
        assert!(sql.contains("`access_id` varchar(255) NULL"));
        assert!(sql.contains("`id` int unsigned NOT NULL AUTO_INCREMENT"));
        assert!(sql.contains("PRIMARY KEY (`id`)"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `tokens` ("));
    }

    #[tokio::test]
    async fn extract_output_is_a_shinka_base_configmap_and_content_addressable() {
        // The rendered ops ARE shinka SqlOps → they content-address exactly like
        // a `directRef` sqlConfigMapRef, so /copy-model can detect source drift
        // against the committed base with the SAME machine direct.rs applies.
        let src = authdb_source();
        let (_m, ops) = extract_base(DatabaseEngine::Mysql, &src, &DatabaseFilter::All, "t")
            .await
            .unwrap();

        let v1 = content_version(&ops);
        assert!(v1.starts_with("sql-"), "content-addressed like a directRef base");

        // Re-extract of the same source → identical bytes → identical version.
        let (_m2, ops2) = extract_base(DatabaseEngine::Mysql, &authdb_source(), &DatabaseFilter::All, "t")
            .await
            .unwrap();
        assert_eq!(content_version(&ops2), v1, "deterministic re-extract");

        // A schema change (new column) → a new version → shinka re-applies.
        let mut changed = authdb_source();
        changed
            .columns
            .push(col("authdb", "tokens", 3, "issued_at", "datetime", false));
        let (_m3, ops3) = extract_base(DatabaseEngine::Mysql, &changed, &DatabaseFilter::All, "t")
            .await
            .unwrap();
        assert_ne!(content_version(&ops3), v1, "schema drift → new base version");
    }

    #[tokio::test]
    async fn extract_filter_scopes_and_orders_databases() {
        let src = MockSchemaSource {
            databases: vec!["authdb".to_string(), "uamdb".to_string(), "sys".to_string()],
            columns: vec![
                col("authdb", "a", 1, "id", "int", true),
                col("uamdb", "u", 1, "id", "int", true),
                col("sys", "junk", 1, "x", "int", false),
            ],
        };
        // Only the two akeyless service dbs, in the filter's order.
        let filter = DatabaseFilter::Only(vec!["uamdb".to_string(), "authdb".to_string()]);
        let (model, ops) = extract_base(DatabaseEngine::Mysql, &src, &filter, "t")
            .await
            .unwrap();
        assert_eq!(model.databases.len(), 2);
        assert_eq!(ops.len(), 2);
        // Filter order preserved → uamdb first (00-), authdb second (01-).
        assert_eq!(ops[0].name, "00-uamdb.sql");
        assert_eq!(ops[1].name, "01-authdb.sql");
    }

    #[tokio::test]
    async fn extract_empty_source_is_a_typed_error_not_silent_success() {
        let src = MockSchemaSource {
            databases: vec![],
            columns: vec![],
        };
        let err = extract_base(DatabaseEngine::Mysql, &src, &DatabaseFilter::All, "root@t")
            .await
            .expect_err("empty source is a typed error");
        assert!(matches!(err, ExtractError::EmptySchema { .. }));
    }

    #[tokio::test]
    async fn extract_filter_selecting_nothing_is_a_typed_error() {
        let src = authdb_source();
        let filter = DatabaseFilter::Only(vec!["does-not-exist".to_string()]);
        let err = extract_base(DatabaseEngine::Mysql, &src, &filter, "root@t")
            .await
            .expect_err("filter matched nothing");
        assert!(matches!(err, ExtractError::EmptySchema { .. }));
    }

    // ---- Pure-render tests ------------------------------------------------

    #[test]
    fn postgres_dialect_uses_double_quote_idents_and_no_auto_increment_attr() {
        let model = build_schema_model(
            DatabaseEngine::Postgres,
            vec!["public".to_string()],
            vec![
                RawColumn {
                    database: "public".to_string(),
                    table: "users".to_string(),
                    column: "id".to_string(),
                    ordinal: 1,
                    data_type: "integer".to_string(),
                    nullable: false,
                    default: Some("nextval('users_id_seq')".to_string()),
                    auto_increment: true,
                    primary_key: true,
                },
            ],
        );
        let ops = render_base_ops(&model);
        let sql = &ops[0].sql;
        assert!(sql.contains("CREATE DATABASE IF NOT EXISTS \"public\";"));
        assert!(sql.contains("\"id\" integer NOT NULL DEFAULT nextval('users_id_seq')"));
        // Postgres identity is in the type/default, not an AUTO_INCREMENT attr.
        assert!(!sql.contains("AUTO_INCREMENT"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn build_schema_model_is_deterministic_regardless_of_row_order() {
        let ordered = vec![
            col("db", "t", 1, "a", "int", true),
            col("db", "t", 2, "b", "text", false),
        ];
        let mut shuffled = ordered.clone();
        shuffled.reverse();
        let m1 = build_schema_model(DatabaseEngine::Mysql, vec!["db".to_string()], ordered);
        let m2 = build_schema_model(DatabaseEngine::Mysql, vec!["db".to_string()], shuffled);
        assert_eq!(m1, m2, "assembly is order-independent");
        // Columns in ordinal order.
        assert_eq!(m1.databases[0].tables[0].columns[0].name, "a");
        assert_eq!(m1.databases[0].tables[0].columns[1].name, "b");
    }

    #[tokio::test]
    async fn base_configmap_is_a_sqlconfigmapref_shaped_object() {
        let src = authdb_source();
        let (_m, ops) = extract_base(DatabaseEngine::Mysql, &src, &DatabaseFilter::All, "t")
            .await
            .unwrap();
        let cm = render_base_configmap("akeyless-schema-apply-sql", "camelot", &ops);
        assert_eq!(cm.metadata.name.as_deref(), Some("akeyless-schema-apply-sql"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("camelot"));
        let data = cm.data.expect("data present");
        // Keys are the ordered op names → direct.rs loads them in lexical order.
        assert!(data.contains_key("00-authdb.sql"));
        assert!(data["00-authdb.sql"].contains("CREATE TABLE IF NOT EXISTS `accesses`"));
    }

    #[test]
    fn ident_escapes_backtick_and_double_quote() {
        assert_eq!(Ident(DatabaseEngine::Mysql, "a`b").to_string(), "`a``b`");
        assert_eq!(
            Ident(DatabaseEngine::Postgres, "a\"b").to_string(),
            "\"a\"\"b\""
        );
    }

    #[test]
    fn table_with_no_columns_still_renders_valid_additive_ddl() {
        // A table reported with no columns (edge) renders an empty create body
        // without a dangling comma.
        let model = SchemaModel {
            engine: DatabaseEngine::Mysql,
            databases: vec![DatabaseModel {
                name: "db".to_string(),
                tables: vec![TableModel {
                    name: "empty".to_string(),
                    columns: vec![],
                    primary_key: vec![],
                }],
            }],
        };
        let ops = render_base_ops(&model);
        assert!(ops[0].sql.contains("CREATE TABLE IF NOT EXISTS `empty` ("));
        assert!(!ops[0].sql.contains(",\n);"), "no dangling comma");
    }
}
