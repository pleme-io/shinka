//! `/copy-model` — the shinka-aware model-copy capability.
//!
//! Shinka *imposes the model*: a `directRef` migration's model is a keyed,
//! lexically-ordered set of [`SqlOp`]s (a `sqlConfigMapRef` ConfigMap) that
//! shinka content-addresses and applies **additively**. `/copy-model` is the
//! general capability that *copies a data model from one environment into that
//! format as a base*, then *molds it forward* with shinka's additive
//! model-evolution. The pipeline, and which shipped primitive realizes each leg:
//!
//! ```text
//!   1. EXTRACT   (extract.rs)  live source  → SchemaModel → base Vec<SqlOp>
//!   2. FEED      (this module) base ops      → sqlConfigMapRef ConfigMap
//!   3. ABSORB    (this module) base ConfigMap + directRef DatabaseMigration CR   (the bundle)
//!   4. MOLD      (this module) base ops + additive EvolutionOps → one ordered Vec<SqlOp>
//!   5. CAMELOT   (this module) SchemaModel   → per-service-DB keyed ops, dep-ordered
//! ```
//!
//! Legs 1–2 landed as `/copy-model` M0 in [`crate::extract`]. This module adds
//! legs 3–5: the absorb-as-base *bundle* (the base ConfigMap **and** the
//! `directRef` CR that points at it, so "absorb" is one emitted artifact set,
//! not a hand-wired CR), the **mold** algebra (additive/safe evolution composed
//! onto the base), and the **camelot** projection (the extracted model rendered
//! to the validated per-service-DB convention in the contract's dependency
//! order).
//!
//! ## Safe by construction — destructive evolution is unrepresentable
//!
//! [`EvolutionOp`] has **no** `DropTable` / `DropColumn` / `RenameColumn` /
//! `Truncate` arm. A destructive evolution therefore has *no code path* — the
//! "safe" property of the shinka model-evolution framework is
//! *truly-unrepresentable at the type level on the destructive axis*
//! (UNREPRESENTABILITY), not a runtime guard. Every representable evolution
//! renders additive DDL (`CREATE … IF NOT EXISTS`) or is an
//! operator-owned additive escape hatch.
//!
//! ## Tier honesty
//!
//! - **Shipped + unit-tested (pure, mock/fixture-driven):** the `EvolutionOp`
//!   vocabulary + its additive rendering, [`render_mold`] (base ⊕ evolutions →
//!   one content-addressed op set), [`project_in_order`] / [`project_camelot`]
//!   (dependency-ordered per-service-DB projection), and the absorb-as-base
//!   [`render_bundle`] (base ConfigMap + `directRef` CR).
//! - **Shipped mold vocabulary (M1):** `CreateDatabase` (a whole new service DB,
//!   fully idempotent both engines) + `RawAdditive` (operator-owned additive
//!   SQL). `AddTable` / `AddColumn` as typed arms are a named
//!   `LiveTODO(mold-typed-alter)` — the model already carries the fields;
//!   engine-correct idempotent `ALTER` rendering (MySQL lacks `ADD COLUMN IF NOT
//!   EXISTS`) is the additive next step, not a reshape.
//! - **`LiveTODO(copy-model-active)`** — the *active* tier of the model-evolution
//!   framework (continuously mold the live DB toward a declared typed target;
//!   `to-spec` + `enjulho`-declarative) is designed in
//!   `theory/SHINKA-MODEL-EVOLUTION.md`, not shipped. `/copy-model` ships the
//!   *base + additive-overlay* tier that the active controller will drive.
//! - **Scope boundary — schema only.** `/copy-model` owns the **schema/model**
//!   leg. The full camelot data contract (per-service TOML confs, the uam RSA
//!   `access_id`, S3 buckets, `mysql_native_password`) is `camelot-bootstrap`'s
//!   job; [`project_camelot`] emits *only* the per-service-DB DDL in the
//!   contract's order.

use k8s_openapi::api::core::v1::ConfigMap;

use crate::crd::{
    ConfigMapRef, DatabaseEngine, DatabaseMigration, DatabaseMigrationSpec, DatabaseSpec,
    DirectDatabaseRef, SecretRef,
};
use crate::direct::{content_version, SqlOp};
use crate::extract::{render_base_configmap, render_base_ops, render_database_ddl, DatabaseModel,
    SchemaModel};

// =============================================================================
// The mold algebra — additive, safe-by-construction evolutions
// =============================================================================

/// One additive evolution molded onto an absorbed base.
///
/// **Safe by construction:** there is no destructive arm (`DropTable`,
/// `DropColumn`, `RenameColumn`, `Truncate` do not exist), so a destructive
/// evolution is *unrepresentable* — the compiler refuses it. Every arm renders
/// additive DDL or carries operator-owned additive SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvolutionOp {
    /// Grow the model with a whole new database (create-database + every table),
    /// rendered additively (`CREATE DATABASE/TABLE IF NOT EXISTS`) — idempotent
    /// on both engines. The common camelot case: adding a new service DB.
    CreateDatabase(DatabaseModel),

    /// An operator-authored additive SQL document, keyed for lexical apply
    /// order. The escape hatch for evolutions the typed vocabulary does not yet
    /// cover (seed `INSERT`s, additive `ALTER`s). The operator owns its
    /// engine-correctness and idempotency — it is *additive by intent*, not by
    /// a type guarantee.
    RawAdditive {
        /// Short evolution name (becomes part of the lexically-ordered op key).
        key: String,
        /// The additive SQL to apply.
        sql: String,
    },
}

impl EvolutionOp {
    /// Render this evolution into a keyed [`SqlOp`]. The key is `evo-NN-<name>.sql`
    /// so it lexically sorts **after** every base op (whose keys begin with a
    /// digit — `evo-` begins with a letter, which sorts after any digit).
    fn render(&self, engine: DatabaseEngine, idx: usize) -> SqlOp {
        match self {
            EvolutionOp::CreateDatabase(db) => SqlOp {
                name: EvolutionKey { idx, name: &db.name }.to_string(),
                sql: render_database_ddl(engine, db),
            },
            EvolutionOp::RawAdditive { key, sql } => SqlOp {
                name: EvolutionKey { idx, name: key }.to_string(),
                sql: sql.clone(),
            },
        }
    }
}

/// A lexically-sortable evolution op key (`evo-NN-<name>.sql`), rendered through
/// a typed `Display` surface (no `format!`). The `evo-` prefix begins with a
/// letter so every evolution sorts after every digit-prefixed base op; the
/// two-digit index orders evolutions among themselves.
struct EvolutionKey<'a> {
    idx: usize,
    name: &'a str,
}

impl std::fmt::Display for EvolutionKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "evo-{:02}-{}.sql", self.idx, self.name)
    }
}

/// A mold plan: an absorbed base plus the additive evolutions to compose onto it.
///
/// The base is the [`SqlOp`] set from [`crate::extract::extract_base`] (or a
/// [`project_camelot`] projection). Evolutions are composed *after* the base in
/// lexical apply order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoldPlan {
    /// Engine the ops target (selects DDL dialect for rendered evolutions).
    pub engine: DatabaseEngine,
    /// The absorbed base ops (already keyed by extract / projection).
    pub base: Vec<SqlOp>,
    /// The additive evolutions to mold onto the base, in application order.
    pub evolutions: Vec<EvolutionOp>,
}

/// Compose a [`MoldPlan`] into one ordered [`SqlOp`] set: the base ops verbatim,
/// then every evolution rendered and keyed to sort after the base.
///
/// The result is shinka-native by construction — it *is* a `Vec<SqlOp>`, the
/// same type `direct.rs` applies — and content-addresses via
/// [`content_version`], so molding produces a new version shinka re-applies
/// (additively, so re-apply against the already-molded target is safe).
pub fn render_mold(plan: &MoldPlan) -> Vec<SqlOp> {
    let mut ops = plan.base.clone();
    for (idx, evo) in plan.evolutions.iter().enumerate() {
        ops.push(evo.render(plan.engine, idx));
    }
    ops
}

/// The content-address version of a molded op set (the marker shinka records in
/// `status.lastMigration.imageTag`, exactly like a `directRef` base).
pub fn mold_version(plan: &MoldPlan) -> String {
    content_version(&render_mold(plan))
}

// =============================================================================
// The camelot projection — extracted model → per-service-DB, dep-ordered
// =============================================================================

/// The akeyless service→DB dependency order the camelot data contract applies
/// in: `auth` is the identity root, `uam` needs `auth`, and the remaining
/// services read `authdb`/`uamdb`. Databases not named here are appended in name
/// order (so an unexpected source DB is never dropped — only ordered last).
///
/// This is the *schema-leg* view of the contract's documented per-service
/// dependency chain (uam→auth, bis→auth+uam). `sdr` uses neo4j (not a SQL
/// database) and so is intentionally absent from the SQL projection.
pub const CAMELOT_SERVICE_DB_ORDER: &[&str] =
    &["authdb", "uamdb", "gatordb", "kfmdb", "bisdb", "logandb"];

/// Project an extracted [`SchemaModel`] into shinka base ops in the camelot
/// service-dependency order (the validated per-service-DB convention).
pub fn project_camelot(model: &SchemaModel) -> Vec<SqlOp> {
    project_in_order(model, CAMELOT_SERVICE_DB_ORDER)
}

/// Project an extracted [`SchemaModel`] into shinka base ops, reordering its
/// databases so those named in `order` come first (in `order`'s sequence) and
/// any remaining databases follow in their existing (name-sorted) order.
///
/// Pure — reuses [`render_base_ops`], so the keyed `NN-<db>.sql` op format,
/// additive DDL, and content-addressing are identical to a plain extract; only
/// the *database order* changes (which sets the apply sequence).
pub fn project_in_order(model: &SchemaModel, order: &[&str]) -> Vec<SqlOp> {
    let mut ordered: Vec<DatabaseModel> = Vec::with_capacity(model.databases.len());

    // Listed databases first, in the given order (skip any the source lacks).
    for wanted in order {
        if let Some(db) = model.databases.iter().find(|d| d.name == *wanted) {
            ordered.push(db.clone());
        }
    }
    // Then every remaining database (source order is already name-sorted).
    for db in &model.databases {
        if !order.iter().any(|w| *w == db.name) {
            ordered.push(db.clone());
        }
    }

    let reordered = SchemaModel {
        engine: model.engine,
        databases: ordered,
    };
    render_base_ops(&reordered)
}

// =============================================================================
// The absorb-as-base bundle — base ConfigMap + directRef DatabaseMigration CR
// =============================================================================

/// Everything needed to make shinka absorb an extracted schema as a base: the
/// `sqlConfigMapRef` ConfigMap (the ops) and the `directRef` `DatabaseMigration`
/// that points at it. Commit both; shinka's direct branch applies the base.
#[derive(Clone, Debug)]
pub struct CopyModelBundle {
    /// The `sqlConfigMapRef`-shaped ConfigMap carrying the base ops.
    pub config_map: ConfigMap,
    /// The `directRef` `DatabaseMigration` that absorbs the ConfigMap as a base.
    pub migration: DatabaseMigration,
}

/// Where the absorb-as-base CR applies the model (the *target*, distinct from the
/// *source* the ops were extracted from).
#[derive(Clone, Debug)]
pub struct AbsorbTarget {
    /// Emitted object names + namespace.
    pub migration_name: String,
    pub namespace: String,
    pub config_map_name: String,
    /// The target engine + host the `directRef` CR applies the base into.
    pub engine: DatabaseEngine,
    pub target_host: String,
    /// The Secret + key holding the target admin password.
    pub credentials_secret: String,
    pub password_key: String,
}

/// Render the `directRef` `DatabaseMigration` that absorbs `config_map_name` as
/// a base against the target. No migrator is set — a `directRef` migration
/// applies the ConfigMap's ops directly (the direct reconcile branch).
pub fn render_directref_migration(target: &AbsorbTarget) -> DatabaseMigration {
    let spec = DatabaseMigrationSpec {
        database: DatabaseSpec {
            cnpg_cluster_ref: None,
            direct_ref: Some(DirectDatabaseRef {
                engine: target.engine,
                host: target.target_host.clone(),
                port: None,
                database: None,
                username: None,
                credentials_secret_ref: SecretRef {
                    name: target.credentials_secret.clone(),
                },
                password_key: target.password_key.clone(),
                sql_config_map_ref: ConfigMapRef {
                    name: target.config_map_name.clone(),
                },
            }),
        },
        migrator: None,
        migrators: None,
        safety: Default::default(),
        timeouts: Default::default(),
    };
    let mut migration = DatabaseMigration::new(&target.migration_name, spec);
    migration.metadata.namespace = Some(target.namespace.clone());
    let labels = migration.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "shinka".to_string(),
    );
    labels.insert("shinka.pleme.io/copy-model".to_string(), "absorb".to_string());
    migration
}

/// Render the full absorb-as-base bundle from a set of base `ops` + a target:
/// the base ConfigMap **and** the `directRef` CR that points at it.
pub fn render_bundle(ops: &[SqlOp], target: &AbsorbTarget) -> CopyModelBundle {
    CopyModelBundle {
        config_map: render_base_configmap(&target.config_map_name, &target.namespace, ops),
        migration: render_directref_migration(target),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{ColumnModel, TableModel};

    fn table(name: &str, cols: &[(&str, &str, bool)]) -> TableModel {
        TableModel {
            name: name.to_string(),
            columns: cols
                .iter()
                .map(|(n, t, _pk)| ColumnModel {
                    name: n.to_string(),
                    data_type: t.to_string(),
                    nullable: true,
                    default: None,
                    auto_increment: false,
                })
                .collect(),
            primary_key: cols
                .iter()
                .filter(|(_, _, pk)| *pk)
                .map(|(n, _, _)| n.to_string())
                .collect(),
        }
    }

    fn db(name: &str, tables: Vec<TableModel>) -> DatabaseModel {
        DatabaseModel {
            name: name.to_string(),
            tables,
        }
    }

    fn base_authdb() -> Vec<SqlOp> {
        let model = SchemaModel {
            engine: DatabaseEngine::Mysql,
            databases: vec![db(
                "authdb",
                vec![table("accesses", &[("id", "int unsigned", true)])],
            )],
        };
        render_base_ops(&model)
    }

    // ---- Mold tests -------------------------------------------------------

    #[test]
    fn mold_composes_base_then_evolutions_in_lexical_apply_order() {
        let base = base_authdb();
        let plan = MoldPlan {
            engine: DatabaseEngine::Mysql,
            base: base.clone(),
            evolutions: vec![
                EvolutionOp::CreateDatabase(db(
                    "cachedb",
                    vec![table("entries", &[("k", "varchar(64)", true)])],
                )),
                EvolutionOp::RawAdditive {
                    key: "seed-roles".to_string(),
                    sql: "INSERT INTO `authdb`.`roles` (name) VALUES ('admin');".to_string(),
                },
            ],
        };
        let ops = render_mold(&plan);
        // base op + 2 evolutions.
        assert_eq!(ops.len(), 3);
        // Base op keeps its extract key; evolutions keyed to sort after it.
        assert_eq!(ops[0].name, "00-authdb.sql");
        assert_eq!(ops[1].name, "evo-00-cachedb.sql");
        assert_eq!(ops[2].name, "evo-01-seed-roles.sql");
        // Keys are already in strict lexical order (direct.rs applies them so).
        let keys: Vec<&str> = ops.iter().map(|o| o.name.as_str()).collect();
        let sorted = {
            let mut k = keys.clone();
            k.sort_unstable();
            k
        };
        assert_eq!(keys, sorted, "base sorts before every evolution");
        // The CreateDatabase evolution renders additive, idempotent DDL.
        assert!(ops[1].sql.contains("CREATE DATABASE IF NOT EXISTS `cachedb`;"));
        assert!(ops[1].sql.contains("CREATE TABLE IF NOT EXISTS `entries` ("));
    }

    #[test]
    fn mold_is_content_addressed_and_drift_sensitive() {
        let base = base_authdb();
        let plan = MoldPlan {
            engine: DatabaseEngine::Mysql,
            base: base.clone(),
            evolutions: vec![EvolutionOp::RawAdditive {
                key: "seed".to_string(),
                sql: "INSERT INTO t VALUES (1);".to_string(),
            }],
        };
        let v1 = mold_version(&plan);
        assert!(v1.starts_with("sql-"), "molds address like a directRef base");

        // Same plan → same version (deterministic).
        assert_eq!(mold_version(&plan.clone()), v1);

        // A changed evolution → a new version → shinka re-applies.
        let mut plan2 = plan.clone();
        if let EvolutionOp::RawAdditive { sql, .. } = &mut plan2.evolutions[0] {
            *sql = "INSERT INTO t VALUES (2);".to_string();
        }
        assert_ne!(mold_version(&plan2), v1, "evolution drift → new version");

        // The bare base (no evolutions) is a distinct version from any mold.
        let bare = MoldPlan {
            engine: DatabaseEngine::Mysql,
            base,
            evolutions: vec![],
        };
        assert_ne!(mold_version(&bare), v1, "molding changes the version");
    }

    // ---- Camelot projection tests ----------------------------------------

    #[test]
    fn project_camelot_orders_by_service_dependency_and_keeps_unlisted() {
        // Source reports databases out of contract order + one extra.
        let model = SchemaModel {
            engine: DatabaseEngine::Mysql,
            databases: vec![
                db("uamdb", vec![table("u", &[("id", "int", true)])]),
                db("authdb", vec![table("a", &[("id", "int", true)])]),
                db("scratchdb", vec![table("s", &[("id", "int", true)])]),
            ],
        };
        let ops = project_camelot(&model);
        // authdb before uamdb (dep order), scratchdb (unlisted) last.
        assert_eq!(ops[0].name, "00-authdb.sql");
        assert_eq!(ops[1].name, "01-uamdb.sql");
        assert_eq!(ops[2].name, "02-scratchdb.sql");
        // Still additive DDL — the projection only reorders.
        assert!(ops[0].sql.contains("CREATE DATABASE IF NOT EXISTS `authdb`;"));
    }

    #[test]
    fn project_in_order_drops_nothing_and_renders_all_present() {
        let model = SchemaModel {
            engine: DatabaseEngine::Postgres,
            databases: vec![db("public", vec![table("t", &[("id", "integer", true)])])],
        };
        // An order naming a database the source lacks must not fabricate it.
        let ops = project_in_order(&model, &["missingdb", "public"]);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].name, "00-public.sql");
    }

    // ---- Absorb-as-base bundle tests -------------------------------------

    #[test]
    fn bundle_emits_configmap_and_a_directref_migration_pointing_at_it() {
        let ops = base_authdb();
        let target = AbsorbTarget {
            migration_name: "copy-model-authdb".to_string(),
            namespace: "camelot".to_string(),
            config_map_name: "akeyless-schema-apply-sql".to_string(),
            engine: DatabaseEngine::Mysql,
            target_host: "akeyless-saas-akeyless-mysql".to_string(),
            credentials_secret: "akeyless-mysql-root".to_string(),
            password_key: "password".to_string(),
        };
        let bundle = render_bundle(&ops, &target);

        // The ConfigMap is the sqlConfigMapRef feed artifact.
        let cm = &bundle.config_map;
        assert_eq!(cm.metadata.name.as_deref(), Some("akeyless-schema-apply-sql"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("camelot"));
        assert!(cm.data.as_ref().unwrap().contains_key("00-authdb.sql"));

        // The migration is a directRef CR whose sqlConfigMapRef IS that ConfigMap.
        let spec = &bundle.migration.spec;
        let direct = spec
            .database
            .require_direct_ref()
            .expect("directRef source");
        assert_eq!(direct.host, "akeyless-saas-akeyless-mysql");
        assert_eq!(direct.engine, DatabaseEngine::Mysql);
        assert_eq!(direct.sql_config_map_ref.name, "akeyless-schema-apply-sql");
        assert_eq!(direct.credentials_secret_ref.name, "akeyless-mysql-root");
        // No CNPG source, no migrator — a pure direct absorb.
        assert!(spec.database.cnpg_cluster_ref.is_none());
        assert!(spec.migrator.is_none());
        assert!(spec.migrators.is_none());
        assert_eq!(
            bundle.migration.metadata.namespace.as_deref(),
            Some("camelot")
        );
    }

    #[test]
    fn bundle_migration_serialises_to_a_valid_directref_yaml() {
        let ops = base_authdb();
        let target = AbsorbTarget {
            migration_name: "copy-model-authdb".to_string(),
            namespace: "camelot".to_string(),
            config_map_name: "akeyless-schema-apply-sql".to_string(),
            engine: DatabaseEngine::Mysql,
            target_host: "mysql".to_string(),
            credentials_secret: "root-creds".to_string(),
            password_key: "password".to_string(),
        };
        let m = render_directref_migration(&target);
        let yaml = serde_yaml::to_string(&m).expect("serialises");
        assert!(yaml.contains("kind: DatabaseMigration"));
        assert!(yaml.contains("directRef:"));
        assert!(yaml.contains("sqlConfigMapRef:"));
        // Round-trips back through the source resolver.
        let back: DatabaseMigration = serde_yaml::from_str(&yaml).expect("round-trips");
        assert!(back.spec.database.require_direct_ref().is_ok());
    }
}
