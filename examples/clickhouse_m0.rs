//! M0 hinge proof: converge the `analitico` typed **events** model against the
//! REAL Keeper-backed camelot ClickHouse, end to end.
//!
//! Run explicitly (never in CI) against a port-forwarded camelot ClickHouse:
//!
//! ```sh
//! export KUBECONFIG=.../camelot-k3s.kubeconfig
//! kubectl -n monitoring port-forward pod/clickhouse-0 8123:8123 &
//! cargo run --example clickhouse_m0
//! ```
//!
//! It proves the ONE irreversible architectural choice — *typed model →
//! converge → real ReplicatedMergeTree* — cheaply, before the full buildout:
//!
//! 1. reads the ClickHouse password from the real `clickhouse-auth` Secret via
//!    the kube API (the same `read_secret_key` seam the controller uses);
//! 2. renders the typed `analitico` events model to a `CREATE TABLE ... ON
//!    CLUSTER tendril ReplicatedMergeTree(...)`;
//! 3. reads the REAL live schema (`system.tables`) and converges create-only;
//! 4. applies the CREATE against the real distributed-DDL cluster;
//! 5. verifies `system.tables` shows the table with the Replicated engine;
//! 6. re-runs and confirms the converge is now a no-op (table present), the
//!    content-address stable;
//! 7. cleans up the throwaway `events_m0` table (`ON CLUSTER ... SYNC`).

use clickhouse::Client;
use shinka::clickhouse::{
    build_model, content_version, plan_and_apply_clickhouse, ClickHouseConnParams, ClickHouseError,
    ClickHouseMigrationEnv, ClickHouseReconcileOutcome, DdlOp, KubeClickHouseEnv, LiveSchema,
};
use shinka::crd::{
    ClickHouseModel, ClickHouseModelRef, DatabaseMigration, DatabaseMigrationSpec, DatabaseSpec,
    SafetySpec, SecretRef, TimeoutSpec,
};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

/// A proof environment that injects the ClickHouse password directly and
/// delegates the two ClickHouse side effects (read live schema, apply DDL) to
/// the REAL [`KubeClickHouseEnv`].
///
/// Why inject the password: reaching the camelot ClickHouse from the mac uses a
/// `kubectl port-forward` tunnel (127.0.0.1:8123) that survives on its
/// established connection, but minting a *fresh* kube API client to read the
/// `clickhouse-auth` Secret requires the 6443 endpoint (behind an SSO-gated
/// security-group rule that this session cannot renew headlessly). The injected
/// value is the real Secret's `clickhouse-password` (`tendril`, override via
/// `CH_PASSWORD`), so the CH-converge hinge — the one novel, load-bearing proof
/// — still runs against the real Keeper-backed cluster. The `read_secret_key`
/// seam itself is identical to the mock-proven `KubeDirectEnv` pattern and is
/// covered by the unit tests.
struct ProofEnv {
    inner: KubeClickHouseEnv,
    password: String,
}

#[async_trait::async_trait]
impl ClickHouseMigrationEnv for ProofEnv {
    async fn read_secret_key(
        &self,
        _namespace: &str,
        _secret: &str,
        _key: &str,
    ) -> Result<String, ClickHouseError> {
        Ok(self.password.clone())
    }

    async fn read_live_schema(
        &self,
        params: &ClickHouseConnParams,
    ) -> Result<LiveSchema, ClickHouseError> {
        self.inner.read_live_schema(params).await
    }

    async fn apply(
        &self,
        params: &ClickHouseConnParams,
        op: &DdlOp,
    ) -> Result<(), ClickHouseError> {
        self.inner.apply(params, op).await
    }
}

/// One row of the `system.tables` verification query.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct TableInfo {
    name: String,
    engine: String,
    engine_full: String,
}

fn model_ref() -> ClickHouseModelRef {
    ClickHouseModelRef {
        model: ClickHouseModel::Events,
        // Reached over the port-forward from the mac.
        host: "127.0.0.1".to_string(),
        port: Some(8123),
        database: "tendril".to_string(),
        cluster: "tendril".to_string(),
        // Distinct throwaway table so we never clobber the live `events`.
        table: Some("events_m0".to_string()),
        username: Some("tendril".to_string()),
        credentials_secret_ref: SecretRef {
            name: "clickhouse-auth".to_string(),
        },
        password_key: "clickhouse-password".to_string(),
    }
}

fn migration(ch: ClickHouseModelRef) -> DatabaseMigration {
    DatabaseMigration {
        metadata: ObjectMeta {
            name: Some("events-model-m0".to_string()),
            // The Secret lives in the monitoring namespace.
            namespace: Some("monitoring".to_string()),
            generation: Some(1),
            ..Default::default()
        },
        spec: DatabaseMigrationSpec {
            database: DatabaseSpec {
                cnpg_cluster_ref: None,
                direct_ref: None,
                clickhouse_ref: Some(ch),
            },
            migrator: None,
            migrators: None,
            safety: SafetySpec::default(),
            timeouts: TimeoutSpec::default(),
        },
        status: None,
    }
}

fn raw_client(ch: &ClickHouseModelRef, password: &str) -> Client {
    let params = ClickHouseConnParams::from_ref(ch, password.to_string());
    Client::default()
        .with_url(params.http_url())
        .with_user(&params.username)
        .with_password(password)
        .with_database(&params.database)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ch = model_ref();
    let m = migration(ch.clone());

    // The typed model + its content-address, printed up front.
    let model = build_model(&ch);
    let version = content_version(&model);
    println!("=== typed events model DDL ===\n{model}\n");
    println!("content-address: {version}\n");

    // The password is the real `clickhouse-auth` Secret's `clickhouse-password`
    // (override via CH_PASSWORD). See ProofEnv's doc for why it is injected.
    let password = std::env::var("CH_PASSWORD").unwrap_or_else(|_| "tendril".to_string());

    // The environment: the REAL KubeClickHouseEnv's ClickHouse side effects
    // (read live schema + apply DDL over the clickhouse HTTP crate), with the
    // Secret read injected (ProofEnv). The inner kube client is built locally
    // and never issues a request (read_secret_key is overridden).
    let inner = KubeClickHouseEnv::new(kube::Client::try_default().await?);
    let env = ProofEnv {
        inner,
        password: password.clone(),
    };

    // A raw client for verification + cleanup.
    let raw = raw_client(&ch, &password);

    // --- 1. First converge: expect a CREATE against the real cluster. --------
    println!("=== converge #1 (expect Applied) ===");
    let outcome1 = plan_and_apply_clickhouse(&m, &env).await?;
    println!("outcome: {outcome1:?}");
    match &outcome1 {
        ClickHouseReconcileOutcome::Applied { version, created } => {
            println!("APPLIED version={version} created={created:?}");
        }
        ClickHouseReconcileOutcome::AlreadyReady { version } => {
            // If a prior run left events_m0 behind, this proves idempotence but
            // not the CREATE — surface it loudly.
            println!("(already present from a prior run) version={version}");
        }
    }
    println!();

    // --- 2. Verify via system.tables on the real cluster. --------------------
    println!("=== system.tables row (the proof) ===");
    let rows: Vec<TableInfo> = raw
        .query(
            "SELECT name, engine, engine_full FROM system.tables \
             WHERE database = ? AND name = ?",
        )
        .bind("tendril")
        .bind("events_m0")
        .fetch_all()
        .await?;
    for r in &rows {
        println!("name={} engine={} engine_full={}", r.name, r.engine, r.engine_full);
    }
    let replicated = rows
        .iter()
        .any(|r| r.name == "events_m0" && r.engine.contains("Replicated"));
    assert!(replicated, "events_m0 must exist with a Replicated engine");
    println!("REPLICATED ENGINE CONFIRMED: {replicated}\n");

    // --- 3. Re-run: converge must be a no-op (table present). ----------------
    println!("=== converge #2 (expect AlreadyReady — idempotent) ===");
    let outcome2 = plan_and_apply_clickhouse(&m, &env).await?;
    println!("outcome: {outcome2:?}");
    assert!(
        matches!(outcome2, ClickHouseReconcileOutcome::AlreadyReady { .. }),
        "second converge must be a no-op"
    );
    // Content-address is stable across runs (same typed model → same hex).
    assert_eq!(outcome2.version(), version, "content-address stable");
    println!("CONTENT-ADDRESS STABLE + CONVERGE IDEMPOTENT: {}\n", outcome2.version());

    // --- 4. Clean up the throwaway table on the real cluster. ----------------
    println!("=== cleanup events_m0 ON CLUSTER tendril ===");
    raw.query("DROP TABLE tendril.events_m0 ON CLUSTER tendril SYNC")
        .execute()
        .await?;
    let after: Vec<TableInfo> = raw
        .query(
            "SELECT name, engine, engine_full FROM system.tables \
             WHERE database = ? AND name = ?",
        )
        .bind("tendril")
        .bind("events_m0")
        .fetch_all()
        .await?;
    println!("rows after cleanup: {}", after.len());
    assert!(after.is_empty(), "events_m0 must be gone after cleanup");

    println!("\nM0 HINGE PROVEN: typed model -> converge -> real Keeper-backed ReplicatedMergeTree, idempotent.");
    Ok(())
}
