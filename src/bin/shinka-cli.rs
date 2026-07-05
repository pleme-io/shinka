//! Shinka CLI - Debug and manage database migrations
//!
//! A command-line tool for interacting with Shinka migrations.
//!
//! ## Usage
//!
//! ```bash
//! # List all migrations
//! shinka-cli list
//! shinka-cli list -n myapp-staging
//!
//! # Get migration status
//! shinka-cli status myapp-backend -n myapp-staging
//!
//! # View migration history
//! shinka-cli history myapp-backend -n myapp-staging --limit 10
//!
//! # Retry a failed migration
//! shinka-cli retry myapp-backend -n myapp-staging
//!
//! # Watch migration status (live updates)
//! shinka-cli watch myapp-backend -n myapp-staging
//!
//! # Get logs from the last migration job
//! shinka-cli logs myapp-backend -n myapp-staging
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    Client,
};

use shinka::crd::{DatabaseMigration, MigrationRun, RETRY_ANNOTATION};

/// CLI command
enum Command {
    List {
        namespace: Option<String>,
        all_namespaces: bool,
    },
    Status {
        name: String,
        namespace: String,
    },
    History {
        name: String,
        namespace: String,
        limit: usize,
    },
    Retry {
        name: String,
        namespace: String,
    },
    Watch {
        name: String,
        namespace: String,
    },
    Logs {
        name: String,
        namespace: String,
        follow: bool,
    },
    /// Extract a source database's schema into a shinka base `sqlConfigMapRef`
    /// ConfigMap (the `/copy-model` extract → feed leg). Connects to a live
    /// source, introspects its schema, and prints the base ConfigMap YAML.
    Extract {
        engine: shinka::crd::DatabaseEngine,
        host: String,
        port: Option<u16>,
        username: Option<String>,
        /// Databases to include (empty = all user databases).
        databases: Vec<String>,
        /// Emitted ConfigMap name + namespace (the feed artifact).
        configmap_name: String,
        namespace: String,
    },
    /// The full `/copy-model` capability: extract a SOURCE schema and emit the
    /// absorb-as-base *bundle* (the base `sqlConfigMapRef` ConfigMap **and** the
    /// `directRef` `DatabaseMigration` CR that points at it) for a TARGET. With
    /// `--camelot`, the base is projected in the camelot service-dependency order.
    CopyModel {
        /// Source (extract-from) connection.
        engine: shinka::crd::DatabaseEngine,
        source_host: String,
        source_port: Option<u16>,
        source_username: Option<String>,
        databases: Vec<String>,
        /// Project in the camelot per-service-DB dependency order.
        camelot: bool,
        /// Optional typed mold file (YAML) of additive evolutions to compose
        /// onto the extracted base (the MOLD leg).
        mold_file: Option<String>,
        /// Target (absorb-into) connection the emitted CR addresses.
        target_host: String,
        credentials_secret: String,
        password_key: String,
        /// Emitted object names + namespace.
        migration_name: String,
        configmap_name: String,
        namespace: String,
    },
}

fn parse_args() -> Result<Command, String> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        return Err(usage());
    }

    match args[1].as_str() {
        "list" => {
            let mut namespace = None;
            let mut all_namespaces = false;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "-n" | "--namespace" => {
                        i += 1;
                        if i < args.len() {
                            namespace = Some(args[i].clone());
                        }
                    }
                    "-A" | "--all-namespaces" => {
                        all_namespaces = true;
                    }
                    _ => {}
                }
                i += 1;
            }

            Ok(Command::List {
                namespace,
                all_namespaces,
            })
        }
        "status" => {
            if args.len() < 3 {
                return Err("Usage: shinka-cli status <name> -n <namespace>".to_string());
            }
            let name = args[2].clone();
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            Ok(Command::Status { name, namespace })
        }
        "history" => {
            if args.len() < 3 {
                return Err("Usage: shinka-cli history <name> -n <namespace>".to_string());
            }
            let name = args[2].clone();
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            let limit = find_flag(&args, &["--limit"])
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            Ok(Command::History {
                name,
                namespace,
                limit,
            })
        }
        "retry" => {
            if args.len() < 3 {
                return Err("Usage: shinka-cli retry <name> -n <namespace>".to_string());
            }
            let name = args[2].clone();
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            Ok(Command::Retry { name, namespace })
        }
        "watch" => {
            if args.len() < 3 {
                return Err("Usage: shinka-cli watch <name> -n <namespace>".to_string());
            }
            let name = args[2].clone();
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            Ok(Command::Watch { name, namespace })
        }
        "logs" => {
            if args.len() < 3 {
                return Err("Usage: shinka-cli logs <name> -n <namespace>".to_string());
            }
            let name = args[2].clone();
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            let follow = args.contains(&"-f".to_string()) || args.contains(&"--follow".to_string());
            Ok(Command::Logs {
                name,
                namespace,
                follow,
            })
        }
        "extract" => {
            let host = find_flag(&args, &["--host"])
                .ok_or_else(|| format!("extract: --host <HOST> is required\n{}", usage()))?;
            let engine = match find_flag(&args, &["--engine"]).as_deref() {
                None | Some("mysql") => shinka::crd::DatabaseEngine::Mysql,
                Some("postgres") => shinka::crd::DatabaseEngine::Postgres,
                Some(other) => {
                    return Err(format!("extract: unknown --engine {other} (mysql|postgres)"))
                }
            };
            let port = find_flag(&args, &["--port"]).and_then(|p| p.parse().ok());
            let username = find_flag(&args, &["-u", "--username"]);
            let databases = find_flag(&args, &["--databases", "-d"])
                .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default();
            let configmap_name = find_flag(&args, &["--configmap", "-c"])
                .unwrap_or_else(|| "shinka-base-schema".to_string());
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            Ok(Command::Extract {
                engine,
                host,
                port,
                username,
                databases,
                configmap_name,
                namespace,
            })
        }
        "copy-model" => {
            let source_host = find_flag(&args, &["--host", "--source-host"])
                .ok_or_else(|| format!("copy-model: --host <SOURCE_HOST> is required\n{}", usage()))?;
            let engine = match find_flag(&args, &["--engine"]).as_deref() {
                None | Some("mysql") => shinka::crd::DatabaseEngine::Mysql,
                Some("postgres") => shinka::crd::DatabaseEngine::Postgres,
                Some(other) => {
                    return Err(format!("copy-model: unknown --engine {other} (mysql|postgres)"))
                }
            };
            let source_port = find_flag(&args, &["--port", "--source-port"]).and_then(|p| p.parse().ok());
            let source_username = find_flag(&args, &["-u", "--username", "--source-username"]);
            let databases = find_flag(&args, &["--databases", "-d"])
                .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default();
            let camelot = args.iter().any(|a| a == "--camelot");
            let mold_file = find_flag(&args, &["--mold", "--mold-file"]);
            let target_host = find_flag(&args, &["--target-host"]).ok_or_else(|| {
                format!("copy-model: --target-host <HOST> is required (the CR applies here)\n{}", usage())
            })?;
            let credentials_secret = find_flag(&args, &["--credentials-secret"]).ok_or_else(|| {
                "copy-model: --credentials-secret <NAME> is required".to_string()
            })?;
            let password_key =
                find_flag(&args, &["--password-key"]).unwrap_or_else(|| "password".to_string());
            let migration_name = find_flag(&args, &["--name", "--migration-name"])
                .unwrap_or_else(|| "copy-model".to_string());
            let configmap_name = find_flag(&args, &["--configmap", "-c"])
                .unwrap_or_else(|| "shinka-base-schema".to_string());
            let namespace =
                find_flag(&args, &["-n", "--namespace"]).unwrap_or_else(|| "default".to_string());
            Ok(Command::CopyModel {
                engine,
                source_host,
                source_port,
                source_username,
                databases,
                camelot,
                mold_file,
                target_host,
                credentials_secret,
                password_key,
                migration_name,
                configmap_name,
                namespace,
            })
        }
        "help" | "-h" | "--help" => Err(usage()),
        cmd => Err(format!("Unknown command: {}\n{}", cmd, usage())),
    }
}

fn find_flag(args: &[String], flags: &[&str]) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        for flag in flags {
            if arg == *flag && i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
        }
    }
    None
}

fn usage() -> String {
    r#"
Shinka CLI - Debug and manage database migrations

USAGE:
    shinka-cli <COMMAND> [OPTIONS]

COMMANDS:
    list        List all migrations
    status      Get migration status
    history     View migration history
    retry       Retry a failed migration
    watch       Watch migration status (live updates)
    logs        Get logs from the last migration job
    extract     Extract a source DB's schema into a shinka base ConfigMap (/copy-model)
    copy-model  Extract a source schema + emit the absorb-as-base bundle (CM + directRef CR)
    help        Print this help message

OPTIONS:
    -n, --namespace <NS>    Kubernetes namespace
    -A, --all-namespaces    List across all namespaces
    --limit <N>             Limit results (for history)
    -f, --follow            Follow logs (for logs command)

EXTRACT OPTIONS (/copy-model extract → feed leg):
    --engine <mysql|postgres>   Source engine (default: mysql)
    --host <HOST>               Source database host (required)
    --port <PORT>               Source port (default: engine standard)
    -u, --username <USER>       Admin user (default: root/postgres)
    -d, --databases <a,b,c>     Databases to include (default: all user DBs)
    -c, --configmap <NAME>      Emitted ConfigMap name (default: shinka-base-schema)
    -n, --namespace <NS>        Emitted ConfigMap namespace (default: default)
    (source password is read from $SHINKA_SOURCE_PASSWORD)

EXAMPLES:
    shinka-cli list -n production
    shinka-cli status my-migration -n default
    shinka-cli history my-migration -n default --limit 5
    shinka-cli retry my-migration -n default
    shinka-cli watch my-migration -n default
    shinka-cli logs my-migration -n default -f
    SHINKA_SOURCE_PASSWORD=... shinka-cli extract --host mte-mysql \
        -d authdb,uamdb -c akeyless-schema-apply-sql -n camelot > base.yaml

COPY-MODEL OPTIONS (/copy-model — extract SOURCE + emit absorb-as-base bundle):
    --host <SOURCE_HOST>          Source database host to extract from (required)
    --engine <mysql|postgres>     Source engine (default: mysql)
    -d, --databases <a,b,c>       Databases to include (default: all user DBs)
    --camelot                     Project in the camelot service-dependency order
    --mold <FILE>                 Typed YAML mold file of additive evolutions to
                                  compose onto the extracted base (the MOLD leg)
    --target-host <HOST>          Target host the emitted directRef CR applies into (required)
    --credentials-secret <NAME>   Target admin-credentials Secret (required)
    --password-key <KEY>          Key in that Secret holding the password (default: password)
    --name <NAME>                 Emitted DatabaseMigration name (default: copy-model)
    -c, --configmap <NAME>        Emitted base ConfigMap name (default: shinka-base-schema)
    -n, --namespace <NS>          Emitted objects' namespace (default: default)
    (source password is read from $SHINKA_SOURCE_PASSWORD)

    SHINKA_SOURCE_PASSWORD=... shinka-cli copy-model --host mte-mysql \
        -d authdb,uamdb --camelot --target-host akeyless-saas-akeyless-mysql \
        --credentials-secret akeyless-mysql-root -c akeyless-schema-apply-sql \
        --name copy-model-camelot -n camelot > absorb-bundle.yaml

    # same, but MOLD the base forward with additive evolutions from a typed file:
    SHINKA_SOURCE_PASSWORD=... shinka-cli copy-model --host mte-mysql \
        -d authdb,uamdb --camelot --mold camelot-evolutions.yaml \
        --target-host akeyless-saas-akeyless-mysql \
        --credentials-secret akeyless-mysql-root -c akeyless-schema-apply-sql \
        --name copy-model-camelot -n camelot > absorb-bundle.yaml
"#
    .to_string()
}

#[tokio::main]
async fn main() {
    let command = match parse_args() {
        Ok(cmd) => cmd,
        Err(msg) => {
            eprintln!("{}", msg);
            std::process::exit(1);
        }
    };

    // `extract` addresses an external source database, not the cluster — run it
    // before (and without) the kube client so it works off-cluster.
    if let Command::Extract {
        engine,
        host,
        port,
        username,
        databases,
        configmap_name,
        namespace,
    } = &command
    {
        let result = run_extract(
            *engine,
            host,
            *port,
            username.as_deref(),
            databases,
            configmap_name,
            namespace,
        )
        .await;
        if let Err(e) = result {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    // `copy-model` likewise addresses an external SOURCE database (extract), and
    // emits static artifacts (no cluster access needed) — run it off-cluster too.
    if let Command::CopyModel {
        engine,
        source_host,
        source_port,
        source_username,
        databases,
        camelot,
        mold_file,
        target_host,
        credentials_secret,
        password_key,
        migration_name,
        configmap_name,
        namespace,
    } = &command
    {
        let result = run_copy_model(
            *engine,
            source_host,
            *source_port,
            source_username.as_deref(),
            databases,
            *camelot,
            mold_file.as_deref(),
            target_host,
            credentials_secret,
            password_key,
            migration_name,
            configmap_name,
            namespace,
        )
        .await;
        if let Err(e) = result {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create Kubernetes client: {}", e);
            std::process::exit(1);
        }
    };

    let result = match command {
        Command::List {
            namespace,
            all_namespaces,
        } => list_migrations(client, namespace, all_namespaces).await,
        Command::Status { name, namespace } => show_status(client, &name, &namespace).await,
        Command::History {
            name,
            namespace,
            limit,
        } => show_history(client, &name, &namespace, limit).await,
        Command::Retry { name, namespace } => retry_migration(client, &name, &namespace).await,
        Command::Watch { name, namespace } => watch_migration(client, &name, &namespace).await,
        Command::Logs {
            name,
            namespace,
            follow,
        } => show_logs(client, &name, &namespace, follow).await,
        // Handled before the kube client is created (external source, no cluster).
        Command::Extract { .. } => unreachable!("extract is handled before client creation"),
        Command::CopyModel { .. } => unreachable!("copy-model is handled before client creation"),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

/// The `/copy-model` extract → feed leg: introspect a live source database and
/// print its schema as a shinka base `sqlConfigMapRef` ConfigMap (YAML).
///
/// The source password is read from `SHINKA_SOURCE_PASSWORD` (never a CLI arg).
/// The emitted ConfigMap is the feed artifact: commit it and point a `directRef`
/// `DatabaseMigration` at it, and shinka's direct branch applies it as the base.
async fn run_extract(
    engine: shinka::crd::DatabaseEngine,
    host: &str,
    port: Option<u16>,
    username: Option<&str>,
    databases: &[String],
    configmap_name: &str,
    namespace: &str,
) -> Result<(), String> {
    use shinka::direct::DirectConnParams;
    use shinka::extract::{extract_base, render_base_configmap, DatabaseFilter, SqlxSchemaSource};

    let password = std::env::var("SHINKA_SOURCE_PASSWORD").map_err(|_| {
        "extract: set SHINKA_SOURCE_PASSWORD to the source admin password".to_string()
    })?;
    let params = DirectConnParams {
        host: host.to_string(),
        port: port.unwrap_or_else(|| engine.default_port()),
        username: username
            .map(|u| u.to_string())
            .unwrap_or_else(|| engine.default_admin_user().to_string()),
        password,
        database: None,
    };
    let target = params.to_string();
    let source = SqlxSchemaSource::new(params, engine, std::time::Duration::from_secs(10));
    let filter = if databases.is_empty() {
        DatabaseFilter::All
    } else {
        DatabaseFilter::Only(databases.to_vec())
    };

    let (model, ops) = extract_base(engine, &source, &filter, &target)
        .await
        .map_err(|e| e.to_string())?;

    eprintln!(
        "# extracted {} databases, {} tables, {} columns from {}",
        model.databases.len(),
        model.table_count(),
        model.column_count(),
        target
    );
    let cm = render_base_configmap(configmap_name, namespace, &ops);
    let yaml = serde_yaml::to_string(&cm).map_err(|e| e.to_string())?;
    println!("{}", yaml);
    Ok(())
}

/// The full `/copy-model` capability: extract a SOURCE schema, (optionally)
/// project it in the camelot service-dependency order, and print the
/// absorb-as-base *bundle* — the base `sqlConfigMapRef` ConfigMap **and** the
/// `directRef` `DatabaseMigration` that points at it — as a two-document YAML.
/// Commit both; shinka's direct branch absorbs the schema as the base against
/// the target.
#[allow(clippy::too_many_arguments)]
async fn run_copy_model(
    engine: shinka::crd::DatabaseEngine,
    source_host: &str,
    source_port: Option<u16>,
    source_username: Option<&str>,
    databases: &[String],
    camelot: bool,
    mold_file: Option<&str>,
    target_host: &str,
    credentials_secret: &str,
    password_key: &str,
    migration_name: &str,
    configmap_name: &str,
    namespace: &str,
) -> Result<(), String> {
    use shinka::copy_model::{project_camelot, render_bundle, render_mold, AbsorbTarget, MoldSpec};
    use shinka::direct::DirectConnParams;
    use shinka::extract::{extract_base, DatabaseFilter, SqlxSchemaSource};

    let password = std::env::var("SHINKA_SOURCE_PASSWORD").map_err(|_| {
        "copy-model: set SHINKA_SOURCE_PASSWORD to the SOURCE admin password".to_string()
    })?;
    let params = DirectConnParams {
        host: source_host.to_string(),
        port: source_port.unwrap_or_else(|| engine.default_port()),
        username: source_username
            .map(|u| u.to_string())
            .unwrap_or_else(|| engine.default_admin_user().to_string()),
        password,
        database: None,
    };
    let target_label = params.to_string();
    let source = SqlxSchemaSource::new(params, engine, std::time::Duration::from_secs(10));
    let filter = if databases.is_empty() {
        DatabaseFilter::All
    } else {
        DatabaseFilter::Only(databases.to_vec())
    };

    let (model, base_ops) = extract_base(engine, &source, &filter, &target_label)
        .await
        .map_err(|e| e.to_string())?;

    // Project in camelot dependency order when asked; otherwise keep extract order.
    let base = if camelot {
        project_camelot(&model)
    } else {
        base_ops
    };

    // MOLD leg: compose additive evolutions from a typed mold file onto the base.
    // The mold file is an operator-authored input (like a values file); it is
    // parsed at a typed boundary, so a destructive evolution cannot be expressed.
    let ops = match mold_file {
        Some(path) => {
            let yaml = std::fs::read_to_string(path)
                .map_err(|e| format!("copy-model: reading mold file {path}: {e}"))?;
            let spec = MoldSpec::from_yaml(&yaml)
                .map_err(|e| format!("copy-model: parsing mold file {path}: {e}"))?;
            let n = spec.evolutions.len();
            eprintln!("# copy-model: molding {n} additive evolution(s) onto the base");
            render_mold(&spec.into_plan(engine, base))
        }
        None => base,
    };

    eprintln!(
        "# copy-model: {} databases, {} tables, {} columns from {} → absorb as base into {}",
        model.databases.len(),
        model.table_count(),
        model.column_count(),
        target_label,
        target_host
    );

    let target = AbsorbTarget {
        migration_name: migration_name.to_string(),
        namespace: namespace.to_string(),
        config_map_name: configmap_name.to_string(),
        engine,
        target_host: target_host.to_string(),
        credentials_secret: credentials_secret.to_string(),
        password_key: password_key.to_string(),
    };
    let bundle = render_bundle(&ops, &target);

    let cm_yaml = serde_yaml::to_string(&bundle.config_map).map_err(|e| e.to_string())?;
    let mig_yaml = serde_yaml::to_string(&bundle.migration).map_err(|e| e.to_string())?;
    // Two-document YAML: ConfigMap first (created before the CR references it).
    println!("{}---\n{}", cm_yaml, mig_yaml);
    Ok(())
}

async fn list_migrations(
    client: Client,
    namespace: Option<String>,
    all_namespaces: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let migrations: Vec<DatabaseMigration> = if all_namespaces {
        let api: Api<DatabaseMigration> = Api::all(client);
        api.list(&ListParams::default()).await?.items
    } else {
        let ns = namespace.as_deref().unwrap_or("default");
        let api: Api<DatabaseMigration> = Api::namespaced(client, ns);
        api.list(&ListParams::default()).await?.items
    };

    if migrations.is_empty() {
        println!("No migrations found");
        return Ok(());
    }

    // Print header
    println!(
        "{:<30} {:<20} {:<12} {:<15} {:<20}",
        "NAME", "NAMESPACE", "PHASE", "RETRIES", "LAST IMAGE TAG"
    );
    println!("{}", "-".repeat(100));

    for m in migrations {
        let name = m.metadata.name.as_deref().unwrap_or("-");
        let ns = m.metadata.namespace.as_deref().unwrap_or("-");
        let phase = m
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .map(|p| format!("{:?}", p))
            .unwrap_or_else(|| "-".to_string());
        let retries = m
            .status
            .as_ref()
            .and_then(|s| s.retry_count)
            .map(|r| r.to_string())
            .unwrap_or_else(|| "0".to_string());
        let image_tag = m
            .status
            .as_ref()
            .and_then(|s| s.last_migration.as_ref())
            .map(|lm| lm.image_tag.clone())
            .unwrap_or_else(|| "-".to_string());

        let image_tag_display = if image_tag.len() > 15 {
            format!("{}...", &image_tag[..12])
        } else {
            image_tag
        };

        println!(
            "{:<30} {:<20} {:<12} {:<15} {:<20}",
            name, ns, phase, retries, image_tag_display
        );
    }

    Ok(())
}

async fn show_status(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let api: Api<DatabaseMigration> = Api::namespaced(client, namespace);
    let migration = api.get(name).await?;

    let status = migration.status.as_ref();
    let spec = &migration.spec;

    println!("Migration: {}/{}", namespace, name);
    println!("{}", "=".repeat(50));

    // Phase
    let phase = status
        .and_then(|s| s.phase.as_ref())
        .map(|p| format!("{:?}", p))
        .unwrap_or_else(|| "Unknown".to_string());
    println!("Phase:     {}", colorize_phase(&phase));

    // Database (CNPG cluster name, or engine://host for a direct source)
    println!("Database:  {}", spec.database.display_target());

    // Deployment (first migrator)
    let deployment_name = migration
        .get_migrators()
        .first()
        .map(|m| m.deployment_ref.name.as_str())
        .unwrap_or("unknown");
    println!("Deployment: {}", deployment_name);

    // Migrator count
    let migrator_count = migration.migrator_count();
    if migrator_count > 1 {
        println!("Migrators: {} configured", migrator_count);
    }

    // Retry count
    let retries = status.and_then(|s| s.retry_count).unwrap_or(0);
    let max_retries = spec.safety.max_retries;
    println!("Retries:   {}/{}", retries, max_retries);

    // Last migration
    if let Some(lm) = status.and_then(|s| s.last_migration.as_ref()) {
        println!("\nLast Migration:");
        println!("  Image Tag:   {}", lm.image_tag);
        if let Some(at) = &lm.completed_at {
            println!("  Completed:   {}", at);
        }
        if let Some(dur) = &lm.duration {
            println!("  Duration:    {}", dur);
        }
    }

    // Conditions
    if let Some(conditions) = status.and_then(|s| s.conditions.as_ref()) {
        println!("\nConditions:");
        for cond in conditions {
            let status_str = if cond.status == "True" { "✓" } else { "✗" };
            let msg = cond.message.as_deref().unwrap_or("-");
            println!("  {} {}: {}", status_str, cond.condition_type, msg);
        }
    }

    // Error message (from last migration)
    if phase == "Failed" {
        if let Some(err) = status
            .and_then(|s| s.last_migration.as_ref())
            .and_then(|lm| lm.error.as_ref())
        {
            println!("\nError: {}", err);
        }
    }

    Ok(())
}

async fn show_history(
    client: Client,
    name: &str,
    namespace: &str,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let api: Api<MigrationRun> = Api::namespaced(client, namespace);

    // List runs for this migration
    let label_selector = format!("shinka.pleme.io/migration={}", name);
    let runs = api
        .list(&ListParams::default().labels(&label_selector))
        .await?;

    if runs.items.is_empty() {
        println!("No migration runs found for {}/{}", namespace, name);
        return Ok(());
    }

    println!("Migration History: {}/{}", namespace, name);
    println!("{}", "=".repeat(80));
    println!(
        "{:<8} {:<12} {:<20} {:<10} {:<30}",
        "ATTEMPT", "PHASE", "STARTED", "DURATION", "MESSAGE"
    );
    println!("{}", "-".repeat(80));

    let mut sorted_runs = runs.items;
    sorted_runs.sort_by(|a, b| {
        let a_time = a.status.as_ref().and_then(|s| s.started_at);
        let b_time = b.status.as_ref().and_then(|s| s.started_at);
        b_time.cmp(&a_time)
    });

    for run in sorted_runs.into_iter().take(limit) {
        let attempt = run.spec.attempt;
        let phase = run
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .map(|p| format!("{}", p))
            .unwrap_or_else(|| "-".to_string());
        let started = run
            .status
            .as_ref()
            .and_then(|s| s.started_at)
            .map(format_time)
            .unwrap_or_else(|| "-".to_string());
        let duration = run
            .status
            .as_ref()
            .and_then(|s| s.duration.clone())
            .unwrap_or_else(|| "-".to_string());
        let message = run
            .status
            .as_ref()
            .and_then(|s| s.message.clone())
            .unwrap_or_else(|| "-".to_string());
        let message_display = if message.len() > 28 {
            format!("{}...", &message[..25])
        } else {
            message
        };

        println!(
            "{:<8} {:<12} {:<20} {:<10} {:<30}",
            attempt, phase, started, duration, message_display
        );
    }

    Ok(())
}

async fn retry_migration(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let api: Api<DatabaseMigration> = Api::namespaced(client, namespace);

    // Add retry annotation
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                RETRY_ANNOTATION: "true"
            }
        }
    });

    api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
        .await?;

    println!("Retry triggered for migration {}/{}", namespace, name);
    println!("The migration will be retried on the next reconciliation.");
    Ok(())
}

async fn watch_migration(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let api: Api<DatabaseMigration> = Api::namespaced(client, namespace);

    println!("Watching migration {}/{}...", namespace, name);
    println!("Press Ctrl+C to stop\n");

    let mut last_phase = String::new();
    let mut last_message = String::new();

    loop {
        match api.get(name).await {
            Ok(migration) => {
                let status = migration.status.as_ref();
                let phase = status
                    .and_then(|s| s.phase.as_ref())
                    .map(|p| format!("{:?}", p))
                    .unwrap_or_else(|| "Unknown".to_string());
                // Get message from last migration error or conditions
                let message = status
                    .and_then(|s| s.last_migration.as_ref())
                    .and_then(|lm| lm.error.clone())
                    .or_else(|| {
                        status
                            .and_then(|s| s.conditions.as_ref())
                            .and_then(|conds| conds.last())
                            .and_then(|c| c.message.clone())
                    })
                    .unwrap_or_default();

                if phase != last_phase || message != last_message {
                    let now = chrono::Local::now().format("%H:%M:%S");
                    println!("[{}] Phase: {} - {}", now, colorize_phase(&phase), message);
                    last_phase = phase.clone();
                    last_message = message;

                    // Exit if terminal state
                    if phase == "Ready" || phase == "Failed" {
                        println!("\nMigration reached terminal state: {}", phase);
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error fetching migration: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}

async fn show_logs(
    client: Client,
    name: &str,
    namespace: &str,
    _follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let api: Api<DatabaseMigration> = Api::namespaced(client.clone(), namespace);
    let migration = api.get(name).await?;

    let job_name = migration
        .status
        .as_ref()
        .and_then(|s| s.current_job.as_ref())
        .ok_or("No current job found for migration")?;

    println!("Fetching logs for job: {}", job_name);
    println!("{}", "=".repeat(50));

    // Get pods for the job
    let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(client, namespace);
    let label_selector = format!("job-name={}", job_name);
    let pod_list = pods
        .list(&ListParams::default().labels(&label_selector))
        .await?;

    if let Some(pod) = pod_list.items.first() {
        let pod_name = pod.metadata.name.as_ref().ok_or("Pod has no name")?;
        let logs = pods
            .logs(pod_name, &kube::api::LogParams::default())
            .await?;
        println!("{}", logs);
    } else {
        println!("No pods found for job {}", job_name);
    }

    Ok(())
}

fn colorize_phase(phase: &str) -> String {
    match phase {
        "Ready" => format!("\x1b[32m{}\x1b[0m", phase), // Green
        "Failed" => format!("\x1b[31m{}\x1b[0m", phase), // Red
        "Migrating" => format!("\x1b[34m{}\x1b[0m", phase), // Blue
        "Pending" | "CheckingHealth" | "WaitingForDatabase" => {
            format!("\x1b[33m{}\x1b[0m", phase) // Yellow
        }
        _ => phase.to_string(),
    }
}

fn format_time(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M").to_string()
}
