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
    help        Print this help message

OPTIONS:
    -n, --namespace <NS>    Kubernetes namespace
    -A, --all-namespaces    List across all namespaces
    --limit <N>             Limit results (for history)
    -f, --follow            Follow logs (for logs command)

EXAMPLES:
    shinka-cli list -n production
    shinka-cli status my-migration -n default
    shinka-cli history my-migration -n default --limit 5
    shinka-cli retry my-migration -n default
    shinka-cli watch my-migration -n default
    shinka-cli logs my-migration -n default -f
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
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
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

    // Database
    println!("Database:  {}", spec.database.cnpg_cluster_ref.name);

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
