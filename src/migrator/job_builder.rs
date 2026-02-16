//! Migration job builder
//!
//! Creates Kubernetes Jobs to run database migrations using
//! the service's container image with a configured migration command.
//!
//! Supports multiple migration tools:
//! - Rust: sqlx (default), refinery, diesel
//! - Go: goose, golang-migrate, atlas, dbmate
//! - Java: flyway, liquibase
//! - Custom: user-defined commands

use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMapEnvSource, Container, EnvFromSource as K8sEnvFromSource, EnvVar, PodSpec,
    PodTemplateSpec, ResourceRequirements as K8sResourceRequirements, SecretEnvSource, Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

use super::MigratorType;
use crate::crd::{DatabaseMigration, EnvFromSource as CrdEnvFromSource, ResourceRequirements};
use crate::Result;

/// Configuration for building a migration job
pub struct MigrationJobConfig {
    /// Container image to use (from deployment)
    pub image: String,
    /// Image tag (for tracking)
    pub image_tag: String,
    /// Command to run (built from MigratorSpec)
    pub command: Vec<String>,
    /// Optional arguments
    pub args: Option<Vec<String>>,
    /// Working directory for the container
    pub working_dir: Option<String>,
    /// Resource requirements
    pub resources: Option<ResourceRequirements>,
    /// Environment variable sources (from ConfigMaps/Secrets)
    pub env_from: Option<Vec<CrdEnvFromSource>>,
    /// Additional environment variables
    pub env: Option<BTreeMap<String, String>>,
    /// Service account name
    pub service_account_name: Option<String>,
    /// The migrator type being used
    pub migrator_type: MigratorType,
    /// Trace context for distributed tracing (OpenTelemetry W3C format)
    pub trace_context: Option<TraceContext>,
    /// Pod scheduling constraints inherited from the referenced deployment
    pub scheduling: PodScheduling,
}

/// Pod scheduling constraints to inherit from the referenced deployment.
/// Ensures migration jobs run on the same node(s) as the application.
#[derive(Debug, Clone, Default)]
pub struct PodScheduling {
    /// Node selector labels (e.g., kubernetes.io/hostname=zek)
    pub node_selector: Option<BTreeMap<String, String>>,
    /// Pod tolerations (e.g., environment=staging:NoSchedule)
    pub tolerations: Option<Vec<Toleration>>,
    /// Pod affinity/anti-affinity rules
    pub affinity: Option<Affinity>,
}

/// Trace context for distributed tracing
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// W3C Trace Context traceparent header value
    pub traceparent: String,
    /// Optional tracestate for vendor-specific data
    pub tracestate: Option<String>,
}

/// Build a Kubernetes Job for running migrations
pub fn build_migration_job(
    migration: &DatabaseMigration,
    config: MigrationJobConfig,
) -> Result<Job> {
    let namespace = migration.namespace_or_default();
    let name = migration.name_or_default();
    let job_name = format!("{}-migration-{}", name, short_hash(&config.image_tag));

    // Build labels
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), name.clone());
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "migration".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "shinka".to_string(),
    );
    labels.insert("shinka.pleme.io/migration".to_string(), name.clone());
    labels.insert(
        "shinka.pleme.io/image-tag".to_string(),
        truncate_label(&config.image_tag, 63),
    );

    // Build annotations
    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    annotations.insert("shinka.pleme.io/image".to_string(), config.image.clone());
    annotations.insert(
        "shinka.pleme.io/migrator-type".to_string(),
        config.migrator_type.to_string(),
    );
    // Disable Istio sidecar injection for migration jobs
    // Migration pods are short-lived and don't need service mesh traffic management.
    // This prevents race conditions where the migration container starts before
    // the Istio proxy is ready, causing "connection reset by peer" errors.
    annotations.insert("sidecar.istio.io/inject".to_string(), "false".to_string());

    // Build resource requirements
    let resources = config.resources.map(|r| {
        let mut limits: BTreeMap<String, Quantity> = BTreeMap::new();
        let mut requests: BTreeMap<String, Quantity> = BTreeMap::new();

        if let Some(mem) = r.memory_limit {
            limits.insert("memory".to_string(), Quantity(mem));
        }
        if let Some(cpu) = r.cpu_limit {
            limits.insert("cpu".to_string(), Quantity(cpu));
        }
        if let Some(mem) = r.memory_request {
            requests.insert("memory".to_string(), Quantity(mem));
        }
        if let Some(cpu) = r.cpu_request {
            requests.insert("cpu".to_string(), Quantity(cpu));
        }

        K8sResourceRequirements {
            limits: if limits.is_empty() {
                None
            } else {
                Some(limits)
            },
            requests: if requests.is_empty() {
                None
            } else {
                Some(requests)
            },
            ..Default::default()
        }
    });

    // Build envFrom sources
    let env_from: Option<Vec<K8sEnvFromSource>> = config.env_from.map(|sources| {
        sources
            .into_iter()
            .filter_map(|source| {
                if let Some(cm_ref) = source.config_map_ref {
                    Some(K8sEnvFromSource {
                        config_map_ref: Some(ConfigMapEnvSource {
                            name: cm_ref.name.clone(),
                            optional: Some(false),
                        }),
                        ..Default::default()
                    })
                } else if let Some(secret_ref) = source.secret_ref {
                    Some(K8sEnvFromSource {
                        secret_ref: Some(SecretEnvSource {
                            name: secret_ref.name.clone(),
                            optional: Some(false),
                        }),
                        ..Default::default()
                    })
                } else {
                    None
                }
            })
            .collect()
    });

    // Build environment variables from config.env
    let mut env_vars: Vec<EnvVar> = config
        .env
        .map(|env_map| {
            env_map
                .into_iter()
                .map(|(key, value)| EnvVar {
                    name: key,
                    value: Some(value),
                    ..Default::default()
                })
                .collect()
        })
        .unwrap_or_default();

    // Inject trace context for distributed tracing (OpenTelemetry W3C format)
    if let Some(trace_ctx) = &config.trace_context {
        env_vars.push(EnvVar {
            name: "TRACEPARENT".to_string(),
            value: Some(trace_ctx.traceparent.clone()),
            ..Default::default()
        });

        if let Some(tracestate) = &trace_ctx.tracestate {
            env_vars.push(EnvVar {
                name: "TRACESTATE".to_string(),
                value: Some(tracestate.clone()),
                ..Default::default()
            });
        }

        // Also set OTEL_* environment variables for compatibility
        env_vars.push(EnvVar {
            name: "OTEL_PROPAGATORS".to_string(),
            value: Some("tracecontext,baggage".to_string()),
            ..Default::default()
        });
    }

    let env: Option<Vec<EnvVar>> = if env_vars.is_empty() {
        None
    } else {
        Some(env_vars)
    };

    // Build container
    let container = Container {
        name: "migration".to_string(),
        image: Some(config.image),
        command: Some(config.command),
        args: config.args,
        working_dir: config.working_dir,
        resources,
        env_from,
        env,
        ..Default::default()
    };

    // Build pod spec (inherit scheduling constraints from the referenced deployment)
    let pod_spec = PodSpec {
        containers: vec![container],
        restart_policy: Some("Never".to_string()),
        service_account_name: config.service_account_name,
        node_selector: config.scheduling.node_selector,
        tolerations: config.scheduling.tolerations,
        affinity: config.scheduling.affinity,
        ..Default::default()
    };

    // Build job spec
    let job_spec = JobSpec {
        template: PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(labels.clone()),
                annotations: Some(annotations.clone()),
                ..Default::default()
            }),
            spec: Some(pod_spec),
        },
        backoff_limit: Some(0), // No retries at Job level (we handle retries)
        ttl_seconds_after_finished: Some(3600), // Clean up after 1 hour
        ..Default::default()
    };

    // Build the Job
    let job = Job {
        metadata: ObjectMeta {
            name: Some(job_name),
            namespace: Some(namespace),
            labels: Some(labels),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: Some(job_spec),
        ..Default::default()
    };

    Ok(job)
}

/// Generate a short hash from a string for job naming
fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:x}", hasher.finish())[..8].to_string()
}

/// Truncate a string to fit Kubernetes label value limit (63 chars)
fn truncate_label(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        s[..max_len].to_string()
    }
}
