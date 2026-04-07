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
    /// TTL in seconds for completed Jobs (Kubernetes garbage collection)
    pub job_ttl_seconds: i32,
    /// Whether to inject mesh sidecar disable annotation
    pub disable_mesh_sidecar: bool,
    /// Annotation key for disabling mesh sidecar (e.g., "sidecar.istio.io/inject")
    pub mesh_sidecar_annotation_key: String,
    /// Annotation value for disabling mesh sidecar (e.g., "false")
    pub mesh_sidecar_annotation_value: String,
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
    // Optionally disable service mesh sidecar injection for migration jobs.
    // Migration pods are short-lived and don't need service mesh traffic management.
    // This prevents race conditions where the migration container starts before
    // the mesh proxy is ready, causing "connection reset by peer" errors.
    if config.disable_mesh_sidecar {
        annotations.insert(
            config.mesh_sidecar_annotation_key.clone(),
            config.mesh_sidecar_annotation_value.clone(),
        );
    }

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
        ttl_seconds_after_finished: Some(config.job_ttl_seconds),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        CnpgClusterRef, DatabaseMigrationSpec, DatabaseSpec, DeploymentRef, MigratorSpec,
        SafetySpec, TimeoutSpec,
    };

    fn make_test_migration() -> DatabaseMigration {
        DatabaseMigration {
            metadata: ObjectMeta {
                name: Some("test-mig".to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: DatabaseMigrationSpec {
                database: DatabaseSpec {
                    cnpg_cluster_ref: CnpgClusterRef {
                        name: "cluster".to_string(),
                        database: None,
                    },
                },
                migrator: Some(MigratorSpec {
                    name: None,
                    migrator_type: MigratorType::Sqlx,
                    deployment_ref: DeploymentRef {
                        name: "backend".to_string(),
                        container_name: None,
                    },
                    image_override: None,
                    command: None,
                    args: None,
                    working_dir: None,
                    migrations_path: None,
                    env: None,
                    tool_config: None,
                    secret_refs: None,
                    env_from: None,
                    resources: None,
                    service_account_name: None,
                }),
                migrators: None,
                safety: SafetySpec::default(),
                timeouts: TimeoutSpec::default(),
            },
            status: None,
        }
    }

    fn make_test_config() -> MigrationJobConfig {
        MigrationJobConfig {
            image: "myapp:v1.0".to_string(),
            image_tag: "v1.0".to_string(),
            command: vec!["sqlx".to_string(), "migrate".to_string(), "run".to_string()],
            args: None,
            working_dir: None,
            resources: None,
            env_from: None,
            env: None,
            service_account_name: None,
            migrator_type: MigratorType::Sqlx,
            trace_context: None,
            scheduling: PodScheduling::default(),
            job_ttl_seconds: 3600,
            disable_mesh_sidecar: false,
            mesh_sidecar_annotation_key: "sidecar.istio.io/inject".to_string(),
            mesh_sidecar_annotation_value: "false".to_string(),
        }
    }

    #[test]
    fn test_short_hash_deterministic() {
        let h1 = short_hash("v1.0");
        let h2 = short_hash("v1.0");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_short_hash_different_inputs() {
        let h1 = short_hash("v1.0");
        let h2 = short_hash("v2.0");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_short_hash_length() {
        let h = short_hash("anything");
        assert_eq!(h.len(), 8);
    }

    #[test]
    fn test_truncate_label_short_string() {
        assert_eq!(truncate_label("short", 63), "short");
    }

    #[test]
    fn test_truncate_label_exact_length() {
        let s = "a".repeat(63);
        assert_eq!(truncate_label(&s, 63), s);
    }

    #[test]
    fn test_truncate_label_long_string() {
        let s = "a".repeat(100);
        let result = truncate_label(&s, 63);
        assert_eq!(result.len(), 63);
    }

    #[test]
    fn test_build_migration_job_basic() {
        let migration = make_test_migration();
        let config = make_test_config();
        let job = build_migration_job(&migration, config).unwrap();

        assert!(job.metadata.name.unwrap().starts_with("test-mig-migration-"));
        assert_eq!(job.metadata.namespace, Some("test-ns".to_string()));

        let labels = job.metadata.labels.unwrap();
        assert_eq!(labels["app.kubernetes.io/name"], "test-mig");
        assert_eq!(labels["app.kubernetes.io/component"], "migration");
        assert_eq!(labels["app.kubernetes.io/managed-by"], "shinka");
    }

    #[test]
    fn test_build_migration_job_container_config() {
        let migration = make_test_migration();
        let config = make_test_config();
        let job = build_migration_job(&migration, config).unwrap();

        let spec = job.spec.unwrap();
        let pod_spec = spec.template.spec.unwrap();
        assert_eq!(pod_spec.containers.len(), 1);

        let container = &pod_spec.containers[0];
        assert_eq!(container.name, "migration");
        assert_eq!(container.image, Some("myapp:v1.0".to_string()));
        assert_eq!(
            container.command,
            Some(vec![
                "sqlx".to_string(),
                "migrate".to_string(),
                "run".to_string()
            ])
        );
    }

    #[test]
    fn test_build_migration_job_never_restart() {
        let migration = make_test_migration();
        let config = make_test_config();
        let job = build_migration_job(&migration, config).unwrap();

        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(pod_spec.restart_policy, Some("Never".to_string()));
    }

    #[test]
    fn test_build_migration_job_backoff_limit_zero() {
        let migration = make_test_migration();
        let config = make_test_config();
        let job = build_migration_job(&migration, config).unwrap();

        assert_eq!(job.spec.unwrap().backoff_limit, Some(0));
    }

    #[test]
    fn test_build_migration_job_with_resources() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.resources = Some(crate::crd::ResourceRequirements {
            memory_limit: Some("256Mi".to_string()),
            cpu_limit: Some("500m".to_string()),
            memory_request: Some("128Mi".to_string()),
            cpu_request: Some("100m".to_string()),
        });
        let job = build_migration_job(&migration, config).unwrap();

        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let res = container.resources.as_ref().unwrap();
        assert!(res.limits.is_some());
        assert!(res.requests.is_some());
    }

    #[test]
    fn test_build_migration_job_with_env() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        let mut env = BTreeMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        config.env = Some(env);
        let job = build_migration_job(&migration, config).unwrap();

        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let env_vars = container.env.as_ref().unwrap();
        assert!(env_vars.iter().any(|e| e.name == "FOO" && e.value == Some("bar".to_string())));
    }

    #[test]
    fn test_build_migration_job_with_trace_context() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.trace_context = Some(TraceContext {
            traceparent: "00-abc-def-01".to_string(),
            tracestate: Some("vendor=data".to_string()),
        });
        let job = build_migration_job(&migration, config).unwrap();

        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let env_vars = container.env.as_ref().unwrap();
        assert!(env_vars.iter().any(|e| e.name == "TRACEPARENT"));
        assert!(env_vars.iter().any(|e| e.name == "TRACESTATE"));
        assert!(env_vars.iter().any(|e| e.name == "OTEL_PROPAGATORS"));
    }

    #[test]
    fn test_build_migration_job_with_mesh_sidecar_disabled() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.disable_mesh_sidecar = true;
        let job = build_migration_job(&migration, config).unwrap();

        let annotations = job.metadata.annotations.unwrap();
        assert_eq!(annotations["sidecar.istio.io/inject"], "false");
    }

    #[test]
    fn test_build_migration_job_without_mesh_sidecar_disabled() {
        let migration = make_test_migration();
        let config = make_test_config();
        let job = build_migration_job(&migration, config).unwrap();

        let annotations = job.metadata.annotations.unwrap();
        assert!(!annotations.contains_key("sidecar.istio.io/inject"));
    }

    #[test]
    fn test_build_migration_job_with_service_account() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.service_account_name = Some("migration-sa".to_string());
        let job = build_migration_job(&migration, config).unwrap();

        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            pod_spec.service_account_name,
            Some("migration-sa".to_string())
        );
    }

    #[test]
    fn test_build_migration_job_with_scheduling() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        let mut node_selector = BTreeMap::new();
        node_selector.insert("kubernetes.io/arch".to_string(), "amd64".to_string());
        config.scheduling = PodScheduling {
            node_selector: Some(node_selector),
            tolerations: None,
            affinity: None,
        };
        let job = build_migration_job(&migration, config).unwrap();

        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        let ns = pod_spec.node_selector.unwrap();
        assert_eq!(ns["kubernetes.io/arch"], "amd64");
    }

    #[test]
    fn test_build_migration_job_with_working_dir() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.working_dir = Some("/app".to_string());
        let job = build_migration_job(&migration, config).unwrap();

        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        assert_eq!(container.working_dir, Some("/app".to_string()));
    }

    #[test]
    fn test_build_migration_job_ttl() {
        let migration = make_test_migration();
        let mut config = make_test_config();
        config.job_ttl_seconds = 7200;
        let job = build_migration_job(&migration, config).unwrap();

        assert_eq!(job.spec.unwrap().ttl_seconds_after_finished, Some(7200));
    }
}
