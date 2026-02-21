//! Error types for Shinka operator
//!
//! Categorized errors following the pattern:
//! - Transient: Retry later (network, temporary failures)
//! - Permanent: Don't retry (validation, configuration errors)
//! - Fatal: Operator should exit (unrecoverable)
//!
//! ## Error Context
//!
//! All errors include:
//! - Descriptive message with resource identifiers
//! - Category for metrics
//! - Remediation hints via `help()` method

use std::time::Duration;

/// Result type alias using Shinka's Error
pub type Result<T> = std::result::Result<T, Error>;

/// Categorized error types for the Shinka operator
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // =========================================================================
    // Kubernetes Errors
    // =========================================================================
    /// Kubernetes API error (transient - retry)
    #[error("kubernetes API error: {0}")]
    KubeApi(#[from] kube::Error),

    /// Failed to get Kubernetes resource
    #[error("resource not found: {kind}/{name} in namespace {namespace}")]
    ResourceNotFound {
        kind: String,
        name: String,
        namespace: String,
    },

    /// Failed to watch resource
    #[error("watch error for {kind}: {message}")]
    WatchError { kind: String, message: String },

    // =========================================================================
    // Database Errors
    // =========================================================================
    /// CNPG cluster not healthy
    #[error("CNPG cluster {name} is not healthy: {reason}")]
    ClusterNotHealthy { name: String, reason: String },

    /// CNPG cluster not found
    #[error("CNPG cluster {name} not found in namespace {namespace}")]
    ClusterNotFound { name: String, namespace: String },

    // =========================================================================
    // Migration Errors
    // =========================================================================
    /// Migration job failed
    #[error("migration job {name} failed: {reason}")]
    MigrationFailed { name: String, reason: String },

    /// Migration job timed out
    #[error("migration job {name} timed out after {duration:?}")]
    MigrationTimeout { name: String, duration: Duration },

    /// Maximum retries exceeded
    #[error("migration {name} exceeded max retries ({max_retries})")]
    MaxRetriesExceeded { name: String, max_retries: u32 },

    /// Pre-flight validation failed (checksum mismatches detected)
    #[error("pre-flight validation failed: {count} migration(s) have issues\n{details}")]
    PreFlightValidationFailed { count: usize, details: String },

    /// Deployment not found for migration
    #[error("deployment {name} not found in namespace {namespace}")]
    DeploymentNotFound { name: String, namespace: String },

    /// No container image found in deployment
    #[error("no container image found in deployment {name}")]
    NoContainerImage { name: String },

    // =========================================================================
    // Configuration Errors
    // =========================================================================
    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Configuration file error
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Missing required field
    #[error("missing required field: {0}")]
    MissingField(String),

    /// Invalid timeout duration
    #[error("invalid timeout: {0}")]
    InvalidTimeout(String),

    // =========================================================================
    // Serialization Errors
    // =========================================================================
    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    // =========================================================================
    // Internal Errors
    // =========================================================================
    /// Internal error (should not happen)
    #[error("internal error: {0}")]
    Internal(String),

    /// Finalizer error
    #[error("finalizer error: {0}")]
    Finalizer(String),
}

impl Error {
    /// Check if the error is transient and should be retried
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Error::KubeApi(_)
                | Error::ClusterNotHealthy { .. }
                | Error::MigrationTimeout { .. }
                | Error::WatchError { .. }
        )
    }

    /// Check if the error is permanent and should not be retried
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::ResourceNotFound { .. }
                | Error::ClusterNotFound { .. }
                | Error::DeploymentNotFound { .. }
                | Error::NoContainerImage { .. }
                | Error::InvalidConfig(_)
                | Error::Configuration(_)
                | Error::MissingField(_)
                | Error::InvalidTimeout(_)
                | Error::MaxRetriesExceeded { .. }
                | Error::PreFlightValidationFailed { .. }
        )
    }

    /// Get the recommended requeue duration for transient errors
    pub fn requeue_duration(&self) -> Duration {
        match self {
            Error::ClusterNotHealthy { .. } => Duration::from_secs(30),
            Error::MigrationTimeout { .. } => Duration::from_secs(60),
            Error::KubeApi(_) => Duration::from_secs(10),
            Error::WatchError { .. } => Duration::from_secs(5),
            _ => Duration::from_secs(60),
        }
    }

    /// Get error category for metrics
    pub fn category(&self) -> &'static str {
        match self {
            Error::KubeApi(_) | Error::WatchError { .. } => "kubernetes",
            Error::ClusterNotHealthy { .. } | Error::ClusterNotFound { .. } => "database",
            Error::MigrationFailed { .. }
            | Error::MigrationTimeout { .. }
            | Error::MaxRetriesExceeded { .. }
            | Error::PreFlightValidationFailed { .. }
            | Error::DeploymentNotFound { .. }
            | Error::NoContainerImage { .. } => "migration",
            Error::InvalidConfig(_)
            | Error::Configuration(_)
            | Error::MissingField(_)
            | Error::InvalidTimeout(_) => "configuration",
            Error::Json(_) => "serialization",
            Error::Internal(_) | Error::Finalizer(_) => "internal",
            Error::ResourceNotFound { .. } => "kubernetes",
        }
    }

    /// Get remediation help for this error
    ///
    /// Returns actionable guidance for resolving the error.
    pub fn help(&self) -> &'static str {
        match self {
            Error::KubeApi(_) => {
                "Check Kubernetes API server health and operator RBAC permissions. \
                 Verify the operator service account has required roles."
            }
            Error::ResourceNotFound { .. } => {
                "Verify the resource exists in the specified namespace. \
                 Check for typos in resource names."
            }
            Error::WatchError { .. } => {
                "The resource watch was interrupted. This is usually transient. \
                 If persistent, check network connectivity to the API server."
            }
            Error::ClusterNotHealthy { .. } => {
                "The CNPG cluster is not ready. Wait for the cluster to become healthy. \
                 Check CNPG cluster status: kubectl get cluster -n <namespace>"
            }
            Error::ClusterNotFound { .. } => {
                "Verify the CNPG cluster name in the DatabaseMigration spec. \
                 Ensure the cluster exists in the same namespace."
            }
            Error::MigrationFailed { .. } => {
                "Check the migration job logs: kubectl logs -n <namespace> job/<job-name>. \
                 Fix the migration SQL and push a new image to trigger retry."
            }
            Error::MigrationTimeout { .. } => {
                "The migration is taking too long. Consider:\n\
                 1. Increasing spec.timeouts.migration in the DatabaseMigration\n\
                 2. Optimizing the migration SQL\n\
                 3. Adding indices before running data migrations"
            }
            Error::MaxRetriesExceeded { .. } => {
                "Maximum retries exhausted. To retry:\n\
                 1. Fix the underlying issue (check pod logs)\n\
                 2. Annotate with: kubectl annotate databasemigration <name> shinka.pleme.io/retry=true\n\
                 3. Or push a new image to trigger automatic retry"
            }
            Error::PreFlightValidationFailed { .. } => {
                "Migration files were modified after being applied. In PreFlight mode:\n\
                 1. Never modify already-applied migrations\n\
                 2. Create new migrations for schema changes\n\
                 3. If intentional, switch to AutoReconcile mode"
            }
            Error::DeploymentNotFound { .. } => {
                "The referenced deployment doesn't exist. Verify:\n\
                 1. spec.migrator.deploymentRef.name is correct\n\
                 2. The deployment exists in the same namespace"
            }
            Error::NoContainerImage { .. } => {
                "No container image found. Check:\n\
                 1. The deployment has at least one container\n\
                 2. If using containerName, verify it exists in the deployment"
            }
            Error::InvalidConfig(_) | Error::Configuration(_) => {
                "Check the operator configuration file (default: /etc/shinka/config.yaml). \
                 Validate YAML syntax and required fields."
            }
            Error::MissingField(_) => {
                "A required field is missing in the DatabaseMigration spec. \
                 Check the CRD documentation for required fields."
            }
            Error::InvalidTimeout(_) => {
                "Invalid timeout value. Use formats like '5m', '300s', or '1h'. \
                 Ensure the value is positive and within reasonable bounds."
            }
            Error::Json(_) => {
                "JSON parsing failed. Check status patch payloads and API responses."
            }
            Error::Internal(_) => {
                "Internal error - this should not happen. Please report this issue \
                 at https://github.com/pleme-io/shinka/issues with full context."
            }
            Error::Finalizer(_) => {
                "Finalizer operation failed. If deletion is stuck:\n\
                 1. Check operator logs for details\n\
                 2. Manually remove finalizer if necessary:\n\
                    kubectl patch databasemigration <name> -p '{\"metadata\":{\"finalizers\":null}}' --type=merge"
            }
        }
    }
}

/// Convert kube runtime finalizer errors
impl<ReconcileErr: std::error::Error + 'static> From<kube::runtime::finalizer::Error<ReconcileErr>>
    for Error
{
    fn from(err: kube::runtime::finalizer::Error<ReconcileErr>) -> Self {
        match err {
            kube::runtime::finalizer::Error::ApplyFailed(e) => Error::Finalizer(e.to_string()),
            kube::runtime::finalizer::Error::CleanupFailed(e) => Error::Finalizer(e.to_string()),
            kube::runtime::finalizer::Error::AddFinalizer(e) => {
                Error::Finalizer(format!("failed to add finalizer: {e}"))
            }
            kube::runtime::finalizer::Error::RemoveFinalizer(e) => {
                Error::Finalizer(format!("failed to remove finalizer: {e}"))
            }
            kube::runtime::finalizer::Error::UnnamedObject => {
                Error::Finalizer("unnamed object".to_string())
            }
            kube::runtime::finalizer::Error::InvalidFinalizer => {
                Error::Finalizer("invalid finalizer".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category() {
        assert_eq!(
            Error::ClusterNotFound {
                name: "test".into(),
                namespace: "ns".into()
            }
            .category(),
            "database"
        );
        assert_eq!(
            Error::MigrationFailed {
                name: "m".into(),
                reason: "r".into()
            }
            .category(),
            "migration"
        );
    }

    #[test]
    fn test_error_is_transient() {
        assert!(Error::ClusterNotHealthy {
            name: "test".into(),
            reason: "not ready".into()
        }
        .is_transient());
        assert!(!Error::ClusterNotFound {
            name: "test".into(),
            namespace: "ns".into()
        }
        .is_transient());
    }

    #[test]
    fn test_error_help_not_empty() {
        // All error types should have non-empty help text
        let errors = vec![
            Error::Internal("test".into()),
            Error::Configuration("test".into()),
            Error::ClusterNotFound {
                name: "test".into(),
                namespace: "ns".into(),
            },
            Error::MigrationFailed {
                name: "test".into(),
                reason: "test error".into(),
            },
        ];
        for err in errors {
            assert!(
                !err.help().is_empty(),
                "Help for {:?} should not be empty",
                err
            );
        }
    }

    #[test]
    fn test_error_requeue_duration() {
        assert_eq!(
            Error::ClusterNotHealthy {
                name: "test".into(),
                reason: "not ready".into()
            }
            .requeue_duration(),
            Duration::from_secs(30)
        );
        assert_eq!(
            Error::KubeApi(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource("test".into())
            ))
            .requeue_duration(),
            Duration::from_secs(10)
        );
    }
}
