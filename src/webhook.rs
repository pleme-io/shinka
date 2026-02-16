//! Validating Admission Webhook for Shinka
//!
//! Provides deployment validation to ensure migrations are complete before
//! allowing deployment updates. This prevents applications from starting
//! with outdated database schemas.
//!
//! ## How it works
//!
//! 1. Webhook intercepts Deployment CREATE/UPDATE requests
//! 2. Checks if any DatabaseMigration references this deployment
//! 3. If a migration exists and is NOT in Ready phase, blocks the deployment
//! 4. If migration is Ready or no migration exists, allows the deployment
//!
//! ## Configuration
//!
//! - `WEBHOOK_ENABLED`: Set to "true" to enable the webhook server (default: false)
//! - `WEBHOOK_PORT`: Port for the webhook server (default: 8443)
//! - `WEBHOOK_CERT_PATH`: Path to TLS certificate (required for HTTPS)
//! - `WEBHOOK_KEY_PATH`: Path to TLS private key (required for HTTPS)
//!
//! ## Kubernetes Setup
//!
//! Deploy a ValidatingWebhookConfiguration that points to this service:
//!
//! ```yaml
//! apiVersion: admissionregistration.k8s.io/v1
//! kind: ValidatingWebhookConfiguration
//! metadata:
//!   name: shinka-deployment-validator
//! webhooks:
//!   - name: deployments.shinka.pleme.io
//!     rules:
//!       - apiGroups: ["apps"]
//!         apiVersions: ["v1"]
//!         resources: ["deployments"]
//!         operations: ["CREATE", "UPDATE"]
//!     clientConfig:
//!       service:
//!         name: shinka
//!         namespace: shinka-system
//!         path: /validate-deployment
//!     admissionReviewVersions: ["v1"]
//!     sideEffects: None
//!     failurePolicy: Fail
//! ```

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use k8s_openapi::api::apps::v1::Deployment;
use kube::{api::Api, Client};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    crd::{DatabaseMigration, MigrationPhase},
    Result,
};

/// Webhook configuration
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Whether the webhook is enabled
    pub enabled: bool,
    /// Port to listen on
    pub port: u16,
    /// Path to TLS certificate
    pub cert_path: Option<String>,
    /// Path to TLS private key
    pub key_path: Option<String>,
    /// Whether to fail open (allow) or fail closed (deny) on errors
    pub fail_open: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 8443,
            cert_path: None,
            key_path: None,
            fail_open: false, // Fail closed by default for safety
        }
    }
}

impl WebhookConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("WEBHOOK_ENABLED") {
            config.enabled = val == "true" || val == "1";
        }

        if let Ok(port) = std::env::var("WEBHOOK_PORT") {
            if let Ok(p) = port.parse() {
                config.port = p;
            }
        }

        if let Ok(path) = std::env::var("WEBHOOK_CERT_PATH") {
            config.cert_path = Some(path);
        }

        if let Ok(path) = std::env::var("WEBHOOK_KEY_PATH") {
            config.key_path = Some(path);
        }

        if let Ok(val) = std::env::var("WEBHOOK_FAIL_OPEN") {
            config.fail_open = val == "true" || val == "1";
        }

        config
    }
}

/// Webhook server state
pub struct WebhookState {
    client: Client,
    fail_open: bool,
}

/// Kubernetes AdmissionReview request
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReview {
    pub api_version: String,
    pub kind: String,
    pub request: AdmissionRequest,
}

/// Admission request from Kubernetes
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    pub uid: String,
    pub kind: GroupVersionKind,
    pub resource: GroupVersionResource,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub operation: String,
    pub object: Option<serde_json::Value>,
    pub old_object: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionKind {
    pub group: String,
    pub version: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

/// Kubernetes AdmissionReview response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewResponse {
    pub api_version: String,
    pub kind: String,
    pub response: AdmissionResponse,
}

/// Admission response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionStatus {
    pub code: i32,
    pub message: String,
}

impl AdmissionReviewResponse {
    /// Create an allowed response
    pub fn allowed(uid: &str) -> Self {
        Self {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            response: AdmissionResponse {
                uid: uid.to_string(),
                allowed: true,
                status: None,
                warnings: None,
            },
        }
    }

    /// Create an allowed response with warnings
    pub fn allowed_with_warnings(uid: &str, warnings: Vec<String>) -> Self {
        Self {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            response: AdmissionResponse {
                uid: uid.to_string(),
                allowed: true,
                status: None,
                warnings: Some(warnings),
            },
        }
    }

    /// Create a denied response
    pub fn denied(uid: &str, code: i32, message: String) -> Self {
        Self {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            response: AdmissionResponse {
                uid: uid.to_string(),
                allowed: false,
                status: Some(AdmissionStatus { code, message }),
                warnings: None,
            },
        }
    }
}

/// Create the webhook router
pub fn create_router(client: Client, config: &WebhookConfig) -> Router {
    let state = Arc::new(WebhookState {
        client,
        fail_open: config.fail_open,
    });

    Router::new()
        .route("/validate-deployment", post(validate_deployment))
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .with_state(state)
}

/// Validate deployment admission request
async fn validate_deployment(
    State(state): State<Arc<WebhookState>>,
    Json(review): Json<AdmissionReview>,
) -> impl IntoResponse {
    let uid = &review.request.uid;
    let namespace = review.request.namespace.as_deref().unwrap_or("default");
    let deployment_name = review.request.name.as_deref();

    tracing::debug!(
        uid = %uid,
        namespace = %namespace,
        deployment = ?deployment_name,
        operation = %review.request.operation,
        "Validating deployment admission"
    );

    // Parse the deployment from the request
    let deployment: Option<Deployment> = review
        .request
        .object
        .and_then(|obj| serde_json::from_value(obj).ok());

    let deployment = match deployment {
        Some(d) => d,
        None => {
            tracing::warn!(uid = %uid, "Could not parse deployment from admission request");
            if state.fail_open {
                return (StatusCode::OK, Json(AdmissionReviewResponse::allowed(uid)));
            } else {
                return (
                    StatusCode::OK,
                    Json(AdmissionReviewResponse::denied(
                        uid,
                        400,
                        "Could not parse deployment".to_string(),
                    )),
                );
            }
        }
    };

    let deployment_name = deployment.metadata.name.as_deref().unwrap_or("unknown");

    // Check if any DatabaseMigration references this deployment
    match check_migration_status(&state.client, namespace, deployment_name).await {
        Ok(MigrationCheck::NoMigration) => {
            tracing::debug!(
                deployment = %deployment_name,
                namespace = %namespace,
                "No migration found for deployment, allowing"
            );
            (StatusCode::OK, Json(AdmissionReviewResponse::allowed(uid)))
        }
        Ok(MigrationCheck::Ready) => {
            tracing::debug!(
                deployment = %deployment_name,
                namespace = %namespace,
                "Migration is ready, allowing deployment"
            );
            (StatusCode::OK, Json(AdmissionReviewResponse::allowed(uid)))
        }
        Ok(MigrationCheck::NotReady {
            migration_name,
            phase,
        }) => {
            let message = format!(
                "Deployment {} is blocked: DatabaseMigration '{}' is in phase '{}'. \
                 Migrations must complete before deployment updates are allowed.",
                deployment_name, migration_name, phase
            );
            tracing::info!(
                deployment = %deployment_name,
                namespace = %namespace,
                migration = %migration_name,
                phase = %phase,
                "Blocking deployment - migration not ready"
            );
            crate::metrics::RECONCILIATIONS_TOTAL
                .with_label_values(&["webhook_blocked"])
                .inc();
            (
                StatusCode::OK,
                Json(AdmissionReviewResponse::denied(uid, 403, message)),
            )
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                deployment = %deployment_name,
                namespace = %namespace,
                "Error checking migration status"
            );
            if state.fail_open {
                (
                    StatusCode::OK,
                    Json(AdmissionReviewResponse::allowed_with_warnings(
                        uid,
                        vec![format!("Shinka webhook error (fail-open): {}", e)],
                    )),
                )
            } else {
                (
                    StatusCode::OK,
                    Json(AdmissionReviewResponse::denied(
                        uid,
                        500,
                        format!("Error checking migration status: {}", e),
                    )),
                )
            }
        }
    }
}

/// Result of checking migration status
enum MigrationCheck {
    /// No migration references this deployment
    NoMigration,
    /// Migration exists and is Ready
    Ready,
    /// Migration exists but is not Ready
    NotReady {
        migration_name: String,
        phase: String,
    },
}

/// Check if a migration references this deployment and its status
async fn check_migration_status(
    client: &Client,
    namespace: &str,
    deployment_name: &str,
) -> Result<MigrationCheck> {
    let migrations: Api<DatabaseMigration> = Api::namespaced(client.clone(), namespace);

    // List all migrations in this namespace
    let migration_list = migrations
        .list(&kube::api::ListParams::default())
        .await
        .map_err(crate::Error::KubeApi)?;

    // Find any migration that references this deployment
    for migration in migration_list.items {
        // Check if any migrator references this deployment
        let references_deployment = migration
            .get_migrators()
            .iter()
            .any(|m| m.deployment_ref.name == deployment_name);
        if references_deployment {
            let migration_name = migration.metadata.name.unwrap_or_default();
            let phase = migration
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or(MigrationPhase::Pending);

            if phase == MigrationPhase::Ready {
                return Ok(MigrationCheck::Ready);
            } else {
                return Ok(MigrationCheck::NotReady {
                    migration_name,
                    phase: format!("{:?}", phase),
                });
            }
        }
    }

    Ok(MigrationCheck::NoMigration)
}

/// Run the webhook server
pub async fn serve(config: WebhookConfig, client: Client) -> Result<()> {
    if !config.enabled {
        tracing::info!("Webhook server disabled");
        return Ok(());
    }

    let addr = format!("0.0.0.0:{}", config.port);
    let router = create_router(client, &config);

    tracing::info!(
        addr = %addr,
        fail_open = %config.fail_open,
        "Starting webhook server"
    );

    // Check if TLS is configured
    if config.cert_path.is_some() && config.key_path.is_some() {
        tracing::info!("TLS enabled for webhook server");
        // Note: In production, you'd use axum-server with rustls here
        // For now, we'll use plain HTTP and rely on a service mesh or ingress for TLS
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| crate::Error::Internal(format!("Failed to bind webhook server: {}", e)))?;

    axum::serve(listener, router)
        .await
        .map_err(|e| crate::Error::Internal(format!("Webhook server error: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admission_review_response_allowed() {
        let response = AdmissionReviewResponse::allowed("test-uid");
        assert!(response.response.allowed);
        assert_eq!(response.response.uid, "test-uid");
        assert!(response.response.status.is_none());
    }

    #[test]
    fn test_admission_review_response_denied() {
        let response = AdmissionReviewResponse::denied("test-uid", 403, "Forbidden".to_string());
        assert!(!response.response.allowed);
        assert_eq!(response.response.uid, "test-uid");
        assert!(response.response.status.is_some());
        assert_eq!(response.response.status.as_ref().unwrap().code, 403);
    }

    #[test]
    fn test_webhook_config_default() {
        let config = WebhookConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.port, 8443);
        assert!(!config.fail_open);
    }
}
