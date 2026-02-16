//! CNPG (CloudNativePG) cluster health checks
//!
//! Checks the health of a CNPG Cluster before running migrations.
//! The cluster must be in "Cluster in healthy state" condition.

use kube::{
    api::{Api, DynamicObject},
    discovery::ApiResource,
    Client,
};
use serde::Deserialize;

use crate::{metrics, Error, Result};
use tracing::instrument;

/// CNPG Cluster health status
#[derive(Debug, Clone)]
pub struct CnpgClusterHealth {
    /// Whether the cluster is healthy
    pub healthy: bool,
    /// Current phase of the cluster
    pub phase: String,
    /// Ready instances count
    pub ready_instances: i32,
    /// Total instances count
    pub instances: i32,
    /// Human-readable status message
    pub message: String,
}

/// CNPG Cluster status structure (partial, only what we need)
#[derive(Debug, Deserialize)]
struct CnpgClusterStatus {
    phase: Option<String>,
    instances: Option<i32>,
    #[serde(rename = "readyInstances")]
    ready_instances: Option<i32>,
    conditions: Option<Vec<CnpgCondition>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CnpgCondition {
    #[serde(rename = "type")]
    condition_type: String,
    status: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Check the health of a CNPG cluster
///
/// Returns the health status of the cluster. A cluster is considered healthy when:
/// 1. It exists
/// 2. The phase is "Cluster in healthy state"
/// 3. Ready instances >= expected instances
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn check_cluster_health(
    client: Client,
    namespace: &str,
    cluster_name: &str,
) -> Result<CnpgClusterHealth> {
    tracing::debug!(
        namespace = %namespace,
        cluster = %cluster_name,
        "Checking CNPG cluster health"
    );

    // Define the CNPG Cluster API resource
    let api_resource = ApiResource {
        group: "postgresql.cnpg.io".to_string(),
        version: "v1".to_string(),
        kind: "Cluster".to_string(),
        api_version: "postgresql.cnpg.io/v1".to_string(),
        plural: "clusters".to_string(),
    };

    let clusters: Api<DynamicObject> = Api::namespaced_with(client, namespace, &api_resource);

    // Get the cluster
    let cluster = clusters.get(cluster_name).await.map_err(|e| {
        if is_not_found(&e) {
            Error::ClusterNotFound {
                name: cluster_name.to_string(),
                namespace: namespace.to_string(),
            }
        } else {
            Error::KubeApi(e)
        }
    })?;

    // Parse the status
    let status: Option<CnpgClusterStatus> = cluster
        .data
        .get("status")
        .and_then(|s| serde_json::from_value(s.clone()).ok());

    let health = match status {
        Some(s) => {
            let phase = s.phase.unwrap_or_else(|| "Unknown".to_string());
            let instances = s.instances.unwrap_or(0);
            let ready_instances = s.ready_instances.unwrap_or(0);

            // Check for healthy condition
            let is_healthy = s.conditions.as_ref().is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.condition_type == "Ready" && c.status == "True")
            });

            // Alternative check: phase-based
            let phase_healthy = phase == "Cluster in healthy state";

            let healthy =
                is_healthy || (phase_healthy && ready_instances >= instances && instances > 0);

            let message = if healthy {
                format!(
                    "Cluster healthy: {}/{} instances ready",
                    ready_instances, instances
                )
            } else {
                format!(
                    "Cluster not healthy: phase={}, {}/{} instances ready",
                    phase, ready_instances, instances
                )
            };

            CnpgClusterHealth {
                healthy,
                phase,
                ready_instances,
                instances,
                message,
            }
        }
        None => CnpgClusterHealth {
            healthy: false,
            phase: "Unknown".to_string(),
            ready_instances: 0,
            instances: 0,
            message: "Cluster status not available".to_string(),
        },
    };

    // Record health metric
    metrics::record_database_health(cluster_name, namespace, health.healthy);

    tracing::info!(
        namespace = %namespace,
        cluster = %cluster_name,
        healthy = %health.healthy,
        phase = %health.phase,
        ready_instances = %health.ready_instances,
        instances = %health.instances,
        "CNPG cluster health check complete"
    );

    Ok(health)
}

/// Check if a kube error is a NotFound error
fn is_not_found(err: &kube::Error) -> bool {
    matches!(
        err,
        kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })
    )
}
