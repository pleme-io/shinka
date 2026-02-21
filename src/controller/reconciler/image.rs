//! Deployment image cache and lookup.

use k8s_openapi::api::apps::v1::Deployment;
use kube::{api::Api, Client};
use std::time::{Duration, Instant};

use crate::{
    migrator::PodScheduling,
    Error, Result,
};

use super::Context;

/// Cached deployment image with expiry time
#[derive(Clone)]
pub(super) struct CachedImage {
    pub image: String,
    pub expires_at: Instant,
}

/// Cache key for deployment images
#[derive(Clone, Hash, Eq, PartialEq)]
pub(super) struct ImageCacheKey {
    pub namespace: String,
    pub deployment: String,
    pub container: Option<String>,
}

impl Context {
    /// Get a cached image or fetch it from the API
    pub(super) async fn get_deployment_image_cached(
        &self,
        namespace: &str,
        deployment_name: &str,
        container_name: Option<&str>,
    ) -> Result<String> {
        let cache_key = ImageCacheKey {
            namespace: namespace.to_string(),
            deployment: deployment_name.to_string(),
            container: container_name.map(|s| s.to_string()),
        };

        // Try to get from cache
        // Note: We use unwrap_or_else to handle lock poisoning gracefully
        // since the cache is not critical - we can always refetch from the API
        {
            let cache = self
                .image_cache
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(cached) = cache.get(&cache_key) {
                if cached.expires_at > Instant::now() {
                    tracing::trace!(
                        namespace = %namespace,
                        deployment = %deployment_name,
                        "Using cached deployment image"
                    );
                    return Ok(cached.image.clone());
                }
            }
        }

        // Fetch from API
        let image =
            get_deployment_image(self.client.clone(), namespace, deployment_name, container_name)
                .await?;

        // Store in cache with 30-second TTL
        {
            let mut cache = self
                .image_cache
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.insert(
                cache_key,
                CachedImage {
                    image: image.clone(),
                    expires_at: Instant::now() + Duration::from_secs(30),
                },
            );

            // Prune expired entries if cache is getting large
            if cache.len() > 100 {
                let now = Instant::now();
                cache.retain(|_, v| v.expires_at > now);
            }
        }

        Ok(image)
    }

    /// Invalidate cached image for a deployment
    ///
    /// Called when the expected-tag annotation is detected, to force a fresh
    /// image lookup instead of waiting for the 30s cache TTL to expire.
    pub fn invalidate_image_cache(&self, namespace: &str, deployment_name: &str) {
        let mut cache = self
            .image_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.retain(|k, _| !(k.namespace == namespace && k.deployment == deployment_name));
    }
}

/// Get the container image from a deployment
pub(super) async fn get_deployment_image(
    client: Client,
    namespace: &str,
    deployment_name: &str,
    container_name: Option<&str>,
) -> Result<String> {
    let deployments: Api<Deployment> = Api::namespaced(client, namespace);

    let deployment = deployments.get(deployment_name).await.map_err(|e| {
        if matches!(
            e,
            kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })
        ) {
            Error::DeploymentNotFound {
                name: deployment_name.to_string(),
                namespace: namespace.to_string(),
            }
        } else {
            Error::KubeApi(e)
        }
    })?;

    let containers = deployment
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(|s| &s.containers)
        .ok_or_else(|| Error::NoContainerImage {
            name: deployment_name.to_string(),
        })?;

    let container = match container_name {
        Some(name) => containers.iter().find(|c| c.name == name),
        None => containers.first(),
    };

    container
        .and_then(|c| c.image.clone())
        .ok_or_else(|| Error::NoContainerImage {
            name: deployment_name.to_string(),
        })
}

/// Get pod scheduling constraints (tolerations, nodeSelector, affinity) from a deployment.
/// Migration jobs must run on the same node(s) as the application to ensure
/// network connectivity to the database and other dependencies.
pub(super) async fn get_deployment_scheduling(
    client: Client,
    namespace: &str,
    deployment_name: &str,
) -> PodScheduling {
    let deployments: Api<Deployment> = Api::namespaced(client, namespace);

    match deployments.get(deployment_name).await {
        Ok(deployment) => {
            let pod_spec = deployment
                .spec
                .as_ref()
                .and_then(|s| s.template.spec.as_ref());

            match pod_spec {
                Some(spec) => PodScheduling {
                    node_selector: spec.node_selector.clone(),
                    tolerations: spec.tolerations.clone(),
                    affinity: spec.affinity.clone(),
                },
                None => PodScheduling::default(),
            }
        }
        Err(e) => {
            tracing::warn!(
                deployment = %deployment_name,
                namespace = %namespace,
                error = %e,
                "Failed to get deployment scheduling constraints, using defaults"
            );
            PodScheduling::default()
        }
    }
}
