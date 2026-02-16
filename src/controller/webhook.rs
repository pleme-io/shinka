//! Release Tracker webhook client
//!
//! Sends migration lifecycle events to the Release Tracker service for
//! observability and coordination. All webhook calls are fire-and-forget
//! to avoid blocking migrations on webhook failures.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Migration event types sent to Release Tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationEventType {
    /// Migration started (transitioned to Migrating phase)
    Started,
    /// Migration completed successfully
    Succeeded,
    /// Migration failed (retries exhausted)
    Failed,
    /// Migration auto-retried due to image change
    AutoRetried,
    /// Database health check passed
    HealthCheckPassed,
    /// Database health check failed (waiting)
    HealthCheckFailed,
    /// Checksum mismatch detected (migration file modified after being applied)
    ChecksumMismatch,
    /// Checksum was automatically reconciled (auto-reconcile mode)
    ChecksumReconciled,
    /// Pre-flight validation failed (one or more checksums don't match)
    PreFlightFailed,
    /// Release deployment detected (expected-tag annotation found)
    ReleaseDetected,
}

/// Migration event payload sent to Release Tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationEvent {
    /// Namespace of the DatabaseMigration
    pub namespace: String,
    /// Name of the DatabaseMigration
    pub name: String,
    /// Name of the deployment being watched
    pub deployment: String,
    /// Current image tag
    pub image_tag: String,
    /// Type of event
    pub event_type: MigrationEventType,
    /// Optional message with additional context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Previous image tag (for auto-retry events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_image_tag: Option<String>,
    /// Duration in seconds (for succeeded events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
    /// Retry count (for failed events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
}

/// Release Tracker webhook client
///
/// Sends migration events to the Release Tracker service.
/// All calls are fire-and-forget with a 5-second timeout.
#[derive(Clone)]
pub struct ReleaseTrackerClient {
    client: Client,
    base_url: String,
}

impl ReleaseTrackerClient {
    /// Create a new client with the given base URL and timeout
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be created.
    pub fn new(base_url: String, timeout: Duration) -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(timeout).build()?;

        Ok(Self { client, base_url })
    }

    /// Send a migration event to the Release Tracker
    ///
    /// This is fire-and-forget: errors are logged but not propagated.
    /// We don't want webhook failures to block or slow down migrations.
    pub async fn notify(&self, event: MigrationEvent) {
        let url = format!("{}/webhook/shinka", self.base_url);

        tracing::debug!(
            namespace = %event.namespace,
            name = %event.name,
            event_type = ?event.event_type,
            "Sending migration event to Release Tracker"
        );

        match self.client.post(&url).json(&event).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    tracing::debug!(
                        namespace = %event.namespace,
                        name = %event.name,
                        event_type = ?event.event_type,
                        "Migration event sent successfully"
                    );
                } else {
                    tracing::warn!(
                        namespace = %event.namespace,
                        name = %event.name,
                        event_type = ?event.event_type,
                        status = %response.status(),
                        "Release Tracker returned non-success status"
                    );
                }
            }
            Err(e) => {
                // Log but don't fail - webhook is optional
                tracing::warn!(
                    namespace = %event.namespace,
                    name = %event.name,
                    event_type = ?event.event_type,
                    error = %e,
                    "Failed to send migration event to Release Tracker"
                );
            }
        }
    }

    /// Convenience method to send a "started" event
    pub async fn notify_started(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
    ) {
        self.notify(MigrationEvent {
            namespace: namespace.to_string(),
            name: name.to_string(),
            deployment: deployment.to_string(),
            image_tag: image_tag.to_string(),
            event_type: MigrationEventType::Started,
            message: None,
            previous_image_tag: None,
            duration_secs: None,
            retry_count: None,
        })
        .await;
    }

    /// Convenience method to send a "succeeded" event
    pub async fn notify_succeeded(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        duration_secs: u64,
    ) {
        self.notify(MigrationEvent {
            namespace: namespace.to_string(),
            name: name.to_string(),
            deployment: deployment.to_string(),
            image_tag: image_tag.to_string(),
            event_type: MigrationEventType::Succeeded,
            message: None,
            previous_image_tag: None,
            duration_secs: Some(duration_secs),
            retry_count: None,
        })
        .await;
    }

    /// Convenience method to send a "failed" event
    pub async fn notify_failed(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        error_message: &str,
        retry_count: u32,
    ) {
        self.notify(MigrationEvent {
            namespace: namespace.to_string(),
            name: name.to_string(),
            deployment: deployment.to_string(),
            image_tag: image_tag.to_string(),
            event_type: MigrationEventType::Failed,
            message: Some(error_message.to_string()),
            previous_image_tag: None,
            duration_secs: None,
            retry_count: Some(retry_count),
        })
        .await;
    }

    /// Convenience method to send an "auto-retried" event
    pub async fn notify_auto_retried(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        new_image_tag: &str,
        previous_image_tag: Option<&str>,
    ) {
        self.notify(MigrationEvent {
            namespace: namespace.to_string(),
            name: name.to_string(),
            deployment: deployment.to_string(),
            image_tag: new_image_tag.to_string(),
            event_type: MigrationEventType::AutoRetried,
            message: Some("Auto-retrying migration after image change".to_string()),
            previous_image_tag: previous_image_tag.map(|s| s.to_string()),
            duration_secs: None,
            retry_count: None,
        })
        .await;
    }
}

/// Optional client that only sends events if configured
#[derive(Clone)]
pub struct OptionalReleaseTrackerClient {
    inner: Option<ReleaseTrackerClient>,
}

impl OptionalReleaseTrackerClient {
    /// Create from an optional URL and timeout
    ///
    /// If the URL is provided but client creation fails, logs a warning and
    /// returns a client without the inner ReleaseTrackerClient (effectively disabled).
    pub fn from_url(url: Option<String>, timeout: Duration) -> Self {
        Self {
            inner: url.and_then(|u| match ReleaseTrackerClient::new(u.clone(), timeout) {
                Ok(client) => Some(client),
                Err(e) => {
                    tracing::warn!(
                        url = %u,
                        error = %e,
                        "Failed to create Release Tracker client, webhooks disabled"
                    );
                    None
                }
            }),
        }
    }

    /// Check if the client is configured
    pub fn is_configured(&self) -> bool {
        self.inner.is_some()
    }

    /// Send a migration event (no-op if not configured)
    pub async fn notify(&self, event: MigrationEvent) {
        if let Some(client) = &self.inner {
            client.notify(event).await;
        }
    }

    /// Convenience method to send a "started" event
    pub async fn notify_started(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
    ) {
        if let Some(client) = &self.inner {
            client
                .notify_started(namespace, name, deployment, image_tag)
                .await;
        }
    }

    /// Convenience method to send a "succeeded" event
    pub async fn notify_succeeded(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        duration_secs: u64,
    ) {
        if let Some(client) = &self.inner {
            client
                .notify_succeeded(namespace, name, deployment, image_tag, duration_secs)
                .await;
        }
    }

    /// Convenience method to send a "failed" event
    pub async fn notify_failed(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        error_message: &str,
        retry_count: u32,
    ) {
        if let Some(client) = &self.inner {
            client
                .notify_failed(
                    namespace,
                    name,
                    deployment,
                    image_tag,
                    error_message,
                    retry_count,
                )
                .await;
        }
    }

    /// Convenience method to send an "auto-retried" event
    pub async fn notify_auto_retried(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        new_image_tag: &str,
        previous_image_tag: Option<&str>,
    ) {
        if let Some(client) = &self.inner {
            client
                .notify_auto_retried(
                    namespace,
                    name,
                    deployment,
                    new_image_tag,
                    previous_image_tag,
                )
                .await;
        }
    }
}
