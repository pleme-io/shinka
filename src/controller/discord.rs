//! Discord webhook client for migration notifications
//!
//! Sends beautiful, rich Discord embeds for migration lifecycle events.
//! All webhook calls are fire-and-forget to avoid blocking migrations.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::webhook::MigrationEventType;
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::util;

/// Discord embed colors (decimal RGB values)
mod colors {
    /// Green - Success
    pub const SUCCESS: u32 = 0x00D26A;
    /// Red - Failure
    pub const FAILURE: u32 = 0xFF4B4B;
    /// Blue - Started/In Progress
    pub const INFO: u32 = 0x5865F2;
    /// Yellow - Warning/Retry
    pub const WARNING: u32 = 0xFEE75C;
    /// Purple - Health Check
    pub const HEALTH: u32 = 0xA855F7;
    /// Orange - Checksum Issues
    pub const CHECKSUM: u32 = 0xF97316;
}

/// Discord webhook message structure
#[derive(Debug, Clone, Serialize)]
pub struct DiscordWebhook {
    /// Username to display (optional, uses webhook default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Avatar URL (optional, uses webhook default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,

    /// Plain text content (optional if embeds provided)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,

    /// Rich embeds
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub embeds: Vec<DiscordEmbed>,
}

/// Discord rich embed structure
#[derive(Debug, Clone, Serialize, Default)]
pub struct DiscordEmbed {
    /// Embed title
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Embed description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// URL to link the title to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Color (decimal RGB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<u32>,

    /// Embed fields
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<DiscordField>,

    /// Author information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<DiscordAuthor>,

    /// Footer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer: Option<DiscordFooter>,

    /// Thumbnail image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail: Option<DiscordImage>,

    /// Timestamp (ISO8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// Discord embed field
#[derive(Debug, Clone, Serialize)]
pub struct DiscordField {
    /// Field name
    pub name: String,

    /// Field value
    pub value: String,

    /// Display inline with other fields
    #[serde(default)]
    pub inline: bool,
}

/// Discord embed author
#[derive(Debug, Clone, Serialize)]
pub struct DiscordAuthor {
    /// Author name
    pub name: String,

    /// Author URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Author icon URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Discord embed footer
#[derive(Debug, Clone, Serialize)]
pub struct DiscordFooter {
    /// Footer text
    pub text: String,

    /// Footer icon URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Discord embed image
#[derive(Debug, Clone, Serialize)]
pub struct DiscordImage {
    /// Image URL
    pub url: String,
}

/// Additional context for enriched Discord embeds
///
/// Used to pass release-specific information without growing build_embed() parameter list.
#[derive(Debug, Default)]
pub struct EmbedContext<'a> {
    /// Current deployment image tag
    pub deployment_current_tag: Option<&'a str>,
    /// Expected release tag (from annotation)
    pub expected_release_tag: Option<&'a str>,
    /// CNPG health status string
    pub cnpg_health: Option<&'a str>,
    /// CNPG instance count string
    pub cnpg_instances: Option<&'a str>,
}

/// Discord notification configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscordConfig {
    /// Webhook URL
    pub webhook_url: Option<String>,

    /// Custom bot username (default: "Shinka")
    #[serde(default = "default_username")]
    pub username: String,

    /// Custom bot avatar URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,

    /// Cluster name for identification (e.g., "plo", "orion")
    #[serde(default = "default_cluster")]
    pub cluster_name: String,

    /// Environment label (e.g., "production", "staging")
    #[serde(default = "default_environment")]
    pub environment: String,

    /// Enable notifications for successful migrations
    #[serde(default = "crate::util::default_true")]
    pub notify_on_success: bool,

    /// Enable notifications for failed migrations
    #[serde(default = "crate::util::default_true")]
    pub notify_on_failure: bool,

    /// Enable notifications for started migrations
    #[serde(default = "crate::util::default_true")]
    pub notify_on_started: bool,

    /// Enable notifications for auto-retries
    #[serde(default = "crate::util::default_true")]
    pub notify_on_retry: bool,

    /// Enable notifications for health check events
    #[serde(default = "crate::util::default_false")]
    pub notify_on_health: bool,

    /// Enable notifications for checksum safety events (mismatches, reconciliations, pre-flight)
    #[serde(default = "crate::util::default_true")]
    pub notify_on_checksum: bool,

    /// Enable notifications for release detection events
    #[serde(default = "crate::util::default_true")]
    pub notify_on_release: bool,

    /// Mention role ID for failures (e.g., "1234567890")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_mention_role: Option<String>,

    /// Mention user IDs for critical failures
    #[serde(default)]
    pub failure_mention_users: Vec<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            webhook_url: None,
            username: default_username(),
            avatar_url: None,
            cluster_name: default_cluster(),
            environment: default_environment(),
            notify_on_success: true,
            notify_on_failure: true,
            notify_on_started: true,
            notify_on_retry: true,
            notify_on_health: false,
            notify_on_checksum: true,
            notify_on_release: true,
            failure_mention_role: None,
            failure_mention_users: Vec::new(),
        }
    }
}

impl DiscordConfig {
    /// Check if a given event type should trigger a notification.
    pub fn should_notify(&self, event_type: &MigrationEventType) -> bool {
        match event_type {
            MigrationEventType::Started => self.notify_on_started,
            MigrationEventType::Succeeded => self.notify_on_success,
            MigrationEventType::Failed => self.notify_on_failure,
            MigrationEventType::AutoRetried => self.notify_on_retry,
            MigrationEventType::HealthCheckPassed | MigrationEventType::HealthCheckFailed => {
                self.notify_on_health
            }
            MigrationEventType::ChecksumMismatch
            | MigrationEventType::ChecksumReconciled
            | MigrationEventType::PreFlightFailed => self.notify_on_checksum,
            MigrationEventType::ReleaseDetected => self.notify_on_release,
        }
    }

    /// Build Discord mention string for failure events.
    ///
    /// Returns `None` when no mentions are configured.
    pub fn build_failure_mentions(&self) -> Option<String> {
        let mut mentions = Vec::new();

        if let Some(role_id) = &self.failure_mention_role {
            mentions.push(format!("<@&{}>", role_id));
        }

        for user_id in &self.failure_mention_users {
            mentions.push(format!("<@{}>", user_id));
        }

        if mentions.is_empty() {
            None
        } else {
            Some(mentions.join(" "))
        }
    }
}

fn default_username() -> String {
    "Shinka".to_string()
}

fn default_cluster() -> String {
    "kubernetes".to_string()
}

fn default_environment() -> String {
    "unknown".to_string()
}

/// Operator lifecycle event types
#[derive(Debug, Clone)]
pub enum OperatorEventType {
    /// Operator has started
    Started,
    /// Operator is shutting down
    ShuttingDown,
}

/// Discord webhook client
#[derive(Clone)]
pub struct DiscordClient {
    client: Client,
    webhook_url: String,
    config: DiscordConfig,
}

impl DiscordClient {
    /// Create a new Discord client
    pub fn new(
        webhook_url: String,
        config: DiscordConfig,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(timeout).build()?;

        Ok(Self {
            client,
            webhook_url,
            config,
        })
    }

    /// Get the current timestamp in ISO8601 format
    fn now_iso8601() -> String {
        chrono::Utc::now().to_rfc3339()
    }

    /// Build an embed for a migration event
    #[allow(clippy::too_many_arguments)]
    fn build_embed(
        &self,
        event_type: &MigrationEventType,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        message: Option<&str>,
        duration_secs: Option<u64>,
        retry_count: Option<u32>,
        previous_image_tag: Option<&str>,
        embed_ctx: &EmbedContext<'_>,
    ) -> DiscordEmbed {
        let (title, color, emoji) = match event_type {
            MigrationEventType::Started => (
                "Migration Started",
                colors::INFO,
                "\u{1F680}", // rocket
            ),
            MigrationEventType::Succeeded => (
                "Migration Succeeded",
                colors::SUCCESS,
                "\u{2705}", // check mark
            ),
            MigrationEventType::Failed => (
                "Migration Failed",
                colors::FAILURE,
                "\u{274C}", // X mark
            ),
            MigrationEventType::AutoRetried => (
                "Migration Auto-Retried",
                colors::WARNING,
                "\u{1F504}", // arrows counterclockwise
            ),
            MigrationEventType::HealthCheckPassed => (
                "Health Check Passed",
                colors::HEALTH,
                "\u{1F49A}", // green heart
            ),
            MigrationEventType::HealthCheckFailed => (
                "Health Check Failed",
                colors::WARNING,
                "\u{1F49B}", // yellow heart
            ),
            MigrationEventType::ChecksumMismatch => (
                "Checksum Mismatch Detected",
                colors::CHECKSUM,
                "\u{26A0}\u{FE0F}", // warning sign
            ),
            MigrationEventType::ChecksumReconciled => (
                "Checksum Auto-Reconciled",
                colors::WARNING,
                "\u{1F527}", // wrench
            ),
            MigrationEventType::PreFlightFailed => (
                "Pre-Flight Validation Failed",
                colors::FAILURE,
                "\u{1F6AB}", // no entry
            ),
            MigrationEventType::ReleaseDetected => (
                "Release Deployment Detected",
                colors::INFO,
                "\u{1F4E6}", // package
            ),
        };

        let mut fields = vec![
            DiscordField {
                name: "Namespace".to_string(),
                value: format!("`{}`", namespace),
                inline: true,
            },
            DiscordField {
                name: "Migration".to_string(),
                value: format!("`{}`", name),
                inline: true,
            },
            DiscordField {
                name: "Deployment".to_string(),
                value: format!("`{}`", deployment),
                inline: true,
            },
            DiscordField {
                name: "Image Tag".to_string(),
                value: format!("`{}`", util::truncate_tag(image_tag, 40)),
                inline: true,
            },
        ];

        // Add duration for success events
        if let Some(secs) = duration_secs {
            fields.push(DiscordField {
                name: "Duration".to_string(),
                value: format!("`{}`", util::format_duration(secs)),
                inline: true,
            });
        }

        // Add retry count for failed events
        if let Some(count) = retry_count {
            fields.push(DiscordField {
                name: "Retries".to_string(),
                value: format!("`{}`", count),
                inline: true,
            });
        }

        // Add previous image tag for auto-retry events
        if let Some(prev_tag) = previous_image_tag {
            fields.push(DiscordField {
                name: "Previous Tag".to_string(),
                value: format!("`{}`", util::truncate_tag(prev_tag, 40)),
                inline: true,
            });
        }

        // Add release context fields from EmbedContext
        if let Some(expected) = embed_ctx.expected_release_tag {
            fields.push(DiscordField {
                name: "Release Deployment".to_string(),
                value: format!("Awaiting `{}`", util::truncate_tag(expected, 40)),
                inline: true,
            });
        }

        if let Some(current) = embed_ctx.deployment_current_tag {
            if embed_ctx.expected_release_tag.is_some() {
                fields.push(DiscordField {
                    name: "Deployment Tag".to_string(),
                    value: format!("`{}`", util::truncate_tag(current, 40)),
                    inline: true,
                });
            }
        }

        if let Some(health) = embed_ctx.cnpg_health {
            let instances = embed_ctx.cnpg_instances.unwrap_or("?");
            fields.push(DiscordField {
                name: "Database Health".to_string(),
                value: format!("{} ({} instances)", health, instances),
                inline: true,
            });
        }

        // Add debug commands for failed events
        if matches!(event_type, MigrationEventType::Failed) {
            fields.push(DiscordField {
                name: "Debug Commands".to_string(),
                value: format!(
                    "```\nkubectl get databasemigration {} -n {} -o yaml\nkubectl logs -n {} -l app={},component=migration --tail=50\n```",
                    name, namespace, namespace, name
                ),
                inline: false,
            });
        }

        // Add error message if present
        let description = message.map(|m| {
            if m.len() > 500 {
                format!("```\n{}...\n```", &m[..497])
            } else {
                format!("```\n{}\n```", m)
            }
        });

        DiscordEmbed {
            title: Some(format!("{} {}", emoji, title)),
            description,
            color: Some(color),
            fields,
            author: Some(DiscordAuthor {
                name: format!(
                    "Shinka | {} | {}",
                    self.config.cluster_name, self.config.environment
                ),
                url: None,
                icon_url: Some(
                    "https://raw.githubusercontent.com/pleme-io/assets/main/shinka-icon.png"
                        .to_string(),
                ),
            }),
            footer: Some(DiscordFooter {
                text: format!("{}/{}", namespace, name),
                icon_url: None,
            }),
            timestamp: Some(Self::now_iso8601()),
            ..Default::default()
        }
    }

    /// Send a migration event notification with result for circuit breaker
    #[allow(clippy::too_many_arguments)]
    pub async fn notify_with_result(
        &self,
        event_type: MigrationEventType,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        message: Option<&str>,
        duration_secs: Option<u64>,
        retry_count: Option<u32>,
        previous_image_tag: Option<&str>,
    ) -> Result<(), String> {
        if !self.config.should_notify(&event_type) {
            tracing::debug!(
                namespace = %namespace,
                name = %name,
                event_type = ?event_type,
                "Discord notification skipped (disabled by config)"
            );
            return Ok(());
        }

        let embed = self.build_embed(
            &event_type,
            namespace,
            name,
            deployment,
            image_tag,
            message,
            duration_secs,
            retry_count,
            previous_image_tag,
            &EmbedContext::default(),
        );

        let content = if matches!(event_type, MigrationEventType::Failed) {
            self.config.build_failure_mentions()
        } else {
            None
        };

        let webhook = DiscordWebhook {
            username: Some(self.config.username.clone()),
            avatar_url: self.config.avatar_url.clone(),
            content,
            embeds: vec![embed],
        };

        tracing::debug!(
            namespace = %namespace,
            name = %name,
            event_type = ?event_type,
            "Sending Discord notification"
        );

        match self
            .client
            .post(&self.webhook_url)
            .json(&webhook)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    tracing::debug!(
                        namespace = %namespace,
                        name = %name,
                        event_type = ?event_type,
                        "Discord notification sent successfully"
                    );
                    Ok(())
                } else {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    Err(format!("Discord webhook returned {}: {}", status, body))
                }
            }
            Err(e) => Err(format!("Failed to send Discord notification: {}", e)),
        }
    }

    /// Send a migration event notification (fire-and-forget, logs errors)
    #[allow(clippy::too_many_arguments)]
    pub async fn notify(
        &self,
        event_type: MigrationEventType,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        message: Option<&str>,
        duration_secs: Option<u64>,
        retry_count: Option<u32>,
        previous_image_tag: Option<&str>,
    ) {
        if let Err(e) = self
            .notify_with_result(
                event_type.clone(),
                namespace,
                name,
                deployment,
                image_tag,
                message,
                duration_secs,
                retry_count,
                previous_image_tag,
            )
            .await
        {
            tracing::warn!(
                namespace = %namespace,
                name = %name,
                event_type = ?event_type,
                error = %e,
                "Discord notification failed"
            );
        }
    }

    /// Convenience: notify migration started
    pub async fn notify_started(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
    ) {
        self.notify(
            MigrationEventType::Started,
            namespace,
            name,
            deployment,
            image_tag,
            None,
            None,
            None,
            None,
        )
        .await;
    }

    /// Convenience: notify migration succeeded
    pub async fn notify_succeeded(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        duration_secs: u64,
    ) {
        self.notify(
            MigrationEventType::Succeeded,
            namespace,
            name,
            deployment,
            image_tag,
            None,
            Some(duration_secs),
            None,
            None,
        )
        .await;
    }

    /// Convenience: notify migration failed
    pub async fn notify_failed(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        error_message: &str,
        retry_count: u32,
    ) {
        self.notify(
            MigrationEventType::Failed,
            namespace,
            name,
            deployment,
            image_tag,
            Some(error_message),
            None,
            Some(retry_count),
            None,
        )
        .await;
    }

    /// Convenience: notify migration auto-retried
    ///
    /// If `previous_error` is provided, it will be included in the notification
    /// to show what caused the previous failure.
    pub async fn notify_auto_retried(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        new_image_tag: &str,
        previous_image_tag: Option<&str>,
        previous_error: Option<&str>,
    ) {
        // Build message with previous error if available
        let message = if let Some(err) = previous_error {
            // Truncate long errors for the message but still show them
            let error_preview = if err.len() > 300 {
                format!("{}...", &err[..297])
            } else {
                err.to_string()
            };
            format!(
                "Auto-retrying migration after image change.\n\n**Previous Failure:**\n{}",
                error_preview
            )
        } else {
            "Auto-retrying migration after image change".to_string()
        };

        self.notify(
            MigrationEventType::AutoRetried,
            namespace,
            name,
            deployment,
            new_image_tag,
            Some(&message),
            None,
            None,
            previous_image_tag,
        )
        .await;
    }

    /// Notify about operator lifecycle events (startup/shutdown)
    pub async fn notify_operator_event(&self, event: OperatorEventType, version: &str) {
        let (title, color, emoji, description) = match event {
            OperatorEventType::Started => (
                "Operator Started",
                colors::SUCCESS,
                "\u{1F7E2}", // green circle
                "Shinka operator is now running and watching for migrations",
            ),
            OperatorEventType::ShuttingDown => (
                "Operator Shutting Down",
                colors::WARNING,
                "\u{1F7E1}", // yellow circle
                "Shinka operator is shutting down gracefully",
            ),
        };

        let embed = DiscordEmbed {
            title: Some(format!("{} {}", emoji, title)),
            description: Some(description.to_string()),
            color: Some(color),
            fields: vec![
                DiscordField {
                    name: "Cluster".to_string(),
                    value: format!("`{}`", self.config.cluster_name),
                    inline: true,
                },
                DiscordField {
                    name: "Environment".to_string(),
                    value: format!("`{}`", self.config.environment),
                    inline: true,
                },
                DiscordField {
                    name: "Version".to_string(),
                    value: format!("`{}`", version),
                    inline: true,
                },
            ],
            author: Some(DiscordAuthor {
                name: "Shinka Operator".to_string(),
                url: None,
                icon_url: Some(
                    "https://raw.githubusercontent.com/pleme-io/assets/main/shinka-icon.png"
                        .to_string(),
                ),
            }),
            footer: Some(DiscordFooter {
                text: format!("{} / {}", self.config.cluster_name, self.config.environment),
                icon_url: None,
            }),
            timestamp: Some(Self::now_iso8601()),
            ..Default::default()
        };

        let webhook = DiscordWebhook {
            username: Some(self.config.username.clone()),
            avatar_url: self.config.avatar_url.clone(),
            content: None,
            embeds: vec![embed],
        };

        tracing::debug!(
            event_type = ?event,
            cluster = %self.config.cluster_name,
            environment = %self.config.environment,
            "Sending operator lifecycle Discord notification"
        );

        match self
            .client
            .post(&self.webhook_url)
            .json(&webhook)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    tracing::debug!(
                        event_type = ?event,
                        "Operator lifecycle Discord notification sent successfully"
                    );
                } else {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    tracing::warn!(
                        event_type = ?event,
                        status = %status,
                        body = %body,
                        "Discord webhook returned non-success status for operator event"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    event_type = ?event,
                    error = %e,
                    "Failed to send operator lifecycle Discord notification"
                );
            }
        }
    }
}

/// Optional Discord client that only sends events if configured
///
/// Includes a circuit breaker to prevent repeated failures from impacting
/// migration performance when Discord is unavailable.
#[derive(Clone)]
pub struct OptionalDiscordClient {
    inner: Option<DiscordClient>,
    circuit_breaker: CircuitBreaker,
}

impl OptionalDiscordClient {
    /// Create from configuration
    pub fn from_config(config: DiscordConfig, timeout: Duration) -> Self {
        let circuit_breaker = CircuitBreaker::new(
            CircuitBreakerConfig::new("discord")
                .with_failure_threshold(3) // Trip after 3 consecutive failures
                .with_success_threshold(2) // Close after 2 successes
                .with_open_timeout(Duration::from_secs(60)), // Wait 60s before retrying
        );

        Self {
            inner: config.webhook_url.as_ref().and_then(|url| {
                if url.is_empty() {
                    return None;
                }
                match DiscordClient::new(url.clone(), config.clone(), timeout) {
                    Ok(client) => {
                        tracing::info!(
                            cluster = %config.cluster_name,
                            environment = %config.environment,
                            "Discord notifications enabled (with circuit breaker)"
                        );
                        Some(client)
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to create Discord client, notifications disabled"
                        );
                        None
                    }
                }
            }),
            circuit_breaker,
        }
    }

    /// Check if Discord notifications are configured
    pub fn is_configured(&self) -> bool {
        self.inner.is_some()
    }

    /// Check if the circuit breaker is allowing requests
    pub async fn is_available(&self) -> bool {
        self.inner.is_some() && self.circuit_breaker.should_allow().await
    }

    /// Get circuit breaker statistics
    pub fn circuit_stats(&self) -> crate::circuit_breaker::CircuitBreakerStats {
        self.circuit_breaker.stats()
    }

    /// Send notification (no-op if not configured or circuit breaker is open)
    #[allow(clippy::too_many_arguments)]
    pub async fn notify(
        &self,
        event_type: MigrationEventType,
        namespace: &str,
        name: &str,
        deployment: &str,
        image_tag: &str,
        message: Option<&str>,
        duration_secs: Option<u64>,
        retry_count: Option<u32>,
        previous_image_tag: Option<&str>,
    ) {
        let Some(client) = &self.inner else {
            return;
        };

        // Check circuit breaker
        if !self.circuit_breaker.should_allow().await {
            tracing::debug!(
                namespace = %namespace,
                name = %name,
                event_type = ?event_type,
                "Discord notification skipped (circuit breaker open)"
            );
            return;
        }

        // Send notification and track success/failure for circuit breaker
        let result = client
            .notify_with_result(
                event_type.clone(),
                namespace,
                name,
                deployment,
                image_tag,
                message,
                duration_secs,
                retry_count,
                previous_image_tag,
            )
            .await;

        match result {
            Ok(()) => {
                self.circuit_breaker.record_success().await;
            }
            Err(e) => {
                tracing::warn!(
                    namespace = %namespace,
                    name = %name,
                    event_type = ?event_type,
                    error = %e,
                    "Discord notification failed (recorded for circuit breaker)"
                );
                self.circuit_breaker.record_failure().await;
            }
        }
    }

    /// Convenience: notify migration started
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

    /// Convenience: notify migration succeeded
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

    /// Convenience: notify migration failed
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

    /// Convenience: notify migration auto-retried
    pub async fn notify_auto_retried(
        &self,
        namespace: &str,
        name: &str,
        deployment: &str,
        new_image_tag: &str,
        previous_image_tag: Option<&str>,
        previous_error: Option<&str>,
    ) {
        if let Some(client) = &self.inner {
            client
                .notify_auto_retried(
                    namespace,
                    name,
                    deployment,
                    new_image_tag,
                    previous_image_tag,
                    previous_error,
                )
                .await;
        }
    }

    /// Notify about operator lifecycle events (startup/shutdown)
    pub async fn notify_operator_event(&self, event: OperatorEventType, version: &str) {
        if let Some(client) = &self.inner {
            client.notify_operator_event(event, version).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DiscordConfig::default();
        assert!(config.webhook_url.is_none());
        assert!(config.notify_on_success);
        assert!(config.notify_on_failure);
        assert!(config.notify_on_started);
        assert!(config.notify_on_retry);
        assert!(!config.notify_on_health);
        assert!(config.notify_on_checksum);
        assert!(config.notify_on_release);
    }

    #[test]
    fn test_should_notify_started() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::Started));

        let mut config = DiscordConfig::default();
        config.notify_on_started = false;
        assert!(!config.should_notify(&MigrationEventType::Started));
    }

    #[test]
    fn test_should_notify_succeeded() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::Succeeded));

        let mut config = DiscordConfig::default();
        config.notify_on_success = false;
        assert!(!config.should_notify(&MigrationEventType::Succeeded));
    }

    #[test]
    fn test_should_notify_failed() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::Failed));

        let mut config = DiscordConfig::default();
        config.notify_on_failure = false;
        assert!(!config.should_notify(&MigrationEventType::Failed));
    }

    #[test]
    fn test_should_notify_auto_retried() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::AutoRetried));
    }

    #[test]
    fn test_should_notify_health_events_off_by_default() {
        let config = DiscordConfig::default();
        assert!(!config.should_notify(&MigrationEventType::HealthCheckPassed));
        assert!(!config.should_notify(&MigrationEventType::HealthCheckFailed));
    }

    #[test]
    fn test_should_notify_health_events_when_enabled() {
        let mut config = DiscordConfig::default();
        config.notify_on_health = true;
        assert!(config.should_notify(&MigrationEventType::HealthCheckPassed));
        assert!(config.should_notify(&MigrationEventType::HealthCheckFailed));
    }

    #[test]
    fn test_should_notify_checksum_events() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::ChecksumMismatch));
        assert!(config.should_notify(&MigrationEventType::ChecksumReconciled));
        assert!(config.should_notify(&MigrationEventType::PreFlightFailed));
    }

    #[test]
    fn test_should_notify_release_detected() {
        let config = DiscordConfig::default();
        assert!(config.should_notify(&MigrationEventType::ReleaseDetected));
    }

    #[test]
    fn test_build_failure_mentions_none() {
        let config = DiscordConfig::default();
        assert!(config.build_failure_mentions().is_none());
    }

    #[test]
    fn test_build_failure_mentions_role_only() {
        let mut config = DiscordConfig::default();
        config.failure_mention_role = Some("12345".to_string());
        let mentions = config.build_failure_mentions().unwrap();
        assert_eq!(mentions, "<@&12345>");
    }

    #[test]
    fn test_build_failure_mentions_users_only() {
        let mut config = DiscordConfig::default();
        config.failure_mention_users = vec!["user1".to_string(), "user2".to_string()];
        let mentions = config.build_failure_mentions().unwrap();
        assert!(mentions.contains("<@user1>"));
        assert!(mentions.contains("<@user2>"));
    }

    #[test]
    fn test_build_failure_mentions_role_and_users() {
        let mut config = DiscordConfig::default();
        config.failure_mention_role = Some("99999".to_string());
        config.failure_mention_users = vec!["user1".to_string()];
        let mentions = config.build_failure_mentions().unwrap();
        assert!(mentions.contains("<@&99999>"));
        assert!(mentions.contains("<@user1>"));
    }

    #[test]
    fn test_discord_config_serde_roundtrip() {
        let config = DiscordConfig {
            webhook_url: Some("https://discord.com/api/webhooks/123/abc".to_string()),
            cluster_name: "test-cluster".to_string(),
            environment: "staging".to_string(),
            ..Default::default()
        };
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: DiscordConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.cluster_name, "test-cluster");
        assert_eq!(deserialized.environment, "staging");
    }

    #[test]
    fn test_optional_discord_not_configured() {
        let config = DiscordConfig::default();
        let client = OptionalDiscordClient::from_config(config, Duration::from_secs(10));
        assert!(!client.is_configured());
    }

    #[test]
    fn test_optional_discord_empty_url() {
        let mut config = DiscordConfig::default();
        config.webhook_url = Some("".to_string());
        let client = OptionalDiscordClient::from_config(config, Duration::from_secs(10));
        assert!(!client.is_configured());
    }
}
