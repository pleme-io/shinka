//! Leader election for Shinka operator
//!
//! Implements leader election using Kubernetes Lease resources to ensure
//! only one instance of the operator is actively reconciling at a time.
//! This provides high availability - if the leader crashes, another replica
//! will take over.
//!
//! ## How it works
//!
//! 1. The operator tries to acquire a Lease in its namespace
//! 2. If successful, it becomes the leader and starts reconciling
//! 3. The leader periodically renews the lease
//! 4. If renewal fails, another replica can acquire the lease
//!
//! ## Configuration
//!
//! - `LEADER_ELECTION_ENABLED`: Set to "true" to enable (default: false for single-replica)
//! - `LEADER_ELECTION_LEASE_NAME`: Name of the Lease resource (default: "shinka-leader")
//! - `LEADER_ELECTION_LEASE_NAMESPACE`: Namespace for the Lease (default: operator's namespace)
//! - `LEADER_ELECTION_LEASE_DURATION`: How long the lease is valid (default: 15s)
//! - `LEADER_ELECTION_RENEW_DEADLINE`: How long to try renewing before giving up (default: 10s)
//! - `LEADER_ELECTION_RETRY_PERIOD`: How often to retry acquiring the lease (default: 2s)

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::{
    api::{Api, Patch, PatchParams, PostParams},
    Client,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::{Error, Result};

/// Configuration for leader election
#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    /// Whether leader election is enabled
    pub enabled: bool,
    /// Name of the Lease resource
    pub lease_name: String,
    /// Namespace for the Lease
    pub lease_namespace: String,
    /// Identity of this candidate (usually pod name)
    pub identity: String,
    /// How long the lease is valid
    pub lease_duration: Duration,
    /// How long to try renewing before giving up
    pub renew_deadline: Duration,
    /// How often to retry acquiring the lease
    pub retry_period: Duration,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_name: "shinka-leader".to_string(),
            lease_namespace: "shinka-system".to_string(),
            identity: std::env::var("POD_NAME").unwrap_or_else(|_| {
                format!(
                    "shinka-{}",
                    uuid::Uuid::new_v4()
                        .to_string()
                        .split('-')
                        .next()
                        .unwrap_or("unknown")
                )
            }),
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        }
    }
}

impl LeaderElectionConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("LEADER_ELECTION_ENABLED") {
            config.enabled = val == "true" || val == "1";
        }

        if let Ok(name) = std::env::var("LEADER_ELECTION_LEASE_NAME") {
            config.lease_name = name;
        }

        if let Ok(ns) = std::env::var("LEADER_ELECTION_LEASE_NAMESPACE") {
            config.lease_namespace = ns;
        } else if let Ok(ns) = std::env::var("POD_NAMESPACE") {
            config.lease_namespace = ns;
        }

        if let Ok(identity) = std::env::var("POD_NAME") {
            config.identity = identity;
        }

        if let Ok(duration) = std::env::var("LEADER_ELECTION_LEASE_DURATION") {
            if let Ok(secs) = duration.parse::<u64>() {
                config.lease_duration = Duration::from_secs(secs);
            }
        }

        if let Ok(deadline) = std::env::var("LEADER_ELECTION_RENEW_DEADLINE") {
            if let Ok(secs) = deadline.parse::<u64>() {
                config.renew_deadline = Duration::from_secs(secs);
            }
        }

        if let Ok(period) = std::env::var("LEADER_ELECTION_RETRY_PERIOD") {
            if let Ok(secs) = period.parse::<u64>() {
                config.retry_period = Duration::from_secs(secs);
            }
        }

        config
    }
}

/// Leader election state
pub struct LeaderElector {
    client: Client,
    config: LeaderElectionConfig,
    is_leader: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl LeaderElector {
    /// Create a new leader elector
    pub fn new(client: Client, config: LeaderElectionConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            client,
            config,
            is_leader: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Check if this instance is currently the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Get a handle to check leader status
    pub fn leader_handle(&self) -> LeaderHandle {
        LeaderHandle {
            is_leader: self.is_leader.clone(),
        }
    }

    /// Signal shutdown to stop the leader election loop
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Run the leader election loop
    ///
    /// This will block until shutdown is signaled. While running, it will:
    /// 1. Try to acquire the lease
    /// 2. If successful, periodically renew it
    /// 3. If renewal fails, mark as not leader and retry acquisition
    pub async fn run(&self) -> Result<()> {
        if !self.config.enabled {
            tracing::info!("Leader election disabled, running as single instance");
            self.is_leader.store(true, Ordering::SeqCst);
            return Ok(());
        }

        tracing::info!(
            identity = %self.config.identity,
            lease_name = %self.config.lease_name,
            lease_namespace = %self.config.lease_namespace,
            "Starting leader election"
        );

        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.config.lease_namespace);

        loop {
            // Check for shutdown
            if *self.shutdown_rx.borrow() {
                tracing::info!("Leader election shutting down");
                self.is_leader.store(false, Ordering::SeqCst);
                return Ok(());
            }

            // Try to acquire or renew the lease
            match self.try_acquire_or_renew(&leases).await {
                Ok(true) => {
                    if !self.is_leader.load(Ordering::SeqCst) {
                        tracing::info!(
                            identity = %self.config.identity,
                            "Became leader"
                        );
                        self.is_leader.store(true, Ordering::SeqCst);
                        crate::metrics::set_leader_status(true);
                    }
                }
                Ok(false) => {
                    if self.is_leader.load(Ordering::SeqCst) {
                        tracing::warn!(
                            identity = %self.config.identity,
                            "Lost leadership"
                        );
                        self.is_leader.store(false, Ordering::SeqCst);
                        crate::metrics::set_leader_status(false);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Error during leader election"
                    );
                    if self.is_leader.load(Ordering::SeqCst) {
                        self.is_leader.store(false, Ordering::SeqCst);
                        crate::metrics::set_leader_status(false);
                    }
                }
            }

            // Wait before next attempt
            let mut shutdown_rx = self.shutdown_rx.clone();
            tokio::select! {
                _ = tokio::time::sleep(self.config.retry_period) => {}
                _ = shutdown_rx.changed() => {
                    tracing::info!("Leader election received shutdown signal");
                    self.is_leader.store(false, Ordering::SeqCst);
                    return Ok(());
                }
            }
        }
    }

    /// Try to acquire or renew the lease
    async fn try_acquire_or_renew(&self, leases: &Api<Lease>) -> Result<bool> {
        let now = chrono::Utc::now();

        // Try to get the existing lease
        match leases.get(&self.config.lease_name).await {
            Ok(lease) => {
                // Check if we hold the lease
                let holder = lease.spec.as_ref().and_then(|s| s.holder_identity.as_ref());

                let renew_time = lease
                    .spec
                    .as_ref()
                    .and_then(|s| s.renew_time.as_ref())
                    .map(|t| t.0);

                let lease_duration_secs = lease
                    .spec
                    .as_ref()
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(15);

                // Check if the lease has expired
                let is_expired = renew_time
                    .map(|t| {
                        let expiry = t + chrono::Duration::seconds(lease_duration_secs as i64);
                        now > expiry
                    })
                    .unwrap_or(true);

                if holder == Some(&self.config.identity) {
                    // We hold the lease, renew it
                    self.renew_lease(leases, &lease).await
                } else if is_expired || holder.is_none() {
                    // Lease expired or no holder, try to acquire
                    self.acquire_lease(leases, Some(&lease)).await
                } else {
                    // Someone else holds the lease
                    tracing::debug!(
                        holder = ?holder,
                        "Another instance holds the lease"
                    );
                    Ok(false)
                }
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })) => {
                // Lease doesn't exist, create it
                self.create_lease(leases).await
            }
            Err(e) => Err(Error::KubeApi(e)),
        }
    }

    /// Create a new lease
    async fn create_lease(&self, leases: &Api<Lease>) -> Result<bool> {
        let now = chrono::Utc::now();
        let lease = Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(self.config.lease_name.clone()),
                namespace: Some(self.config.lease_namespace.clone()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                holder_identity: Some(self.config.identity.clone()),
                lease_duration_seconds: Some(self.config.lease_duration.as_secs() as i32),
                acquire_time: Some(MicroTime(now)),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(0),
                preferred_holder: None,
                strategy: None,
            }),
        };

        match leases.create(&PostParams::default(), &lease).await {
            Ok(_) => {
                tracing::info!(
                    identity = %self.config.identity,
                    "Created lease and became leader"
                );
                Ok(true)
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 409, .. })) => {
                // Conflict - someone else created it first
                tracing::debug!("Lease creation conflict, another instance became leader");
                Ok(false)
            }
            Err(e) => Err(Error::KubeApi(e)),
        }
    }

    /// Acquire an existing lease
    async fn acquire_lease(&self, leases: &Api<Lease>, existing: Option<&Lease>) -> Result<bool> {
        let now = chrono::Utc::now();
        let transitions = existing
            .and_then(|l| l.spec.as_ref())
            .and_then(|s| s.lease_transitions)
            .unwrap_or(0);

        let patch = serde_json::json!({
            "spec": {
                "holderIdentity": self.config.identity,
                "leaseDurationSeconds": self.config.lease_duration.as_secs(),
                "acquireTime": now.to_rfc3339(),
                "renewTime": now.to_rfc3339(),
                "leaseTransitions": transitions + 1
            }
        });

        match leases
            .patch(
                &self.config.lease_name,
                &PatchParams::default(),
                &Patch::Merge(patch),
            )
            .await
        {
            Ok(_) => {
                tracing::info!(
                    identity = %self.config.identity,
                    transitions = transitions + 1,
                    "Acquired lease"
                );
                Ok(true)
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 409, .. })) => {
                tracing::debug!("Lease acquisition conflict");
                Ok(false)
            }
            Err(e) => Err(Error::KubeApi(e)),
        }
    }

    /// Renew the lease we hold
    async fn renew_lease(&self, leases: &Api<Lease>, _existing: &Lease) -> Result<bool> {
        let now = chrono::Utc::now();
        let deadline = Instant::now() + self.config.renew_deadline;

        while Instant::now() < deadline {
            let patch = serde_json::json!({
                "spec": {
                    "renewTime": now.to_rfc3339()
                }
            });

            match leases
                .patch(
                    &self.config.lease_name,
                    &PatchParams::default(),
                    &Patch::Merge(patch),
                )
                .await
            {
                Ok(_) => {
                    tracing::trace!(
                        identity = %self.config.identity,
                        "Renewed lease"
                    );
                    return Ok(true);
                }
                Err(kube::Error::Api(kube::error::ErrorResponse { code: 409, .. })) => {
                    // Conflict - lost the lease
                    tracing::warn!("Lost lease during renewal (conflict)");
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error renewing lease, retrying");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        tracing::warn!("Failed to renew lease within deadline");
        Ok(false)
    }
}

/// Handle to check leader status without owning the elector
#[derive(Clone)]
pub struct LeaderHandle {
    is_leader: Arc<AtomicBool>,
}

impl LeaderHandle {
    /// Check if this instance is currently the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LeaderElectionConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.lease_name, "shinka-leader");
        assert_eq!(config.lease_duration, Duration::from_secs(15));
    }

    #[test]
    fn test_leader_handle() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let handle = LeaderHandle {
            is_leader: is_leader.clone(),
        };

        assert!(!handle.is_leader());

        is_leader.store(true, Ordering::SeqCst);
        assert!(handle.is_leader());
    }
}
