//! Finalizers for DatabaseMigration resources
//!
//! Handles cleanup when a DatabaseMigration is deleted.

use k8s_openapi::api::batch::v1::Job;
use kube::{
    api::{Api, DeleteParams, ListParams},
    Client,
};

use crate::{Error, Result};

/// Finalizer name for Shinka-managed resources
pub const FINALIZER: &str = "shinka.pleme.io/finalizer";

/// Cleanup resources when a DatabaseMigration is deleted
///
/// Deletes any migration jobs created for this DatabaseMigration.
pub async fn cleanup(client: Client, namespace: &str, name: &str) -> Result<()> {
    tracing::info!(
        namespace = %namespace,
        migration = %name,
        "Cleaning up resources for deleted DatabaseMigration"
    );

    let jobs: Api<Job> = Api::namespaced(client, namespace);

    // Find all jobs with our label
    let lp = ListParams::default().labels(&format!("shinka.pleme.io/migration={}", name));

    let job_list = jobs.list(&lp).await.map_err(Error::KubeApi)?;

    // Delete each job
    for job in job_list.items {
        if let Some(job_name) = job.metadata.name {
            tracing::debug!(
                namespace = %namespace,
                job = %job_name,
                "Deleting migration job"
            );

            // Ignore errors (job may already be deleted)
            if let Err(e) = jobs.delete(&job_name, &DeleteParams::default()).await {
                tracing::warn!(
                    namespace = %namespace,
                    job = %job_name,
                    error = %e,
                    "Failed to delete migration job"
                );
            }
        }
    }

    tracing::info!(
        namespace = %namespace,
        migration = %name,
        "Cleanup complete"
    );

    Ok(())
}
