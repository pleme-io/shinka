//! Migration job watcher
//!
//! Provides both blocking and non-blocking methods to monitor Kubernetes Jobs.
//! - `check_job_status`: Non-blocking, single check - ideal for reconciler pattern
//! - `wait_for_job_completion`: Blocking watch - for legacy compatibility
//!
//! Enhanced to extract detailed error information from pod logs.

use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams, LogParams},
    runtime::watcher::{watcher, Config as WatcherConfig, Event},
    Client,
};
use std::time::{Duration, Instant};

use crate::{Error, Result};
use tracing::instrument;

/// Result of a migration job
#[derive(Debug, Clone)]
pub struct JobResult {
    /// Whether the job succeeded
    pub success: bool,
    /// Duration of the job
    pub duration: Duration,
    /// Error message if failed (from Job condition - generic)
    pub error: Option<String>,
    /// Detailed error from pod logs (actual migration error)
    pub pod_logs_error: Option<String>,
    /// Full pod logs (last N lines) for debugging
    pub pod_logs_tail: Option<String>,
}

/// Status of a job check (non-blocking)
#[derive(Debug, Clone)]
pub enum JobStatus {
    /// Job is still running
    Running,
    /// Job completed (success or failure)
    Completed(JobResult),
    /// Job not found
    NotFound,
}

/// Check job status without blocking (non-blocking, single check)
///
/// This is the preferred method for reconcilers - check once and requeue if still running.
/// Returns immediately with the current job status.
#[instrument(skip(client), fields(namespace = %namespace, job = %job_name))]
pub async fn check_job_status(
    client: Client,
    namespace: &str,
    job_name: &str,
    job_duration: Duration,
) -> Result<JobStatus> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

    match jobs.get(job_name).await {
        Ok(job) => {
            if let Some(result) = check_job_completion(&job) {
                // Job completed - fetch logs and build result
                let job_result =
                    build_job_result(client.clone(), namespace, job_name, result, job_duration)
                        .await?;
                Ok(JobStatus::Completed(job_result))
            } else {
                // Job still running
                tracing::trace!(
                    namespace = %namespace,
                    job = %job_name,
                    "Job still running"
                );
                Ok(JobStatus::Running)
            }
        }
        Err(kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })) => {
            tracing::warn!(
                namespace = %namespace,
                job = %job_name,
                "Job not found"
            );
            Ok(JobStatus::NotFound)
        }
        Err(e) => Err(Error::KubeApi(e)),
    }
}

/// Wait for a migration job to complete (blocking)
///
/// Watches the job until it succeeds, fails, or times out.
/// On failure, fetches pod logs to extract the actual error message.
#[instrument(skip(client), fields(namespace = %namespace, job = %job_name, timeout_secs = %timeout.as_secs()))]
pub async fn wait_for_job_completion(
    client: Client,
    namespace: &str,
    job_name: &str,
    timeout: Duration,
) -> Result<JobResult> {
    let start = Instant::now();
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

    tracing::info!(
        namespace = %namespace,
        job = %job_name,
        timeout_secs = %timeout.as_secs(),
        "Waiting for migration job to complete"
    );

    // First check if job already exists and is complete
    if let Ok(job) = jobs.get(job_name).await {
        if let Some(result) = check_job_completion(&job) {
            return build_job_result(client.clone(), namespace, job_name, result, start.elapsed())
                .await;
        }
    }

    // Watch for job updates
    let watcher_config = WatcherConfig::default().fields(&format!("metadata.name={}", job_name));

    let mut stream = watcher(jobs.clone(), watcher_config).boxed();

    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let remaining = deadline - Instant::now();

        tokio::select! {
            event = stream.next() => {
                match event {
                    Some(Ok(Event::Apply(job))) | Some(Ok(Event::InitApply(job))) => {
                        if let Some(result) = check_job_completion(&job) {
                            return build_job_result(client.clone(), namespace, job_name, result, start.elapsed()).await;
                        }
                    }
                    Some(Ok(Event::Delete(_))) => {
                        return Err(Error::MigrationFailed {
                            name: job_name.to_string(),
                            reason: "Job was deleted".to_string(),
                        });
                    }
                    Some(Ok(Event::Init)) | Some(Ok(Event::InitDone)) => {
                        // Initial list events, continue watching
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "Watch error, continuing");
                    }
                    None => {
                        // Stream ended, check job one more time
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(remaining) => {
                break;
            }
        }
    }

    // Final check
    match jobs.get(job_name).await {
        Ok(job) => {
            if let Some(result) = check_job_completion(&job) {
                build_job_result(client, namespace, job_name, result, start.elapsed()).await
            } else {
                Err(Error::MigrationTimeout {
                    name: job_name.to_string(),
                    duration: timeout,
                })
            }
        }
        Err(e) => Err(Error::KubeApi(e)),
    }
}

/// Build a JobResult, fetching pod logs on failure for detailed error info
async fn build_job_result(
    client: Client,
    namespace: &str,
    job_name: &str,
    status_result: (bool, Option<String>),
    duration: Duration,
) -> Result<JobResult> {
    let (success, error) = status_result;

    if success {
        // Success - no need for logs
        return Ok(JobResult {
            success: true,
            duration,
            error: None,
            pod_logs_error: None,
            pod_logs_tail: None,
        });
    }

    // Failure - fetch pod logs for detailed error
    let (pod_logs_error, pod_logs_tail) =
        match fetch_pod_logs_for_job(client, namespace, job_name).await {
            Ok(logs) => {
                let extracted_error = extract_error_from_logs(&logs);
                let tail = get_logs_tail(&logs, 50); // Last 50 lines
                (extracted_error, Some(tail))
            }
            Err(e) => {
                tracing::warn!(
                    namespace = %namespace,
                    job = %job_name,
                    error = %e,
                    "Failed to fetch pod logs for error details"
                );
                (None, None)
            }
        };

    Ok(JobResult {
        success: false,
        duration,
        error,
        pod_logs_error,
        pod_logs_tail,
    })
}

/// Check if a job has completed
/// Returns Some((success, error_message)) if complete, None if still running
fn check_job_completion(job: &Job) -> Option<(bool, Option<String>)> {
    let status = job.status.as_ref()?;

    // Check for success
    if let Some(succeeded) = status.succeeded {
        if succeeded > 0 {
            return Some((true, None));
        }
    }

    // Check for failure
    if let Some(failed) = status.failed {
        if failed > 0 {
            let error = status
                .conditions
                .as_ref()
                .and_then(|conditions| {
                    conditions.iter().find(|c| c.type_ == "Failed").map(|c| {
                        c.message
                            .clone()
                            .unwrap_or_else(|| "Job failed".to_string())
                    })
                })
                .or_else(|| Some("Job failed".to_string()));
            return Some((false, error));
        }
    }

    // Still running
    None
}

/// Delete a completed migration job
#[allow(dead_code)]
pub async fn delete_job(client: Client, namespace: &str, job_name: &str) -> Result<()> {
    let jobs: Api<Job> = Api::namespaced(client, namespace);

    tracing::debug!(
        namespace = %namespace,
        job = %job_name,
        "Deleting migration job"
    );

    // Delete with propagation policy to delete pods
    jobs.delete(job_name, &Default::default())
        .await
        .map_err(|e| {
            if is_not_found(&e) {
                // Job already deleted, that's fine
                tracing::debug!(job = %job_name, "Job already deleted");
                return Error::Internal("Job already deleted".to_string());
            }
            Error::KubeApi(e)
        })?;

    Ok(())
}

/// Check if a kube error is a NotFound error
#[allow(dead_code)]
fn is_not_found(err: &kube::Error) -> bool {
    matches!(
        err,
        kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })
    )
}

/// Fetch pod logs for a migration job
///
/// Migration jobs typically have one pod - fetches logs from the first pod found.
async fn fetch_pod_logs_for_job(
    client: Client,
    namespace: &str,
    job_name: &str,
) -> Result<String> {
    let pods: Api<Pod> = Api::namespaced(client, namespace);

    // List pods for this job
    let label_selector = format!("job-name={}", job_name);
    let pod_list = pods
        .list(&ListParams::default().labels(&label_selector))
        .await
        .map_err(Error::KubeApi)?;

    // Get logs from the first pod (migration jobs only have one pod)
    if let Some(pod) = pod_list.items.first() {
        let pod_name = pod.metadata.name.as_ref().ok_or_else(|| {
            Error::Internal("Pod has no name".to_string())
        })?;

        // Request all logs (not just tail) to get full error context
        let log_params = LogParams {
            previous: false,
            ..Default::default()
        };

        let logs = pods
            .logs(pod_name, &log_params)
            .await
            .map_err(Error::KubeApi)?;

        Ok(logs)
    } else {
        Err(Error::Internal(format!(
            "No pods found for job {}",
            job_name
        )))
    }
}

/// Extract meaningful error messages from pod logs
///
/// Looks for common error patterns in migration output:
/// - Lines containing ERROR, Error, error, FATAL, fatal, panic
/// - PostgreSQL error messages (ERROR:, FATAL:)
/// - SQLx errors
/// - Stack traces
fn extract_error_from_logs(logs: &str) -> Option<String> {
    let mut error_lines = Vec::new();
    let mut capture_next_lines = 0;

    for line in logs.lines() {
        let line_lower = line.to_lowercase();

        // Check for error indicators
        let is_error_line = line_lower.contains("error")
            || line_lower.contains("fatal")
            || line_lower.contains("panic")
            || line_lower.contains("failed")
            || line.contains("ERROR:")
            || line.contains("FATAL:")
            || line.contains("syntax error")
            || line.contains("invalid input")
            || line.contains("does not exist")
            || line.contains("already exists")
            || line.contains("permission denied")
            || line.contains("connection refused")
            || line.contains("timeout");

        if is_error_line {
            error_lines.push(line.to_string());
            // Capture a few lines after an error for context
            capture_next_lines = 3;
        } else if capture_next_lines > 0 {
            error_lines.push(line.to_string());
            capture_next_lines -= 1;
        }
    }

    if error_lines.is_empty() {
        // If no explicit errors found, return the last few lines as they might contain the issue
        let all_lines: Vec<&str> = logs.lines().collect();
        if all_lines.len() > 5 {
            return Some(all_lines[all_lines.len() - 5..].join("\n"));
        } else if !all_lines.is_empty() {
            return Some(all_lines.join("\n"));
        }
        return None;
    }

    // Limit to most relevant errors (first 10 lines)
    let result: Vec<String> = error_lines.into_iter().take(10).collect();
    Some(result.join("\n"))
}

/// Get the last N lines of logs
fn get_logs_tail(logs: &str, n: usize) -> String {
    let lines: Vec<&str> = logs.lines().collect();
    let start = if lines.len() > n { lines.len() - n } else { 0 };
    lines[start..].join("\n")
}
