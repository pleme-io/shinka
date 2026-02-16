//! REST API handlers for Shinka
//!
//! Provides RESTful endpoints for migration management, database health,
//! and queue operations. All handlers delegate to the core MigrationApiService.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::instrument;

use super::service::MigrationApiService;
use super::types::{
    ApiError, AwaitQuery, AwaitResponse, ClusterHealth, DatabaseInfo, DatabaseReadiness,
    MigrationFilter, MigrationHistoryEntry, MigrationList, MigrationResource, QueueItem,
    QueueStatus,
};

/// REST API state
pub type ApiState = Arc<MigrationApiService>;

/// Create the REST API router
pub fn create_router(service: Arc<MigrationApiService>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Migration endpoints
        .route("/api/v1/migrations", get(list_migrations))
        .route(
            "/api/v1/namespaces/{namespace}/migrations/{name}",
            get(get_migration),
        )
        .route(
            "/api/v1/namespaces/{namespace}/migrations/{name}/history",
            get(get_migration_history),
        )
        .route(
            "/api/v1/namespaces/{namespace}/migrations/{name}/retry",
            post(retry_migration),
        )
        .route(
            "/api/v1/namespaces/{namespace}/migrations/{name}/cancel",
            post(cancel_migration),
        )
        .route(
            "/api/v1/namespaces/{namespace}/migrations/{name}/await",
            get(await_migration),
        )
        .route("/api/v1/migrations/watch", get(watch_migrations))
        // Database endpoints
        .route("/api/v1/databases", get(list_databases))
        .route(
            "/api/v1/namespaces/{namespace}/clusters/{cluster}/ready",
            get(check_database_ready),
        )
        .route(
            "/api/v1/namespaces/{namespace}/clusters/{cluster}/health",
            get(get_cluster_health),
        )
        // Queue endpoints
        .route("/api/v1/queue/status", get(get_queue_status))
        .route("/api/v1/queue/items", get(list_queue_items))
        .route("/api/v1/queue/pause", post(pause_queue))
        .route("/api/v1/queue/resume", post(resume_queue))
        // Add state and middleware
        .layer(cors)
        .with_state(service)
}

// =============================================================================
// Response Types
// =============================================================================

/// Standard API response wrapper
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub data: T,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ApiError,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = Json(ErrorResponse { error: self });
        (status, body).into_response()
    }
}

// =============================================================================
// Query Parameters
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ListMigrationsQuery {
    pub namespace: Option<String>,
    pub phase: Option<String>,
    pub limit: Option<i32>,
    #[serde(rename = "continue")]
    pub continue_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListQueueItemsQuery {
    pub status: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseReadyQuery {
    pub database: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WatchQuery {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub phase: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RetryRequest {
    #[serde(default)]
    pub reset_retry_count: bool,
}

#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PauseRequest {
    pub reason: Option<String>,
}

// =============================================================================
// Migration Handlers
// =============================================================================

/// GET /api/v1/migrations
#[instrument(skip(state))]
async fn list_migrations(
    State(state): State<ApiState>,
    Query(query): Query<ListMigrationsQuery>,
) -> Result<Json<ApiResponse<MigrationList>>, ApiError> {
    let filter = MigrationFilter {
        namespace: query.namespace,
        phases: query.phase.map(|p| vec![p]).unwrap_or_default(),
        limit: query.limit,
        continue_token: query.continue_token,
        ..Default::default()
    };

    let result = state.list_migrations(filter).await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/namespaces/:namespace/migrations/:name
#[instrument(skip(state))]
async fn get_migration(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<MigrationResource>>, ApiError> {
    let result = state.get_migration(&namespace, &name).await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/namespaces/:namespace/migrations/:name/history
#[instrument(skip(state))]
async fn get_migration_history(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<ApiResponse<Vec<MigrationHistoryEntry>>>, ApiError> {
    let result = state
        .get_migration_history(&namespace, &name, query.limit)
        .await?;
    Ok(Json(ApiResponse::new(result)))
}

/// POST /api/v1/namespaces/:namespace/migrations/:name/retry
#[instrument(skip(state))]
async fn retry_migration(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    Json(body): Json<RetryRequest>,
) -> Result<Json<ApiResponse<MigrationResource>>, ApiError> {
    let result = state
        .retry_migration(&namespace, &name, body.reset_retry_count)
        .await?;
    Ok(Json(ApiResponse::new(result)))
}

/// POST /api/v1/namespaces/:namespace/migrations/:name/cancel
#[instrument(skip(state))]
async fn cancel_migration(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    Json(body): Json<CancelRequest>,
) -> Result<Json<ApiResponse<MigrationResource>>, ApiError> {
    let result = state
        .cancel_migration(&namespace, &name, body.reason)
        .await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/namespaces/:namespace/migrations/:name/await?tag=...&timeout=...
///
/// Blocks until the migration reaches Ready or Failed for the expected tag,
/// or until the timeout expires. Max timeout: 900s.
#[instrument(skip(state))]
async fn await_migration(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<AwaitQuery>,
) -> Result<Json<ApiResponse<AwaitResponse>>, ApiError> {
    if query.tag.is_empty() {
        return Err(ApiError::invalid_argument("'tag' query parameter is required"));
    }

    let result = state
        .await_migration(&namespace, &name, &query.tag, query.timeout)
        .await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/migrations/watch - Server-Sent Events for migration updates
#[instrument(skip(state))]
async fn watch_migrations(
    State(state): State<ApiState>,
    Query(query): Query<WatchQuery>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let phases = query.phase.map(|p| vec![p]).unwrap_or_default();
    let stream = state
        .watch_migrations(query.namespace, query.name, phases)
        .await;

    let event_stream = stream.map(|event| {
        let data = serde_json::to_string(&event).unwrap_or_default();
        Ok(Event::default()
            .event(event.event_type.to_string())
            .data(data))
    });

    Sse::new(event_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(30))
            .text("ping"),
    )
}

// =============================================================================
// Database Handlers
// =============================================================================

/// GET /api/v1/databases
#[instrument(skip(state))]
async fn list_databases(
    State(state): State<ApiState>,
    Query(query): Query<ListMigrationsQuery>,
) -> Result<Json<ApiResponse<Vec<DatabaseInfo>>>, ApiError> {
    let result = state.list_databases(query.namespace).await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/namespaces/:namespace/clusters/:cluster/ready
#[instrument(skip(state))]
async fn check_database_ready(
    State(state): State<ApiState>,
    Path((namespace, cluster)): Path<(String, String)>,
    Query(query): Query<DatabaseReadyQuery>,
) -> Result<Json<ApiResponse<DatabaseReadiness>>, ApiError> {
    let result = state
        .check_database_ready(&namespace, &cluster, query.database)
        .await?;
    Ok(Json(ApiResponse::new(result)))
}

/// GET /api/v1/namespaces/:namespace/clusters/:cluster/health
#[instrument(skip(state))]
async fn get_cluster_health(
    State(state): State<ApiState>,
    Path((namespace, cluster)): Path<(String, String)>,
) -> Result<Json<ApiResponse<ClusterHealth>>, ApiError> {
    let result = state.get_cluster_health(&namespace, &cluster).await?;
    Ok(Json(ApiResponse::new(result)))
}

// =============================================================================
// Queue Handlers
// =============================================================================

/// GET /api/v1/queue/status
#[instrument(skip(state))]
async fn get_queue_status(State(state): State<ApiState>) -> Json<ApiResponse<QueueStatus>> {
    let result = state.get_queue_status().await;
    Json(ApiResponse::new(result))
}

/// GET /api/v1/queue/items
#[instrument(skip(state))]
async fn list_queue_items(
    State(state): State<ApiState>,
    Query(query): Query<ListQueueItemsQuery>,
) -> Result<Json<ApiResponse<Vec<QueueItem>>>, ApiError> {
    let result = state.list_queue_items(query.status, query.limit).await?;
    Ok(Json(ApiResponse::new(result)))
}

/// POST /api/v1/queue/pause
#[instrument(skip(state))]
async fn pause_queue(
    State(state): State<ApiState>,
    Json(body): Json<PauseRequest>,
) -> Json<ApiResponse<QueueStatus>> {
    let result = state.pause_queue(body.reason).await;
    Json(ApiResponse::new(result))
}

/// POST /api/v1/queue/resume
#[instrument(skip(state))]
async fn resume_queue(State(state): State<ApiState>) -> Json<ApiResponse<QueueStatus>> {
    let result = state.resume_queue().await;
    Json(ApiResponse::new(result))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        let not_found = ApiError::not_found("Migration", "test");
        assert_eq!(not_found.status, 404);

        let invalid = ApiError::invalid_argument("bad input");
        assert_eq!(invalid.status, 400);

        let internal = ApiError::internal("something broke");
        assert_eq!(internal.status, 500);
    }
}
