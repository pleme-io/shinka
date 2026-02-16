//! Health, metrics, and API HTTP server for Shinka operator
//!
//! Provides:
//! - /healthz - Liveness probe
//! - /readyz - Readiness probe
//! - /metrics - Prometheus metrics
//! - /graphql - GraphQL API endpoint (default protocol)
//! - /api/v1/* - REST API endpoints

use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use kube::Client;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::api::{self, MigrationApiService, ShinkaSchemA};
use crate::metrics;

/// Shared state for health server
#[derive(Clone)]
pub struct HealthState {
    /// Whether the operator is ready to serve
    pub ready: Arc<RwLock<bool>>,
    /// GraphQL schema (optional, set when API is enabled)
    pub graphql_schema: Option<ShinkaSchemA>,
    /// API service (optional, set when API is enabled)
    pub api_service: Option<Arc<MigrationApiService>>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            ready: Arc::new(RwLock::new(false)),
            graphql_schema: None,
            api_service: None,
        }
    }
}

impl HealthState {
    /// Create a new health state without API
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new health state with API support
    pub fn with_api(client: Client) -> Self {
        let api_service = Arc::new(MigrationApiService::new(client));
        let graphql_schema = api::create_schema(api_service.clone());

        Self {
            ready: Arc::new(RwLock::new(false)),
            graphql_schema: Some(graphql_schema),
            api_service: Some(api_service),
        }
    }

    /// Set the ready state
    pub async fn set_ready(&self, ready: bool) {
        let mut guard = self.ready.write().await;
        *guard = ready;
    }
}

/// Create the health server router with optional GraphQL/REST API
pub fn router(state: HealthState) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(liveness))
        .route("/readyz", get(readiness))
        .route("/metrics", get(prometheus_metrics));

    // Add GraphQL endpoint if API is enabled (default protocol)
    if state.graphql_schema.is_some() {
        router = router
            .route("/graphql", post(graphql_handler))
            .route("/graphql", get(graphql_playground));

        tracing::info!("GraphQL API enabled at /graphql (default protocol)");
    }

    router.with_state(state)
}

/// Create combined router with both health endpoints and REST API
pub fn router_with_rest(state: HealthState, api_service: Arc<MigrationApiService>) -> Router {
    let health_router = router(state);
    let rest_router = api::create_router(api_service);

    tracing::info!("REST API enabled at /api/v1/* (backward compatibility)");

    // Combine both routers - REST API routes will be checked first
    rest_router.merge(health_router)
}

/// Liveness probe - always returns OK if the process is alive
async fn liveness() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness probe - returns OK when the operator is ready
async fn readiness(State(state): State<HealthState>) -> impl IntoResponse {
    let ready = *state.ready.read().await;
    if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Prometheus metrics endpoint
async fn prometheus_metrics() -> impl IntoResponse {
    let body = metrics::gather();
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        body,
    )
}

/// GraphQL handler - POST /graphql
async fn graphql_handler(State(state): State<HealthState>, req: GraphQLRequest) -> GraphQLResponse {
    match &state.graphql_schema {
        Some(schema) => schema.execute(req.into_inner()).await.into(),
        None => async_graphql::Response::from_errors(vec![async_graphql::ServerError::new(
            "GraphQL not enabled",
            None,
        )])
        .into(),
    }
}

/// GraphQL Playground - GET /graphql
async fn graphql_playground() -> impl IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

/// Start the health server
pub async fn serve(addr: &str, state: HealthState) -> crate::Result<()> {
    // Use router_with_rest if API service is available, otherwise basic router
    let app = match &state.api_service {
        Some(api_service) => router_with_rest(state.clone(), api_service.clone()),
        None => router(state),
    };

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::Error::Internal(format!("failed to bind health server: {e}")))?;

    tracing::info!(addr = %addr, "Health server started");

    axum::serve(listener, app)
        .await
        .map_err(|e| crate::Error::Internal(format!("health server error: {e}")))
}
