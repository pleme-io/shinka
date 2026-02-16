//! Circuit Breaker pattern implementation
//!
//! Provides protection against cascading failures when external services
//! (Discord, Release Tracker, etc.) are unavailable.
//!
//! ## States
//!
//! - **Closed**: Normal operation, requests are allowed
//! - **Open**: Circuit is tripped, requests are rejected immediately
//! - **HalfOpen**: Testing if service has recovered
//!
//! ## Configuration
//!
//! - `failure_threshold`: Number of failures before opening circuit (default: 5)
//! - `success_threshold`: Number of successes to close circuit (default: 2)
//! - `timeout`: Time to wait before transitioning from Open to HalfOpen (default: 30s)

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests allowed
    Closed,
    /// Circuit tripped - requests rejected
    Open,
    /// Testing recovery - limited requests allowed
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Name for logging/metrics
    pub name: String,
    /// Number of consecutive failures to trip the circuit
    pub failure_threshold: u32,
    /// Number of consecutive successes to close the circuit
    pub success_threshold: u32,
    /// Time to wait before testing recovery
    pub open_timeout: Duration,
    /// Maximum requests allowed in half-open state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            failure_threshold: 5,
            success_threshold: 2,
            open_timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    pub fn with_open_timeout(mut self, timeout: Duration) -> Self {
        self.open_timeout = timeout;
        self
    }
}

/// Circuit breaker state (thread-safe)
struct CircuitBreakerState {
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    last_failure_time: Option<Instant>,
    half_open_requests: u32,
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_failure_time: None,
            half_open_requests: 0,
        }
    }
}

/// Circuit breaker for protecting external service calls
#[derive(Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
    // Metrics
    requests_total: Arc<AtomicU64>,
    rejections_total: Arc<AtomicU64>,
    failures_total: Arc<AtomicU64>,
    state_transitions: Arc<AtomicU32>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(CircuitBreakerState::default())),
            requests_total: Arc::new(AtomicU64::new(0)),
            rejections_total: Arc::new(AtomicU64::new(0)),
            failures_total: Arc::new(AtomicU64::new(0)),
            state_transitions: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Get the current state
    pub async fn state(&self) -> CircuitState {
        let state = self.state.read().await;
        self.evaluate_state(&state)
    }

    /// Check if a request should be allowed
    pub async fn should_allow(&self) -> bool {
        self.requests_total.fetch_add(1, Ordering::Relaxed);

        let mut state = self.state.write().await;
        let current_state = self.evaluate_state(&state);

        match current_state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                self.rejections_total.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    circuit = %self.config.name,
                    state = %current_state,
                    "Circuit breaker rejected request"
                );
                false
            }
            CircuitState::HalfOpen => {
                if state.half_open_requests < self.config.half_open_max_requests {
                    state.half_open_requests += 1;
                    true
                } else {
                    self.rejections_total.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        circuit = %self.config.name,
                        state = %current_state,
                        "Circuit breaker rejected request (half-open limit)"
                    );
                    false
                }
            }
        }
    }

    /// Record a successful request
    pub async fn record_success(&self) {
        let mut state = self.state.write().await;
        state.consecutive_failures = 0;
        state.consecutive_successes += 1;

        let current_state = self.evaluate_state(&state);

        if current_state == CircuitState::HalfOpen
            && state.consecutive_successes >= self.config.success_threshold
        {
            tracing::info!(
                circuit = %self.config.name,
                "Circuit breaker closing after successful recovery"
            );
            state.state = CircuitState::Closed;
            state.consecutive_successes = 0;
            state.half_open_requests = 0;
            self.state_transitions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a failed request
    pub async fn record_failure(&self) {
        self.failures_total.fetch_add(1, Ordering::Relaxed);

        let mut state = self.state.write().await;
        state.consecutive_successes = 0;
        state.consecutive_failures += 1;
        state.last_failure_time = Some(Instant::now());

        let current_state = self.evaluate_state(&state);

        match current_state {
            CircuitState::Closed => {
                if state.consecutive_failures >= self.config.failure_threshold {
                    tracing::warn!(
                        circuit = %self.config.name,
                        failures = state.consecutive_failures,
                        "Circuit breaker opening due to failures"
                    );
                    state.state = CircuitState::Open;
                    self.state_transitions.fetch_add(1, Ordering::Relaxed);
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open trips back to open
                tracing::warn!(
                    circuit = %self.config.name,
                    "Circuit breaker reopening after half-open failure"
                );
                state.state = CircuitState::Open;
                state.half_open_requests = 0;
                self.state_transitions.fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Evaluate the current state, potentially transitioning from Open to HalfOpen
    fn evaluate_state(&self, state: &CircuitBreakerState) -> CircuitState {
        if state.state == CircuitState::Open {
            if let Some(last_failure) = state.last_failure_time {
                if last_failure.elapsed() >= self.config.open_timeout {
                    return CircuitState::HalfOpen;
                }
            }
        }
        state.state
    }

    /// Execute a function with circuit breaker protection
    pub async fn execute<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        if !self.should_allow().await {
            return Err(CircuitBreakerError::Open);
        }

        match f.await {
            Ok(result) => {
                self.record_success().await;
                Ok(result)
            }
            Err(e) => {
                self.record_failure().await;
                Err(CircuitBreakerError::Inner(e))
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            name: self.config.name.clone(),
            requests_total: self.requests_total.load(Ordering::Relaxed),
            rejections_total: self.rejections_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            state_transitions: self.state_transitions.load(Ordering::Relaxed),
        }
    }

    /// Force the circuit open (for testing or manual intervention)
    pub async fn force_open(&self) {
        let mut state = self.state.write().await;
        state.state = CircuitState::Open;
        state.last_failure_time = Some(Instant::now());
        tracing::warn!(circuit = %self.config.name, "Circuit breaker forced open");
    }

    /// Force the circuit closed (for testing or manual intervention)
    pub async fn force_closed(&self) {
        let mut state = self.state.write().await;
        state.state = CircuitState::Closed;
        state.consecutive_failures = 0;
        state.consecutive_successes = 0;
        state.half_open_requests = 0;
        tracing::info!(circuit = %self.config.name, "Circuit breaker forced closed");
    }
}

/// Error type for circuit breaker protected operations
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, request was rejected
    Open,
    /// Inner operation failed
    Inner(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::Open => write!(f, "circuit breaker is open"),
            CircuitBreakerError::Inner(e) => write!(f, "{}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::Open => None,
            CircuitBreakerError::Inner(e) => Some(e),
        }
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub name: String,
    pub requests_total: u64,
    pub rejections_total: u64,
    pub failures_total: u64,
    pub state_transitions: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_by_default() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::new("test"));
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.should_allow().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig::new("test").with_failure_threshold(3);
        let cb = CircuitBreaker::new(config);

        // Record failures
        for _ in 0..3 {
            cb.record_failure().await;
        }

        assert_eq!(cb.state().await, CircuitState::Open);
        assert!(!cb.should_allow().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_closes_after_successes() {
        let config = CircuitBreakerConfig::new("test")
            .with_failure_threshold(2)
            .with_success_threshold(2)
            .with_open_timeout(Duration::from_millis(10));

        let cb = CircuitBreaker::new(config);

        // Trip the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        // Record successes
        cb.record_success().await;
        cb.record_success().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_execute() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::new("test"));

        // Successful execution
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.execute(async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);

        // Failed execution
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.execute(async { Err::<i32, _>("error") }).await;
        assert!(matches!(result, Err(CircuitBreakerError::Inner("error"))));
    }
}
