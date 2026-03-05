# Shinka (進化)

GitOps-native database migration operator for Kubernetes. Shinka watches for deployment image changes and automatically runs database migrations using Kubernetes Jobs, with CloudNativePG health checks, checksum reconciliation, leader election, and full observability. Built in Rust with [kube-rs](https://github.com/kube-rs/kube).

## Architecture

Shinka runs as a single controller watching `DatabaseMigration` CRDs. When a watched deployment's container image changes, the operator creates a Kubernetes Job to run the migration, waits for completion, and transitions the resource through a well-defined state machine.

```
  Deployment (image change)        Shinka Operator
  ─────────────────────────   ┌──────────────────────────┐
  new image tag detected ───► │  DatabaseMigration        │
                              │  Controller               │
                              │                            │
                              │  State Machine:            │
                              │  Pending                   │
                              │    └─► CheckingHealth      │
                              │          ├─► Migrating     │
                              │          │     ├─► Ready   │
                              │          │     └─► Failed  │
                              │          └─► WaitingForDB  │
                              │                            │
                              │  HTTP Server (port 8080)   │
                              │  ├─ REST API (/api/v1/*)   │
                              │  ├─ GraphQL (/graphql)     │
                              │  ├─ Health (/healthz)      │
                              │  └─ Metrics (/metrics)     │
                              │                            │
                              │  Leader Election (Lease)   │
                              │  Admission Webhook (opt)   │
                              └──────────────────────────┘
                                         │
                                         ▼
                              Migration Job (K8s Job)
                              ├─ runs migration command
                              ├─ inherits tolerations,
                              │  nodeSelector, affinity
                              └─ reports success/failure
```

## Features

- **CRD-driven** -- `DatabaseMigration` and `MigrationRun` custom resources (`shinka.pleme.io/v1alpha1`)
- **Automatic triggers** -- Detects deployment image changes and runs migrations before new code serves traffic
- **Multi-tool support** -- SQLx, SeaORM, Refinery, Diesel (Rust); Goose, golang-migrate, Atlas, Dbmate (Go); Flyway, Liquibase (Java); or custom commands
- **Multiple sequential migrators** -- Run migrations from different tools in order (e.g., transitioning from SQLx to SeaORM)
- **CNPG health checks** -- Validates CloudNativePG cluster health before running any migration
- **Checksum reconciliation** -- Three modes: `strict` (fail on mismatch), `auto-reconcile` (fix automatically), `pre-flight` (validate all before running any)
- **Automatic retry** -- Configurable `maxRetries` with automatic retry on failure; auto-retry on new image push when in Failed state
- **Circuit breaker** -- Prevents cascading failures during database instability
- **Non-blocking reconciliation** -- Checks job status without blocking the controller loop
- **REST, GraphQL, and gRPC APIs** -- Query migration status, wait for readiness, stream events via SSE
- **Init container (shinka-wait)** -- Block application startup until migrations are confirmed complete
- **Release tag annotations** -- Fast-track deployments by annotating the expected image tag (eliminates cache/requeue delays)
- **Pod scheduling inheritance** -- Migration jobs inherit tolerations, nodeSelector, and affinity from the source deployment
- **Image override** -- Specify a container image directly for ephemeral environments where the deployment does not exist yet
- **Prometheus metrics** -- Migration duration, success/failure counts, in-flight gauges, database health, checksum events, leader election status
- **Kubernetes native events** -- Emits events on the DatabaseMigration resource for migration lifecycle transitions
- **Discord and webhook notifications** -- Configurable notifications for migration lifecycle events
- **Leader election** -- Safe multi-replica deployments with lease-based leader election
- **Credential redaction** -- Automatic redaction of sensitive data in logs
- **CLI tool (shinka-cli)** -- List, inspect, retry, watch, and fetch logs for migrations from the command line

## State Machine

```
Pending ──► CheckingHealth ──► [healthy] ──► Migrating ──► Ready
                             │
                             └── [unhealthy] ──► WaitingForDatabase (requeue 30s)

Migrating ──► [failed] ──► Retry (if retryCount < maxRetries) ──► Migrating
           │
           └── [retries exhausted] ──► Failed
```

A new image push on a deployment resets `Ready` back to `Pending`. A new image push while in `Failed` triggers an automatic retry (resets retry count).

## Requirements

- Kubernetes 1.28+
- [CloudNativePG](https://cloudnative-pg.io/) (database cluster management)
- [FluxCD](https://fluxcd.io/) (GitOps reconciliation, optional)
- Protobuf compiler (`protoc`) only if building with the `grpc` feature

## Installation

### With Kustomize

```bash
kubectl apply -k deploy/
```

### With FluxCD

```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
spec:
  sourceRef:
    kind: GitRepository
    name: shinka
  path: ./deploy
  prune: true
  interval: 10m
```

### Docker Image

```bash
docker pull ghcr.io/pleme-io/shinka:latest
```

## Usage

### Quick Start

#### 1. Create a DatabaseMigration

```yaml
apiVersion: shinka.pleme.io/v1alpha1
kind: DatabaseMigration
metadata:
  name: myapp-backend
  namespace: production
spec:
  database:
    cnpgClusterRef:
      name: myapp-postgres
      database: myapp
  migrator:
    type: sqlx
    deploymentRef:
      name: myapp-backend
    command: ["./backend", "--migrate"]
    envFrom:
      - secretRef:
          name: myapp-db-credentials
  safety:
    requireHealthyCluster: true
    maxRetries: 3
    checksumMode: auto-reconcile
  timeouts:
    migration: "5m"
```

#### 2. Block application startup with shinka-wait

Add an init container so pods wait for migrations to complete:

```yaml
initContainers:
  - name: wait-for-migration
    image: ghcr.io/pleme-io/shinka:latest
    env:
      - name: RUN_MODE
        value: "wait"
      - name: SHINKA_URL
        value: "http://shinka.shinka-system.svc.cluster.local:8080"
      - name: MIGRATION_NAME
        value: "myapp-backend"
      - name: MIGRATION_NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
      - name: TIMEOUT_SECONDS
        value: "300"
```

#### 3. Multiple Migrators

Run migrations from multiple tools in sequence:

```yaml
spec:
  migrators:
    - name: sqlx-legacy
      type: sqlx
      deploymentRef:
        name: backend
      command: ["./backend", "--migrate-sqlx"]
    - name: seaorm-new
      type: seaorm
      deploymentRef:
        name: backend
      command: ["./backend", "--migrate-seaorm"]
  safety:
    continueOnFailure: false
```

## CRD Reference

### DatabaseMigration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `spec.database.cnpgClusterRef.name` | string | required | CNPG Cluster name |
| `spec.database.cnpgClusterRef.database` | string | -- | Database name within the cluster |
| `spec.migrator.type` | enum | `sqlx` | Migration tool: `sqlx`, `seaorm`, `refinery`, `diesel`, `goose`, `golang-migrate`, `atlas`, `dbmate`, `flyway`, `liquibase`, `custom` |
| `spec.migrator.deploymentRef.name` | string | required | Deployment to watch for image changes |
| `spec.migrator.deploymentRef.containerName` | string | first | Container to extract image from |
| `spec.migrator.imageOverride` | string | -- | Use this image instead of resolving from deployment |
| `spec.migrator.command` | string[] | tool default | Migration command |
| `spec.migrator.args` | string[] | -- | Additional arguments |
| `spec.migrator.envFrom` | EnvFromSource[] | -- | Environment from ConfigMaps/Secrets |
| `spec.migrator.env` | map | -- | Additional environment variables |
| `spec.migrator.resources` | object | -- | CPU/memory requests and limits for migration jobs |
| `spec.migrator.serviceAccountName` | string | -- | Service account for migration jobs |
| `spec.migrators` | MigratorSpec[] | -- | Ordered list of migrators (overrides `spec.migrator`) |
| `spec.safety.requireHealthyCluster` | bool | `true` | Require CNPG cluster healthy before migrating |
| `spec.safety.maxRetries` | int | `3` | Maximum retry attempts |
| `spec.safety.checksumMode` | enum | `auto-reconcile` | `strict`, `auto-reconcile`, or `pre-flight` |
| `spec.safety.continueOnFailure` | bool | `false` | Continue running subsequent migrators if one fails |
| `spec.timeouts.migration` | string | `5m` | Migration job timeout |

### MigrationRun

Automatically created by the operator for each migration execution. Provides an audit trail with attempt number, duration, phase, job name, and database health snapshot at time of migration.

### Annotations

| Annotation | Description |
|------------|-------------|
| `shinka.pleme.io/retry=true` | Trigger manual retry of a failed migration |
| `release.shinka.pleme.io/expected-tag=<tag>` | Fast-track requeue for expected deployment image (eliminates up to 90s delay) |

## Configuration

Configuration is loaded from environment variables, with optional overrides from a YAML config file (`SHINKA_CONFIG`).

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCH_NAMESPACE` | all | Namespace to watch (empty = all namespaces) |
| `HEALTH_ADDR` | `0.0.0.0:8080` | Health/API server listen address |
| `METRICS_PORT` | `9090` | Prometheus metrics port |
| `LOG_LEVEL` | `info` | Log level |
| `LOG_FORMAT` | `json` | Log format (`json` or `pretty`) |
| `SHINKA_CONFIG` | -- | Path to YAML config file |

### Migration Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_MIGRATION_TIMEOUT` | `300` | Default timeout in seconds |
| `DEFAULT_MAX_RETRIES` | `3` | Default max retries |
| `REQUEUE_INTERVAL` | `60` | Idle requeue interval in seconds |
| `DATABASE_CONNECT_TIMEOUT` | `10` | Database connection timeout in seconds |

### Leader Election Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ELECTION_ENABLED` | `true` | Enable lease-based leader election |
| `LEADER_ELECTION_LEASE_NAME` | `shinka-leader` | Lease resource name |
| `LEADER_ELECTION_LEASE_NAMESPACE` | `shinka-system` | Lease resource namespace |

### Notification Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `RELEASE_TRACKER_URL` | -- | Webhook URL for migration lifecycle events |
| `DISCORD_WEBHOOK_URL` | -- | Discord webhook URL for notifications |
| `DISCORD_CLUSTER_NAME` | -- | Cluster name shown in Discord embeds |
| `DISCORD_ENVIRONMENT` | -- | Environment name shown in Discord embeds |

## API

Shinka exposes REST and GraphQL APIs on the health server address (default `:8080`).

### REST

```
GET  /api/v1/migrations                                    # List all migrations
GET  /api/v1/namespaces/{ns}/migrations/{name}             # Get migration status
GET  /api/v1/namespaces/{ns}/migrations/{name}/history     # Migration run history
POST /api/v1/namespaces/{ns}/migrations/{name}/retry       # Retry a failed migration
POST /api/v1/namespaces/{ns}/migrations/{name}/cancel      # Cancel a running migration
GET  /api/v1/namespaces/{ns}/migrations/{name}/await       # Long-poll until ready
GET  /api/v1/migrations/watch                              # Server-Sent Events stream
GET  /api/v1/databases                                     # List tracked databases
GET  /api/v1/namespaces/{ns}/clusters/{cluster}/ready      # Database readiness check
GET  /api/v1/namespaces/{ns}/clusters/{cluster}/health     # Cluster health details
GET  /api/v1/queue/status                                  # Migration queue status
GET  /api/v1/queue/items                                   # List queued items
POST /api/v1/queue/pause                                   # Pause the queue
POST /api/v1/queue/resume                                  # Resume the queue
```

### GraphQL

Available at `/graphql` (POST for queries/mutations, GET for playground). Supports queries for migrations, databases, and queue status, mutations for retry/trigger, and subscriptions for real-time events.

### gRPC (optional)

Compile with the `grpc` feature flag:

```bash
cargo build --release --features grpc
```

## CLI

`shinka-cli` connects directly to the Kubernetes API (uses your kubeconfig).

```bash
# List all migrations
shinka-cli list -n production
shinka-cli list -A                                # all namespaces

# Get detailed status
shinka-cli status myapp-backend -n production

# View migration run history
shinka-cli history myapp-backend -n production --limit 10

# Retry a failed migration
shinka-cli retry myapp-backend -n production

# Watch migration status in real time
shinka-cli watch myapp-backend -n production

# Fetch logs from the last migration job
shinka-cli logs myapp-backend -n production
shinka-cli logs myapp-backend -n production -f    # follow
```

## Metrics

Prometheus metrics exposed at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `shinka_migrations_total` | counter | Total migrations by name, namespace, status |
| `shinka_migration_duration_seconds` | histogram | Migration duration (buckets: 1s to 600s) |
| `shinka_migrations_in_flight` | gauge | Currently running migrations per namespace |
| `shinka_database_health` | gauge | CNPG cluster health (1=healthy, 0=unhealthy) |
| `shinka_reconciliations_total` | counter | Reconciliation attempts by result |
| `shinka_errors_total` | counter | Errors by name, namespace, category |
| `shinka_retry_attempts_total` | counter | Retry attempts by migration |
| `shinka_migration_auto_retries_total` | counter | Auto-retries triggered by image change in Failed state |
| `shinka_migration_phase` | gauge | Current phase (0=Pending through 5=Failed) |
| `shinka_checksum_mismatches_total` | counter | Checksum mismatches by outcome |
| `shinka_checksum_reconciliations_total` | counter | Automatic checksum fixes |
| `shinka_migration_rollbacks_total` | counter | Transaction rollbacks by reason |
| `shinka_leader_status` | gauge | Leader election status (1=leader, 0=follower) |

A Grafana dashboard and Prometheus alerting rules are included in `deploy/`.

## Development

### Building with Cargo

```bash
cargo build --release                   # operator + CLI
cargo build --release --features grpc   # with gRPC support
cargo build --release --features tls    # with TLS admission webhook
```

Produces two binaries:
- `shinka` -- the operator (also runs as init container with `RUN_MODE=wait`)
- `shinka-cli` -- the CLI tool

### Building with Nix

```bash
# Build binary
nix build .#default

# Build OCI image (amd64)
nix build .#dockerImage-amd64

# Build OCI image (arm64)
nix build .#dockerImage-arm64

# Build and push to registry
nix run .#release
```

### Running Tests

```bash
cargo test
```

## Project Structure

```
shinka/
  src/
    main.rs                 # Entry point (operator / wait mode via RUN_MODE env)
    lib.rs                  # Public API surface
    config.rs               # Environment + YAML config loading
    error.rs                # Error types
    metrics.rs              # Prometheus metrics (14 metric families)
    health.rs               # Health check + GraphQL API server
    wait.rs                 # Init container wait logic (polls REST API)
    leader.rs               # Lease-based leader election
    circuit_breaker.rs      # Circuit breaker for database connectivity
    redact.rs               # Credential redaction in logs
    util.rs                 # Shared utilities
    webhook.rs              # Admission webhook server (optional TLS)
    crd/
      database_migration.rs # DatabaseMigration CRD definition
      migration_run.rs      # MigrationRun CRD (audit trail)
    controller/
      reconciler/           # Main reconciliation logic
      discord.rs            # Discord webhook notifications
      events.rs             # Kubernetes event emission
      finalizers.rs         # Resource cleanup on deletion
      webhook.rs            # Release tracker webhook client
    api/
      rest.rs               # REST API handlers (14 endpoints)
      graphql.rs            # GraphQL schema (queries, mutations, subscriptions)
      grpc.rs               # gRPC service (optional feature)
      service.rs            # Core API service layer
      types.rs              # API request/response types
    database/               # CNPG cluster health checking
    migrator/
      mod.rs                # Migrator type registry
      types.rs              # MigratorType enum (11 tools)
      job_builder.rs        # Kubernetes Job construction
      job_watcher.rs        # Job completion monitoring
      checksum_reconciler.rs # Database checksum validation
      preflight.rs          # Pre-flight checksum checks
    bin/
      shinka-cli.rs         # CLI tool (list, status, history, retry, watch, logs)
      shinka_wait.rs        # Standalone wait binary
  proto/
    shinka.proto            # Protobuf service definitions (optional gRPC)
  deploy/
    crds/                   # DatabaseMigration CRD YAML
    rbac/                   # ServiceAccount, Role, RoleBinding
    deployment.yaml         # Operator Deployment
    service.yaml            # ClusterIP Service
    servicemonitor.yaml     # Prometheus ServiceMonitor
    prometheus-rules.yaml   # Alerting rules
    grafana-dashboard.yaml  # Grafana dashboard ConfigMap
    kustomization.yaml      # Kustomize base
  module/
    default.nix             # Home-manager module
    nixos.nix               # NixOS module
```

## Related Projects

- [kenshi](https://github.com/pleme-io/kenshi) -- Build pipeline and test gate operator (coordinates with Shinka for ephemeral environment migrations)
- [substrate](https://github.com/pleme-io/substrate) -- Nix build patterns (`buildRustService`, `rust-service-flake.nix`) used by this repo
- [forge](https://github.com/pleme-io/forge) -- CI/CD build platform for image release
- [k8s](https://github.com/pleme-io/k8s) -- GitOps manifests reconciled by FluxCD

## License

[MIT](LICENSE)
