# Shinka API Design

> **Note:** This is a design document describing the target API surface. Some sections may not yet be implemented or may differ from the current codebase. For the current API, refer to the source code in `src/api/` and the README.

> Comprehensive API specification for application integration with Shinka migration operator.

## Overview

Shinka exposes APIs that allow applications to:

1. **Query migration status** before rolling out
2. **Wait for migrations** to complete (blocking or polling)
3. **Receive notifications** when migration state changes
4. **Integrate with deployment orchestration** (Kubernetes, CI/CD)

## API Endpoints

### REST API (v1)

Base URL: `http://shinka.shinka-system.svc.cluster.local:8080/api/v1`

---

#### Migrations

##### List Migrations

```http
GET /migrations
GET /migrations?namespace={namespace}
GET /migrations?phase={phase}
GET /migrations?database={cluster-name}
GET /migrations?migrator={type}
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `namespace` | string | Filter by namespace |
| `phase` | string | Filter by phase: `Pending`, `CheckingHealth`, `WaitingForDatabase`, `Migrating`, `Ready`, `Failed` |
| `database` | string | Filter by CNPG cluster name |
| `migrator` | string | Filter by migrator type: `sqlx`, `goose`, etc. |
| `ready` | bool | Filter to only ready (`true`) or not ready (`false`) |
| `limit` | int | Max results (default: 100) |
| `continue` | string | Pagination token |

**Response:**

```json
{
  "apiVersion": "shinka.pleme.io/v1",
  "kind": "MigrationList",
  "metadata": {
    "continue": "eyJvZmZzZXQiOjEwMH0=",
    "remainingItemCount": 50
  },
  "items": [
    {
      "metadata": {
        "name": "myapp-backend",
        "namespace": "myapp-staging",
        "uid": "abc123",
        "creationTimestamp": "2025-01-15T10:00:00Z"
      },
      "spec": {
        "database": {
          "cnpgClusterRef": { "name": "myapp-postgres", "database": "myapp" }
        },
        "migrator": {
          "type": "sqlx",
          "deploymentRef": { "name": "myapp-backend" }
        }
      },
      "status": {
        "phase": "Ready",
        "ready": true,
        "lastMigration": {
          "imageTag": "sha-abc123",
          "success": true,
          "duration": "12s",
          "completedAt": "2025-01-15T10:05:00Z"
        },
        "conditions": [
          {
            "type": "Ready",
            "status": "True",
            "reason": "MigrationSucceeded",
            "message": "Migration completed successfully",
            "lastTransitionTime": "2025-01-15T10:05:00Z"
          }
        ]
      }
    }
  ]
}
```

---

##### Get Migration Status

```http
GET /migrations/{namespace}/{name}
```

**Response:**

```json
{
  "apiVersion": "shinka.pleme.io/v1",
  "kind": "Migration",
  "metadata": {
    "name": "myapp-backend",
    "namespace": "myapp-staging",
    "uid": "abc123",
    "generation": 5,
    "creationTimestamp": "2025-01-15T10:00:00Z"
  },
  "spec": {
    /* ... */
  },
  "status": {
    "phase": "Ready",
    "ready": true,
    "observedGeneration": 5,
    "lastMigration": {
      "imageTag": "sha-abc123",
      "success": true,
      "duration": "12s",
      "completedAt": "2025-01-15T10:05:00Z",
      "jobName": "myapp-backend-migration-a1b2c3d4"
    },
    "retryCount": 0,
    "currentJob": null,
    "conditions": [
      /* ... */
    ],
    "history": [
      {
        "imageTag": "sha-abc123",
        "success": true,
        "duration": "12s",
        "completedAt": "2025-01-15T10:05:00Z"
      },
      {
        "imageTag": "sha-xyz789",
        "success": true,
        "duration": "8s",
        "completedAt": "2025-01-14T15:30:00Z"
      }
    ]
  }
}
```

---

##### Check Migration Ready

Simple boolean check for application readiness gates.

```http
GET /migrations/{namespace}/{name}/ready
```

**Response (success):**

```json
{
  "ready": true,
  "phase": "Ready",
  "message": "Migration completed successfully for image sha-abc123"
}
```

**Response (not ready):**

```json
{
  "ready": false,
  "phase": "Migrating",
  "message": "Migration in progress for image sha-def456",
  "retryAfter": 5
}
```

**HTTP Status Codes:**

- `200 OK` - Migration is ready
- `202 Accepted` - Migration in progress (includes `Retry-After` header)
- `503 Service Unavailable` - Migration failed or database unhealthy

---

##### Wait for Migration (Long Poll)

Block until migration reaches desired state or timeout.

```http
GET /migrations/{namespace}/{name}/wait
GET /migrations/{namespace}/{name}/wait?timeout=60s
GET /migrations/{namespace}/{name}/wait?for=Ready
GET /migrations/{namespace}/{name}/wait?imageTag=sha-abc123
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `timeout` | duration | Max wait time (default: `30s`, max: `5m`) |
| `for` | string | Wait for phase: `Ready`, `Migrating`, `Failed` |
| `imageTag` | string | Wait for specific image tag to be migrated |

**Response:**

```json
{
  "ready": true,
  "phase": "Ready",
  "waited": "12.5s",
  "migration": {
    /* full migration object */
  }
}
```

---

##### Get Migration Conditions

Detailed conditions for debugging and monitoring.

```http
GET /migrations/{namespace}/{name}/conditions
```

**Response:**

```json
{
  "conditions": [
    {
      "type": "Ready",
      "status": "True",
      "reason": "MigrationSucceeded",
      "message": "Migration completed successfully",
      "lastTransitionTime": "2025-01-15T10:05:00Z",
      "observedGeneration": 5
    },
    {
      "type": "DatabaseHealthy",
      "status": "True",
      "reason": "ClusterReady",
      "message": "CNPG cluster myapp-postgres is healthy",
      "lastTransitionTime": "2025-01-15T10:04:55Z"
    },
    {
      "type": "DeploymentFound",
      "status": "True",
      "reason": "DeploymentExists",
      "message": "Deployment myapp-backend found with image sha-abc123",
      "lastTransitionTime": "2025-01-15T10:04:50Z"
    }
  ],
  "summary": {
    "ready": true,
    "allConditionsHealthy": true,
    "lastTransition": "2025-01-15T10:05:00Z"
  }
}
```

---

##### Get Migration History

Historical migrations for the resource.

```http
GET /migrations/{namespace}/{name}/history
GET /migrations/{namespace}/{name}/history?limit=10
```

**Response:**

```json
{
  "migrations": [
    {
      "imageTag": "sha-abc123",
      "migratorType": "sqlx",
      "success": true,
      "duration": "12s",
      "startedAt": "2025-01-15T10:04:48Z",
      "completedAt": "2025-01-15T10:05:00Z",
      "jobName": "myapp-backend-migration-a1b2c3d4",
      "retryCount": 0
    },
    {
      "imageTag": "sha-xyz789",
      "migratorType": "sqlx",
      "success": true,
      "duration": "8s",
      "startedAt": "2025-01-14T15:29:52Z",
      "completedAt": "2025-01-14T15:30:00Z",
      "jobName": "myapp-backend-migration-e5f6g7h8",
      "retryCount": 0
    }
  ],
  "stats": {
    "totalMigrations": 25,
    "successCount": 24,
    "failureCount": 1,
    "averageDuration": "10.5s",
    "lastSuccess": "2025-01-15T10:05:00Z",
    "lastFailure": "2025-01-10T14:20:00Z"
  }
}
```

---

#### Databases

##### List Database Clusters

```http
GET /databases
GET /databases?namespace={namespace}
GET /databases?healthy={true|false}
```

**Response:**

```json
{
  "databases": [
    {
      "name": "myapp-postgres",
      "namespace": "myapp-staging",
      "healthy": true,
      "status": {
        "phase": "Cluster in healthy state",
        "instances": 3,
        "readyInstances": 3,
        "primaryPod": "myapp-postgres-1"
      },
      "migrations": {
        "total": 3,
        "ready": 2,
        "pending": 1,
        "failed": 0
      }
    }
  ]
}
```

---

##### Check Database Ready

```http
GET /databases/{namespace}/{cluster}/ready
```

**Response:**

```json
{
  "ready": true,
  "healthy": true,
  "phase": "Cluster in healthy state",
  "instances": {
    "total": 3,
    "ready": 3
  },
  "migrations": {
    "allReady": false,
    "pending": ["api-backend"],
    "ready": ["auth-service", "payment-service"]
  }
}
```

---

##### Wait for All Migrations on Database

```http
GET /databases/{namespace}/{cluster}/wait
GET /databases/{namespace}/{cluster}/wait?timeout=120s
```

Wait for ALL migrations targeting this database to complete.

**Response:**

```json
{
  "ready": true,
  "waited": "45.2s",
  "migrations": [
    { "name": "auth-service", "ready": true },
    { "name": "payment-service", "ready": true },
    { "name": "api-backend", "ready": true }
  ]
}
```

---

#### Queue & Parallelization

##### Get Queue Status

```http
GET /queue
GET /queue?namespace={namespace}
```

**Response:**

```json
{
  "active": 3,
  "pending": 5,
  "maxConcurrent": 10,
  "strategy": "fair",
  "queued": [
    {
      "name": "api-backend",
      "namespace": "production",
      "position": 1,
      "priority": 100,
      "queuedAt": "2025-01-15T10:05:00Z",
      "estimatedStart": "2025-01-15T10:05:30Z"
    }
  ],
  "active": [
    {
      "name": "auth-service",
      "namespace": "production",
      "startedAt": "2025-01-15T10:04:00Z",
      "duration": "60s"
    }
  ],
  "byNamespace": {
    "production": { "active": 2, "pending": 3 },
    "staging": { "active": 1, "pending": 2 }
  }
}
```

---

#### Events & Webhooks

##### Subscribe to Events (WebSocket)

```
WS /events
WS /events?namespace={namespace}
WS /events?migration={namespace}/{name}
```

**Event Messages:**

```json
{
  "type": "MigrationStarted",
  "timestamp": "2025-01-15T10:04:48Z",
  "migration": {
    "name": "myapp-backend",
    "namespace": "myapp-staging"
  },
  "data": {
    "imageTag": "sha-abc123",
    "migratorType": "sqlx"
  }
}

{
  "type": "MigrationCompleted",
  "timestamp": "2025-01-15T10:05:00Z",
  "migration": {
    "name": "myapp-backend",
    "namespace": "myapp-staging"
  },
  "data": {
    "success": true,
    "duration": "12s",
    "imageTag": "sha-abc123"
  }
}

{
  "type": "MigrationFailed",
  "timestamp": "2025-01-15T10:06:00Z",
  "migration": {
    "name": "api-backend",
    "namespace": "production"
  },
  "data": {
    "error": "Migration job failed: exit code 1",
    "retryCount": 2,
    "maxRetries": 3,
    "willRetry": true
  }
}

{
  "type": "DatabaseHealthChanged",
  "timestamp": "2025-01-15T10:03:00Z",
  "database": {
    "name": "myapp-postgres",
    "namespace": "myapp-staging"
  },
  "data": {
    "healthy": true,
    "previousPhase": "Cluster is being created",
    "currentPhase": "Cluster in healthy state"
  }
}
```

---

##### Configure Webhook

```http
POST /webhooks
```

**Request:**

```json
{
  "name": "slack-notifications",
  "url": "https://hooks.slack.com/services/xxx",
  "events": ["MigrationFailed", "MigrationCompleted"],
  "filter": {
    "namespaces": ["production"],
    "onlyFailures": false
  },
  "headers": {
    "Authorization": "Bearer xxx"
  },
  "retryPolicy": {
    "maxRetries": 3,
    "backoff": "exponential"
  }
}
```

---

### gRPC API

For high-performance integrations.

```protobuf
syntax = "proto3";

package shinka.v1;

service MigrationService {
  // Query migrations
  rpc ListMigrations(ListMigrationsRequest) returns (ListMigrationsResponse);
  rpc GetMigration(GetMigrationRequest) returns (Migration);
  rpc GetMigrationStatus(GetMigrationStatusRequest) returns (MigrationStatus);

  // Readiness checks
  rpc CheckReady(CheckReadyRequest) returns (CheckReadyResponse);
  rpc WaitForReady(WaitForReadyRequest) returns (stream WaitForReadyResponse);

  // Database operations
  rpc ListDatabases(ListDatabasesRequest) returns (ListDatabasesResponse);
  rpc CheckDatabaseReady(CheckDatabaseReadyRequest) returns (CheckDatabaseReadyResponse);

  // Event streaming
  rpc WatchEvents(WatchEventsRequest) returns (stream MigrationEvent);

  // Queue management
  rpc GetQueueStatus(GetQueueStatusRequest) returns (QueueStatus);
}

message CheckReadyRequest {
  string namespace = 1;
  string name = 2;
  optional string image_tag = 3;  // Wait for specific image
}

message CheckReadyResponse {
  bool ready = 1;
  string phase = 2;
  string message = 3;
  optional int32 retry_after_seconds = 4;
}

message WaitForReadyRequest {
  string namespace = 1;
  string name = 2;
  optional string image_tag = 3;
  int32 timeout_seconds = 4;
}

message WaitForReadyResponse {
  bool ready = 1;
  string phase = 2;
  double waited_seconds = 3;
  Migration migration = 4;
}
```

---

### GraphQL API

For flexible querying and subscriptions.

```graphql
type Query {
  # Migrations
  migrations(
    namespace: String
    phase: MigrationPhase
    database: String
    migrator: MigratorType
    ready: Boolean
    first: Int
    after: String
  ): MigrationConnection!

  migration(namespace: String!, name: String!): Migration
  migrationReady(namespace: String!, name: String!): ReadinessStatus!
  migrationHistory(
    namespace: String!
    name: String!
    limit: Int
  ): [MigrationRun!]!

  # Databases
  databases(namespace: String, healthy: Boolean): [Database!]!
  database(namespace: String!, name: String!): Database
  databaseReady(namespace: String!, name: String!): DatabaseReadiness!

  # Queue
  queueStatus: QueueStatus!
}

type Mutation {
  # Trigger manual migration (if allowed)
  triggerMigration(namespace: String!, name: String!): Migration!

  # Retry failed migration
  retryMigration(namespace: String!, name: String!): Migration!

  # Webhooks
  createWebhook(input: WebhookInput!): Webhook!
  deleteWebhook(name: String!): Boolean!
}

type Subscription {
  # Watch specific migration
  migrationEvents(namespace: String!, name: String!): MigrationEvent!

  # Watch all migrations in namespace
  namespaceMigrationEvents(namespace: String!): MigrationEvent!

  # Watch all events
  allMigrationEvents: MigrationEvent!

  # Watch database health
  databaseHealthEvents(namespace: String!, name: String!): DatabaseHealthEvent!
}

type Migration {
  metadata: ObjectMeta!
  spec: MigrationSpec!
  status: MigrationStatus!
  ready: Boolean!
  conditions: [Condition!]!
  history(limit: Int): [MigrationRun!]!
}

type ReadinessStatus {
  ready: Boolean!
  phase: MigrationPhase!
  message: String!
  retryAfter: Int
  conditions: [Condition!]!
}

type MigrationEvent {
  type: EventType!
  timestamp: DateTime!
  migration: Migration!
  data: EventData!
}

enum EventType {
  MIGRATION_STARTED
  MIGRATION_COMPLETED
  MIGRATION_FAILED
  MIGRATION_RETRYING
  DATABASE_HEALTH_CHANGED
  PHASE_CHANGED
}
```

---

## Kubernetes Integration

### Init Container

Applications can use an init container that blocks until migrations complete:

```yaml
initContainers:
  - name: wait-for-migration
    image: ghcr.io/pleme-io/shinka-wait:latest
    env:
      - name: SHINKA_API
        value: "http://shinka.shinka-system.svc.cluster.local:8080"
      - name: MIGRATION_NAME
        value: "my-app"
      - name: MIGRATION_NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
      - name: TIMEOUT
        value: "300s"
```

### Readiness Gate

Pods can use readiness gates tied to migration status:

```yaml
spec:
  readinessGates:
    - conditionType: "shinka.pleme.io/migration-ready"
```

### Pod Annotation for Auto-Discovery

```yaml
metadata:
  annotations:
    shinka.pleme.io/wait-for-migration: "my-database-migration"
    shinka.pleme.io/wait-timeout: "120s"
```

---

## SDK Examples

### Go Client

```go
import "github.com/pleme-io/shinka/sdk/go"

client := shinka.NewClient("http://shinka.shinka-system:8080")

// Check if ready
ready, err := client.Migrations("production").
    Get("api-backend").
    IsReady(ctx)

// Wait for migration
migration, err := client.Migrations("production").
    Get("api-backend").
    WaitForReady(ctx, 60*time.Second)

// Watch events
events := client.Migrations("production").
    Get("api-backend").
    Watch(ctx)

for event := range events {
    fmt.Printf("Event: %s\n", event.Type)
}
```

### Rust Client

```rust
use shinka_sdk::Client;

let client = Client::new("http://shinka.shinka-system:8080");

// Check if ready
let status = client.migrations("production", "api-backend")
    .ready()
    .await?;

// Wait for migration
let migration = client.migrations("production", "api-backend")
    .wait_for_ready(Duration::from_secs(60))
    .await?;

// Stream events
let mut events = client.migrations("production", "api-backend")
    .watch()
    .await?;

while let Some(event) = events.next().await {
    println!("Event: {:?}", event.event_type);
}
```

### TypeScript Client

```typescript
import { ShinkaClient } from "@pleme/shinka-sdk";

const client = new ShinkaClient("http://shinka.shinka-system:8080");

// Check if ready
const ready = await client.migrations("production", "api-backend").isReady();

// Wait for migration
const migration = await client
  .migrations("production", "api-backend")
  .waitForReady({ timeout: "60s" });

// Subscribe to events
const subscription = client
  .migrations("production", "api-backend")
  .subscribe((event) => {
    console.log("Event:", event.type);
  });
```

---

## Error Responses

All endpoints return consistent error responses:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Migration 'api-backend' not found in namespace 'production'",
    "details": {
      "namespace": "production",
      "name": "api-backend"
    }
  }
}
```

**Error Codes:**
| Code | HTTP Status | Description |
|------|-------------|-------------|
| `NOT_FOUND` | 404 | Resource not found |
| `TIMEOUT` | 408 | Wait timeout exceeded |
| `MIGRATION_FAILED` | 503 | Migration is in failed state |
| `DATABASE_UNHEALTHY` | 503 | Target database is not healthy |
| `INVALID_REQUEST` | 400 | Invalid request parameters |
| `INTERNAL_ERROR` | 500 | Internal server error |
