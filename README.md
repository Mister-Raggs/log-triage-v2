# log-triage-v2

A Go-based reimagining of a production log triage system originally built at Amazon, designed to find the closest log entry to an incident timestamp in sub-millisecond time across millions of log lines.

## The Original Problem

At Amazon, logs from a high-volume internal service (~42M rows/hour) were stored in Timber, an internal log aggregation tool, published with a 1-hour delay. During on-call incidents, engineers needed to quickly locate the relevant log entry nearest to a reported timestamp — buried in millions of plain-text lines formatted as:

```
2024-01-15T10:30:45.123Z [ERROR] request failed: connection timeout
```

The original solution (Lambda + Java) fetched logs via internal libraries, found the closest entry using binary search, exposed results via an MCP endpoint, and used AWS Step Functions + LLM confidence scoring to automate on-call SOP workflows. This reduced average triage time from **15 minutes to under 45 seconds**.

## What v2 Improves

| Decision | Why |
|---|---|
| **Rewritten in Go** | Better concurrency primitives (`goroutines`, `sync.RWMutex`), lower latency, smaller binaries — ideal for a latency-sensitive query service |
| **Simulated log ingestion** | No internal dependencies — anyone can clone and run it. Generator replicates the 42M rows/hour production volume |
| **Observability from day one** | Prometheus metrics (request count, latency histogram, index size) + structured logging via `zap` instead of bolt-on monitoring |
| **Containerized + K8s-native** | Multi-stage Docker build, Kubernetes manifests with probes and resource limits — fully portable |
| **Separated concerns** | Ingestion, indexing, query, and analysis are distinct packages with clear interfaces |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Pod                        │
│                                                         │
│  ┌──────────────┐         ┌──────────────────────────┐  │
│  │  Generator    │  logs   │  Server                  │  │
│  │  (sidecar)   │───────▶│                           │  │
│  │              │  file   │  ┌────────┐  ┌─────────┐ │  │
│  │  Simulates   │         │  │Ingestion│─▶│  Index  │ │  │
│  │  42M rows/hr │         │  │(goroutine)│ │(sorted  │ │  │
│  └──────────────┘         │  └────────┘  │ slice +  │ │  │
│                           │              │ binary   │ │  │
│                           │  ┌────────┐  │ search)  │ │  │
│                           │  │Query   │◀─┤         │ │  │
│                           │  │Handler │  └─────────┘ │  │
│                           │  └───┬────┘              │  │
│                           │      │                   │  │
│                           │  ┌───▼────┐              │  │
│                           │  │  HTTP  │              │  │
│                           │  │ Server │              │  │
│                           │  │:8080   │              │  │
│                           │  └────────┘              │  │
│                           └──────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │
         ▼
    POST /query     GET /health     GET /metrics
```

### Package Structure

```
log-triage-v2/
├── cmd/
│   ├── generator/     # Log generation CLI — configurable rate, duration, output
│   └── server/        # HTTP server — ingestion, query API, metrics
├── internal/
│   ├── parser/        # Log line parsing: "TIMESTAMP [TAG] BODY" → LogEntry
│   ├── index/         # Time-sorted in-memory index with binary search
│   └── query/         # Request/response types + query execution logic
├── k8s/               # Kubernetes manifests (Deployment, Service, ConfigMap)
├── Dockerfile         # Multi-stage build → ~15MB final image
└── README.md
```

## Quick Start

### Run Locally (no Docker)

```bash
# Generate some test logs
go run ./cmd/generator -rate 1000 -duration 30s -output /tmp/test.log

# Start the server (ingests the log file)
go run ./cmd/server -log-file /tmp/test.log

# Query for the nearest log to a timestamp
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"timestamp": "2024-01-15T10:30:45.123Z"}' | jq .

# Query with tag filter and multiple results
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"timestamp": "2024-01-15T10:30:45.123Z", "tag": "ERROR", "count": 5}' | jq .
```

### Run with Docker

```bash
# Build the image
docker build -t log-triage-v2 .

# Generate logs and pipe to server
docker run --rm log-triage-v2 generator -rate 1000 -duration 10s > /tmp/test.log
docker run --rm -p 8080:8080 -v /tmp/test.log:/data/logs/output.log log-triage-v2 \
  server -log-file /data/logs/output.log

# Query
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"timestamp": "2024-01-15T10:30:45.123Z"}' | jq .
```

### Deploy to Minikube

```bash
# Start minikube
minikube start

# Build the image inside minikube's Docker daemon
eval $(minikube docker-env)
docker build -t log-triage-v2:latest .

# Deploy everything
kubectl apply -f k8s/

# Wait for the pod to be ready
kubectl wait --for=condition=ready pod -l app=log-triage --timeout=60s

# Access the service
minikube service log-triage --url

# Query (use the URL from above)
curl -s -X POST $(minikube service log-triage --url)/query \
  -H "Content-Type: application/json" \
  -d '{"timestamp": "2024-01-15T10:30:45.123Z"}' | jq .
```

## API Reference

### POST /query

Find the nearest log entries to a given timestamp.

**Request:**
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "tag": "ERROR",
  "count": 5
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `timestamp` | string (ISO 8601) | Yes | Target timestamp to search near |
| `tag` | string | No | Filter by severity: ERROR, WARN, INFO, TRACE |
| `count` | int | No | Number of results (default 1, max 100) |

**Response:**
```json
{
  "entries": [
    {
      "timestamp": "2024-01-15T10:30:45.100Z",
      "tag": "ERROR",
      "body": "request failed: connection timeout to downstream service",
      "distance": "23ms"
    }
  ],
  "total": 1,
  "query_ms": 0.042
}
```

### GET /health

```json
{"status": "ok"}
```

### GET /metrics

Prometheus-format metrics including:
- `logtriage_requests_total` — request count by endpoint and status
- `logtriage_request_duration_seconds` — latency histogram
- `logtriage_index_size` — current number of indexed entries
- `logtriage_ingested_total` — total lines ingested
- `logtriage_parse_errors_total` — lines that failed to parse

## Benchmarking

Run the generator at full production volume:

```bash
# Generate at 42M rows/hour (11,667 lines/sec)
go run ./cmd/generator -rate 11667 -duration 60s -output /tmp/bench.log

# Run Go benchmarks
go test -bench=. -benchmem ./internal/...
```

## Running Tests

```bash
go test ./...
```
