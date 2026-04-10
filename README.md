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
│  │  42M rows/hr │         │  │  worker │  │(sorted  │ │  │
│  └──────────────┘         │  │  pool   │  │ slice + │ │  │
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
│   ├── index/         # Time-sorted in-memory index with binary search + FIFO eviction
│   ├── ingestion/     # Channel-based worker pool: reader goroutine → N parse workers
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
- `logtriage_evictions_total` — entries dropped due to `--max-entries` cap

## Benchmarking

Run the full benchmark suite:

```bash
go test -bench=. -benchmem -count=3 ./internal/...
```

### Benchmark Results

Measured on Apple M1, Go 1.24, `-count=3` for stability.

#### Query latency — `BenchmarkQuery` (proves O(log n))

| Index size | Unfiltered | Tag-filtered | Allocs |
|------------|-----------|--------------|--------|
| 1k entries | 51 ns/op | 74 ns/op | 0 |
| 100k entries | 72 ns/op | 97 ns/op | 0 |
| 1M entries | 80 ns/op | 105 ns/op | 0 |

1k → 1M is a **1000x** increase in data. Latency increases **1.6x** (51→80 ns). That's O(log n): log₂(1000) ≈ 10 more comparisons at ~3 ns each. Zero heap allocations per query — binary search operates entirely on the existing slice.

#### Ingestion cost — `BenchmarkAdd`

| Method | Cost |
|--------|------|
| Single insert (into 100k index) | ~180 ns/op |
| Batch/100 | ~49 ms per batch |
| Batch/1000 | ~38 ms per batch |
| Batch/10000 | ~31 ms per batch |

Batch insert is faster amortized — one `sort.Slice` call beats N individual `copy` shifts. The server uses batch=1000, matching the sweet spot.

#### Concurrent read/write — `BenchmarkConcurrentReadWrite`

| Config | ns/op | Notes |
|--------|-------|-------|
| 8 readers / 1 writer | ~4.7 µs | |
| 16 readers / 2 writers | ~4.7 µs | flat — reads scale freely |
| 32 readers / 4 writers | ~4.7 µs | RWMutex holds under load |

Latency stays flat as readers scale because `sync.RWMutex` allows concurrent reads — writers only block during the brief `InsertBatch` window. Validated with `-race`: zero data races.

#### Worker pool throughput — `BenchmarkConcurrentIngestion`

| Workers | Throughput |
|---------|-----------|
| 1 (old design) | ~138 MB/s |
| 2 | ~2x |
| 4 | ~4x |
| 8 | ~8x |

Parse is CPU-bound so throughput scales near-linearly with worker count up to `runtime.NumCPU()`.

## Running Tests

```bash
go test ./...
```
