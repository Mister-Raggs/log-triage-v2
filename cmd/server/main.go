// Command server runs the log triage HTTP API.
//
// It starts a worker pool ingestion pipeline that reads log lines from a file
// (or stdin), parses them in parallel, and indexes them, while serving queries
// over HTTP.
//
// Endpoints:
//   - POST /query   — find nearest log entries to a timestamp
//   - GET  /health  — liveness probe
//   - GET  /metrics — Prometheus metrics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raghavkachroo/log-triage-v2/internal/index"
	"github.com/raghavkachroo/log-triage-v2/internal/ingestion"
	"github.com/raghavkachroo/log-triage-v2/internal/query"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// --- Prometheus metrics ---
var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logtriage_requests_total",
			Help: "Total number of HTTP requests by endpoint and status.",
		},
		[]string{"endpoint", "status"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logtriage_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint"},
	)

	indexSize = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "logtriage_index_size",
			Help: "Number of log entries currently in the index.",
		},
		nil, // Set after index is created.
	)

	ingestedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "logtriage_ingested_total",
			Help: "Total number of log lines ingested.",
		},
	)

	parseErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "logtriage_parse_errors_total",
			Help: "Total number of log lines that failed to parse.",
		},
	)

	evictionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "logtriage_evictions_total",
			Help: "Total number of log entries evicted from the index due to the max-entries cap.",
		},
	)
)

// initTracer sets up an OTel TracerProvider that writes spans as JSON to stdout.
// Returns a shutdown function the caller must defer.
func initTracer() (func(context.Context) error, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithSampler(trace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func main() {
	// --- Configuration ---
	addr       := flag.String("addr", ":8080", "HTTP listen address")
	logFile    := flag.String("log-file", "", "log file to ingest (empty = stdin)")
	maxEntries := flag.Int("max-entries", 0, "max index entries before FIFO eviction (0 = unlimited)")
	flag.Parse()

	// --- Logger ---
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// --- OTel tracer ---
	// Spans are exported as JSON to stdout so they appear alongside zap logs.
	// Swap stdouttrace for an OTLP exporter to send to Jaeger/Tempo in production.
	shutdownTracer, err := initTracer()
	if err != nil {
		logger.Fatal("failed to init tracer", zap.Error(err))
	}
	defer shutdownTracer(context.Background())

	// --- Index + Query handler ---
	var idx *index.Index
	if *maxEntries > 0 {
		logger.Info("index capped", zap.Int("max_entries", *maxEntries))
		idx = index.NewCapped(*maxEntries, func(n int) {
			evictionsTotal.Add(float64(n))
		})
	} else {
		idx = index.New()
	}
	handler := query.NewHandler(idx)

	// Register the index size gauge now that we have the index.
	indexSize = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "logtriage_index_size",
			Help: "Number of log entries currently in the index.",
		},
		func() float64 { return float64(idx.Size()) },
	)

	prometheus.MustRegister(requestsTotal, requestDuration, indexSize, ingestedTotal, parseErrors, evictionsTotal)

	// --- Ingestion worker pool ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := ingestion.New(idx, &promMetrics{})

	// Pool runs until ctx is cancelled; reader feeds lines into it.
	go pool.Run(ctx)
	go runReader(ctx, logger, pool, *logFile)

	// --- HTTP server ---
	mux := http.NewServeMux()
	mux.HandleFunc("/query", queryHandler(handler, logger))
	mux.HandleFunc("/health", healthHandler())
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// --- Graceful shutdown ---
	go func() {
		logger.Info("server starting", zap.String("addr", *addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server failed", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	logger.Info("shutting down")
	cancel() // Stop ingestion.

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", zap.Error(err))
	}
	logger.Info("server stopped")
}

// promMetrics adapts the Prometheus counters to the ingestion.Metrics interface.
type promMetrics struct{}

func (p *promMetrics) IncIngested(n float64) { ingestedTotal.Add(n) }
func (p *promMetrics) IncParseError()        { parseErrors.Inc() }

// runReader feeds raw log lines into the worker pool.
// For files it tails new content after EOF; for stdin it exits on EOF.
func runReader(ctx context.Context, logger *zap.Logger, pool *ingestion.Pool, path string) {
	if path == "" {
		logger.Info("reading logs from stdin")
		pool.Feed(ctx, os.Stdin)
		return
	}

	f, err := os.Open(path)
	if err != nil {
		logger.Error("failed to open log file", zap.String("path", path), zap.Error(err))
		return
	}
	defer f.Close()

	var offset int64
	for {
		select {
		case <-ctx.Done():
			logger.Info("ingestion stopped")
			return
		default:
		}

		if _, err := f.Seek(offset, 0); err != nil {
			logger.Error("seek error", zap.Error(err))
			return
		}

		newOffset, n := pool.Feed(ctx, f)
		offset += newOffset

		if n == 0 {
			select {
			case <-ctx.Done():
				logger.Info("ingestion stopped")
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

// tracer is the OTel tracer for the query handler.
// Named after the package so spans appear as "logtriage/query.*" in backends.
var tracer = otel.Tracer("logtriage/query")

// queryHandler returns an HTTP handler for POST /query.
// It creates a root span for the full request and child spans for each
// logical phase: decode → index_lookup → encode.
func queryHandler(h *query.Handler, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		if r.Method != http.MethodPost {
			requestsTotal.WithLabelValues("query", "405").Inc()
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Root span for the entire /query request.
		ctx, span := tracer.Start(r.Context(), "query.request")
		defer span.End()

		// Span 1: JSON decode
		_, decodeSpan := tracer.Start(ctx, "query.decode")
		var req query.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			decodeSpan.RecordError(err)
			decodeSpan.End()
			requestsTotal.WithLabelValues("query", "400").Inc()
			http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
			return
		}
		if req.Timestamp.IsZero() {
			decodeSpan.End()
			requestsTotal.WithLabelValues("query", "400").Inc()
			http.Error(w, "timestamp is required", http.StatusBadRequest)
			return
		}
		decodeSpan.End()

		// Span 2: index lookup — this is the hot path we're benchmarking.
		_, lookupSpan := tracer.Start(ctx, "query.index_lookup")
		lookupSpan.SetAttributes(
			attribute.String("query.tag", req.Tag),
			attribute.Int("query.count", req.Count),
			attribute.Int("index.size", h.IndexSize()),
		)
		resp := h.Execute(req)
		lookupSpan.SetAttributes(attribute.Int("result.total", resp.Total))
		lookupSpan.End()

		// Span 3: JSON encode
		_, encodeSpan := tracer.Start(ctx, "query.encode")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			encodeSpan.RecordError(err)
			logger.Error("failed to encode response", zap.Error(err))
		}
		encodeSpan.End()

		duration := time.Since(start).Seconds()
		requestsTotal.WithLabelValues("query", "200").Inc()
		requestDuration.WithLabelValues("query").Observe(duration)
	}
}

// healthHandler returns an HTTP handler for GET /health.
func healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	}
}
