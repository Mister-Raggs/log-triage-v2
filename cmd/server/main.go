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
)

func main() {
	// --- Configuration ---
	addr := flag.String("addr", ":8080", "HTTP listen address")
	logFile := flag.String("log-file", "", "log file to ingest (empty = stdin)")
	flag.Parse()

	// --- Logger ---
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// --- Index + Query handler ---
	idx := index.New()
	handler := query.NewHandler(idx)

	// Register the index size gauge now that we have the index.
	indexSize = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "logtriage_index_size",
			Help: "Number of log entries currently in the index.",
		},
		func() float64 { return float64(idx.Size()) },
	)

	prometheus.MustRegister(requestsTotal, requestDuration, indexSize, ingestedTotal, parseErrors)

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

// queryHandler returns an HTTP handler for POST /query.
func queryHandler(h *query.Handler, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		if r.Method != http.MethodPost {
			requestsTotal.WithLabelValues("query", "405").Inc()
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req query.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			requestsTotal.WithLabelValues("query", "400").Inc()
			http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
			return
		}

		if req.Timestamp.IsZero() {
			requestsTotal.WithLabelValues("query", "400").Inc()
			http.Error(w, "timestamp is required", http.StatusBadRequest)
			return
		}

		resp := h.Execute(req)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logger.Error("failed to encode response", zap.Error(err))
		}

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
