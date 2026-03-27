// Command generator produces realistic fake log lines at configurable volume.
//
// Usage:
//
//	go run ./cmd/generator -rate 11667 -duration 60s -output logs/output.log
//
// The default rate of 11,667 lines/sec matches ~42M rows/hour, the throughput
// of the production system this project reimagines.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
	"go.uber.org/zap"
)

// Tag distribution mirrors a realistic production service:
// mostly INFO/TRACE, occasional WARNs, rare ERRORs.
var tagWeights = []struct {
	tag    string
	weight int
}{
	{"TRACE", 40},
	{"INFO", 40},
	{"WARN", 15},
	{"ERROR", 5},
}

// Sample message templates per tag. In a real system these would come from
// actual service output; here we use realistic-looking strings.
var messages = map[string][]string{
	"ERROR": {
		"request failed: connection timeout to downstream service",
		"database query exceeded 5s threshold: SELECT * FROM orders WHERE status='pending'",
		"null pointer exception in PaymentProcessor.processRefund",
		"TLS handshake failed: certificate expired for api.internal.svc",
		"out of memory: heap allocation failed during batch processing",
		"circuit breaker tripped for inventory-service after 10 consecutive failures",
		"failed to serialize response: invalid UTF-8 sequence in user.display_name",
		"rate limit exceeded for customer_id=8832: 1200 req/min (limit: 1000)",
	},
	"WARN": {
		"disk usage at 87% on /data/logs — approaching eviction threshold",
		"request latency p99 elevated: 2340ms (SLA: 2000ms)",
		"retry attempt 3/5 for message publish to kafka topic order-events",
		"connection pool near capacity: 48/50 active connections",
		"deprecated API version v1 called by client sdk-java/2.3.1",
		"GC pause exceeded 200ms: 347ms stop-the-world pause detected",
	},
	"INFO": {
		"server started on port 8080",
		"health check passed: all dependencies healthy",
		"processed batch of 500 events in 120ms",
		"cache hit ratio: 94.2% (last 5 min window)",
		"new deployment rolled out: version 2.14.3-rc1",
		"user session created: session_id=a3f8c912 region=us-west-2",
		"order completed: order_id=ORD-98234 total=142.50 currency=USD",
		"index compaction completed: freed 2.3GB, 12400 segments merged to 340",
	},
	"TRACE": {
		"entering function processOrder with order_id=ORD-98234",
		"SQL query plan: index scan on orders_pkey, estimated rows=1",
		"HTTP request: GET /api/v2/inventory/SKU-4421 -> 200 in 12ms",
		"message dequeued from kafka partition 7 offset 1284023",
		"context propagation: trace_id=abc123 span_id=def456 parent=ghi789",
		"mutex acquired on ShardManager.rebalance after 0.3ms wait",
	},
}

func main() {
	// --- Configuration via flags ---
	rate := flag.Int("rate", 11667, "log lines per second (11667 ≈ 42M/hour)")
	duration := flag.Duration("duration", 0, "how long to generate (0 = until interrupted)")
	output := flag.String("output", "", "output file path (empty = stdout)")
	flag.Parse()

	// --- Logger setup ---
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// --- Output writer setup ---
	var w *bufio.Writer
	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			logger.Fatal("failed to create output file", zap.Error(err))
		}
		defer f.Close()
		w = bufio.NewWriterSize(f, 256*1024) // 256KB buffer for throughput
		defer w.Flush()
	} else {
		w = bufio.NewWriterSize(os.Stdout, 256*1024)
		defer w.Flush()
	}

	// --- Graceful shutdown ---
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// --- Generation loop ---
	var totalLines atomic.Int64
	interval := time.Second / time.Duration(*rate) // time between lines
	startTime := time.Now()

	logger.Info("generator starting",
		zap.Int("rate_per_sec", *rate),
		zap.Duration("duration", *duration),
		zap.String("output", *output),
	)

	// Report throughput every 5 seconds.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			elapsed := time.Since(startTime).Seconds()
			count := totalLines.Load()
			logger.Info("generator throughput",
				zap.Int64("total_lines", count),
				zap.Float64("lines_per_sec", float64(count)/elapsed),
			)
		}
	}()

	// Optional deadline via context — ensures the loop exits cleanly and
	// deferred flushes run even if duration elapses mid-sleep.
	ctx := context.Background()
	var cancel context.CancelFunc
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Forward OS signals into context cancellation.
	go func() {
		<-stop
		logger.Info("generator stopped by signal", zap.Int64("total_lines", totalLines.Load()))
		cancel()
	}()

	rateTicker := time.NewTicker(interval)
	defer rateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("generator done", zap.Int64("total_lines", totalLines.Load()))
			return
		case <-rateTicker.C:
		}

		line := generateLine(time.Now())
		_, _ = w.WriteString(line)
		_ = w.WriteByte('\n')
		totalLines.Add(1)

		// Flush periodically to keep file readable during generation.
		if totalLines.Load()%1000 == 0 {
			_ = w.Flush()
		}
	}
}

// generateLine produces a single log line with the current timestamp.
func generateLine(now time.Time) string {
	tag := pickTag()
	msgs := messages[tag]
	body := msgs[rand.Intn(len(msgs))]
	return fmt.Sprintf("%s [%s] %s", now.UTC().Format(parser.TimeLayout), tag, body)
}

// pickTag selects a random tag according to the configured weight distribution.
func pickTag() string {
	total := 0
	for _, tw := range tagWeights {
		total += tw.weight
	}
	r := rand.Intn(total)
	for _, tw := range tagWeights {
		r -= tw.weight
		if r < 0 {
			return tw.tag
		}
	}
	return "INFO" // fallback, shouldn't reach here
}
