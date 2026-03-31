package ingestion

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/index"
)

// noopMetrics satisfies the Metrics interface without any overhead.
type noopMetrics struct{}

func (n *noopMetrics) IncIngested(float64) {}
func (n *noopMetrics) IncParseError()      {}

// makeLines returns a single string containing n valid log lines.
func makeLines(n int) string {
	var b strings.Builder
	b.Grow(n * 80)
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%s [INFO] benchmark ingestion line %d\n",
			base.Add(time.Duration(i)*time.Millisecond).UTC().Format("2006-01-02T15:04:05.000Z"),
			i,
		)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// BenchmarkConcurrentIngestion — measures end-to-end throughput of the worker
// pool at different worker counts against a fixed 100k-line input.
//
// Compare single-worker (effectively the old design) against NumCPU workers
// to show the concurrency gain. Parse is CPU-bound, so throughput should
// scale close to linearly up to NumCPU.
// ---------------------------------------------------------------------------

func BenchmarkConcurrentIngestion(b *testing.B) {
	const lineCount = 100_000
	input := makeLines(lineCount)

	workerCounts := []int{1, 2, 4, 8}

	for _, w := range workerCounts {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))

			for i := 0; i < b.N; i++ {
				idx := index.New()
				pool := NewWithWorkers(idx, &noopMetrics{}, w)

				ctx, cancel := context.WithCancel(context.Background())

				// Pool runs until ctx cancelled + channel drained.
				done := make(chan struct{})
				go func() {
					pool.Run(ctx)
					close(done)
				}()

				// Feed all lines synchronously, then cancel to shut down.
				pool.Feed(ctx, strings.NewReader(input))
				cancel()
				<-done

				b.ReportMetric(float64(idx.Size()), "entries/op")
			}
		})
	}
}
