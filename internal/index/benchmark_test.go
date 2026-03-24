package index

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

// benchBase is a fixed reference point so benchmarks are reproducible.
var benchBase = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

// buildIndex creates a pre-populated index with n entries spread 1ms apart.
// Using millisecond spacing mirrors real production log density at ~42M rows/hour
// (≈11,667 entries/sec ≈ one entry every ~86µs).
func buildIndex(n int) *Index {
	idx := New()
	batch := make([]parser.LogEntry, n)
	tags := []string{"INFO", "ERROR", "WARN", "TRACE"}
	for i := range batch {
		batch[i] = parser.LogEntry{
			Timestamp: benchBase.Add(time.Duration(i) * time.Millisecond),
			Tag:       tags[i%len(tags)],
			Body:      fmt.Sprintf("benchmark entry %d", i),
		}
	}
	idx.InsertBatch(batch)
	return idx
}

// ---------------------------------------------------------------------------
// BenchmarkQuery — proves O(log n) by testing at 1k, 100k, and 1M entries.
//
// If the query is truly O(log n), we expect roughly:
//   1k  → ~10 comparisons
//   100k → ~17 comparisons
//   1M  → ~20 comparisons
//
// Wall-clock time should grow sub-linearly (approximately constant) because
// binary search over contiguous memory is dominated by cache effects, not
// comparison count, at these sizes.
// ---------------------------------------------------------------------------

func BenchmarkQuery(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"1k", 1_000},
		{"100k", 100_000},
		{"1M", 1_000_000},
	}

	for _, sz := range sizes {
		idx := buildIndex(sz.n)
		// Query the middle of the range — worst case for binary search (deepest tree path).
		mid := benchBase.Add(time.Duration(sz.n/2) * time.Millisecond)

		b.Run(fmt.Sprintf("unfiltered/%s", sz.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idx.Nearest(mid, "")
			}
		})

		b.Run(fmt.Sprintf("filtered/%s", sz.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idx.Nearest(mid, "ERROR")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BenchmarkAdd — measures ingestion cost for both single Insert and InsertBatch.
//
// Single Insert is O(n) per call due to slice shifting, so it degrades at scale.
// InsertBatch is O(n log n) for the whole batch — much cheaper amortized.
// This benchmark quantifies why the server uses InsertBatch (batches of 1000).
// ---------------------------------------------------------------------------

func BenchmarkAdd(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		// Pre-populate so we're measuring Insert into a realistically-sized index.
		idx := buildIndex(100_000)
		// Insert timestamps beyond the existing range so sort.Search hits the end
		// quickly — this isolates the cost of the append+shift, not the search.
		offset := 100_000
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.Insert(parser.LogEntry{
				Timestamp: benchBase.Add(time.Duration(offset+i) * time.Millisecond),
				Tag:       "INFO",
				Body:      "bench insert",
			})
		}
	})

	// Batch insert — measure the cost of inserting 1000 entries at a time,
	// matching the server's ingestion batch size.
	for _, batchSize := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("batch/%d", batchSize), func(b *testing.B) {
			batch := make([]parser.LogEntry, batchSize)
			for i := range batch {
				batch[i] = parser.LogEntry{
					Timestamp: benchBase.Add(time.Duration(i) * time.Millisecond),
					Tag:       "WARN",
					Body:      "bench batch",
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := buildIndex(100_000)
				idx.InsertBatch(batch)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BenchmarkConcurrentReadWrite — validates the sync.RWMutex strategy.
//
// This is the most important benchmark: it simulates the production access
// pattern where many goroutines query the index concurrently while a writer
// goroutine continuously ingests new entries. The index must remain correct
// under this load — no panics, no data races, and reads should not starve.
//
// Run with -race to verify: go test -bench=ConcurrentReadWrite -race
// ---------------------------------------------------------------------------

func BenchmarkConcurrentReadWrite(b *testing.B) {
	configs := []struct {
		name    string
		readers int
		writers int
	}{
		{"readers=8/writers=1", 8, 1},
		{"readers=16/writers=2", 16, 2},
		{"readers=32/writers=4", 32, 4},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			idx := buildIndex(100_000)

			// Total operations split across goroutines.
			opsPerGoroutine := b.N / (cfg.readers + cfg.writers)
			if opsPerGoroutine == 0 {
				opsPerGoroutine = 1
			}

			var readOps atomic.Int64
			var writeOps atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()

			var wg sync.WaitGroup

			// Launch reader goroutines.
			for r := 0; r < cfg.readers; r++ {
				wg.Add(1)
				go func(seed int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(seed)))
					for i := 0; i < opsPerGoroutine; i++ {
						// Query a random point in the index's time range.
						offset := rng.Intn(100_000)
						ts := benchBase.Add(time.Duration(offset) * time.Millisecond)
						idx.Nearest(ts, "")
						readOps.Add(1)
					}
				}(r)
			}

			// Launch writer goroutines.
			for w := 0; w < cfg.writers; w++ {
				wg.Add(1)
				go func(seed int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(seed + 1000)))
					for i := 0; i < opsPerGoroutine; i++ {
						// Insert at a random timestamp within the range.
						offset := rng.Intn(200_000)
						idx.Insert(parser.LogEntry{
							Timestamp: benchBase.Add(time.Duration(offset) * time.Millisecond),
							Tag:       "ERROR",
							Body:      "concurrent write",
						})
						writeOps.Add(1)
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()

			b.ReportMetric(float64(readOps.Load()), "reads")
			b.ReportMetric(float64(writeOps.Load()), "writes")
		})
	}
}
