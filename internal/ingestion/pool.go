// Package ingestion implements a channel-based worker pool for log ingestion.
//
// Architecture:
//
//	reader goroutine
//	     │
//	     │  chan string (raw lines)
//	     ▼
//	┌─────────────────────────┐
//	│  worker 0  worker 1 ... │  (N = runtime.NumCPU())
//	│  parse → batch → index  │
//	└─────────────────────────┘
//
// The reader owns I/O; workers own CPU (parsing + index writes).
// Separating them means I/O wait never blocks parsing and vice versa.
// At 42M rows/hour (~11,667 lines/sec) on a multi-core machine, N workers
// parsing in parallel saturates the index's RWMutex more efficiently than
// a single goroutine doing parse + insert serially.
package ingestion

import (
	"bufio"
	"context"
	"io"
	"runtime"
	"sync"

	"github.com/raghavkachroo/log-triage-v2/internal/index"
	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

const (
	// lineChanCap is the buffer between the reader and the worker pool.
	// Large enough to smooth over bursts without unbounded memory growth.
	lineChanCap = 8192

	// batchSize is how many parsed entries a worker accumulates before
	// calling InsertBatch. Larger batches mean fewer lock acquisitions.
	batchSize = 1000
)

// Metrics is a callback interface so the pool can increment counters without
// importing Prometheus directly — keeps this package testable without the
// full metrics stack.
type Metrics interface {
	IncIngested(n float64)
	IncParseError()
}

// Pool is a channel-based worker pool for parsing and indexing log lines.
type Pool struct {
	idx     *index.Index
	metrics Metrics
	workers int
	lines   chan string
}

// New creates a Pool with workers = runtime.NumCPU().
// Call Run to start it.
func New(idx *index.Index, m Metrics) *Pool {
	return &Pool{
		idx:     idx,
		metrics: m,
		workers: runtime.NumCPU(),
		lines:   make(chan string, lineChanCap),
	}
}

// NewWithWorkers creates a Pool with an explicit worker count.
// Useful for testing and benchmarking.
func NewWithWorkers(idx *index.Index, m Metrics, workers int) *Pool {
	return &Pool{
		idx:     idx,
		metrics: m,
		workers: workers,
		lines:   make(chan string, lineChanCap),
	}
}

// Run starts the worker pool and blocks until ctx is cancelled and all
// workers have drained the line channel and exited.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.work()
		}()
	}
	// When ctx is cancelled, drain the channel so workers exit cleanly.
	<-ctx.Done()
	close(p.lines)
	wg.Wait()
}

// Feed reads lines from r and sends them to the worker pool.
// Returns (bytes read, lines sent). Blocks until r returns EOF or ctx is cancelled.
func (p *Pool) Feed(ctx context.Context, r io.Reader) (int64, int) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var bytesRead int64
	var linesSent int

	for scanner.Scan() {
		line := scanner.Text()
		bytesRead += int64(len(line)) + 1

		select {
		case p.lines <- line:
			linesSent++
		case <-ctx.Done():
			return bytesRead, linesSent
		}
	}
	return bytesRead, linesSent
}

// work is the per-worker loop: pull lines from the channel, parse them,
// accumulate a batch, and flush to the index.
func (p *Pool) work() {
	batch := make([]parser.LogEntry, 0, batchSize)

	flush := func() {
		if len(batch) > 0 {
			p.idx.InsertBatch(batch)
			p.metrics.IncIngested(float64(len(batch)))
			batch = batch[:0]
		}
	}

	for line := range p.lines {
		entry, err := parser.Parse(line)
		if err != nil {
			p.metrics.IncParseError()
			continue
		}
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			flush()
		}
	}

	// Drain remaining batch when channel is closed.
	flush()
}
