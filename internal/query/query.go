// Package query defines the request/response types and handler logic for log queries.
//
// This separates the query logic from HTTP transport so it can be tested independently
// and potentially reused with other transports (gRPC, CLI, etc).
package query

import (
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/index"
	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

// Request represents a log query. Timestamp is required; Tag is optional.
type Request struct {
	Timestamp time.Time `json:"timestamp"`
	Tag       string    `json:"tag,omitempty"` // Optional: ERROR, WARN, INFO, TRACE
	Count     int       `json:"count"`         // Number of results to return (default 1)
}

// Response wraps the query result.
type Response struct {
	Entries []EntryResult `json:"entries"`
	Total   int           `json:"total"`    // Number of entries returned
	QueryMs float64       `json:"query_ms"` // Query execution time in milliseconds
}

// EntryResult is a single log entry in the response.
type EntryResult struct {
	Timestamp string `json:"timestamp"`
	Tag       string `json:"tag"`
	Body      string `json:"body"`
	Distance  string `json:"distance"` // Human-readable distance from query timestamp
}

// Handler executes queries against an index.
type Handler struct {
	idx *index.Index
}

// NewHandler creates a query handler backed by the given index.
func NewHandler(idx *index.Index) *Handler {
	return &Handler{idx: idx}
}

// Execute runs a query and returns the result.
func (h *Handler) Execute(req Request) Response {
	start := time.Now()

	count := req.Count
	if count <= 0 {
		count = 1
	}
	if count > 100 {
		count = 100 // Cap to prevent abuse.
	}

	var entries []EntryResult

	if count == 1 {
		// Single result — use the faster Nearest path.
		entry, ok := h.idx.Nearest(req.Timestamp, req.Tag)
		if ok {
			entries = append(entries, toResult(entry, req.Timestamp))
		}
	} else {
		// Multiple results.
		results := h.idx.NearestN(req.Timestamp, req.Tag, count)
		for _, e := range results {
			entries = append(entries, toResult(e, req.Timestamp))
		}
	}

	elapsed := time.Since(start)

	return Response{
		Entries: entries,
		Total:   len(entries),
		QueryMs: float64(elapsed.Microseconds()) / 1000.0,
	}
}

// IndexSize returns the current size of the underlying index.
func (h *Handler) IndexSize() int {
	return h.idx.Size()
}

func toResult(e parser.LogEntry, queryTS time.Time) EntryResult {
	dist := e.Timestamp.Sub(queryTS)
	if dist < 0 {
		dist = -dist
	}

	return EntryResult{
		Timestamp: e.Timestamp.Format(parser.TimeLayout),
		Tag:       e.Tag,
		Body:      e.Body,
		Distance:  dist.String(),
	}
}
