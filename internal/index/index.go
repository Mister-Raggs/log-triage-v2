// Package index provides a concurrent, time-ordered in-memory index of log entries.
//
// Design choices:
//   - Uses a sorted slice + binary search rather than a tree. At 42M entries/hour,
//     binary search over a contiguous slice is cache-friendly and gives O(log n) lookups
//     with minimal allocation overhead compared to a balanced BST.
//   - A RWMutex allows concurrent reads (queries) with exclusive writes (ingestion).
//   - The Nearest method returns the single closest entry by absolute time difference,
//     with optional tag filtering. This mirrors the original system's "find closest log
//     to incident timestamp" use case.
package index

import (
	"sort"
	"sync"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

// EvictionFunc is called after entries are evicted, with the count removed.
// Used to increment an external counter (e.g. Prometheus) without coupling
// the index to any specific metrics library.
type EvictionFunc func(evicted int)

// Index is a concurrency-safe, time-ordered collection of log entries.
type Index struct {
	mu        sync.RWMutex
	entries   []parser.LogEntry
	maxEntries int         // 0 = unlimited
	onEvict   EvictionFunc // nil = no callback
}

// New creates an empty Index with no eviction cap.
func New() *Index {
	return &Index{
		entries: make([]parser.LogEntry, 0, 1024),
	}
}

// NewCapped creates an Index that evicts the oldest entries (FIFO) once it
// exceeds maxEntries. onEvict is called with the number of entries removed;
// pass nil if you don't need the callback.
//
// FIFO is the natural eviction strategy for a time-ordered log index: the
// oldest entries are always at entries[0], so eviction is a cheap slice
// re-slice with no reordering needed.
func NewCapped(maxEntries int, onEvict EvictionFunc) *Index {
	return &Index{
		entries:    make([]parser.LogEntry, 0, min(maxEntries, 1024)),
		maxEntries: maxEntries,
		onEvict:    onEvict,
	}
}

// Insert adds a log entry to the index in sorted order.
// Uses binary search to find the insertion point so the slice stays sorted.
func (idx *Index) Insert(entry parser.LogEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Timestamp.After(entry.Timestamp)
	})

	idx.entries = append(idx.entries, parser.LogEntry{})
	copy(idx.entries[pos+1:], idx.entries[pos:])
	idx.entries[pos] = entry

	idx.evictLocked()
}

// InsertBatch adds multiple entries. It appends them all, then re-sorts once.
// This is significantly faster than individual Insert calls for bulk ingestion.
func (idx *Index) InsertBatch(entries []parser.LogEntry) {
	if len(entries) == 0 {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries = append(idx.entries, entries...)
	sort.Slice(idx.entries, func(i, j int) bool {
		return idx.entries[i].Timestamp.Before(idx.entries[j].Timestamp)
	})

	idx.evictLocked()
}

// evictLocked drops the oldest entries when the index exceeds its cap.
// Must be called with mu held for writing.
func (idx *Index) evictLocked() {
	if idx.maxEntries == 0 || len(idx.entries) <= idx.maxEntries {
		return
	}
	evicted := len(idx.entries) - idx.maxEntries
	// Entries are sorted by time; oldest are at the front — drop them.
	idx.entries = idx.entries[evicted:]
	if idx.onEvict != nil {
		idx.onEvict(evicted)
	}
}

// Nearest finds the log entry closest to the given timestamp.
// If tag is non-empty, only entries matching that tag are considered.
// Returns the entry and true if found, or a zero-value entry and false if
// the index is empty or no entries match the tag filter.
func (idx *Index) Nearest(ts time.Time, tag string) (parser.LogEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return parser.LogEntry{}, false
	}

	// If no tag filter, use fast path with direct binary search.
	if tag == "" {
		return idx.nearestUnfiltered(ts), true
	}

	return idx.nearestFiltered(ts, tag)
}

// NearestN returns up to n log entries closest to the given timestamp.
// Entries are returned sorted by distance (closest first).
func (idx *Index) NearestN(ts time.Time, tag string, n int) []parser.LogEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 || n <= 0 {
		return nil
	}

	// Find insertion point for the target timestamp.
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Timestamp.After(ts)
	})

	// Expand outward from pos, collecting the n closest entries.
	var results []parser.LogEntry
	left := pos - 1
	right := pos

	for len(results) < n && (left >= 0 || right < len(idx.entries)) {
		pickLeft := left >= 0
		pickRight := right < len(idx.entries)

		if pickLeft && pickRight {
			// Compare distances.
			distL := absDuration(idx.entries[left].Timestamp.Sub(ts))
			distR := absDuration(idx.entries[right].Timestamp.Sub(ts))
			if distL <= distR {
				if tag == "" || idx.entries[left].Tag == tag {
					results = append(results, idx.entries[left])
				}
				left--
			} else {
				if tag == "" || idx.entries[right].Tag == tag {
					results = append(results, idx.entries[right])
				}
				right++
			}
		} else if pickLeft {
			if tag == "" || idx.entries[left].Tag == tag {
				results = append(results, idx.entries[left])
			}
			left--
		} else {
			if tag == "" || idx.entries[right].Tag == tag {
				results = append(results, idx.entries[right])
			}
			right++
		}
	}

	return results
}

// Size returns the number of entries in the index.
func (idx *Index) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// nearestUnfiltered finds the closest entry by timestamp without tag filtering.
func (idx *Index) nearestUnfiltered(ts time.Time) parser.LogEntry {
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Timestamp.After(ts)
	})

	switch {
	case pos == 0:
		return idx.entries[0]
	case pos == len(idx.entries):
		return idx.entries[len(idx.entries)-1]
	default:
		before := idx.entries[pos-1]
		after := idx.entries[pos]
		if absDuration(ts.Sub(before.Timestamp)) <= absDuration(after.Timestamp.Sub(ts)) {
			return before
		}
		return after
	}
}

// nearestFiltered finds the closest entry matching the given tag.
// Falls back to linear scan over candidates near the binary search point.
func (idx *Index) nearestFiltered(ts time.Time, tag string) (parser.LogEntry, bool) {
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Timestamp.After(ts)
	})

	// Search outward from the insertion point in both directions.
	var best parser.LogEntry
	found := false
	bestDist := time.Duration(0)

	left := pos - 1
	right := pos

	for left >= 0 || right < len(idx.entries) {
		// Check left candidate.
		if left >= 0 {
			if idx.entries[left].Tag == tag {
				dist := absDuration(ts.Sub(idx.entries[left].Timestamp))
				if !found || dist < bestDist {
					best = idx.entries[left]
					bestDist = dist
					found = true
				}
				// Once we've found a match and the next candidate on this side
				// is farther away than our best, we can stop expanding left.
				if found && left > 0 {
					nextDist := absDuration(ts.Sub(idx.entries[left-1].Timestamp))
					if nextDist > bestDist {
						left = -1 // stop left expansion
					} else {
						left--
					}
				} else {
					left--
				}
			} else {
				left--
			}
		}

		// Check right candidate.
		if right < len(idx.entries) {
			if idx.entries[right].Tag == tag {
				dist := absDuration(idx.entries[right].Timestamp.Sub(ts))
				if !found || dist < bestDist {
					best = idx.entries[right]
					bestDist = dist
					found = true
				}
				if found && right < len(idx.entries)-1 {
					nextDist := absDuration(idx.entries[right+1].Timestamp.Sub(ts))
					if nextDist > bestDist {
						right = len(idx.entries) // stop right expansion
					} else {
						right++
					}
				} else {
					right++
				}
			} else {
				right++
			}
		}

		// Early termination: if both sides are farther than our best match, stop.
		if found {
			leftFarther := left < 0 || absDuration(ts.Sub(idx.entries[max(left, 0)].Timestamp)) > bestDist
			rightFarther := right >= len(idx.entries) || absDuration(idx.entries[min(right, len(idx.entries)-1)].Timestamp.Sub(ts)) > bestDist
			if leftFarther && rightFarther {
				break
			}
		}
	}

	return best, found
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
