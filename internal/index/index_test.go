package index

import (
	"fmt"
	"testing"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

var baseTime = time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

func entry(offsetSec int, tag string) parser.LogEntry {
	return parser.LogEntry{
		Timestamp: baseTime.Add(time.Duration(offsetSec) * time.Second),
		Tag:       tag,
		Body:      fmt.Sprintf("log at +%ds", offsetSec),
	}
}

func TestInsertAndSize(t *testing.T) {
	idx := New()
	if idx.Size() != 0 {
		t.Fatal("new index should be empty")
	}

	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(5, "ERROR"))
	idx.Insert(entry(20, "WARN"))

	if idx.Size() != 3 {
		t.Fatalf("size: got %d, want 3", idx.Size())
	}
}

func TestInsertMaintainsSortOrder(t *testing.T) {
	idx := New()
	// Insert out of order.
	idx.Insert(entry(30, "INFO"))
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "INFO"))

	// Verify internal order.
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for i := 1; i < len(idx.entries); i++ {
		if idx.entries[i].Timestamp.Before(idx.entries[i-1].Timestamp) {
			t.Errorf("entries not sorted: [%d]=%v > [%d]=%v",
				i-1, idx.entries[i-1].Timestamp, i, idx.entries[i].Timestamp)
		}
	}
}

func TestInsertBatch(t *testing.T) {
	idx := New()
	batch := []parser.LogEntry{
		entry(30, "INFO"),
		entry(10, "ERROR"),
		entry(20, "WARN"),
		entry(5, "TRACE"),
	}
	idx.InsertBatch(batch)

	if idx.Size() != 4 {
		t.Fatalf("size: got %d, want 4", idx.Size())
	}

	// Verify sorted order.
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for i := 1; i < len(idx.entries); i++ {
		if idx.entries[i].Timestamp.Before(idx.entries[i-1].Timestamp) {
			t.Errorf("batch entries not sorted at index %d", i)
		}
	}
}

func TestNearest_ExactMatch(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "ERROR"))
	idx.Insert(entry(30, "WARN"))

	got, ok := idx.Nearest(baseTime.Add(20*time.Second), "")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.Tag != "ERROR" {
		t.Errorf("tag: got %q, want ERROR", got.Tag)
	}
}

func TestNearest_CloserToBefore(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "ERROR"))

	// Query at +14s — closer to +10s.
	got, ok := idx.Nearest(baseTime.Add(14*time.Second), "")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if !got.Timestamp.Equal(baseTime.Add(10 * time.Second)) {
		t.Errorf("got %v, want entry at +10s", got.Timestamp)
	}
}

func TestNearest_CloserToAfter(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "ERROR"))

	// Query at +16s — closer to +20s.
	got, ok := idx.Nearest(baseTime.Add(16*time.Second), "")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if !got.Timestamp.Equal(baseTime.Add(20 * time.Second)) {
		t.Errorf("got %v, want entry at +20s", got.Timestamp)
	}
}

func TestNearest_WithTagFilter(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "ERROR"))
	idx.Insert(entry(30, "INFO"))
	idx.Insert(entry(40, "ERROR"))

	// Query at +25s with tag=ERROR — should get +20s (closer than +40s).
	got, ok := idx.Nearest(baseTime.Add(25*time.Second), "ERROR")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.Tag != "ERROR" {
		t.Errorf("tag: got %q, want ERROR", got.Tag)
	}
	if !got.Timestamp.Equal(baseTime.Add(20 * time.Second)) {
		t.Errorf("got %v, want entry at +20s", got.Timestamp)
	}
}

func TestNearest_TagFilterNoMatch(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "INFO"))

	_, ok := idx.Nearest(baseTime.Add(15*time.Second), "ERROR")
	if ok {
		t.Fatal("expected no match for ERROR tag in INFO-only index")
	}
}

func TestNearest_EmptyIndex(t *testing.T) {
	idx := New()
	_, ok := idx.Nearest(baseTime, "")
	if ok {
		t.Fatal("expected no match in empty index")
	}
}

func TestNearest_BeforeAllEntries(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "INFO"))

	got, ok := idx.Nearest(baseTime, "") // Before all entries
	if !ok {
		t.Fatal("expected to find entry")
	}
	if !got.Timestamp.Equal(baseTime.Add(10 * time.Second)) {
		t.Errorf("got %v, want first entry", got.Timestamp)
	}
}

func TestNearest_AfterAllEntries(t *testing.T) {
	idx := New()
	idx.Insert(entry(10, "INFO"))
	idx.Insert(entry(20, "INFO"))

	got, ok := idx.Nearest(baseTime.Add(100*time.Second), "")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if !got.Timestamp.Equal(baseTime.Add(20 * time.Second)) {
		t.Errorf("got %v, want last entry", got.Timestamp)
	}
}

func TestNearestN(t *testing.T) {
	idx := New()
	for i := 0; i < 10; i++ {
		idx.Insert(entry(i*10, "INFO"))
	}

	results := idx.NearestN(baseTime.Add(35*time.Second), "", 3)
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	// Should be +30s, +40s, +20s (sorted by distance).
	expected := []int{30, 40, 20}
	for i, exp := range expected {
		want := baseTime.Add(time.Duration(exp) * time.Second)
		if !results[i].Timestamp.Equal(want) {
			t.Errorf("result[%d]: got %v, want %v", i, results[i].Timestamp, want)
		}
	}
}

func TestEviction_CapEnforced(t *testing.T) {
	evicted := 0
	idx := NewCapped(5, func(n int) { evicted += n })

	for i := 0; i < 10; i++ {
		idx.Insert(entry(i*10, "INFO"))
	}

	if idx.Size() != 5 {
		t.Fatalf("size: got %d, want 5", idx.Size())
	}
	if evicted != 5 {
		t.Fatalf("evicted: got %d, want 5", evicted)
	}

	// Oldest entries should be gone — index should start at +50s.
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	want := baseTime.Add(50 * time.Second)
	if !idx.entries[0].Timestamp.Equal(want) {
		t.Errorf("oldest entry: got %v, want %v", idx.entries[0].Timestamp, want)
	}
}

func TestEviction_BatchCapEnforced(t *testing.T) {
	evicted := 0
	idx := NewCapped(3, func(n int) { evicted += n })

	batch := []parser.LogEntry{
		entry(10, "INFO"),
		entry(20, "INFO"),
		entry(30, "INFO"),
		entry(40, "INFO"),
		entry(50, "INFO"),
	}
	idx.InsertBatch(batch)

	if idx.Size() != 3 {
		t.Fatalf("size: got %d, want 3", idx.Size())
	}
	if evicted != 2 {
		t.Fatalf("evicted: got %d, want 2", evicted)
	}
}

func TestEviction_NilCallback(t *testing.T) {
	// Should not panic when onEvict is nil.
	idx := NewCapped(2, nil)
	idx.Insert(entry(1, "INFO"))
	idx.Insert(entry(2, "INFO"))
	idx.Insert(entry(3, "INFO")) // triggers eviction, nil callback
	if idx.Size() != 2 {
		t.Fatalf("size: got %d, want 2", idx.Size())
	}
}

func BenchmarkInsert(b *testing.B) {
	idx := New()
	for i := 0; i < b.N; i++ {
		idx.Insert(entry(i, "INFO"))
	}
}

func BenchmarkNearest(b *testing.B) {
	idx := New()
	// Pre-populate with 100k entries.
	batch := make([]parser.LogEntry, 100000)
	for i := range batch {
		batch[i] = entry(i, "INFO")
	}
	idx.InsertBatch(batch)

	target := baseTime.Add(50000 * time.Second) // middle of the range
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Nearest(target, "")
	}
}
