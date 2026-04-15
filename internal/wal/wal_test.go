package wal

import (
	"os"
	"testing"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

var base = time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

func entry(offsetSec int, tag, body string) parser.LogEntry {
	return parser.LogEntry{
		Timestamp: base.Add(time.Duration(offsetSec) * time.Second),
		Tag:       tag,
		Body:      body,
	}
}

func TestWriteAndReplay(t *testing.T) {
	f, err := os.CreateTemp("", "wal-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []parser.LogEntry{
		entry(0, "INFO", "server started"),
		entry(1, "ERROR", "connection timeout"),
		entry(2, "WARN", "disk usage high"),
	}

	if err := w.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("replay: got %d entries, want %d", len(got), len(entries))
	}
	for i, want := range entries {
		if !got[i].Timestamp.Equal(want.Timestamp) {
			t.Errorf("[%d] timestamp: got %v, want %v", i, got[i].Timestamp, want.Timestamp)
		}
		if got[i].Tag != want.Tag {
			t.Errorf("[%d] tag: got %q, want %q", i, got[i].Tag, want.Tag)
		}
		if got[i].Body != want.Body {
			t.Errorf("[%d] body: got %q, want %q", i, got[i].Body, want.Body)
		}
	}
}

func TestReplay_NonExistentFile(t *testing.T) {
	// Should return nil, nil — fresh start with no WAL.
	entries, err := Replay("/tmp/does-not-exist-wal-test.log")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if entries != nil {
		t.Fatalf("expected nil entries, got %v", entries)
	}
}

func TestReplay_TruncatedLastLine(t *testing.T) {
	// Simulate a crash mid-write: write valid entries then append a truncated line.
	f, err := os.CreateTemp("", "wal-crash-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(entry(0, "INFO", "good entry")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a truncated line directly to simulate crash mid-write.
	// Cut off mid-timestamp so it fails parser.Parse.
	f2, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f2.WriteString("2024-01-15T10:00") // truncated mid-timestamp, no newline
	f2.Close()

	got, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	// Only the valid entry should be replayed; truncated line skipped.
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].Tag != "INFO" {
		t.Errorf("tag: got %q, want INFO", got[0].Tag)
	}
}

func TestAppend_SurvivesReopen(t *testing.T) {
	// Verify that re-opening the WAL appends rather than truncating.
	f, err := os.CreateTemp("", "wal-append-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	// First open: write one entry.
	w1, _ := Open(path)
	w1.Write(entry(0, "INFO", "first"))
	w1.Close()

	// Second open: write another entry.
	w2, _ := Open(path)
	w2.Write(entry(1, "WARN", "second"))
	w2.Close()

	got, _ := Replay(path)
	if len(got) != 2 {
		t.Fatalf("got %d entries after reopen, want 2", len(got))
	}
}
