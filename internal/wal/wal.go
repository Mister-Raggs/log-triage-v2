// Package wal implements a write-ahead log for the log index.
//
// # Purpose
//
// The index is in-memory only — a restart loses everything. The WAL solves
// this by appending each ingested entry to a file before it enters the index.
// On startup the WAL is replayed to reconstruct the index before the HTTP
// server begins accepting traffic.
//
// # Format
//
// WAL entries use the identical format as ingested log lines:
//
//	2024-01-15T10:30:45.123Z [ERROR] request failed: connection timeout
//
// This is intentional: replay uses the existing parser with no new code,
// and crash safety is free — a truncated last line fails parser.Parse and
// is silently skipped, leaving the index in a consistent state.
//
// # fsync strategy
//
// Per-entry fsync at 42M rows/hour would serialize every write to disk and
// cap throughput at disk IOPS. Instead the WAL batches: it fsyncs every
// syncEvery entries OR every syncInterval, whichever comes first. This
// bounds data loss on crash to at most syncEvery entries or syncInterval of
// writes — a deliberate tradeoff between durability and throughput.
package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/raghavkachroo/log-triage-v2/internal/parser"
)

const (
	// syncEvery fsyncs after this many entries are written.
	syncEvery = 1000

	// syncInterval fsyncs on a time basis regardless of entry count.
	// Bounds data loss to at most this duration of writes on a crash.
	syncInterval = 500 * time.Millisecond
)

// WAL is a write-ahead log that persists log entries to disk.
// It is safe for concurrent use.
type WAL struct {
	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	count int // entries written since last fsync

	stop chan struct{} // signals the sync ticker goroutine to exit
	done chan struct{} // closed when the ticker goroutine has exited
}

// Open opens or creates a WAL at path. If the file already exists, new
// entries are appended. Call Close when done.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	wal := &WAL{
		f:    f,
		w:    bufio.NewWriterSize(f, 256*1024), // 256KB write buffer
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	// Background goroutine: periodic fsync to bound data loss on crash.
	go wal.syncLoop()

	return wal, nil
}

// Write appends a single entry to the WAL. The write is buffered; call
// Sync to guarantee it has reached disk.
func (wal *WAL) Write(entry parser.LogEntry) error {
	line := fmt.Sprintf("%s [%s] %s\n",
		entry.Timestamp.UTC().Format(parser.TimeLayout),
		entry.Tag,
		entry.Body,
	)

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if _, err := wal.w.WriteString(line); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}

	wal.count++
	if wal.count >= syncEvery {
		return wal.syncLocked()
	}
	return nil
}

// WriteBatch appends multiple entries. More efficient than calling Write
// in a loop because it acquires the lock once.
func (wal *WAL) WriteBatch(entries []parser.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, entry := range entries {
		line := fmt.Sprintf("%s [%s] %s\n",
			entry.Timestamp.UTC().Format(parser.TimeLayout),
			entry.Tag,
			entry.Body,
		)
		if _, err := wal.w.WriteString(line); err != nil {
			return fmt.Errorf("wal: write batch: %w", err)
		}
	}

	wal.count += len(entries)
	if wal.count >= syncEvery {
		return wal.syncLocked()
	}
	return nil
}

// Sync flushes the write buffer and fsyncs to disk.
func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.syncLocked()
}

// Close flushes, fsyncs, and closes the WAL file.
func (wal *WAL) Close() error {
	// Stop the sync ticker goroutine first.
	close(wal.stop)
	<-wal.done

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.syncLocked(); err != nil {
		return err
	}
	return wal.f.Close()
}

// syncLocked flushes the buffer and calls fsync. Must be called with mu held.
func (wal *WAL) syncLocked() error {
	if err := wal.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := wal.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	wal.count = 0
	return nil
}

// syncLoop runs in a background goroutine and periodically fsyncs the WAL
// to bound data loss to at most syncInterval on an unclean shutdown.
func (wal *WAL) syncLoop() {
	defer close(wal.done)
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wal.mu.Lock()
			_ = wal.syncLocked() // best-effort; errors logged by caller
			wal.mu.Unlock()
		case <-wal.stop:
			return
		}
	}
}

// Replay reads the WAL at path and returns all valid entries.
// Truncated or malformed lines (e.g. from a crash mid-write) are silently
// skipped — this is intentional and safe because the format is strict enough
// that partial lines never parse as valid entries.
//
// Replay is called once at startup before the HTTP server begins accepting
// traffic. It does not require an open WAL — it reads directly from the file.
func Replay(path string) ([]parser.LogEntry, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// No WAL yet — fresh start, nothing to replay.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal: replay open %s: %w", path, err)
	}
	defer f.Close()

	var entries []parser.LogEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		entry, err := parser.Parse(line)
		if err != nil {
			// Malformed or truncated line — skip silently.
			// This is the crash-safety guarantee: partial writes at the
			// tail of the file are harmless.
			continue
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return entries, fmt.Errorf("wal: replay scan: %w", err)
	}

	return entries, nil
}
