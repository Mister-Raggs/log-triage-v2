// Package parser handles parsing raw log lines into structured LogEntry values.
//
// Log format: "2024-01-15T10:30:45.123Z [ERROR] something went wrong"
// The parser is intentionally strict — malformed lines are rejected so callers
// can count parse failures as a signal of upstream format drift.
package parser

import (
	"fmt"
	"strings"
	"time"
)

// LogEntry represents a single parsed log line with a timestamp, severity tag,
// and the remaining message body.
type LogEntry struct {
	Timestamp time.Time
	Tag       string // One of: ERROR, WARN, INFO, TRACE
	Body      string
}

// TimeLayout is the timestamp format used in generated logs.
// ISO 8601 with millisecond precision and UTC timezone.
const TimeLayout = "2006-01-02T15:04:05.000Z"

// validTags is the set of recognized severity levels.
var validTags = map[string]bool{
	"ERROR": true,
	"WARN":  true,
	"INFO":  true,
	"TRACE": true,
}

// Parse takes a raw log line and returns a structured LogEntry.
// Returns an error if the line doesn't conform to the expected format.
func Parse(line string) (LogEntry, error) {
	if line == "" {
		return LogEntry{}, fmt.Errorf("empty log line")
	}

	// Split into at most 3 parts: timestamp, [TAG], body
	// Example: "2024-01-15T10:30:45.123Z [ERROR] request failed"
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return LogEntry{}, fmt.Errorf("malformed log line: expected 'TIMESTAMP [TAG] BODY', got %q", line)
	}

	// Parse timestamp
	ts, err := time.Parse(TimeLayout, parts[0])
	if err != nil {
		return LogEntry{}, fmt.Errorf("invalid timestamp %q: %w", parts[0], err)
	}

	// Parse tag — must be wrapped in brackets
	tag := parts[1]
	if !strings.HasPrefix(tag, "[") || !strings.HasSuffix(tag, "]") {
		return LogEntry{}, fmt.Errorf("invalid tag format %q: must be wrapped in brackets", tag)
	}
	tag = tag[1 : len(tag)-1] // Strip brackets

	if !validTags[tag] {
		return LogEntry{}, fmt.Errorf("unknown tag %q: must be one of ERROR, WARN, INFO, TRACE", tag)
	}

	return LogEntry{
		Timestamp: ts,
		Tag:       tag,
		Body:      parts[2],
	}, nil
}
