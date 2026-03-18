package parser

import (
	"testing"
	"time"
)

func TestParse_ValidLines(t *testing.T) {
	tests := []struct {
		name string
		line string
		want LogEntry
	}{
		{
			name: "error log",
			line: "2024-01-15T10:30:45.123Z [ERROR] request failed: connection timeout",
			want: LogEntry{
				Timestamp: time.Date(2024, 1, 15, 10, 30, 45, 123000000, time.UTC),
				Tag:       "ERROR",
				Body:      "request failed: connection timeout",
			},
		},
		{
			name: "info log",
			line: "2024-06-01T00:00:00.000Z [INFO] server started on port 8080",
			want: LogEntry{
				Timestamp: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				Tag:       "INFO",
				Body:      "server started on port 8080",
			},
		},
		{
			name: "warn log",
			line: "2024-12-31T23:59:59.999Z [WARN] disk usage above 90%",
			want: LogEntry{
				Timestamp: time.Date(2024, 12, 31, 23, 59, 59, 999000000, time.UTC),
				Tag:       "WARN",
				Body:      "disk usage above 90%",
			},
		},
		{
			name: "trace log",
			line: "2024-03-10T12:00:00.500Z [TRACE] entering function processOrder",
			want: LogEntry{
				Timestamp: time.Date(2024, 3, 10, 12, 0, 0, 500000000, time.UTC),
				Tag:       "TRACE",
				Body:      "entering function processOrder",
			},
		},
		{
			name: "body with spaces and special chars",
			line: "2024-01-01T00:00:00.000Z [ERROR] user_id=42 action=\"delete\" status=500 msg=\"internal server error\"",
			want: LogEntry{
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				Tag:       "ERROR",
				Body:      "user_id=42 action=\"delete\" status=500 msg=\"internal server error\"",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.line)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Timestamp.Equal(tt.want.Timestamp) {
				t.Errorf("timestamp: got %v, want %v", got.Timestamp, tt.want.Timestamp)
			}
			if got.Tag != tt.want.Tag {
				t.Errorf("tag: got %q, want %q", got.Tag, tt.want.Tag)
			}
			if got.Body != tt.want.Body {
				t.Errorf("body: got %q, want %q", got.Body, tt.want.Body)
			}
		})
	}
}

func TestParse_InvalidLines(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{name: "empty line", line: ""},
		{name: "no tag or body", line: "2024-01-15T10:30:45.123Z"},
		{name: "no body", line: "2024-01-15T10:30:45.123Z [ERROR]"},
		{name: "bad timestamp", line: "not-a-date [ERROR] something"},
		{name: "unbracketed tag", line: "2024-01-15T10:30:45.123Z ERROR something"},
		{name: "unknown tag", line: "2024-01-15T10:30:45.123Z [DEBUG] something"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.line)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	line := "2024-01-15T10:30:45.123Z [ERROR] request failed: connection timeout to downstream service"
	for i := 0; i < b.N; i++ {
		_, _ = Parse(line)
	}
}
