// Package store reads feature rows from the feature store.
//
// The interface is intentionally narrow: ReadSince returns rows whose
// window_end strictly exceeds the cursor. Concrete implementations can back
// onto Delta, Iceberg, or a thin REST gateway in front of either.
//
// For Phase 3 we ship a `file://` driver that reads JSON-lines exports —
// enough for the e2e harness and a placeholder until the production driver
// (querying Delta via spark-thrift or a Trino gateway) is selected.
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Row struct {
	TenantID      string                 `json:"tenant_id"`
	SessionID     string                 `json:"session_id"`
	WindowEnd     time.Time              `json:"window_end"`
	FeatureWindow string                 `json:"feature_window"`
	Features      map[string]interface{} `json:"features"`
}

type Reader interface {
	ReadSince(ctx context.Context, since time.Time, limit int) ([]Row, error)
	Close() error
}

func Open(dsn string) (Reader, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	switch u.Scheme {
	case "file", "":
		return &fileReader{root: u.Path}, nil
	default:
		return nil, fmt.Errorf("unsupported store scheme %q (file:// supported; delta/iceberg pending)", u.Scheme)
	}
}

type fileReader struct{ root string }

func (f *fileReader) Close() error { return nil }

func (f *fileReader) ReadSince(_ context.Context, since time.Time, limit int) ([]Row, error) {
	var rows []Row
	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".jsonl") && !strings.HasSuffix(path, ".json") {
			return nil
		}
		fh, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fh.Close()
		s := bufio.NewScanner(fh)
		s.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for s.Scan() {
			line := s.Bytes()
			if len(line) == 0 {
				continue
			}
			var r Row
			if err := json.Unmarshal(line, &r); err != nil {
				continue
			}
			if r.WindowEnd.After(since) {
				rows = append(rows, r)
			}
		}
		return s.Err()
	}
	if err := filepath.Walk(f.root, walk); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].WindowEnd.Before(rows[j].WindowEnd) })
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}
