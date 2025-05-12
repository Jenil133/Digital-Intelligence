package adapter

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/example/riskos-adapter/internal/store"
)

type fakePub struct {
	mu   sync.Mutex
	msgs [][]byte
}

func (f *fakePub) Publish(_ context.Context, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, append([]byte(nil), body...))
	return nil
}
func (f *fakePub) Close() {}

type fakeStore struct{ rows []store.Row }

func (f *fakeStore) ReadSince(_ context.Context, since time.Time, limit int) ([]store.Row, error) {
	var out []store.Row
	for _, r := range f.rows {
		if r.WindowEnd.After(since) {
			out = append(out, r)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (f *fakeStore) Close() error { return nil }

func TestTickEmitsAndAdvancesCursor(t *testing.T) {
	t1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)

	src := &fakeStore{rows: []store.Row{
		{TenantID: "t1", SessionID: "s1", WindowEnd: t1, FeatureWindow: "session", Features: map[string]interface{}{"events_1m": 5}},
		{TenantID: "t1", SessionID: "s2", WindowEnd: t2, FeatureWindow: "session", Features: map[string]interface{}{"events_1m": 8}},
	}}
	pub := &fakePub{}
	a := New(src, pub, Config{
		BatchLimit:     100,
		CheckpointPath: filepath.Join(t.TempDir(), "cp"),
	})

	next, n, err := a.tick(context.Background(), t1.Add(-time.Hour))
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n != 2 {
		t.Fatalf("emitted %d, want 2", n)
	}
	if !next.Equal(t2) {
		t.Fatalf("cursor advanced to %v, want %v", next, t2)
	}

	var p DecisionPayload
	if err := json.Unmarshal(pub.msgs[0], &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.TenantID != "t1" || p.SchemaVersion == "" {
		t.Fatalf("payload mismatch: %+v", p)
	}
}
