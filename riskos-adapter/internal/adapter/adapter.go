package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/example/riskos-adapter/internal/bus"
	"github.com/example/riskos-adapter/internal/store"
)

type Config struct {
	FeatureStoreDSN string
	BusURL          string
	BusSubject      string
	PollInterval    time.Duration
	BatchLimit      int
	CheckpointPath  string
}

func ConfigFromEnv() Config {
	return Config{
		FeatureStoreDSN: envOr("FEATURE_STORE_DSN", "file:///tmp/features"),
		BusURL:          envOr("BUS_URL", "nats://localhost:4222"),
		BusSubject:      envOr("BUS_SUBJECT", "riskos.di.features"),
		PollInterval:    envDuration("POLL_INTERVAL", 2*time.Second),
		BatchLimit:      envInt("BATCH_LIMIT", 1000),
		CheckpointPath:  envOr("CHECKPOINT_PATH", "/var/lib/riskos-adapter/checkpoint"),
	}
}

type DecisionPayload struct {
	TenantID      string                 `json:"tenant_id"`
	SessionID     string                 `json:"session_id"`
	WindowEnd     time.Time              `json:"window_end"`
	FeatureWindow string                 `json:"feature_window"`
	Features      map[string]interface{} `json:"features"`
	SchemaVersion string                 `json:"schema_version"`
	EmittedAt     time.Time              `json:"emitted_at"`
}

type Adapter struct {
	src store.Reader
	pub bus.Publisher
	cfg Config
}

func New(src store.Reader, pub bus.Publisher, cfg Config) *Adapter {
	return &Adapter{src: src, pub: pub, cfg: cfg}
}

func (a *Adapter) Run(ctx context.Context) error {
	cursor, err := a.loadCheckpoint()
	if err != nil {
		slog.Warn("checkpoint missing — starting from now", "err", err)
		cursor = time.Now().UTC()
	}
	t := time.NewTicker(a.cfg.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			next, n, err := a.tick(ctx, cursor)
			if err != nil {
				slog.Error("tick failed", "err", err, "cursor", cursor)
				continue
			}
			if n > 0 {
				slog.Info("emitted", "rows", n, "cursor_from", cursor, "cursor_to", next)
				cursor = next
				if err := a.saveCheckpoint(cursor); err != nil {
					slog.Error("checkpoint save", "err", err)
				}
			}
		}
	}
}

func (a *Adapter) tick(ctx context.Context, since time.Time) (time.Time, int, error) {
	rows, err := a.src.ReadSince(ctx, since, a.cfg.BatchLimit)
	if err != nil {
		return since, 0, fmt.Errorf("read: %w", err)
	}
	maxTs := since
	for _, r := range rows {
		body, err := json.Marshal(DecisionPayload{
			TenantID:      r.TenantID,
			SessionID:     r.SessionID,
			WindowEnd:     r.WindowEnd,
			FeatureWindow: r.FeatureWindow,
			Features:      r.Features,
			SchemaVersion: "1.0.0",
			EmittedAt:     time.Now().UTC(),
		})
		if err != nil {
			return maxTs, 0, fmt.Errorf("marshal: %w", err)
		}
		if err := a.pub.Publish(ctx, body); err != nil {
			return maxTs, 0, fmt.Errorf("publish: %w", err)
		}
		if r.WindowEnd.After(maxTs) {
			maxTs = r.WindowEnd
		}
	}
	return maxTs, len(rows), nil
}

func (a *Adapter) loadCheckpoint() (time.Time, error) {
	b, err := os.ReadFile(a.cfg.CheckpointPath)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, string(b))
}

func (a *Adapter) saveCheckpoint(t time.Time) error {
	if err := os.MkdirAll(dir(a.cfg.CheckpointPath), 0o755); err != nil {
		return err
	}
	tmp := a.cfg.CheckpointPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(t.UTC().Format(time.RFC3339Nano)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, a.cfg.CheckpointPath)
}

func dir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if dur, err := time.ParseDuration(v); err == nil {
			return dur
		}
	}
	return d
}
