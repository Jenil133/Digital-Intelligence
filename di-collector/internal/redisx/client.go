package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/example/di-collector/internal/signal"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func New(url string) (*Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return &Client{rdb: redis.NewClient(opt)}, nil
}

func (c *Client) Close() error { return c.rdb.Close() }

const (
	streamMaxLen = 1_000_000
	dedupTTL     = 24 * time.Hour
)

// Publish writes a signal to the per-tenant stream and dedupes by event_id.
// Returns (true, nil) on first write, (false, nil) when the event_id was
// already seen within dedupTTL.
func (c *Client) Publish(ctx context.Context, s *signal.Signal) (bool, error) {
	dedupKey := fmt.Sprintf("di:dedup:%s:%s", s.TenantID, s.EventID)
	ok, err := c.rdb.SetNX(ctx, dedupKey, 1, dedupTTL).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: %w", err)
	}
	if !ok {
		return false, nil
	}

	payload, err := json.Marshal(s)
	if err != nil {
		return false, fmt.Errorf("marshal: %w", err)
	}

	stream := fmt.Sprintf("di:signals:%s", s.TenantID)
	_, err = c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]interface{}{"data": payload},
	}).Result()
	if err != nil {
		return false, fmt.Errorf("xadd: %w", err)
	}
	return true, nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}
