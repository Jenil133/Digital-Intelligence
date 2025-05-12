package ratelimit

import (
	"net/http"
	"sync"
	"time"

	"github.com/example/di-collector/internal/auth"
	"github.com/example/di-collector/internal/metrics"
)

type Config struct {
	RPS   float64
	Burst float64
}

// tokenBucket is a per-tenant bucket. Refill is computed lazily on access
// to avoid a background goroutine per tenant.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	updated  time.Time
	rps      float64
	capacity float64
}

func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := now.Sub(b.updated).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rps
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.updated = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type PerTenant struct {
	cfg     Config
	mu      sync.RWMutex
	buckets map[string]*tokenBucket
}

func NewPerTenant(cfg Config) *PerTenant {
	if cfg.RPS <= 0 {
		cfg.RPS = 1000
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.RPS * 2
	}
	return &PerTenant{cfg: cfg, buckets: make(map[string]*tokenBucket)}
}

func (p *PerTenant) bucket(tenant string) *tokenBucket {
	p.mu.RLock()
	b, ok := p.buckets[tenant]
	p.mu.RUnlock()
	if ok {
		return b
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if b, ok = p.buckets[tenant]; ok {
		return b
	}
	b = &tokenBucket{
		tokens:   p.cfg.Burst,
		updated:  time.Now(),
		rps:      p.cfg.RPS,
		capacity: p.cfg.Burst,
	}
	p.buckets[tenant] = b
	return b
}

func (p *PerTenant) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := auth.TenantFrom(r.Context())
		if !ok {
			http.Error(w, `{"error":"no tenant","code":"no_tenant"}`, http.StatusUnauthorized)
			return
		}
		if !p.bucket(tenant).allow(time.Now()) {
			metrics.RateLimitDropped.WithLabelValues(tenant).Inc()
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded","code":"rate_limited"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
