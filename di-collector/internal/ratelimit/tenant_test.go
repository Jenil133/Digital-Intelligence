package ratelimit

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	b := &tokenBucket{tokens: 3, updated: start, rps: 1, capacity: 3}

	for i := 0; i < 3; i++ {
		if !b.allow(start) {
			t.Fatalf("call %d: expected allow", i)
		}
	}
	if b.allow(start) {
		t.Fatal("4th call: expected deny (bucket empty)")
	}

	// One second later, one token refilled.
	if !b.allow(start.Add(time.Second)) {
		t.Fatal("after refill: expected allow")
	}
	if b.allow(start.Add(time.Second)) {
		t.Fatal("post-refill: expected deny")
	}
}

func TestPerTenantIsolation(t *testing.T) {
	p := NewPerTenant(Config{RPS: 1, Burst: 1})
	now := time.Now()
	if !p.bucket("a").allow(now) {
		t.Fatal("a first call should pass")
	}
	if p.bucket("a").allow(now) {
		t.Fatal("a second call should be limited")
	}
	if !p.bucket("b").allow(now) {
		t.Fatal("b should have its own bucket")
	}
}
