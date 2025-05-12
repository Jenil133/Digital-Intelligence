package signal

import (
	"errors"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		s    Signal
		want error
	}{
		{
			name: "ok",
			s:    Signal{EventID: "e1", TenantID: "t1", SessionID: "s1", Timestamp: now},
		},
		{
			name: "missing event id",
			s:    Signal{TenantID: "t1", SessionID: "s1", Timestamp: now},
			want: ErrMissingEventID,
		},
		{
			name: "missing tenant",
			s:    Signal{EventID: "e1", SessionID: "s1", Timestamp: now},
			want: ErrMissingTenant,
		},
		{
			name: "missing session",
			s:    Signal{EventID: "e1", TenantID: "t1", Timestamp: now},
			want: ErrMissingSession,
		},
		{
			name: "future ts beyond skew",
			s:    Signal{EventID: "e1", TenantID: "t1", SessionID: "s1", Timestamp: now.Add(10 * time.Minute)},
			want: ErrFutureTimestamp,
		},
		{
			name: "future ts within skew is fine",
			s:    Signal{EventID: "e1", TenantID: "t1", SessionID: "s1", Timestamp: now.Add(2 * time.Minute)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.s.Validate(now)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
