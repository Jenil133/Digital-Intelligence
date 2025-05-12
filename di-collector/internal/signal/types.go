package signal

import (
	"errors"
	"time"
)

type DeviceTelemetry struct {
	OS          string  `json:"os"`
	OSVersion   string  `json:"os_version"`
	IP          string  `json:"ip"`
	Fingerprint string  `json:"fingerprint"`
	Battery     float64 `json:"battery,omitempty"`
	GeoLat      float64 `json:"geo_lat,omitempty"`
	GeoLon      float64 `json:"geo_lon,omitempty"`
}

type BehavioralEvent struct {
	Kind       string  `json:"kind"`
	DwellMs    int64   `json:"dwell_ms,omitempty"`
	TypingCPS  float64 `json:"typing_cps,omitempty"`
	MouseEntr  float64 `json:"mouse_entropy,omitempty"`
	TargetElem string  `json:"target_elem,omitempty"`
}

type Signal struct {
	EventID    string            `json:"event_id"`
	TenantID   string            `json:"tenant_id"`
	SessionID  string            `json:"session_id"`
	Timestamp  time.Time         `json:"ts"`
	Device     DeviceTelemetry   `json:"device"`
	Behavioral []BehavioralEvent `json:"behavioral"`
}

var (
	ErrMissingEventID  = errors.New("event_id is required")
	ErrMissingTenant   = errors.New("tenant_id is required")
	ErrMissingSession  = errors.New("session_id is required")
	ErrFutureTimestamp = errors.New("timestamp is in the future")
)

const MaxClockSkew = 5 * time.Minute

func (s *Signal) Validate(now time.Time) error {
	if s.EventID == "" {
		return ErrMissingEventID
	}
	if s.TenantID == "" {
		return ErrMissingTenant
	}
	if s.SessionID == "" {
		return ErrMissingSession
	}
	if s.Timestamp.After(now.Add(MaxClockSkew)) {
		return ErrFutureTimestamp
	}
	return nil
}
