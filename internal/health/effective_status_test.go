package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDeriveEffectiveStatus_Table covers every row of the design doc's R3 /
// implementation plan A3 table. now is passed explicitly so the freshness
// window is testable without sleeping and the function stays pure -- no
// clock, no I/O, no upstream call.
func TestDeriveEffectiveStatus_Table(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-1 * time.Minute)
	stale := now.Add(-20 * time.Minute) // older than the 15m freshness window
	pastExpiry := now.Add(-1 * time.Hour)
	futureExpiry := now.Add(1 * time.Hour)

	tests := []struct {
		name string
		in   EffectiveStatusInput
		want string
	}{
		{
			name: "not connected reports the existing status verbatim",
			in:   EffectiveStatusInput{Connected: false, State: "error"},
			want: "error",
		},
		{
			name: "not connected, connecting",
			in:   EffectiveStatusInput{Connected: false, State: "connecting"},
			want: "connecting",
		},
		{
			name: "not connected, disabled",
			in:   EffectiveStatusInput{Connected: false, State: "disabled"},
			want: "disabled",
		},
		{
			// The design doc's opening example, verbatim: dxgusto reported
			// status="ready" with connected=false in the same record (D1/D7,
			// two StateView writers never reconciled). Passing State through
			// here would reproduce that exact lie on effective_status.
			name: "not connected but State claims ready -> unknown, not the stale lie",
			in:   EffectiveStatusInput{Connected: false, State: "ready"},
			want: "unknown",
		},
		{
			name: "not connected but State claims connected -> unknown",
			in:   EffectiveStatusInput{Connected: false, State: "connected"},
			want: "unknown",
		},
		{
			name: "not connected, State claims Ready with different casing -> unknown",
			in:   EffectiveStatusInput{Connected: false, State: "Ready"},
			want: "unknown",
		},
		{
			// The 2026-08-14 bug: a fully disconnected client with a dead
			// token was falling through to `return in.State` verbatim
			// ("error"), never "auth_expired" -- so mcp-reauth.sh's
			// flush-skip (gated on seeing auth_expired) never engaged for
			// the most common daily-triage failure mode.
			name: "not connected, auth failure after success -> auth_expired, not a verbatim State pass-through",
			in: EffectiveStatusInput{
				Connected:         false,
				State:             "error",
				LastSuccessAt:     pastExpiry,
				LastAuthFailureAt: now,
			},
			want: "auth_expired",
		},
		{
			name: "not connected, token expiry known and in the past -> auth_expired",
			in: EffectiveStatusInput{
				Connected:      false,
				State:          "error",
				TokenExpiresAt: &pastExpiry,
			},
			want: "auth_expired",
		},
		{
			name: "not connected, token expiry known and in the future -> State passes through unchanged",
			in: EffectiveStatusInput{
				Connected:      false,
				State:          "error",
				TokenExpiresAt: &futureExpiry,
			},
			want: "error",
		},
		{
			// auth_expired must win over the ready/connected-lie handling:
			// a stale token is a stronger, more specific signal than "State
			// claims a live connection with nothing to back it."
			name: "not connected, State claims ready AND token expired -> auth_expired takes priority over unknown",
			in: EffectiveStatusInput{
				Connected:      false,
				State:          "ready",
				TokenExpiresAt: &pastExpiry,
			},
			want: "auth_expired",
		},
		{
			name: "connected, auth failure after success -> auth_expired (the zombie)",
			in: EffectiveStatusInput{
				Connected:         true,
				State:             "ready",
				LastSuccessAt:     fresh,
				LastAuthFailureAt: now, // after LastSuccessAt
			},
			want: "auth_expired",
		},
		{
			name: "connected, token expiry known and in the past -> auth_expired",
			in: EffectiveStatusInput{
				Connected:      true,
				State:          "ready",
				LastSuccessAt:  fresh,
				TokenExpiresAt: &pastExpiry,
			},
			want: "auth_expired",
		},
		{
			name: "connected, last_success_at older than freshness window -> unknown",
			in: EffectiveStatusInput{
				Connected:     true,
				State:         "ready",
				LastSuccessAt: stale,
			},
			want: "unknown",
		},
		{
			name: "connected, zero last_success_at (never called) -> unknown, NOT ready",
			in: EffectiveStatusInput{
				Connected: true,
				State:     "ready",
			},
			want: "unknown",
		},
		{
			name: "connected, recent success, token valid -> ready",
			in: EffectiveStatusInput{
				Connected:      true,
				State:          "ready",
				LastSuccessAt:  fresh,
				TokenExpiresAt: &futureExpiry,
			},
			want: "ready",
		},
		{
			name: "connected, recent success, no OAuth token at all -> ready",
			in: EffectiveStatusInput{
				Connected:     true,
				State:         "ready",
				LastSuccessAt: fresh,
			},
			want: "ready",
		},
		{
			name: "connected, auth failure exactly equal to success time is NOT after -> falls through to freshness",
			in: EffectiveStatusInput{
				Connected:         true,
				State:             "ready",
				LastSuccessAt:     fresh,
				LastAuthFailureAt: fresh,
			},
			want: "ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeriveEffectiveStatus(tt.in, now)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEffectiveStatusFreshnessWindow_Is15Minutes(t *testing.T) {
	assert.Equal(t, 15*time.Minute, EffectiveStatusFreshnessWindow)
}
