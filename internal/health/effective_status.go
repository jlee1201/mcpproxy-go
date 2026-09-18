// Package health provides unified health status calculation for upstream MCP servers.
package health

import (
	"strings"
	"time"
)

// EffectiveStatusFreshnessWindow is how long a recorded success is
// considered current. Past this, a caller with no fresher signal must be
// told "unknown" rather than implicitly trusting a `status` field that
// never expires (design doc D1). Named constant, not configurable -- see
// implementation plan A3.
const EffectiveStatusFreshnessWindow = 15 * time.Minute

// EffectiveStatusInput is the pure, local input to DeriveEffectiveStatus.
// Every field is already computed elsewhere (StateView / StateManager, the
// persisted OAuth token record) -- this function adds no I/O and makes no
// upstream call, by design (design doc R3, "zero cost, all local").
type EffectiveStatusInput struct {
	// Connected mirrors today's cached `connected` field. When false, the
	// existing State string is returned unchanged -- effective_status only
	// adds information on top of a live connection.
	Connected bool

	// State is today's `status` string ("error", "connecting", "disabled",
	// etc.) for the not-connected branch of the table.
	State string

	// LastSuccessAt / LastAuthFailureAt come from
	// StateManager.LastSuccessAt() / LastAuthFailureAt() (A2). Zero Time
	// means "never, since process start."
	LastSuccessAt     time.Time
	LastAuthFailureAt time.Time

	// TokenExpiresAt is the persisted OAuth token's expiry, if any is known.
	// nil means no OAuth token / no expiry information -- NOT "valid
	// forever" (that conflation is D3.2, fixed separately in A4).
	TokenExpiresAt *time.Time
}

// DeriveEffectiveStatus computes a freshness-aware status alongside the
// existing (cached, never-expiring) `status` field. now is passed in
// explicitly so this function stays pure and unit-testable without a clock
// or a running runtime -- see docs/reports/mcpproxy-status-staleness-design.md
// R3 for the table this implements verbatim:
//
//	not connected, last_auth_failure_at > last_success_at      -> auth_expired
//	not connected, token expiry known and in the past          -> auth_expired
//	not connected (otherwise)                                  -> today's state, unchanged (unless it claims ready/connected -- see below)
//	connected, last_auth_failure_at > last_success_at          -> auth_expired
//	connected, token expiry known and in the past              -> auth_expired
//	connected, last_success_at older than the freshness window -> unknown
//	connected, recent success, token valid                     -> ready
//
// A zero LastSuccessAt (never called since process start) counts as stale,
// so it falls into "unknown", never "ready".
func DeriveEffectiveStatus(in EffectiveStatusInput, now time.Time) string {
	if !in.Connected {
		// 2026-08-14 bug fix: a fully disconnected client with a dead/expired
		// token was falling through to `return in.State` verbatim (almost
		// always "error"), never "auth_expired" -- even though the exact
		// same auth-failure/expiry signal is checked two branches down for
		// the Connected==true case. Since mcp-reauth.sh's flush-skip is
		// gated on seeing auth_expired, and disconnected-with-dead-token is
		// the most common daily-triage failure mode, the skip never
		// engaged. Check this first, before the ready/connected-lie
		// handling below: a stale token is a stronger, more specific signal
		// than "State claims a live connection with nothing to back it."
		if in.LastAuthFailureAt.After(in.LastSuccessAt) {
			return "auth_expired"
		}
		if in.TokenExpiresAt != nil && !in.TokenExpiresAt.IsZero() && in.TokenExpiresAt.Before(now) {
			return "auth_expired"
		}

		// dxgusto, the design doc's opening example: status="ready",
		// connected=false, in the same record (D1/D7 -- two writers, never
		// reconciled). Passing State through verbatim here would reproduce
		// that exact lie on effective_status too. A cached "ready"/
		// "connected" string with no live connection to back it has nothing
		// to justify a positive answer -- report unknown, not the stale
		// claim. Every other State value (error/connecting/disabled/etc.)
		// is unaffected and still passes through unchanged.
		if strings.EqualFold(in.State, "ready") || strings.EqualFold(in.State, "connected") {
			return "unknown"
		}
		return in.State
	}

	if in.LastAuthFailureAt.After(in.LastSuccessAt) {
		return "auth_expired"
	}

	if in.TokenExpiresAt != nil && !in.TokenExpiresAt.IsZero() && in.TokenExpiresAt.Before(now) {
		return "auth_expired"
	}

	if in.LastSuccessAt.IsZero() || now.Sub(in.LastSuccessAt) > EffectiveStatusFreshnessWindow {
		return "unknown"
	}

	return "ready"
}
