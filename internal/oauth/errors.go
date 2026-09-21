// Package oauth provides OAuth authentication functionality for MCP servers.
package oauth

import "errors"

// OAuth-specific sentinel errors for consistent error handling across the codebase.
// Note: ErrFlowInProgress and ErrFlowTimeout are defined in coordinator.go
var (
	// ErrServerNotOAuth indicates server doesn't use OAuth authentication.
	// This is returned when attempting OAuth operations on a non-OAuth server.
	ErrServerNotOAuth = errors.New("server does not use OAuth")

	// ErrTokenExpired indicates OAuth token has expired.
	// This triggers token refresh or re-authentication flow.
	ErrTokenExpired = errors.New("OAuth token has expired")

	// ErrRefreshFailed indicates token refresh failed after all retry attempts.
	// This typically requires manual re-authentication via browser.
	ErrRefreshFailed = errors.New("OAuth token refresh failed")

	// ErrNoRefreshToken indicates refresh token is not available.
	// Some OAuth providers don't issue refresh tokens.
	ErrNoRefreshToken = errors.New("no refresh token available")

	// ErrPendingInteractiveLogin indicates a proactive refresh was skipped
	// because the server is busy (already connecting/authenticating/
	// discovering) or parked awaiting a user-driven interactive OAuth login --
	// see client.GetState().IsBusyOrParked(). Despite the name, this covers
	// the whole busy-or-parked set, not just StatePendingAuth: none of those
	// states can usefully be refreshed right now, and none should be
	// classified as a refresh failure. Callers should reschedule a recheck
	// rather than treating this as a failure (it must never increment
	// RetryCount or reach the Failed state — the wait has no bound the
	// retry/backoff machinery can reason about). Wrapped with %w so
	// classifyRefreshError and any other caller can match it via errors.Is
	// instead of string-matching the message, which broke silently whenever
	// the message text was reworded.
	ErrPendingInteractiveLogin = errors.New("server is busy or parked pending interactive login, refresh skipped")
)
