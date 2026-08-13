package oauth

import (
	"errors"
	"fmt"
	"testing"

	mcptransport "github.com/mark3labs/mcp-go/client/transport"
	"github.com/stretchr/testify/assert"
)

// TestIsAuthFailure_TypedSentinel exercises the wrapped-sentinel path exactly
// as production builds it: mcp-go's OAuthAuthorizationRequiredError, wrapped
// by transport.Error, wrapped again by mcpproxy's core.Client.CallTool. This
// is the live error observed on dxgusto:
//
//	CallTool failed for 'echo': transport error: failed to send request: no valid token available, authorization required
func TestIsAuthFailure_TypedSentinel(t *testing.T) {
	inner := &mcptransport.OAuthAuthorizationRequiredError{}
	wrapped := mcptransport.NewError(fmt.Errorf("failed to send request: %w", inner))
	err := fmt.Errorf("CallTool failed for '%s': %w", "echo", wrapped)

	assert.True(t, IsAuthFailure(err), "wrapped OAuthAuthorizationRequiredError must be detected via errors.Is/As, not just string matching")
	assert.Contains(t, err.Error(), "no valid token available, authorization required")
}

// TestIsAuthFailure_StringFallback covers upstreams / transports that don't
// preserve a typed error, only the string. Verified against a live upstream.
func TestIsAuthFailure_StringFallback(t *testing.T) {
	err := errors.New("transport error: failed to send request: no valid token available, authorization required")
	assert.True(t, IsAuthFailure(err))
}

func TestIsAuthFailure_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"no valid token available", errors.New("no valid token available, authorization required"), true},
		{"authorization required", errors.New("OAuth authorization required for slack"), true},
		{"unauthorized 401", errors.New("unauthorized (401)"), true},
		{"invalid_token", errors.New("oauth error: invalid_token"), true},
		{"invalid_grant", errors.New("token refresh failed: invalid_grant"), true},
		{"missing access token", errors.New("Missing or invalid access token"), true},
		{"mcp-go sentinel directly", mcptransport.ErrOAuthAuthorizationRequired, true},
		{"mcp-go ErrUnauthorized", mcptransport.ErrUnauthorized, true},
		{"mcpproxy sentinel", ErrOAuthRequired, true},
		{"wrapped mcpproxy sentinel", fmt.Errorf("connect: %w", ErrOAuthRequired), true},

		// Must NOT be classified as auth failures -- these are transport/
		// connectivity errors that belong to isConnectionError instead, and a
		// false positive here would take a healthy server down via
		// SetOAuthError until someone runs `auth login`.
		{"connection refused", errors.New("connection refused"), false},
		{"context canceled", errors.New("context canceled"), false},
		{"terminated", errors.New("TypeError: terminated"), false},
		{"broken pipe", errors.New("write: broken pipe"), false},
		// Deliberately narrower than oauth.IsOAuthError: bare "token" or
		// "authentication" must NOT trip this classifier, or a benign error
		// that happens to mention a token (e.g. a rate-limit message) would
		// wrongly flip a working server into the no-retry OAuth-error gate.
		{"bare token mention", errors.New("rate limit token bucket exceeded"), false},
		{"bare authentication mention", errors.New("authentication service unavailable"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsAuthFailure(tt.err))
		})
	}
}
