// Package oauth provides OAuth authentication functionality for MCP servers.
package oauth

import (
	"errors"
	"strings"

	mcptransport "github.com/mark3labs/mcp-go/client/transport"
)

// ErrOAuthRequired is mcpproxy's own sentinel for "this call failed because
// there is no valid OAuth token." Code that originates an auth failure
// locally (rather than receiving one from mcp-go's transport) can wrap this
// with fmt.Errorf("...: %w", ErrOAuthRequired) to participate in
// IsAuthFailure without adding another string to any list.
var ErrOAuthRequired = errors.New("oauth authorization required")

// authFailureIndicators is the ONE string-matching fallback for auth-failure
// classification post-connect. It is deliberately narrower than
// containsOAuthError (status.go), which matches bare "token" and
// "authentication" -- fine for that function's job of choosing a display
// string, but wrong here: IsAuthFailure decides whether to flip a connected
// client into StateError via SetOAuthError, engaging 8e31b1e's no-retry
// backoff. A benign error that happens to mention "token" (e.g. a rate-limit
// message) must not take a healthy server down until someone runs
// `auth login`. Do not widen this list to match containsOAuthError's without
// re-checking that constraint.
var authFailureIndicators = []string{
	"no valid token available",
	"authorization required",
	"unauthorized (401)",
	"invalid_token",
	"invalid_grant",
	"missing or invalid access token",
}

// IsAuthFailure reports whether err represents an authentication failure --
// as opposed to a transport/connectivity failure -- observed on an
// established connection (post-connect CallTool / ListTools / tool-count
// fetch). This is the single place mcpproxy classifies auth failures for
// that purpose; do not duplicate this string list elsewhere.
//
// Connect-time auth requirements are classified separately by
// isOAuthAuthorizationRequired (internal/upstream/managed/client.go:829-845)
// -- that path is already correct and out of scope here.
//
// Known duplicate not yet consolidated: internal/upstream/manager.go:938-948
// (Manager.CallTool) re-derives an equivalent classification from the same
// raw error, but only to rewrite the error message for the caller -- it
// never touches StateManager, so the daemon still doesn't act on what it
// just figured out. That's a second copy of this list. Fold it into this
// function in a follow-up; manager.go is owned by a parallel change as of
// this writing and is intentionally left untouched.
func IsAuthFailure(err error) bool {
	if err == nil {
		return false
	}

	// Typed / sentinel checks first (errors.Is / errors.As), per the repo's
	// stated direction away from pure string matching.
	if errors.Is(err, ErrOAuthRequired) {
		return true
	}
	if errors.Is(err, mcptransport.ErrOAuthAuthorizationRequired) {
		return true
	}
	if errors.Is(err, mcptransport.ErrUnauthorized) {
		return true
	}
	var oauthRequiredErr *mcptransport.OAuthAuthorizationRequiredError
	if errors.As(err, &oauthRequiredErr) {
		return true
	}

	// String fallback for upstreams / wrapping layers that don't preserve a
	// typed error.
	return isAuthFailureString(err.Error())
}

func isAuthFailureString(errStr string) bool {
	lower := strings.ToLower(errStr)
	for _, indicator := range authFailureIndicators {
		if strings.Contains(lower, strings.ToLower(indicator)) {
			return true
		}
	}
	return false
}
