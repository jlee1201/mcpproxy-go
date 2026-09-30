package upstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/core"
)

// loginJoinTimeout bounds how long a login request waits on other in-flight
// flows. It leaves headroom under the API server's 120s WriteTimeout for the
// final start() (metadata check plus client start), or the response is dropped.
var loginJoinTimeout = 60 * time.Second

// maxLoginStartAttempts caps start() calls, so a lock re-grabbed by auto-reconnect
// right after each joined flow fails can't spin until the deadline.
const maxLoginStartAttempts = 3

// ErrLoginFlowStillInProgress means another sign-in for the server was still
// running when the login request gave up waiting on it. Retrying is safe.
var ErrLoginFlowStillInProgress = errors.New("oauth_flow_in_progress")

// startOrJoinLogin runs start; if another flow already holds the server's
// coordinator lock, it waits (within timeout overall) for that flow instead of
// failing. A joined flow that succeeds counts as this login's success. A joined
// flow that fails, or one that ended before we could observe its outcome, is
// followed by another start so the user gets a fresh flow, not a guessed result.
func startOrJoinLogin(
	coordinator *oauth.OAuthFlowCoordinator,
	serverName string,
	timeout time.Duration,
	start func() (*core.OAuthStartResult, error),
) (*core.OAuthStartResult, error) {
	deadline := time.Now().Add(timeout)
	for attempt := 1; ; attempt++ {
		result, err := start()
		if !errors.Is(err, oauth.ErrFlowInProgress) {
			return result, err
		}
		remaining := time.Until(deadline)
		if attempt >= maxLoginStartAttempts || remaining <= 0 {
			return result, fmt.Errorf("%w: another sign-in for %s is still running: %v", ErrLoginFlowStillInProgress, serverName, err)
		}

		switch coordinator.JoinFlow(context.Background(), serverName, remaining) {
		case oauth.JoinSucceeded:
			if result == nil {
				result = &core.OAuthStartResult{}
			}
			result.JoinedExistingFlow = true
			return result, nil
		case oauth.JoinTimedOut:
			return result, fmt.Errorf("%w: another sign-in for %s is still running after %s", ErrLoginFlowStillInProgress, serverName, timeout)
		}
	}
}

// storedTokenValid reports whether storage holds an unexpired access token for the server.
func (m *Manager) storedTokenValid(serverName, serverURL string) bool {
	if m.storage == nil {
		return false
	}
	token, err := m.storage.GetOAuthToken(oauth.GenerateServerKey(serverName, serverURL))
	if err != nil || token == nil || token.AccessToken == "" {
		return false
	}
	return token.ExpiresAt.IsZero() || token.ExpiresAt.After(time.Now())
}
