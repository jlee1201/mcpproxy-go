package upstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/core"
)

// loginJoinTimeout bounds how long a login request waits on another in-flight
// flow; it must stay under the API server's 120s WriteTimeout or the response is dropped.
var loginJoinTimeout = 90 * time.Second

// ErrLoginFlowStillInProgress means another sign-in for the server was still
// running when the login request gave up waiting on it. Retrying is safe.
var ErrLoginFlowStillInProgress = errors.New("oauth_flow_in_progress")

// startOrJoinLogin runs start; if another flow already holds the server's
// coordinator lock, it waits (up to timeout) for that flow instead of failing.
// A joined flow that leaves a valid token counts as this login's success;
// otherwise start runs once more to open a fresh flow.
func startOrJoinLogin(
	coordinator *oauth.OAuthFlowCoordinator,
	serverName string,
	timeout time.Duration,
	start func() (*core.OAuthStartResult, error),
	tokenValid func() bool,
) (*core.OAuthStartResult, error) {
	result, err := start()
	if !errors.Is(err, oauth.ErrFlowInProgress) {
		return result, err
	}

	// nil is ambiguous here: the flow succeeded, or it ended before we registered
	// as a waiter. Either way the stored token is what decides.
	waitErr := coordinator.WaitForFlow(context.Background(), serverName, timeout)
	if errors.Is(waitErr, oauth.ErrFlowTimeout) && coordinator.IsFlowActive(serverName) {
		return result, fmt.Errorf("%w: another sign-in for %s is still running after %s", ErrLoginFlowStillInProgress, serverName, timeout)
	}
	if waitErr == nil && tokenValid() {
		if result == nil {
			result = &core.OAuthStartResult{}
		}
		result.JoinedExistingFlow = true
		return result, nil
	}

	result, err = start()
	if errors.Is(err, oauth.ErrFlowInProgress) {
		return result, fmt.Errorf("%w: %v", ErrLoginFlowStillInProgress, err)
	}
	return result, err
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
