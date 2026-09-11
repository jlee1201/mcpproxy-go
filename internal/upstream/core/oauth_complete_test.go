package core

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newTestOAuthClient builds a minimal, network-free *Client for exercising
// markOAuthComplete. serverName must be unique per test - the callback
// server manager and token store manager it touches are process-wide
// singletons keyed by this name.
func newTestOAuthClient(t *testing.T, serverName string) *Client {
	t.Helper()
	c, err := NewClientWithOptions(
		serverName,
		&config.ServerConfig{Name: serverName, Protocol: "streamable-http", URL: "https://example.com/mcp"},
		zap.NewNop(),
		nil,  // logConfig
		nil,  // globalConfig
		nil,  // storage - forces markOAuthComplete's in-memory (non-DB) completion path
		false,
		nil, // secretResolver
	)
	require.NoError(t, err)
	return c
}

// TestMarkOAuthComplete_TearsDownWhenNoSiblingFlow is the single-flow case:
// the flow that completes is the ONLY registered waiter. markOAuthComplete
// must release its own state before asking the callback manager to tear the
// server down, otherwise the still-registered "own" entry would make the
// server look permanently in-use and the listener would never be torn down.
func TestMarkOAuthComplete_TearsDownWhenNoSiblingFlow(t *testing.T) {
	serverName := "test-mark-complete-no-sibling"
	client := newTestOAuthClient(t, serverName)

	manager := oauth.GetGlobalCallbackManager()
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServerForce(serverName) })

	// Mirrors what handleOAuthAuthorization/handleOAuthAuthorizationWithResult/
	// waitForOAuthCallbackAsync do before calling markOAuthComplete: register
	// this flow's own state, receive the callback, THEN mark complete.
	cb.Register("own-state")

	client.markOAuthComplete("own-state")

	_, exists := manager.GetCallbackServer(serverName)
	assert.False(t, exists, "callback server should be torn down once the only flow completes")
}

// TestMarkOAuthComplete_SurvivesSiblingFlow reproduces the actual bug the
// user observed: a manual "auth login" (or an automatic reconnect) completes
// while a SIBLING flow for the same server is still waiting on its own
// browser round-trip. Completing flow A must not sever flow B's callback -
// concretely, flow B's channel must survive and the HTTP listener must still
// route a real callback to it afterward.
func TestMarkOAuthComplete_SurvivesSiblingFlow(t *testing.T) {
	serverName := "test-mark-complete-survives-sibling"
	client := newTestOAuthClient(t, serverName)

	manager := oauth.GetGlobalCallbackManager()
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServerForce(serverName) })

	cb.Register("own-state")
	chSibling := cb.Register("sibling-state")

	client.markOAuthComplete("own-state")

	_, exists := manager.GetCallbackServer(serverName)
	require.True(t, exists, "callback server must survive while the sibling flow is still waiting")

	// Prove the actual HTTP listener survived, not just the bookkeeping map
	// entry: route a real callback to the sibling's state.
	resp, err := http.Get(fmt.Sprintf("%s?state=sibling-state&code=sibling-code", cb.RedirectURI))
	require.NoError(t, err, "callback listener should still be serving the sibling flow")
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	select {
	case params := <-chSibling:
		assert.Equal(t, "sibling-state", params["state"])
		assert.Equal(t, "sibling-code", params["code"])
	case <-time.After(2 * time.Second):
		t.Fatal("sibling flow never received its callback after the other flow completed")
	}
}

// TestMarkOAuthComplete_EmptyStateDoesNotReapSibling covers Connect()'s fast
// path (a persisted token already valid, no browser round-trip - see
// connection.go's markOAuthComplete("") call site), which has no state of
// its own to release. It must be a safe no-op and, critically, must not
// mistake a sibling's still-registered waiter for "idle" and reap the
// server out from under it.
func TestMarkOAuthComplete_EmptyStateDoesNotReapSibling(t *testing.T) {
	serverName := "test-mark-complete-empty-state"
	client := newTestOAuthClient(t, serverName)

	manager := oauth.GetGlobalCallbackManager()
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServerForce(serverName) })

	chSibling := cb.Register("sibling-state")

	assert.NotPanics(t, func() { client.markOAuthComplete("") })

	_, exists := manager.GetCallbackServer(serverName)
	assert.True(t, exists, "empty-state completion must not reap a server with a live sibling waiter")

	select {
	case p := <-chSibling:
		t.Fatalf("sibling waiter should not have received anything: %v", p)
	case <-time.After(200 * time.Millisecond):
		// expected: sibling still waiting, untouched
	}
}
