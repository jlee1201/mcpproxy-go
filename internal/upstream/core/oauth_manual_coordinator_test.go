package core

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newTestOAuthClientAtURL is newTestOAuthClient (see oauth_complete_test.go) with
// a caller-chosen URL, so tests that need a deterministic, network-free
// failure can point the client at a loopback address nothing is listening on
// instead of a real host.
func newTestOAuthClientAtURL(t *testing.T, serverName, url string) *Client {
	t.Helper()
	c, err := NewClientWithOptions(
		serverName,
		&config.ServerConfig{Name: serverName, Protocol: "streamable-http", URL: url},
		zap.NewNop(),
		nil,
		nil,
		nil,
		false,
		nil,
	)
	require.NoError(t, err)
	return c
}

// unusedLoopbackURL binds a loopback listener, immediately closes it, and
// returns an http:// URL for that exact address. Any connection attempt to it
// fails fast and deterministically with "connection refused" - no real
// network egress, no dependency on a well-known port happening to be closed.
func unusedLoopbackURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return fmt.Sprintf("http://%s/mcp", addr)
}

// --- ForceOAuthFlowWithResult (Defect 2, synchronous manual path) ---
//
// These reproduce the actual race: a sibling flow (an automatic reconnect
// already running through tryOAuthAuth, or another manual call) is registered
// with the global coordinator for this server when ForceOAuthFlowWithResult
// is called. Pre-fix, ForceOAuthFlowWithResult never touches the coordinator
// at all, so it races straight past the sibling into its own OAuth attempt -
// this is what produced the spurious re-auth error page. The seeded-flow
// style mirrors oauth_complete_test.go's approach to the callback manager.

func TestForceOAuthFlowWithResult_WaitAndReuse_SiblingSucceeds(t *testing.T) {
	serverName := "test-force-oauth-wait-success"
	client := newTestOAuthClient(t, serverName)
	coordinator := oauth.GetGlobalCoordinator()

	_, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, true, nil)
	}()

	// Short ctx: pre-fix this call ignores the coordinator and heads straight
	// into real OAuth/network work instead of waiting, so a red run needs a
	// bound that doesn't hang - it does not need this long to pass post-fix.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := client.ForceOAuthFlowWithResult(ctx)
	require.NoError(t, err, "must report the sibling flow's success, not fail or time out")
	require.NotNil(t, result)
	assert.False(t, result.BrowserOpened, "must not have opened its own browser tab")
	assert.False(t, coordinator.IsFlowActive(serverName), "must not have started a second flow of its own")
}

func TestForceOAuthFlowWithResult_WaitAndReuse_SiblingFails(t *testing.T) {
	serverName := "test-force-oauth-wait-failure"
	client := newTestOAuthClient(t, serverName)
	coordinator := oauth.GetGlobalCoordinator()

	_, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	siblingErr := fmt.Errorf("sibling flow: authorization denied")
	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, false, siblingErr)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := client.ForceOAuthFlowWithResult(ctx)
	require.Error(t, err, "must surface the sibling flow's failure, not silently succeed")
	assert.ErrorIs(t, err, siblingErr)
	require.NotNil(t, result)
	assert.False(t, coordinator.IsFlowActive(serverName), "must not have started a second flow of its own")
}

func TestForceOAuthFlowWithResult_GivesUpOnStuckSibling(t *testing.T) {
	serverName := "test-force-oauth-stuck-sibling"
	client := newTestOAuthClient(t, serverName)
	coordinator := oauth.GetGlobalCoordinator()

	_, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	// Sibling never completes - simulates a hung browser round-trip. Clean up
	// so this doesn't leak a 10-minute stale entry into later tests.
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	// The caller's own ctx deadline (not the production ~5min bound) is what
	// must cut this short - WaitForFlow's three-way select races ctx.Done()
	// against its own internal timeout, and the shorter one wins.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = client.ForceOAuthFlowWithResult(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "must give up rather than hang forever on a stuck sibling")
	assert.Less(t, elapsed, 1*time.Second, "must give up via ctx deadline, not block for the full flow timeout")
	assert.True(t, coordinator.IsFlowActive(serverName), "the stuck sibling's flow must be untouched, not clobbered")
}

func TestForceOAuthFlowWithResult_NoSibling_EndsOwnFlowOnFailure(t *testing.T) {
	serverName := "test-force-oauth-no-sibling"
	client := newTestOAuthClientAtURL(t, serverName, unusedLoopbackURL(t))
	coordinator := oauth.GetGlobalCoordinator()
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ForceOAuthFlowWithResult(ctx)
	require.Error(t, err, "connection-refused loopback target must fail")
	assert.False(t, coordinator.IsFlowActive(serverName), "the flow this call started for itself must be ended on failure, not leaked")
}

// --- StartOAuthFlowQuick (Defect 2, async login-API path) ---
//
// StartOAuthFlowQuick's own doc comment promises it "Returns OAuthStartResult
// immediately... without blocking the HTTP response for the full OAuth flow."
// Unlike ForceOAuthFlowWithResult, it must never block on a sibling - it must
// fail fast, the same way it already fails fast on isOAuthInProgress().

func TestStartOAuthFlowQuick_FastFailsOnActiveSibling_WithoutDisturbingIt(t *testing.T) {
	serverName := "test-quick-oauth-active-sibling"
	client := newTestOAuthClient(t, serverName)
	coordinator := oauth.GetGlobalCoordinator()

	siblingFlow, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err = client.StartOAuthFlowQuick(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "must fail fast rather than start a second, racing flow")
	assert.Less(t, elapsed, 500*time.Millisecond, "must fail fast, not wait on the sibling (this is the synchronous login-API path)")

	active := coordinator.GetActiveFlow(serverName)
	require.NotNil(t, active, "the sibling's flow must still be active")
	assert.Equal(t, siblingFlow.CorrelationID, active.CorrelationID, "must not have ended/replaced the sibling's flow")
}

func TestStartOAuthFlowQuick_NoSibling_EndsOwnFlowOnEarlyFailure(t *testing.T) {
	serverName := "test-quick-oauth-no-sibling"
	client := newTestOAuthClientAtURL(t, serverName, unusedLoopbackURL(t))
	coordinator := oauth.GetGlobalCoordinator()
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.StartOAuthFlowQuick(ctx)
	require.Error(t, err, "connection-refused loopback target must fail before any goroutine handoff")
	assert.False(t, coordinator.IsFlowActive(serverName), "must end its own flow when it fails before handing off to waitForOAuthCallbackAsync")
}

// --- waitForOAuthCallbackAsync (the goroutine StartOAuthFlowQuick hands the flow to) ---

// TestWaitForOAuthCallbackAsync_EndsHandedOffFlowOnCtxCancel simulates exactly
// what StartOAuthFlowQuick does right before spawning this goroutine: it owns
// an active coordinator flow and is handing it off. Pre-fix, this function
// never calls EndFlow, so the seeded flow leaks (IsFlowActive stays true)
// forever once its caller has returned - the manual path can never again
// start a flow for this server without waiting out the 10-minute stale timer.
func TestWaitForOAuthCallbackAsync_EndsHandedOffFlowOnCtxCancel(t *testing.T) {
	serverName := "test-callback-async-handoff"
	client := newTestOAuthClient(t, serverName)
	coordinator := oauth.GetGlobalCoordinator()

	_, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, false, nil) })

	manager := oauth.GetGlobalCallbackManager()
	_, err = manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServerForce(serverName) })

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// oauthHandler is nil: safe here because the ctx.Done() branch this test
	// drives never dereferences it (only the success branch, deep inside the
	// channel-received case, calls oauthHandler.ProcessAuthorizationResponse).
	client.waitForOAuthCallbackAsync(ctx, nil, "test-verifier", "test-state", "test-correlation")

	assert.False(t, coordinator.IsFlowActive(serverName), "must end the flow handed to it once its own wait is done, not leak it")
}
