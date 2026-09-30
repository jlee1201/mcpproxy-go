package upstream

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStart mimics StartOAuthFlowQuick: it fails with ErrFlowInProgress while
// the coordinator holds a flow for serverName, otherwise it "starts" one.
func fakeStart(coordinator *oauth.OAuthFlowCoordinator, serverName string, calls *int32) func() (*core.OAuthStartResult, error) {
	return func() (*core.OAuthStartResult, error) {
		atomic.AddInt32(calls, 1)
		if coordinator.IsFlowActive(serverName) {
			return &core.OAuthStartResult{}, fmt.Errorf("OAuth authorization already in progress for %s: %w", serverName, oauth.ErrFlowInProgress)
		}
		return &core.OAuthStartResult{BrowserOpened: true, AuthURL: "https://example.com/authorize"}, nil
	}
}

func seedFlow(t *testing.T, coordinator *oauth.OAuthFlowCoordinator, serverName string) *oauth.OAuthFlowContext {
	t.Helper()
	flow, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, flow.CorrelationID, false, nil) })
	return flow
}

func TestStartOrJoinLogin_NoFlowActive_StartsOwnFlow(t *testing.T) {
	serverName := "test-join-none-active"
	coordinator := oauth.GetGlobalCoordinator()
	var calls int32

	result, err := startOrJoinLogin(coordinator, serverName, time.Second, fakeStart(coordinator, serverName, &calls), func() bool { return false })

	require.NoError(t, err)
	assert.False(t, result.JoinedExistingFlow)
	assert.True(t, result.BrowserOpened)
	assert.EqualValues(t, 1, calls)
}

func TestStartOrJoinLogin_JoinsInFlightFlowThatSucceeds(t *testing.T) {
	serverName := "test-join-inflight-success"
	coordinator := oauth.GetGlobalCoordinator()
	flow := seedFlow(t, coordinator, serverName)
	var tokenStored atomic.Bool
	go func() {
		time.Sleep(100 * time.Millisecond)
		tokenStored.Store(true)
		coordinator.EndFlow(serverName, flow.CorrelationID, true, nil)
	}()
	var calls int32

	result, err := startOrJoinLogin(coordinator, serverName, 3*time.Second, fakeStart(coordinator, serverName, &calls), tokenStored.Load)

	require.NoError(t, err, "a login that lands on an in-flight sign-in must wait for it, not fail")
	assert.True(t, result.JoinedExistingFlow)
	assert.EqualValues(t, 1, calls, "must not open a second browser flow when the joined one produced a token")
}

func TestStartOrJoinLogin_JoinedFlowFails_StartsFreshFlow(t *testing.T) {
	serverName := "test-join-inflight-failure"
	coordinator := oauth.GetGlobalCoordinator()
	flow := seedFlow(t, coordinator, serverName)
	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, flow.CorrelationID, false, errors.New("user closed the tab"))
	}()
	var calls int32

	result, err := startOrJoinLogin(coordinator, serverName, 3*time.Second, fakeStart(coordinator, serverName, &calls), func() bool { return false })

	require.NoError(t, err)
	assert.False(t, result.JoinedExistingFlow, "the result is from our own fresh flow")
	assert.True(t, result.BrowserOpened)
	assert.EqualValues(t, 2, calls)
}

// The sibling ends between our failed start and WaitForFlow registering, so
// WaitForFlow returns nil without telling us the outcome.
func TestStartOrJoinLogin_FlowEndsBeforeWaiterRegisters_UsesTokenToDecide(t *testing.T) {
	coordinator := oauth.GetGlobalCoordinator()
	for _, tc := range []struct {
		name       string
		tokenValid bool
		wantJoined bool
		wantCalls  int32
	}{
		{"token present", true, true, 1},
		{"no token", false, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverName := "test-join-race-" + tc.name
			var calls int32
			start := func() (*core.OAuthStartResult, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					return &core.OAuthStartResult{}, fmt.Errorf("in progress: %w", oauth.ErrFlowInProgress)
				}
				return &core.OAuthStartResult{BrowserOpened: true}, nil
			}

			result, err := startOrJoinLogin(coordinator, serverName, time.Second, start, func() bool { return tc.tokenValid })

			require.NoError(t, err)
			assert.Equal(t, tc.wantJoined, result.JoinedExistingFlow)
			assert.Equal(t, tc.wantCalls, calls)
		})
	}
}

func TestStartOrJoinLogin_InFlightFlowOutlastsTimeout_ReturnsStillInProgress(t *testing.T) {
	serverName := "test-join-inflight-timeout"
	coordinator := oauth.GetGlobalCoordinator()
	seedFlow(t, coordinator, serverName)
	var calls int32

	start := time.Now()
	_, err := startOrJoinLogin(coordinator, serverName, 200*time.Millisecond, fakeStart(coordinator, serverName, &calls), func() bool { return false })

	require.ErrorIs(t, err, ErrLoginFlowStillInProgress)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.True(t, coordinator.IsFlowActive(serverName), "must not disturb the flow it was waiting on")
}

func TestLoginJoinTimeout_StaysUnderHTTPWriteTimeout(t *testing.T) {
	// internal/server/server.go sets WriteTimeout: 120s on the API server.
	assert.Less(t, loginJoinTimeout, 120*time.Second)
}
