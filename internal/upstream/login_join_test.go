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

	result, err := startOrJoinLogin(coordinator, serverName, time.Second, fakeStart(coordinator, serverName, &calls))

	require.NoError(t, err)
	assert.False(t, result.JoinedExistingFlow)
	assert.True(t, result.BrowserOpened)
	assert.EqualValues(t, 1, calls)
}

func TestStartOrJoinLogin_JoinsInFlightFlowThatSucceeds(t *testing.T) {
	serverName := "test-join-inflight-success"
	coordinator := oauth.GetGlobalCoordinator()
	flow := seedFlow(t, coordinator, serverName)
	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, flow.CorrelationID, true, nil)
	}()
	var calls int32

	result, err := startOrJoinLogin(coordinator, serverName, 3*time.Second, fakeStart(coordinator, serverName, &calls))

	require.NoError(t, err, "a login that lands on an in-flight sign-in must wait for it, not fail")
	assert.True(t, result.JoinedExistingFlow)
	assert.EqualValues(t, 1, calls, "must not open a second browser flow when the joined one succeeded")
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

	result, err := startOrJoinLogin(coordinator, serverName, 3*time.Second, fakeStart(coordinator, serverName, &calls))

	require.NoError(t, err)
	assert.False(t, result.JoinedExistingFlow, "the result is from our own fresh flow")
	assert.True(t, result.BrowserOpened)
	assert.EqualValues(t, 2, calls)
}

// The sibling ends between our failed start and JoinFlow registering, so its
// outcome is unknown. A stored token is not proof it succeeded (it may be the
// revoked one that triggered the flow), so a fresh flow must be started.
func TestStartOrJoinLogin_FlowEndsBeforeWaiterRegisters_StartsFreshFlow(t *testing.T) {
	coordinator := oauth.GetGlobalCoordinator()
	serverName := "test-join-race-ended"
	var calls int32
	start := func() (*core.OAuthStartResult, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return &core.OAuthStartResult{}, fmt.Errorf("in progress: %w", oauth.ErrFlowInProgress)
		}
		return &core.OAuthStartResult{BrowserOpened: true}, nil
	}

	result, err := startOrJoinLogin(coordinator, serverName, time.Second, start)

	require.NoError(t, err)
	assert.False(t, result.JoinedExistingFlow)
	assert.EqualValues(t, 2, calls)
}

// Auto-reconnect grabs the lock again right after the first joined flow fails;
// the login keeps joining instead of surfacing a 409 straight away.
func TestStartOrJoinLogin_LockRegrabbedAfterFailure_JoinsAgain(t *testing.T) {
	serverName := "test-join-regrab"
	coordinator := oauth.GetGlobalCoordinator()
	first := seedFlow(t, coordinator, serverName)
	go func() {
		time.Sleep(50 * time.Millisecond)
		coordinator.EndFlow(serverName, first.CorrelationID, false, errors.New("token rejected"))
	}()
	var calls int32
	start := func() (*core.OAuthStartResult, error) {
		if atomic.AddInt32(&calls, 1) == 2 {
			// Reconnect wins the race for the lock just before our second start.
			second, err := coordinator.StartFlow(serverName)
			require.NoError(t, err)
			go func() {
				time.Sleep(50 * time.Millisecond)
				coordinator.EndFlow(serverName, second.CorrelationID, true, nil)
			}()
		}
		return fakeStart(coordinator, serverName, new(int32))()
	}

	result, err := startOrJoinLogin(coordinator, serverName, 3*time.Second, start)

	require.NoError(t, err)
	assert.True(t, result.JoinedExistingFlow)
	assert.EqualValues(t, 2, calls)
}

func TestStartOrJoinLogin_StartKeepsReportingInProgress_GivesUpAfterAttemptCap(t *testing.T) {
	serverName := "test-join-attempt-cap"
	coordinator := oauth.GetGlobalCoordinator()
	var calls int32
	start := func() (*core.OAuthStartResult, error) {
		atomic.AddInt32(&calls, 1)
		return &core.OAuthStartResult{}, fmt.Errorf("in progress: %w", oauth.ErrFlowInProgress)
	}

	begin := time.Now()
	_, err := startOrJoinLogin(coordinator, serverName, 5*time.Second, start)

	require.ErrorIs(t, err, ErrLoginFlowStillInProgress)
	assert.EqualValues(t, maxLoginStartAttempts, calls)
	assert.Less(t, time.Since(begin), time.Second)
}

func TestStartOrJoinLogin_InFlightFlowOutlastsTimeout_ReturnsStillInProgress(t *testing.T) {
	serverName := "test-join-inflight-timeout"
	coordinator := oauth.GetGlobalCoordinator()
	seedFlow(t, coordinator, serverName)
	var calls int32

	start := time.Now()
	_, err := startOrJoinLogin(coordinator, serverName, 200*time.Millisecond, fakeStart(coordinator, serverName, &calls))

	require.ErrorIs(t, err, ErrLoginFlowStillInProgress)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.True(t, coordinator.IsFlowActive(serverName), "must not disturb the flow it was waiting on")
}

func TestLoginJoinTimeout_LeavesHeadroomUnderHTTPWriteTimeout(t *testing.T) {
	// internal/server/server.go sets WriteTimeout: 120s on the API server; the
	// final start() after a failed join needs time of its own.
	assert.LessOrEqual(t, loginJoinTimeout, 60*time.Second)
}
