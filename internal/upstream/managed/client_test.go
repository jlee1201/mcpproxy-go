package managed

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	mcptransport "github.com/mark3labs/mcp-go/client/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/secret"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/types"
)

// TestZombieInvalidation_PostConnectAuthFailureFlipsState reproduces the
// D2/A1 zombie: connect succeeds (state Ready, a success recorded), then a
// tool call fails with mcp-go's real "no valid token available,
// authorization required" error, wrapped exactly the way
// internal/upstream/core.Client.CallTool wraps it in production. Before A1
// this error only satisfies isConnectionError's transport-string list (it
// doesn't), so the client stays Ready forever -- the whole bug. After A1 it
// must classify as an auth failure and flip state via SetOAuthError.
//
// This test is written FIRST and must fail against the recordCallFailure
// stub (a no-op): state stays Ready and IsOAuthError() stays false.
func TestZombieInvalidation_PostConnectAuthFailureFlipsState(t *testing.T) {
	cfg := &config.ServerConfig{Name: "test-zombie"}
	logger := zap.NewNop()

	mc, err := NewClient("test-zombie", cfg, logger, nil, nil, nil, secret.NewResolver())
	require.NoError(t, err)

	// Simulate a successful Connect: state Ready, a success recorded (the
	// MCP initialize that happened during the real Connect path).
	mc.StateManager.TransitionTo(types.StateReady)
	mc.StateManager.RecordSuccess()
	require.True(t, mc.StateManager.IsReady())
	successAt := mc.StateManager.LastSuccessAt()
	require.False(t, successAt.IsZero())

	// Build the error exactly as internal/upstream/core.Client.CallTool
	// wraps a mid-session 401 in production:
	//   CallTool failed for 'echo': transport error: failed to send request: no valid token available, authorization required
	inner := &mcptransport.OAuthAuthorizationRequiredError{}
	wrapped := mcptransport.NewError(fmt.Errorf("failed to send request: %w", inner))
	callErr := fmt.Errorf("CallTool failed for '%s': %w", "echo", wrapped)

	// A tool call after a small delay so LastAuthFailureAt is unambiguously
	// after LastSuccessAt even at low clock resolution.
	time.Sleep(2 * time.Millisecond)
	mc.recordCallFailure(callErr)

	assert.Equal(t, types.StateError, mc.StateManager.GetState(),
		"a post-connect auth failure must flip a Ready client to Error")
	assert.True(t, mc.StateManager.IsOAuthError(),
		"a post-connect auth failure must set IsOAuthError so 8e31b1e's no-retry guard engages")
	assert.True(t, mc.StateManager.LastAuthFailureAt().After(successAt),
		"last_auth_failure_at must be recorded after last_success_at -- that ordering is the zombie's proof")
}

// TestGetConfig_BlocksBehindHeldMutex documents *why* the actor-pool fix
// (2026-08-13) stopped calling GetConfig() from the hot event-consumption
// path: Connect()/Disconnect() hold mc.mu for their entire duration
// (including network I/O), and GetConfig() takes mc.mu.RLock(), so a
// concurrent GetConfig() call blocks for as long as a connect attempt is in
// flight. This is the actual mechanism that stalled
// Supervisor.updateSnapshotFromEvent (the sole caller of ActorPoolSimple's
// GetServerState, which used to call GetConfig()) for however long a
// concurrent Connect() took, overflowing the 50-slot event channel upstream
// during an OAuth reconnect burst ("Event channel full, dropping event").
func TestGetConfig_BlocksBehindHeldMutex(t *testing.T) {
	cfg := &config.ServerConfig{Name: "test-lock"}
	mc, err := NewClient("test-lock", cfg, zap.NewNop(), nil, nil, nil, secret.NewResolver())
	require.NoError(t, err)

	const holdTime = 200 * time.Millisecond

	// Simulate Connect() holding mc.mu across a slow network call.
	mc.mu.Lock()
	unlocked := make(chan struct{})
	go func() {
		time.Sleep(holdTime)
		mc.mu.Unlock()
		close(unlocked)
	}()

	start := time.Now()
	_ = mc.GetConfig()
	elapsed := time.Since(start)
	<-unlocked

	assert.GreaterOrEqual(t, elapsed, holdTime-10*time.Millisecond,
		"GetConfig() should block for the full mutex hold -- this is the mechanism, not a flake to tolerate")
}

// TestLockFreeAccessors_DoNotBlockBehindHeldMutex is the fix side of the
// same story: the three accessors GetServerState now uses instead of
// GetConfig() must never touch mc.mu at all. This test holds mc.mu.Lock()
// from the SAME goroutine that then calls each accessor -- a deliberately
// harsher check than a timing race: sync.RWMutex is not reentrant, so if any
// of these accessors tried to Lock/RLock mc.mu they would deadlock this
// goroutine against itself and the test would hang (failing on the test
// binary's timeout) rather than merely running slow.
func TestLockFreeAccessors_DoNotBlockBehindHeldMutex(t *testing.T) {
	cfg := &config.ServerConfig{Name: "test-nolock"}
	mc, err := NewClient("test-nolock", cfg, zap.NewNop(), nil, nil, nil, secret.NewResolver())
	require.NoError(t, err)
	mc.StateManager.TransitionTo(types.StateReady)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	assertFast := func(name string, fn func()) {
		done := make(chan struct{})
		go func() {
			fn()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s deadlocked against a self-held mc.mu -- it must not take mc.mu at all", name)
		}
	}

	assertFast("IsConnected", func() { _ = mc.IsConnected() })
	assertFast("GetConnectionInfo", func() { _ = mc.GetConnectionInfo() })
	assertFast("GetCachedToolCountNonBlocking", func() { _ = mc.GetCachedToolCountNonBlocking() })
}

// TestBackgroundHealthCheck_HasNoPeriodicPoll is a source-level regression
// guard for a removed 30s ListTools poll. That poll drove tryReconnect() on
// error independently of Manager.RetryConnection, Manager.ConnectAll, and the
// supervisor's 30s reconcile ticker -- a fourth (really third, but the one
// that mattered most) OAuth-unaware retry path that alone accounted for ~1K
// calls/hr per healthy connection and aggressively re-dialed OAuth-expired
// servers. Verified via 22h production monitoring: ~30K calls/day -> 0
// steady-state.
//
// A behavioral test can't prove this: a fast unit test can't wait out a real
// 30s interval, and shrinking the interval to something observable doesn't
// test the real code path. So this asserts the shape at the source level
// instead: backgroundHealthCheck's body must contain no ticker/timer
// construct and no ListTools call, and must contain a receive on
// mc.stopMonitoring. It also asserts performHealthCheck (the helper the old
// poll called) no longer exists in the file. If either function is missing
// entirely, the test fails loudly rather than passing vacuously.
func TestBackgroundHealthCheck_HasNoPeriodicPoll(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "client.go", nil, 0)
	require.NoError(t, err, "parse client.go")

	var backgroundHealthCheck *ast.FuncDecl
	performHealthCheckExists := false

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		switch fn.Name.Name {
		case "backgroundHealthCheck":
			backgroundHealthCheck = fn
		case "performHealthCheck":
			performHealthCheckExists = true
		}
	}

	require.NotNil(t, backgroundHealthCheck, "backgroundHealthCheck FuncDecl not found in client.go -- did it get renamed or removed?")
	assert.False(t, performHealthCheckExists, "performHealthCheck must not exist -- it was the helper the removed 30s poll called")

	sawStopReceive := false
	ast.Inspect(backgroundHealthCheck.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.UnaryExpr:
			if node.Op == token.ARROW {
				if sel, ok := node.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "stopMonitoring" {
					sawStopReceive = true
				}
			}
		case *ast.SelectorExpr:
			if node.Sel.Name == "NewTicker" || node.Sel.Name == "Tick" || node.Sel.Name == "After" {
				t.Fatalf("backgroundHealthCheck must not use time.%s -- that reintroduces a periodic poll", node.Sel.Name)
			}
			if node.Sel.Name == "ListTools" {
				t.Fatal("backgroundHealthCheck must not call ListTools -- that reintroduces the removed health poll")
			}
		}
		return true
	})

	assert.True(t, sawStopReceive, "backgroundHealthCheck must receive on mc.stopMonitoring -- that's the only thing it should wait on")
}
