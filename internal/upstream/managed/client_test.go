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

// TestOAuthAuthorizationRequired_UsesExtendedBackoff is a source-level
// regression guard for a bug found during the reconnect-storm backport
// review: Connect()'s isOAuthAuthorizationRequired branch called
// mc.StateManager.SetError(err) (the short default backoff ladder) instead
// of SetOAuthError(err) (the 5min->24h extended OAuth ladder that
// fork/main's equivalent branch already uses), silently defeating the PR's
// own anti-reconnect-storm goal for exactly the OAuth-blocked-server case
// (#1013/#1039).
//
// mc.coreClient is a concrete *core.Client with no fake/mock seam, so there
// is no way to deterministically drive Connect() into this branch with a
// real network call. A test that instead calls
// mc.StateManager.SetOAuthError(...) directly and asserts IsOAuthError()
// would pass whether or not Connect() itself ever calls SetOAuthError --
// i.e. it can't fail on the actual regression. So this asserts the shape at
// the source level instead: the if-block guarded by
// mc.isOAuthAuthorizationRequired(err) inside Connect() must call
// StateManager.SetOAuthError and must NOT call StateManager.SetError.
func TestOAuthAuthorizationRequired_UsesExtendedBackoff(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "client.go", nil, 0)
	require.NoError(t, err, "parse client.go")

	var connectFn *ast.FuncDecl
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv != nil && fn.Name.Name == "Connect" {
			connectFn = fn
			break
		}
	}
	require.NotNil(t, connectFn, "Connect FuncDecl not found in client.go -- did it get renamed?")

	var oauthRequiredBranch *ast.BlockStmt
	ast.Inspect(connectFn.Body, func(n ast.Node) bool {
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		call, ok := ifStmt.Cond.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if ok && sel.Sel.Name == "isOAuthAuthorizationRequired" {
			oauthRequiredBranch = ifStmt.Body
		}
		return true
	})
	require.NotNil(t, oauthRequiredBranch,
		"no `if mc.isOAuthAuthorizationRequired(err)` branch found in Connect() -- did it get renamed or restructured?")

	sawSetOAuthError := false
	sawSetError := false
	ast.Inspect(oauthRequiredBranch, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "SetOAuthError":
			sawSetOAuthError = true
		case "SetError":
			sawSetError = true
		}
		return true
	})

	assert.True(t, sawSetOAuthError,
		"the isOAuthAuthorizationRequired branch must call StateManager.SetOAuthError to use the extended OAuth backoff ladder")
	assert.False(t, sawSetError,
		"the isOAuthAuthorizationRequired branch must not call StateManager.SetError -- that's the short default ladder this fix replaces")
}

// TestTryReconnect_PreservesRetryCountAcrossFailures locks in a second
// regression found during the same review: tryReconnect() (the sole caller
// of which is ForceReconnect, triggered e.g. by RefreshManager's
// oauth_token_refresh event) called mc.StateManager.Reset() before
// reconnecting, which zeroes retryCount/lastRetryTime immediately -- the
// exact state ResetForReconnect() exists to preserve across a reconnect
// attempt so exponential backoff isn't defeated. ResetForReconnect already
// had a StateManager-level unit test (TestResetForReconnect_PreservesRetryCount
// in internal/upstream/types), but nothing exercised the actual production
// call site, so the regression (using plain Reset() there instead) shipped
// silently.
//
// Uses an empty ServerConfig so mc.coreClient.Connect fails fast and
// deterministically (empty Command -> stdio transport -> immediate exec
// error), never touching the network.
func TestTryReconnect_PreservesRetryCountAcrossFailures(t *testing.T) {
	cfg := &config.ServerConfig{Name: "test-reconnect-backoff"}
	mc, err := NewClient("test-reconnect-backoff", cfg, zap.NewNop(), nil, nil, nil, secret.NewResolver())
	require.NoError(t, err)

	// Simulate 3 prior failed attempts, as ConnectAll/RetryConnection's
	// backoff loop would have produced before ForceReconnect ever fires.
	mc.StateManager.SetError(fmt.Errorf("prior failure"))
	mc.StateManager.SetError(fmt.Errorf("prior failure"))
	mc.StateManager.SetError(fmt.Errorf("prior failure"))
	require.Equal(t, 3, mc.StateManager.GetConnectionInfo().RetryCount)

	// Now simulate the realistic trigger for tryReconnect: the server was
	// previously OAuth-blocked, and a fresh token just arrived (the
	// oauth_token_refresh event that drives ForceReconnect -> tryReconnect).
	mc.StateManager.SetOAuthError(fmt.Errorf("oauth authorization required"))
	require.True(t, mc.StateManager.IsOAuthError())

	// tryReconnect is unexported; called directly (synchronously, not via
	// `go mc.tryReconnect()`) so the assertion below isn't racing it.
	mc.tryReconnect()

	info := mc.StateManager.GetConnectionInfo()
	assert.Equal(t, 4, info.RetryCount,
		"retryCount must carry the prior 3 failures forward (+1 for this attempt's failure) -- "+
			"the old Reset()-based code would have zeroed it to 0 before Connect(), landing on 1 instead")
	assert.False(t, mc.StateManager.IsOAuthError(),
		"tryReconnect's fresh Connect() attempt failed for a non-OAuth reason (empty config -> stdio "+
			"exec error), so the stale isOAuthError=true from before this reconnect must not survive -- "+
			"otherwise ShouldRetryOAuth() would misclassify this plain connectivity failure as still "+
			"OAuth-blocked and park it behind the 5min->24h OAuth ladder instead of the normal one")
}
