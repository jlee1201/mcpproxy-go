package managed

import (
	"fmt"
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
