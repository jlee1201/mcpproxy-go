package core

import (
	"context"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// A connect attempt that waits on another goroutine's OAuth flow must not report
// success on its own: nothing was started or initialized, so the managed client
// would mark the server Ready with no server info (ListTools: "server info not
// available"). Against an unreachable upstream the only correct outcome is an error.
func TestTryOAuthAuth_JoinedFlowDoesNotReportBareSuccess(t *testing.T) {
	serverName := "test-joined-flow-no-bare-success"
	c, err := NewClientWithOptions(
		serverName,
		&config.ServerConfig{Name: serverName, Protocol: "streamable-http", URL: "http://127.0.0.1:1/mcp"},
		zap.NewNop(), nil, nil, nil, false, nil,
	)
	require.NoError(t, err)

	coordinator := oauth.GetGlobalCoordinator()
	flowCtx, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, flowCtx.CorrelationID, true, nil) })

	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, flowCtx.CorrelationID, true, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = c.tryOAuthAuth(ctx)

	require.Error(t, err, "joined flow must be followed by a real connect+initialize, not a bare nil")
	assert.Nil(t, c.GetServerInfo())
}

func TestTrySSEOAuthAuth_JoinedFlowDoesNotReportBareSuccess(t *testing.T) {
	serverName := "test-joined-flow-sse-no-bare-success"
	c, err := NewClientWithOptions(
		serverName,
		&config.ServerConfig{Name: serverName, Protocol: "sse", URL: "http://127.0.0.1:1/sse"},
		zap.NewNop(), nil, nil, nil, false, nil,
	)
	require.NoError(t, err)

	coordinator := oauth.GetGlobalCoordinator()
	flowCtx, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, flowCtx.CorrelationID, true, nil) })

	go func() {
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow(serverName, flowCtx.CorrelationID, true, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = c.trySSEOAuthAuth(ctx)

	require.Error(t, err)
	assert.Nil(t, c.GetServerInfo())
}
