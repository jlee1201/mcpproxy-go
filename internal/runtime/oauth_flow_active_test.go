package runtime

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGetAllServers_ReportsOAuthFlowActive(t *testing.T) {
	serverName := "test-runtime-oauth-flow-active"
	tempDir := t.TempDir()
	cfg := &config.Config{
		DataDir: tempDir,
		Listen:  "127.0.0.1:9000",
		Servers: []*config.ServerConfig{
			{Name: serverName, Protocol: "streamable-http", URL: "http://127.0.0.1:1/mcp", Enabled: false},
		},
	}
	rt, err := New(cfg, filepath.Join(tempDir, "config.yaml"), zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close() })
	require.NoError(t, rt.LoadConfiguredServers(nil))
	rt.Supervisor().Start()

	find := func() map[string]interface{} {
		servers, err := rt.GetAllServers()
		require.NoError(t, err)
		for _, s := range servers {
			if s["name"] == serverName {
				return s
			}
		}
		return nil
	}
	// LoadConfiguredServers adds servers asynchronously.
	require.Eventually(t, func() bool { return find() != nil }, 5*time.Second, 20*time.Millisecond)
	flowActive := func() interface{} { return find()["oauth_flow_active"] }

	coordinator := oauth.GetGlobalCoordinator()
	flow, err := coordinator.StartFlow(serverName)
	require.NoError(t, err)
	t.Cleanup(func() { coordinator.EndFlow(serverName, flow.CorrelationID, false, nil) })

	assert.Equal(t, true, flowActive())

	coordinator.EndFlow(serverName, flow.CorrelationID, true, nil)
	assert.Equal(t, false, flowActive())
}
