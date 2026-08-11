package contracts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConvertGenericServersToTyped_OAuth verifies OAuth config is properly extracted
func TestConvertGenericServersToTyped_OAuth(t *testing.T) {
	// Simulate the map structure returned from the management service
	genericServers := []map[string]interface{}{
		{
			"id":            "sentry",
			"name":          "sentry",
			"url":           "https://mcp.sentry.dev/mcp",
			"protocol":      "http",
			"enabled":       true,
			"quarantined":   false,
			"connected":     false,
			"status":        "connecting",
			"authenticated": false,
			"tool_count":    0,
			"created":       time.Date(2025, 11, 29, 15, 49, 25, 0, time.UTC),
			"updated":       time.Date(2025, 11, 29, 15, 49, 25, 0, time.UTC),
			"oauth": map[string]interface{}{
				"auth_url":  "https://mcp.sentry.dev/oauth/authorize",
				"token_url": "https://mcp.sentry.dev/oauth/token",
				"client_id": "test-client-id",
				"scopes":    []interface{}{"read", "write"},
				"extra_params": map[string]interface{}{
					"resource": "https://mcp.sentry.dev/mcp",
					"audience": "sentry-api",
				},
				"redirect_port": 8080,
			},
			"last_error": "OAuth authorization required",
		},
	}

	servers := ConvertGenericServersToTyped(genericServers)

	require.Len(t, servers, 1, "Should convert exactly one server")

	server := servers[0]
	assert.Equal(t, "sentry", server.Name)
	assert.Equal(t, "https://mcp.sentry.dev/mcp", server.URL)
	assert.Equal(t, false, server.Authenticated)

	// The critical assertions: OAuth config should be extracted
	require.NotNil(t, server.OAuth, "OAuth config should not be nil")
	assert.Equal(t, "https://mcp.sentry.dev/oauth/authorize", server.OAuth.AuthURL)
	assert.Equal(t, "https://mcp.sentry.dev/oauth/token", server.OAuth.TokenURL)
	assert.Equal(t, "test-client-id", server.OAuth.ClientID)
	assert.Equal(t, []string{"read", "write"}, server.OAuth.Scopes)
	assert.Equal(t, 8080, server.OAuth.RedirectPort)

	require.NotNil(t, server.OAuth.ExtraParams, "ExtraParams should not be nil")
	assert.Equal(t, "https://mcp.sentry.dev/mcp", server.OAuth.ExtraParams["resource"])
	assert.Equal(t, "sentry-api", server.OAuth.ExtraParams["audience"])
}

// TestConvertGenericServersToTyped_EmptyOAuth verifies empty OAuth config creates non-nil OAuth struct
func TestConvertGenericServersToTyped_EmptyOAuth(t *testing.T) {
	genericServers := []map[string]interface{}{
		{
			"id":            "test-server",
			"name":          "test-server",
			"enabled":       true,
			"connected":     false,
			"authenticated": false,
			"tool_count":    0,
			"oauth":         map[string]interface{}{}, // Empty OAuth config
		},
	}

	servers := ConvertGenericServersToTyped(genericServers)

	require.Len(t, servers, 1)
	require.NotNil(t, servers[0].OAuth, "Even empty oauth map should create non-nil OAuth config")
	assert.Empty(t, servers[0].OAuth.AuthURL)
	assert.Empty(t, servers[0].OAuth.ClientID)
}

// TestConvertGenericServersToTyped_NoOAuth verifies servers without OAuth have nil OAuth field
func TestConvertGenericServersToTyped_NoOAuth(t *testing.T) {
	genericServers := []map[string]interface{}{
		{
			"id":            "test-server",
			"name":          "test-server",
			"enabled":       true,
			"connected":     true,
			"authenticated": false,
			"tool_count":    5,
			// No oauth field at all
		},
	}

	servers := ConvertGenericServersToTyped(genericServers)

	require.Len(t, servers, 1)
	assert.Nil(t, servers[0].OAuth, "Servers without OAuth config should have nil OAuth field")
}

// TestConvertGenericServersToTyped_HealthAndFreshnessFields is the D6/A4
// regression guard: this converter used to drop `health` entirely (only
// management/service.go's separate extraction kept it), and effective_status
// / last_success_at / last_auth_failure_at (A2/A3) didn't exist yet. Any
// caller routed through ConvertGenericServersToTyped must see all four.
func TestConvertGenericServersToTyped_HealthAndFreshnessFields(t *testing.T) {
	successAt := time.Date(2026, 8, 11, 11, 55, 0, 0, time.UTC)
	authFailureAt := time.Date(2026, 8, 11, 11, 50, 0, 0, time.UTC)
	healthStatus := &HealthStatus{
		Level:      "healthy",
		AdminState: "enabled",
		Summary:    "Connected (5 tools)",
	}

	genericServers := []map[string]interface{}{
		{
			"id":                   "gcalgusto",
			"name":                 "gcalgusto",
			"enabled":              true,
			"connected":            true,
			"health":               healthStatus,
			"effective_status":     "ready",
			"last_success_at":      successAt,
			"last_auth_failure_at": authFailureAt,
		},
	}

	servers := ConvertGenericServersToTyped(genericServers)

	require.Len(t, servers, 1)
	server := servers[0]

	require.NotNil(t, server.Health, "health must survive ConvertGenericServersToTyped")
	assert.Equal(t, "healthy", server.Health.Level)
	assert.Equal(t, "Connected (5 tools)", server.Health.Summary)

	assert.Equal(t, "ready", server.EffectiveStatus)
	require.NotNil(t, server.LastSuccessAt)
	assert.Equal(t, successAt, *server.LastSuccessAt)
	require.NotNil(t, server.LastAuthFailureAt)
	assert.Equal(t, authFailureAt, *server.LastAuthFailureAt)
}

// TestConvertGenericServersToTyped_FreshnessFieldsAbsentWhenNotSet verifies
// the additive fields stay nil/empty for a generic map produced by an old
// binary that doesn't populate them -- no panics, no zero-time surprises.
func TestConvertGenericServersToTyped_FreshnessFieldsAbsentWhenNotSet(t *testing.T) {
	genericServers := []map[string]interface{}{
		{
			"id":        "legacy-server",
			"name":      "legacy-server",
			"enabled":   true,
			"connected": true,
		},
	}

	servers := ConvertGenericServersToTyped(genericServers)

	require.Len(t, servers, 1)
	server := servers[0]
	assert.Nil(t, server.Health)
	assert.Empty(t, server.EffectiveStatus)
	assert.Nil(t, server.LastSuccessAt)
	assert.Nil(t, server.LastAuthFailureAt)
}
