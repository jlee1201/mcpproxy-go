package upstream

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/secret"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/managed"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/types"
)

// TestRefreshOAuthToken_DynamicOAuthDiscovery tests that RefreshOAuthToken works
// for servers that use dynamic OAuth discovery (no OAuth in static config).
//
// Bug: The current implementation checks serverConfig.OAuth which is nil for
// servers that discover OAuth via Protected Resource Metadata at runtime.
// These servers have OAuth tokens stored in the database but not in their config.
//
// Related: spec 023-oauth-state-persistence
func TestRefreshOAuthToken_DynamicOAuthDiscovery(t *testing.T) {
	logger := zap.NewNop()
	sugaredLogger := logger.Sugar()

	// Create a server config WITHOUT OAuth block (simulates dynamic OAuth discovery)
	// This is how servers like atlassian-remote, slack work - they discover OAuth
	// requirements at runtime via Protected Resource Metadata
	serverConfig := &config.ServerConfig{
		Name:     "test-dynamic-oauth",
		URL:      "https://example.com/mcp",
		Protocol: "http",
		Enabled:  true,
		Created:  time.Now(),
		// NOTE: No OAuth field set - this is the key part of the test
		// OAuth was discovered at runtime, not configured statically
	}

	// Create an in-memory storage with OAuth tokens for this server
	// This simulates a server that authenticated via dynamic OAuth discovery
	tempDir := t.TempDir()
	db, err := storage.NewBoltDB(tempDir, sugaredLogger)
	require.NoError(t, err)
	defer db.Close()

	// Generate the server key using the same function as PersistentTokenStore
	// This is critical - tokens are stored with key = hash(name|url), not just name
	serverKey := oauth.GenerateServerKey(serverConfig.Name, serverConfig.URL)

	// Store an OAuth token for the server (as if it had authenticated previously)
	// The ServerName field is used as the storage key (must match GenerateServerKey output)
	token := &storage.OAuthTokenRecord{
		ServerName:   serverKey,             // Key used for storage lookup (hash-based)
		DisplayName:  "test-dynamic-oauth",  // Human-readable name for RefreshManager
		AccessToken:  "expired-access-token",
		RefreshToken: "valid-refresh-token",
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(-1 * time.Hour), // Expired
		Created:      time.Now().Add(-2 * time.Hour),
		Updated:      time.Now().Add(-1 * time.Hour),
	}
	err = db.SaveOAuthToken(token)
	require.NoError(t, err)

	// Verify token was saved with the correct key
	savedToken, err := db.GetOAuthToken(serverKey)
	require.NoError(t, err)
	require.NotNil(t, savedToken, "Token should be saved in database with server_key")
	assert.Equal(t, "valid-refresh-token", savedToken.RefreshToken)

	// Create the manager with a client for this server
	manager := &Manager{
		clients:        make(map[string]*managed.Client),
		logger:         logger,
		storage:        db,
		secretResolver: secret.NewResolver(),
	}

	// Create a managed client for the server
	client, err := managed.NewClient(
		"test-dynamic-oauth",
		serverConfig,
		logger,
		nil,              // logConfig
		&config.Config{}, // globalConfig
		db,               // bolt storage
		secret.NewResolver(),
	)
	require.NoError(t, err)
	manager.clients["test-dynamic-oauth"] = client

	// Attempt to refresh the OAuth token
	// BUG: This currently fails with "server does not use OAuth: test-dynamic-oauth"
	// because it checks serverConfig.OAuth which is nil
	err = manager.RefreshOAuthToken("test-dynamic-oauth")

	// The refresh should NOT fail with "server does not use OAuth"
	// It should either:
	// 1. Successfully trigger a token refresh, or
	// 2. Fail with a different error (network, invalid token, etc.)
	if err != nil {
		assert.NotContains(t, err.Error(), "server does not use OAuth",
			"RefreshOAuthToken should not fail just because OAuth is not in static config. "+
				"The server has OAuth tokens in the database from dynamic discovery.")
	}
}

// TestRefreshOAuthToken_StaticOAuthConfig tests the happy path where OAuth
// is configured statically in the server config.
func TestRefreshOAuthToken_StaticOAuthConfig(t *testing.T) {
	logger := zap.NewNop()
	sugaredLogger := logger.Sugar()

	// Create a server config WITH OAuth block (traditional static config)
	serverConfig := &config.ServerConfig{
		Name:     "test-static-oauth",
		URL:      "https://example.com/mcp",
		Protocol: "http",
		Enabled:  true,
		Created:  time.Now(),
		OAuth: &config.OAuthConfig{
			ClientID: "test-client-id",
			Scopes:   []string{"read", "write"},
		},
	}

	tempDir := t.TempDir()
	db, err := storage.NewBoltDB(tempDir, sugaredLogger)
	require.NoError(t, err)
	defer db.Close()

	manager := &Manager{
		clients:        make(map[string]*managed.Client),
		logger:         logger,
		storage:        db,
		secretResolver: secret.NewResolver(),
	}

	client, err := managed.NewClient(
		"test-static-oauth",
		serverConfig,
		logger,
		nil,
		&config.Config{},
		db,
		secret.NewResolver(),
	)
	require.NoError(t, err)
	manager.clients["test-static-oauth"] = client

	// This should not fail with "server does not use OAuth"
	// It may fail with connection errors, but that's expected in a unit test
	err = manager.RefreshOAuthToken("test-static-oauth")

	// Should not fail with the OAuth detection error
	if err != nil {
		assert.NotContains(t, err.Error(), "server does not use OAuth")
	}
}

// TestRefreshOAuthToken_ServerNotFound tests that non-existent servers return proper error.
func TestRefreshOAuthToken_ServerNotFound(t *testing.T) {
	logger := zap.NewNop()

	manager := &Manager{
		clients: make(map[string]*managed.Client),
		logger:  logger,
	}

	err := manager.RefreshOAuthToken("non-existent-server")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "server not found")
}

// TestScanForNewTokens_ClearsOAuthErrorWhenTokenPresent reproduces the OAuth
// token-conflict loop: after a fresh `auth login`, the new token is persisted,
// but the client is still flagged IsOAuthError. scanForNewTokens detects the
// token and calls RetryConnection — which (since 8e31b1e) bails out on the
// IsOAuthError guard, so the reconnect never runs, the flag is never cleared on
// success, and the daemon loops forever on "Detected persisted OAuth token;
// triggering reconnect". A disable+enable works only because it rebuilds the
// client with a fresh StateManager.
//
// The fix: when scanForNewTokens sees a freshly-persisted token (the user's
// browser action), it must clear the OAuth-error gate so RetryConnection
// actually attempts the reconnect.
func TestScanForNewTokens_ClearsOAuthErrorWhenTokenPresent(t *testing.T) {
	logger := zap.NewNop()
	sugaredLogger := logger.Sugar()

	serverConfig := &config.ServerConfig{
		Name:     "test-oauth-recovery",
		URL:      "http://127.0.0.1:1/mcp", // unroutable: background Connect fails fast
		Protocol: "http",
		Enabled:  true,
		Created:  time.Now(),
	}

	tempDir := t.TempDir()
	db, err := storage.NewBoltDB(tempDir, sugaredLogger)
	require.NoError(t, err)
	defer db.Close()

	// Persist a FRESH (future-expiry) token — simulates the user having just
	// completed `auth login`.
	serverKey := oauth.GenerateServerKey(serverConfig.Name, serverConfig.URL)
	token := &storage.OAuthTokenRecord{
		ServerName:   serverKey,
		DisplayName:  serverConfig.Name,
		AccessToken:  "fresh-access-token",
		RefreshToken: "fresh-refresh-token",
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(1 * time.Hour),
		Created:      time.Now(),
		Updated:      time.Now(),
	}
	require.NoError(t, db.SaveOAuthToken(token))

	manager := &Manager{
		clients:        make(map[string]*managed.Client),
		logger:         logger,
		storage:        db,
		secretResolver: secret.NewResolver(),
		tokenReconnect: make(map[string]time.Time),
	}

	client, err := managed.NewClient(
		serverConfig.Name,
		serverConfig,
		logger,
		nil,
		&config.Config{},
		db,
		secret.NewResolver(),
	)
	require.NoError(t, err)
	manager.clients[serverConfig.Name] = client

	// Put the client into OAuth-error state, as it would be after the token expired.
	client.StateManager.SetOAuthError(errors.New("OAuth authentication required"))
	require.True(t, client.StateManager.IsOAuthError())
	require.Equal(t, types.StateError, client.GetState())

	// Act: the daemon detects the freshly-persisted token.
	manager.scanForNewTokens()

	// Assert: the OAuth-error gate is cleared so RetryConnection can actually
	// reconnect. Before the fix this stays true forever → the reconnect loop.
	// (A background failure to the unroutable URL uses SetError, not
	// SetOAuthError, so it cannot flip this flag back to true.)
	assert.False(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must clear the OAuth-error flag when a fresh token is present")
}
