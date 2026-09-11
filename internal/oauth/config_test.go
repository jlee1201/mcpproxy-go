package oauth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"

	"github.com/mark3labs/mcp-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupTestStorage(t *testing.T) *storage.BoltDB {
	t.Helper()
	logger := zap.NewNop().Sugar()
	// NewBoltDB expects a directory, not a file path
	db, err := storage.NewBoltDB(t.TempDir(), logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func TestCreateOAuthConfig_WithExtraParams(t *testing.T) {
	// Test that CreateOAuthConfig correctly uses extra_params from config
	storage := setupTestStorage(t)
	serverConfig := &config.ServerConfig{
		Name: "test-server",
		URL:  "https://example.com/mcp",
		OAuth: &config.OAuthConfig{
			ClientID: "test-client",
			ExtraParams: map[string]string{
				"resource": "https://mcp.example.com/api",
				"custom":   "value",
			},
		},
	}

	oauthConfig := CreateOAuthConfig(serverConfig, storage)

	require.NotNil(t, oauthConfig)
	// The OAuth config should be created with the provided configuration
	assert.Equal(t, "test-client", oauthConfig.ClientID)
}

func TestIsOAuthCapable(t *testing.T) {
	tests := []struct {
		name     string
		config   *config.ServerConfig
		expected bool
	}{
		{
			name:     "explicit OAuth config",
			config:   &config.ServerConfig{OAuth: &config.OAuthConfig{}},
			expected: true,
		},
		{
			name:     "HTTP protocol without OAuth",
			config:   &config.ServerConfig{Protocol: "http"},
			expected: true,
		},
		{
			name:     "SSE protocol without OAuth",
			config:   &config.ServerConfig{Protocol: "sse"},
			expected: true,
		},
		{
			name:     "stdio protocol without OAuth",
			config:   &config.ServerConfig{Protocol: "stdio"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsOAuthCapable(tt.config)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// MockTokenStore implements client.TokenStore for testing
type MockTokenStore struct {
	token *client.Token
	err   error
}

func (m *MockTokenStore) GetToken(ctx context.Context) (*client.Token, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.token, nil
}

func (m *MockTokenStore) SaveToken(ctx context.Context, token *client.Token) error {
	m.token = token
	return nil
}

func (m *MockTokenStore) DeleteToken(ctx context.Context) error {
	m.token = nil
	return nil
}

// TestTokenStoreManager_HasValidToken_NoStore validates false when no token store exists
func TestTokenStoreManager_HasValidToken_NoStore(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	result := manager.HasValidToken(context.Background(), "nonexistent-server", nil)

	assert.False(t, result, "Expected false for nonexistent server")
}

// TestTokenStoreManager_HasValidToken_InMemoryStore validates true for in-memory stores
func TestTokenStoreManager_HasValidToken_InMemoryStore(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create in-memory token store
	memStore := client.NewMemoryTokenStore()
	manager.stores["test-server"] = memStore

	result := manager.HasValidToken(context.Background(), "test-server", nil)

	assert.True(t, result, "Expected true for in-memory store (no expiration checking)")
}

// TestTokenStoreManager_HasValidToken_MockStore_NoToken validates behavior with mock that doesn't match PersistentTokenStore
func TestTokenStoreManager_HasValidToken_MockStore_NoToken(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create mock store with no token (returns error)
	// Note: MockTokenStore doesn't match *PersistentTokenStore type,
	// so HasValidToken() will treat it as an in-memory store and return true
	mockStore := &MockTokenStore{
		token: nil,
		err:   fmt.Errorf("token not found"),
	}
	manager.stores["test-server"] = mockStore

	// Create temporary test storage
	tempDir := t.TempDir()
	testStorage, err := storage.NewManager(tempDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer testStorage.Close()

	result := manager.HasValidToken(context.Background(), "test-server", testStorage.GetBoltDB())

	// MockTokenStore falls through to in-memory behavior (returns true)
	assert.True(t, result, "Mock store is treated as in-memory (always valid)")
}

// TestTokenStoreManager_HasValidToken_MockStore_ValidToken validates mock with valid token
func TestTokenStoreManager_HasValidToken_MockStore_ValidToken(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create mock store with valid token (expires in 1 hour)
	// Note: MockTokenStore doesn't match *PersistentTokenStore type,
	// so HasValidToken() treats it as in-memory and returns true
	validToken := &client.Token{
		AccessToken:  "valid-access-token",
		RefreshToken: "valid-refresh-token",
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(1 * time.Hour),
	}
	mockStore := &MockTokenStore{
		token: validToken,
		err:   nil,
	}
	manager.stores["test-server"] = mockStore

	// Create temporary test storage
	tempDir := t.TempDir()
	testStorage, err := storage.NewManager(tempDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer testStorage.Close()

	result := manager.HasValidToken(context.Background(), "test-server", testStorage.GetBoltDB())

	assert.True(t, result, "Mock store is treated as in-memory (always valid)")
}

// TestTokenStoreManager_HasValidToken_MockStore_ExpiredToken validates mock with expired token
func TestTokenStoreManager_HasValidToken_MockStore_ExpiredToken(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create mock store with expired token (expired 1 hour ago)
	// Note: MockTokenStore doesn't match *PersistentTokenStore type,
	// so HasValidToken() treats it as in-memory and returns true (doesn't check expiration)
	expiredToken := &client.Token{
		AccessToken:  "expired-access-token",
		RefreshToken: "expired-refresh-token",
		TokenType:    "Bearer",
		ExpiresAt:    time.Now().Add(-1 * time.Hour),
	}
	mockStore := &MockTokenStore{
		token: expiredToken,
		err:   nil,
	}
	manager.stores["test-server"] = mockStore

	// Create temporary test storage
	tempDir := t.TempDir()
	testStorage, err := storage.NewManager(tempDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer testStorage.Close()

	result := manager.HasValidToken(context.Background(), "test-server", testStorage.GetBoltDB())

	// MockTokenStore is treated as in-memory (doesn't check expiration)
	assert.True(t, result, "Mock store is treated as in-memory (no expiration check)")
}

// TestTokenStoreManager_HasValidToken_PersistentStore_NoExpiration validates true for token with no expiration
func TestTokenStoreManager_HasValidToken_PersistentStore_NoExpiration(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create mock persistent store with token that has no expiration (zero time)
	noExpirationToken := &client.Token{
		AccessToken:  "no-expiration-access-token",
		RefreshToken: "no-expiration-refresh-token",
		TokenType:    "Bearer",
		ExpiresAt:    time.Time{}, // Zero time = no expiration
	}
	mockStore := &MockTokenStore{
		token: noExpirationToken,
		err:   nil,
	}
	manager.stores["test-server"] = mockStore

	// Create temporary test storage
	tempDir := t.TempDir()
	testStorage, err := storage.NewManager(tempDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer testStorage.Close()

	result := manager.HasValidToken(context.Background(), "test-server", testStorage.GetBoltDB())

	assert.True(t, result, "Expected true for token with no expiration (zero time)")
}

// TestTokenStoreManager_HasValidToken_NilStorage validates graceful handling of nil storage
func TestTokenStoreManager_HasValidToken_NilStorage(t *testing.T) {
	manager := &TokenStoreManager{
		stores:         make(map[string]client.TokenStore),
		completedOAuth: make(map[string]time.Time),
		logger:         zap.NewNop().Named("test"),
	}

	// Create in-memory token store (not persistent)
	memStore := client.NewMemoryTokenStore()
	manager.stores["test-server"] = memStore

	// Call with nil storage - should still work for in-memory stores
	result := manager.HasValidToken(context.Background(), "test-server", nil)

	assert.True(t, result, "Expected true for in-memory store with nil storage")
}

// =============================================================================
// T007-T008: Tests for RFC 8707 Resource Auto-Detection (User Story 1)
// =============================================================================

// T007: Test CreateOAuthConfig auto-detects resource from Protected Resource Metadata
func TestCreateOAuthConfig_AutoDetectsResource(t *testing.T) {
	// Variable to hold server URL for use in handler
	var serverURL string

	// Create a mock server that returns Protected Resource Metadata with resource field
	mockMetadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/oauth-protected-resource" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"resource": "https://api.example.com/mcp/v1",
				"authorization_servers": ["https://auth.example.com"],
				"scopes_supported": ["mcp"]
			}`))
			return
		}
		// Return 401 with resource_metadata link in WWW-Authenticate header
		if r.Method == "POST" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="invalid_request", resource_metadata="%s/.well-known/oauth-protected-resource"`, serverURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockMetadataServer.Close()

	// Assign server URL after server is created
	serverURL = mockMetadataServer.URL

	testStorage := setupTestStorage(t)
	serverConfig := &config.ServerConfig{
		Name: "test-resource-detection",
		URL:  mockMetadataServer.URL + "/mcp",
		// No OAuth config - should auto-detect
	}

	// Call CreateOAuthConfigWithExtraParams to get both config and extraParams
	oauthConfig, extraParams := CreateOAuthConfigWithExtraParams(serverConfig, testStorage)

	require.NotNil(t, oauthConfig, "OAuth config should be created")
	require.NotNil(t, extraParams, "Extra params should be returned")

	// Verify resource was auto-detected from Protected Resource Metadata
	resource, hasResource := extraParams["resource"]
	assert.True(t, hasResource, "extraParams should contain 'resource' key")
	assert.Equal(t, "https://api.example.com/mcp/v1", resource, "Resource should be auto-detected from metadata")
}

// T022: Test that manual extra_params.resource overrides auto-detected value
func TestCreateOAuthConfig_ManualOverride(t *testing.T) {
	// Variable to hold server URL for use in handler
	var serverURL string

	// Create a mock server that returns Protected Resource Metadata with resource field
	mockMetadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/oauth-protected-resource" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"resource": "https://auto-detected-resource.example.com/mcp",
				"authorization_servers": ["https://auth.example.com"],
				"scopes_supported": ["mcp"]
			}`))
			return
		}
		// Return 401 with resource_metadata link in WWW-Authenticate header
		if r.Method == "POST" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="invalid_request", resource_metadata="%s/.well-known/oauth-protected-resource"`, serverURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockMetadataServer.Close()

	// Assign server URL after server is created
	serverURL = mockMetadataServer.URL

	testStorage := setupTestStorage(t)
	serverConfig := &config.ServerConfig{
		Name: "test-manual-override",
		URL:  mockMetadataServer.URL + "/mcp",
		OAuth: &config.OAuthConfig{
			ExtraParams: map[string]string{
				"resource": "https://manual-override.example.com/api", // Manual override
			},
		},
	}

	// Call CreateOAuthConfigWithExtraParams
	oauthConfig, extraParams := CreateOAuthConfigWithExtraParams(serverConfig, testStorage)

	require.NotNil(t, oauthConfig, "OAuth config should be created")
	require.NotNil(t, extraParams, "Extra params should be returned")

	// Verify manual resource overrides auto-detected value
	resource, hasResource := extraParams["resource"]
	assert.True(t, hasResource, "extraParams should contain 'resource' key")
	assert.Equal(t, "https://manual-override.example.com/api", resource, "Manual resource should override auto-detected value")
}

// T023: Test that manual extra_params are merged with auto-detected params
func TestCreateOAuthConfig_MergesExtraParams(t *testing.T) {
	// Variable to hold server URL for use in handler
	var serverURL string

	// Create a mock server that returns Protected Resource Metadata
	mockMetadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/oauth-protected-resource" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"resource": "https://auto-detected.example.com/mcp",
				"authorization_servers": ["https://auth.example.com"],
				"scopes_supported": ["mcp"]
			}`))
			return
		}
		if r.Method == "POST" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="invalid_request", resource_metadata="%s/.well-known/oauth-protected-resource"`, serverURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockMetadataServer.Close()

	serverURL = mockMetadataServer.URL

	testStorage := setupTestStorage(t)
	serverConfig := &config.ServerConfig{
		Name: "test-merge-params",
		URL:  mockMetadataServer.URL + "/mcp",
		OAuth: &config.OAuthConfig{
			ExtraParams: map[string]string{
				"tenant_id": "12345",          // Additional param (should be preserved)
				"audience":  "custom-audience", // Additional param (should be preserved)
				// Note: no "resource" - should be auto-detected
			},
		},
	}

	// Call CreateOAuthConfigWithExtraParams
	oauthConfig, extraParams := CreateOAuthConfigWithExtraParams(serverConfig, testStorage)

	require.NotNil(t, oauthConfig, "OAuth config should be created")
	require.NotNil(t, extraParams, "Extra params should be returned")

	// Verify manual params are preserved
	assert.Equal(t, "12345", extraParams["tenant_id"], "Manual tenant_id should be preserved")
	assert.Equal(t, "custom-audience", extraParams["audience"], "Manual audience should be preserved")

	// Verify resource is auto-detected since not manually specified
	resource, hasResource := extraParams["resource"]
	assert.True(t, hasResource, "extraParams should contain auto-detected 'resource' key")
	assert.Equal(t, "https://auto-detected.example.com/mcp", resource, "Resource should be auto-detected")
}

// =============================================================================
// Spec 022: Tests for OAuth Redirect URI Port Persistence
// =============================================================================

// TestStartCallbackServerWithPreferredPort verifies that StartCallbackServer
// uses the preferred port when available.
func TestStartCallbackServerWithPreferredPort(t *testing.T) {
	manager := GetGlobalCallbackManager()

	// Find an available port to use as preferred
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	preferredPort := listener.Addr().(*net.TCPAddr).Port
	listener.Close() // Release the port so we can bind to it again

	// Start callback server with preferred port
	serverName := "test-preferred-port"
	callbackServer, err := manager.StartCallbackServer(serverName, preferredPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServer(serverName) })

	// Verify the callback server is using the preferred port
	assert.Equal(t, preferredPort, callbackServer.Port, "Callback server should use preferred port")
	assert.Contains(t, callbackServer.RedirectURI, fmt.Sprintf(":%d/", preferredPort), "Redirect URI should contain preferred port")
}

// TestStartCallbackServerFallback verifies that StartCallbackServer falls back
// to dynamic allocation when the preferred port is unavailable.
func TestStartCallbackServerFallback(t *testing.T) {
	manager := GetGlobalCallbackManager()

	// Occupy a port first
	occupyListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	occupiedPort := occupyListener.Addr().(*net.TCPAddr).Port
	defer occupyListener.Close() // Keep port occupied during test

	// Try to start callback server with the occupied port
	serverName := "test-fallback-port"
	callbackServer, err := manager.StartCallbackServer(serverName, occupiedPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServer(serverName) })

	// Verify the callback server fell back to a different port
	assert.NotEqual(t, occupiedPort, callbackServer.Port, "Callback server should use different port when preferred is occupied")
	assert.Greater(t, callbackServer.Port, 0, "Callback server should have valid port")
}

// TestStartCallbackServerDynamicPort verifies that StartCallbackServer
// uses dynamic allocation when preferredPort is 0.
func TestStartCallbackServerDynamicPort(t *testing.T) {
	manager := GetGlobalCallbackManager()

	// Start callback server with dynamic port (preferredPort = 0)
	serverName := "test-dynamic-port"
	callbackServer, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServer(serverName) })

	// Verify a valid port was allocated
	assert.Greater(t, callbackServer.Port, 0, "Callback server should have valid port")
	assert.Contains(t, callbackServer.RedirectURI, "http://127.0.0.1:", "Redirect URI should have localhost base")
}

// TestCallbackServer_RoutesByState verifies that a callback is delivered ONLY
// to the waiter that owns its OAuth state, and that a callback for an unknown/
// stale state is dropped instead of poisoning an active waiter. This is the
// regression test for the concurrent-flow "state mismatch" bug where multiple
// flows shared one channel and a callback could be stolen by the wrong flow.
func TestCallbackServer_RoutesByState(t *testing.T) {
	manager := GetGlobalCallbackManager()
	serverName := "test-route-by-state"
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServer(serverName) })

	// Two concurrent flows register their own state-keyed waiters.
	chA := cb.Register("state-A")
	chB := cb.Register("state-B")

	// A callback arrives for state-A only.
	resp, err := http.Get(fmt.Sprintf("%s?state=state-A&code=code-A", cb.RedirectURI))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "matched callback should return 200")

	// Waiter A receives exactly its callback...
	select {
	case params := <-chA:
		assert.Equal(t, "state-A", params["state"])
		assert.Equal(t, "code-A", params["code"])
	case <-time.After(2 * time.Second):
		t.Fatal("waiter A did not receive its callback")
	}

	// ...and waiter B is untouched (no cross-delivery).
	select {
	case p := <-chB:
		t.Fatalf("waiter B wrongly received a callback: %v", p)
	case <-time.After(200 * time.Millisecond):
		// expected: B still waiting
	}

	// A stale/unknown-state callback is dropped (409) and does NOT poison B.
	resp2, err := http.Get(fmt.Sprintf("%s?state=state-STALE&code=code-X", cb.RedirectURI))
	require.NoError(t, err)
	_ = resp2.Body.Close()
	assert.Equal(t, http.StatusConflict, resp2.StatusCode, "unknown/stale callback should return 409, not 200")

	select {
	case p := <-chB:
		t.Fatalf("waiter B wrongly received the stale callback: %v", p)
	case <-time.After(200 * time.Millisecond):
		// expected: B still waiting, unpoisoned
	}

	cb.Unregister("state-A")
	cb.Unregister("state-B")
}

// TestStopCallbackServer_SurvivesWithRemainingWaiter is the regression test
// for the bug where one OAuth flow completing (state-A) tore the shared
// callback server down out from under a SIBLING flow (state-B) still waiting
// on it - closing state-B's channel and producing a spurious "callback
// channel closed (server shutdown or superseded)" error even though state-B's
// own browser callback hadn't arrived yet. StopCallbackServer must only tear
// the server down once NO flow is still waiting on it.
func TestStopCallbackServer_SurvivesWithRemainingWaiter(t *testing.T) {
	manager := GetGlobalCallbackManager()
	serverName := "test-survives-with-remaining-waiter"
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.StopCallbackServer(serverName) })

	chA := cb.Register("state-A")
	chB := cb.Register("state-B")

	// Flow A completes: it unregisters its OWN state first (as the real
	// markOAuthComplete call site does) and then asks the manager to tear
	// the server down.
	cb.Unregister("state-A")
	err = manager.StopCallbackServer(serverName)
	require.NoError(t, err)

	select {
	case p, ok := <-chA:
		assert.False(t, ok, "state-A channel should be closed (by its own Unregister), not deliver a value: %v", p)
	default:
		t.Fatal("state-A channel should already be closed, not still blocking")
	}

	// Flow B's registration must have survived - and the HTTP listener
	// itself must still be serving, not just the bookkeeping map entry. A
	// stale map entry pointing at a shut-down listener would still fail
	// flow B's real callback.
	resp, err := http.Get(fmt.Sprintf("%s?state=state-B&code=code-B", cb.RedirectURI))
	require.NoError(t, err, "callback server should still be listening for flow B")
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	select {
	case params := <-chB:
		assert.Equal(t, "state-B", params["state"])
		assert.Equal(t, "code-B", params["code"])
	case <-time.After(2 * time.Second):
		t.Fatal("waiter B did not receive its callback after StopCallbackServer ran with a sibling waiter still registered")
	}

	_, exists := manager.GetCallbackServer(serverName)
	assert.True(t, exists, "callback server should still be registered while a waiter remains")

	cb.Unregister("state-B")
}

// TestStopCallbackServer_TearsDownWhenLastWaiterGone verifies the other half
// of the contract: once the LAST flow has unregistered its state,
// StopCallbackServer must actually tear the server down (otherwise a naive
// "never shut down while anyone's waiting" fix would just leak the listener
// forever).
func TestStopCallbackServer_TearsDownWhenLastWaiterGone(t *testing.T) {
	manager := GetGlobalCallbackManager()
	serverName := "test-tears-down-when-last-waiter-gone"
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)

	cb.Register("state-A")
	cb.Unregister("state-A") // last (only) flow releases its own state

	err = manager.StopCallbackServer(serverName)
	require.NoError(t, err)

	_, exists := manager.GetCallbackServer(serverName)
	assert.False(t, exists, "callback server should be removed once no waiters remain")

	// The listener itself should be gone too, not just the map entry.
	_, getErr := http.Get(fmt.Sprintf("%s?state=state-A&code=code-A", cb.RedirectURI))
	assert.Error(t, getErr, "listener should no longer be accepting connections")
}

// TestStopCallbackServer_NoWaitersEverRegistered covers the common case (no
// concurrent flows at all) to guard against a nil-map or off-by-one in the
// idle check.
func TestStopCallbackServer_NoWaitersEverRegistered(t *testing.T) {
	manager := GetGlobalCallbackManager()
	serverName := "test-no-waiters-ever-registered"
	_, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)

	err = manager.StopCallbackServer(serverName)
	require.NoError(t, err)

	_, exists := manager.GetCallbackServer(serverName)
	assert.False(t, exists, "callback server with zero waiters should tear down immediately")
}

// TestStopCallbackServerForce_TearsDownDespiteRemainingWaiters verifies the
// escape hatch for daemon shutdown / test cleanup: it must tear down and
// close every outstanding waiter regardless of how many flows are still in
// flight.
func TestStopCallbackServerForce_TearsDownDespiteRemainingWaiters(t *testing.T) {
	manager := GetGlobalCallbackManager()
	serverName := "test-force-tears-down-despite-waiters"
	cb, err := manager.StartCallbackServer(serverName, 0)
	require.NoError(t, err)

	chA := cb.Register("state-A")
	chB := cb.Register("state-B")

	err = manager.StopCallbackServerForce(serverName)
	require.NoError(t, err)

	_, exists := manager.GetCallbackServer(serverName)
	assert.False(t, exists, "force variant must remove the server even with live waiters")

	for name, ch := range map[string]<-chan map[string]string{"A": chA, "B": chB} {
		select {
		case _, ok := <-ch:
			assert.False(t, ok, "waiter %s's channel should be closed by the force teardown", name)
		case <-time.After(1 * time.Second):
			t.Fatalf("waiter %s's channel was never closed by the force teardown", name)
		}
	}
}

// T008: Test CreateOAuthConfig falls back to server URL when metadata lacks resource field
func TestCreateOAuthConfig_FallsBackToServerURL(t *testing.T) {
	// Variable to hold server URL for use in handler
	var serverURL string

	// Create a mock server that returns Protected Resource Metadata WITHOUT resource field
	mockMetadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/oauth-protected-resource" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Metadata without resource field
			w.Write([]byte(`{
				"authorization_servers": ["https://auth.example.com"],
				"scopes_supported": ["mcp"]
			}`))
			return
		}
		// Return 401 with resource_metadata link in WWW-Authenticate header
		if r.Method == "POST" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="invalid_request", resource_metadata="%s/.well-known/oauth-protected-resource"`, serverURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockMetadataServer.Close()

	// Assign server URL after server is created
	serverURL = mockMetadataServer.URL

	testStorage := setupTestStorage(t)
	serverConfig := &config.ServerConfig{
		Name: "test-fallback",
		URL:  mockMetadataServer.URL + "/mcp",
		// No OAuth config - should auto-detect and fallback
	}

	// Call CreateOAuthConfigWithExtraParams to get both config and extraParams
	oauthConfig, extraParams := CreateOAuthConfigWithExtraParams(serverConfig, testStorage)

	require.NotNil(t, oauthConfig, "OAuth config should be created")
	require.NotNil(t, extraParams, "Extra params should be returned")

	// Verify resource falls back to server URL when metadata doesn't provide it
	resource, hasResource := extraParams["resource"]
	assert.True(t, hasResource, "extraParams should contain 'resource' key (fallback)")
	assert.Equal(t, mockMetadataServer.URL+"/mcp", resource, "Resource should fall back to server URL")
}
