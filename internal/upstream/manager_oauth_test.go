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
		ServerName:   serverKey,            // Key used for storage lookup (hash-based)
		DisplayName:  "test-dynamic-oauth", // Human-readable name for RefreshManager
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

// newScanTestManager builds a Manager + errored managed.Client wired to a
// fresh BoltDB, for the scanForNewTokens fingerprint-gate tests below. It
// mirrors the setup in TestScanForNewTokens_ClearsOAuthErrorWhenTokenPresent.
func newScanTestManager(t *testing.T, serverName string) (*Manager, *managed.Client, *storage.BoltDB, *config.ServerConfig) {
	t.Helper()
	logger := zap.NewNop()
	sugaredLogger := logger.Sugar()

	serverConfig := &config.ServerConfig{
		Name:     serverName,
		URL:      "http://127.0.0.1:1/mcp", // unroutable: background Connect fails fast
		Protocol: "http",
		Enabled:  true,
		Created:  time.Now(),
	}

	tempDir := t.TempDir()
	db, err := storage.NewBoltDB(tempDir, sugaredLogger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	manager := &Manager{
		clients:          make(map[string]*managed.Client),
		logger:           logger,
		storage:          db,
		secretResolver:   secret.NewResolver(),
		tokenReconnect:   make(map[string]time.Time),
		tokenFingerprint: make(map[string]string),
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

	client.StateManager.SetOAuthError(errors.New("OAuth authentication required"))
	require.True(t, client.StateManager.IsOAuthError())
	require.Equal(t, types.StateError, client.GetState())

	return manager, client, db, serverConfig
}

// saveTestToken persists a token WITH a refresh token — the normal shape,
// and the one that matters for the expired-token recovery case: mcp-go can
// only refresh a token that has one.
func saveTestToken(t *testing.T, db *storage.BoltDB, cfg *config.ServerConfig, accessToken string, expiresAt time.Time) {
	t.Helper()
	saveTestTokenWithRefresh(t, db, cfg, accessToken, "refresh-"+accessToken, expiresAt)
}

// saveTestTokenNoRefresh persists a token with NO refresh token — the
// genuinely unrecoverable shape scanForNewTokens must skip when expired.
func saveTestTokenNoRefresh(t *testing.T, db *storage.BoltDB, cfg *config.ServerConfig, accessToken string, expiresAt time.Time) {
	t.Helper()
	saveTestTokenWithRefresh(t, db, cfg, accessToken, "", expiresAt)
}

func saveTestTokenWithRefresh(t *testing.T, db *storage.BoltDB, cfg *config.ServerConfig, accessToken, refreshToken string, expiresAt time.Time) {
	t.Helper()
	serverKey := oauth.GenerateServerKey(cfg.Name, cfg.URL)
	token := &storage.OAuthTokenRecord{
		ServerName:   serverKey,
		DisplayName:  cfg.Name,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresAt:    expiresAt,
		Created:      time.Now(),
		Updated:      time.Now(),
	}
	require.NoError(t, db.SaveOAuthToken(token))
}

// TestScanForNewTokens_SkipsExpiredTokenWithNoRefreshToken asserts that an
// expired, persisted token with NO refresh token — genuinely unrecoverable,
// since mcp-go cannot refresh a token that has no refresh token — does not
// clear the OAuth-error gate or trigger a reconnect (D4/R4).
func TestScanForNewTokens_SkipsExpiredTokenWithNoRefreshToken(t *testing.T) {
	manager, client, db, cfg := newScanTestManager(t, "test-scan-expired-no-refresh")
	saveTestTokenNoRefresh(t, db, cfg, "expired-access-token", time.Now().Add(-1*time.Hour))

	manager.scanForNewTokens()

	assert.True(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must NOT clear the OAuth-error flag for an expired token with no refresh token")
	assert.Empty(t, manager.tokenFingerprint,
		"the expiry+no-refresh skip must happen before recording a fingerprint, so it can't poison the unchanged-token gate")
}

// TestScanForNewTokens_FiresOnExpiredTokenWithRefreshToken asserts that an
// expired access token that STILL HAS a refresh token fires once. This is
// the normal overnight case: the daemon survives a laptop sleep, tokens
// expire mid-run, and RetryConnection's Connect drives mcp-go's
// getValidToken to perform the refresh grant with no user action.
//
// RefreshManager does not cover this at runtime — it only refreshes
// already-expired tokens during daemon startup (executeStartupRefreshes);
// its runtime scheduling path explicitly skips a token that's already
// expired. Every other reconnect path (ConnectAll, the runtime ticker,
// supervisor reconcile) is gated on IsOAuthError. So scanForNewTokens is the
// only lazy recovery for this case — treating "expired" as always-skip
// (an earlier, over-broad version of this gate) would leave these servers
// dead until a manual `auth login`.
func TestScanForNewTokens_FiresOnExpiredTokenWithRefreshToken(t *testing.T) {
	manager, client, db, cfg := newScanTestManager(t, "test-scan-expired-with-refresh")
	saveTestToken(t, db, cfg, "expired-access-token", time.Now().Add(-1*time.Hour))

	manager.scanForNewTokens()

	assert.False(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must still fire once for an expired token that has a refresh token, so Connect can refresh it")
	assert.Equal(t, tokenFingerprint("expired-access-token"), manager.tokenFingerprint[cfg.Name],
		"the fingerprint must be recorded so a later scan on this same token — refreshed or not — doesn't refire")
}

// TestScanForNewTokens_SkipsUnchangedToken is the test carrying the entire
// anti-storm guarantee: once scanForNewTokens has acted on a token
// (recovered it, or made the one doomed attempt on a token whose refresh
// token also turned out to be dead), it must never act on that same token
// again. This — not the expiry check — is what stops D4: an expired token
// with a refresh token is now deliberately allowed to fire (see
// TestScanForNewTokens_FiresOnExpiredTokenWithRefreshToken), so nothing
// but "already seen this exact fingerprint" bounds it to one attempt.
//
// Note: this seeds manager.tokenFingerprint directly to simulate "already
// fired on this token", rather than driving two real scanForNewTokens calls
// back-to-back. RetryConnection spawns a background goroutine
// (Disconnect→Connect) that calls StateManager.Reset() — which clears
// isOAuthError as a side effect of disconnecting, independent of anything
// under test here — so a real first scan races that goroutine against a
// synchronous second scan in the test. Seeding the fingerprint isolates the
// gate itself from that unrelated timing.
func TestScanForNewTokens_SkipsUnchangedToken(t *testing.T) {
	manager, client, db, cfg := newScanTestManager(t, "test-scan-unchanged")
	saveTestToken(t, db, cfg, "same-access-token", time.Now().Add(1*time.Hour))

	// Simulate scanForNewTokens having already fired for this exact token on
	// a prior tick.
	manager.tokenFingerprint[cfg.Name] = tokenFingerprint("same-access-token")
	oauthRetryCountBefore := client.StateManager.GetConnectionInfo().OAuthRetryCount

	manager.scanForNewTokens()

	// Not just "still errored": nothing about the OAuth-error state moved at
	// all. If the gate had cleared and immediately re-set (e.g. a bug that
	// fires, fails, and re-enters SetOAuthError), IsOAuthError() alone
	// couldn't tell the difference from "never touched" — OAuthRetryCount
	// can, since ClearOAuthError resets it to 0 and SetOAuthError increments
	// it. Skipped and untouched must leave it exactly where it was.
	assert.True(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must not re-fire for an unchanged token fingerprint")
	assert.Equal(t, oauthRetryCountBefore, client.StateManager.GetConnectionInfo().OAuthRetryCount,
		"an unchanged token must not touch the OAuth-error state at all, not even clear-then-immediately-re-set")
	assert.Equal(t, tokenFingerprint("same-access-token"), manager.tokenFingerprint[cfg.Name],
		"the recorded fingerprint itself must be untouched by a skipped scan")
}

// TestScanForNewTokens_FiresOnRotatedToken asserts that when the persisted
// token's fingerprint differs from the last one this server fired on (a
// genuinely new token, e.g. after another `auth login`), the scan fires
// again. See TestScanForNewTokens_SkipsUnchangedToken for why the "prior
// fingerprint" is seeded directly instead of produced by a real first scan.
func TestScanForNewTokens_FiresOnRotatedToken(t *testing.T) {
	manager, client, db, cfg := newScanTestManager(t, "test-scan-rotated")

	// Simulate scanForNewTokens having already fired on an older token.
	manager.tokenFingerprint[cfg.Name] = tokenFingerprint("old-access-token")

	// A rotated token: different access token, still unexpired.
	saveTestToken(t, db, cfg, "new-access-token", time.Now().Add(1*time.Hour))

	manager.scanForNewTokens()

	assert.False(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must fire again when the token fingerprint changes")
	assert.Equal(t, tokenFingerprint("new-access-token"), manager.tokenFingerprint[cfg.Name],
		"scanForNewTokens must record the fingerprint it fired on, so a future unchanged scan can be gated")
}

// TestScanForNewTokens_ZeroExpiryStillFires asserts that a persisted token
// with a zero ExpiresAt (expiry unknown, as observed live for notiongusto
// and gmailgusto) is treated as usable, not expired, and still fires the
// post-login self-heal.
func TestScanForNewTokens_ZeroExpiryStillFires(t *testing.T) {
	manager, client, db, cfg := newScanTestManager(t, "test-scan-zero-expiry")
	saveTestToken(t, db, cfg, "zero-expiry-access-token", time.Time{})

	manager.scanForNewTokens()

	assert.False(t, client.StateManager.IsOAuthError(),
		"scanForNewTokens must treat a zero ExpiresAt as unknown, not expired, and still fire")
	assert.Equal(t, tokenFingerprint("zero-expiry-access-token"), manager.tokenFingerprint[cfg.Name],
		"a zero-expiry token that fires must still be recorded, so a later unchanged scan is gated")
}
