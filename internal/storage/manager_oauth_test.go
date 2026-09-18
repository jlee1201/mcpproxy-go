package storage_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Verify ClearOAuthState removes both legacy (server name) and hashed serverKey tokens.
func TestManager_ClearOAuthState_RemovesHashedToken(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-clear-oauth-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	serverName := "demo"
	serverURL := "https://example.com"
	serverKey := oauth.GenerateServerKey(serverName, serverURL)

	// Seed tokens under both the legacy name and the hashed serverKey
	err = mgr.GetBoltDB().SaveOAuthToken(&storage.OAuthTokenRecord{ServerName: serverName, AccessToken: "legacy"})
	require.NoError(t, err)

	err = mgr.GetBoltDB().SaveOAuthToken(&storage.OAuthTokenRecord{ServerName: serverKey, AccessToken: "hashed"})
	require.NoError(t, err)

	// Clear and verify both are gone
	require.NoError(t, mgr.ClearOAuthState(serverName))

	_, err = mgr.GetBoltDB().GetOAuthToken(serverName)
	require.Error(t, err)

	_, err = mgr.GetBoltDB().GetOAuthToken(serverKey)
	require.Error(t, err)
}

// TestManager_CleanupStaleServerData_DeletesRealOAuthToken is the regression
// test for round-5's finding that round-4's CleanupStaleServerData OAuth fix
// was a silent no-op: it deleted by plain ServerName, but production tokens
// are stored under oauth.GenerateServerKey(name, url) via
// PersistentTokenStore, so the delete call always succeeded while deleting
// nothing. This test goes through the real PersistentTokenStore.SaveToken
// path (not a hand-written bucket entry under an assumed key) so it can't
// pass on a false key-shape assumption the way the original test did.
func TestManager_CleanupStaleServerData_DeletesRealOAuthToken(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-cleanup-oauth-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	serverName := "dropped-from-config"
	serverURL := "https://example.com/dropped-from-config"

	_, err = mgr.RegisterServerIdentity(&config.ServerConfig{Name: serverName, URL: serverURL}, "/tmp/mcp_config.json")
	require.NoError(t, err)

	tokenStore := oauth.NewPersistentTokenStore(serverName, serverURL, mgr.GetBoltDB())
	require.NoError(t, tokenStore.SaveToken(context.Background(), &client.Token{AccessToken: "some-access-token"}))

	// A negative threshold makes even a just-registered (LastSeen=now)
	// identity count as stale (time.Since(now) > -1s), so this doesn't need
	// to fake LastSeen the way retention_test.go's registerStaleIdentity
	// does -- it can use only the public Manager API.
	deleted, err := mgr.CleanupStaleServerData(-1*time.Second, map[string]bool{})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = tokenStore.GetToken(context.Background())
	assert.ErrorIs(t, err, transport.ErrNoToken, "oauth token for a removed-and-cleaned-up server must be deleted, not left behind forever")
}

// TestManager_CleanupStaleServerData_PreservesRealOAuthTokenForLiveServerSharingName
// covers the exact-key-delete guarantee: if a still-live identity happens to
// share its ServerName with the stale identity being cleaned up (e.g. the
// same name re-added with a new URL, which changes the hashed OAuth key and
// the content-hashed identity ID but not the name), deleting the stale
// identity's OAuth token must NOT touch the live identity's token, even
// though they share a ServerName.
func TestManager_CleanupStaleServerData_PreservesRealOAuthTokenForLiveServerSharingName(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-cleanup-oauth-shared-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	sharedName := "shared-server-name"
	staleURL := "https://example.com/" + sharedName + "-v1"
	liveURL := "https://example.com/" + sharedName + "-v2"

	staleIdentity, err := mgr.RegisterServerIdentity(&config.ServerConfig{Name: sharedName, URL: staleURL}, "/tmp/mcp_config.json")
	require.NoError(t, err)
	liveIdentity, err := mgr.RegisterServerIdentity(&config.ServerConfig{Name: sharedName, URL: liveURL}, "/tmp/mcp_config.json")
	require.NoError(t, err)
	require.NotEqual(t, staleIdentity.ID, liveIdentity.ID, "test setup requires two distinct identities sharing one ServerName")

	staleStore := oauth.NewPersistentTokenStore(sharedName, staleURL, mgr.GetBoltDB())
	require.NoError(t, staleStore.SaveToken(context.Background(), &client.Token{AccessToken: "stale-servers-token"}))

	liveStore := oauth.NewPersistentTokenStore(sharedName, liveURL, mgr.GetBoltDB())
	require.NoError(t, liveStore.SaveToken(context.Background(), &client.Token{AccessToken: "live-servers-token"}))

	// Both identities have LastSeen=now, so the negative threshold marks
	// both stale; liveIdentity survives only because it's still configured.
	deleted, err := mgr.CleanupStaleServerData(-1*time.Second, map[string]bool{
		liveIdentity.ID: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = staleStore.GetToken(context.Background())
	assert.ErrorIs(t, err, transport.ErrNoToken, "the removed identity's own token must be deleted")

	token, err := liveStore.GetToken(context.Background())
	require.NoError(t, err, "the live identity's token must survive even though a stale identity shared its ServerName")
	assert.Equal(t, "live-servers-token", token.AccessToken)
}

// TestBoltDB_UpdateOAuthClientCredentials_WithCallbackPort verifies that UpdateOAuthClientCredentials
// stores and retrieves the callback port alongside client credentials (Spec 022).
func TestBoltDB_UpdateOAuthClientCredentials_WithCallbackPort(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-oauth-port-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	serverKey := "test-server_abc123"
	clientID := "dcr-client-id"
	clientSecret := "dcr-client-secret"
	callbackPort := 54321

	// Store credentials with callback port
	err = mgr.GetBoltDB().UpdateOAuthClientCredentials(serverKey, clientID, clientSecret, callbackPort)
	require.NoError(t, err)

	// Retrieve and verify
	gotClientID, gotClientSecret, gotPort, err := mgr.GetBoltDB().GetOAuthClientCredentials(serverKey)
	require.NoError(t, err)
	require.Equal(t, clientID, gotClientID)
	require.Equal(t, clientSecret, gotClientSecret)
	require.Equal(t, callbackPort, gotPort)
}

// TestBoltDB_GetOAuthClientCredentials_LegacyRecord verifies that GetOAuthClientCredentials
// returns 0 for legacy records that don't have a callback port (backward compatibility).
func TestBoltDB_GetOAuthClientCredentials_LegacyRecord(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-oauth-legacy-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	serverKey := "legacy-server_xyz789"

	// Save a legacy-style token record (only ClientID/ClientSecret, no CallbackPort)
	err = mgr.GetBoltDB().SaveOAuthToken(&storage.OAuthTokenRecord{
		ServerName:   serverKey,
		AccessToken:  "test-token",
		ClientID:     "legacy-client-id",
		ClientSecret: "legacy-client-secret",
		// CallbackPort not set - should default to 0
	})
	require.NoError(t, err)

	// Retrieve credentials - port should be 0
	gotClientID, gotClientSecret, gotPort, err := mgr.GetBoltDB().GetOAuthClientCredentials(serverKey)
	require.NoError(t, err)
	require.Equal(t, "legacy-client-id", gotClientID)
	require.Equal(t, "legacy-client-secret", gotClientSecret)
	require.Equal(t, 0, gotPort, "Legacy records should return port 0")
}

// TestBoltDB_ClearOAuthClientCredentials_PreservesToken verifies that ClearOAuthClientCredentials
// clears DCR fields but preserves token data (Spec 022).
func TestBoltDB_ClearOAuthClientCredentials_PreservesToken(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-oauth-clear-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	mgr, err := storage.NewManager(tmpDir, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	serverKey := "clear-test-server_def456"

	// First, save a complete OAuth token record with DCR credentials and token
	err = mgr.GetBoltDB().SaveOAuthToken(&storage.OAuthTokenRecord{
		ServerName:   serverKey,
		AccessToken:  "access-token-123",
		RefreshToken: "refresh-token-456",
		ClientID:     "dcr-client-id",
		ClientSecret: "dcr-client-secret",
		CallbackPort: 12345,
		RedirectURI:  "http://127.0.0.1:12345/oauth/callback",
	})
	require.NoError(t, err)

	// Clear DCR credentials
	err = mgr.GetBoltDB().ClearOAuthClientCredentials(serverKey)
	require.NoError(t, err)

	// Verify token is still accessible
	record, err := mgr.GetBoltDB().GetOAuthToken(serverKey)
	require.NoError(t, err)
	require.Equal(t, "access-token-123", record.AccessToken, "Access token should be preserved")
	require.Equal(t, "refresh-token-456", record.RefreshToken, "Refresh token should be preserved")

	// Verify DCR credentials are cleared
	require.Empty(t, record.ClientID, "ClientID should be cleared")
	require.Empty(t, record.ClientSecret, "ClientSecret should be cleared")
	require.Equal(t, 0, record.CallbackPort, "CallbackPort should be cleared")
	require.Empty(t, record.RedirectURI, "RedirectURI should be cleared")
}
