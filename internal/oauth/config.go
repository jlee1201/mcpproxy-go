package oauth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"

	"github.com/mark3labs/mcp-go/client"
	"go.uber.org/zap"
)

const (
	// Default OAuth redirect URI base - port will be dynamically assigned
	DefaultRedirectURIBase = "http://127.0.0.1"
	DefaultRedirectPath    = "/oauth/callback"
)

// CallbackServerManager manages OAuth callback servers for dynamic port allocation
type CallbackServerManager struct {
	servers map[string]*CallbackServer
	mu      sync.RWMutex
	logger  *zap.Logger
}

// CallbackServer represents an active OAuth callback server.
//
// Callbacks are routed to waiters by their OAuth `state` parameter. This is
// deliberate: a single callback server is shared per upstream server name, but
// multiple OAuth flows (a manual `auth login`, an automatic reconnect, a retry)
// can be in flight against it concurrently. Each flow generates a unique random
// `state` and registers a dedicated channel via Register(state). handleCallback
// delivers each callback ONLY to the waiter that owns its state. Without this,
// concurrent waiters raced on one shared channel and a callback could be
// delivered to the wrong flow, producing spurious "state mismatch" rejections
// and discarding a valid token (the completing browser tab always looked
// "successful" to the user while the token was silently dropped).
type CallbackServer struct {
	Port        int
	RedirectURI string
	Server      *http.Server
	logger      *zap.Logger

	mu      sync.Mutex
	waiters map[string]chan map[string]string // OAuth state -> dedicated waiter channel
	closed  bool
}

// Register creates and registers a dedicated callback channel for the given
// OAuth state, returning it for the caller to wait on. The caller MUST call
// Unregister(state) when the flow completes or times out (defer it). The
// channel is buffered so handleCallback never blocks delivering to it.
func (c *CallbackServer) Register(state string) <-chan map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan map[string]string, 1)
	if c.closed {
		// Server is shutting down; hand back an already-closed channel so the
		// caller unblocks immediately rather than waiting for its timeout.
		close(ch)
		return ch
	}
	if c.waiters == nil {
		c.waiters = make(map[string]chan map[string]string)
	}
	c.waiters[state] = ch
	c.logger.Debug("Registered OAuth callback waiter", zap.String("state", state), zap.Int("active_waiters", len(c.waiters)))
	return ch
}

// Unregister removes and closes the waiter channel for the given state. Safe to
// call more than once. Delivery in handleCallback and Unregister are serialized
// by c.mu, so a channel is never closed while a send is in flight.
func (c *CallbackServer) Unregister(state string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.waiters[state]; ok {
		delete(c.waiters, state)
		close(ch)
	}
}

var globalCallbackManager = &CallbackServerManager{
	servers: make(map[string]*CallbackServer),
	logger:  zap.L().Named("oauth-callback"),
}

// Global browser-open throttle, keyed by upstream server name. Unlike the
// per-Client lastOAuthTimestamp, this coordinates across Client instances so an
// automatic reconnect running on a freshly-created Client won't reopen a browser
// tab while another flow for the same server just did (single-flight, defense in
// depth on top of state-routed callbacks). Manual `auth login` flows bypass it.
var (
	browserOpenMu   sync.Mutex
	lastBrowserOpen = make(map[string]time.Time)
)

// RecordBrowserOpen notes that a browser was just opened for server's OAuth flow.
func RecordBrowserOpen(server string) {
	browserOpenMu.Lock()
	lastBrowserOpen[server] = time.Now()
	browserOpenMu.Unlock()
}

// TimeSinceBrowserOpen reports how long since the last recorded browser open for
// server, across all Client instances. Returns a very large duration if none.
func TimeSinceBrowserOpen(server string) time.Duration {
	browserOpenMu.Lock()
	defer browserOpenMu.Unlock()
	t, ok := lastBrowserOpen[server]
	if !ok {
		return 365 * 24 * time.Hour
	}
	return time.Since(t)
}

// Global token store manager to persist tokens across client instances
type TokenStoreManager struct {
	stores                  map[string]client.TokenStore
	completedOAuth          map[string]time.Time // Track successful OAuth completions
	mu                      sync.RWMutex
	logger                  *zap.Logger
	oauthCompletionCallback func(serverName string)                      // Callback when OAuth completes
	tokenSavedCallback      func(serverName string, expiresAt time.Time) // Callback when token is saved
}

var globalTokenStoreManager = &TokenStoreManager{
	stores:         make(map[string]client.TokenStore),
	completedOAuth: make(map[string]time.Time),
	logger:         zap.L().Named("oauth-tokens"),
}

// GetOrCreateTokenStore returns a shared token store for the given server
func (m *TokenStoreManager) GetOrCreateTokenStore(serverName string) client.TokenStore {
	m.mu.Lock()
	defer m.mu.Unlock()

	if store, exists := m.stores[serverName]; exists {
		m.logger.Info("Reusing existing token store",
			zap.String("server", serverName),
			zap.String("note", "tokens should be available if OAuth was completed"))
		return store
	}

	store := client.NewMemoryTokenStore()
	m.stores[serverName] = store
	m.logger.Info("Created new token store", zap.String("server", serverName))
	return store
}

// HasTokenStore checks if a token store exists for the server in memory (for debugging)
// Note: This only checks the in-memory store, not persisted tokens in BBolt.
// Use HasPersistedToken() to check for tokens in persistent storage.
func (m *TokenStoreManager) HasTokenStore(serverName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.stores[serverName]
	return exists
}

// HasPersistedToken checks if a token exists in persistent storage (BBolt) for the server.
// This is the preferred method to check for existing tokens as it reflects actual token availability.
func HasPersistedToken(serverName, serverURL string, boltStorage *storage.BoltDB) (hasToken bool, hasRefreshToken bool, isExpired bool) {
	if boltStorage == nil {
		return false, false, false
	}

	serverKey := GenerateServerKey(serverName, serverURL)
	record, err := boltStorage.GetOAuthToken(serverKey)
	if err != nil || record == nil {
		return false, false, false
	}

	hasToken = record.AccessToken != ""
	hasRefreshToken = record.RefreshToken != ""
	isExpired = time.Now().After(record.ExpiresAt)
	return
}

// GetPersistedRefreshToken retrieves the refresh token from persistent storage if available.
// Returns empty string if no token exists or token has no refresh_token.
func GetPersistedRefreshToken(serverName, serverURL string, boltStorage *storage.BoltDB) string {
	if boltStorage == nil {
		return ""
	}

	serverKey := GenerateServerKey(serverName, serverURL)
	record, err := boltStorage.GetOAuthToken(serverKey)
	if err != nil || record == nil {
		return ""
	}

	return record.RefreshToken
}

// TokenRefreshResult contains the result of a token refresh attempt.
type TokenRefreshResult struct {
	Success     bool
	NewToken    *storage.OAuthTokenRecord
	Error       error
	Attempt     int
	MaxAttempts int
}

// RefreshTokenConfig contains configuration for token refresh operations.
type RefreshTokenConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRefreshConfig returns the default token refresh configuration.
func DefaultRefreshConfig() RefreshTokenConfig {
	return RefreshTokenConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     10 * time.Second,
	}
}

// GetTokenStoreManager returns the global token store manager for debugging
func GetTokenStoreManager() *TokenStoreManager {
	return globalTokenStoreManager
}

// SetOAuthCompletionCallback sets a callback function to be called when OAuth completes
func (m *TokenStoreManager) SetOAuthCompletionCallback(callback func(serverName string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.oauthCompletionCallback = callback
}

// SetTokenSavedCallback sets a callback function to be called when a token is saved.
// Used by RefreshManager to reschedule proactive refresh when tokens are updated.
func (m *TokenStoreManager) SetTokenSavedCallback(callback func(serverName string, expiresAt time.Time)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenSavedCallback = callback
}

// NotifyTokenSaved triggers the token saved callback if set.
// Called by PersistentTokenStore.SaveToken() to notify the RefreshManager.
func (m *TokenStoreManager) NotifyTokenSaved(serverName string, expiresAt time.Time) {
	m.mu.RLock()
	callback := m.tokenSavedCallback
	m.mu.RUnlock()

	if callback != nil {
		callback(serverName, expiresAt)
	}
}

// MarkOAuthCompleted records that OAuth was successfully completed for a server
// This method is used by CLI processes to notify server processes about OAuth completion
func (m *TokenStoreManager) MarkOAuthCompleted(serverName string) {
	m.mu.Lock()
	callback := m.oauthCompletionCallback
	completionTime := time.Now()
	m.completedOAuth[serverName] = completionTime
	m.mu.Unlock()

	m.logger.Info("OAuth completion recorded",
		zap.String("server", serverName),
		zap.Time("completion_time", completionTime))

	// Trigger in-process callback if available (for same process)
	if callback != nil {
		m.logger.Info("Triggering in-process OAuth completion callback",
			zap.String("server", serverName))
		callback(serverName)
	} else {
		m.logger.Info("No in-process callback registered - OAuth completion will be handled via database events",
			zap.String("server", serverName))
	}
}

// MarkOAuthCompletedWithDB records OAuth completion in the database for cross-process notification
// This is the new preferred method that works across different processes
func (m *TokenStoreManager) MarkOAuthCompletedWithDB(serverName string, storage DatabaseOAuthNotifier) error {
	m.mu.Lock()
	completionTime := time.Now()
	m.completedOAuth[serverName] = completionTime
	m.mu.Unlock()

	// Save to database for cross-process notification
	event := &CompletionEvent{
		ServerName:  serverName,
		CompletedAt: completionTime,
	}

	if err := storage.SaveOAuthCompletionEvent(event); err != nil {
		m.logger.Error("Failed to save OAuth completion event to database",
			zap.String("server", serverName),
			zap.Error(err))
		return err
	}

	m.logger.Info("OAuth completion saved to database for cross-process notification",
		zap.String("server", serverName),
		zap.Time("completion_time", completionTime))

	return nil
}

// DatabaseOAuthNotifier interface for database-based OAuth completion notifications
type DatabaseOAuthNotifier interface {
	SaveOAuthCompletionEvent(event *CompletionEvent) error
}

// OAuthCompletionEvent represents an OAuth completion event (re-exported from storage)
type CompletionEvent = storage.OAuthCompletionEvent

// HasRecentOAuthCompletion checks if OAuth was recently completed for a server
func (m *TokenStoreManager) HasRecentOAuthCompletion(serverName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	completionTime, exists := m.completedOAuth[serverName]
	if !exists {
		return false
	}

	// Consider OAuth recent if completed within last 5 minutes
	isRecent := time.Since(completionTime) < 5*time.Minute
	m.logger.Debug("Checking OAuth completion status",
		zap.String("server", serverName),
		zap.Time("completion_time", completionTime),
		zap.Bool("is_recent", isRecent))

	return isRecent
}

// HasValidToken checks if a server has a valid, non-expired OAuth token
// Returns true if token exists and hasn't expired (with grace period)
func (m *TokenStoreManager) HasValidToken(ctx context.Context, serverName string, storage *storage.BoltDB) bool {
	m.mu.RLock()
	store, exists := m.stores[serverName]
	m.mu.RUnlock()

	if !exists {
		m.logger.Debug("No token store found for server",
			zap.String("server", serverName))
		return false
	}

	// Try to get token from persistent store if available
	if persistentStore, ok := store.(*PersistentTokenStore); ok && storage != nil {
		token, err := persistentStore.GetToken(ctx)
		if err != nil {
			// No token or error retrieving it
			m.logger.Debug("Failed to retrieve token from persistent store",
				zap.String("server", serverName),
				zap.Error(err))
			return false
		}

		// Check if token is expired (considering grace period)
		now := time.Now()
		if token.ExpiresAt.IsZero() {
			// No expiration time means token is always valid (unusual but possible)
			m.logger.Debug("Token has no expiration, treating as valid",
				zap.String("server", serverName))
			return true
		}

		isExpired := now.After(token.ExpiresAt)
		m.logger.Debug("Token expiration check",
			zap.String("server", serverName),
			zap.Bool("is_expired", isExpired),
			zap.Time("expires_at", token.ExpiresAt),
			zap.Duration("time_until_expiry", token.ExpiresAt.Sub(now)))

		return !isExpired
	}

	// For in-memory stores, just check if store exists
	// (no expiration checking for non-persistent stores)
	m.logger.Debug("Using in-memory token store, assuming valid",
		zap.String("server", serverName))
	return true
}

// CreateOAuthConfigWithExtraParams creates an OAuth configuration and returns auto-detected extra parameters.
// This function implements RFC 8707 resource auto-detection for zero-config OAuth.
//
// The function returns:
//   - *client.OAuthConfig: The OAuth configuration for mcp-go client
//   - map[string]string: Extra parameters (including auto-detected resource) to inject into authorization URL
//
// Resource auto-detection logic (in priority order):
//  1. Manual extra_params.resource from config (highest priority - preserves backward compatibility)
//  2. Auto-detected resource from RFC 9728 Protected Resource Metadata
//  3. Fallback to server URL if metadata is unavailable or lacks resource field
func CreateOAuthConfigWithExtraParams(serverConfig *config.ServerConfig, storage *storage.BoltDB) (*client.OAuthConfig, map[string]string) {
	logger := zap.L().Named("oauth")

	// Initialize extraParams map
	extraParams := make(map[string]string)

	// Priority 1: Check for manual extra_params.resource from config
	if serverConfig.OAuth != nil && len(serverConfig.OAuth.ExtraParams) > 0 {
		for key, value := range serverConfig.OAuth.ExtraParams {
			extraParams[key] = value
		}
		if resource, hasResource := extraParams["resource"]; hasResource {
			logger.Info("Using manual resource parameter from config",
				zap.String("server", serverConfig.Name),
				zap.String("resource", resource))
		}
	}

	// Priority 2 & 3: Auto-detect resource if not manually specified
	if _, hasResource := extraParams["resource"]; !hasResource {
		detectedResource := autoDetectResource(serverConfig, logger)
		if detectedResource != "" {
			extraParams["resource"] = detectedResource
		}
	}

	// Create the base OAuth config, passing extraParams for transport wrapper injection
	oauthConfig := createOAuthConfigInternal(serverConfig, storage, extraParams)

	return oauthConfig, extraParams
}

// autoDetectResource attempts to discover the RFC 8707 resource parameter.
// Returns the detected resource URL, or server URL as fallback, or empty string on failure.
func autoDetectResource(serverConfig *config.ServerConfig, logger *zap.Logger) string {
	// Use a client with timeout to avoid blocking on slow/unreachable servers
	client := &http.Client{Timeout: 5 * time.Second}

	// POST is the only method guaranteed by MCP spec for the main endpoint
	resp, err := client.Post(serverConfig.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		logger.Debug("Failed to make preflight request for resource detection",
			zap.String("server", serverConfig.Name),
			zap.Error(err))
		// Fallback to server URL
		return serverConfig.URL
	}
	defer resp.Body.Close()

	// Check for 401 with WWW-Authenticate header containing resource_metadata
	if resp.StatusCode == http.StatusUnauthorized {
		wwwAuth := resp.Header.Get("WWW-Authenticate")
		metadataURL := ExtractResourceMetadataURL(wwwAuth)

		if metadataURL != "" {
			// Try to fetch Protected Resource Metadata
			metadata, err := DiscoverProtectedResourceMetadata(metadataURL, 5*time.Second)
			if err != nil {
				logger.Debug("Failed to fetch Protected Resource Metadata",
					zap.String("server", serverConfig.Name),
					zap.String("metadata_url", metadataURL),
					zap.Error(err))
				// Fallback to server URL
				return serverConfig.URL
			}

			// Use resource from metadata if available
			if metadata.Resource != "" {
				logger.Info("Auto-detected resource parameter from Protected Resource Metadata (RFC 9728)",
					zap.String("server", serverConfig.Name),
					zap.String("resource", metadata.Resource))
				return metadata.Resource
			}

			// Metadata exists but lacks resource field - fallback to server URL
			logger.Info("Protected Resource Metadata lacks resource field, using server URL as fallback",
				zap.String("server", serverConfig.Name),
				zap.String("fallback_resource", serverConfig.URL))
			return serverConfig.URL
		}

		// No resource_metadata in WWW-Authenticate - fallback to server URL
		logger.Debug("WWW-Authenticate header lacks resource_metadata, using server URL as fallback",
			zap.String("server", serverConfig.Name),
			zap.String("fallback_resource", serverConfig.URL))
		return serverConfig.URL
	}

	// Non-401 response means server doesn't require authentication at this endpoint.
	// Return empty string to avoid adding unnecessary resource parameter to OAuth flows.
	// This is correct behavior: resource parameter is only needed for OAuth-protected servers.
	logger.Debug("Server did not return 401, skipping resource auto-detection",
		zap.String("server", serverConfig.Name),
		zap.Int("status_code", resp.StatusCode))
	return ""
}

// CreateOAuthConfig creates an OAuth configuration for dynamic client registration
// This implements proper callback server coordination required for Cloudflare OAuth
//
// Note: For zero-config OAuth with auto-detected resource parameter, use
// CreateOAuthConfigWithExtraParams() instead, which returns both config and extraParams.
func CreateOAuthConfig(serverConfig *config.ServerConfig, storage *storage.BoltDB) *client.OAuthConfig {
	// Extract manual extra_params from config for backward compatibility
	var extraParams map[string]string
	if serverConfig.OAuth != nil && len(serverConfig.OAuth.ExtraParams) > 0 {
		extraParams = serverConfig.OAuth.ExtraParams
	}
	return createOAuthConfigInternal(serverConfig, storage, extraParams)
}

// createOAuthConfigInternal is the internal implementation that accepts extraParams
// for transport wrapper injection. This enables both manual and auto-detected params
// to be injected into token exchange and refresh requests.
func createOAuthConfigInternal(serverConfig *config.ServerConfig, storage *storage.BoltDB, extraParams map[string]string) *client.OAuthConfig {
	startTime := time.Now()
	logger := zap.L().Named("oauth")

	logger.Debug("🚀 Starting OAuth config creation",
		zap.String("server", serverConfig.Name),
		zap.String("url", serverConfig.URL))

	// Defer logging of total duration
	defer func() {
		logger.Debug("⏱️ OAuth config creation completed",
			zap.String("server", serverConfig.Name),
			zap.Duration("total_duration", time.Since(startTime)))
	}()

	logger.Debug("Creating OAuth config for dynamic registration",
		zap.String("server", serverConfig.Name))

	// Scope discovery waterfall (FR-003):
	// 1. Config-specified scopes (highest priority - manual override)
	// 2. RFC 9728 Protected Resource Metadata
	// 3. RFC 8414 Authorization Server Metadata
	// 4. Empty scopes (server specifies via WWW-Authenticate)
	var scopes []string

	// Priority 1: Check config-specified scopes first (manual override)
	if serverConfig.OAuth != nil && len(serverConfig.OAuth.Scopes) > 0 {
		scopes = serverConfig.OAuth.Scopes
		logger.Info("✅ Using config-specified OAuth scopes",
			zap.String("server", serverConfig.Name),
			zap.Strings("scopes", scopes))
	}

	// Priority 2: Try RFC 9728 Protected Resource Metadata discovery
	if len(scopes) == 0 {
		baseURL, err := parseBaseURL(serverConfig.URL)
		if err == nil && baseURL != "" {
			logger.Debug("Attempting Protected Resource Metadata scope discovery (RFC 9728)",
				zap.String("server", serverConfig.Name),
				zap.String("base_url", baseURL))

			// Make a preflight HEAD request to get WWW-Authenticate header
			resp, err := http.Head(serverConfig.URL)
			if err == nil && resp.StatusCode == 401 {
				wwwAuth := resp.Header.Get("WWW-Authenticate")
				if metadataURL := ExtractResourceMetadataURL(wwwAuth); metadataURL != "" {
					discoveredScopes, err := DiscoverScopesFromProtectedResource(metadataURL, 5*time.Second)
					if err == nil && len(discoveredScopes) > 0 {
						scopes = discoveredScopes
						logger.Info("✅ Auto-discovered OAuth scopes from Protected Resource Metadata (RFC 9728)",
							zap.String("server", serverConfig.Name),
							zap.String("metadata_url", metadataURL),
							zap.Strings("scopes", scopes))
					} else if err != nil {
						logger.Debug("Protected Resource Metadata discovery failed",
							zap.String("server", serverConfig.Name),
							zap.Error(err))
					} else {
						// err == nil but no scopes returned
						logger.Warn("Protected Resource Metadata returned no scopes - some clients wait for this before showing OAuth UI",
							zap.String("server", serverConfig.Name),
							zap.String("metadata_url", metadataURL))
					}
				} else {
					logger.Warn("WWW-Authenticate header missing resource_metadata; OAuth clients may refuse to launch browser until PRM exists",
						zap.String("server", serverConfig.Name),
						zap.Any("www_authenticate", resp.Header["Www-Authenticate"]))
				}
			} else if err != nil {
				logger.Warn("Preflight request for WWW-Authenticate header failed; OAuth clients may not see login button",
					zap.String("server", serverConfig.Name),
					zap.Error(err))
			} else {
				logger.Warn("Preflight request did not return 401; server did not advertise WWW-Authenticate metadata",
					zap.String("server", serverConfig.Name),
					zap.Int("status_code", resp.StatusCode))
			}
		}
	}

	// Priority 3: Fallback to RFC 8414 Authorization Server Metadata
	if len(scopes) == 0 {
		baseURL, err := parseBaseURL(serverConfig.URL)
		if err == nil && baseURL != "" {
			logger.Debug("Attempting Authorization Server Metadata scope discovery (RFC 8414)",
				zap.String("server", serverConfig.Name),
				zap.String("base_url", baseURL))

			discoveredScopes, err := DiscoverScopesFromAuthorizationServer(baseURL, 5*time.Second)
			if err == nil && len(discoveredScopes) > 0 {
				scopes = discoveredScopes
				logger.Info("✅ Auto-discovered OAuth scopes from Authorization Server Metadata (RFC 8414)",
					zap.String("server", serverConfig.Name),
					zap.Strings("scopes", scopes))
			} else if err == nil {
				logger.Warn("Authorization Server Metadata returned no scopes; some OAuth clients will wait until scopes_supported is published",
					zap.String("server", serverConfig.Name),
					zap.String("metadata_url", baseURL+"/.well-known/oauth-authorization-server"))
			} else {
				logger.Warn("Authorization Server Metadata discovery failed",
					zap.String("server", serverConfig.Name),
					zap.String("metadata_url", baseURL+"/.well-known/oauth-authorization-server"),
					zap.Error(err))
			}
		}
	}

	// Priority 4: Final fallback to empty scopes (valid OAuth 2.1)
	if len(scopes) == 0 {
		scopes = []string{}
		logger.Warn("No OAuth scopes discovered; falling back to empty list. Some providers (e.g., Google) require `openid`/`email` to be set manually.",
			zap.String("server", serverConfig.Name),
			zap.String("hint", "Set oauth.scopes in server config or ensure PRM/AS metadata advertises scopes_supported"))
	}

	// Check for stored callback port from previous DCR (Spec 022: OAuth Redirect URI Port Persistence)
	var preferredPort int
	var serverKey string
	if storage != nil {
		serverKey = GenerateServerKey(serverConfig.Name, serverConfig.URL)
		storedClientID, _, storedPort, err := storage.GetOAuthClientCredentials(serverKey)
		if err == nil && storedPort > 0 {
			preferredPort = storedPort
			logger.Info("🔄 Found stored callback port from previous DCR",
				zap.String("server", serverConfig.Name),
				zap.Int("preferred_port", preferredPort))
		} else if err == nil && storedClientID != "" && storedPort == 0 {
			// Spec 022: Legacy DCR credentials exist but port is unknown
			// Clear them to force fresh registration with port tracking
			logger.Warn("⚠️ Legacy DCR credentials found without stored port, clearing for re-registration",
				zap.String("server", serverConfig.Name),
				zap.String("client_id", storedClientID))
			if clearErr := storage.ClearOAuthClientCredentials(serverKey); clearErr != nil {
				logger.Warn("Failed to clear legacy DCR credentials",
					zap.String("server", serverConfig.Name),
					zap.Error(clearErr))
			}
		}
	}

	// Start callback server first to get the exact port (as documented in successful approach)
	logger.Info("🔧 Starting OAuth callback server",
		zap.String("server", serverConfig.Name),
		zap.Int("preferred_port", preferredPort),
		zap.String("approach", "MCPProxy callback server coordination for exact URI matching"))

	// Start our own callback server to get exact port for Cloudflare OAuth
	callbackServer, err := globalCallbackManager.StartCallbackServer(serverConfig.Name, preferredPort)
	if err != nil {
		logger.Error("Failed to start OAuth callback server",
			zap.String("server", serverConfig.Name),
			zap.Error(err))
		return nil
	}

	// Spec 022: Detect port conflict and clear DCR credentials if port changed
	// This forces fresh DCR with the new port
	if preferredPort > 0 && callbackServer.Port != preferredPort {
		logger.Warn("⚠️ Callback port changed, clearing DCR credentials for re-registration",
			zap.String("server", serverConfig.Name),
			zap.Int("stored_port", preferredPort),
			zap.Int("new_port", callbackServer.Port))
		if storage != nil {
			if err := storage.ClearOAuthClientCredentials(serverKey); err != nil {
				logger.Warn("Failed to clear DCR credentials after port change",
					zap.String("server", serverConfig.Name),
					zap.Error(err))
			}
		}
	}

	logger.Info("Using exact redirect URI from allocated callback server",
		zap.String("server", serverConfig.Name),
		zap.String("redirect_uri", callbackServer.RedirectURI),
		zap.Int("port", callbackServer.Port))

	logger.Info("OAuth callback server started successfully",
		zap.String("server", serverConfig.Name),
		zap.String("redirect_uri", callbackServer.RedirectURI),
		zap.Int("port", callbackServer.Port))

	// Try to find a working metadata URL by validating multiple URL patterns
	// Different servers use different URL formats:
	// - Smithery: Uses separate domains (server.smithery.ai/x for MCP, auth.smithery.ai/x for OAuth)
	//   OAuth metadata at: https://auth.smithery.ai/.well-known/oauth-authorization-server/googledrive
	// - Cloudflare: Same domain for MCP and OAuth
	//   OAuth metadata at: https://logs.mcp.cloudflare.com/.well-known/oauth-authorization-server
	var authServerMetadataURL string
	if serverConfig.URL != "" {
		// First, try to discover the auth server URL from Protected Resource Metadata
		// This is necessary for servers like Smithery that use separate domains
		authServerURL := DiscoverAuthServerURL(serverConfig.URL, 5*time.Second)
		urlToUse := serverConfig.URL
		if authServerURL != "" {
			urlToUse = authServerURL
			logger.Info("Using discovered auth server URL for metadata discovery",
				zap.String("server", serverConfig.Name),
				zap.String("mcp_url", serverConfig.URL),
				zap.String("auth_server_url", authServerURL))
		}

		// Now find the working metadata URL using the auth server URL (or server URL as fallback)
		workingURL, err := FindWorkingMetadataURL(urlToUse, 10*time.Second)
		if err != nil {
			logger.Warn("Could not find working OAuth metadata URL, will rely on auto-discovery",
				zap.String("server", serverConfig.Name),
				zap.String("url_tried", urlToUse),
				zap.Error(err))
		} else {
			authServerMetadataURL = workingURL
			logger.Info("Using validated OAuth metadata URL",
				zap.String("server", serverConfig.Name),
				zap.String("metadata_url", authServerMetadataURL))
		}
	} else {
		logger.Info("Skipping OAuth metadata URL - no server URL configured",
			zap.String("server", serverConfig.Name))
	}

	// Use persistent token store to persist tokens across daemon restarts if storage is available
	var tokenStore client.TokenStore
	if storage != nil {
		tokenStore = NewPersistentTokenStore(serverConfig.Name, serverConfig.URL, storage)
		logger.Info("🔧 Using persistent token store for OAuth tokens",
			zap.String("server", serverConfig.Name),
			zap.String("storage", "BBolt database"))

		// Check if token exists in persistent storage
		existingToken, err := tokenStore.GetToken(context.Background())
		if err != nil {
			logger.Info("🔍 No existing token found in persistent storage",
				zap.String("server", serverConfig.Name),
				zap.Error(err))
		} else {
			logger.Info("✅ Found existing token in persistent storage",
				zap.String("server", serverConfig.Name),
				zap.Time("expires_at", existingToken.ExpiresAt),
				zap.Bool("expired", time.Now().After(existingToken.ExpiresAt)))
		}
	} else {
		tokenStore = globalTokenStoreManager.GetOrCreateTokenStore(serverConfig.Name)
		logger.Info("🔧 Using in-memory token store for OAuth tokens (CLI mode)",
			zap.String("server", serverConfig.Name),
			zap.String("storage", "memory"))
	}

	// Create HTTP client with transport wrapper to inject extra params into token requests
	// extraParams may contain auto-detected resource (RFC 8707) or manual config params
	var httpClient *http.Client

	if len(extraParams) > 0 {
		// Log extra params with selective masking for security
		masked := maskExtraParams(extraParams)
		logger.Debug("OAuth extra parameters will be injected into token requests",
			zap.String("server", serverConfig.Name),
			zap.Any("extra_params", masked))

		// Create HTTP client with wrapper to inject extra params into token exchange/refresh
		wrapper := NewOAuthTransportWrapper(http.DefaultTransport, extraParams, logger)
		httpClient = &http.Client{
			Transport: wrapper,
			Timeout:   30 * time.Second,
		}

		logger.Info("✅ Created OAuth HTTP client with extra params wrapper for token requests",
			zap.String("server", serverConfig.Name),
			zap.Int("extra_params_count", len(extraParams)))
	}

	// Check if static OAuth credentials are provided in config
	// If not provided, will attempt DCR or fall back to public client OAuth with PKCE
	var clientID, clientSecret string
	var registrationMode string

	if serverConfig.OAuth != nil && serverConfig.OAuth.ClientID != "" {
		// Use static credentials from config
		clientID = serverConfig.OAuth.ClientID
		clientSecret = serverConfig.OAuth.ClientSecret
		registrationMode = "static credentials"
		logger.Info("✅ Using static OAuth credentials from config",
			zap.String("server", serverConfig.Name),
			zap.String("client_id", clientID))
	} else {
		// Try to load persisted DCR credentials for token refresh
		if storage != nil {
			serverKey := GenerateServerKey(serverConfig.Name, serverConfig.URL)
			persistedClientID, persistedClientSecret, _, err := storage.GetOAuthClientCredentials(serverKey)
			if err == nil && persistedClientID != "" {
				clientID = persistedClientID
				clientSecret = persistedClientSecret
				registrationMode = "persisted DCR credentials"
				logger.Info("✅ Using persisted DCR credentials for token refresh",
					zap.String("server", serverConfig.Name),
					zap.String("client_id", clientID))
			} else {
				// No persisted credentials - will attempt DCR or use public client OAuth with PKCE
				clientID = ""
				clientSecret = ""
				registrationMode = "public client (PKCE)"
				logger.Info("🔓 No persisted DCR credentials found - will attempt DCR or use public client mode",
					zap.String("server", serverConfig.Name),
					zap.String("mode", "Public client OAuth with PKCE"))
			}
		} else {
			// No storage available (CLI mode) - will attempt DCR
			clientID = ""
			clientSecret = ""
			registrationMode = "public client (PKCE)"
			logger.Info("🔓 No storage available - will attempt DCR or use public client mode",
				zap.String("server", serverConfig.Name),
				zap.String("mode", "Public client OAuth with PKCE"))
		}
	}

	oauthConfig := &client.OAuthConfig{
		ClientID:              clientID,
		ClientSecret:          clientSecret,
		RedirectURI:           callbackServer.RedirectURI, // Exact redirect URI with allocated port
		Scopes:                scopes,
		TokenStore:            tokenStore,            // Shared token store for this server
		PKCEEnabled:           true,                  // Always enable PKCE for security
		AuthServerMetadataURL: authServerMetadataURL, // Explicit metadata URL for proper discovery
		HTTPClient:            httpClient,            // Custom HTTP client with extra params wrapper (if configured)
	}

	logger.Info("OAuth config created successfully",
		zap.String("server", serverConfig.Name),
		zap.Strings("scopes", scopes),
		zap.Bool("pkce_enabled", true),
		zap.String("redirect_uri", callbackServer.RedirectURI),
		zap.String("auth_server_metadata_url", authServerMetadataURL),
		zap.String("registration_mode", registrationMode),
		zap.String("discovery_mode", "explicit metadata URL"), // Using explicit metadata URL to avoid discovery timeouts
		zap.String("token_store", "shared"))                   // Using shared token store for token persistence

	return oauthConfig
}

// StartCallbackServer starts a new OAuth callback server for the given server name.
// If preferredPort > 0, it attempts to bind to that port first for redirect URI persistence (Spec 022).
// Falls back to dynamic allocation if the preferred port is unavailable.
func (m *CallbackServerManager) StartCallbackServer(serverName string, preferredPort int) (*CallbackServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we already have a server for this name
	if existing, exists := m.servers[serverName]; exists {
		m.logger.Debug("Reusing existing callback server",
			zap.String("server", serverName),
			zap.Int("port", existing.Port))
		return existing, nil
	}

	var listener net.Listener
	var err error

	// Try preferred port first if specified (Spec 022: OAuth Redirect URI Port Persistence)
	if preferredPort > 0 {
		listener, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", preferredPort))
		if err != nil {
			m.logger.Warn("Preferred port unavailable, falling back to dynamic allocation",
				zap.String("server", serverName),
				zap.Int("preferred_port", preferredPort),
				zap.Error(err))
			// Fall through to dynamic allocation
		} else {
			m.logger.Info("✅ Using preferred port for OAuth callback (port persistence)",
				zap.String("server", serverName),
				zap.Int("port", preferredPort))
		}
	}

	// Fall back to dynamic port allocation if no listener yet
	if listener == nil {
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("failed to allocate dynamic port: %w", err)
		}
	}

	// Extract the dynamically allocated port
	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port
	redirectURI := fmt.Sprintf("%s:%d%s", DefaultRedirectURIBase, port, DefaultRedirectPath)

	// Create HTTP server with dedicated mux
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // Security: prevent Slowloris attacks
		ReadTimeout:       30 * time.Second, // Extended timeout for OAuth discovery
		WriteTimeout:      30 * time.Second, // Extended timeout for OAuth responses
	}

	// Create callback server instance
	callbackServer := &CallbackServer{
		Port:        port,
		RedirectURI: redirectURI,
		Server:      server,
		logger:      m.logger.With(zap.String("server", serverName), zap.Int("port", port)),
		waiters:     make(map[string]chan map[string]string),
	}

	// Set up HTTP handler for OAuth callback
	mux.HandleFunc(DefaultRedirectPath, func(w http.ResponseWriter, r *http.Request) {
		callbackServer.handleCallback(w, r)
	})

	// Add a debug handler for the root path to see all requests
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		callbackServer.logger.Info("📥 HTTP request received on callback server",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("query", r.URL.RawQuery),
			zap.String("user_agent", r.UserAgent()),
			zap.String("remote_addr", r.RemoteAddr))

		if r.URL.Path == DefaultRedirectPath {
			callbackServer.handleCallback(w, r)
		} else {
			w.Header().Set("Content-Type", "text/html")
			debugPage := fmt.Sprintf(`
				<html>
					<body>
						<h1>OAuth Callback Server Debug</h1>
						<p>Path: %s</p>
						<p>Expected: %s</p>
						<p>Server: %s</p>
						<p>Port: %d</p>
					</body>
				</html>
			`, r.URL.Path, DefaultRedirectPath, serverName, port)
			if _, err := w.Write([]byte(debugPage)); err != nil {
				callbackServer.logger.Error("Error writing debug page", zap.Error(err))
			}
		}
	})

	// Start the server using the existing listener
	go func() {
		defer listener.Close()
		callbackServer.logger.Info("Starting OAuth callback server")

		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			callbackServer.logger.Error("OAuth callback server error", zap.Error(err))
		} else {
			callbackServer.logger.Info("OAuth callback server stopped")
		}
	}()

	// Store the server
	m.servers[serverName] = callbackServer

	callbackServer.logger.Info("OAuth callback server started successfully",
		zap.String("redirect_uri", redirectURI))

	return callbackServer, nil
}

// handleCallback handles OAuth callback requests
func (c *CallbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	// NOTE: do NOT log r.URL.RawQuery here — on the authorization-code redirect it
	// is `code=…&state=…`, i.e. the raw authorization code, and this logs at INFO
	// (lands in daemon.log). The sanitized shape is logged just below.
	c.logger.Info("🎯 OAuth callback received",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("remote_addr", r.RemoteAddr),
		zap.String("user_agent", r.UserAgent()))

	// Extract query parameters
	params := make(map[string]string)
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			params[key] = values[0]
		}
	}

	// Log callback shape WITHOUT leaking the authorization code or full state.
	state := params["state"]
	c.logger.Info("🔍 OAuth callback parameters extracted",
		zap.Bool("has_code", params["code"] != ""),
		zap.String("state", state),
		zap.String("error", params["error"]),
		zap.String("error_description", params["error_description"]),
		zap.Int("total_params", len(params)))

	// Route the callback to the waiter that owns this state. Delivery happens
	// under c.mu so it is serialized with Unregister (no send on a closed chan).
	// `known` = a live flow owns this state; distinct from `delivered` — a same-state
	// DUPLICATE (double 302 / refresh) finds the cap-1 buffer already full, which is a
	// harmless no-op for a flow that succeeded, NOT a stale/unknown callback. Base the
	// user-facing page on `known` so a duplicate still shows success, not the 409.
	c.mu.Lock()
	ch, known := c.waiters[state]
	if known {
		select {
		case ch <- params:
		default: // buffer already holds this state's callback — duplicate, no-op
		}
	}
	activeWaiters := len(c.waiters)
	c.mu.Unlock()

	w.Header().Set("Content-Type", "text/html")

	if !known {
		// No live flow owns this state. This is almost always a STALE browser
		// tab from an earlier login attempt completing late. Previously this
		// path poisoned whatever flow was currently waiting; now we drop it
		// cleanly and tell the user the truth so they don't think it worked.
		c.logger.Warn("⚠️ OAuth callback for unknown/stale state — no active waiter; ignoring (likely a stale browser tab from a previous attempt)",
			zap.String("state", state),
			zap.Int("active_waiters", activeWaiters))
		stalePage := `
		<html>
			<body>
				<h1>This authorization link is stale</h1>
				<p>It was already used or superseded by a newer sign-in attempt, so it was ignored.</p>
				<p>Close this tab, return to mcpproxy, and start a fresh login. Do not reuse old authorization tabs.</p>
			</body>
		</html>
	`
		w.WriteHeader(http.StatusConflict)
		if _, err := w.Write([]byte(stalePage)); err != nil {
			c.logger.Error("Error writing OAuth callback response", zap.Error(err))
		}
		return
	}

	c.logger.Info("✅ OAuth callback routed to its waiter (token exchange proceeds in the flow)",
		zap.String("state", state))
	// Note: token exchange happens in the waiting flow AFTER this response is
	// written, so we can only honestly say the callback was received & routed.
	receivedPage := `
		<html>
			<body>
				<h1>Authorization received</h1>
				<p>Completing sign-in&hellip; you can close this window and return to the application.</p>
				<script>
					setTimeout(function() {
						window.close();
					}, 2000);
				</script>
			</body>
		</html>
	`
	if _, err := w.Write([]byte(receivedPage)); err != nil {
		c.logger.Error("Error writing OAuth callback response", zap.Error(err))
	}
}

// GetCallbackServer retrieves the callback server for a given server name
func (m *CallbackServerManager) GetCallbackServer(serverName string) (*CallbackServer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	server, exists := m.servers[serverName]
	return server, exists
}

// GetCallbackServer is a global helper to access callback servers
func GetCallbackServer(serverName string) (*CallbackServer, bool) {
	return globalCallbackManager.GetCallbackServer(serverName)
}

// StopCallbackServer tears down the callback server for a given server name,
// but ONLY if no OAuth flow is still waiting on it. A single callback server
// is shared by every concurrent flow for that server name (see CallbackServer
// docs), so one flow completing must not sever a SIBLING flow's callback.
//
// Callers MUST release their own waiter (CallbackServer.Unregister(state))
// before calling this - otherwise their own still-registered entry would
// always make the server look "in use" and it would never tear down. This is
// why markOAuthComplete unregisters its state first and calls this second.
//
// If a waiter remains, this is a no-op: the server stays up, and whichever
// flow is last to finish will tear it down when IT calls this. For
// unconditional teardown regardless of live waiters (daemon shutdown, test
// cleanup), use StopCallbackServerForce.
func (m *CallbackServerManager) StopCallbackServer(serverName string) error {
	return m.stopCallbackServer(serverName, false)
}

// StopCallbackServerForce unconditionally tears down the callback server for
// serverName, closing every outstanding waiter channel regardless of how
// many OAuth flows are still in flight. Use for daemon shutdown and test
// cleanup; use StopCallbackServer for normal per-flow completion.
func (m *CallbackServerManager) StopCallbackServerForce(serverName string) error {
	return m.stopCallbackServer(serverName, true)
}

func (m *CallbackServerManager) stopCallbackServer(serverName string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	server, exists := m.servers[serverName]
	if !exists {
		return nil // Already stopped or never started
	}

	if !force {
		server.mu.Lock()
		activeWaiters := len(server.waiters)
		server.mu.Unlock()
		if activeWaiters > 0 {
			m.logger.Debug("Skipping OAuth callback server teardown - other flow(s) still waiting on it",
				zap.String("server", serverName),
				zap.Int("active_waiters", activeWaiters))
			return nil
		}
	}

	// Shutdown the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Server.Shutdown(ctx); err != nil {
		m.logger.Error("Error shutting down OAuth callback server",
			zap.String("server", serverName),
			zap.Error(err))
	}

	// Close every outstanding waiter channel (only relevant when force=true;
	// the non-force path only reaches here once waiters is already empty)
	// and mark the server closed so any in-flight flows unblock instead of
	// hanging until their 120s timeout.
	server.mu.Lock()
	server.closed = true
	for state, ch := range server.waiters {
		delete(server.waiters, state)
		close(ch)
	}
	server.mu.Unlock()

	// Remove from map
	delete(m.servers, serverName)

	m.logger.Info("OAuth callback server stopped",
		zap.String("server", serverName),
		zap.Int("port", server.Port),
		zap.Bool("force", force))

	return nil
}

// GetGlobalCallbackManager returns the global callback manager instance
func GetGlobalCallbackManager() *CallbackServerManager {
	return globalCallbackManager
}

// ShouldUseOAuth determines if OAuth should be attempted for a given server
// Headers are tried first if configured, then OAuth as fallback on auth errors
func ShouldUseOAuth(serverConfig *config.ServerConfig) bool {
	logger := zap.L().Named("oauth")

	// Check if OAuth is disabled for tests
	if os.Getenv("MCPPROXY_DISABLE_OAUTH") == "true" {
		logger.Debug("OAuth disabled for tests", zap.String("server", serverConfig.Name))
		return false
	}

	// Only HTTP and SSE transports support OAuth
	if serverConfig.Protocol == "stdio" {
		logger.Debug("OAuth not supported for stdio protocol", zap.String("server", serverConfig.Name))
		return false
	}

	// If headers are configured, try headers first, not OAuth
	if len(serverConfig.Headers) > 0 {
		logger.Debug("Headers configured - will try headers first, OAuth as fallback if needed",
			zap.String("server", serverConfig.Name),
			zap.Int("header_count", len(serverConfig.Headers)))
		return false
	}

	// For HTTP/SSE servers without headers, try OAuth-enabled clients
	logger.Debug("No headers configured - OAuth-enabled client will be used",
		zap.String("server", serverConfig.Name),
		zap.String("protocol", serverConfig.Protocol))

	return true
}

// IsOAuthConfigured checks if server has OAuth configuration in config file
// This is mainly for informational purposes - we don't require pre-configuration
func IsOAuthConfigured(serverConfig *config.ServerConfig) bool {
	return serverConfig.OAuth != nil
}

// parseBaseURL extracts the base URL (scheme + host) from a full URL
func parseBaseURL(fullURL string) (string, error) {
	if fullURL == "" {
		return "", fmt.Errorf("empty URL")
	}

	// Handle URLs that might not have a scheme
	if !strings.HasPrefix(fullURL, "http://") && !strings.HasPrefix(fullURL, "https://") {
		fullURL = "https://" + fullURL
	}

	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid URL: missing scheme or host")
	}

	return fmt.Sprintf("%s://%s", u.Scheme, u.Host), nil
}

// IsOAuthCapable determines if a server can use OAuth authentication
// Returns true if:
//  1. OAuth is explicitly configured in config, OR
//  2. Server uses HTTP-based protocol (OAuth auto-detection available)
func IsOAuthCapable(serverConfig *config.ServerConfig) bool {
	// Explicitly configured
	if serverConfig.OAuth != nil {
		return true
	}

	// Auto-detection available for HTTP-based protocols
	protocol := strings.ToLower(serverConfig.Protocol)
	switch protocol {
	case "http", "sse", "streamable-http", "auto":
		return true // OAuth can be auto-detected
	case "stdio":
		return false // OAuth not applicable for stdio
	default:
		// Unknown protocol - assume HTTP-based and try OAuth
		return true
	}
}
