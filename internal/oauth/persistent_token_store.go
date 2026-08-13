package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"

	"github.com/mark3labs/mcp-go/client"
	transport "github.com/mark3labs/mcp-go/client/transport"
	"go.uber.org/zap"
)

const (
	// TokenRefreshGracePeriod defines how long before expiration we should trigger a refresh.
	// This prevents race conditions where a token expires during an API call.
	// Setting this to 5 minutes allows proactive token refresh before expiration.
	TokenRefreshGracePeriod = 5 * time.Minute

	// refreshCoalesceLease bounds how long a follower waits for the leader's
	// refresh to land, and how long a leader "owns" the refresh before its lease
	// is considered stale (so a failed refresh can't deadlock future callers).
	refreshCoalesceLease = 30 * time.Second
)

// PersistentTokenStore implements client.TokenStore using BBolt storage
type PersistentTokenStore struct {
	serverName string // Original server name (used for RefreshManager)
	serverKey  string // Unique key combining server name and URL (used for storage)
	storage    *storage.BoltDB
	logger     *zap.Logger

	// Refresh coalescing: when a token needs refreshing, exactly one caller (the
	// "leader") returns the expired token to mc-go so it performs a single
	// refresh_token grant; concurrent callers wait for the leader's SaveToken and
	// then receive the rotated token. This stops multiple goroutines submitting
	// the same refresh token, which strict providers reject as reuse.
	refreshMu    sync.Mutex
	refreshWait  chan struct{} // non-nil while a refresh is in flight; closed on completion
	refreshStart time.Time     // when the current leader started (for stale-lease recovery)
}

// NewPersistentTokenStore creates a new persistent token store for a server
func NewPersistentTokenStore(serverName, serverURL string, storage *storage.BoltDB) client.TokenStore {
	// Create unique key combining server name and URL to handle servers with same name but different URLs
	serverKey := GenerateServerKey(serverName, serverURL)

	return &PersistentTokenStore{
		serverName: serverName,
		serverKey:  serverKey,
		storage:    storage,
		logger:     zap.L().Named("persistent-token-store").With(zap.String("server", serverName), zap.String("server_key", serverKey)),
	}
}

// GenerateServerKey creates a unique key for a server by combining name and URL
// Exported for use by connection.go when persisting DCR credentials
func GenerateServerKey(serverName, serverURL string) string {
	// Create a unique identifier by combining server name and URL
	combined := fmt.Sprintf("%s|%s", serverName, serverURL)

	// Generate SHA256 hash for consistent length and uniqueness
	hash := sha256.Sum256([]byte(combined))
	hashStr := hex.EncodeToString(hash[:])

	// Return first 16 characters of hash for readability (still highly unique)
	key := fmt.Sprintf("%s_%s", serverName, hashStr[:16])

	// Log key generation for debugging server key mismatches
	zap.L().Debug("Generated OAuth server key",
		zap.String("server_name", serverName),
		zap.String("server_url", serverURL),
		zap.String("generated_key", key))

	return key
}

// GetToken retrieves the OAuth token from persistent storage
func (p *PersistentTokenStore) GetToken(ctx context.Context) (*client.Token, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.logger.Debug("🔍 Loading OAuth token from persistent storage",
		zap.String("server_name", p.serverName),
		zap.String("server_key", p.serverKey))

	record, err := p.storage.GetOAuthToken(p.serverKey)
	if err != nil {
		p.logger.Debug("❌ No stored OAuth token found",
			zap.String("server_name", p.serverName),
			zap.String("server_key", p.serverKey),
			zap.Error(err))
		return nil, transport.ErrNoToken
	}

	now := time.Now()
	timeUntilExpiry := record.ExpiresAt.Sub(now)
	isExpired := now.After(record.ExpiresAt)
	needsRefresh := timeUntilExpiry < TokenRefreshGracePeriod

	// Log token status for debugging
	if isExpired {
		p.logger.Warn("⚠️ OAuth token has expired and needs refresh",
			zap.String("server_key", p.serverKey),
			zap.Time("expires_at", record.ExpiresAt),
			zap.Duration("expired_since", -timeUntilExpiry),
			zap.Bool("has_refresh_token", record.RefreshToken != ""))
	} else if needsRefresh {
		p.logger.Info("⏰ OAuth token will expire soon, proactive refresh recommended",
			zap.String("server_key", p.serverKey),
			zap.Time("expires_at", record.ExpiresAt),
			zap.Duration("time_until_expiry", timeUntilExpiry),
			zap.Duration("grace_period", TokenRefreshGracePeriod),
			zap.Bool("has_refresh_token", record.RefreshToken != ""))
	} else {
		p.logger.Debug("✅ OAuth token is valid and not expiring soon",
			zap.String("server_key", p.serverKey),
			zap.Time("expires_at", record.ExpiresAt),
			zap.Duration("time_until_expiry", timeUntilExpiry),
			zap.Bool("has_refresh_token", record.RefreshToken != ""))
	}

	// Join scopes back into space-separated string
	scope := strings.Join(record.Scopes, " ")

	// Adjust ExpiresAt to trigger proactive refresh within grace period
	// This prevents race conditions where tokens expire during API calls
	//
	// IMPORTANT: Only apply grace period if the token has enough remaining lifetime.
	// For short-lived tokens (e.g., 30 seconds), subtracting 5 minutes would make
	// them appear expired immediately, causing unnecessary re-authentication.
	adjustedExpiresAt := record.ExpiresAt
	if timeUntilExpiry > TokenRefreshGracePeriod {
		// Token has enough lifetime - apply full grace period for proactive refresh
		adjustedExpiresAt = record.ExpiresAt.Add(-TokenRefreshGracePeriod)
		p.logger.Debug("Applied grace period adjustment for proactive refresh",
			zap.Duration("grace_period", TokenRefreshGracePeriod),
			zap.Time("original_expires_at", record.ExpiresAt),
			zap.Time("adjusted_expires_at", adjustedExpiresAt))
	} else if timeUntilExpiry > 0 {
		// Token is short-lived but not yet expired - use actual expiration
		// This allows the token to be used until it actually expires
		p.logger.Debug("Skipping grace period for short-lived token",
			zap.Duration("time_until_expiry", timeUntilExpiry),
			zap.Duration("grace_period", TokenRefreshGracePeriod),
			zap.Time("expires_at", record.ExpiresAt))
	}
	// If timeUntilExpiry <= 0, token is already expired - adjustedExpiresAt stays as record.ExpiresAt

	token := &client.Token{
		AccessToken:  record.AccessToken,
		RefreshToken: record.RefreshToken,
		TokenType:    record.TokenType,
		ExpiresAt:    adjustedExpiresAt,
		Scope:        scope,
	}

	// Log token metadata for debugging (using the new logging utility)
	LogTokenMetadata(p.logger, TokenMetadata{
		TokenType:       record.TokenType,
		ExpiresAt:       record.ExpiresAt,
		ExpiresIn:       timeUntilExpiry,
		Scope:           scope,
		HasRefreshToken: record.RefreshToken != "",
	})

	// Warn if returning an expired token without a refresh token - mcp-go cannot refresh this
	if isExpired && record.RefreshToken == "" {
		p.logger.Warn("⚠️ Returning expired token WITHOUT refresh_token - refresh will fail",
			zap.String("server_name", p.serverName),
			zap.String("server_key", p.serverKey),
			zap.Time("expired_at", record.ExpiresAt))
	} else if record.RefreshToken == "" {
		p.logger.Warn("⚠️ Token has no refresh_token - cannot be refreshed when it expires",
			zap.String("server_name", p.serverName),
			zap.String("server_key", p.serverKey),
			zap.Time("expires_at", record.ExpiresAt))
	}

	// Coalesce concurrent refreshes: if mc-go will treat this token as expired
	// (now past the adjusted ExpiresAt) and we have a refresh token, make exactly
	// one caller trigger the refresh and have the rest reuse its rotated token.
	// This prevents concurrent goroutines from submitting the same refresh token,
	// which strict providers (e.g. Notion) reject as reuse and then revoke the
	// whole family — forcing a full interactive re-auth.
	if record.RefreshToken != "" && time.Now().After(token.ExpiresAt) {
		token = p.coalesceRefresh(ctx, token)
	}

	// Return the token - mcp-go library will check IsExpired() and handle refresh if needed
	// For long-lived tokens, we subtract the grace period from ExpiresAt to trigger refresh earlier
	// For short-lived tokens, we use the actual expiration to avoid falsely marking them as expired
	return token, nil
}

// PeekToken returns the persisted token as a read-only look, without
// triggering `coalesceRefresh`'s leader/follower election and without
// applying the proactive-refresh grace-period adjustment `GetToken` uses for
// mcp-go's benefit.
//
// Callers that only want to know "is there a token, and has it changed"
// (e.g. Manager.scanForNewTokens) must use this instead of GetToken: GetToken
// elects the caller refresh leader whenever the (grace-adjusted) token looks
// expired and a refresh token is present, and a caller with no intention of
// ever calling SaveToken holds that lease until the 30s stale-lease takeover,
// starving real refreshers (D5).
//
// ExpiresAt is returned exactly as persisted, including a zero value, which
// means the record's expiry is unknown rather than "expired" or "valid
// forever" — callers must not reinterpret it.
func (p *PersistentTokenStore) PeekToken(ctx context.Context) (*client.Token, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	record, err := p.storage.GetOAuthToken(p.serverKey)
	if err != nil || record == nil {
		p.logger.Debug("🔍 PeekToken: no stored OAuth token found",
			zap.String("server_name", p.serverName),
			zap.String("server_key", p.serverKey),
			zap.Error(err))
		return nil, transport.ErrNoToken
	}

	return &client.Token{
		AccessToken:  record.AccessToken,
		RefreshToken: record.RefreshToken,
		TokenType:    record.TokenType,
		ExpiresAt:    record.ExpiresAt, // raw, no grace-period adjustment
		Scope:        strings.Join(record.Scopes, " "),
	}, nil
}

// SaveToken stores the OAuth token to persistent storage
func (p *PersistentTokenStore) SaveToken(ctx context.Context, token *client.Token) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	now := time.Now()
	timeUntilExpiry := token.ExpiresAt.Sub(now)

	p.logger.Info("💾 Saving OAuth token to persistent storage",
		zap.String("server_key", p.serverKey),
		zap.String("token_type", token.TokenType),
		zap.Time("expires_at", token.ExpiresAt),
		zap.Duration("valid_for", timeUntilExpiry),
		zap.Bool("has_refresh_token", token.RefreshToken != ""),
		zap.String("scope", token.Scope))

	// Parse scopes from token.Scope (space-separated string)
	var scopes []string
	if token.Scope != "" {
		scopes = strings.Split(token.Scope, " ")
	}

	record := &storage.OAuthTokenRecord{
		ServerName:   p.serverKey,  // Storage key (for bucket lookup)
		DisplayName:  p.serverName, // Actual server name (for RefreshManager)
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		TokenType:    token.TokenType,
		ExpiresAt:    token.ExpiresAt,
		Scopes:       scopes,
		Created:      now,
		Updated:      now,
	}

	err := p.storage.SaveOAuthToken(record)
	if err != nil {
		p.logger.Error("❌ Failed to save OAuth token to persistent storage",
			zap.String("server", p.serverName),
			zap.String("server_key", p.serverKey),
			zap.Error(err))
		return fmt.Errorf("failed to save OAuth token: %w", err)
	}

	p.logger.Info("✅ OAuth token saved to persistent storage successfully",
		zap.String("server", p.serverName),
		zap.String("server_key", p.serverKey),
		zap.Duration("valid_for", timeUntilExpiry))

	// Log token metadata for debugging (using the standard logging utility)
	LogTokenMetadata(p.logger, TokenMetadata{
		TokenType:       token.TokenType,
		ExpiresAt:       token.ExpiresAt,
		ExpiresIn:       timeUntilExpiry,
		Scope:           token.Scope,
		HasRefreshToken: token.RefreshToken != "",
	})

	// Wake any callers coalescing on this refresh so they can use the rotated
	// token instead of re-submitting the old (now-consumed) refresh token.
	p.finishRefresh()

	// Notify RefreshManager about the new token so it can schedule proactive refresh
	// Use serverName (not serverKey) so RefreshManager can look up the actual server
	globalTokenStoreManager.NotifyTokenSaved(p.serverName, token.ExpiresAt)

	return nil
}

// ClearToken removes the OAuth token from persistent storage
func (p *PersistentTokenStore) ClearToken() error {
	p.logger.Info("🗑️ Clearing OAuth token from persistent storage",
		zap.String("server_key", p.serverKey))

	err := p.storage.DeleteOAuthToken(p.serverKey)
	if err != nil {
		p.logger.Error("❌ Failed to clear OAuth token from persistent storage",
			zap.String("server_key", p.serverKey),
			zap.Error(err))
		return fmt.Errorf("failed to clear OAuth token: %w", err)
	}

	p.logger.Info("✅ OAuth token cleared from persistent storage successfully",
		zap.String("server_key", p.serverKey))
	return nil
}

// beginRefresh assigns exactly one leader to perform a token refresh at a time.
// Returns isLeader=true for the caller that should trigger the refresh; other
// concurrent callers get isLeader=false and a channel that closes when the
// leader's SaveToken lands. A stale lease (leader that never completed) is taken
// over so a failed refresh cannot deadlock future callers.
func (p *PersistentTokenStore) beginRefresh() (isLeader bool, wait chan struct{}) {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if p.refreshWait != nil {
		if time.Since(p.refreshStart) < refreshCoalesceLease {
			return false, p.refreshWait
		}
		// Stale lease: the previous leader ran past the lease (hung/slow/errored and
		// never called SaveToken). Close its channel NOW so its followers wake and
		// retry instead of each blocking on their own timeout, then take over with a
		// fresh channel. All refreshWait mutations happen under refreshMu, so each
		// channel is closed exactly once (here on takeover, or in finishRefresh —
		// whichever runs first for that generation; the other sees a non-matching
		// refreshWait). A late SaveToken from the dead leader can still close our new
		// channel early, but only ever wakes followers with an already-persisted valid
		// token — never a deadlock or double-close.
		close(p.refreshWait)
		p.refreshWait = nil
	}
	p.refreshWait = make(chan struct{})
	p.refreshStart = time.Now()
	return true, p.refreshWait
}

// finishRefresh wakes any followers waiting on the in-flight refresh. Safe to
// call when no refresh is in flight (no-op).
func (p *PersistentTokenStore) finishRefresh() {
	p.refreshMu.Lock()
	if p.refreshWait != nil {
		close(p.refreshWait)
		p.refreshWait = nil
	}
	p.refreshMu.Unlock()
}

// coalesceRefresh serializes concurrent refreshes of an expired token. The
// leader returns the expired token so mc-go performs a single refresh_token
// grant; followers wait for that refresh to persist a rotated token and return
// it instead of submitting the same (now-consumed) refresh token.
func (p *PersistentTokenStore) coalesceRefresh(ctx context.Context, expired *client.Token) *client.Token {
	isLeader, wait := p.beginRefresh()
	if isLeader {
		p.logger.Debug("🔑 Refresh leader: returning expired token for a single refresh",
			zap.String("server_key", p.serverKey))
		return expired
	}

	p.logger.Debug("⏳ Refresh follower: waiting for in-flight refresh to complete",
		zap.String("server_key", p.serverKey))
	select {
	case <-wait:
		if fresh, err := p.storage.GetOAuthToken(p.serverKey); err == nil {
			p.logger.Debug("✅ Refresh follower: using rotated token from leader",
				zap.String("server_key", p.serverKey))
			return p.recordToToken(fresh)
		}
		// Re-read failed; fall back to the expired token (mc-go will try to refresh).
		return expired
	case <-ctx.Done():
		return expired
	case <-time.After(refreshCoalesceLease):
		p.logger.Warn("⚠️ Refresh follower timed out waiting for leader; proceeding to refresh itself",
			zap.String("server_key", p.serverKey))
		return expired
	}
}

// recordToToken converts a stored record into an mc-go token, applying the same
// proactive-refresh grace adjustment as GetToken.
func (p *PersistentTokenStore) recordToToken(record *storage.OAuthTokenRecord) *client.Token {
	adjustedExpiresAt := record.ExpiresAt
	if time.Until(record.ExpiresAt) > TokenRefreshGracePeriod {
		adjustedExpiresAt = record.ExpiresAt.Add(-TokenRefreshGracePeriod)
	}
	return &client.Token{
		AccessToken:  record.AccessToken,
		RefreshToken: record.RefreshToken,
		TokenType:    record.TokenType,
		ExpiresAt:    adjustedExpiresAt,
		Scope:        strings.Join(record.Scopes, " "),
	}
}
