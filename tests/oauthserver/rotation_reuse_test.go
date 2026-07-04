package oauthserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client/transport"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"
)

// TestConcurrentRefreshDoesNotTripReuseDetection reproduces the Notion re-auth
// loop.
//
// When several requests hit an expired access token at once, mcpproxy (via
// mc-go's OAuthHandler) fires multiple concurrent refresh_token grants that all
// read and submit the SAME refresh token — mc-go's getValidToken does
// read-token -> refresh -> save with no serialization. Against a provider that
// rotates refresh tokens with reuse detection (like Notion), the first grant
// wins and every other one is rejected as a reuse, revoking the token family
// and forcing a full interactive re-auth (the browser click).
//
// Correct behavior: concurrent token acquisition on an expired token must be
// coalesced so exactly ONE refresh happens and all callers succeed.
//
// This test FAILS today (refresh is not serialized) and should pass once
// per-server refresh coalescing is in place. It is the regression guard for
// that fix.
func TestConcurrentRefreshDoesNotTripReuseDetection(t *testing.T) {
	srv := Start(t, Options{
		StrictRefreshRotation: true,
		AccessTokenExpiry:     time.Hour,
		RefreshTokenExpiry:    24 * time.Hour,
		// Delay token responses so every concurrent caller reads the seeded
		// refresh token before the winning refresh persists a rotated one —
		// makes the reuse collision deterministic.
		ErrorMode: ErrorMode{TokenSlowResponse: 150 * time.Millisecond},
	})
	defer srv.Shutdown()

	// Mint an initial refresh token for the public client, as if a prior
	// authorization_code exchange had produced it.
	const subject = "testuser"
	initialRefreshToken := srv.Server.generateRefreshToken(subject, srv.PublicClientID, []string{"read"}, "")

	// Persist an already-expired access token + that refresh token into
	// mcpproxy's real persistent token store, so the next request forces a
	// refresh through mc-go.
	store := newTestTokenStore(t, srv.IssuerURL)
	if err := store.SaveToken(context.Background(), &transport.Token{
		AccessToken:  "expired-access-token",
		TokenType:    "Bearer",
		RefreshToken: initialRefreshToken,
		ExpiresAt:    time.Now().Add(-time.Hour), // expired -> forces refresh
	}); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	handler := transport.NewOAuthHandler(transport.OAuthConfig{
		ClientID:              srv.PublicClientID,
		TokenStore:            store,
		AuthServerMetadataURL: srv.IssuerURL + "/.well-known/oauth-authorization-server",
		PKCEEnabled:           true,
	})

	const concurrency = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errsCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all at once to maximize overlap
			_, err := handler.GetAuthorizationHeader(context.Background())
			errsCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errsCh)

	var failures int
	for err := range errsCh {
		if err != nil {
			failures++
			t.Logf("concurrent GetAuthorizationHeader failed: %v", err)
		}
	}

	if failures > 0 {
		t.Fatalf("concurrent token refresh tripped refresh-token reuse detection: "+
			"%d/%d GetAuthorizationHeader calls failed (want 0). Refresh is not "+
			"coalesced per server, so multiple grants submit the same refresh token.",
			failures, concurrency)
	}
}

func newTestTokenStore(t *testing.T, serverURL string) transport.TokenStore {
	t.Helper()
	db, err := storage.NewBoltDB(t.TempDir(), zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return oauth.NewPersistentTokenStore("rotation-test", serverURL, db)
}
