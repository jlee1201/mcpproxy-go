package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/oauth"
)

// TestOAuthStatusForToken_ZeroExpiryIsUnknownNotAuthenticated is the D3.2/A4
// regression guard: a persisted token record with a zero ExpiresAt (live on
// notiongusto and gmailgusto) must report unknown validity, never "valid
// forever".
func TestOAuthStatusForToken_ZeroExpiryIsUnknownNotAuthenticated(t *testing.T) {
	tokenValid, status := oauthStatusForToken(time.Time{}, time.Now())

	assert.False(t, tokenValid, "zero ExpiresAt must not report token_valid=true")
	assert.Equal(t, oauth.OAuthStatusUnknown, status, "zero ExpiresAt must report oauth_status=unknown, not authenticated")
}

func TestOAuthStatusForToken_Table(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		expiresAt      time.Time
		wantTokenValid bool
		wantStatus     oauth.OAuthStatus
	}{
		{
			name:           "zero expiry -> unknown",
			expiresAt:      time.Time{},
			wantTokenValid: false,
			wantStatus:     oauth.OAuthStatusUnknown,
		},
		{
			name:           "future expiry -> authenticated",
			expiresAt:      now.Add(time.Hour),
			wantTokenValid: true,
			wantStatus:     oauth.OAuthStatusAuthenticated,
		},
		{
			name:           "past expiry -> expired",
			expiresAt:      now.Add(-time.Hour),
			wantTokenValid: false,
			wantStatus:     oauth.OAuthStatusExpired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenValid, status := oauthStatusForToken(tt.expiresAt, now)
			assert.Equal(t, tt.wantTokenValid, tokenValid)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}
