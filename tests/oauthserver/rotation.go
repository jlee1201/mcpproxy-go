package oauthserver

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// handleStrictRefreshRotation implements refresh-token rotation with reuse
// detection (RFC 6749 §10.4): each refresh token is single-use; presenting an
// already-consumed token is treated as theft and revokes the whole token
// family. The check-consume-rotate happens under a single lock so concurrent
// refreshes with the SAME token are serialized deterministically — the first
// succeeds and every other one is rejected as a reuse.
//
// This models strict providers (e.g. Notion). Enabled via
// Options.StrictRefreshRotation. Assumes the client has already been validated
// by handleRefreshTokenGrant.
func (s *OAuthTestServer) handleStrictRefreshRotation(w http.ResponseWriter, r *http.Request, clientID string) {
	refreshToken := r.FormValue("refresh_token")
	scope := r.FormValue("scope")

	s.mu.Lock()
	if s.consumedRefreshTokens == nil {
		s.consumedRefreshTokens = make(map[string]*RefreshTokenData)
	}

	// Reuse detection: this token was already exchanged. Treat as theft and
	// revoke the entire family so subsequent refreshes also fail (forcing a
	// full interactive re-auth) — exactly the production symptom.
	if consumed, reused := s.consumedRefreshTokens[refreshToken]; reused {
		for tok, data := range s.refreshTokens {
			if data.Subject == consumed.Subject && data.ClientID == consumed.ClientID {
				delete(s.refreshTokens, tok)
			}
		}
		s.mu.Unlock()
		s.tokenError(w, http.StatusBadRequest, "invalid_grant", "Refresh token reuse detected")
		return
	}

	data, ok := s.refreshTokens[refreshToken]
	if !ok || data.IsExpired() {
		s.mu.Unlock()
		s.tokenError(w, http.StatusBadRequest, "invalid_grant", "Invalid or expired refresh token")
		return
	}
	if data.ClientID != clientID {
		s.mu.Unlock()
		s.tokenError(w, http.StatusBadRequest, "invalid_grant", "Refresh token belongs to different client")
		return
	}

	// Consume + rotate atomically.
	delete(s.refreshTokens, refreshToken)
	s.consumedRefreshTokens[refreshToken] = data

	scopes := data.Scopes
	if scope != "" {
		scopes = s.intersectScopes(strings.Fields(scope), data.Scopes)
		if len(scopes) == 0 {
			s.mu.Unlock()
			s.tokenError(w, http.StatusBadRequest, "invalid_scope", "Requested scopes not in original grant")
			return
		}
	}

	nb := make([]byte, 32)
	_, _ = rand.Read(nb)
	newRefreshToken := hex.EncodeToString(nb)
	s.refreshTokens[newRefreshToken] = &RefreshTokenData{
		Token:     newRefreshToken,
		ClientID:  clientID,
		Subject:   data.Subject,
		Scopes:    scopes,
		Resource:  data.Resource,
		ExpiresAt: time.Now().Add(s.options.RefreshTokenExpiry),
	}
	subject := data.Subject
	resource := data.Resource
	s.mu.Unlock()

	accessToken, err := s.generateAccessToken(subject, clientID, scopes, resource)
	if err != nil {
		s.tokenError(w, http.StatusInternalServerError, "server_error", "Failed to generate access token")
		return
	}

	s.recordIssuedToken(TokenInfo{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		ClientID:     clientID,
		Subject:      subject,
		Scopes:       scopes,
		Resource:     resource,
		IssuedAt:     time.Now(),
		ExpiresAt:    time.Now().Add(s.options.AccessTokenExpiry),
	})

	s.sendTokenResponse(w, TokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(s.options.AccessTokenExpiry.Seconds()),
		RefreshToken: newRefreshToken,
		Scope:        strings.Join(scopes, " "),
	})
}
