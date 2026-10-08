package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/contracts"
)

// newDCRTestServer serves an MCP endpoint that demands OAuth, plus OAuth metadata
// advertising a registration endpoint that answers with registerStatus.
func newDCRTestServer(t *testing.T, registerStatus int, registerCalls *int32) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+srv.URL+`/.well-known/oauth-protected-resource"`)
		w.WriteHeader(http.StatusUnauthorized)
	})
	mux.HandleFunc("/.well-known/oauth-protected-resource", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"resource":              srv.URL + "/mcp",
			"authorization_servers": []string{srv.URL},
		})
	})
	asMeta := func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                           srv.URL,
			"authorization_endpoint":           srv.URL + "/authorize",
			"token_endpoint":                   srv.URL + "/token",
			"registration_endpoint":            srv.URL + "/register",
			"response_types_supported":         []string{"code"},
			"code_challenge_methods_supported": []string{"S256"},
		})
	}
	mux.HandleFunc("/.well-known/oauth-authorization-server", asMeta)
	mux.HandleFunc("/.well-known/oauth-authorization-server/mcp", asMeta)
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(registerCalls, 1)
		w.WriteHeader(registerStatus)
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// A transient DCR failure (non-403) must abort the flow instead of falling through to an
// authorize URL without client_id (which Runlayer rejects with 422 "client_id Field required").
func TestStartOAuthFlowQuick_DCRTransientFailureAborts(t *testing.T) {
	t.Setenv("HEADLESS", "1")
	for _, status := range []int{http.StatusInternalServerError, http.StatusTooManyRequests, http.StatusBadGateway} {
		var calls int32
		srv := newDCRTestServer(t, status, &calls)

		const name = "dcr-transient-failure"
		c, err := NewClientWithOptions(
			name,
			&config.ServerConfig{Name: name, Protocol: "streamable-http", URL: srv.URL + "/mcp"},
			zap.NewNop(), nil, nil, nil, false, nil,
		)
		require.NoError(t, err)

		done := make(chan struct{})
		var result *OAuthStartResult
		go func() {
			defer close(done)
			result, err = c.StartOAuthFlowQuick(t.Context())
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("StartOAuthFlowQuick did not return")
		}

		require.Error(t, err, "status %d", status)
		var flowErr *contracts.OAuthFlowError
		require.True(t, errors.As(err, &flowErr), "want OAuthFlowError, got %T: %v", err, err)
		assert.Equal(t, contracts.OAuthCodeDCRFailed, flowErr.ErrorCode)
		assert.Equal(t, contracts.OAuthErrorDCRFailed, flowErr.ErrorType)
		assert.Positive(t, atomic.LoadInt32(&calls), "DCR should have been attempted")
		if result != nil {
			assert.Empty(t, result.AuthURL, "must not hand out an authorize URL without client_id")
		}
	}
}
