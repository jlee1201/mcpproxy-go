package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	uptransport "github.com/mark3labs/mcp-go/client/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/contracts"
)

// newDCRTestServer serves an MCP endpoint that demands OAuth, plus OAuth metadata
// advertising a registration endpoint that answers with registerStatus (0 = none advertised).
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
		meta := map[string]any{
			"issuer":                           srv.URL,
			"authorization_endpoint":           srv.URL + "/authorize",
			"token_endpoint":                   srv.URL + "/token",
			"response_types_supported":         []string{"code"},
			"code_challenge_methods_supported": []string{"S256"},
		}
		if registerStatus != 0 {
			meta["registration_endpoint"] = srv.URL + "/register"
		}
		_ = json.NewEncoder(w).Encode(meta)
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

type dcrEntryPoint struct {
	name string
	// run returns the error and any authorize URL the entry point produced.
	run func(t *testing.T, c *Client) (authURL string, err error)
}

var dcrEntryPoints = []dcrEntryPoint{
	{"StartOAuthFlowQuick", func(t *testing.T, c *Client) (string, error) {
		res, err := c.StartOAuthFlowQuick(t.Context())
		if res == nil {
			return "", err
		}
		return res.AuthURL, err
	}},
	{"ForceOAuthFlowWithResult", func(t *testing.T, c *Client) (string, error) {
		res, err := c.ForceOAuthFlowWithResult(t.Context())
		if res == nil {
			return "", err
		}
		return res.AuthURL, err
	}},
}

// A failed DCR with no client_id must abort every OAuth entry point instead of falling through
// to an authorize URL without client_id (which Runlayer rejects with 422 "client_id Field required").
func TestOAuthFlow_DCRFailureAborts(t *testing.T) {
	t.Setenv("HEADLESS", "1")
	cases := []struct {
		name           string
		registerStatus int // 0 = server advertises no registration_endpoint
		want403        bool
	}{
		{"500", http.StatusInternalServerError, false},
		{"429", http.StatusTooManyRequests, false},
		{"502", http.StatusBadGateway, false},
		{"400", http.StatusBadRequest, false},
		{"no registration endpoint", 0, false},
		{"403", http.StatusForbidden, true},
	}
	for _, ep := range dcrEntryPoints {
		for _, tc := range cases {
			t.Run(ep.name+"/"+tc.name, func(t *testing.T) {
				var calls int32
				srv := newDCRTestServer(t, tc.registerStatus, &calls)
				name := "dcr-failure-" + ep.name + "-" + tc.name
				c, err := NewClientWithOptions(
					name,
					&config.ServerConfig{Name: name, Protocol: "streamable-http", URL: srv.URL + "/mcp"},
					zap.NewNop(), nil, nil, nil, false, nil,
				)
				require.NoError(t, err)

				type outcome struct {
					authURL string
					err     error
				}
				done := make(chan outcome, 1)
				go func() {
					u, e := ep.run(t, c)
					done <- outcome{u, e}
				}()
				var got outcome
				select {
				case got = <-done:
				case <-time.After(30 * time.Second):
					t.Fatal("OAuth entry point did not return")
				}

				require.Error(t, got.err)
				assert.Empty(t, got.authURL, "must not hand out an authorize URL without client_id")
				if tc.registerStatus != 0 {
					assert.Positive(t, atomic.LoadInt32(&calls), "DCR should have been attempted")
				}

				var flowErr *contracts.OAuthFlowError
				require.True(t, errors.As(got.err, &flowErr), "want OAuthFlowError, got %T: %v", got.err, got.err)
				if tc.want403 {
					assert.Equal(t, contracts.OAuthCodeNoClientID, flowErr.ErrorCode)
				} else {
					assert.Equal(t, contracts.OAuthCodeDCRFailed, flowErr.ErrorCode)
					assert.Equal(t, contracts.OAuthErrorDCRFailed, flowErr.ErrorType)
					require.NotNil(t, flowErr.Details)
					require.NotNil(t, flowErr.Details.DCRStatus)
					assert.True(t, flowErr.Details.DCRStatus.Attempted)
					assert.False(t, flowErr.Details.DCRStatus.Success)
				}
			})
		}
	}
}

// handleOAuthAuthorization (reached from Connect's auto-auth path) can't be driven end to end
// in a test, so exercise the shared helper it uses directly.
func TestDCRFailureAbort(t *testing.T) {
	c, err := NewClientWithOptions(
		"dcr-abort-helper",
		&config.ServerConfig{Name: "dcr-abort-helper", Protocol: "streamable-http", URL: "http://127.0.0.1:1/mcp"},
		zap.NewNop(), nil, nil, nil, false, nil,
	)
	require.NoError(t, err)

	regErr := errors.New("registration request failed with status 500")

	noID := uptransport.NewOAuthHandler(uptransport.OAuthConfig{})
	flowErr := c.dcrFailureAbort(noID, regErr, "corr-1")
	require.NotNil(t, flowErr)
	assert.Equal(t, contracts.OAuthCodeDCRFailed, flowErr.ErrorCode)
	assert.Equal(t, "corr-1", flowErr.CorrelationID)
	assert.Contains(t, flowErr.Message, "status 500")

	withID := uptransport.NewOAuthHandler(uptransport.OAuthConfig{ClientID: "abc"})
	assert.Nil(t, c.dcrFailureAbort(withID, regErr, "corr-2"), "an existing client_id means the flow can proceed")
}
