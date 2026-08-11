package contracts

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOAuthConfig_TokenValidFalse_ReachesTheWire is the D3.1/A4 regression
// guard. TokenValid used to carry `json:"token_valid,omitempty"`, so a
// legitimate false (token present but expired/invalid) was indistinguishable
// on the wire from "no token field at all" -- the exact bug learnings.md
// mis-described as "token_valid flips True->None at expiry". It never
// flipped to None; false was silently dropped.
func TestOAuthConfig_TokenValidFalse_ReachesTheWire(t *testing.T) {
	cfg := OAuthConfig{
		ClientID:   "test-client",
		TokenValid: false,
	}

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))

	value, present := decoded["token_valid"]
	require.True(t, present, "token_valid must be present on the wire even when false")
	assert.Equal(t, false, value)
}

// TestOAuthConfig_TokenValidTrue_StillSerializes is the control: true must
// keep working exactly as before.
func TestOAuthConfig_TokenValidTrue_StillSerializes(t *testing.T) {
	cfg := OAuthConfig{TokenValid: true}

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.Equal(t, true, decoded["token_valid"])
}
