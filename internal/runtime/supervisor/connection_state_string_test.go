package supervisor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/types"
)

// TestConnectionStateString_NormalizesCasing is the D6/A4 regression guard.
// updateStateView (one writer) lowercased state.State via strings.ToLower;
// updateSnapshotFromEvent (the other writer) did not, so status.State's
// casing depended on which write path last ran ("error" vs "Error"). Both
// now go through this one helper.
func TestConnectionStateString_NormalizesCasing(t *testing.T) {
	tests := []struct {
		name  string
		state types.ConnectionState
		want  string
	}{
		{"Ready", types.StateReady, "ready"},
		{"Error", types.StateError, "error"},
		{"Disconnected", types.StateDisconnected, "disconnected"},
		{"Connecting", types.StateConnecting, "connecting"},
		{"PendingAuth", types.StatePendingAuth, "pending auth"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci := &types.ConnectionInfo{State: tt.state}
			assert.Equal(t, tt.want, connectionStateString(ci))
		})
	}
}

func TestConnectionStateString_NilConnectionInfo(t *testing.T) {
	assert.Equal(t, "", connectionStateString(nil))
}
