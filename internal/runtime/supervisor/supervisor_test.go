package supervisor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/runtime/configsvc"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/types"
)

// MockUpstreamAdapter is a test double for UpstreamAdapter
type MockUpstreamAdapter struct {
	mu             sync.Mutex
	addedServers   map[string]*config.ServerConfig
	removedServers []string
	connected      map[string]bool
	disconnected   []string
	eventCh        chan Event
	states         map[string]*ServerState
	droppedEvents  atomic.Uint64
}

func NewMockUpstreamAdapter() *MockUpstreamAdapter {
	return &MockUpstreamAdapter{
		addedServers:   make(map[string]*config.ServerConfig),
		removedServers: make([]string, 0),
		connected:      make(map[string]bool),
		disconnected:   make([]string, 0),
		eventCh:        make(chan Event, 100),
		states:         make(map[string]*ServerState),
	}
}

func (m *MockUpstreamAdapter) AddServer(name string, cfg *config.ServerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addedServers[name] = cfg
	m.states[name] = &ServerState{
		Name:      name,
		Config:    cfg,
		Enabled:   cfg.Enabled,
		Connected: false,
	}
	return nil
}

func (m *MockUpstreamAdapter) RemoveServer(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removedServers = append(m.removedServers, name)
	delete(m.states, name)
	return nil
}

func (m *MockUpstreamAdapter) ConnectServer(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected[name] = true
	if state, ok := m.states[name]; ok {
		state.Connected = true
	}
	return nil
}

func (m *MockUpstreamAdapter) DisconnectServer(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnected = append(m.disconnected, name)
	if state, ok := m.states[name]; ok {
		state.Connected = false
	}
	return nil
}

func (m *MockUpstreamAdapter) ConnectAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name := range m.states {
		m.connected[name] = true
	}
	return nil
}

func (m *MockUpstreamAdapter) GetServerState(name string) (*ServerState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.states[name]; ok {
		return state, nil
	}
	return nil, nil
}

func (m *MockUpstreamAdapter) GetAllStates() map[string]*ServerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy to prevent data races. Must copy the *ServerState value,
	// not just the map -- copying only the pointer left the caller reading
	// the same struct that ConnectServer/DisconnectServer mutate in place,
	// unsynchronized once this function returns and releases m.mu (found by
	// -race once Supervisor.updateSnapshot became the first real caller of
	// this method).
	statesCopy := make(map[string]*ServerState, len(m.states))
	for k, v := range m.states {
		copied := *v
		statesCopy[k] = &copied
	}
	return statesCopy
}

func (m *MockUpstreamAdapter) IsUserLoggedOut(name string) bool {
	// Mock always returns false - tests can override behavior if needed
	return false
}

func (m *MockUpstreamAdapter) ShouldSkipReconnect(name string) bool {
	return false
}

func (m *MockUpstreamAdapter) Subscribe() <-chan Event {
	return m.eventCh
}

func (m *MockUpstreamAdapter) Unsubscribe(ch <-chan Event) {
	close(m.eventCh)
}

func (m *MockUpstreamAdapter) Close() {
	close(m.eventCh)
}

func (m *MockUpstreamAdapter) DroppedEventCount() uint64 {
	return m.droppedEvents.Load()
}

// SetServerTools sets tools for a specific server (for testing)
func (m *MockUpstreamAdapter) SetServerTools(name string, tools []*config.ToolMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.states[name]; ok {
		state.Tools = tools
		state.ToolCount = len(tools)
	}
}

func TestSupervisor_New(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())
	if supervisor == nil {
		t.Fatal("Expected non-nil supervisor")
	}

	snapshot := supervisor.CurrentSnapshot()
	require.NotNil(t, snapshot, "Expected non-nil snapshot")

	if snapshot.Version != 0 {
		t.Errorf("Expected version 0, got %d", snapshot.Version)
	}
}

func TestSupervisor_Reconcile_AddServer(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true, Quarantined: false},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Trigger reconciliation
	configSnapshot := configSvc.Current()
	err := supervisor.reconcile(configSnapshot)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Wait a bit for goroutines to complete
	time.Sleep(50 * time.Millisecond)

	// Verify server was added (with lock)
	mockUpstream.mu.Lock()
	_, addedOk := mockUpstream.addedServers["test-server"]
	connectedOk := mockUpstream.connected["test-server"]
	mockUpstream.mu.Unlock()

	if !addedOk {
		t.Error("Expected server to be added")
	}

	// Verify server was connected
	if !connectedOk {
		t.Error("Expected server to be connected")
	}
}

func TestSupervisor_Reconcile_RemoveServer(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// First reconciliation - add server
	configSnapshot := configSvc.Current()
	_ = supervisor.reconcile(configSnapshot)

	// Wait for first reconciliation to complete
	time.Sleep(50 * time.Millisecond)

	// Update config to remove server
	newCfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{}, // No servers
	}

	_ = configSvc.Update(newCfg, configsvc.UpdateTypeModify, "test")

	// Second reconciliation - remove server
	newSnapshot := configSvc.Current()
	err := supervisor.reconcile(newSnapshot)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Wait for goroutines to complete
	time.Sleep(50 * time.Millisecond)

	// Verify server was removed (with lock)
	mockUpstream.mu.Lock()
	removedServers := make([]string, len(mockUpstream.removedServers))
	copy(removedServers, mockUpstream.removedServers)
	mockUpstream.mu.Unlock()

	found := false
	for _, name := range removedServers {
		if name == "test-server" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected server to be removed")
	}
}

func TestSupervisor_Reconcile_DisableServer(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// First reconciliation - add and connect
	_ = supervisor.reconcile(configSvc.Current())

	// Wait for first reconciliation to complete
	time.Sleep(50 * time.Millisecond)

	// Mark as connected in mock (with lock)
	mockUpstream.mu.Lock()
	if state, ok := mockUpstream.states["test-server"]; ok {
		state.Connected = true
	}
	mockUpstream.mu.Unlock()

	// Update config to disable server
	newCfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: false}, // Disabled
		},
	}

	_ = configSvc.Update(newCfg, configsvc.UpdateTypeModify, "test")

	// Second reconciliation - should disconnect
	err := supervisor.reconcile(configSvc.Current())
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Wait for goroutines to complete
	time.Sleep(50 * time.Millisecond)

	// Verify server was disconnected (with lock)
	mockUpstream.mu.Lock()
	disconnected := make([]string, len(mockUpstream.disconnected))
	copy(disconnected, mockUpstream.disconnected)
	mockUpstream.mu.Unlock()

	found := false
	for _, name := range disconnected {
		if name == "test-server" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected server to be disconnected")
	}
}

func TestSupervisor_CurrentSnapshot(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "server1", Enabled: true},
			{Name: "server2", Enabled: false},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Reconcile to populate snapshot
	_ = supervisor.reconcile(configSvc.Current())

	snapshot := supervisor.CurrentSnapshot()
	require.NotNil(t, snapshot, "Expected non-nil snapshot")

	if len(snapshot.Servers) != 2 {
		t.Errorf("Expected 2 servers, got %d", len(snapshot.Servers))
	}

	// Verify server states
	if state, ok := snapshot.Servers["server1"]; ok {
		if !state.Enabled {
			t.Error("Expected server1 to be enabled")
		}
	} else {
		t.Error("Expected server1 in snapshot")
	}

	if state, ok := snapshot.Servers["server2"]; ok {
		if state.Enabled {
			t.Error("Expected server2 to be disabled")
		}
	} else {
		t.Error("Expected server2 in snapshot")
	}
}

func TestSupervisor_SnapshotClone(t *testing.T) {
	original := &ServerStateSnapshot{
		Servers: map[string]*ServerState{
			"test": {
				Name:    "test",
				Enabled: true,
				Config:  &config.ServerConfig{Name: "test", Enabled: true},
			},
		},
		Timestamp: time.Now(),
		Version:   1,
	}

	cloned := original.Clone()

	// Verify deep copy
	if cloned == original {
		t.Error("Clone returned same pointer")
	}

	// Modify original
	original.Servers["test"].Enabled = false
	original.Servers["test"].Config.Enabled = false

	// Cloned should be unchanged
	if !cloned.Servers["test"].Enabled {
		t.Error("Clone was mutated")
	}

	if !cloned.Servers["test"].Config.Enabled {
		t.Error("Clone config was mutated")
	}
}

func TestSupervisor_Subscribe(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	eventCh := supervisor.Subscribe()

	// Emit an event
	supervisor.emitEvent(Event{
		Type:       EventReconciliationComplete,
		Timestamp:  time.Now(),
		ServerName: "",
		Payload:    map[string]interface{}{"version": int64(1)},
	})

	// Should receive event
	select {
	case event := <-eventCh:
		if event.Type != EventReconciliationComplete {
			t.Errorf("Expected EventReconciliationComplete, got %s", event.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive event")
	}

	supervisor.Unsubscribe(eventCh)
}

func TestSupervisor_RefreshToolsFromDiscovery(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "server1", Enabled: true},
			{Name: "server2", Enabled: true},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Reconcile to populate initial state
	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)

	// Create discovered tools
	tools := []*config.ToolMetadata{
		{
			Name:        "tool1",
			ServerName:  "server1",
			Description: "Test tool 1",
			ParamsJSON:  `{"type":"object","properties":{"arg1":{"type":"string"}}}`,
		},
		{
			Name:        "tool2",
			ServerName:  "server1",
			Description: "Test tool 2",
			ParamsJSON:  `{"type":"object","properties":{"arg2":{"type":"number"}}}`,
		},
		{
			Name:        "tool3",
			ServerName:  "server2",
			Description: "Test tool 3",
			ParamsJSON:  `{"type":"object","properties":{"arg3":{"type":"boolean"}}}`,
		},
	}

	// Refresh tools from discovery
	err := supervisor.RefreshToolsFromDiscovery(tools)
	if err != nil {
		t.Fatalf("RefreshToolsFromDiscovery failed: %v", err)
	}

	// Verify StateView was updated
	snapshot := supervisor.StateView().Snapshot()

	// Check server1 has 2 tools
	if server1, ok := snapshot.Servers["server1"]; ok {
		if server1.ToolCount != 2 {
			t.Errorf("Expected server1 to have 2 tools, got %d", server1.ToolCount)
		}
		if len(server1.Tools) != 2 {
			t.Errorf("Expected server1 Tools array to have 2 items, got %d", len(server1.Tools))
		}
		if server1.Tools[0].Name != "tool1" {
			t.Errorf("Expected first tool to be 'tool1', got '%s'", server1.Tools[0].Name)
		}
		if server1.Tools[0].Description != "Test tool 1" {
			t.Errorf("Expected first tool description to be 'Test tool 1', got '%s'", server1.Tools[0].Description)
		}
	} else {
		t.Error("Expected server1 in StateView snapshot")
	}

	// Check server2 has 1 tool
	if server2, ok := snapshot.Servers["server2"]; ok {
		if server2.ToolCount != 1 {
			t.Errorf("Expected server2 to have 1 tool, got %d", server2.ToolCount)
		}
		if len(server2.Tools) != 1 {
			t.Errorf("Expected server2 Tools array to have 1 item, got %d", len(server2.Tools))
		}
		if server2.Tools[0].Name != "tool3" {
			t.Errorf("Expected tool to be 'tool3', got '%s'", server2.Tools[0].Name)
		}
	} else {
		t.Error("Expected server2 in StateView snapshot")
	}
}

func TestSupervisor_RefreshToolsFromDiscovery_EmptyTools(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Test with nil tools
	err := supervisor.RefreshToolsFromDiscovery(nil)
	if err != nil {
		t.Errorf("Expected no error with nil tools, got %v", err)
	}

	// Test with empty tools slice
	err = supervisor.RefreshToolsFromDiscovery([]*config.ToolMetadata{})
	if err != nil {
		t.Errorf("Expected no error with empty tools, got %v", err)
	}
}

func TestSupervisor_InspectionExemption_GrantAndRevoke(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Test: Server starts with no exemptions
	if supervisor.IsInspectionExempted("test-server") {
		t.Error("Expected no exemption initially")
	}

	// Test: Grant exemption
	err := supervisor.RequestInspectionExemption("test-server", 5*time.Second)
	if err != nil {
		t.Fatalf("RequestInspectionExemption failed: %v", err)
	}

	// Test: Exemption is active
	if !supervisor.IsInspectionExempted("test-server") {
		t.Error("Expected exemption to be active after request")
	}

	// Test: Revoke exemption
	supervisor.RevokeInspectionExemption("test-server")

	// Test: Exemption is revoked
	if supervisor.IsInspectionExempted("test-server") {
		t.Error("Expected exemption to be revoked")
	}
}

func TestSupervisor_InspectionExemption_AutoExpiry(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Grant short-lived exemption (100ms)
	err := supervisor.RequestInspectionExemption("test-server", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("RequestInspectionExemption failed: %v", err)
	}

	// Exemption should be active immediately
	if !supervisor.IsInspectionExempted("test-server") {
		t.Error("Expected exemption to be active")
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Exemption should be expired now
	if supervisor.IsInspectionExempted("test-server") {
		t.Error("Expected exemption to be expired")
	}
}

func TestSupervisor_InspectionExemption_QuarantinedServerConnects(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "quarantined-server", Enabled: true, Quarantined: true},
		},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Initial reconciliation - quarantined server should NOT connect (no exemption)
	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)

	mockUpstream.mu.Lock()
	connected := mockUpstream.connected["quarantined-server"]
	mockUpstream.mu.Unlock()

	if connected {
		t.Error("Expected quarantined server NOT to connect initially without exemption")
	}

	// Grant exemption
	err := supervisor.RequestInspectionExemption("quarantined-server", 5*time.Second)
	if err != nil {
		t.Fatalf("RequestInspectionExemption failed: %v", err)
	}

	// Wait for reconciliation triggered by RequestInspectionExemption to complete
	time.Sleep(100 * time.Millisecond)

	// Now quarantined server SHOULD be connected due to exemption
	mockUpstream.mu.Lock()
	connected = mockUpstream.connected["quarantined-server"]
	mockUpstream.mu.Unlock()

	if !connected {
		t.Error("Expected quarantined server to connect with active exemption")
	}

	// Revoke exemption
	supervisor.RevokeInspectionExemption("quarantined-server")

	// Exemption should no longer be active
	if supervisor.IsInspectionExempted("quarantined-server") {
		t.Error("Expected exemption to be revoked")
	}

	// Note: In a unit test, we can verify the exemption logic works correctly.
	// Full disconnection behavior is best tested in integration tests where
	// the supervisor's event loop and state synchronization are fully active.
}

func TestSupervisor_InspectionExemption_MultipleServers(t *testing.T) {
	cfg := &config.Config{
		Listen:  "127.0.0.1:8080",
		Servers: []*config.ServerConfig{},
	}

	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// Grant exemptions for multiple servers
	_ = supervisor.RequestInspectionExemption("server1", 5*time.Second)
	_ = supervisor.RequestInspectionExemption("server2", 5*time.Second)
	_ = supervisor.RequestInspectionExemption("server3", 5*time.Second)

	// All should be exempted
	if !supervisor.IsInspectionExempted("server1") {
		t.Error("Expected server1 to be exempted")
	}
	if !supervisor.IsInspectionExempted("server2") {
		t.Error("Expected server2 to be exempted")
	}
	if !supervisor.IsInspectionExempted("server3") {
		t.Error("Expected server3 to be exempted")
	}

	// Revoke one exemption
	supervisor.RevokeInspectionExemption("server2")

	// server2 should be revoked, others still active
	if !supervisor.IsInspectionExempted("server1") {
		t.Error("Expected server1 to still be exempted")
	}
	if supervisor.IsInspectionExempted("server2") {
		t.Error("Expected server2 exemption to be revoked")
	}
	if !supervisor.IsInspectionExempted("server3") {
		t.Error("Expected server3 to still be exempted")
	}
}

// --- Regression tests for the actor_pool event-channel-overflow fix
// (2026-08-13): dropped connected/state_changed events used to leave
// StateView/CurrentSnapshot stuck showing stale Connected forever, since
// updateSnapshot only ever carried forward whatever was already cached.
// See actor_pool.go's DroppedEventCount/emitEvent doc comments and
// supervisor.go's updateSnapshot doc comment for the full causal chain.

func TestSupervisor_UpdateSnapshot_SelfHealsStaleConnectedState(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond) // let the async ConnectServer action land

	mockUpstream.mu.Lock()
	require.True(t, mockUpstream.connected["test-server"], "mock should be connected by now")
	mockUpstream.mu.Unlock()

	// Deterministically seed the exact divergence a dropped event would
	// leave behind: force Supervisor's own cached snapshot to Connected=false
	// while the live upstream genuinely is connected. There is deliberately
	// no code path left that produces this on its own -- that's the fix --
	// so this pokes the internal snapshot directly rather than relying on
	// the reconcile-vs-ConnectServer-goroutine race (which is timing-
	// dependent and could go either way).
	supervisor.snapshot.Store(&ServerStateSnapshot{
		Servers: map[string]*ServerState{
			"test-server": {Name: "test-server", Enabled: true, Connected: false},
		},
		Timestamp: time.Now(),
		Version:   999,
	})

	// Regression check: call updateSnapshot again with no new event and no
	// config change -- simulating a periodic or drop-triggered resync.
	// Pre-fix, this carried forward whatever was already cached (false, just
	// seeded above) forever, with no other path to correct it. Post-fix, it
	// reads live state from the upstream on every call.
	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())

	snap := supervisor.CurrentSnapshot()
	require.True(t, snap.Servers["test-server"].Connected,
		"updateSnapshot should self-heal from live state, not carry forward a stale cached value")

	status, ok := supervisor.StateView().GetServer("test-server")
	require.True(t, ok)
	require.True(t, status.Connected, "StateView should also reflect live state after resync")
}

func TestSupervisor_UpdateSnapshot_DoesNotRefireConnectCallback(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	var callbackCount atomic.Int32
	supervisor.SetOnServerConnectedCallback(func(_ string) {
		callbackCount.Add(1)
	})

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)

	// Multiple resync passes while the server stays connected must NOT
	// re-trigger reactive tool discovery. Only updateSnapshotFromEvent (a
	// real connect *event*) may invoke that callback; this path must not,
	// or a periodic/drop-triggered resync would re-poll ListTools on every
	// pass for every already-connected server.
	for i := 0; i < 5; i++ {
		supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())
	}

	require.EqualValues(t, 0, callbackCount.Load(),
		"updateSnapshot must never invoke the reactive-connect callback -- only updateSnapshotFromEvent may")
}

func TestSupervisor_UpdateSnapshot_EmitsEventOnCorrection(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)
	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates()) // settle: cache now matches live (Connected=true)

	eventCh := supervisor.Subscribe()
	defer supervisor.Unsubscribe(eventCh)

	// Simulate a dropped disconnect notification: live state disagrees with
	// what Supervisor has cached.
	mockUpstream.mu.Lock()
	mockUpstream.states["test-server"].Connected = false
	mockUpstream.mu.Unlock()

	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())

	select {
	case ev := <-eventCh:
		require.Equal(t, EventServerStateChanged, ev.Type)
		require.Equal(t, "test-server", ev.ServerName)
		require.Equal(t, "resync", ev.Payload["source"])
	case <-time.After(1 * time.Second):
		t.Fatal("expected a state-changed event when resync corrects cached state -- " +
			"the Web UI has no other path to learn about the correction")
	}
}

func TestSupervisor_CheckDroppedEventsAndReconcile(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	// No drop observed yet -> must not reconcile.
	supervisor.checkDroppedEventsAndReconcile()
	time.Sleep(20 * time.Millisecond)
	mockUpstream.mu.Lock()
	_, added := mockUpstream.addedServers["test-server"]
	mockUpstream.mu.Unlock()
	require.False(t, added, "expected no reconcile when dropped count is unchanged (0)")

	// A drop is detected -> must trigger reconcile immediately, without
	// waiting for the normal 30s ticker.
	mockUpstream.droppedEvents.Store(1)
	supervisor.checkDroppedEventsAndReconcile()
	time.Sleep(50 * time.Millisecond)

	mockUpstream.mu.Lock()
	_, added = mockUpstream.addedServers["test-server"]
	connected := mockUpstream.connected["test-server"]
	mockUpstream.mu.Unlock()
	require.True(t, added, "expected reconcile to run after a detected drop")
	require.True(t, connected)
	require.EqualValues(t, 1, supervisor.lastDroppedEvents)

	// Calling again with the same dropped count must be a no-op.
	supervisor.checkDroppedEventsAndReconcile()
	require.EqualValues(t, 1, supervisor.lastDroppedEvents)
}

func TestStateViewNeedsUpdate(t *testing.T) {
	cfgA := &config.ServerConfig{Name: "a"}
	base := &ServerState{Connected: true, ToolCount: 3, Config: cfgA}

	require.True(t, stateViewNeedsUpdate(nil, base), "nil prev must always need a write")

	identical := &ServerState{Connected: true, ToolCount: 3, Config: cfgA}
	require.False(t, stateViewNeedsUpdate(base, identical), "identical state should not need a write")

	changedConnected := &ServerState{Connected: false, ToolCount: 3, Config: cfgA}
	require.True(t, stateViewNeedsUpdate(base, changedConnected))

	changedToolCount := &ServerState{Connected: true, ToolCount: 4, Config: cfgA}
	require.True(t, stateViewNeedsUpdate(base, changedToolCount))

	withErr := &ServerState{Connected: true, ToolCount: 3, Config: cfgA,
		ConnectionInfo: &types.ConnectionInfo{LastError: errors.New("boom")}}
	require.True(t, stateViewNeedsUpdate(base, withErr))

	sameErrTwice := &ServerState{Connected: true, ToolCount: 3, Config: cfgA,
		ConnectionInfo: &types.ConnectionInfo{LastError: errors.New("boom")}}
	require.False(t, stateViewNeedsUpdate(withErr, sameErrTwice), "same error text twice should not need a write")

	// Must compare config FIELDS, not pointer identity -- configFieldsDiffer
	// deliberately ignores Name (not one of configChanged's fields either)
	// so a different *ServerConfig pointer with otherwise-identical
	// URL/Protocol/Command/Enabled/Quarantined must NOT be seen as changed.
	samePointerDifferentName := &ServerState{Connected: true, ToolCount: 3,
		Config: &config.ServerConfig{Name: "a-renamed"}}
	require.False(t, stateViewNeedsUpdate(base, samePointerDifferentName),
		"a config field configFieldsDiffer doesn't track (Name) must not trigger a write")

	changedURL := &ServerState{Connected: true, ToolCount: 3,
		Config: &config.ServerConfig{Name: "a", URL: "https://changed.example.com"}}
	require.True(t, stateViewNeedsUpdate(base, changedURL))
}

func TestConfigFieldsDiffer(t *testing.T) {
	a := &config.ServerConfig{Name: "x", URL: "https://a.example.com", Enabled: true}
	require.False(t, configFieldsDiffer(a, a), "identical pointer is never different")
	require.False(t, configFieldsDiffer(nil, nil))
	require.True(t, configFieldsDiffer(nil, a))
	require.True(t, configFieldsDiffer(a, nil))

	sameFields := &config.ServerConfig{Name: "renamed-only", URL: a.URL, Enabled: a.Enabled}
	require.False(t, configFieldsDiffer(a, sameFields),
		"a different pointer with identical tracked fields must read as unchanged")

	diffURL := &config.ServerConfig{Name: "x", URL: "https://b.example.com", Enabled: true}
	require.True(t, configFieldsDiffer(a, diffURL))

	diffEnabled := &config.ServerConfig{Name: "x", URL: a.URL, Enabled: false}
	require.True(t, configFieldsDiffer(a, diffEnabled))
}

// TestSupervisor_UpdateSnapshot_PreservesDiscoveredToolCount is a regression
// test for a bug found in the SAME review pass that produced the fix (not
// pre-existing): ActorPoolSimple.GetAllStates()'s ToolCount comes from
// client.GetCachedToolCountNonBlocking(), which reads mc.toolCount -- a
// field with zero production writers (only Client.GetCachedToolCount(ctx),
// itself with zero production callers, ever sets it). Blindly taking
// liveState.ToolCount on every resync would silently reset every connected
// server's real, discovery-derived tool count to 0 every ~30s (or faster,
// via the drop-triggered fast path). updateSnapshotFromEvent already guards
// this same case; updateSnapshot must too.
func TestSupervisor_UpdateSnapshot_PreservesDiscoveredToolCount(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)

	// Simulate background tool discovery populating real tools/count, the
	// way runtime.DiscoverAndIndexTools -> RefreshToolsFromDiscovery does.
	tools := []*config.ToolMetadata{
		{Name: "tool_a", ServerName: "test-server"},
		{Name: "tool_b", ServerName: "test-server"},
		{Name: "tool_c", ServerName: "test-server"},
	}
	require.NoError(t, supervisor.RefreshToolsFromDiscovery(tools))

	snap := supervisor.CurrentSnapshot()
	require.Equal(t, 3, snap.Servers["test-server"].ToolCount, "sanity: discovery populated 3 tools")

	// The mock's own live ToolCount (GetAllStates -> AddServer's zero-value)
	// is 0/unset here -- exactly like production's mc.toolCount always
	// being 0. A resync must not use it and stomp the discovered count.
	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())

	snap = supervisor.CurrentSnapshot()
	require.Equal(t, 3, snap.Servers["test-server"].ToolCount,
		"updateSnapshot must preserve discovery-derived ToolCount, not reset it from live state's always-0 cache")

	status, ok := supervisor.StateView().GetServer("test-server")
	require.True(t, ok)
	require.Equal(t, 3, status.ToolCount, "StateView must also keep the discovered count")
}

// TestSupervisor_UpdateSnapshot_PreservesConnectedAtAcrossResyncs is a
// regression test for the same review pass: LastSeen feeds StateView's
// ConnectedAt (a user-visible "when did this connect" timestamp via
// updateStateView's "if state.Connected && !state.LastSeen.IsZero()"
// branch). Bumping it to time.Now() on every resync would make that field
// read as "last time we resynced" instead of "when it actually connected".
func TestSupervisor_UpdateSnapshot_PreservesConnectedAtAcrossResyncs(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)
	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates()) // settle

	status, ok := supervisor.StateView().GetServer("test-server")
	require.True(t, ok)
	require.NotNil(t, status.ConnectedAt, "should have a ConnectedAt after settling while connected")
	firstConnectedAt := *status.ConnectedAt

	time.Sleep(20 * time.Millisecond)

	// Multiple further resyncs while the server stays connected the whole
	// time must not move ConnectedAt.
	for i := 0; i < 3; i++ {
		supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())
	}

	status, ok = supervisor.StateView().GetServer("test-server")
	require.True(t, ok)
	require.NotNil(t, status.ConnectedAt)
	require.True(t, status.ConnectedAt.Equal(firstConnectedAt),
		"ConnectedAt must not advance on resyncs that don't change Connected -- "+
			"got %v, want unchanged %v", status.ConnectedAt, firstConnectedAt)
}

// TestSupervisor_UpdateSnapshot_NoSpuriousEventsWhenNothingChanged guards
// against configFieldsDiffer/stateViewNeedsUpdate over-firing on every
// resync tick because of comparing config pointer identity instead of
// fields (raised in review: configsvc.Service.Current() happens to return
// stable pointers between real config updates, but relying on that would be
// fragile). Calling updateSnapshot repeatedly with a truly-unchanged config
// snapshot and unchanged live state must emit nothing after the first
// settle.
func TestSupervisor_UpdateSnapshot_NoSpuriousEventsWhenNothingChanged(t *testing.T) {
	cfg := &config.Config{
		Listen: "127.0.0.1:8080",
		Servers: []*config.ServerConfig{
			{Name: "test-server", Enabled: true},
		},
	}
	configSvc := configsvc.NewService(cfg, "/tmp/config.json", zap.NewNop())
	defer configSvc.Close()

	mockUpstream := NewMockUpstreamAdapter()
	defer mockUpstream.Close()

	supervisor := New(configSvc, mockUpstream, zap.NewNop())

	_ = supervisor.reconcile(configSvc.Current())
	time.Sleep(50 * time.Millisecond)
	supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates()) // settle

	eventCh := supervisor.Subscribe()
	defer supervisor.Unsubscribe(eventCh)

	for i := 0; i < 5; i++ {
		supervisor.updateSnapshot(configSvc.Current(), mockUpstream.GetAllStates())
	}

	select {
	case ev := <-eventCh:
		t.Fatalf("expected no events when nothing changed across 5 resyncs, got %+v", ev)
	case <-time.After(200 * time.Millisecond):
		// Correct: no spurious events.
	}
}
