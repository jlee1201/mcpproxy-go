package supervisor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream"
)

// ActorPoolSimple is a simplified facade over UpstreamManager that delegates all operations.
// Phase 7.3: Avoids double lifecycle management by using UpstreamManager's existing client management.
type ActorPoolSimple struct {
	manager *upstream.Manager
	logger  *zap.Logger

	// Event forwarding
	listeners []chan Event
	eventMu   sync.RWMutex

	// droppedEvents counts events dropped by emitEvent because a listener's
	// buffer was full (see emitEvent). Supervisor polls this to trigger an
	// early corrective reconcile instead of waiting out the normal ticker --
	// see Supervisor.reconciliationLoop's fast-check case.
	droppedEvents atomic.Uint64
}

// NewActorPoolSimple creates a simplified actor pool that delegates to UpstreamManager.
func NewActorPoolSimple(manager *upstream.Manager, logger *zap.Logger) *ActorPoolSimple {
	if logger == nil {
		logger = zap.NewNop()
	}

	pool := &ActorPoolSimple{
		manager:   manager,
		logger:    logger,
		listeners: make([]chan Event, 0),
	}

	// Subscribe to manager notifications and forward as events
	manager.AddNotificationHandler(pool)

	return pool
}

// SendNotification implements upstream.NotificationHandler interface
func (p *ActorPoolSimple) SendNotification(notification *upstream.Notification) {
	// Convert upstream notifications to supervisor events
	var eventType EventType

	switch notification.Level {
	case upstream.NotificationInfo:
		if notification.Title == "Server Connected" {
			eventType = EventServerConnected
		} else {
			eventType = EventServerStateChanged
		}
	case upstream.NotificationWarning, upstream.NotificationError:
		if notification.Title == "Server Disconnected" {
			eventType = EventServerDisconnected
		} else {
			eventType = EventServerStateChanged
		}
	default:
		eventType = EventServerStateChanged
	}

	event := Event{
		Type:       eventType,
		ServerName: notification.ServerName,
		Timestamp:  notification.Timestamp,
		Payload: map[string]interface{}{
			"level":     notification.Level.String(),
			"title":     notification.Title,
			"message":   notification.Message,
			"connected": notification.Title == "Server Connected",
		},
	}

	p.emitEvent(event)
}

// AddServer adds a server configuration to the manager.
func (p *ActorPoolSimple) AddServer(name string, cfg *config.ServerConfig) error {
	p.logger.Debug("Adding server via manager", zap.String("name", name))

	if err := p.manager.AddServerConfig(name, cfg); err != nil {
		return fmt.Errorf("failed to add server config: %w", err)
	}

	// Emit event
	p.emitEvent(Event{
		Type:       EventServerAdded,
		ServerName: name,
		Timestamp:  time.Now(),
		Payload: map[string]interface{}{
			"enabled":     cfg.Enabled,
			"quarantined": cfg.Quarantined,
		},
	})

	return nil
}

// RemoveServer removes a server from the manager.
func (p *ActorPoolSimple) RemoveServer(name string) error {
	p.logger.Debug("Removing server via manager", zap.String("name", name))

	p.manager.RemoveServer(name)

	// Emit event
	p.emitEvent(Event{
		Type:       EventServerRemoved,
		ServerName: name,
		Timestamp:  time.Now(),
		Payload:    map[string]interface{}{},
	})

	return nil
}

// ConnectServer tells the manager to connect a server.
func (p *ActorPoolSimple) ConnectServer(ctx context.Context, name string) error {
	p.logger.Debug("Connecting server via manager", zap.String("name", name))

	client, exists := p.manager.GetClient(name)
	if !exists {
		return fmt.Errorf("server %s not found", name)
	}

	// Attempt to connect (managed client will handle state checks)
	// If client is already connecting/connected, Connect() will return an error
	// which we log but don't treat as fatal (supervisor will retry)
	if err := client.Connect(ctx); err != nil {
		p.logger.Debug("Connect returned error (may be already connecting/connected)",
			zap.String("server", name),
			zap.Error(err))
		// Not returning error - this is expected if client is already connecting
		// Supervisor's reconciliation logic will handle retries if needed
	}

	return nil
}

// DisconnectServer tells the manager to disconnect a server.
func (p *ActorPoolSimple) DisconnectServer(name string) error {
	p.logger.Debug("Disconnecting server via manager", zap.String("name", name))

	client, exists := p.manager.GetClient(name)
	if !exists {
		return fmt.Errorf("server %s not found", name)
	}

	return client.Disconnect()
}

// ConnectAll tells the manager to connect all servers.
func (p *ActorPoolSimple) ConnectAll(ctx context.Context) error {
	p.logger.Debug("Connecting all servers via manager")
	return p.manager.ConnectAll(ctx)
}

// GetServerState returns the current state of a server from the manager.
// Phase 7.1 FIX: Fetches tools for the specific server to avoid blocking all servers.
//
// Deliberately does NOT call client.GetConfig() and does not populate
// Config/Enabled/Quarantined. This is the sole caller-facing accessor used on
// the hot event-consumption path (Supervisor.updateSnapshotFromEvent, which
// only reads ToolCount and ConnectionInfo from the result -- verified as the
// only production call site). client.GetConfig() takes mc.mu.RLock(), and
// managed.Client.Connect()/Disconnect() hold that same mutex for the whole
// call including network I/O. During an OAuth reconnect burst, that meant
// this method blocked -- on the single event-forwarder goroutine -- for
// however long a concurrent Connect() took, stalling delivery for every
// other server's events too and overflowing the 50-slot channel upstream
// ("Event channel full, dropping event"). IsConnected()/GetConnectionInfo()/
// GetCachedToolCountNonBlocking() are all StateManager-/cache-scoped and
// never touch mc.mu. If a future caller needs Config/Enabled/Quarantined
// from this method, populate them explicitly and re-audit every caller for
// this same blocking risk -- don't just add GetConfig() back in.
func (p *ActorPoolSimple) GetServerState(name string) (*ServerState, error) {
	client, exists := p.manager.GetClient(name)
	if !exists {
		return nil, fmt.Errorf("server %s not found", name)
	}

	connected := client.IsConnected()

	state := &ServerState{
		Name:      name,
		Connected: connected,
	}

	// Get connection info (StateManager-scoped copy, does not take mc.mu)
	connInfo := client.GetConnectionInfo()
	state.ConnectionInfo = &connInfo

	// Phase 7.1 FIX: Don't fetch tools here! It blocks on slow servers.
	// Tools are populated via:
	// 1. Background tool indexing (runtime.DiscoverAndIndexTools)
	// 2. Cached tool count from managed client (non-blocking)
	// State updates happen via events, not synchronous tool fetching.
	if connected {
		// Use cached tool count (non-blocking) instead of fetching tools
		state.ToolCount = client.GetCachedToolCountNonBlocking()
	}

	return state, nil
}

// GetAllStates returns the current state of all servers from the manager.
func (p *ActorPoolSimple) GetAllStates() map[string]*ServerState {
	states := make(map[string]*ServerState)

	// Get all clients from manager
	clients := p.manager.GetAllClients()

	for name, client := range clients {
		connected := client.IsConnected()

		state := &ServerState{
			Name:      name,
			Config:    client.Config,
			Enabled:   client.Config.Enabled,
			Connected: connected,
		}

		if client.Config.Quarantined {
			state.Quarantined = true
		}

		// Get connection info
		connInfo := client.GetConnectionInfo()
		state.ConnectionInfo = &connInfo

		// Phase 7.1 FIX: Don't fetch tools synchronously! Use cached data instead.
		if connected {
			// Use cached tool count (non-blocking)
			state.ToolCount = client.GetCachedToolCountNonBlocking()
		}

		states[name] = state
	}

	return states
}

// IsUserLoggedOut returns true if the user explicitly logged out from the server.
// This prevents automatic reconnection after explicit logout.
func (p *ActorPoolSimple) IsUserLoggedOut(name string) bool {
	client, exists := p.manager.GetClient(name)
	if !exists {
		return false
	}
	return client.IsUserLoggedOut()
}

func (p *ActorPoolSimple) ShouldSkipReconnect(name string) bool {
	client, exists := p.manager.GetClient(name)
	if !exists {
		return false
	}
	if client.StateManager.IsOAuthError() {
		return true
	}
	return !client.ShouldRetry()
}

// Subscribe returns a channel that receives supervisor events.
func (p *ActorPoolSimple) Subscribe() <-chan Event {
	p.eventMu.Lock()
	defer p.eventMu.Unlock()

	// F3: bumped 50 -> 500 as defense-in-depth headroom against burst
	// producers (see emitEvent). Not a fix by itself -- F1 (this file) and
	// the live resync in Supervisor.updateSnapshot are what actually bound
	// staleness; this just buys more room before that matters.
	ch := make(chan Event, 500)
	p.listeners = append(p.listeners, ch)
	return ch
}

// Unsubscribe removes a subscriber channel.
func (p *ActorPoolSimple) Unsubscribe(ch <-chan Event) {
	p.eventMu.Lock()
	defer p.eventMu.Unlock()

	for i, listener := range p.listeners {
		if listener == ch {
			p.listeners = append(p.listeners[:i], p.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// emitEvent sends an event to all subscribers.
func (p *ActorPoolSimple) emitEvent(event Event) {
	p.eventMu.RLock()
	defer p.eventMu.RUnlock()

	for _, ch := range p.listeners {
		select {
		case ch <- event:
		default:
			p.droppedEvents.Add(1)
			p.logger.Warn("Event channel full, dropping event",
				zap.String("event_type", string(event.Type)),
				zap.String("server", event.ServerName))
		}
	}
}

// DroppedEventCount returns the number of events dropped so far because a
// listener's channel was full. Supervisor polls this on a short ticker to
// trigger an immediate corrective reconcile (which re-derives Connected
// state live from the manager, see updateSnapshot) rather than waiting for
// the next scheduled reconcile -- a dropped connected/disconnected
// notification would otherwise leave the cached state wrong until whatever
// later event happens to get through.
func (p *ActorPoolSimple) DroppedEventCount() uint64 {
	return p.droppedEvents.Load()
}

// Close cleans up the pool.
func (p *ActorPoolSimple) Close() {
	p.eventMu.Lock()
	defer p.eventMu.Unlock()

	for _, ch := range p.listeners {
		close(ch)
	}
	p.listeners = nil
}
