package supervisor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"strings"

	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/runtime/configsvc"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/runtime/stateview"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/upstream/types"
)

// Supervisor manages the desired vs actual state reconciliation for upstream servers.
// It subscribes to config changes and emits events when server states change.
type Supervisor struct {
	logger *zap.Logger

	// Config service for desired state
	configSvc *configsvc.Service

	// Upstream adapter for actual state
	upstream UpstreamInterface

	// State tracking
	snapshot atomic.Value // *ServerStateSnapshot
	version  int64
	stateMu  sync.RWMutex

	// State view for read model (Phase 4)
	stateView *stateview.View

	// Event publishing
	listeners []chan Event
	eventMu   sync.RWMutex

	// lastDroppedEvents is the last-observed value of upstream.DroppedEventCount(),
	// used by reconciliationLoop's fast-check case to detect a new drop and
	// trigger an early corrective reconcile.
	lastDroppedEvents uint64

	// Callback for reactive tool discovery on server connection
	onServerConnectedCallback func(serverName string)
	callbackMu                sync.RWMutex

	// Inspection exemptions for temporary connections to quarantined servers
	inspectionExemptions   map[string]time.Time
	inspectionExemptionsMu sync.RWMutex

	// Circuit breaker for inspection failures (Phase 2: Issue #105 stability)
	inspectionFailures   map[string]*inspectionFailureInfo
	inspectionFailuresMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// inspectionFailureInfo tracks inspection failures for circuit breaker pattern
type inspectionFailureInfo struct {
	consecutiveFailures int
	lastFailureTime     time.Time
	cooldownUntil       time.Time
}

// UpstreamInterface defines the interface for upstream adapters.
type UpstreamInterface interface {
	AddServer(name string, cfg *config.ServerConfig) error
	RemoveServer(name string) error
	ConnectServer(ctx context.Context, name string) error
	DisconnectServer(name string) error
	ConnectAll(ctx context.Context) error
	GetServerState(name string) (*ServerState, error)
	GetAllStates() map[string]*ServerState
	IsUserLoggedOut(name string) bool     // Returns true if user explicitly logged out (prevents auto-reconnect)
	ShouldSkipReconnect(name string) bool // Returns true if server should not be auto-reconnected (OAuth error or backoff active)
	Subscribe() <-chan Event
	Unsubscribe(ch <-chan Event)
	Close()
	// DroppedEventCount returns the cumulative count of events dropped due to
	// a full listener channel. Used to trigger an early corrective reconcile.
	DroppedEventCount() uint64
}

// New creates a new supervisor.
func New(configSvc *configsvc.Service, upstream UpstreamInterface, logger *zap.Logger) *Supervisor {
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Supervisor{
		logger:               logger,
		configSvc:            configSvc,
		upstream:             upstream,
		version:              0,
		stateView:            stateview.New(),
		listeners:            make([]chan Event, 0),
		inspectionExemptions: make(map[string]time.Time),
		inspectionFailures:   make(map[string]*inspectionFailureInfo),
		ctx:                  ctx,
		cancel:               cancel,
	}

	// Initialize empty snapshot
	s.snapshot.Store(&ServerStateSnapshot{
		Servers:   make(map[string]*ServerState),
		Timestamp: time.Now(),
		Version:   0,
	})

	return s
}

// Start begins the supervisor's reconciliation loop.
func (s *Supervisor) Start() {
	s.logger.Info("Starting supervisor")

	// Subscribe to config changes
	configUpdates := s.configSvc.Subscribe(s.ctx)

	// Subscribe to upstream events
	upstreamEvents := s.upstream.Subscribe()

	// Start event forwarding goroutine
	s.wg.Add(1)
	go s.forwardUpstreamEvents(upstreamEvents)

	// Start reconciliation loop
	s.wg.Add(1)
	go s.reconciliationLoop(configUpdates)

	// Start exemption cleanup loop
	s.wg.Add(1)
	go s.exemptionCleanupLoop()

	// Phase 7.1: Trigger initial reconciliation to populate StateView
	go func() {
		time.Sleep(500 * time.Millisecond) // Give servers time to connect
		currentConfig := s.configSvc.Current()
		if err := s.reconcile(currentConfig); err != nil {
			s.logger.Error("Initial reconciliation failed", zap.Error(err))
		} else {
			s.logger.Info("Initial reconciliation completed, StateView populated")
		}
	}()

	s.logger.Info("Supervisor started")
}

// reconciliationLoop processes config updates and reconciles state.
func (s *Supervisor) reconciliationLoop(configUpdates <-chan configsvc.Update) {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// dropCheckTicker is a cheap (atomic-load-only) fast path: if the
	// upstream event channel dropped anything since we last checked, don't
	// wait out the full 30s ticker to self-correct -- reconcile now. See
	// ActorPoolSimple.DroppedEventCount's doc comment for why this matters:
	// a dropped connected/disconnected event otherwise leaves updateSnapshot's
	// cached state wrong until some unrelated later event happens to land.
	dropCheckTicker := time.NewTicker(3 * time.Second)
	defer dropCheckTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Supervisor reconciliation loop stopping")
			return

		case <-dropCheckTicker.C:
			s.checkDroppedEventsAndReconcile()

		case update, ok := <-configUpdates:
			if !ok {
				s.logger.Warn("Config updates channel closed")
				return
			}

			s.logger.Info("Config update received, reconciling",
				zap.String("type", string(update.Type)),
				zap.Int64("version", update.Snapshot.Version))

			if err := s.reconcile(update.Snapshot); err != nil {
				s.logger.Error("Reconciliation failed", zap.Error(err))
				s.emitEvent(Event{
					Type:      EventReconciliationFailed,
					Timestamp: time.Now(),
					Payload: map[string]interface{}{
						"error":   err.Error(),
						"version": update.Snapshot.Version,
					},
				})
			} else {
				s.emitEvent(Event{
					Type:      EventReconciliationComplete,
					Timestamp: time.Now(),
					Payload: map[string]interface{}{
						"version": update.Snapshot.Version,
					},
				})
			}

		case <-ticker.C:
			// Periodic reconciliation to handle drift
			s.logger.Debug("Periodic reconciliation check")
			currentConfig := s.configSvc.Current()
			if err := s.reconcile(currentConfig); err != nil {
				s.logger.Error("Periodic reconciliation failed", zap.Error(err))
			}
		}
	}
}

// checkDroppedEventsAndReconcile compares the upstream's current
// DroppedEventCount against the last-seen value and, if it increased,
// triggers an immediate reconcile (which re-derives Connected state live via
// updateSnapshot) instead of waiting for the next scheduled tick. Extracted
// from reconciliationLoop's ticker case so it's testable without a real
// timer.
func (s *Supervisor) checkDroppedEventsAndReconcile() {
	dropped := s.upstream.DroppedEventCount()
	if dropped == s.lastDroppedEvents {
		return
	}
	s.logger.Warn("Detected dropped upstream events, triggering early corrective reconcile",
		zap.Uint64("total_dropped", dropped),
		zap.Uint64("previously_seen", s.lastDroppedEvents))
	s.lastDroppedEvents = dropped
	if err := s.reconcile(s.configSvc.Current()); err != nil {
		s.logger.Error("Corrective reconciliation failed", zap.Error(err))
	}
}

// exemptionCleanupLoop periodically checks for expired inspection exemptions.
func (s *Supervisor) exemptionCleanupLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Exemption cleanup loop stopping")
			return

		case <-ticker.C:
			// Check for expired exemptions
			s.inspectionExemptionsMu.Lock()
			expiredServers := make([]string, 0)
			for serverName, expiryTime := range s.inspectionExemptions {
				if time.Now().After(expiryTime) {
					expiredServers = append(expiredServers, serverName)
					delete(s.inspectionExemptions, serverName)
				}
			}
			s.inspectionExemptionsMu.Unlock()

			// Trigger reconciliation for each expired exemption to disconnect servers
			for _, serverName := range expiredServers {
				s.logger.Warn("⚠️ Inspection exemption expired, triggering disconnect",
					zap.String("server", serverName))

				currentConfig := s.configSvc.Current()
				if err := s.reconcile(currentConfig); err != nil {
					s.logger.Error("Failed to trigger reconciliation after exemption expiry",
						zap.String("server", serverName),
						zap.Error(err))
				}
			}
		}
	}
}

// reconcile compares desired vs actual state and takes corrective actions.
// Phase 6 Fix: Made fully async to prevent blocking HTTP server startup.
func (s *Supervisor) reconcile(configSnapshot *configsvc.Snapshot) error {
	// Read live state BEFORE taking stateMu, and deliberately outside it.
	// GetAllStates() is non-blocking with respect to mc.mu (see
	// ActorPoolSimple.GetAllStates), but it does take Manager.mu.RLock(),
	// which AddServerConfig holds for its whole duration including secret
	// resolution (found in adversarial review of the F1 fix). Since this
	// reconcile's own action dispatch below can itself trigger
	// AddServerConfig for a newly-added server, calling GetAllStates() while
	// holding stateMu would let that stall block stateMu -- which is exactly
	// the lock updateSnapshotFromEvent (the hot per-event path) needs,
	// reintroducing the same class of stall via a new route. Reading it here
	// means a stall only delays this reconcile's own goroutine, never the
	// event-forwarding one.
	//
	// Accepted tradeoff: reading liveStates before stateMu (rather than
	// inside the same critical section as the write) opens a narrow window
	// between this read and updateSnapshot's write below where a real,
	// successfully-delivered event for the same server could land via
	// updateSnapshotFromEvent and get transiently overwritten by this
	// reconcile's now-stale liveStates. That's strictly better than the bug
	// being fixed here (an unbounded-forever stale value): the overwrite
	// self-heals within one more reconcile interval -- ~3s if drops are
	// still occurring (the fast path re-fires), otherwise the normal 30s
	// ticker -- and it can only happen when the event path is working
	// correctly in the first place. Never blocking the hot
	// per-event path is the harder requirement to give up, so this is the
	// intentional choice, not an oversight.
	liveStates := s.upstream.GetAllStates()

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	s.logger.Debug("Starting reconciliation",
		zap.Int("desired_servers", configSnapshot.ServerCount()))

	plan := s.computeReconcilePlan(configSnapshot)

	// Phase 6 Fix: Execute actions asynchronously to prevent blocking
	// Each action runs in its own goroutine with timeout
	actionCount := 0
	for serverName, action := range plan.Actions {
		if action == ActionNone {
			continue // Skip no-op actions
		}

		actionCount++
		// Launch each action in a goroutine - no waiting!
		go func(name string, act ReconcileAction, snapshot *configsvc.Snapshot) {
			if err := s.executeAction(name, act, snapshot); err != nil {
				s.logger.Error("Failed to execute action",
					zap.String("server", name),
					zap.String("action", string(act)),
					zap.Error(err))
			} else {
				s.logger.Debug("Action completed successfully",
					zap.String("server", name),
					zap.String("action", string(act)))
			}
		}(serverName, action, configSnapshot)
	}

	// Update state snapshot immediately (actions run in background)
	s.updateSnapshot(configSnapshot, liveStates)

	s.logger.Debug("Reconciliation dispatched",
		zap.Int("actions_dispatched", actionCount),
		zap.String("note", "actions running asynchronously"))

	return nil
}

// computeReconcilePlan determines what actions need to be taken.
func (s *Supervisor) computeReconcilePlan(configSnapshot *configsvc.Snapshot) *ReconcilePlan {
	plan := &ReconcilePlan{
		Actions:   make(map[string]ReconcileAction),
		Timestamp: time.Now(),
		Reason:    "config_update",
	}

	currentSnapshot := s.CurrentSnapshot()
	desiredServers := configSnapshot.Config.Servers

	// Check for servers that need to be added or updated
	for _, desiredServer := range desiredServers {
		if desiredServer == nil {
			continue
		}

		name := desiredServer.Name
		currentState, exists := currentSnapshot.Servers[name]

		if !exists {
			// New server needs to be added
			if desiredServer.Enabled && (!desiredServer.Quarantined || s.IsInspectionExempted(name)) {
				plan.Actions[name] = ActionConnect
			} else {
				plan.Actions[name] = ActionNone
			}
		} else {
			// Existing server - check if config changed
			if s.configChanged(currentState.Config, desiredServer) {
				plan.Actions[name] = ActionReconnect
			} else if desiredServer.Enabled && (!desiredServer.Quarantined || s.IsInspectionExempted(name)) && !currentState.Connected {
				// Should be connected but isn't (or has inspection exemption)
				// Don't auto-reconnect if user explicitly logged out or backoff is active
				if s.upstream.IsUserLoggedOut(name) {
					plan.Actions[name] = ActionNone
				} else if s.upstream.ShouldSkipReconnect(name) {
					plan.Actions[name] = ActionNone
				} else {
					plan.Actions[name] = ActionConnect
				}
			} else if (!desiredServer.Enabled || (desiredServer.Quarantined && !s.IsInspectionExempted(name))) && currentState.Connected {
				// Shouldn't be connected but is (or exemption expired)
				plan.Actions[name] = ActionDisconnect
			} else {
				plan.Actions[name] = ActionNone
			}
		}
	}

	// Check for servers that need to be removed
	desiredNames := make(map[string]bool)
	for _, srv := range desiredServers {
		if srv != nil {
			desiredNames[srv.Name] = true
		}
	}

	for name := range currentSnapshot.Servers {
		if !desiredNames[name] {
			plan.Actions[name] = ActionRemove
		}
	}

	return plan
}

// configChanged checks if server configuration has changed.
func (s *Supervisor) configChanged(old, new *config.ServerConfig) bool {
	return configFieldsDiffer(old, new)
}

// executeAction performs the specified action on a server.
func (s *Supervisor) executeAction(serverName string, action ReconcileAction, configSnapshot *configsvc.Snapshot) error {
	s.logger.Debug("Executing action",
		zap.String("server", serverName),
		zap.String("action", string(action)))

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	switch action {
	case ActionNone:
		// No action needed
		return nil

	case ActionConnect:
		// Add server and connect
		serverConfig := configSnapshot.GetServer(serverName)
		if serverConfig == nil {
			return fmt.Errorf("server config not found: %s", serverName)
		}

		if err := s.upstream.AddServer(serverName, serverConfig); err != nil {
			return fmt.Errorf("failed to add server: %w", err)
		}

		// Connect if enabled and (not quarantined OR has inspection exemption)
		if serverConfig.Enabled && (!serverConfig.Quarantined || s.IsInspectionExempted(serverName)) {
			// Log security event when connecting quarantined server via exemption
			if serverConfig.Quarantined && s.IsInspectionExempted(serverName) {
				s.logger.Warn("⚠️ Connecting quarantined server for inspection",
					zap.String("server", serverName))
			}

			if err := s.upstream.ConnectServer(ctx, serverName); err != nil {
				s.logger.Warn("Failed to connect server (will retry)",
					zap.String("server", serverName),
					zap.Error(err))
				// Don't return error - managed client will retry
			}
		}

		return nil

	case ActionDisconnect:
		return s.upstream.DisconnectServer(serverName)

	case ActionReconnect:
		// Disconnect then reconnect
		if err := s.upstream.DisconnectServer(serverName); err != nil {
			s.logger.Warn("Failed to disconnect server during reconnect",
				zap.String("server", serverName),
				zap.Error(err))
		}

		// Get updated config
		serverConfig := configSnapshot.GetServer(serverName)
		if serverConfig == nil {
			return fmt.Errorf("server config not found: %s", serverName)
		}

		// Add with new config
		if err := s.upstream.AddServer(serverName, serverConfig); err != nil {
			return fmt.Errorf("failed to add server: %w", err)
		}

		// Connect if enabled and (not quarantined OR has inspection exemption)
		if serverConfig.Enabled && (!serverConfig.Quarantined || s.IsInspectionExempted(serverName)) {
			// Log security event when connecting quarantined server via exemption
			if serverConfig.Quarantined && s.IsInspectionExempted(serverName) {
				s.logger.Warn("⚠️ Reconnecting quarantined server for inspection",
					zap.String("server", serverName))
			}

			if err := s.upstream.ConnectServer(ctx, serverName); err != nil {
				s.logger.Warn("Failed to reconnect server (will retry)",
					zap.String("server", serverName),
					zap.Error(err))
			}
		}

		return nil

	case ActionRemove:
		return s.upstream.RemoveServer(serverName)

	default:
		return fmt.Errorf("unknown action: %s", action)
	}
}

// updateSnapshot updates the current state snapshot.
//
// F4/self-healing fix (2026-08-13): this used to carry forward whatever
// Connected/ConnectionInfo/ToolCount was already cached in the Supervisor's
// own snapshot, on the theory (see the removed comment below) that querying
// live state here would block on ListTools(). That's no longer true --
// ActorPoolSimple.GetAllStates() was fixed under Phase 7.1 to use only
// non-blocking, mc.mu-free accessors (IsConnected/GetConnectionInfo/
// GetCachedToolCountNonBlocking) -- but nobody undid this caller-side
// workaround, so "carry forward the cache" remained the only path, and it
// has no invalidator: if an upstream connected/disconnected/state_changed
// event was ever dropped (see actor_pool.go's "Event channel full, dropping
// event"), the stale value it should have corrected stayed wrong
// *indefinitely* -- reconcile()'s own action-planning reads this same stale
// value (computeReconcilePlan), so it can't self-heal either. Reading live
// state here on every reconcile (30s ticker, every config change, and now
// also the drop-triggered fast path in reconciliationLoop) bounds staleness
// to one reconcile interval instead of forever.
//
// Several things this must NOT do, found in a second (adversarial-review)
// pass over the first version of this fix: (a) it must not call the
// reactive-tool-discovery callback that updateSnapshotFromEvent fires on
// connect -- updateStateView (below) has no such call, so routing through it
// is safe by construction; re-adding that call here would re-trigger a real
// ListTools()+reindex on every tick for every already-connected server.
// (b) it must not unconditionally deep-clone the whole StateView
// (stateview.UpdateServer clones every server on every call) for every
// configured server on every tick -- with N servers that's O(N^2) work done
// while s.stateMu is held, which stalls the same lock the hot per-event path
// (updateSnapshotFromEvent) needs. So this only calls updateStateView for
// servers whose live-relevant fields actually changed, and it emits a
// state-changed event for those so the Web UI (which has no other poll path
// for the server list) still gets nudged even though no per-event
// notification carried the correction. (c) it must not take ToolCount
// straight from live state -- see the comment inline below, that field is
// never populated in production and doing so would zero out every
// discovery-derived tool count on every reconcile. (d) LastSeen must not be
// bumped on every pass either, or StateView's "connected at" timestamp would
// read as "last resynced" instead of "when it actually connected". (e) the
// live read itself (liveStates, passed in) must happen in reconcile()
// BEFORE s.stateMu is acquired, not in here -- GetAllStates() takes
// Manager.mu.RLock(), which AddServerConfig can hold for a while (secret
// resolution); doing that read while holding stateMu would let that stall
// block stateMu, which is exactly the lock the hot per-event path needs.
func (s *Supervisor) updateSnapshot(configSnapshot *configsvc.Snapshot, liveStates map[string]*ServerState) {
	s.version++

	// Fallback for a server the manager doesn't know about yet (e.g. not yet
	// added this reconcile), and the source of truth for anything live state
	// doesn't carry (Tools/ToolCount from background discovery, LastSeen).
	currentSnapshot := s.CurrentSnapshot()
	previousStates := make(map[string]*ServerState)
	if currentSnapshot != nil {
		for name, state := range currentSnapshot.Servers {
			previousStates[name] = state
		}
	}

	// Merge desired and actual state
	newSnapshot := &ServerStateSnapshot{
		Servers:   make(map[string]*ServerState),
		Timestamp: time.Now(),
		Version:   s.version,
	}

	var corrected []string

	// Add all configured servers
	for _, srv := range configSnapshot.Config.Servers {
		if srv == nil {
			continue
		}

		prevState := previousStates[srv.Name]

		state := &ServerState{
			Name:           srv.Name,
			Config:         srv,
			Enabled:        srv.Enabled,
			Quarantined:    srv.Quarantined,
			DesiredVersion: configSnapshot.Version,
			LastReconcile:  time.Now(),
		}

		if liveState, ok := liveStates[srv.Name]; ok {
			state.Connected = liveState.Connected
			state.ConnectionInfo = liveState.ConnectionInfo

			// ToolCount/LastSeen need care, not a blind take:
			//
			// liveState.ToolCount comes from
			// client.GetCachedToolCountNonBlocking(), which reads
			// mc.toolCount -- and nothing in the runtime background
			// discovery path (RefreshToolsFromDiscovery) ever writes that
			// field; only the client's own GetCachedToolCount(ctx) does,
			// which has zero production callers. So mc.toolCount is always
			// 0 in production, and blindly taking liveState.ToolCount here
			// would reset every connected server's displayed tool count to
			// 0 on every reconcile. updateSnapshotFromEvent already guards
			// this exact case ("if len(status.Tools) == 0" before writing
			// ToolCount) -- mirror it: only take the live count when we
			// don't already have discovery-derived tools cached.
			if prevState != nil && len(prevState.Tools) > 0 {
				state.ToolCount = prevState.ToolCount
				state.Tools = prevState.Tools
			} else {
				state.ToolCount = liveState.ToolCount
			}

			// LastSeen feeds StateView's ConnectedAt ("when it connected",
			// a user-visible uptime field) -- it must not become "when we
			// last resynced". Only bump it on an actual disconnected->
			// connected transition (or if we've never seen this server
			// connected before); otherwise carry the old value forward.
			if liveState.Connected && (prevState == nil || !prevState.Connected) {
				state.LastSeen = time.Now()
			} else if prevState != nil {
				state.LastSeen = prevState.LastSeen
			}
		} else if prevState != nil {
			// Manager doesn't have this client yet -- fall back to whatever
			// we last had rather than resetting to zero-value.
			state.Connected = prevState.Connected
			state.ConnectionInfo = prevState.ConnectionInfo
			state.LastSeen = prevState.LastSeen
			state.ToolCount = prevState.ToolCount
			state.Tools = prevState.Tools
		}

		newSnapshot.Servers[srv.Name] = state

		// Only pay for the StateView clone (updateStateView) when something
		// a reader actually cares about changed, or this is a server we've
		// never written to StateView before.
		if stateViewNeedsUpdate(prevState, state) {
			s.updateStateView(srv.Name, state)
			corrected = append(corrected, srv.Name)
		}
	}

	s.snapshot.Store(newSnapshot)

	// Remove servers from stateview that are no longer in config
	currentView := s.stateView.Snapshot()
	for name := range currentView.Servers {
		if _, exists := newSnapshot.Servers[name]; !exists {
			s.stateView.RemoveServer(name)
		}
	}

	// Nudge listeners (-> Runtime.supervisorEventForwarder -> SSE
	// servers.changed) for anything this resync actually changed. The
	// frontend only refreshes the server list on mount or on that SSE
	// event, so a silent StateView correction with no accompanying event
	// would never reach it.
	for _, name := range corrected {
		s.emitEvent(Event{
			Type:       EventServerStateChanged,
			ServerName: name,
			Timestamp:  time.Now(),
			Payload:    map[string]interface{}{"source": "resync"},
		})
	}
}

// stateViewNeedsUpdate reports whether state differs from prev in a way
// that matters to StateView readers (status/connected/health/effective_status),
// used to skip the O(N) StateView clone in updateStateView when a resync
// found nothing new for this server. A nil prev (never written before)
// always needs the write.
func stateViewNeedsUpdate(prev, state *ServerState) bool {
	if prev == nil {
		return true
	}
	if prev.Connected != state.Connected || prev.ToolCount != state.ToolCount {
		return true
	}
	prevErr, newErr := "", ""
	if prev.ConnectionInfo != nil && prev.ConnectionInfo.LastError != nil {
		prevErr = prev.ConnectionInfo.LastError.Error()
	}
	if state.ConnectionInfo != nil && state.ConnectionInfo.LastError != nil {
		newErr = state.ConnectionInfo.LastError.Error()
	}
	if prevErr != newErr {
		return true
	}
	// A config change (rename/URL/enabled/etc.) also needs a fresh write
	// even if Connected hasn't flipped yet. Compare the actual fields that
	// matter -- NOT pointer identity. configSnapshot.Config.Servers entries
	// are stable pointers between config updates (configsvc.Service.Current()
	// returns the same *Snapshot until the next real Update()), so pointer
	// comparison happens to work in the steady state, but relying on that
	// is fragile: it would silently stop detecting anything (never mind
	// over- or under-firing) the moment configsvc started returning
	// per-read copies. Same field list configChanged already uses for the
	// same reason (deciding whether a change is actionable).
	return configFieldsDiffer(prev.Config, state.Config)
}

// configFieldsDiffer reports whether two server configs differ in a field
// that stateViewNeedsUpdate/configChanged care about. Mirrors
// Supervisor.configChanged's field list (kept as a free function so
// stateViewNeedsUpdate stays testable without a *Supervisor).
func configFieldsDiffer(a, b *config.ServerConfig) bool {
	if a == b {
		return false
	}
	if a == nil || b == nil {
		return true
	}
	return a.URL != b.URL ||
		a.Protocol != b.Protocol ||
		a.Command != b.Command ||
		a.Enabled != b.Enabled ||
		a.Quarantined != b.Quarantined
}

// connectionStateString normalizes types.ConnectionState to the single
// lowercase string StateView writes for status.State (D6 / A4): before this,
// updateStateView lowercased (line ~566, "error") but
// updateSnapshotFromEvent did not (line ~777, "Error"), so which casing a
// caller saw depended on which of the two write paths last ran. One helper,
// used by both.
func connectionStateString(ci *types.ConnectionInfo) string {
	if ci == nil {
		return ""
	}
	return strings.ToLower(ci.State.String())
}

// updateStateView updates the stateview with current server state.
func (s *Supervisor) updateStateView(name string, state *ServerState) {
	s.stateView.UpdateServer(name, func(status *stateview.ServerStatus) {
		status.Config = state.Config
		status.Enabled = state.Enabled
		status.Quarantined = state.Quarantined
		status.Connected = state.Connected
		status.ToolCount = state.ToolCount

		// Phase 7.1: Convert ToolMetadata to ToolInfo and cache in StateView
		if state.Tools != nil {
			status.Tools = make([]stateview.ToolInfo, len(state.Tools))
			for i, tool := range state.Tools {
				// Parse ParamsJSON into InputSchema
				var inputSchema map[string]interface{}
				if tool.ParamsJSON != "" {
					// ParamsJSON is already a JSON string, we'll store it as-is
					// The API endpoint will parse it if needed
					inputSchema = map[string]interface{}{
						"type":       "object",
						"properties": map[string]interface{}{}, // TODO: Parse ParamsJSON
					}
				}

				status.Tools[i] = stateview.ToolInfo{
					Name:        tool.Name,
					Description: tool.Description,
					InputSchema: inputSchema,
					Annotations: tool.Annotations,
				}
			}
		} else {
			status.Tools = nil
		}

		// Map connection state to string
		// Use detailed state from ConnectionInfo when available to avoid mislabeling disconnected servers as "connecting"
		if state.ConnectionInfo != nil {
			status.State = connectionStateString(state.ConnectionInfo)
		} else if state.Connected {
			status.State = "connected"
		} else if state.Enabled && !state.Quarantined {
			status.State = "connecting"
		} else if state.Enabled {
			status.State = "disconnected"
		} else {
			status.State = "idle"
		}

		// Update connection time if connected
		if state.Connected && !state.LastSeen.IsZero() {
			t := state.LastSeen
			status.ConnectedAt = &t
		}

		// CRITICAL: Clear error when connected, even if ConnectionInfo is unavailable
		// This ensures stale OAuth/connection errors don't persist after successful reconnection
		if state.Connected {
			status.LastError = ""
			status.LastErrorTime = nil
		}

		// Update connection info if available
		if state.ConnectionInfo != nil {
			// Extract LastError from ConnectionInfo and convert to string with limit
			if state.ConnectionInfo.LastError != nil {
				errorStr := state.ConnectionInfo.LastError.Error()
				// Limit error string to 500 characters to prevent UI hangs
				const maxErrorLen = 500
				if len(errorStr) > maxErrorLen {
					errorStr = errorStr[:maxErrorLen] + "... (truncated)"
				}
				status.LastError = errorStr

				// Set last error time if available
				if !state.ConnectionInfo.LastRetryTime.IsZero() {
					t := state.ConnectionInfo.LastRetryTime
					status.LastErrorTime = &t
				}
			}
			// Note: error already cleared above if connected=true
			// Only set error from ConnectionInfo if it has one

			// Copy retry count
			status.RetryCount = state.ConnectionInfo.RetryCount

			// Copy call-outcome bookkeeping (A2/A3): last_success_at /
			// last_auth_failure_at flow through exactly like RetryCount.
			if !state.ConnectionInfo.LastSuccessAt.IsZero() {
				t := state.ConnectionInfo.LastSuccessAt
				status.LastSuccessAt = &t
			}
			if !state.ConnectionInfo.LastAuthFailureAt.IsZero() {
				t := state.ConnectionInfo.LastAuthFailureAt
				status.LastAuthFailureAt = &t
			}

			// Store full connection info in metadata for debugging
			if status.Metadata == nil {
				status.Metadata = make(map[string]interface{})
			}
			status.Metadata["connection_info"] = state.ConnectionInfo
		}
	})
}

// SetOnServerConnectedCallback sets a callback to be invoked when a server connects.
// This allows for reactive tool discovery instead of relying on periodic polling.
func (s *Supervisor) SetOnServerConnectedCallback(callback func(serverName string)) {
	s.callbackMu.Lock()
	defer s.callbackMu.Unlock()
	s.onServerConnectedCallback = callback
}

// RefreshToolsFromDiscovery updates both the Supervisor snapshot and StateView with tools from background discovery.
// This is called after DiscoverAndIndexTools completes to populate the UI cache.
func (s *Supervisor) RefreshToolsFromDiscovery(tools []*config.ToolMetadata) error {
	if tools == nil {
		return nil
	}

	// Group tools by server name
	toolsByServer := make(map[string][]*config.ToolMetadata)
	for _, tool := range tools {
		toolsByServer[tool.ServerName] = append(toolsByServer[tool.ServerName], tool)
	}

	// Update Supervisor's snapshot first (source of truth for StateView)
	s.stateMu.Lock()
	currentSnapshot := s.snapshot.Load().(*ServerStateSnapshot)

	// Clone the snapshot
	newServers := make(map[string]*ServerState)
	for name, state := range currentSnapshot.Servers {
		// Shallow copy of ServerState
		newState := *state
		newServers[name] = &newState
	}

	// Update tool counts and tools for servers with discovered tools
	for serverName, serverTools := range toolsByServer {
		if state, exists := newServers[serverName]; exists {
			state.ToolCount = len(serverTools)
			state.Tools = serverTools
		}
	}

	newSnapshot := &ServerStateSnapshot{
		Servers:   newServers,
		Timestamp: time.Now(),
		Version:   currentSnapshot.Version + 1,
	}

	s.snapshot.Store(newSnapshot)
	s.version++
	s.stateMu.Unlock()

	// Update StateView for each server
	for serverName, serverTools := range toolsByServer {
		s.stateView.UpdateServer(serverName, func(status *stateview.ServerStatus) {
			// Defensive check: Only update if we have more or equal tools than currently shown
			// This prevents overwriting valid tools with stale data from delayed discoveries
			// Exception: Always update if current tools is 0 (initial population)
			if len(status.Tools) > 0 && len(serverTools) < len(status.Tools) {
				s.logger.Debug("StateView already has more tools, skipping update to prevent stale data",
					zap.String("server", serverName),
					zap.Int("current_tools", len(status.Tools)),
					zap.Int("new_tools", len(serverTools)))
				return
			}

			status.ToolCount = len(serverTools)
			status.Tools = make([]stateview.ToolInfo, len(serverTools))

			for i, tool := range serverTools {
				// Parse ParamsJSON into InputSchema
				var inputSchema map[string]interface{}
				if tool.ParamsJSON != "" {
					// ParamsJSON is already a JSON string
					inputSchema = map[string]interface{}{
						"type":       "object",
						"properties": map[string]interface{}{}, // TODO: Parse ParamsJSON
					}
				}

				status.Tools[i] = stateview.ToolInfo{
					Name:        tool.Name,
					Description: tool.Description,
					InputSchema: inputSchema,
					Annotations: tool.Annotations,
				}
			}
		})
	}

	s.logger.Debug("Refreshed tools in Supervisor snapshot and StateView from discovery",
		zap.Int("server_count", len(toolsByServer)),
		zap.Int("total_tools", len(tools)))

	return nil
}

// forwardUpstreamEvents forwards upstream events to supervisor listeners.
func (s *Supervisor) forwardUpstreamEvents(upstreamEvents <-chan Event) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return

		case event, ok := <-upstreamEvents:
			if !ok {
				return
			}

			// Forward to supervisor listeners
			s.emitEvent(event)

			// Update snapshot on state changes
			if event.Type == EventServerStateChanged || event.Type == EventServerConnected || event.Type == EventServerDisconnected {
				s.updateSnapshotFromEvent(event)
			}
		}
	}
}

// updateSnapshotFromEvent updates the snapshot based on an upstream event.
func (s *Supervisor) updateSnapshotFromEvent(event Event) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	current := s.CurrentSnapshot()
	if state, ok := current.Servers[event.ServerName]; ok {
		// Update connection status
		if connected, ok := event.Payload["connected"].(bool); ok {
			state.Connected = connected
			state.LastSeen = event.Timestamp

			// Phase 7.1 FIX: Don't fetch tools on connect events - let background indexing handle it
			// Just update the cached tool count and connection info from the client (non-blocking)
			var toolCount int
			var connInfo *types.ConnectionInfo
			if actualState, err := s.upstream.GetServerState(event.ServerName); err == nil {
				toolCount = actualState.ToolCount
				// Update ConnectionInfo for error propagation to UI
				state.ConnectionInfo = actualState.ConnectionInfo
				connInfo = actualState.ConnectionInfo
			} else {
				s.logger.Warn("Failed to get server state for tool count",
					zap.String("server", event.ServerName),
					zap.Error(err))
			}

			// Update stateview
			s.stateView.UpdateServer(event.ServerName, func(status *stateview.ServerStatus) {
				status.Connected = connected

				// Use detailed state from ConnectionInfo if available
				if connInfo != nil && connInfo.State != types.StateDisconnected {
					status.State = connectionStateString(connInfo)
				} else if connected {
					status.State = "connected"
				} else {
					status.State = "disconnected"
				}

				if connected {
					t := event.Timestamp
					status.ConnectedAt = &t
					// Don't populate Tools here - background indexing will handle it
					// Only update the count if tools haven't been discovered yet
					// This prevents overwriting tool data from background discovery
					if len(status.Tools) == 0 {
						status.ToolCount = toolCount
					}
					// If tools are already populated, keep the existing count

					// CRITICAL: Clear error when connected, even if connInfo is unavailable
					// This ensures stale OAuth/connection errors don't persist after successful reconnection
					status.LastError = ""
					status.LastErrorTime = nil
				} else {
					t := event.Timestamp
					status.DisconnectedAt = &t
					status.Tools = nil // Clear tools on disconnect
					status.ToolCount = 0
				}

				// Update ConnectionInfo for immediate error propagation to UI
				if connInfo != nil {
					if connInfo.LastError != nil {
						errorStr := connInfo.LastError.Error()
						const maxErrorLen = 500
						if len(errorStr) > maxErrorLen {
							errorStr = errorStr[:maxErrorLen] + "... (truncated)"
						}
						status.LastError = errorStr

						if !connInfo.LastRetryTime.IsZero() {
							t := connInfo.LastRetryTime
							status.LastErrorTime = &t
						}
					}
					// Note: We already cleared error above when connected=true
					// Only set error from connInfo if it has one
					status.RetryCount = connInfo.RetryCount

					// Copy call-outcome bookkeeping (A2/A3), same as the
					// other writer above.
					if !connInfo.LastSuccessAt.IsZero() {
						t := connInfo.LastSuccessAt
						status.LastSuccessAt = &t
					}
					if !connInfo.LastAuthFailureAt.IsZero() {
						t := connInfo.LastAuthFailureAt
						status.LastAuthFailureAt = &t
					}
				}
			})

			// Trigger reactive tool discovery when server connects
			if connected {
				s.callbackMu.RLock()
				callback := s.onServerConnectedCallback
				s.callbackMu.RUnlock()

				if callback != nil {
					// Run callback asynchronously to avoid blocking supervisor
					go callback(event.ServerName)
				}
			}
		}
	}
}

// CurrentSnapshot returns the current state snapshot (lock-free read).
func (s *Supervisor) CurrentSnapshot() *ServerStateSnapshot {
	return s.snapshot.Load().(*ServerStateSnapshot)
}

// StateView returns the read-only state view (Phase 4).
// This provides a lock-free view of server statuses for API consumers.
func (s *Supervisor) StateView() *stateview.View {
	return s.stateView
}

// Subscribe returns a channel that receives supervisor events.
func (s *Supervisor) Subscribe() <-chan Event {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	ch := make(chan Event, 500) // F3: bumped from 200, defense-in-depth headroom (see actor_pool.go Subscribe)
	s.listeners = append(s.listeners, ch)
	return ch
}

// Unsubscribe removes a subscriber.
func (s *Supervisor) Unsubscribe(ch <-chan Event) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	for i, listener := range s.listeners {
		if listener == ch {
			s.listeners = append(s.listeners[:i], s.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// emitEvent sends an event to all subscribers.
func (s *Supervisor) emitEvent(event Event) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	for _, ch := range s.listeners {
		select {
		case ch <- event:
		default:
			s.logger.Warn("Supervisor event channel full, dropping event",
				zap.String("event_type", string(event.Type)))
		}
	}
}

// Stop gracefully stops the supervisor.
func (s *Supervisor) Stop() {
	s.logger.Info("Stopping supervisor")
	s.cancel()
	s.wg.Wait()

	// Close upstream adapter
	s.upstream.Close()

	// Close event channels
	s.eventMu.Lock()
	for _, ch := range s.listeners {
		close(ch)
	}
	s.listeners = nil
	s.eventMu.Unlock()

	s.logger.Info("Supervisor stopped")
}

// RequestInspectionExemption grants temporary connection permission for a quarantined server.
// This allows security inspection to temporarily connect to quarantined servers.
// Triggers immediate reconciliation to connect the server.
func (s *Supervisor) RequestInspectionExemption(serverName string, duration time.Duration) error {
	s.inspectionExemptionsMu.Lock()
	expiryTime := time.Now().Add(duration)
	s.inspectionExemptions[serverName] = expiryTime
	s.inspectionExemptionsMu.Unlock()

	s.logger.Warn("⚠️ Temporary connection exemption granted for quarantined server inspection",
		zap.String("server", serverName),
		zap.Duration("duration", duration),
		zap.Time("expires_at", expiryTime))

	// Trigger immediate reconciliation to connect the server
	currentConfig := s.configSvc.Current()
	if err := s.reconcile(currentConfig); err != nil {
		s.logger.Error("Failed to trigger reconciliation after exemption grant",
			zap.String("server", serverName),
			zap.Error(err))
		return fmt.Errorf("failed to trigger reconciliation: %w", err)
	}

	return nil
}

// RevokeInspectionExemption revokes the temporary connection permission and triggers disconnection.
func (s *Supervisor) RevokeInspectionExemption(serverName string) {
	s.inspectionExemptionsMu.Lock()
	_, exists := s.inspectionExemptions[serverName]
	if exists {
		delete(s.inspectionExemptions, serverName)
	}
	s.inspectionExemptionsMu.Unlock()

	if exists {
		s.logger.Warn("⚠️ Inspection exemption revoked for quarantined server",
			zap.String("server", serverName))

		// Trigger immediate reconciliation to disconnect the server
		currentConfig := s.configSvc.Current()
		if err := s.reconcile(currentConfig); err != nil {
			s.logger.Error("Failed to trigger reconciliation after exemption revocation",
				zap.String("server", serverName),
				zap.Error(err))
		}
	}
}

// IsInspectionExempted checks if a server has an active inspection exemption.
// Automatically cleans up expired exemptions.
func (s *Supervisor) IsInspectionExempted(serverName string) bool {
	s.inspectionExemptionsMu.Lock()
	defer s.inspectionExemptionsMu.Unlock()

	expiryTime, exists := s.inspectionExemptions[serverName]
	if !exists {
		return false
	}

	// Check if exemption has expired
	if time.Now().After(expiryTime) {
		delete(s.inspectionExemptions, serverName)
		s.logger.Warn("⚠️ Inspection exemption expired, forcing disconnect",
			zap.String("server", serverName))
		return false
	}

	return true
}

// ===== Circuit Breaker for Inspection Failures (Issue #105) =====

const (
	maxInspectionFailures = 3                // Max consecutive failures before cooldown
	inspectionCooldown    = 5 * time.Minute  // Cooldown duration after max failures
	failureResetTimeout   = 10 * time.Minute // Reset counter if no failures for this long
)

// CanInspect checks if inspection is allowed for a server (circuit breaker)
// Returns (allowed bool, reason string, cooldownRemaining time.Duration)
func (s *Supervisor) CanInspect(serverName string) (bool, string, time.Duration) {
	s.inspectionFailuresMu.RLock()
	defer s.inspectionFailuresMu.RUnlock()

	info, exists := s.inspectionFailures[serverName]
	if !exists {
		// No failure history - allow inspection
		return true, "", 0
	}

	now := time.Now()

	// Check if cooldown is active
	if now.Before(info.cooldownUntil) {
		remaining := info.cooldownUntil.Sub(now)
		reason := fmt.Sprintf("Server '%s' has failed inspection %d times. Circuit breaker active - please wait %v before retrying. This prevents cascading failures with unstable servers (see issue #105).",
			serverName, info.consecutiveFailures, remaining.Round(time.Second))
		return false, reason, remaining
	}

	// Check if failures should be reset (no failures for failureResetTimeout)
	if now.Sub(info.lastFailureTime) > failureResetTimeout {
		// Failures are old - will be reset on next inspection
		return true, "", 0
	}

	// Within failure window but not in cooldown
	return true, "", 0
}

// RecordInspectionFailure records an inspection failure for circuit breaker
func (s *Supervisor) RecordInspectionFailure(serverName string) {
	s.inspectionFailuresMu.Lock()
	defer s.inspectionFailuresMu.Unlock()

	now := time.Now()

	info, exists := s.inspectionFailures[serverName]
	if !exists {
		info = &inspectionFailureInfo{}
		s.inspectionFailures[serverName] = info
	}

	// Reset counter if last failure was too long ago
	if now.Sub(info.lastFailureTime) > failureResetTimeout {
		info.consecutiveFailures = 0
	}

	info.consecutiveFailures++
	info.lastFailureTime = now

	s.logger.Warn("Inspection failure recorded",
		zap.String("server", serverName),
		zap.Int("consecutive_failures", info.consecutiveFailures),
		zap.Int("max_before_cooldown", maxInspectionFailures))

	// Activate cooldown if max failures reached
	if info.consecutiveFailures >= maxInspectionFailures {
		info.cooldownUntil = now.Add(inspectionCooldown)
		s.logger.Error("⚠️ Inspection circuit breaker activated - too many failures",
			zap.String("server", serverName),
			zap.Int("failures", info.consecutiveFailures),
			zap.Duration("cooldown", inspectionCooldown),
			zap.Time("cooldown_until", info.cooldownUntil),
			zap.String("issue", "#105 - preventing cascading failures"))
	}
}

// RecordInspectionSuccess records a successful inspection, resetting failure counter
func (s *Supervisor) RecordInspectionSuccess(serverName string) {
	s.inspectionFailuresMu.Lock()
	defer s.inspectionFailuresMu.Unlock()

	info, exists := s.inspectionFailures[serverName]
	if !exists {
		return
	}

	if info.consecutiveFailures > 0 {
		s.logger.Info("Inspection succeeded - resetting failure counter",
			zap.String("server", serverName),
			zap.Int("previous_failures", info.consecutiveFailures))
	}

	// Reset failure counter
	delete(s.inspectionFailures, serverName)
}

// GetInspectionStats returns inspection failure statistics for a server
func (s *Supervisor) GetInspectionStats(serverName string) (failures int, inCooldown bool, cooldownRemaining time.Duration) {
	s.inspectionFailuresMu.RLock()
	defer s.inspectionFailuresMu.RUnlock()

	info, exists := s.inspectionFailures[serverName]
	if !exists {
		return 0, false, 0
	}

	now := time.Now()
	if now.Before(info.cooldownUntil) {
		return info.consecutiveFailures, true, info.cooldownUntil.Sub(now)
	}

	return info.consecutiveFailures, false, 0
}
