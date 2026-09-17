// Package oauth provides OAuth 2.1 authentication support for MCP servers.
// This file implements OAuth flow coordination to prevent race conditions.
package oauth

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Default timeouts for OAuth flow coordination.
const (
	// DefaultFlowTimeout is the maximum time to wait for an OAuth flow to complete.
	DefaultFlowTimeout = 5 * time.Minute
	// StaleFlowTimeout is the time after which a flow is considered stale and can be cleared.
	StaleFlowTimeout = 10 * time.Minute
)

// ErrFlowTimeout indicates that waiting for an OAuth flow timed out.
var ErrFlowTimeout = errors.New("timeout waiting for OAuth flow to complete")

// ErrFlowInProgress indicates an OAuth flow is already in progress for the server.
var ErrFlowInProgress = errors.New("OAuth flow already in progress")

// flowWaiter represents a goroutine waiting for an OAuth flow to complete.
type flowWaiter struct {
	done   chan struct{}
	result error
}

// OAuthFlowCoordinator coordinates OAuth flows to ensure only one flow runs per server.
// This prevents race conditions where multiple reconnection attempts trigger concurrent OAuth flows.
type OAuthFlowCoordinator struct {
	// activeFlows tracks the active OAuth flow context for each server.
	activeFlows map[string]*OAuthFlowContext
	// flowLocks provides per-server mutexes to serialize OAuth operations.
	flowLocks map[string]*sync.Mutex
	// waiters tracks goroutines waiting for a flow to complete.
	waiters map[string][]*flowWaiter
	// expiryTimers fires expireFlow for a server's active flow at flowTimeout,
	// so an abandoned flow is reaped exactly when it goes stale instead of waiting
	// on a periodic sweep (or on the next StartFlow call, which may never come).
	expiryTimers map[string]*time.Timer
	// flowTimeout is the staleness threshold used by StartFlow/IsFlowActive/the
	// expiry timer. Defaults to StaleFlowTimeout; overridable in tests so the
	// real time.AfterFunc path can be exercised without a 10-minute wait.
	flowTimeout time.Duration
	// mu protects all map operations.
	mu sync.RWMutex
	// logger for coordinator operations.
	logger *zap.Logger
}

// globalCoordinator is the singleton OAuth flow coordinator.
var globalCoordinator *OAuthFlowCoordinator
var coordinatorOnce sync.Once

// GetGlobalCoordinator returns the global OAuth flow coordinator instance.
func GetGlobalCoordinator() *OAuthFlowCoordinator {
	coordinatorOnce.Do(func() {
		globalCoordinator = NewOAuthFlowCoordinator()
	})
	return globalCoordinator
}

// NewOAuthFlowCoordinator creates a new OAuth flow coordinator.
func NewOAuthFlowCoordinator() *OAuthFlowCoordinator {
	return &OAuthFlowCoordinator{
		activeFlows:  make(map[string]*OAuthFlowContext),
		flowLocks:    make(map[string]*sync.Mutex),
		waiters:      make(map[string][]*flowWaiter),
		expiryTimers: make(map[string]*time.Timer),
		flowTimeout:  StaleFlowTimeout,
		logger:       zap.L().Named("oauth-coordinator"),
	}
}

// stopExpiryTimerLocked stops and removes the expiry timer for serverName, if any.
// Callers must hold c.mu.
func (c *OAuthFlowCoordinator) stopExpiryTimerLocked(serverName string) {
	if t, exists := c.expiryTimers[serverName]; exists {
		t.Stop()
		delete(c.expiryTimers, serverName)
	}
}

// getOrCreateLock returns the mutex for the given server, creating one if needed.
func (c *OAuthFlowCoordinator) getOrCreateLock(serverName string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()

	if lock, exists := c.flowLocks[serverName]; exists {
		return lock
	}

	lock := &sync.Mutex{}
	c.flowLocks[serverName] = lock
	return lock
}

// StartFlow starts a new OAuth flow for the given server.
// If a flow is already in progress, returns the existing flow context and ErrFlowInProgress.
// The caller should use WaitForFlow() instead if they want to wait for the existing flow.
func (c *OAuthFlowCoordinator) StartFlow(serverName string) (*OAuthFlowContext, error) {
	lock := c.getOrCreateLock(serverName)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if a flow is already active
	if existingFlow, exists := c.activeFlows[serverName]; exists {
		// Check if the flow is stale. This duplicates the expiry timer below as
		// a deliberate backstop for the scheduling-jitter window between a flow
		// going stale and its own time.AfterFunc actually firing — do not delete
		// this as "now-redundant dead code" without keeping some such backstop.
		if time.Since(existingFlow.StartTime) > c.flowTimeout {
			c.logger.Warn("Clearing stale OAuth flow",
				zap.String("server", serverName),
				zap.String("correlation_id", existingFlow.CorrelationID),
				zap.Duration("age", time.Since(existingFlow.StartTime)),
			)
			// Clear stale flow and proceed. Notify its waiters with a timeout
			// error first (mirroring expireFlow) so a goroutine still parked in
			// WaitForFlow for THIS flow isn't later resolved with the new flow's
			// unrelated result.
			staleWaiters := c.waiters[serverName]
			delete(c.activeFlows, serverName)
			delete(c.waiters, serverName)
			c.stopExpiryTimerLocked(serverName)
			for _, waiter := range staleWaiters {
				waiter.result = ErrFlowTimeout
				close(waiter.done)
			}
		} else {
			c.logger.Info("OAuth flow already in progress",
				zap.String("server", serverName),
				zap.String("existing_correlation_id", existingFlow.CorrelationID),
				zap.String("state", existingFlow.State.String()),
			)
			return existingFlow, ErrFlowInProgress
		}
	}

	// Create new flow context
	flowCtx := NewOAuthFlowContext(serverName)
	c.activeFlows[serverName] = flowCtx

	// Schedule this flow's own expiry: if nothing ends it (EndFlow) or replaces
	// it (a later StartFlow) before flowTimeout elapses, reap it exactly then
	// rather than relying on a periodic sweep or the next StartFlow call.
	correlationID := flowCtx.CorrelationID
	c.stopExpiryTimerLocked(serverName) // defensive; should already be clear
	c.expiryTimers[serverName] = time.AfterFunc(c.flowTimeout, func() {
		c.expireFlow(serverName, correlationID)
	})

	c.logger.Info("Started new OAuth flow",
		zap.String("server", serverName),
		zap.String("correlation_id", flowCtx.CorrelationID),
	)

	return flowCtx, nil
}

// EndFlow marks an OAuth flow as completed (success or failure).
// This notifies any waiting goroutines and cleans up the flow state.
// correlationID must match the flow's own CorrelationID (from the
// *OAuthFlowContext StartFlow returned); if the active flow for serverName is
// a *different* flow (StartFlow's stale-clear branch or expireFlow already
// superseded this one), EndFlow is a no-op — mirroring expireFlow's guard —
// so a slow, superseded caller can never clobber a newer flow's state,
// waiters, or expiry timer out from under it.
func (c *OAuthFlowCoordinator) EndFlow(serverName, correlationID string, success bool, err error) {
	lock := c.getOrCreateLock(serverName)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	flowCtx, exists := c.activeFlows[serverName]
	if exists && flowCtx.CorrelationID != correlationID {
		c.logger.Warn("EndFlow called for a superseded flow; ignoring",
			zap.String("server", serverName),
			zap.String("ending_correlation_id", correlationID),
			zap.String("active_correlation_id", flowCtx.CorrelationID),
		)
		c.mu.Unlock()
		return
	}

	waiters := c.waiters[serverName]

	// Update flow state
	if flowCtx != nil {
		if success {
			flowCtx.State = FlowCompleted
		} else {
			flowCtx.State = FlowFailed
		}

		LogOAuthFlowEnd(c.logger, serverName, flowCtx.CorrelationID, success, flowCtx.Duration())
	}

	// Clean up
	delete(c.activeFlows, serverName)
	delete(c.waiters, serverName)
	c.stopExpiryTimerLocked(serverName)
	c.mu.Unlock()

	// Notify all waiters
	for _, waiter := range waiters {
		waiter.result = err
		close(waiter.done)
	}
}

// IsFlowActive checks if an OAuth flow is currently active for the given server.
func (c *OAuthFlowCoordinator) IsFlowActive(serverName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	flow, exists := c.activeFlows[serverName]
	if !exists {
		return false
	}

	// Check if the flow is stale. Deliberate backstop, same rationale as the
	// duplicate check in StartFlow above — do not remove without keeping one.
	if time.Since(flow.StartTime) > c.flowTimeout {
		return false
	}

	return true
}

// GetActiveFlow returns the active OAuth flow context for the given server, if any.
func (c *OAuthFlowCoordinator) GetActiveFlow(serverName string) *OAuthFlowContext {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.activeFlows[serverName]
}

// WaitForFlow waits for an active OAuth flow to complete.
// Returns nil if no flow is active (caller should start one).
// Returns ErrFlowTimeout if the wait times out.
// Returns the flow's error if it failed.
func (c *OAuthFlowCoordinator) WaitForFlow(ctx context.Context, serverName string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = DefaultFlowTimeout
	}

	c.mu.Lock()
	flow, exists := c.activeFlows[serverName]
	if !exists {
		c.mu.Unlock()
		return nil // No flow to wait for
	}

	// Create a waiter
	waiter := &flowWaiter{
		done: make(chan struct{}),
	}
	c.waiters[serverName] = append(c.waiters[serverName], waiter)
	c.mu.Unlock()

	c.logger.Info("Waiting for OAuth flow to complete",
		zap.String("server", serverName),
		zap.String("flow_correlation_id", flow.CorrelationID),
		zap.Duration("timeout", timeout),
	)

	// Wait for flow completion or timeout
	select {
	case <-waiter.done:
		return waiter.result
	case <-time.After(timeout):
		c.logger.Warn("Timeout waiting for OAuth flow",
			zap.String("server", serverName),
			zap.Duration("timeout", timeout),
		)
		return ErrFlowTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UpdateFlowState updates the state of an active OAuth flow.
func (c *OAuthFlowCoordinator) UpdateFlowState(serverName string, state OAuthFlowState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if flow, exists := c.activeFlows[serverName]; exists {
		flow.State = state
		c.logger.Debug("Updated OAuth flow state",
			zap.String("server", serverName),
			zap.String("correlation_id", flow.CorrelationID),
			zap.String("new_state", state.String()),
		)
	}
}

// expireFlow reaps the active flow for serverName if it is still the one
// identified by correlationID (i.e. no EndFlow and no newer StartFlow beat the
// timer to it). Scheduled by StartFlow via time.AfterFunc(c.flowTimeout, ...)
// so an abandoned flow is cleaned up exactly at its expiry instead of via a
// periodic sweep — StartFlow's own inline staleness check already gave every
// flow a hard deadline, so a one-shot timer per flow just enforces that
// deadline proactively rather than lazily on the next StartFlow call (which,
// for a server nobody retries, might never come).
//
// Lock ordering: the per-server flowLock is always acquired before c.mu, and
// held for this call's whole body — matching StartFlow/EndFlow, which do the
// same — so there is no deadlock risk between these three methods.
func (c *OAuthFlowCoordinator) expireFlow(serverName, correlationID string) {
	lock := c.getOrCreateLock(serverName)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	flow, exists := c.activeFlows[serverName]
	if !exists || flow.CorrelationID != correlationID {
		// Already ended (EndFlow) or superseded by a newer flow; nothing to do.
		c.mu.Unlock()
		return
	}

	c.logger.Warn("OAuth flow expired without completion, cleaning up",
		zap.String("server", serverName),
		zap.String("correlation_id", correlationID),
		zap.Duration("age", time.Since(flow.StartTime)),
	)

	waiters := c.waiters[serverName]
	delete(c.activeFlows, serverName)
	delete(c.waiters, serverName)
	// Stop() is safe (and a correct no-op) even though this timer is the one
	// currently invoking us — it only prevents a future fire, which there
	// won't be since AfterFunc timers are one-shot. Using the shared helper
	// here (instead of a bare delete) also makes direct expireFlow calls, like
	// the ones in tests, behave the same as the real timer-fired path.
	c.stopExpiryTimerLocked(serverName)
	c.mu.Unlock()

	// Notify any waiters with timeout error
	for _, waiter := range waiters {
		waiter.result = ErrFlowTimeout
		close(waiter.done)
	}
}
