package oauth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthFlowCoordinator_StartFlow(t *testing.T) {
	t.Run("first flow starts successfully", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()
		flowCtx, err := coordinator.StartFlow("test-server")

		require.NoError(t, err)
		require.NotNil(t, flowCtx)
		assert.Equal(t, "test-server", flowCtx.ServerName)
		assert.NotEmpty(t, flowCtx.CorrelationID)
		assert.Equal(t, FlowInitiated, flowCtx.State)

		// Clean up
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)
	})

	t.Run("second flow returns ErrFlowInProgress", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		// Start first flow
		flowCtx1, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		// Try to start second flow - should fail
		flowCtx2, err := coordinator.StartFlow("test-server")
		assert.Equal(t, ErrFlowInProgress, err)
		assert.Equal(t, flowCtx1, flowCtx2) // Returns existing flow context

		// Clean up
		coordinator.EndFlow("test-server", flowCtx1.CorrelationID, true, nil)
	})

	t.Run("different servers can have concurrent flows", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx1, err := coordinator.StartFlow("server-1")
		require.NoError(t, err)

		flowCtx2, err := coordinator.StartFlow("server-2")
		require.NoError(t, err)

		assert.NotEqual(t, flowCtx1.CorrelationID, flowCtx2.CorrelationID)

		// Clean up
		coordinator.EndFlow("server-1", flowCtx1.CorrelationID, true, nil)
		coordinator.EndFlow("server-2", flowCtx2.CorrelationID, true, nil)
	})
}

func TestOAuthFlowCoordinator_EndFlow(t *testing.T) {
	t.Run("end flow success clears active flow", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		assert.True(t, coordinator.IsFlowActive("test-server"))

		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)
		assert.False(t, coordinator.IsFlowActive("test-server"))
	})

	t.Run("end flow failure clears active flow", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		coordinator.EndFlow("test-server", flowCtx.CorrelationID, false, assert.AnError)
		assert.False(t, coordinator.IsFlowActive("test-server"))
	})

	t.Run("can start new flow after ending previous", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx1, _ := coordinator.StartFlow("test-server")
		coordinator.EndFlow("test-server", flowCtx1.CorrelationID, true, nil)

		flowCtx2, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		assert.NotEqual(t, flowCtx1.CorrelationID, flowCtx2.CorrelationID)

		coordinator.EndFlow("test-server", flowCtx2.CorrelationID, true, nil)
	})

	t.Run("EndFlow with a stale correlationID does not clobber a newer flow", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		oldFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		// Simulate the old flow's owner finishing late: force it stale so the
		// next StartFlow supersedes it (mirrors the AfterFunc timer's job, but
		// exercised via the inline stale-clear path instead).
		coordinator.mu.Lock()
		oldFlow.StartTime = time.Now().Add(-2 * StaleFlowTimeout)
		coordinator.mu.Unlock()

		newFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		require.NotEqual(t, oldFlow.CorrelationID, newFlow.CorrelationID)

		// The old owner's EndFlow call arrives after supersession. It must be
		// a no-op: the new flow stays active and untouched.
		coordinator.EndFlow("test-server", oldFlow.CorrelationID, true, nil)

		assert.True(t, coordinator.IsFlowActive("test-server"),
			"EndFlow must not clobber a newer flow by correlation-ID mismatch")
		active := coordinator.GetActiveFlow("test-server")
		require.NotNil(t, active)
		assert.Equal(t, newFlow.CorrelationID, active.CorrelationID)

		coordinator.EndFlow("test-server", newFlow.CorrelationID, true, nil)
	})
}

func TestOAuthFlowCoordinator_WaitForFlow(t *testing.T) {
	t.Run("returns nil when no flow active", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		err := coordinator.WaitForFlow(context.Background(), "test-server", time.Second)
		assert.NoError(t, err)
	})

	t.Run("waits for flow completion", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		// Start waiter in goroutine
		var waitErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			waitErr = coordinator.WaitForFlow(context.Background(), "test-server", 5*time.Second)
		}()

		// End flow after short delay
		time.Sleep(100 * time.Millisecond)
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)

		wg.Wait()
		assert.NoError(t, waitErr)
	})

	t.Run("returns timeout error when flow takes too long", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		err = coordinator.WaitForFlow(context.Background(), "test-server", 100*time.Millisecond)
		assert.Equal(t, ErrFlowTimeout, err)

		// Clean up
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, false, nil)
	})

	t.Run("returns context error when context cancelled", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err = coordinator.WaitForFlow(ctx, "test-server", 5*time.Second)
		assert.ErrorIs(t, err, context.Canceled)

		// Clean up
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, false, nil)
	})
}

func TestOAuthFlowCoordinator_IsFlowActive(t *testing.T) {
	coordinator := NewOAuthFlowCoordinator()

	assert.False(t, coordinator.IsFlowActive("test-server"))

	flowCtx, _ := coordinator.StartFlow("test-server")
	assert.True(t, coordinator.IsFlowActive("test-server"))

	coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)
	assert.False(t, coordinator.IsFlowActive("test-server"))
}

func TestOAuthFlowCoordinator_ConcurrentAccess(t *testing.T) {
	coordinator := NewOAuthFlowCoordinator()
	serverName := "concurrent-test"

	var wg sync.WaitGroup
	successCount := 0
	var winningFlow *OAuthFlowContext
	var mu sync.Mutex

	// Try to start 10 concurrent flows - only one should succeed
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			flowCtx, err := coordinator.StartFlow(serverName)
			if err == nil {
				mu.Lock()
				successCount++
				winningFlow = flowCtx
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, successCount, "Only one flow should have started")
	assert.True(t, coordinator.IsFlowActive(serverName))
	require.NotNil(t, winningFlow)

	coordinator.EndFlow(serverName, winningFlow.CorrelationID, true, nil)
}

func TestOAuthFlowCoordinator_MultipleWaiters(t *testing.T) {
	coordinator := NewOAuthFlowCoordinator()

	flowCtx, err := coordinator.StartFlow("test-server")
	require.NoError(t, err)

	var wg sync.WaitGroup
	waitResults := make([]error, 5)

	// Start multiple waiters
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			waitResults[idx] = coordinator.WaitForFlow(context.Background(), "test-server", 5*time.Second)
		}(i)
	}

	// Give waiters time to register
	time.Sleep(100 * time.Millisecond)

	// End flow - all waiters should be notified
	coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)

	wg.Wait()

	// All waiters should succeed
	for i, err := range waitResults {
		assert.NoError(t, err, "Waiter %d should not have error", i)
	}
}

func TestGetGlobalCoordinator(t *testing.T) {
	coord1 := GetGlobalCoordinator()
	coord2 := GetGlobalCoordinator()

	assert.Same(t, coord1, coord2, "GetGlobalCoordinator should return singleton")
}

// TestOAuthFlowCoordinator_ExpiryTimer covers the per-flow expiry timer that
// replaced the old, never-invoked CleanupStaleFlows sweep: StartFlow schedules
// a one-shot timer via time.AfterFunc, and expireFlow (which the timer calls)
// must only reap the flow it was scheduled for.
func TestOAuthFlowCoordinator_ExpiryTimer(t *testing.T) {
	t.Run("StartFlow schedules a timer for the server", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		coordinator.mu.RLock()
		_, hasTimer := coordinator.expiryTimers["test-server"]
		coordinator.mu.RUnlock()
		assert.True(t, hasTimer, "StartFlow should schedule an expiry timer")

		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)
	})

	t.Run("EndFlow stops the timer so it never fires", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)

		coordinator.mu.RLock()
		_, hasTimer := coordinator.expiryTimers["test-server"]
		coordinator.mu.RUnlock()
		assert.False(t, hasTimer, "EndFlow should remove the expiry timer")
	})

	t.Run("EndFlow with a stale correlationID leaves a superseding flow's timer running", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		oldFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		coordinator.mu.Lock()
		oldFlow.StartTime = time.Now().Add(-2 * StaleFlowTimeout)
		coordinator.mu.Unlock()

		newFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		// Late EndFlow for the superseded old flow must not touch the new
		// flow's timer.
		coordinator.EndFlow("test-server", oldFlow.CorrelationID, true, nil)

		coordinator.mu.RLock()
		_, hasTimer := coordinator.expiryTimers["test-server"]
		coordinator.mu.RUnlock()
		assert.True(t, hasTimer, "a stale EndFlow must not stop the current flow's timer")

		coordinator.EndFlow("test-server", newFlow.CorrelationID, true, nil)
	})

	t.Run("a real timer (short flowTimeout) reaps an abandoned flow without any direct expireFlow call", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()
		coordinator.flowTimeout = 20 * time.Millisecond

		_, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		require.True(t, coordinator.IsFlowActive("test-server"))

		// Exercise the actual time.AfterFunc wiring StartFlow installs — no
		// call to expireFlow here, unlike the other subtests in this group.
		// Poll activeFlows/expiryTimers directly rather than IsFlowActive:
		// IsFlowActive's own staleness check would report "inactive" purely
		// from elapsed time, racing ahead of expireFlow's callback actually
		// running and clearing state — this waits for the real effect.
		require.Eventually(t, func() bool {
			coordinator.mu.RLock()
			defer coordinator.mu.RUnlock()
			_, activeExists := coordinator.activeFlows["test-server"]
			_, hasTimer := coordinator.expiryTimers["test-server"]
			return !activeExists && !hasTimer
		}, 2*time.Second, 5*time.Millisecond, "the real expiry timer should have reaped the flow")
	})

	t.Run("StartFlow's inline stale-clear notifies the old flow's waiters instead of leaking them", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		oldFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)

		waitErrCh := make(chan error, 1)
		go func() {
			waitErrCh <- coordinator.WaitForFlow(context.Background(), "test-server", 2*time.Second)
		}()
		require.Eventually(t, func() bool {
			coordinator.mu.RLock()
			defer coordinator.mu.RUnlock()
			return len(coordinator.waiters["test-server"]) == 1
		}, 2*time.Second, 5*time.Millisecond, "waiter should register")

		// Force the flow stale so the next StartFlow supersedes it via the
		// inline stale-clear branch (not expireFlow/the timer).
		coordinator.mu.Lock()
		oldFlow.StartTime = time.Now().Add(-2 * StaleFlowTimeout)
		coordinator.mu.Unlock()

		newFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		require.NotEqual(t, oldFlow.CorrelationID, newFlow.CorrelationID)

		// The waiter registered against the OLD flow must get ErrFlowTimeout
		// now, not the new flow's eventual EndFlow result later.
		assert.Equal(t, ErrFlowTimeout, <-waitErrCh)

		coordinator.EndFlow("test-server", newFlow.CorrelationID, true, nil)
	})

	t.Run("expireFlow reaps an abandoned flow and notifies waiters", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		assert.True(t, coordinator.IsFlowActive("test-server"))

		waitErrCh := make(chan error, 1)
		go func() {
			waitErrCh <- coordinator.WaitForFlow(context.Background(), "test-server", 2*time.Second)
		}()
		require.Eventually(t, func() bool {
			coordinator.mu.RLock()
			defer coordinator.mu.RUnlock()
			return len(coordinator.waiters["test-server"]) == 1
		}, 2*time.Second, 5*time.Millisecond, "waiter should register")

		// Simulate the timer firing (StaleFlowTimeout is 10m; call directly
		// rather than waiting for it in a unit test).
		coordinator.expireFlow("test-server", flowCtx.CorrelationID)

		assert.False(t, coordinator.IsFlowActive("test-server"))
		assert.Equal(t, ErrFlowTimeout, <-waitErrCh)
	})

	t.Run("expireFlow is a no-op if the flow already ended", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		flowCtx, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		coordinator.EndFlow("test-server", flowCtx.CorrelationID, true, nil)

		// A stale timer firing after EndFlow already ran must not panic or
		// touch state — nothing is active to reap.
		coordinator.expireFlow("test-server", flowCtx.CorrelationID)
		assert.False(t, coordinator.IsFlowActive("test-server"))
	})

	t.Run("expireFlow does not clobber a newer flow for the same server", func(t *testing.T) {
		coordinator := NewOAuthFlowCoordinator()

		oldFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		coordinator.EndFlow("test-server", oldFlow.CorrelationID, true, nil)

		newFlow, err := coordinator.StartFlow("test-server")
		require.NoError(t, err)
		require.NotEqual(t, oldFlow.CorrelationID, newFlow.CorrelationID)

		// A stale timer for the OLD flow firing late must not delete the new one.
		coordinator.expireFlow("test-server", oldFlow.CorrelationID)
		assert.True(t, coordinator.IsFlowActive("test-server"),
			"expireFlow must not reap a newer flow by correlation-ID mismatch")

		coordinator.EndFlow("test-server", newFlow.CorrelationID, true, nil)
	})
}
