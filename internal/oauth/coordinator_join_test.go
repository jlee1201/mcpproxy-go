package oauth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthFlowCoordinator_JoinFlow(t *testing.T) {
	endAfter := func(c *OAuthFlowCoordinator, flow *OAuthFlowContext, success bool, err error) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			c.EndFlow(flow.ServerName, flow.CorrelationID, success, err)
		}()
	}

	t.Run("no active flow is reported as such, not as success", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		assert.Equal(t, JoinNoFlow, c.JoinFlow(context.Background(), "s", time.Second))
	})

	t.Run("successful flow", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		endAfter(c, flow, true, nil)
		assert.Equal(t, JoinSucceeded, c.JoinFlow(context.Background(), "s", time.Second))
	})

	t.Run("failed flow with a nil error is still a failure", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		endAfter(c, flow, false, nil)
		assert.Equal(t, JoinFailed, c.JoinFlow(context.Background(), "s", time.Second))
	})

	t.Run("failed flow with an error", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		endAfter(c, flow, false, errors.New("user closed the tab"))
		assert.Equal(t, JoinFailed, c.JoinFlow(context.Background(), "s", time.Second))
	})

	t.Run("expired flow is a failure, not the caller's timeout", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		go func() {
			time.Sleep(50 * time.Millisecond)
			c.expireFlow("s", flow.CorrelationID)
		}()
		assert.Equal(t, JoinFailed, c.JoinFlow(context.Background(), "s", time.Second))
	})

	t.Run("caller timeout leaves the flow running and removes the waiter", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		defer c.EndFlow("s", flow.CorrelationID, true, nil)

		assert.Equal(t, JoinTimedOut, c.JoinFlow(context.Background(), "s", 50*time.Millisecond))
		assert.True(t, c.IsFlowActive("s"))
		c.mu.RLock()
		defer c.mu.RUnlock()
		assert.Empty(t, c.waiters["s"])
	})

	t.Run("canceled context removes the waiter", func(t *testing.T) {
		c := NewOAuthFlowCoordinator()
		flow, err := c.StartFlow("s")
		require.NoError(t, err)
		defer c.EndFlow("s", flow.CorrelationID, true, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.Equal(t, JoinCanceled, c.JoinFlow(ctx, "s", time.Second))
		c.mu.RLock()
		defer c.mu.RUnlock()
		assert.Empty(t, c.waiters["s"])
	})
}

func TestOAuthFlowCoordinator_WaitForFlowTimeoutRemovesWaiter(t *testing.T) {
	c := NewOAuthFlowCoordinator()
	flow, err := c.StartFlow("s")
	require.NoError(t, err)
	defer c.EndFlow("s", flow.CorrelationID, true, nil)

	assert.ErrorIs(t, c.WaitForFlow(context.Background(), "s", 50*time.Millisecond), ErrFlowTimeout)
	c.mu.RLock()
	defer c.mu.RUnlock()
	assert.Empty(t, c.waiters["s"])
}
