package runtime

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
)

// TestNew_WiresActivityRetentionConfigThroughToService is the wiring-level
// counterpart to the two SetRetentionConfig unit tests below: it drives the
// real New() constructor (see runtime.go) end-to-end instead of hand-calling
// SetRetentionConfig with a copy-pasted expression. If someone ever deletes
// the SetRetentionConfig call in New() - the exact class of bug item 2 was
// opened to fix - the unit tests below would keep passing (they don't touch
// New() at all) while this test fails, because rt.activityService would be
// left on ActivityService's own hardcoded defaults instead of the
// non-default config values below.
func TestNew_WiresActivityRetentionConfigThroughToService(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		DataDir:                    tempDir,
		Listen:                     "127.0.0.1:0",
		Servers:                    []*config.ServerConfig{},
		ActivityRetentionDays:      30,
		ActivityMaxRecords:         5000,
		ActivityCleanupIntervalMin: 15,
	}

	rt, err := New(cfg, filepath.Join(tempDir, "config.yaml"), zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close() })

	require.NotNil(t, rt.activityService, "New() must construct an ActivityService")
	assert.Equal(t, 30*24*time.Hour, rt.activityService.maxAge, "New() must wire cfg.ActivityRetentionDays through to the activity service, not leave it on the default")
	assert.Equal(t, 5000, rt.activityService.maxRecords, "New() must wire cfg.ActivityMaxRecords through to the activity service")
	assert.Equal(t, 15*time.Minute, rt.activityService.checkInterval, "New() must wire cfg.ActivityCleanupIntervalMin through to the activity service")
}

// TestNewActivityService_ConfiguredRetentionOverridesDefaults mirrors the
// SetRetentionConfig call New() makes right after constructing the
// ActivityService (see runtime.go): it must translate the config's
// activity-retention-days/-max-records/-cleanup-interval-min knobs into the
// service's internal retention state, not silently leave the service on its
// own conservative defaults. maxBytes has no config field yet, so New()
// passes 0 there deliberately - this test locks in that "0 means leave
// ActivityService's own DefaultRetentionMaxBytes in place" behavior too.
func TestNewActivityService_ConfiguredRetentionOverridesDefaults(t *testing.T) {
	logger := zap.NewNop()
	svc := NewActivityService(nil, logger)

	cfg := &config.Config{
		ActivityRetentionDays:      30,
		ActivityMaxRecords:         5000,
		ActivityCleanupIntervalMin: 15,
	}

	svc.SetRetentionConfig(
		time.Duration(cfg.ActivityRetentionDays)*24*time.Hour,
		cfg.ActivityMaxRecords,
		0,
		time.Duration(cfg.ActivityCleanupIntervalMin)*time.Minute,
	)

	assert.Equal(t, 30*24*time.Hour, svc.maxAge, "configured retention days must override the service's default max age")
	assert.Equal(t, 5000, svc.maxRecords, "configured max records must override the service's default")
	assert.Equal(t, 15*time.Minute, svc.checkInterval, "configured cleanup interval must override the service's default")
	assert.Equal(t, int64(DefaultRetentionMaxBytes), svc.maxBytes, "maxBytes has no config field yet; passing 0 must leave the service's own default byte budget in place, not zero it out")
}

// TestNewActivityService_ZeroConfigKeepsConservativeDefaults verifies the
// flip side: an unset (zero-value) config - e.g. a config.Config built
// without GetDefaultConfig() - must not disable retention entirely.
// SetRetentionConfig's "ignore non-positive values" guard means the service
// falls back to its own hardcoded conservative defaults (7d/10k/1h) instead.
func TestNewActivityService_ZeroConfigKeepsConservativeDefaults(t *testing.T) {
	logger := zap.NewNop()
	svc := NewActivityService(nil, logger)

	cfg := &config.Config{} // zero-value: no ActivityRetentionDays etc. set

	svc.SetRetentionConfig(
		time.Duration(cfg.ActivityRetentionDays)*24*time.Hour,
		cfg.ActivityMaxRecords,
		0,
		time.Duration(cfg.ActivityCleanupIntervalMin)*time.Minute,
	)

	assert.Equal(t, DefaultRetentionMaxAge, svc.maxAge)
	assert.Equal(t, DefaultRetentionMaxRecords, svc.maxRecords)
	assert.Equal(t, DefaultRetentionCheckInterval, svc.checkInterval)
	assert.Equal(t, int64(DefaultRetentionMaxBytes), svc.maxBytes)
}

// TestEmitActivitySystemStart verifies system_start event emission (Spec 024)
func TestEmitActivitySystemStart(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	rt := &Runtime{
		logger:    logger,
		eventSubs: make(map[chan Event]struct{}),
	}

	// Subscribe to events
	eventChan := rt.SubscribeEvents()
	defer rt.UnsubscribeEvents(eventChan)

	done := make(chan Event, 1)

	// Listen for activity.system.start event
	go func() {
		select {
		case evt := <-eventChan:
			if evt.Type == EventTypeActivitySystemStart {
				done <- evt
			}
		case <-time.After(2 * time.Second):
			t.Log("Timeout waiting for activity.system.start event")
		}
	}()

	// Emit system start event
	rt.EmitActivitySystemStart("v1.2.3", "127.0.0.1:8080", 150, "/path/to/config.json")

	// Wait for event
	select {
	case evt := <-done:
		assert.Equal(t, EventTypeActivitySystemStart, evt.Type, "Event type should be activity.system.start")
		assert.NotNil(t, evt.Payload, "Event payload should not be nil")
		assert.Equal(t, "v1.2.3", evt.Payload["version"], "Event should contain version")
		assert.Equal(t, "127.0.0.1:8080", evt.Payload["listen_address"], "Event should contain listen_address")
		assert.Equal(t, int64(150), evt.Payload["startup_duration_ms"], "Event should contain startup_duration_ms")
		assert.Equal(t, "/path/to/config.json", evt.Payload["config_path"], "Event should contain config_path")
		assert.NotZero(t, evt.Timestamp, "Event should have a timestamp")
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive activity.system.start event within timeout")
	}
}

// TestEmitActivitySystemStop verifies system_stop event emission (Spec 024)
func TestEmitActivitySystemStop(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	rt := &Runtime{
		logger:    logger,
		eventSubs: make(map[chan Event]struct{}),
	}

	// Subscribe to events
	eventChan := rt.SubscribeEvents()
	defer rt.UnsubscribeEvents(eventChan)

	done := make(chan Event, 1)

	// Listen for activity.system.stop event
	go func() {
		select {
		case evt := <-eventChan:
			if evt.Type == EventTypeActivitySystemStop {
				done <- evt
			}
		case <-time.After(2 * time.Second):
			t.Log("Timeout waiting for activity.system.stop event")
		}
	}()

	// Emit system stop event
	rt.EmitActivitySystemStop("signal", "SIGTERM", 3600, "")

	// Wait for event
	select {
	case evt := <-done:
		assert.Equal(t, EventTypeActivitySystemStop, evt.Type, "Event type should be activity.system.stop")
		assert.NotNil(t, evt.Payload, "Event payload should not be nil")
		assert.Equal(t, "signal", evt.Payload["reason"], "Event should contain reason")
		assert.Equal(t, "SIGTERM", evt.Payload["signal"], "Event should contain signal")
		assert.Equal(t, int64(3600), evt.Payload["uptime_seconds"], "Event should contain uptime_seconds")
		assert.Equal(t, "", evt.Payload["error_message"], "Event should contain error_message")
		assert.NotZero(t, evt.Timestamp, "Event should have a timestamp")
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive activity.system.stop event within timeout")
	}
}

// TestEmitActivitySystemStop_WithError verifies system_stop event includes error info
func TestEmitActivitySystemStop_WithError(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	rt := &Runtime{
		logger:    logger,
		eventSubs: make(map[chan Event]struct{}),
	}

	// Subscribe to events
	eventChan := rt.SubscribeEvents()
	defer rt.UnsubscribeEvents(eventChan)

	done := make(chan Event, 1)

	// Listen for activity.system.stop event
	go func() {
		select {
		case evt := <-eventChan:
			if evt.Type == EventTypeActivitySystemStop {
				done <- evt
			}
		case <-time.After(2 * time.Second):
			t.Log("Timeout waiting for activity.system.stop event")
		}
	}()

	// Emit system stop event with error
	rt.EmitActivitySystemStop("error", "", 120, "database connection lost")

	// Wait for event
	select {
	case evt := <-done:
		assert.Equal(t, EventTypeActivitySystemStop, evt.Type)
		assert.Equal(t, "error", evt.Payload["reason"])
		assert.Equal(t, "", evt.Payload["signal"])
		assert.Equal(t, int64(120), evt.Payload["uptime_seconds"])
		assert.Equal(t, "database connection lost", evt.Payload["error_message"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive activity.system.stop event within timeout")
	}
}

// TestEmitActivityInternalToolCall verifies internal_tool_call event emission (Spec 024)
func TestEmitActivityInternalToolCall(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	rt := &Runtime{
		logger:    logger,
		eventSubs: make(map[chan Event]struct{}),
	}

	// Subscribe to events
	eventChan := rt.SubscribeEvents()
	defer rt.UnsubscribeEvents(eventChan)

	done := make(chan Event, 1)

	// Listen for activity.internal_tool_call.completed event
	go func() {
		select {
		case evt := <-eventChan:
			if evt.Type == EventTypeActivityInternalToolCall {
				done <- evt
			}
		case <-time.After(2 * time.Second):
			t.Log("Timeout waiting for activity.internal_tool_call.completed event")
		}
	}()

	// Emit internal tool call event
	intent := map[string]interface{}{
		"operation_type":   "read",
		"data_sensitivity": "public",
	}
	testArgs := map[string]interface{}{
		"username": "octocat",
	}
	testResponse := map[string]interface{}{
		"login": "octocat",
		"id":    1,
	}
	rt.EmitActivityInternalToolCall(
		"call_tool_read",
		"github",
		"get_user",
		"call_tool_read",
		"sess-123",
		"req-456",
		"success",
		"",
		250,
		testArgs,
		testResponse,
		intent,
	)

	// Wait for event
	select {
	case evt := <-done:
		assert.Equal(t, EventTypeActivityInternalToolCall, evt.Type)
		assert.Equal(t, "call_tool_read", evt.Payload["internal_tool_name"])
		assert.Equal(t, "github", evt.Payload["target_server"])
		assert.Equal(t, "get_user", evt.Payload["target_tool"])
		assert.Equal(t, "call_tool_read", evt.Payload["tool_variant"])
		assert.Equal(t, "sess-123", evt.Payload["session_id"])
		assert.Equal(t, "req-456", evt.Payload["request_id"])
		assert.Equal(t, "success", evt.Payload["status"])
		assert.Equal(t, "", evt.Payload["error_message"])
		assert.Equal(t, int64(250), evt.Payload["duration_ms"])
		assert.NotNil(t, evt.Payload["intent"])
		// Verify arguments and response are captured
		assert.NotNil(t, evt.Payload["arguments"])
		args := evt.Payload["arguments"].(map[string]interface{})
		assert.Equal(t, "octocat", args["username"])
		assert.NotNil(t, evt.Payload["response"])
		resp := evt.Payload["response"].(map[string]interface{})
		assert.Equal(t, "octocat", resp["login"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive activity.internal_tool_call.completed event within timeout")
	}
}

// TestEmitActivityConfigChange verifies config_change event emission (Spec 024)
func TestEmitActivityConfigChange(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	rt := &Runtime{
		logger:    logger,
		eventSubs: make(map[chan Event]struct{}),
	}

	// Subscribe to events
	eventChan := rt.SubscribeEvents()
	defer rt.UnsubscribeEvents(eventChan)

	done := make(chan Event, 1)

	// Listen for activity.config_change event
	go func() {
		select {
		case evt := <-eventChan:
			if evt.Type == EventTypeActivityConfigChange {
				done <- evt
			}
		case <-time.After(2 * time.Second):
			t.Log("Timeout waiting for activity.config_change event")
		}
	}()

	// Emit config change event
	prevValues := map[string]interface{}{"enabled": true}
	newValues := map[string]interface{}{"enabled": false}
	rt.EmitActivityConfigChange(
		"server_updated",
		"github",
		"mcp",
		[]string{"enabled"},
		prevValues,
		newValues,
	)

	// Wait for event
	select {
	case evt := <-done:
		assert.Equal(t, EventTypeActivityConfigChange, evt.Type)
		assert.Equal(t, "server_updated", evt.Payload["action"])
		assert.Equal(t, "github", evt.Payload["affected_entity"])
		assert.Equal(t, "mcp", evt.Payload["source"])
		assert.NotNil(t, evt.Payload["changed_fields"])
		assert.NotNil(t, evt.Payload["previous_values"])
		assert.NotNil(t, evt.Payload["new_values"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive activity.config_change event within timeout")
	}
}
