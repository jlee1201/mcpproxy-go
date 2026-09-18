package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
	"github.com/smart-mcp-proxy/mcpproxy-go/internal/storage"
)

// TestReplayToolCall_RefusesWhenArgumentsTruncated is the regression test
// for round-5 finding #4: when a ToolCallRecord's Arguments were cleared by
// truncateToolCallRecordToFit (because the record was too large to store),
// ArgumentsTruncated is set to true and Arguments is nil. Replaying such a
// call with no override arguments must refuse outright -- silently falling
// back to nil arguments would call the live upstream tool with the wrong
// (empty) arguments instead of surfacing that the originals are gone.
func TestReplayToolCall_RefusesWhenArgumentsTruncated(t *testing.T) {
	rt := newTestRuntime(t)

	identity, err := rt.storageManager.RegisterServerIdentity(
		&config.ServerConfig{Name: "srv-truncated", URL: "https://example.com/srv-truncated"},
		"/tmp/mcp_config.json",
	)
	if err != nil {
		t.Fatalf("failed to register server identity: %v", err)
	}

	record := &storage.ToolCallRecord{
		ID:                 "call-truncated-args",
		ServerID:           identity.ID,
		ServerName:         identity.ServerName,
		ToolName:           "some_tool",
		Arguments:          nil,
		ArgumentsTruncated: true,
		Response:           "ok",
		Timestamp:          time.Unix(1_700_000_000, 0),
	}
	if err := rt.storageManager.RecordToolCall(record); err != nil {
		t.Fatalf("failed to record tool call: %v", err)
	}

	_, err = rt.ReplayToolCall("call-truncated-args", nil)
	if err == nil {
		t.Fatal("expected ReplayToolCall to refuse replay of a record with truncated arguments, got nil error")
	}
	if got := err.Error(); !strings.Contains(got, "call-truncated-args") || !strings.Contains(got, "truncated") {
		t.Fatalf("expected error to mention the call ID and truncation, got: %q", got)
	}
}

// TestReplayToolCall_ProceedsWhenArgumentsNotTruncated is the counterpart
// regression test: a record with real (non-truncated) empty Arguments must
// not be mistaken for a truncated one. It should fail later, for an
// unrelated reason (no matching upstream client in this test's server-less
// config), never with the truncation-refusal error.
func TestReplayToolCall_ProceedsWhenArgumentsNotTruncated(t *testing.T) {
	rt := newTestRuntime(t)

	identity, err := rt.storageManager.RegisterServerIdentity(
		&config.ServerConfig{Name: "srv-normal", URL: "https://example.com/srv-normal"},
		"/tmp/mcp_config.json",
	)
	if err != nil {
		t.Fatalf("failed to register server identity: %v", err)
	}

	record := &storage.ToolCallRecord{
		ID:                 "call-normal-args",
		ServerID:           identity.ID,
		ServerName:         identity.ServerName,
		ToolName:           "some_tool",
		Arguments:          nil,
		ArgumentsTruncated: false,
		Response:           "ok",
		Timestamp:          time.Unix(1_700_000_000, 0),
	}
	if err := rt.storageManager.RecordToolCall(record); err != nil {
		t.Fatalf("failed to record tool call: %v", err)
	}

	_, err = rt.ReplayToolCall("call-normal-args", nil)
	if err == nil {
		t.Fatal("expected an error since this test's runtime has no upstream servers, got nil")
	}
	if strings.Contains(err.Error(), "truncated") {
		t.Fatalf("record was not truncated; must not fail with the truncation-refusal error, got: %q", err.Error())
	}
}
