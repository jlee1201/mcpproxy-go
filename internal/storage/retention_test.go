package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/smart-mcp-proxy/mcpproxy-go/internal/config"
)

// registerStaleIdentity creates and saves a ServerIdentity whose LastSeen is
// old enough to be stale under the given threshold, bypassing the normal
// RegisterServerIdentity path (which always stamps LastSeen = time.Now()) so
// tests can set up a server that hasn't connected in a long time.
func registerStaleIdentity(t *testing.T, manager *Manager, name string, staleBy time.Duration) *ServerIdentity {
	t.Helper()

	identity := NewServerIdentity(&config.ServerConfig{Name: name, URL: "https://example.com/" + name}, "/tmp/mcp_config.json")
	identity.LastSeen = time.Now().Add(-staleBy)

	require.NoError(t, manager.saveServerIdentity(identity))
	return identity
}

// TestPruneActivitiesByBudget verifies that PruneActivitiesByBudget keeps the
// newest records and deletes the oldest once the bucket exceeds the byte
// budget. This is the cap that actually bounds activity_records size: the
// existing age/count caps alone permit far more bytes than intended at
// real-world record sizes.
func TestPruneActivitiesByBudget(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	padding := strings.Repeat("x", 500)

	var oneSize int64
	for i := 0; i < 5; i++ {
		record := &ActivityRecord{
			Type:      ActivityTypeToolCall,
			Status:    "success",
			Response:  padding,
			Timestamp: time.Date(2024, 6, 1, 12, 0, i, 0, time.UTC),
		}
		err := manager.SaveActivity(record)
		require.NoError(t, err)

		if oneSize == 0 {
			data, err := record.MarshalBinary()
			require.NoError(t, err)
			oneSize = int64(len(data))
		}
	}

	count, err := manager.CountActivities()
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// Budget for ~2.5 records: expect the 2 newest to survive.
	budget := oneSize*2 + oneSize/2
	deleted, err := manager.PruneActivitiesByBudget(budget)
	require.NoError(t, err)
	assert.Equal(t, 3, deleted)

	count, err = manager.CountActivities()
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestPruneActivitiesByBudget_NoOpUnderBudget verifies nothing is deleted
// when the bucket is already within budget.
func TestPruneActivitiesByBudget_NoOpUnderBudget(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	err := manager.SaveActivity(&ActivityRecord{
		Type:      ActivityTypeToolCall,
		Status:    "success",
		Timestamp: time.Now().UTC(),
	})
	require.NoError(t, err)

	deleted, err := manager.PruneActivitiesByBudget(10 * 1024 * 1024)
	require.NoError(t, err)
	assert.Equal(t, 0, deleted)

	count, err := manager.CountActivities()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestTrimBucketToByteBudget exercises the shared trim primitive directly
// against a real bbolt bucket: oldest keys (by lexicographic/chronological
// order) must be the ones removed once total value bytes exceed the budget.
func TestTrimBucketToByteBudget(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const bucketName = "trim_test_bucket"
	value := strings.Repeat("z", 100) // 100 bytes per entry

	err := manager.db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("%03d", i) // lexicographic order == insertion order
			if err := bucket.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		// Budget for 3 entries (300 bytes): entries 000-006 should be
		// removed, leaving 007, 008, 009.
		return trimBucketToByteBudget(bucket, 300)
	})
	require.NoError(t, err)

	err = manager.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		require.NotNil(t, bucket)

		var remaining []string
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			remaining = append(remaining, string(k))
		}
		assert.Equal(t, []string{"007", "008", "009"}, remaining)
		return nil
	})
	require.NoError(t, err)
}

// TestRecordToolCall_UnboundedGrowthIsNowCapped is the regression test for
// the actual bloat bug: before this change, RecordToolCall had no eviction
// at all for an actively-used server, and the only cleanup path
// (CleanupStaleServerData) never fires for a server seen every day. Writing
// well past DefaultToolCallsBucketMaxBytes must leave the bucket bounded,
// keep the newest calls, and drop the oldest.
func TestRecordToolCall_UnboundedGrowthIsNowCapped(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	padding := strings.Repeat("y", 100*1024) // 100KB payload per call

	// DefaultToolCallsBucketMaxBytes is 5MB; 80 calls at 100KB is well past
	// that, forcing at least one trim pass.
	const numCalls = 80

	for i := 0; i < numCalls; i++ {
		record := &ToolCallRecord{
			ID:        fmt.Sprintf("call-%03d", i),
			ServerID:  serverID,
			ToolName:  "some_tool",
			Response:  padding,
			Timestamp: time.Unix(int64(1_700_000_000+i), 0),
			RequestID: fmt.Sprintf("req-%03d", i),
		}
		require.NoError(t, manager.RecordToolCall(record))
	}

	survivors, err := manager.GetServerToolCalls(serverID, numCalls)
	require.NoError(t, err)

	require.Less(t, len(survivors), numCalls,
		"bucket should have been trimmed well below the full write count")
	require.NotEmpty(t, survivors)

	// GetServerToolCalls returns newest-first; the most recent call must
	// have survived, and the oldest must have been evicted.
	assert.Equal(t, fmt.Sprintf("call-%03d", numCalls-1), survivors[0].ID)
	assert.NotEqual(t, "call-000", survivors[len(survivors)-1].ID)

	// Confirm total live bytes actually respect the budget (allowing a
	// little slack for JSON envelope overhead beyond the raw padding).
	var total int
	for _, r := range survivors {
		total += len(r.Response.(string))
	}
	assert.LessOrEqual(t, int64(total), int64(2*DefaultToolCallsBucketMaxBytes))
}

// TestRecordServerDiagnostic_TrimsToByteBudget mirrors the tool_calls trim
// test for diagnostics, which has the identical unbounded-growth shape.
func TestRecordServerDiagnostic_TrimsToByteBudget(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	padding := strings.Repeat("d", 100*1024) // 100KB payload per diagnostic

	const numRecords = 40 // 4MB total, past the 2MB default

	for i := 0; i < numRecords; i++ {
		record := &DiagnosticRecord{
			ServerID:  serverID,
			Type:      "warning",
			Category:  "connection",
			Message:   padding,
			Timestamp: time.Unix(int64(1_700_000_000+i), 0),
		}
		require.NoError(t, manager.RecordServerDiagnostic(record))
	}

	survivors, err := manager.GetServerDiagnostics(serverID, numRecords)
	require.NoError(t, err)

	require.Less(t, len(survivors), numRecords,
		"diagnostics bucket should have been trimmed well below the full write count")
	require.NotEmpty(t, survivors)
}

// TestCleanupStaleServerData_DoesNotDeadlock is the regression test for the
// deadlock this function shipped with: it acquires m.mu.Lock() and then
// used to call the public ListServerIdentities, which re-acquires m.mu via
// RLock() -- sync.RWMutex is not reentrant, so that call never returned.
// Guarded with a timeout so a regression hangs the test instead of the
// whole suite.
func TestCleanupStaleServerData_DoesNotDeadlock(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	registerStaleIdentity(t, manager, "stale-server", 48*time.Hour)

	done := make(chan struct{})
	var deleted int
	var err error
	go func() {
		deleted, err = manager.CleanupStaleServerData(24*time.Hour, map[string]bool{})
		close(done)
	}()

	select {
	case <-done:
		require.NoError(t, err)
		assert.Equal(t, 1, deleted)
	case <-time.After(5 * time.Second):
		t.Fatal("CleanupStaleServerData deadlocked (did not return within 5s)")
	}
}

// TestCleanupStaleServerData_KeepsConfiguredButStaleServer verifies the
// double-gate: a server that's stale by LastSeen but still present in
// configuredServerIDs (merely disconnected -- expired OAuth, an outage,
// etc.) must survive, while one that's stale AND no longer configured gets
// removed.
func TestCleanupStaleServerData_KeepsConfiguredButStaleServer(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	stillConfigured := registerStaleIdentity(t, manager, "still-configured-but-disconnected", 48*time.Hour)
	removed := registerStaleIdentity(t, manager, "dropped-from-config", 48*time.Hour)

	deleted, err := manager.CleanupStaleServerData(24*time.Hour, map[string]bool{
		stillConfigured.ID: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	remaining, err := manager.ListServerIdentities()
	require.NoError(t, err)

	var remainingIDs []string
	for _, id := range remaining {
		remainingIDs = append(remainingIDs, id.ID)
	}
	assert.Contains(t, remainingIDs, stillConfigured.ID, "still-configured server's data must survive despite being stale")
	assert.NotContains(t, remainingIDs, removed.ID, "server dropped from config and stale must be cleaned up")
}

// TestCleanupStaleServerData_NilMapIsNoOp verifies the safety net: a nil
// configuredServerIDs (caller could not determine current config) must be
// treated as "do nothing", never as "everything is eligible for deletion".
func TestCleanupStaleServerData_NilMapIsNoOp(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	registerStaleIdentity(t, manager, "stale-server", 48*time.Hour)

	deleted, err := manager.CleanupStaleServerData(24*time.Hour, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, deleted)

	remaining, err := manager.ListServerIdentities()
	require.NoError(t, err)
	assert.Len(t, remaining, 1, "a nil configuredServerIDs map must not delete anything")
}
