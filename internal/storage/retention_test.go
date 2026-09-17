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

	// Budget for ~2.5 records: expect the 3 newest to survive. The single
	// most-recent record is always protected regardless of the budget (see
	// trimBucketToByteBudget), so one more record survives than the raw
	// budget math alone would suggest.
	budget := oneSize*2 + oneSize/2
	deleted, err := manager.PruneActivitiesByBudget(budget)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted)

	count, err = manager.CountActivities()
	require.NoError(t, err)
	assert.Equal(t, 3, count)
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
// The single newest key (009) is always protected and uncounted, so the
// survivor set is one entry larger than a naive 300-byte/100-byte-each
// budget would suggest.
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
		// Budget for 3 entries (300 bytes) plus the always-protected
		// newest: entries 000-005 should be removed, leaving 006-009.
		_, err = trimBucketToByteBudget(bucket, 300, nil)
		return err
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
		assert.Equal(t, []string{"006", "007", "008", "009"}, remaining)
		return nil
	})
	require.NoError(t, err)
}

// TestTrimBucketToByteBudget_OversizedRecordDoesNotCascadeWipe is the
// regression test for the self-deletion/cascade-wipe bug: before the
// protectedKey exemption, a single record whose own size exceeded the
// budget would inflate the running cumulative total, causing every older
// (and much smaller, individually-within-budget) record to be deleted too
// -- a single oversized write could silently wipe an entire bucket's
// history. The just-written key must survive, and the well-behaved older
// records must not be collaterally evicted just because of it.
func TestTrimBucketToByteBudget_OversizedRecordDoesNotCascadeWipe(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const bucketName = "trim_oversized_test_bucket"
	const budget = 300

	normalValue := strings.Repeat("n", 100) // 3 of these exactly fill the budget
	hugeValue := strings.Repeat("h", 10_000) // dwarfs the budget by itself

	err := manager.db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		// Pre-existing, normal-sized, already-within-budget records.
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%03d", i)
			if err := bucket.Put([]byte(key), []byte(normalValue)); err != nil {
				return err
			}
		}
		// The just-written record, far larger than the whole budget.
		hugeKey := "999"
		if err := bucket.Put([]byte(hugeKey), []byte(hugeValue)); err != nil {
			return err
		}
		_, err = trimBucketToByteBudget(bucket, budget, []byte(hugeKey))
		return err
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
		// The huge just-written record survives, and none of the
		// older, individually-within-budget records were collaterally
		// wiped just because the new record's own size dwarfs the budget.
		assert.Equal(t, []string{"000", "001", "002", "999"}, remaining)
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

// TestRecordToolCall_OversizedResponseIsTruncatedBeforeWrite verifies the
// per-record cap (MaxToolCallRecordBytes): a single response larger than
// the cap must be truncated before it's written, rather than persisted at
// full size and relying solely on trimBucketToByteBudget's protectedKey
// exemption to keep it around indefinitely at an unbounded size.
func TestRecordToolCall_OversizedResponseIsTruncatedBeforeWrite(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	oversized := strings.Repeat("r", MaxToolCallRecordBytes+1024)

	record := &ToolCallRecord{
		ID:        "call-huge",
		ServerID:  serverID,
		ToolName:  "some_tool",
		Response:  oversized,
		Timestamp: time.Unix(1_700_000_000, 0),
		RequestID: "req-huge",
	}
	require.NoError(t, manager.RecordToolCall(record))

	survivors, err := manager.GetServerToolCalls(serverID, 10)
	require.NoError(t, err)
	require.Len(t, survivors, 1)

	responseStr, ok := survivors[0].Response.(string)
	require.True(t, ok)
	assert.Less(t, len(responseStr), MaxToolCallRecordBytes,
		"oversized response must have been truncated before write, not stored at full size")
	assert.Contains(t, responseStr, "omitted")
}

// TestRecordToolCall_OversizedErrorIsTruncatedBeforeWrite is the round-3
// regression test: the per-record cap originally only cleared
// Response/Arguments, never Error, so a record with a small Response but a
// huge Error string sailed through untouched. On a later write, once that
// record was no longer the protected/newest key, its outsized bytes alone
// could cascade-evict every older (individually small) record in the
// bucket. truncateToolCallRecordToFit must catch Error too, and a
// subsequent unrelated small write must not wipe the bucket.
func TestRecordToolCall_OversizedErrorIsTruncatedBeforeWrite(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	oversizedError := strings.Repeat("e", MaxToolCallRecordBytes+1024)

	huge := &ToolCallRecord{
		ID:        "call-huge-error",
		ServerID:  serverID,
		ToolName:  "some_tool",
		Response:  "small response",
		Error:     oversizedError,
		Timestamp: time.Unix(1_700_000_000, 0),
		RequestID: "req-huge-error",
	}
	require.NoError(t, manager.RecordToolCall(huge))

	// A second, unrelated, small write -- the huge-error record is no
	// longer the protected/newest key at this point.
	small := &ToolCallRecord{
		ID:        "call-small",
		ServerID:  serverID,
		ToolName:  "some_tool",
		Response:  "ok",
		Timestamp: time.Unix(1_700_000_001, 0),
		RequestID: "req-small",
	}
	require.NoError(t, manager.RecordToolCall(small))

	survivors, err := manager.GetServerToolCalls(serverID, 10)
	require.NoError(t, err)
	require.Len(t, survivors, 2, "the oversized-Error record must not have cascade-wiped the bucket")

	var hugeSurvivor *ToolCallRecord
	for _, r := range survivors {
		if r.ID == "call-huge-error" {
			hugeSurvivor = r
		}
	}
	require.NotNil(t, hugeSurvivor, "the huge-error record itself must survive (truncated, not deleted)")
	assert.Less(t, len(hugeSurvivor.Error), MaxToolCallRecordBytes,
		"oversized Error must have been truncated before write, not stored at full size")
	assert.Contains(t, hugeSurvivor.Error, "omitted")
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
