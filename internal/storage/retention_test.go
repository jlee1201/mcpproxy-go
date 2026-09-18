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

	// Response stays an object after truncation, matching the OAS contract
	// (contracts.ToolCallRecord.Response is swaggertype:"object") -- a bare
	// string placeholder would silently violate that for typed API consumers.
	responseObj, ok := survivors[0].Response.(map[string]interface{})
	require.True(t, ok, "truncated Response must remain a JSON object, not a string")
	assert.Equal(t, true, responseObj["truncated"])
	assert.Contains(t, responseObj, "original_bytes")
	assert.False(t, survivors[0].ArgumentsTruncated,
		"clearing Response alone was enough to fit the budget; Arguments must not have been touched")
}

// TestRecordToolCall_OversizedResponseDoesNotTruncateArguments is the
// regression test for round-5 finding #4's reorder fix: when clearing
// Response alone is enough to fit the per-record budget,
// truncateToolCallRecordToFit must stop there and leave Arguments intact --
// Arguments is what ReplayToolCall falls back to when no override is
// supplied, so clearing it destroys replay capability and should only
// happen if clearing Response alone wasn't sufficient.
func TestRecordToolCall_OversizedResponseDoesNotTruncateArguments(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	oversized := strings.Repeat("r", MaxToolCallRecordBytes+1024)

	record := &ToolCallRecord{
		ID:        "call-huge-response-real-args",
		ServerID:  serverID,
		ToolName:  "some_tool",
		Arguments: map[string]interface{}{"path": "/tmp/important-file.txt"},
		Response:  oversized,
		Timestamp: time.Unix(1_700_000_000, 0),
		RequestID: "req-huge-response-real-args",
	}
	require.NoError(t, manager.RecordToolCall(record))

	survivors, err := manager.GetServerToolCalls(serverID, 10)
	require.NoError(t, err)
	require.Len(t, survivors, 1)

	assert.False(t, survivors[0].ArgumentsTruncated,
		"Response alone was enough to fit the budget; Arguments must survive intact")
	assert.Equal(t, "/tmp/important-file.txt", survivors[0].Arguments["path"],
		"Arguments must be preserved verbatim when only Response needed clearing")
}

// TestRecordToolCall_OversizedArgumentsAndResponseTruncatesArguments covers
// the case where clearing Response alone is NOT enough: Arguments must then
// be cleared too, and ArgumentsTruncated must be set to true so callers
// (specifically ReplayToolCall) can tell that a nil Arguments here means
// "unknown", not "no arguments".
func TestRecordToolCall_OversizedArgumentsAndResponseTruncatesArguments(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	oversized := strings.Repeat("r", MaxToolCallRecordBytes+1024)

	record := &ToolCallRecord{
		ID:       "call-huge-args-and-response",
		ServerID: serverID,
		ToolName: "some_tool",
		Arguments: map[string]interface{}{
			"blob": strings.Repeat("a", MaxToolCallRecordBytes),
		},
		Response:  oversized,
		Timestamp: time.Unix(1_700_000_000, 0),
		RequestID: "req-huge-args-and-response",
	}
	require.NoError(t, manager.RecordToolCall(record))

	survivors, err := manager.GetServerToolCalls(serverID, 10)
	require.NoError(t, err)
	require.Len(t, survivors, 1)

	assert.True(t, survivors[0].ArgumentsTruncated,
		"clearing Response alone was not enough; Arguments must have been cleared and flagged")
	assert.Nil(t, survivors[0].Arguments)
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

// TestRecordToolCall_MinimalFallbackCapsServerName is the regression test
// for round-4's medium-severity truncation-path finding: the minimal
// last-resort record (built when even clearing Response/Arguments/Error
// still exceeds maxBytes) used to copy ID/ServerID/ServerName/ToolName
// verbatim with no cap of its own. A pathologically long ServerName (e.g. a
// misconfigured or malicious upstream server) could make even the "minimal"
// record exceed maxBytes -- which trimBucketToByteBudget's protected-newest-
// key exemption would then keep forever regardless of size: the exact
// cascade-wipe class this package's truncation logic exists to prevent,
// just reached through the fallback path instead of the normal one.
// truncateFallbackField now caps every such field before the minimal
// record is built.
func TestRecordToolCall_MinimalFallbackCapsServerName(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	serverID := "test-server-id"
	hugeServerName := strings.Repeat("s", MaxToolCallRecordBytes+4096)

	huge := &ToolCallRecord{
		ID:         "call-huge-server-name",
		ServerID:   serverID,
		ServerName: hugeServerName,
		ToolName:   "some_tool",
		Response:   "small",
		Timestamp:  time.Unix(1_700_000_000, 0),
		RequestID:  "req-huge-server-name",
	}
	require.NoError(t, manager.RecordToolCall(huge))

	small := &ToolCallRecord{
		ID:        "call-small-2",
		ServerID:  serverID,
		ToolName:  "some_tool",
		Response:  "ok",
		Timestamp: time.Unix(1_700_000_001, 0),
		RequestID: "req-small-2",
	}
	require.NoError(t, manager.RecordToolCall(small))

	survivors, err := manager.GetServerToolCalls(serverID, 10)
	require.NoError(t, err)
	require.Len(t, survivors, 2, "the huge-ServerName record must not have cascade-wiped the bucket via an unbounded minimal fallback")

	var hugeSurvivor *ToolCallRecord
	for _, r := range survivors {
		if r.ID == "call-huge-server-name" {
			hugeSurvivor = r
		}
	}
	require.NotNil(t, hugeSurvivor)
	assert.Less(t, len(hugeSurvivor.ServerName), MaxToolCallRecordBytes,
		"ServerName in the minimal fallback record must be capped, not stored at full size")
	assert.Contains(t, hugeSurvivor.ServerName, "...(truncated)")
}

// TestTruncateToolCallRecordToFit_RefusesToWriteIfStillOversized exercises
// the last-resort defensive branch directly: even after every field is
// capped by truncateFallbackField, an absurdly small maxBytes budget still
// can't be met, so the function must return an error rather than silently
// persist a record over budget.
func TestTruncateToolCallRecordToFit_RefusesToWriteIfStillOversized(t *testing.T) {
	record := &ToolCallRecord{
		ID:         "x",
		ServerID:   "y",
		ServerName: "z",
		ToolName:   "t",
		Response:   strings.Repeat("r", 1000),
		Timestamp:  time.Unix(1_700_000_000, 0),
	}

	_, err := truncateToolCallRecordToFit(record, 10)
	require.Error(t, err, "must refuse to write rather than silently persist a record over an unreachable budget")
}

// TestSaveActivity_OversizedResponseIsTruncatedBeforeWrite is the regression
// test for round-5's finding that activity_records had no per-record byte
// cap at all (unlike tool_calls/diagnostics), leaving it exposed to the same
// single-oversized-record cascade-wipe class MaxToolCallRecordBytes exists
// to prevent -- just never applied here. A response larger than
// MaxActivityRecordBytes must be truncated before write, not stored at full
// size, and ResponseTruncated must be set so callers can tell.
func TestSaveActivity_OversizedResponseIsTruncatedBeforeWrite(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	oversized := strings.Repeat("r", MaxActivityRecordBytes+1024)

	record := &ActivityRecord{
		Type:       ActivityTypeToolCall,
		ServerName: "test-server",
		ToolName:   "some_tool",
		Response:   oversized,
		Status:     "success",
	}
	require.NoError(t, manager.SaveActivity(record))

	stored, err := manager.GetActivity(record.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)

	assert.Less(t, len(stored.Response), MaxActivityRecordBytes,
		"oversized response must have been truncated before write, not stored at full size")
	assert.Contains(t, stored.Response, "omitted")
	assert.True(t, stored.ResponseTruncated, "ResponseTruncated must be set when the response is truncated on write")
}

// TestPruneActivitiesByBudget_OversizedRecordDoesNotCascadeWipe proves the
// per-record cap above actually prevents the cascade-wipe end to end: write
// several normal-sized activities, then one that would have been oversized
// without MaxActivityRecordBytes, then run the same trimBucketToByteBudget
// pass PruneActivitiesByBudget uses. The older, individually-small records
// must survive.
func TestPruneActivitiesByBudget_OversizedRecordDoesNotCascadeWipe(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	base := time.Unix(1_700_000_000, 0)
	for i := 0; i < 3; i++ {
		require.NoError(t, manager.SaveActivity(&ActivityRecord{
			Type:       ActivityTypeToolCall,
			ServerName: "test-server",
			ToolName:   "some_tool",
			Response:   "small response",
			Status:     "success",
			Timestamp:  base.Add(time.Duration(i) * time.Second),
		}))
	}

	require.NoError(t, manager.SaveActivity(&ActivityRecord{
		Type:       ActivityTypeToolCall,
		ServerName: "test-server",
		ToolName:   "some_tool",
		Response:   strings.Repeat("h", MaxActivityRecordBytes+4096),
		Status:     "success",
		Timestamp:  base.Add(10 * time.Second),
	}))

	// The per-record cap already bounded the huge record's stored size, so
	// even a tight budget here must not need to evict anything.
	deleted, err := manager.PruneActivitiesByBudget(int64(MaxActivityRecordBytes) * 2)
	require.NoError(t, err)
	assert.Equal(t, 0, deleted, "the per-record cap should keep total bucket bytes low enough that nothing needs eviction")

	count, err := manager.CountActivities()
	require.NoError(t, err)
	assert.Equal(t, 4, count, "no record should have been cascade-wiped")
}

// TestSaveActivity_MinimalFallbackCapsServerName mirrors
// TestRecordToolCall_MinimalFallbackCapsServerName for activity_records: the
// minimal last-resort record's fields must be capped by truncateFallbackField,
// not copied verbatim, or a pathologically long ServerName could make even
// the "minimal" record exceed maxBytes.
func TestSaveActivity_MinimalFallbackCapsServerName(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	hugeServerName := strings.Repeat("s", MaxActivityRecordBytes+4096)

	record := &ActivityRecord{
		Type:       ActivityTypeToolCall,
		ServerName: hugeServerName,
		ToolName:   "some_tool",
		Response:   "small",
		Status:     "success",
	}
	require.NoError(t, manager.SaveActivity(record))

	stored, err := manager.GetActivity(record.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)

	assert.Less(t, len(stored.ServerName), MaxActivityRecordBytes,
		"ServerName in the minimal fallback record must be capped, not stored at full size")
	assert.Contains(t, stored.ServerName, "...(truncated)")
}

// TestTruncateActivityRecordToFit_RefusesToWriteIfStillOversized mirrors
// TestTruncateToolCallRecordToFit_RefusesToWriteIfStillOversized: even after
// every field is capped, an absurdly small maxBytes budget still can't be
// met, so the function must return an error rather than silently persist a
// record over budget.
func TestTruncateActivityRecordToFit_RefusesToWriteIfStillOversized(t *testing.T) {
	record := &ActivityRecord{
		ID:         "x",
		Type:       ActivityTypeToolCall,
		ServerName: "y",
		ToolName:   "t",
		Response:   strings.Repeat("r", 1000),
		Timestamp:  time.Unix(1_700_000_000, 0),
	}

	_, err := truncateActivityRecordToFit(record, 10)
	require.Error(t, err, "must refuse to write rather than silently persist a record over an unreachable budget")
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

// TestTrimAllServerBuckets_TrimsQuietServerWithNoRecentWrites is the
// regression test for round-4's Finding 1: RecordToolCall only trims on its
// OWN write, so a server that stays configured but goes quiet (no new tool
// calls) never gets its pre-existing, already-bloated bucket trimmed -- and
// CleanupStaleServerData intentionally leaves a still-configured server's
// data alone too, since "still configured" means it isn't stale. This seeds
// an oversized tool_calls bucket directly (bypassing RecordToolCall, which
// would trim on the way in) to simulate a bucket that was already over
// budget before this fix existed, then verifies TrimAllServerBuckets -- with
// no write ever occurring for that server -- trims it down to budget.
func TestTrimAllServerBuckets_TrimsQuietServerWithNoRecentWrites(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	identity := registerStaleIdentity(t, manager, "quiet-but-configured", 1*time.Hour)

	const recordSize = 100
	numRecords := (DefaultToolCallsBucketMaxBytes / recordSize) + 20 // well over budget
	value := strings.Repeat("v", recordSize)
	bucketName := fmt.Sprintf("server_%s_tool_calls", identity.ID)

	err := manager.db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		for i := 0; i < numRecords; i++ {
			key := fmt.Sprintf("%06d", i)
			if err := bucket.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	var sizeBefore int
	err = manager.db.db.View(func(tx *bbolt.Tx) error {
		sizeBefore = tx.Bucket([]byte(bucketName)).Stats().KeyN
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numRecords, sizeBefore, "sanity check: bucket seeded above budget without going through RecordToolCall's own trim")

	trimmed, err := manager.TrimAllServerBuckets()
	require.NoError(t, err)
	assert.Equal(t, 1, trimmed, "the one oversized bucket should have been trimmed")

	err = manager.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		require.NotNil(t, bucket)
		// trimBucketToByteBudget always exempts the single newest key from
		// the cumulative-byte check regardless of budget (see its own
		// doc comment / TestTrimBucketToByteBudget_OversizedRecordDoesNotCascadeWipe),
		// so the surviving count is "however many fit within budget" plus
		// that one always-protected key.
		maxSurviving := DefaultToolCallsBucketMaxBytes/recordSize + 1
		assert.LessOrEqual(t, bucket.Stats().KeyN, maxSurviving,
			"bucket must be trimmed back down to budget even though no RecordToolCall write ever happened for this server")
		assert.Less(t, bucket.Stats().KeyN, numRecords,
			"trim must have actually removed records, not left the bucket untouched")
		return nil
	})
	require.NoError(t, err)
}

// TestTrimAllServerBuckets_TrimsMultipleServersIndependently is the
// regression test for round-5 finding #5's fix: TrimAllServerBuckets now
// trims each server's buckets in its own transaction (releasing m.mu
// between servers) instead of one giant transaction covering every server.
// This verifies that refactor didn't break the aggregation across servers
// -- every oversized server must still end up trimmed and counted, not
// just the first or last one processed.
func TestTrimAllServerBuckets_TrimsMultipleServersIndependently(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const recordSize = 100
	numRecords := (DefaultToolCallsBucketMaxBytes / recordSize) + 20
	value := strings.Repeat("v", recordSize)

	var identities []*ServerIdentity
	for i := 0; i < 3; i++ {
		identity := registerStaleIdentity(t, manager, fmt.Sprintf("quiet-server-%d", i), 1*time.Hour)
		identities = append(identities, identity)

		bucketName := fmt.Sprintf("server_%s_tool_calls", identity.ID)
		require.NoError(t, manager.db.db.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return err
			}
			for j := 0; j < numRecords; j++ {
				key := fmt.Sprintf("%06d", j)
				if err := bucket.Put([]byte(key), []byte(value)); err != nil {
					return err
				}
			}
			return nil
		}))
	}

	trimmed, err := manager.TrimAllServerBuckets()
	require.NoError(t, err)
	assert.Equal(t, 3, trimmed, "all three oversized servers must be trimmed and counted, not just one")

	for _, identity := range identities {
		bucketName := fmt.Sprintf("server_%s_tool_calls", identity.ID)
		err = manager.db.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(bucketName))
			require.NotNil(t, bucket)
			assert.Less(t, bucket.Stats().KeyN, numRecords,
				"server %s's bucket must have actually been trimmed", identity.ServerName)
			return nil
		})
		require.NoError(t, err)
	}
}

// TestTrimAllServerBuckets_NoOpWhenAllBucketsWithinBudget verifies the
// negative case: buckets already within budget are left untouched and
// TrimAllServerBuckets reports zero buckets trimmed.
func TestTrimAllServerBuckets_NoOpWhenAllBucketsWithinBudget(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	identity := registerStaleIdentity(t, manager, "well-behaved", 1*time.Hour)

	err := manager.RecordToolCall(&ToolCallRecord{
		ServerID:   identity.ID,
		ServerName: identity.ServerName,
		ToolName:   "some_tool",
		Timestamp:  time.Now(),
	})
	require.NoError(t, err)

	trimmed, err := manager.TrimAllServerBuckets()
	require.NoError(t, err)
	assert.Equal(t, 0, trimmed, "a bucket already within budget must not be counted as trimmed")
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

// TestCleanupStaleServerData_SkipsOAuthDeleteIfTokenSavedDuringCleanup is the
// regression test for round-5 medium finding #2: PersistentTokenStore.SaveToken
// writes OAuth tokens via *storage.BoltDB directly, bypassing Manager.mu
// entirely, so a concurrent save has no lock-based coordination with
// CleanupStaleServerData's delete. The fix re-reads the token from inside the
// delete's own transaction and skips deleting if its Updated timestamp is
// after staleDecisionTime (computed before the transaction began) -- i.e. a
// save that landed after staleness was decided. This test writes directly
// into the oauth_tokens bucket (bypassing SaveOAuthToken, which always
// stamps Updated=time.Now() and so can't produce a deterministic future
// timestamp) to simulate that race window without depending on real
// goroutine timing.
func TestCleanupStaleServerData_SkipsOAuthDeleteIfTokenSavedDuringCleanup(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	identity := registerStaleIdentity(t, manager, "racy-server", 60*24*time.Hour)
	hashedKey := GenerateOAuthServerKey(identity.ServerName, identity.Attributes.URL)

	futureUpdated := time.Now().Add(1 * time.Hour)
	record := &OAuthTokenRecord{
		ServerName:  hashedKey,
		AccessToken: "still-alive-token",
		Updated:     futureUpdated,
	}
	data, err := record.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, manager.db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(OAuthTokenBucket))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(hashedKey), data)
	}))

	deleted, err := manager.CleanupStaleServerData(30*24*time.Hour, map[string]bool{})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted, "identity itself must still be cleaned up")

	// The OAuth token must survive: staleDecisionTime (computed at the top
	// of CleanupStaleServerData, before this update transaction) is before
	// futureUpdated, so the guard must have skipped the delete.
	err = manager.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		require.NotNil(t, bucket)
		assert.NotNil(t, bucket.Get([]byte(hashedKey)), "token saved after staleness was decided must survive this cleanup pass")
		return nil
	})
	require.NoError(t, err)
}

// NOTE: the OAuth-token-cleanup regression tests that used to live here
// (TestCleanupStaleServerData_DeletesOAuthToken and
// TestCleanupStaleServerData_PreservesOAuthTokenForLiveServerSharingName)
// moved to manager_oauth_test.go (package storage_test). They saved tokens
// directly under the plain ServerName via manager.db.SaveOAuthToken, but
// production tokens are stored under oauth.GenerateServerKey(name, url) (a
// SHA256-suffixed key) via PersistentTokenStore -- so they gave false
// confidence for a fix (round 4's CleanupStaleServerData OAuth cleanup)
// that was a silent no-op against real data (round 5 finding). The
// replacement tests go through oauth.NewPersistentTokenStore's real
// SaveToken/GetToken path, which requires importing internal/oauth --
// internal/storage cannot import it back (oauth already imports storage),
// so they had to move to the external storage_test package.
