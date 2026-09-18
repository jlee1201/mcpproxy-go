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

// TestCleanupStaleServerData_DeletesOAuthToken is the regression test for
// round-4's medium-severity finding: oauth_tokens is keyed by plain
// ServerName (see BoltDB.SaveOAuthToken/GetOAuthToken), not by the
// content-hashed identity ID every other bucket here uses, so
// CleanupStaleServerData -- which iterates by ID -- never touched it. A
// server dropped from config entirely left its refresh/access token behind
// forever. This proves the token is now deleted along with everything else.
func TestCleanupStaleServerData_DeletesOAuthToken(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	removed := registerStaleIdentity(t, manager, "dropped-from-config", 48*time.Hour)
	require.NoError(t, manager.db.SaveOAuthToken(&OAuthTokenRecord{
		ServerName:  removed.ServerName,
		AccessToken: "some-access-token",
	}))

	deleted, err := manager.CleanupStaleServerData(24*time.Hour, map[string]bool{})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = manager.db.GetOAuthToken(removed.ServerName)
	assert.Error(t, err, "oauth token for a removed-and-cleaned-up server must be deleted, not left behind forever")
}

// TestCleanupStaleServerData_PreservesOAuthTokenForLiveServerSharingName
// covers the guard added alongside the fix above: if a still-live identity
// happens to share its ServerName with the stale identity being cleaned up
// (e.g. the same name re-added with a new URL, which changes the
// content-hashed ID but not the name), deleting oauth_tokens by name must
// NOT wipe the live server's token.
func TestCleanupStaleServerData_PreservesOAuthTokenForLiveServerSharingName(t *testing.T) {
	manager, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	sharedName := "shared-server-name"

	stale := registerStaleIdentity(t, manager, sharedName, 48*time.Hour)

	live := NewServerIdentity(&config.ServerConfig{Name: sharedName, URL: "https://example.com/" + sharedName + "-v2"}, "/tmp/mcp_config.json")
	live.LastSeen = time.Now()
	require.NoError(t, manager.saveServerIdentity(live))
	require.NotEqual(t, stale.ID, live.ID, "test setup requires two distinct identities sharing one ServerName")

	require.NoError(t, manager.db.SaveOAuthToken(&OAuthTokenRecord{
		ServerName:  sharedName,
		AccessToken: "live-servers-token",
	}))

	deleted, err := manager.CleanupStaleServerData(24*time.Hour, map[string]bool{
		live.ID: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	token, err := manager.db.GetOAuthToken(sharedName)
	require.NoError(t, err, "the live identity's token must survive even though a stale identity shared its ServerName")
	assert.Equal(t, "live-servers-token", token.AccessToken)
}
