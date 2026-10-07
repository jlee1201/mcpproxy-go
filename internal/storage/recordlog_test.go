package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type testRec struct {
	ID    string    `json:"id"`
	TS    time.Time `json:"ts"`
	Group string    `json:"group"`
	Body  string    `json:"body,omitempty"`
}

func testSpec() recordLogSpec[testRec] {
	return recordLogSpec[testRec]{
		prefix:   "test",
		identify: func(r *testRec) (string, time.Time, string) { return r.ID, r.TS, r.Group },
		lighten: func(r *testRec) *testRec {
			c := *r
			c.Body = ""
			return &c
		},
	}
}

func openTestLog(t *testing.T, dir string, segMax int64) *recordLog[testRec] {
	t.Helper()
	l, err := openRecordLog(dir, testSpec(), segMax, zap.NewNop().Sugar())
	require.NoError(t, err)
	return l
}

func appendRec(t *testing.T, l *recordLog[testRec], id, group string, ts time.Time, body string) {
	t.Helper()
	rec := &testRec{ID: id, TS: ts, Group: group, Body: body}
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, l.append(rec, data))
}

func ids(entries []logEntry[testRec]) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.id
	}
	return out
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "test-*.jsonl"))
	require.NoError(t, err)
	return m
}

func TestRecordLog_AppendSnapshotLoad_NewestFirstAndFullBody(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		appendRec(t, l, fmt.Sprintf("r%d", i), "g", base.Add(time.Duration(i)*time.Minute), "payload-"+fmt.Sprint(i))
	}

	snap := l.snapshot(nil)
	assert.Equal(t, []string{"r4", "r3", "r2", "r1", "r0"}, ids(snap))
	assert.Empty(t, snap[0].light.Body, "index copy must be lightened")

	full, err := l.load(snap[0])
	require.NoError(t, err)
	assert.Equal(t, "payload-4", full.Body, "load must return the full record from disk")
}

func TestRecordLog_ReloadsFromDiskAfterRestart(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 10; i++ {
		appendRec(t, l, fmt.Sprintf("r%d", i), "g", base.Add(time.Duration(i)*time.Second), "x")
	}
	require.NoError(t, l.close())

	l2 := openTestLog(t, dir, 0)
	defer l2.close()
	assert.Equal(t, 10, l2.count())
	assert.Equal(t, "r9", l2.snapshot(nil)[0].id)

	// Reopen resumes the last non-full segment (no file churn per restart) and
	// only ever appends: earlier records stay readable at their old offsets.
	appendRec(t, l2, "r10", "g", time.Now(), "x")
	assert.Equal(t, 11, l2.count())
	assert.Len(t, segFiles(t, dir), 1)
	for _, e := range l2.snapshot(nil) {
		_, err := l2.load(e)
		require.NoError(t, err)
	}
}

func TestRecordLog_RotatesSegmentsAtSizeLimit(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 2048)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	body := strings.Repeat("b", 500)
	for i := 0; i < 20; i++ {
		appendRec(t, l, fmt.Sprintf("r%02d", i), "g", base.Add(time.Duration(i)*time.Second), body)
	}
	files := segFiles(t, dir)
	assert.Greater(t, len(files), 3, "20 x ~550B records at a 2KB cap must span several segments")
	for _, f := range files {
		st, err := os.Stat(f)
		require.NoError(t, err)
		assert.LessOrEqual(t, st.Size(), int64(2048+600), "segment may overshoot by at most one record")
	}
	// Every record is still loadable across segment boundaries.
	for _, e := range l.snapshot(nil) {
		full, err := l.load(e)
		require.NoError(t, err)
		assert.Equal(t, body, full.Body)
	}
}

func TestRecordLog_TombstoneSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		appendRec(t, l, fmt.Sprintf("r%d", i), "g", base.Add(time.Duration(i)*time.Second), "x")
	}
	ok, err := l.tombstone("r1")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = l.tombstone("missing")
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, l.close())

	l2 := openTestLog(t, dir, 0)
	defer l2.close()
	assert.Equal(t, []string{"r2", "r0"}, ids(l2.snapshot(nil)))
	_, found := l2.lookup("r1")
	assert.False(t, found)
}

func TestRecordLog_RecoversFromPartialTrailingLine(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	base := time.Now().Add(-time.Hour)
	appendRec(t, l, "r0", "g", base, "x")
	appendRec(t, l, "r1", "g", base.Add(time.Second), "x")
	require.NoError(t, l.close())

	// Simulate a crash mid-write: a torn record with no trailing newline.
	files := segFiles(t, dir)
	require.Len(t, files, 1)
	f, err := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString(`{"id":"torn","ts":"2026-01-0`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	l2 := openTestLog(t, dir, 0)
	defer l2.close()
	assert.Equal(t, []string{"r1", "r0"}, ids(l2.snapshot(nil)), "torn tail must be dropped, earlier records kept")

	// New writes after recovery must be readable (torn bytes truncated, not left mid-line).
	appendRec(t, l2, "r2", "g", time.Now(), "after-crash")
	l2.close()
	l3 := openTestLog(t, dir, 0)
	defer l3.close()
	assert.Equal(t, []string{"r2", "r1", "r0"}, ids(l3.snapshot(nil)))
	e, ok := l3.lookup("r2")
	require.True(t, ok)
	full, err := l3.load(e)
	require.NoError(t, err)
	assert.Equal(t, "after-crash", full.Body)
}

func TestRecordLog_IgnoresCorruptLinesInOldSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fmt.Sprintf("test-%012d.jsonl", 1))
	good, _ := json.Marshal(&testRec{ID: "ok", TS: time.Now(), Group: "g"})
	require.NoError(t, os.WriteFile(path, []byte("not json at all\n"+string(good)+"\n"), 0o644))

	l := openTestLog(t, dir, 0)
	defer l.close()
	assert.Equal(t, []string{"ok"}, ids(l.snapshot(nil)))
}

func TestRecordLog_PruneDeletesSegmentFilesWithNoLiveRecords(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 1024)
	defer l.close()
	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-time.Minute)
	body := strings.Repeat("b", 400)
	for i := 0; i < 6; i++ {
		appendRec(t, l, fmt.Sprintf("old%d", i), "g", old.Add(time.Duration(i)*time.Second), body)
	}
	for i := 0; i < 3; i++ {
		appendRec(t, l, fmt.Sprintf("new%d", i), "g", recent.Add(time.Duration(i)*time.Second), body)
	}
	before := len(segFiles(t, dir))
	require.Greater(t, before, 2)

	assert.Equal(t, 6, l.pruneOlderThan(time.Now().Add(-24*time.Hour)))
	assert.Equal(t, 3, l.count())
	assert.Less(t, len(segFiles(t, dir)), before, "fully-dead segments must be removed from disk")

	// Survivors stay loadable; a pruned record's file may be gone.
	for _, e := range l.snapshot(nil) {
		_, err := l.load(e)
		require.NoError(t, err)
	}
}

func TestRecordLog_PrunedRecordsStayGoneAfterRestartOnceSegmentDeleted(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 1024)
	old := time.Now().Add(-48 * time.Hour)
	body := strings.Repeat("b", 400)
	for i := 0; i < 6; i++ {
		appendRec(t, l, fmt.Sprintf("old%d", i), "g", old.Add(time.Duration(i)*time.Second), body)
	}
	appendRec(t, l, "keep", "g", time.Now(), body)
	l.pruneOlderThan(time.Now().Add(-24 * time.Hour))
	require.NoError(t, l.close())

	l2 := openTestLog(t, dir, 1024)
	defer l2.close()
	// Records in segments that had no live records are gone for good; any
	// that shared a segment with a survivor may reappear until the next sweep.
	l2.pruneOlderThan(time.Now().Add(-24 * time.Hour))
	assert.Equal(t, []string{"keep"}, ids(l2.snapshot(nil)))
}

func TestRecordLog_OutOfOrderTimestampsSortNewestFirst(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	appendRec(t, l, "b", "g", base.Add(2*time.Minute), "x")
	appendRec(t, l, "a", "g", base.Add(1*time.Minute), "x") // arrives late, older ts
	appendRec(t, l, "c", "g", base.Add(3*time.Minute), "x")
	assert.Equal(t, []string{"c", "b", "a"}, ids(l.snapshot(nil)))
}

func TestRecordLog_PruneExcessKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 20; i++ {
		appendRec(t, l, fmt.Sprintf("r%02d", i), "g", base.Add(time.Duration(i)*time.Second), "x")
	}
	assert.Equal(t, 0, l.pruneExcess(20, 0.9), "at the limit: nothing to prune")
	assert.Equal(t, 12, l.pruneExcess(10, 0.8), "drop down to 80% of the max")
	got := ids(l.snapshot(nil))
	assert.Len(t, got, 8)
	assert.Equal(t, "r19", got[0])
	assert.Equal(t, "r12", got[7])
}

func TestRecordLog_TrimToBudgetPerGroupKeepsNewestAndProtected(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	body := strings.Repeat("b", 1000)
	for i := 0; i < 10; i++ {
		appendRec(t, l, fmt.Sprintf("a%d", i), "A", base.Add(time.Duration(2*i)*time.Second), body)
		appendRec(t, l, fmt.Sprintf("b%d", i), "B", base.Add(time.Duration(2*i+1)*time.Second), body)
	}
	bBefore := l.groupBytes("B")

	// Budget smaller than one record: only the newest (always) and the protected ID survive.
	removed := l.trimToBudget("A", false, 10, "a0")
	assert.Equal(t, 8, removed)
	var aIDs []string
	for _, e := range l.snapshot(func(r *testRec) bool { return r.Group == "A" }) {
		aIDs = append(aIDs, e.id)
	}
	assert.ElementsMatch(t, []string{"a9", "a0"}, aIDs)
	assert.Equal(t, bBefore, l.groupBytes("B"), "other groups must be untouched")
}

func TestRecordLog_DropGroupAndGroupAccounting(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 4; i++ {
		appendRec(t, l, fmt.Sprintf("a%d", i), "A", base.Add(time.Duration(i)*time.Second), "x")
		appendRec(t, l, fmt.Sprintf("b%d", i), "B", base.Add(time.Duration(i)*time.Second), "x")
	}
	assert.ElementsMatch(t, []string{"A", "B"}, l.groups())
	assert.Equal(t, 4, l.dropGroup("A"))
	assert.Equal(t, int64(0), l.groupBytes("A"))
	assert.Equal(t, 4, l.count())
	assert.Equal(t, l.groupBytes("B"), l.bytesLive())
}

func TestRecordLog_LoadAfterSegmentDeletedReturnsErrRecordGone(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 0)
	defer l.close()
	appendRec(t, l, "r0", "g", time.Now(), "x")
	entry := l.snapshot(nil)[0]
	for _, f := range segFiles(t, dir) {
		require.NoError(t, os.Remove(f))
	}
	_, err := l.load(entry)
	assert.ErrorIs(t, err, errRecordGone)
}

func TestRecordLog_AppendAfterCloseFails(t *testing.T) {
	l := openTestLog(t, t.TempDir(), 0)
	require.NoError(t, l.close())
	rec := &testRec{ID: "x", TS: time.Now()}
	data, _ := json.Marshal(rec)
	assert.Error(t, l.append(rec, data))
}

func TestRecordLog_ConcurrentAppendListPruneRace(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, 4096)
	defer l.close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				appendRec(t, l, fmt.Sprintf("w%d-%03d", w, i), fmt.Sprintf("g%d", w%2), time.Now(), strings.Repeat("p", 200))
			}
		}(w)
	}
	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, e := range l.snapshot(nil) {
					// A concurrent prune may delete the segment; anything else is a bug.
					if _, err := l.load(e); err != nil {
						assert.ErrorIs(t, err, errRecordGone)
					}
				}
				l.trimToBudget("g0", false, 20_000, "")
				l.pruneOlderThan(time.Now().Add(-time.Hour))
				_, _ = l.tombstone("w0-001")
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	// Index must stay internally consistent with its own accounting.
	var sum int64
	for _, e := range l.snapshot(nil) {
		sum += int64(e.n)
	}
	assert.Equal(t, sum, l.bytesLive())
	assert.Equal(t, len(l.snapshot(nil)), l.count())
}

// seedLegacyBuckets writes pre-file-history data into config.db the way older
// builds did, so the migration has something to drop.
func seedLegacyBuckets(t *testing.T, dataDir string) {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(dataDir, "config.db"), 0o644, &bbolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{ActivityRecordsBucket, "server_abc_tool_calls", "server_def_tool_calls", "server_abc_diagnostics"} {
			b, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 200; i++ {
				if err := b.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(strings.Repeat("v", 2000))); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestNewManager_DropsLegacyHistoryBucketsButKeepsDiagnostics(t *testing.T) {
	dir := t.TempDir()
	seedLegacyBuckets(t, dir)

	m, err := NewManager(dir, zap.NewNop().Sugar())
	require.NoError(t, err)

	var buckets []string
	require.NoError(t, m.db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	}))
	assert.NotContains(t, buckets, ActivityRecordsBucket)
	assert.NotContains(t, buckets, "server_abc_tool_calls")
	assert.NotContains(t, buckets, "server_def_tool_calls")
	assert.Contains(t, buckets, "server_abc_diagnostics", "diagnostics stay in config.db")

	// Fresh history goes to files, and the legacy bucket is not recreated.
	require.NoError(t, m.SaveActivity(&ActivityRecord{ID: "act1", Type: ActivityTypeToolCall, Timestamp: time.Now(), ServerName: "s", ToolName: "t", Status: "success"}))
	got, err := m.GetActivity("act1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotEmpty(t, segFilesWithPrefix(t, filepath.Join(dir, HistoryDirName), "activity"))
	require.NoError(t, m.Close())

	// A second open (restart) finds the history and does not need to drop anything.
	m2, err := NewManager(dir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer m2.Close()
	got, err = m2.GetActivity("act1")
	require.NoError(t, err)
	require.NotNil(t, got, "activity must persist across restart via the history files")
}

func TestNewManager_DroppingLegacyBucketsLetsCompactionShrinkConfigDB(t *testing.T) {
	dir := t.TempDir()
	seedLegacyBuckets(t, dir)
	dbPath := filepath.Join(dir, "config.db")
	before, err := os.Stat(dbPath)
	require.NoError(t, err)

	m, err := NewManager(dir, zap.NewNop().Sugar())
	require.NoError(t, err)
	defer m.Close()

	opts := OnlineCompactionOptions{
		MinFileBytes: 1, MinReclaimBytes: 1, MinReclaimRatio: 0.1,
		MaxLiveBytes: 1 << 30, LockWait: 5 * time.Second,
	}
	ok, err := m.db.compactOnline(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, ok, "the dropped buckets leave reclaimable pages, so compaction must run")

	after, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Less(t, after.Size(), before.Size()/2, "config.db must actually shrink once history leaves it")
}

func segFilesWithPrefix(t *testing.T, dir, prefix string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, prefix+"-*.jsonl"))
	require.NoError(t, err)
	return m
}

func TestManager_GetServerToolCallsAndSessionOrderingAndIsolation(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		require.NoError(t, m.RecordToolCall(&ToolCallRecord{
			ID: fmt.Sprintf("s1-%d", i), ServerID: "srv1", ToolName: "t", MCPSessionID: "sess-a",
			Timestamp: base.Add(time.Duration(i) * time.Second), Response: "r",
		}))
		require.NoError(t, m.RecordToolCall(&ToolCallRecord{
			ID: fmt.Sprintf("s2-%d", i), ServerID: "srv2", ToolName: "t", MCPSessionID: "sess-b",
			Timestamp: base.Add(time.Duration(i) * time.Second), Response: "r",
		}))
	}

	calls, err := m.GetServerToolCalls("srv1", 3)
	require.NoError(t, err)
	require.Len(t, calls, 3)
	assert.Equal(t, "s1-4", calls[0].ID, "newest first")
	for _, c := range calls {
		assert.Equal(t, "srv1", c.ServerID)
		assert.Equal(t, "r", c.Response, "list results carry the full record, not the lightened index copy")
	}

	sess, total, err := m.GetToolCallsBySession("sess-b", 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	require.Len(t, sess, 2)
	assert.Equal(t, "s2-4", sess[0].ID)
}

func TestManager_CleanupStaleServerDataDropsHistoryGroup(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()
	stale := registerStaleIdentity(t, m, "gone-server", 48*time.Hour)
	require.NoError(t, m.RecordToolCall(&ToolCallRecord{ID: "c1", ServerID: stale.ID, ToolName: "t", Timestamp: time.Now(), Response: "r"}))
	require.NoError(t, m.RecordToolCall(&ToolCallRecord{ID: "c2", ServerID: "live-server", ToolName: "t", Timestamp: time.Now(), Response: "r"}))

	cleaned, err := m.CleanupStaleServerData(24*time.Hour, map[string]bool{"live-server": true})
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)

	assert.Equal(t, int64(0), m.toolCallLog.groupBytes(stale.ID), "stale server's file-backed history must be dropped with its identity")
	assert.Greater(t, m.toolCallLog.groupBytes("live-server"), int64(0))
}
