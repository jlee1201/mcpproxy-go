package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func activityBucketBytes(t *testing.T, m *Manager) (int64, int) {
	t.Helper()
	var size int64
	var n int
	require.NoError(t, m.db.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(ActivityRecordsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			size += int64(len(v))
			n++
			return nil
		})
	}))
	return size, n
}

func saveSizedActivity(t *testing.T, m *Manager, ts time.Time, responseBytes int) *ActivityRecord {
	t.Helper()
	rec := &ActivityRecord{
		Type:       ActivityTypeToolCall,
		ServerName: "srv",
		ToolName:   "tool",
		Response:   strings.Repeat("x", responseBytes),
		Status:     "success",
		Timestamp:  ts,
	}
	require.NoError(t, m.SaveActivity(rec))
	return rec
}

func TestSaveActivity_EnforcesByteBudgetOnWrite(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const budget = 200 * 1024
	m.SetActivityByteBudget(budget)

	base := time.Now().UTC().Add(-time.Hour)
	var last *ActivityRecord
	for i := 0; i < 100; i++ {
		last = saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Second), 10*1024)
		size, _ := activityBucketBytes(t, m)
		require.LessOrEqualf(t, size, int64(budget), "bucket over budget after write %d", i)
	}

	size, n := activityBucketBytes(t, m)
	assert.Less(t, n, 100, "older records should have been trimmed")
	assert.Equal(t, size, m.activityBytes, "tracked size should match the bucket exactly after writes")

	got, err := m.GetActivity(last.ID)
	require.NoError(t, err)
	require.NotNil(t, got, "newest record must survive the trim")
}

func TestSaveActivity_NoBudgetKeepsEverything(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 50; i++ {
		saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Second), 10*1024)
	}

	_, n := activityBucketBytes(t, m)
	assert.Equal(t, 50, n)
}

func TestSaveActivity_ProtectsJustWrittenOutOfOrderRecord(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const budget = 100 * 1024
	m.SetActivityByteBudget(budget)

	base := time.Now().UTC()
	for i := 0; i < 20; i++ {
		saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Second), 10*1024)
	}

	// Older than everything in the bucket, so it's the first trim candidate.
	old := saveSizedActivity(t, m, base.Add(-time.Hour), 10*1024)

	got, err := m.GetActivity(old.ID)
	require.NoError(t, err)
	assert.NotNil(t, got, "the record just written must not be trimmed by its own write")
}

func TestSaveActivity_BudgetStaysAccurateAfterDeletes(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	const budget = 200 * 1024
	m.SetActivityByteBudget(budget)

	base := time.Now().UTC().Add(-time.Hour)
	var recs []*ActivityRecord
	for i := 0; i < 15; i++ {
		recs = append(recs, saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Second), 10*1024))
	}

	require.NoError(t, m.DeleteActivity(recs[0].ID))
	_, err := m.PruneExcessActivities(5, 1)
	require.NoError(t, err)

	saveSizedActivity(t, m, base.Add(time.Minute), 10*1024)
	size, _ := activityBucketBytes(t, m)
	assert.Equal(t, size, m.activityBytes, "tracked size should be rescanned after deletes")

	for i := 0; i < 40; i++ {
		saveSizedActivity(t, m, base.Add(2*time.Minute+time.Duration(i)*time.Second), 10*1024)
		size, _ := activityBucketBytes(t, m)
		require.LessOrEqual(t, size, int64(budget))
	}
}

func TestStreamActivities_CancelReleasesStorageLock(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	// More than the stream's channel buffer, so the producer blocks on send.
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 300; i++ {
		saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Millisecond), 16)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := m.StreamActivities(ctx, DefaultActivityFilter())
	// On failure, unblock the producer so cleanup's Close doesn't hang too.
	defer func() {
		for range ch {
		}
	}()
	<-ch
	require.Eventually(t, func() bool { return len(ch) == cap(ch) }, 2*time.Second, 5*time.Millisecond,
		"producer should fill the buffer and block on send")
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- m.SaveActivity(&ActivityRecord{Type: ActivityTypeToolCall, Status: "success"})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SaveActivity blocked behind an abandoned export stream")
	}
}

func TestStreamActivities_YieldsAllRecords(t *testing.T) {
	m, cleanup := setupTestStorageForActivity(t)
	defer cleanup()

	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 250; i++ {
		saveSizedActivity(t, m, base.Add(time.Duration(i)*time.Millisecond), 16)
	}

	filter := DefaultActivityFilter()
	n := 0
	for range m.StreamActivities(context.Background(), filter) {
		n++
	}
	assert.Equal(t, 250, n)
}
