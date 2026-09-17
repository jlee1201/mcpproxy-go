package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func testLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	return zap.NewNop().Sugar()
}

// seedTestDB creates a bbolt file at path with the given buckets, each
// populated with n key/value pairs.
func seedTestDB(t *testing.T, path string, buckets map[string]int) {
	t.Helper()

	db, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer db.Close()

	err = db.Update(func(tx *bbolt.Tx) error {
		for name, n := range buckets {
			b, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < n; i++ {
				key := []byte{byte(i >> 8), byte(i)}
				if err := b.Put(key, []byte("value-payload")); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// bucketKeyCounts opens path read-only and returns key counts per bucket.
func bucketKeyCounts(t *testing.T, path string) map[string]int {
	t.Helper()

	db, err := bbolt.Open(path, 0644, &bbolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer db.Close()

	counts := map[string]int{}
	err = db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			counts[string(name)] = b.Stats().KeyN
			return nil
		})
	})
	require.NoError(t, err)
	return counts
}

// TestCompactDBFile_PreservesData verifies compactDBFile produces a file
// with identical bucket key counts to the source, and swaps it into place
// atomically (dbPath exists, no leftover tmp file).
func TestCompactDBFile_PreservesData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")
	seedTestDB(t, dbPath, map[string]int{
		"oauth_tokens": 5,
		"upstreams":    3,
		"cache":        0,
	})

	before := bucketKeyCounts(t, dbPath)

	err = compactDBFile(dbPath, testLogger(t))
	require.NoError(t, err)

	// No leftover temp file after a successful swap.
	stale, globErr := filepath.Glob(dbPath + ".compact-tmp*")
	require.NoError(t, globErr)
	assert.Empty(t, stale, "compaction temp file must not survive a successful swap")

	// dbPath must exist and be openable (the atomic rename landed).
	after := bucketKeyCounts(t, dbPath)
	assert.Equal(t, before, after, "bucket key counts must be identical after compaction")
}

// TestCompactDBFile_RemovesStaleTmpFile verifies leftover .compact-tmp.*
// files from prior crashed attempts (including this run's own PID-suffixed
// name, e.g. a recycled PID) don't break the next compaction.
func TestCompactDBFile_RemovesStaleTmpFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")
	seedTestDB(t, dbPath, map[string]int{"upstreams": 2})

	// Simulate stale tmp files left behind by crashed prior attempts: one
	// matching this run's own PID-suffixed name, and one from a legacy/other
	// attempt with a different suffix -- both must be cleared by the glob.
	require.NoError(t, os.WriteFile(fmt.Sprintf("%s.compact-tmp.%d", dbPath, os.Getpid()), []byte("garbage"), 0644))
	require.NoError(t, os.WriteFile(dbPath+".compact-tmp.99999999", []byte("garbage"), 0644))

	err = compactDBFile(dbPath, testLogger(t))
	require.NoError(t, err, "a stale tmp file must not break compaction")

	counts := bucketKeyCounts(t, dbPath)
	assert.Equal(t, 2, counts["upstreams"])

	stale, _ := filepath.Glob(dbPath + ".compact-tmp*")
	assert.Empty(t, stale, "no compaction temp files should survive a successful swap")
}

// TestVerifyBucketCounts_DetectsMismatch confirms the safety check that
// aborts a swap if the compacted output doesn't match the source.
func TestVerifyBucketCounts_DetectsMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "src.db")
	dstPath := filepath.Join(tmpDir, "dst.db")
	seedTestDB(t, srcPath, map[string]int{"upstreams": 5})
	seedTestDB(t, dstPath, map[string]int{"upstreams": 3}) // mismatched count

	srcDB, err := bbolt.Open(srcPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer srcDB.Close()

	dstDB, err := bbolt.Open(dstPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer dstDB.Close()

	err = verifyBucketCounts(srcDB, dstDB)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key count mismatch")
}

// TestVerifyBucketCounts_DetectsMissingBucket confirms a bucket present in
// the source but absent from the compacted output is caught.
func TestVerifyBucketCounts_DetectsMissingBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "src.db")
	dstPath := filepath.Join(tmpDir, "dst.db")
	seedTestDB(t, srcPath, map[string]int{"upstreams": 5, "oauth_tokens": 1})
	seedTestDB(t, dstPath, map[string]int{"upstreams": 5}) // missing oauth_tokens

	srcDB, err := bbolt.Open(srcPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer srcDB.Close()

	dstDB, err := bbolt.Open(dstPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)
	defer dstDB.Close()

	err = verifyBucketCounts(srcDB, dstDB)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from compacted output")
}

// TestCompactDBFile_ReclaimsSpaceAfterDeletes is the round-3 regression test
// for the startup-compaction-ordering finding: bbolt.Compact only reclaims
// already-freed pages, it does not shrink live data. This proves the
// documented "reclaims on the restart *after* eviction has freed pages"
// story is real: a bucket that had entries written and then deleted (as
// trimBucketToByteBudget does on an ordinary write) leaves free pages
// behind, and compactDBFile must shrink the on-disk file size once those
// free pages exist -- as opposed to a file that is still full of live data,
// which compaction cannot shrink (see maybeCompactOnStartup's doc comment).
func TestCompactDBFile_ReclaimsSpaceAfterDeletes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_reclaim_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")

	db, err := bbolt.Open(dbPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)

	const bucketName = "bloated"
	const numRecords = 200
	payload := []byte(strings.Repeat("v", 32*1024)) // 32KB per record, ~6.4MB total

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		for i := 0; i < numRecords; i++ {
			key := fmt.Sprintf("%05d", i)
			if err := b.Put([]byte(key), payload); err != nil {
				return err
			}
		}
		return nil
	}))

	// Simulate what write-time trimming does on a subsequent write: delete
	// most of the records, freeing their pages without shrinking the file
	// (bbolt keeps freed pages for reuse, it doesn't return them to the OS
	// on its own).
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		for i := 0; i < numRecords-5; i++ {
			key := fmt.Sprintf("%05d", i)
			if err := b.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)

	require.NoError(t, compactDBFile(dbPath, testLogger(t)))

	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)

	assert.Less(t, afterInfo.Size(), beforeInfo.Size(),
		"compaction must shrink the file once deletes have freed pages -- the exact mechanism the documented second-restart reclaim relies on")

	counts := bucketKeyCounts(t, dbPath)
	assert.Equal(t, 5, counts[bucketName], "the surviving records must still all be present after compaction")
}

// TestMaybeCompactOnStartup_NoOpBelowThreshold verifies a config.db under
// the size threshold is left completely untouched.
func TestMaybeCompactOnStartup_NoOpBelowThreshold(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")
	seedTestDB(t, dbPath, map[string]int{"upstreams": 2})

	before, err := os.Stat(dbPath)
	require.NoError(t, err)

	maybeCompactOnStartup(dbPath, testLogger(t))

	stale, globErr := filepath.Glob(dbPath + ".compact-tmp*")
	require.NoError(t, globErr)
	assert.Empty(t, stale, "no compaction attempt should have started below threshold")

	after, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before.ModTime(), after.ModTime(), "file must be untouched below threshold")
}

// TestOpenBoltDBAtStablePath_DetectsRenameDuringBlockedOpen is the round-3
// regression test for the flock-vs-rename race: flock binds to the fd's
// inode at open() time, not to the path, so an ordinary bbolt.Open(dbPath)
// call that starts while a compaction-style rename is about to land can
// unblock afterward still bound to the pre-rename, now-orphaned inode.
//
// This reproduces the race directly: a "locker" holds dbPath's original
// inode locked while a second open is attempted (and blocks on flock);
// while it's blocked, dbPath is renamed onto (simulating compactDBFile's
// swap); the locker is then released, unblocking the second open on the
// stale inode. openBoltDBAtStablePath must detect the identity mismatch
// and retry, returning a handle bound to the new (post-rename) file, not
// the orphaned one.
func TestOpenBoltDBAtStablePath_DetectsRenameDuringBlockedOpen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "stable_open_race_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")
	seedTestDB(t, dbPath, map[string]int{"orig": 1})

	// Hold the original inode's flock open, like a compaction's srcDB does
	// mid-swap.
	locker, err := bbolt.Open(dbPath, 0644, &bbolt.Options{Timeout: 2 * time.Second})
	require.NoError(t, err)

	type openResult struct {
		db  *bbolt.DB
		err error
	}
	resultCh := make(chan openResult, 1)
	go func() {
		db, err := openBoltDBAtStablePath(dbPath, 0644, &bbolt.Options{Timeout: 5 * time.Second})
		resultCh <- openResult{db, err}
	}()

	// Give the goroutine time to reach the blocking flock() call inside its
	// first attempt's bbolt.Open, bound to the original (soon-to-be-stale)
	// inode.
	time.Sleep(200 * time.Millisecond)

	// Simulate compactDBFile's atomic swap: build a replacement file at a
	// temp path, then rename it onto dbPath. The locker's fd (and the
	// blocked goroutine's fd, once its flock() call proceeds) remain bound
	// to the original, now-unlinked-but-still-open inode.
	replacementPath := dbPath + ".replacement"
	seedTestDB(t, replacementPath, map[string]int{"fresh": 1})
	require.NoError(t, os.Rename(replacementPath, dbPath))

	// Release the original inode's lock, unblocking the goroutine's flock().
	require.NoError(t, locker.Close())

	var result openResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("openBoltDBAtStablePath did not return after the locker was released")
	}
	require.NoError(t, result.err)
	defer result.db.Close()

	// The returned handle must reflect the NEW (post-rename) file, not the
	// orphaned pre-rename one.
	err = result.db.View(func(tx *bbolt.Tx) error {
		assert.Nil(t, tx.Bucket([]byte("orig")), "must not still be bound to the orphaned pre-rename file")
		assert.NotNil(t, tx.Bucket([]byte("fresh")), "must be bound to the current (post-rename) file")
		return nil
	})
	require.NoError(t, err)
}

// TestMaybeCompactOnStartup_MissingFileIsNoOp verifies a not-yet-created
// config.db (first daemon run) doesn't error or create anything.
func TestMaybeCompactOnStartup_MissingFileIsNoOp(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")

	maybeCompactOnStartup(dbPath, testLogger(t)) // must not panic

	_, statErr := os.Stat(dbPath)
	assert.True(t, os.IsNotExist(statErr), "no file should be created")
}
