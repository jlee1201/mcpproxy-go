package storage

import (
	"os"
	"path/filepath"
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
	_, statErr := os.Stat(dbPath + ".compact-tmp")
	assert.True(t, os.IsNotExist(statErr), "compaction temp file must not survive a successful swap")

	// dbPath must exist and be openable (the atomic rename landed).
	after := bucketKeyCounts(t, dbPath)
	assert.Equal(t, before, after, "bucket key counts must be identical after compaction")
}

// TestCompactDBFile_RemovesStaleTmpFile verifies a leftover .compact-tmp
// file from a prior crashed attempt doesn't break the next compaction.
func TestCompactDBFile_RemovesStaleTmpFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "config.db")
	seedTestDB(t, dbPath, map[string]int{"upstreams": 2})

	// Simulate a stale tmp file left behind by a crashed prior attempt.
	require.NoError(t, os.WriteFile(dbPath+".compact-tmp", []byte("garbage"), 0644))

	err = compactDBFile(dbPath, testLogger(t))
	require.NoError(t, err, "a stale tmp file must not break compaction")

	counts := bucketKeyCounts(t, dbPath)
	assert.Equal(t, 2, counts["upstreams"])
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

	_, statErr := os.Stat(dbPath + ".compact-tmp")
	assert.True(t, os.IsNotExist(statErr), "no compaction attempt should have started below threshold")

	after, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before.ModTime(), after.ModTime(), "file must be untouched below threshold")
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
