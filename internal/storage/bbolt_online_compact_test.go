package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
)

// forceOpts disables every gate so compactOnline always runs.
func forceOpts() OnlineCompactionOptions {
	return OnlineCompactionOptions{
		Interval:     time.Hour,
		MaxLiveBytes: 1 << 40,
		LockWait:     5 * time.Second,
	}
}

// newBloatedBoltDB returns an open BoltDB whose file is much larger than its
// live data: it writes ~8MB then deletes all but a few keys. The remaining
// keys are returned.
func newBloatedBoltDB(t *testing.T) (*BoltDB, string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	db, err := NewBoltDB(dir, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("bloat"))
		if err != nil {
			return err
		}
		for i := 0; i < 128; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%04d", i)), payload); err != nil {
				return err
			}
		}
		return nil
	}))
	kept := map[string]string{}
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("bloat"))
		for i := 0; i < 128; i++ {
			k := []byte(fmt.Sprintf("k%04d", i))
			if i%32 == 0 {
				kept[string(k)] = string(payload[:16])
				if err := b.Put(k, payload[:16]); err != nil {
					return err
				}
				continue
			}
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}))
	return db, filepath.Join(dir, configDBFilename), kept
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

func TestCompactOnline_ShrinksFileAndKeepsHandleUsable(t *testing.T) {
	skipIfWindows(t)
	db, path, kept := newBloatedBoltDB(t)
	before := fileSize(t, path)
	require.Greater(t, before, int64(4*1024*1024), "fixture should be bloated")

	ok, err := db.compactOnline(context.Background(), forceOpts())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Less(t, fileSize(t, path), before/4, "file should shrink to roughly live size")

	// Old data intact, handle writable after the swap.
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("bloat"))
		require.NotNil(t, b)
		for k, v := range kept {
			assert.Equal(t, v, string(b.Get([]byte(k))))
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("bloat")).Put([]byte("after-swap"), []byte("x"))
	}))

	// Durable: a fresh open of the path sees post-swap writes.
	require.NoError(t, db.Close())
	reopened, err := NewBoltDB(filepath.Dir(path), testLogger(t))
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, reopened.View(func(tx *bbolt.Tx) error {
		assert.Equal(t, "x", string(tx.Bucket([]byte("bloat")).Get([]byte("after-swap"))))
		return nil
	}))
}

func TestCompactOnline_KeepsFlockAcrossSwap(t *testing.T) {
	skipIfWindows(t)
	db, path, _ := newBloatedBoltDB(t)

	ok, err := db.compactOnline(context.Background(), forceOpts())
	require.NoError(t, err)
	require.True(t, ok)

	// A second process opening config.db right after the swap must still be
	// excluded by the flock on the new live file.
	other, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: 200 * time.Millisecond})
	if err == nil {
		_ = other.Close()
	}
	assert.ErrorIs(t, err, errors.ErrTimeout)
}

func TestCompactOnline_ConcurrentWritersLoseNothing(t *testing.T) {
	skipIfWindows(t)
	db, _, _ := newBloatedBoltDB(t)

	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		mu      sync.Mutex
		written = map[string]bool{}
	)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				err := db.Update(func(tx *bbolt.Tx) error {
					return tx.Bucket([]byte("bloat")).Put([]byte(key), []byte("v"))
				})
				if err == nil {
					mu.Lock()
					written[key] = true
					mu.Unlock()
				}
				time.Sleep(15 * time.Millisecond) // bursty traffic; writers queued on bbolt's writer lock hold our RLock, so a tight loop leaves no gap
			}
		}(w)
	}
	// Reader that nests View inside View, the pattern that would deadlock
	// against a blocking writer lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.View(func(tx *bbolt.Tx) error {
				return db.View(func(tx2 *bbolt.Tx) error { return nil })
			})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// A pass may legitimately skip when writers keep the lock busy for its
	// whole LockWait (production retries next interval); retry until 5 land.
	for done := 0; done < 5; {
		ok, err := db.compactOnline(context.Background(), forceOpts())
		require.NoError(t, err)
		if ok {
			done++
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop.Store(true)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("goroutines did not finish: deadlock between compaction and transactions")
	}

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("bloat"))
		for k := range written {
			if b.Get([]byte(k)) == nil {
				t.Errorf("acknowledged write %q missing after compactions", k)
			}
		}
		return nil
	}))
	assert.NotEmpty(t, written)
}

func TestCompactOnline_SkipsWhenGatesNotMet(t *testing.T) {
	skipIfWindows(t)
	db, path, _ := newBloatedBoltDB(t)
	before := fileSize(t, path)

	for name, mutate := range map[string]func(*OnlineCompactionOptions){
		"file too small":      func(o *OnlineCompactionOptions) { o.MinFileBytes = before * 2 },
		"too little reclaim":  func(o *OnlineCompactionOptions) { o.MinReclaimBytes = before * 2 },
		"ratio not met":       func(o *OnlineCompactionOptions) { o.MinReclaimRatio = 0.999 },
		"live data too large": func(o *OnlineCompactionOptions) { o.MaxLiveBytes = 1 },
	} {
		opts := forceOpts()
		mutate(&opts)
		ok, err := db.compactOnline(context.Background(), opts)
		require.NoError(t, err, name)
		assert.False(t, ok, name)
	}
	assert.Equal(t, before, fileSize(t, path), "no gate-skipped pass may touch the file")
}

func TestCompactOnline_RespectsMinGap(t *testing.T) {
	skipIfWindows(t)
	db, _, _ := newBloatedBoltDB(t)
	opts := forceOpts()
	opts.MinGap = time.Hour

	ok, err := db.compactOnline(context.Background(), opts)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = db.compactOnline(context.Background(), opts)
	require.NoError(t, err)
	assert.False(t, ok, "second pass inside MinGap must be skipped")
}

func TestCompactOnline_SkipsWhenTransactionsDoNotDrain(t *testing.T) {
	skipIfWindows(t)
	db, path, _ := newBloatedBoltDB(t)
	before := fileSize(t, path)

	release := make(chan struct{})
	started := make(chan struct{})
	viewDone := make(chan struct{})
	go func() {
		defer close(viewDone)
		_ = db.View(func(*bbolt.Tx) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	opts := forceOpts()
	opts.LockWait = 150 * time.Millisecond
	t0 := time.Now()
	ok, err := db.compactOnline(context.Background(), opts)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Less(t, time.Since(t0), 2*time.Second)

	// TryLock must not have queued a writer: new readers proceed meanwhile.
	require.NoError(t, db.View(func(*bbolt.Tx) error { return nil }))
	close(release)
	<-viewDone
	assert.Equal(t, before, fileSize(t, path))
}

func TestOnlineCompactionLoop_CompactsAndStopsOnClose(t *testing.T) {
	skipIfWindows(t)
	t.Setenv(DisableOnlineCompactionEnv, "")
	db, path, _ := newBloatedBoltDB(t)
	before := fileSize(t, path)

	opts := forceOpts()
	opts.InitialDelay = 20 * time.Millisecond
	opts.Interval = 20 * time.Millisecond
	opts.MinGap = time.Hour
	db.StartOnlineCompaction(opts)
	db.StartOnlineCompaction(opts) // idempotent

	require.Eventually(t, func() bool { return fileSize(t, path) < before/4 }, 10*time.Second, 25*time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return: compactor not stopped")
	}
	assert.NoError(t, db.Close(), "double Close is a no-op")
	assert.ErrorIs(t, db.View(func(*bbolt.Tx) error { return nil }), errors.ErrDatabaseNotOpen)
}

func TestOnlineCompaction_DisabledByEnv(t *testing.T) {
	t.Setenv(DisableOnlineCompactionEnv, "1")
	db, path, _ := newBloatedBoltDB(t)
	before := fileSize(t, path)

	opts := forceOpts()
	opts.InitialDelay = 10 * time.Millisecond
	db.StartOnlineCompaction(opts)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, before, fileSize(t, path))
}

func TestEstimateReclaimableBytesWithRetry_WinsRaceAgainstDepartingHolder(t *testing.T) {
	skipIfWindows(t)
	path := filepath.Join(t.TempDir(), configDBFilename)
	seedTestDB(t, path, map[string]int{"a": 10})

	oldA, oldT, oldD := startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay
	startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay = 10, 150*time.Millisecond, 50*time.Millisecond
	defer func() {
		startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay = oldA, oldT, oldD
	}()

	holder, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	go func() {
		time.Sleep(600 * time.Millisecond) // simulates the previous daemon finishing shutdown
		_ = holder.Close()
	}()

	_, err = estimateReclaimableBytesWithRetry(path, testLogger(t))
	assert.NoError(t, err)
}

func TestEstimateReclaimableBytesWithRetry_GivesUpAndSkipsRetryOnOtherErrors(t *testing.T) {
	skipIfWindows(t)
	oldA, oldT, oldD := startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay
	startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay = 3, 100*time.Millisecond, 10*time.Millisecond
	defer func() {
		startupEstimateAttempts, startupEstimateLockTimeout, startupEstimateRetryDelay = oldA, oldT, oldD
	}()

	// Permanent lock holder: bounded failure after all attempts.
	path := filepath.Join(t.TempDir(), configDBFilename)
	seedTestDB(t, path, map[string]int{"a": 10})
	holder, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer holder.Close()
	_, err = estimateReclaimableBytesWithRetry(path, testLogger(t))
	assert.ErrorIs(t, err, errors.ErrTimeout)

	// Non-timeout error (corrupt file) returns without burning the retry budget.
	bad := filepath.Join(t.TempDir(), configDBFilename)
	require.NoError(t, os.WriteFile(bad, make([]byte, 16384), 0644))
	t0 := time.Now()
	_, err = estimateReclaimableBytesWithRetry(bad, testLogger(t))
	require.Error(t, err)
	assert.NotErrorIs(t, err, errors.ErrTimeout)
	assert.Less(t, time.Since(t0), 2*time.Second, "must not burn the retry budget (4 attempts x 3s lock timeout)")
}

// An opener already blocked on the OLD inode's flock when the swap lands gets
// that lock the moment the old handle closes. openBoltDBAtStablePath must
// notice the inode change and re-block on the new live file instead of
// returning a handle bound to the orphaned inode.
func TestCompactOnline_BlockedOpenerRebindsToNewInode(t *testing.T) {
	skipIfWindows(t)
	db, path, _ := newBloatedBoltDB(t)

	type result struct {
		db  *bbolt.DB
		err error
	}
	opened := make(chan result, 1)
	go func() {
		d, err := openBoltDBAtStablePath(path, 0644, &bbolt.Options{Timeout: 10 * time.Second})
		opened <- result{d, err}
	}()
	time.Sleep(200 * time.Millisecond) // let the opener block on the old inode

	ok, err := db.compactOnline(context.Background(), forceOpts())
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("bloat")).Put([]byte("post-swap"), []byte("y"))
	}))

	select {
	case r := <-opened:
		if r.db != nil {
			_ = r.db.Close()
		}
		t.Fatalf("opener returned while live handle still held the lock: err=%v", r.err)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, db.Close())
	select {
	case r := <-opened:
		require.NoError(t, r.err)
		defer r.db.Close()
		require.NoError(t, r.db.View(func(tx *bbolt.Tx) error {
			assert.Equal(t, "y", string(tx.Bucket([]byte("bloat")).Get([]byte("post-swap"))),
				"opener must be bound to the new inode, not the orphaned one")
			return nil
		}))
	case <-time.After(5 * time.Second):
		t.Fatal("opener never acquired the lock after the live handle closed")
	}
}

func TestCompactOnline_FailureBacksOffAndStaleTmpIsCleaned(t *testing.T) {
	skipIfWindows(t)
	db, path, _ := newBloatedBoltDB(t)

	opts := forceOpts()
	opts.MinGap = time.Hour
	db.compactor.lastFailure = time.Now()
	assert.False(t, db.shouldCompactOnline(opts), "a recent failed pass must back off by MinGap")
	db.compactor.lastFailure = time.Time{}
	assert.True(t, db.shouldCompactOnline(opts))

	stale := path + ".compact-tmp.99999"
	require.NoError(t, os.WriteFile(stale, []byte("junk"), 0644))
	ok, err := db.compactOnline(context.Background(), opts)
	require.NoError(t, err)
	require.True(t, ok)
	_, statErr := os.Stat(stale)
	assert.True(t, os.IsNotExist(statErr), "stale tmp from a crashed pass should be removed")
}

func TestStartOnlineCompaction_NoopAfterClose(t *testing.T) {
	skipIfWindows(t)
	db, _, _ := newBloatedBoltDB(t)
	require.NoError(t, db.Close())
	db.StartOnlineCompaction(forceOpts())
	assert.Nil(t, db.compactor.cancel, "must not spawn a compactor on a closed db")
}

// Retention deletes leave pages allocated but sparsely filled, with an almost
// empty freelist. The gate must key off in-use bytes, not freelist size
// (2026-10-07: a 62MB config.db with 5 free pages never compacted).
func TestCompactOnline_ReclaimsSparsePagesWithEmptyFreelist(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	db, err := NewBoltDB(dir, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	path := filepath.Join(dir, configDBFilename)

	// FillPercent at its minimum splits pages ~10% full, in sequential order:
	// lots of allocated-but-sparse pages and (almost) nothing on the freelist.
	val := make([]byte, 1024)
	const keys = 3000
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("sparse"))
		if err != nil {
			return err
		}
		b.FillPercent = 0.1
		for i := 0; i < keys; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%06d", i)), val); err != nil {
				return err
			}
		}
		return nil
	}))
	db.mu.RLock()
	free := db.db.Stats().FreePageN
	db.mu.RUnlock()
	require.Less(t, free, 20, "fixture must leave the freelist nearly empty")

	opts := OnlineCompactionOptions{
		MinFileBytes:    1 << 20,
		MinReclaimBytes: 1 << 20,
		MinReclaimRatio: 0.30,
		MaxLiveBytes:    1 << 40,
		LockWait:        5 * time.Second,
	}
	before := fileSize(t, path)
	require.True(t, db.shouldCompactOnline(opts), "sparse pages must count as reclaimable")

	ok, err := db.compactOnline(context.Background(), opts)
	require.NoError(t, err)
	require.True(t, ok)
	after := fileSize(t, path)
	assert.Less(t, after, before/2, "before=%d after=%d", before, after)

	var n int
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		n = tx.Bucket([]byte("sparse")).Stats().KeyN
		return nil
	}))
	assert.Equal(t, keys, n)

	// Estimator tracks reality: within 25% of the file it actually produced.
	db.mu.RLock()
	est, err := estimateCompactedBytes(db.db)
	db.mu.RUnlock()
	require.NoError(t, err)
	assert.InDelta(t, float64(after), float64(est), 0.25*float64(after), "est=%d actual=%d", est, after)
}
