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
	assert.Less(t, time.Since(t0), 80*time.Millisecond)
}
