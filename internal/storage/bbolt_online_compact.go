package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	goruntime "runtime"

	"go.etcd.io/bbolt"
)

// OnlineCompactionOptions tunes the background compactor. Startup compaction
// (maybeCompactOnStartup) is best-effort and loses the lock race against the
// previous daemon; bbolt also never returns free pages to the OS, so a burst
// leaves config.db at its high-water size forever. The online compactor
// reclaims that space while the daemon runs, with no restart.
type OnlineCompactionOptions struct {
	InitialDelay    time.Duration // first check after start; lets startup contention clear
	Interval        time.Duration // period between checks
	MinFileBytes    int64         // don't bother below this file size
	MinReclaimBytes int64         // free+pending pages must be at least this many bytes...
	MinReclaimRatio float64       // ...and at least this fraction of the file
	MaxLiveBytes    int64         // skip if live data exceeds this: every DB op blocks while compacting
	MinGap          time.Duration // minimum spacing between successful compactions
	LockWait        time.Duration // how long to wait for in-flight transactions to drain
}

// DefaultOnlineCompactionOptions returns production defaults.
func DefaultOnlineCompactionOptions() OnlineCompactionOptions {
	return OnlineCompactionOptions{
		InitialDelay:    2 * time.Minute,
		Interval:        10 * time.Minute,
		MinFileBytes:    compactionThresholdBytes,
		MinReclaimBytes: compactionMinReclaimableBytes,
		MinReclaimRatio: 0.30,
		MaxLiveBytes:    64 * 1024 * 1024,
		MinGap:          time.Hour,
		LockWait:        30 * time.Second,
	}
}

// DisableOnlineCompactionEnv turns the online compactor off when set to a
// non-empty value, as an escape hatch if a field issue needs ruling out.
const DisableOnlineCompactionEnv = "MCPPROXY_DISABLE_ONLINE_COMPACTION"

type compactorState struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	lastCompact time.Time // guarded by BoltDB.mu write lock / loop goroutine only
	lastFailure time.Time // same guard; failed passes back off by MinGap too

	warnedLiveTooLarge atomic.Bool // log the MaxLiveBytes skip once, not every interval
}

// StartOnlineCompaction launches the background compactor. It is a no-op if
// already running, on Windows (rename over an open file is unsupported), or
// when DisableOnlineCompactionEnv is set. Only the daemon bootstrap should
// call it: CLI standalone commands open the same constructors and must never
// compact. Close stops it.
func (b *BoltDB) StartOnlineCompaction(opts OnlineCompactionOptions) {
	if goruntime.GOOS == "windows" || os.Getenv(DisableOnlineCompactionEnv) != "" {
		return
	}

	b.compactor.mu.Lock()
	defer b.compactor.mu.Unlock()
	if b.compactor.cancel != nil {
		return
	}
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.compactor.cancel = cancel
	b.compactor.done = make(chan struct{})
	go b.compactionLoop(ctx, opts, b.compactor.done)
}

// StartOnlineCompaction starts online compaction on the manager's database.
func (m *Manager) StartOnlineCompaction(opts OnlineCompactionOptions) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db != nil {
		db.StartOnlineCompaction(opts)
	}
}

// stopCompactor cancels the loop and waits for any in-flight pass to finish.
// It must not be called with b.mu held: an in-flight pass needs b.mu.
func (b *BoltDB) stopCompactor() {
	b.compactor.mu.Lock()
	cancel, done := b.compactor.cancel, b.compactor.done
	b.compactor.cancel, b.compactor.done = nil, nil
	b.compactor.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
}

func (b *BoltDB) compactionLoop(ctx context.Context, opts OnlineCompactionOptions, done chan struct{}) {
	defer close(done)

	timer := time.NewTimer(opts.InitialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if _, err := b.compactOnline(ctx, opts); err != nil {
			b.logger.Warnw("Online config.db compaction failed; will retry next interval", "error", err)
		}
		timer.Reset(opts.Interval)
	}
}

// compactOnline runs one gated compaction pass. It reports whether the file
// was actually compacted. A skipped pass (gates not met, transactions never
// drained in LockWait) returns (false, nil).
func (b *BoltDB) compactOnline(ctx context.Context, opts OnlineCompactionOptions) (bool, error) {
	if !b.shouldCompactOnline(opts) {
		return false, nil
	}

	// TryLock polling, never a blocking Lock(): a blocked writer makes new
	// RLock calls queue behind it, which deadlocks any goroutine that
	// re-enters a read (nested View -> View) while holding its first RLock.
	deadline := time.Now().Add(opts.LockWait)
	for !b.mu.TryLock() {
		if ctx.Err() != nil || time.Now().After(deadline) {
			b.logger.Infow("Online compaction skipped: transactions did not drain in time; will retry next interval", "lock_wait", opts.LockWait)
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer b.mu.Unlock()

	if b.closed || ctx.Err() != nil {
		return false, nil
	}
	err := b.swapCompactedLocked()
	if err != nil {
		b.compactor.lastFailure = time.Now()
	}
	return true, err
}

// shouldCompactOnline is the cheap gate, evaluated under a shared lock.
func (b *BoltDB) shouldCompactOnline(opts OnlineCompactionOptions) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return false
	}
	if !b.compactor.lastCompact.IsZero() && time.Since(b.compactor.lastCompact) < opts.MinGap {
		return false
	}
	if !b.compactor.lastFailure.IsZero() && time.Since(b.compactor.lastFailure) < opts.MinGap {
		return false
	}
	info, err := os.Stat(b.path)
	if err != nil {
		return false
	}
	size := info.Size()
	if size < opts.MinFileBytes {
		return false
	}
	live, err := estimateCompactedBytes(b.db)
	if err != nil {
		b.logger.Warnw("Online compaction skipped: could not estimate compacted size", "error", err)
		return false
	}
	free := size - live
	if free < opts.MinReclaimBytes || float64(free) < opts.MinReclaimRatio*float64(size) {
		return false
	}
	if live > opts.MaxLiveBytes {
		if b.compactor.warnedLiveTooLarge.CompareAndSwap(false, true) {
			b.logger.Warnw("Online compaction skipped: live data too large to compact while holding the database lock",
				"live_bytes", live, "max_live_bytes", opts.MaxLiveBytes)
		}
		return false
	}
	return true
}

// swapCompactedLocked compacts b.db into a temp file, renames it over b.path
// and makes it the live handle. The caller holds b.mu exclusively, so no
// transaction is in flight and none can start.
//
// The flock is never released across the swap: dst is opened (and locked)
// before the rename and stays open as the new live handle, so another process
// opening config.db after the rename blocks on dst's lock rather than racing
// into an unlocked file. The old handle is closed only after the swap, and is
// never used again. Any failure before the rename leaves b.db untouched.
func (b *BoltDB) swapCompactedLocked() error {
	start := time.Now()
	tmpPath := fmt.Sprintf("%s.compact-tmp.%d", b.path, os.Getpid())
	// Stale leftovers from a crashed pass (any PID); we hold the source flock,
	// so no other process is compacting this file.
	if stale, _ := filepath.Glob(b.path + ".compact-tmp.*"); len(stale) > 0 {
		for _, f := range stale {
			_ = os.Remove(f)
		}
	}

	beforeInfo, err := os.Stat(b.path)
	if err != nil {
		return fmt.Errorf("stat before compaction: %w", err)
	}

	dst, err := bbolt.Open(tmpPath, 0644, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open compaction temp file: %w", err)
	}
	if err := bbolt.Compact(dst, b.db, 64*1024*1024); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: %w", err)
	}
	if err := verifyBucketCounts(b.db, dst); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("post-compaction verification failed: %w", err)
	}
	if err := os.Rename(tmpPath, b.path); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("swap compacted file into place: %w", err)
	}

	old := b.db
	b.db = dst
	b.compactor.lastCompact = time.Now()
	if err := old.Close(); err != nil {
		b.logger.Warnw("Failed to close pre-compaction db handle after swap", "error", err)
	}

	afterInfo, _ := os.Stat(b.path)
	var after int64
	if afterInfo != nil {
		after = afterInfo.Size()
	}
	b.logger.Infow("Online config.db compaction complete",
		"before_bytes", beforeInfo.Size(), "after_bytes", after,
		"reclaimed_bytes", beforeInfo.Size()-after,
		"db_paused_ms", time.Since(start).Milliseconds())
	return nil
}

// estimateCompactedBytes estimates how large the file would be after a
// compaction: the bytes actually in use across all buckets (bbolt.Compact
// repacks pages full) plus a few pages of meta/freelist/root overhead.
//
// Free-list size is the wrong measure of reclaimable space. Retention deletes
// leave leaf pages sparsely filled but still allocated, so a 62MB file can
// hold ~12MB of data with only a handful of pages on the freelist (observed
// 2026-10-07: 16KB pages, 5 free pages, 46MB of allocated bucket pages for
// ~12MB in use, plus 16MB of unused tail). Callers must hold a lock that keeps
// db open; this takes its own read transaction.
func estimateCompactedBytes(db *bbolt.DB) (int64, error) {
	var inuse int64
	err := db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(_ []byte, b *bbolt.Bucket) error {
			st := b.Stats() // aggregates nested buckets
			inuse += int64(st.BranchInuse + st.LeafInuse)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	const overheadPages = 8 // 2 meta + freelist + bucket-root pages, rounded up
	return inuse + overheadPages*int64(db.Info().PageSize), nil
}
