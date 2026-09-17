package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
	"go.uber.org/zap"
)

// DatabaseLockedError indicates that the database is locked by another process
type DatabaseLockedError struct {
	Path string
	Err  error
}

func (e *DatabaseLockedError) Error() string {
	return fmt.Sprintf("database %s is locked by another process", e.Path)
}

func (e *DatabaseLockedError) Unwrap() error {
	return e.Err
}

// BoltDB wraps bolt database operations
type BoltDB struct {
	db     *bbolt.DB
	logger *zap.SugaredLogger
}

// compactionThresholdBytes is the minimum config.db size before a startup
// compaction pass is attempted. Below this, compaction isn't worth the I/O.
const compactionThresholdBytes = 20 * 1024 * 1024 // 20MB

// openBoltDBAtStablePath opens path with bbolt, guarding against a narrow
// race with compactDBFile's atomic rename: flock binds to the fd's inode at
// open() time, not to the path, so if this call's internal open() happens
// just before a concurrent compaction's os.Rename swaps a new file onto
// path, and this call's flock then blocks until that compaction's srcDB
// releases its own flock on the (now-orphaned, unlinked-but-still-open)
// pre-rename inode, this call can succeed while bound to that orphaned
// inode -- silently writing every subsequent operation on this handle into
// a file nothing else will ever open again. This is not limited to two
// concurrent compactions racing each other (compactDBFile's own srcDB flock
// already serializes those): it's any ordinary bbolt.Open(path) call,
// including this one from NewBoltDB, racing a compaction it never knows is
// running.
//
// Detect it by comparing path's identity (device+inode) immediately before
// and immediately after Open. A mismatch means path was renamed onto while
// Open was in flight, so this handle's identity is not provably the current
// file -- close it and retry against the now-stable path. This can also
// false-positive (retry a perfectly good handle) if the rename happened to
// land between our "before" stat and bbolt's internal open(), which is
// harmless: the retry's own before/after stats will then agree and return
// immediately.
func openBoltDBAtStablePath(path string, mode os.FileMode, options *bbolt.Options) (*bbolt.DB, error) {
	const maxAttempts = 3
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		before, beforeErr := os.Stat(path)

		db, err := bbolt.Open(path, mode, options)
		if err != nil {
			return nil, err
		}

		after, afterErr := os.Stat(path)

		// Nothing to compare against (e.g. first-ever creation of path) --
		// accept the open as-is.
		if beforeErr != nil || afterErr != nil {
			return db, nil
		}

		if os.SameFile(before, after) {
			return db, nil
		}

		_ = db.Close()
		lastErr = fmt.Errorf("file at %s changed identity during open (concurrent compaction swap), retrying", path)
	}

	return nil, fmt.Errorf("failed to open a stable handle for %s after %d attempts: %w", path, maxAttempts, lastErr)
}

// NewBoltDB creates a new BoltDB instance
func NewBoltDB(dataDir string, logger *zap.SugaredLogger) (*BoltDB, error) {
	dbPath := filepath.Join(dataDir, "config.db")

	maybeCompactOnStartup(dbPath, logger)

	// Try to open with timeout, if it fails, immediately return database locked error
	db, err := openBoltDBAtStablePath(dbPath, 0644, &bbolt.Options{
		Timeout: 10 * time.Second,
	})
	if err != nil {
		logger.Warnf("Failed to open database on first attempt: %v", err)

		// Check if it's a timeout or lock issue - return immediately without recovery attempts
		if err == errors.ErrTimeout {
			logger.Info("Database timeout detected, another mcpproxy instance may be running")
			return nil, &DatabaseLockedError{
				Path: dbPath,
				Err:  err,
			}
		}

		// For other errors, return wrapped error
		return nil, fmt.Errorf("failed to open bolt database: %w", err)
	}

	boltDB := &BoltDB{
		db:     db,
		logger: logger,
	}

	// Initialize buckets and schema
	if err := boltDB.initBuckets(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	return boltDB, nil
}

// Close closes the database
func (b *BoltDB) Close() error {
	return b.db.Close()
}

// initBuckets creates required buckets and sets up schema
func (b *BoltDB) initBuckets() error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		// Create buckets
		buckets := []string{
			UpstreamsBucket,
			ToolStatsBucket,
			ToolHashBucket,
			OAuthTokenBucket,
			MetaBucket,
			ActivityRecordsBucket,
		}

		for _, bucket := range buckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
			}
		}

		// Set schema version
		metaBucket := tx.Bucket([]byte(MetaBucket))
		versionBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(versionBytes, CurrentSchemaVersion)
		return metaBucket.Put([]byte(SchemaVersionKey), versionBytes)
	})
}

// GetSchemaVersion returns the current schema version
func (b *BoltDB) GetSchemaVersion() (uint64, error) {
	var version uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(MetaBucket))
		if bucket == nil {
			return fmt.Errorf("meta bucket not found")
		}

		versionBytes := bucket.Get([]byte(SchemaVersionKey))
		if versionBytes == nil {
			version = 0
			return nil
		}

		version = binary.LittleEndian.Uint64(versionBytes)
		return nil
	})

	return version, err
}

// Upstream operations

// SaveUpstream saves an upstream server record
func (b *BoltDB) SaveUpstream(record *UpstreamRecord) error {
	record.Updated = time.Now()

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(UpstreamsBucket))
		data, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(record.ID), data)
	})
}

// GetUpstream retrieves an upstream server record by ID
func (b *BoltDB) GetUpstream(id string) (*UpstreamRecord, error) {
	var record *UpstreamRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(UpstreamsBucket))
		data := bucket.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("upstream not found")
		}

		record = &UpstreamRecord{}
		return record.UnmarshalBinary(data)
	})

	return record, err
}

// ListUpstreams returns all upstream server records
func (b *BoltDB) ListUpstreams() ([]*UpstreamRecord, error) {
	var records []*UpstreamRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(UpstreamsBucket))
		return bucket.ForEach(func(_, v []byte) error {
			record := &UpstreamRecord{}
			if err := record.UnmarshalBinary(v); err != nil {
				return err
			}
			records = append(records, record)
			return nil
		})
	})

	return records, err
}

// DeleteUpstream deletes an upstream server record
func (b *BoltDB) DeleteUpstream(id string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(UpstreamsBucket))
		return bucket.Delete([]byte(id))
	})
}

// Tool statistics operations

// IncrementToolStats increments the usage count for a tool
func (b *BoltDB) IncrementToolStats(toolName string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolStatsBucket))

		// Get existing record
		var record ToolStatRecord
		data := bucket.Get([]byte(toolName))
		if data != nil {
			if err := record.UnmarshalBinary(data); err != nil {
				return err
			}
		} else {
			record.ToolName = toolName
		}

		// Increment count and update timestamp
		record.Count++
		record.LastUsed = time.Now()

		// Save back
		newData, err := record.MarshalBinary()
		if err != nil {
			return err
		}

		return bucket.Put([]byte(toolName), newData)
	})
}

// GetToolStats retrieves tool statistics
func (b *BoltDB) GetToolStats(toolName string) (*ToolStatRecord, error) {
	var record *ToolStatRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolStatsBucket))
		data := bucket.Get([]byte(toolName))
		if data == nil {
			return fmt.Errorf("tool stats not found")
		}

		record = &ToolStatRecord{}
		return record.UnmarshalBinary(data)
	})

	return record, err
}

// ListToolStats returns all tool statistics
func (b *BoltDB) ListToolStats() ([]*ToolStatRecord, error) {
	var records []*ToolStatRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolStatsBucket))
		return bucket.ForEach(func(_, v []byte) error {
			record := &ToolStatRecord{}
			if err := record.UnmarshalBinary(v); err != nil {
				return err
			}
			records = append(records, record)
			return nil
		})
	})

	return records, err
}

// Tool hash operations

// SaveToolHash saves a tool hash for change detection
func (b *BoltDB) SaveToolHash(toolName, hash string) error {
	record := &ToolHashRecord{
		ToolName: toolName,
		Hash:     hash,
		Updated:  time.Now(),
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolHashBucket))
		data, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(toolName), data)
	})
}

// GetToolHash retrieves a tool hash
func (b *BoltDB) GetToolHash(toolName string) (string, error) {
	var hash string

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolHashBucket))
		data := bucket.Get([]byte(toolName))
		if data == nil {
			return fmt.Errorf("tool hash not found")
		}

		record := &ToolHashRecord{}
		if err := record.UnmarshalBinary(data); err != nil {
			return err
		}

		hash = record.Hash
		return nil
	})

	return hash, err
}

// DeleteToolHash deletes a tool hash
func (b *BoltDB) DeleteToolHash(toolName string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ToolHashBucket))
		return bucket.Delete([]byte(toolName))
	})
}

// Generic operations

// Backup creates a backup of the database
func (b *BoltDB) Backup(destPath string) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(destPath, 0644)
	})
}

// Stats returns database statistics
func (b *BoltDB) Stats() (*bbolt.Stats, error) {
	stats := b.db.Stats()
	return &stats, nil
}

// copyFile copies a file from src to dst
//
//nolint:unused // Reserved for future backup functionality
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// removeFile safely removes a file
//
//nolint:unused // Reserved for future cleanup functionality
func removeFile(path string) error {
	return os.Remove(path)
}

// OAuth token operations

// SaveOAuthToken saves an OAuth token record
func (b *BoltDB) SaveOAuthToken(record *OAuthTokenRecord) error {
	record.Updated = time.Now()

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		data, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(record.ServerName), data)
	})
}

// GetOAuthToken retrieves an OAuth token record by server name
func (b *BoltDB) GetOAuthToken(serverName string) (*OAuthTokenRecord, error) {
	var record *OAuthTokenRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		data := bucket.Get([]byte(serverName))
		if data == nil {
			return fmt.Errorf("oauth token not found")
		}

		record = &OAuthTokenRecord{}
		return record.UnmarshalBinary(data)
	})

	return record, err
}

// DeleteOAuthToken deletes an OAuth token record
func (b *BoltDB) DeleteOAuthToken(serverName string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		return bucket.Delete([]byte(serverName))
	})
}

// UpdateOAuthClientCredentials updates the client credentials (from DCR) and callback port on an existing token
// This is called after successful Dynamic Client Registration to persist the obtained client_id/secret
// and the callback port used for the redirect_uri (Spec 022: OAuth Redirect URI Port Persistence)
func (b *BoltDB) UpdateOAuthClientCredentials(serverKey, clientID, clientSecret string, callbackPort int) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		data := bucket.Get([]byte(serverKey))

		var record *OAuthTokenRecord
		if data != nil {
			// Update existing record
			record = &OAuthTokenRecord{}
			if err := record.UnmarshalBinary(data); err != nil {
				return err
			}
			record.ClientID = clientID
			record.ClientSecret = clientSecret
			record.CallbackPort = callbackPort
			record.Updated = time.Now()
		} else {
			// Create minimal record with just client credentials
			// Full token will be saved later during OAuth completion
			record = &OAuthTokenRecord{
				ServerName:   serverKey,
				ClientID:     clientID,
				ClientSecret: clientSecret,
				CallbackPort: callbackPort,
				Created:      time.Now(),
				Updated:      time.Now(),
			}
		}

		newData, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(serverKey), newData)
	})
}

// GetOAuthClientCredentials retrieves the client credentials and callback port for token refresh
// callbackPort returns 0 if not stored (legacy records or fresh records without DCR)
func (b *BoltDB) GetOAuthClientCredentials(serverKey string) (clientID, clientSecret string, callbackPort int, err error) {
	err = b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		data := bucket.Get([]byte(serverKey))
		if data == nil {
			return nil // No credentials stored
		}

		record := &OAuthTokenRecord{}
		if err := record.UnmarshalBinary(data); err != nil {
			return err
		}
		clientID = record.ClientID
		clientSecret = record.ClientSecret
		callbackPort = record.CallbackPort
		return nil
	})
	return
}

// ClearOAuthClientCredentials clears only the DCR-related fields (ClientID, ClientSecret, CallbackPort)
// while preserving any existing token data. This is called when the callback port conflicts and
// fresh DCR is required (Spec 022: OAuth Redirect URI Port Persistence)
func (b *BoltDB) ClearOAuthClientCredentials(serverKey string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		data := bucket.Get([]byte(serverKey))
		if data == nil {
			return nil // Nothing to clear
		}

		record := &OAuthTokenRecord{}
		if err := record.UnmarshalBinary(data); err != nil {
			return err
		}

		// Clear DCR-related fields
		record.ClientID = ""
		record.ClientSecret = ""
		record.CallbackPort = 0
		record.RedirectURI = ""
		record.Updated = time.Now()

		newData, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(serverKey), newData)
	})
}

// ListOAuthTokens returns all OAuth token records
func (b *BoltDB) ListOAuthTokens() ([]*OAuthTokenRecord, error) {
	var records []*OAuthTokenRecord

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(OAuthTokenBucket))
		return bucket.ForEach(func(_, v []byte) error {
			record := &OAuthTokenRecord{}
			if err := record.UnmarshalBinary(v); err != nil {
				return err
			}
			records = append(records, record)
			return nil
		})
	})

	return records, err
}

// maybeCompactOnStartup opportunistically compacts an oversized config.db
// before it is opened for normal use. It's a one-time reclaim step for
// files that grew large before the retention limits in RecordToolCall,
// RecordServerDiagnostic, and PruneActivitiesByBudget existed: bbolt.Compact
// only returns freed space to the OS, it doesn't shrink live data, so it's
// not a substitute for those limits, only a way to reclaim what they would
// have prevented. Once those limits keep the live-data floor low, bbolt
// reuses freed pages internally and this should rarely fire again.
//
// Known limitation: this runs here, before NewBoltDB's caller has ever
// opened the database, which means it runs before the per-record caps and
// trimBucketToByteBudget have ever executed against this file. On a file
// that was already bloated with LIVE (uncapped) data -- the exact scenario
// this PR exists for -- there are no free pages yet for bbolt.Compact to
// reclaim, so the first restart after upgrading to this fix compacts very
// little; only after the daemon has run once under the new write-time caps
// (shrinking the live-data floor on subsequent writes) does a *second*
// restart's compaction have real free space to reclaim. compactDBFile logs
// before/after size on every run, so a restart that reclaims little is
// visible in the logs rather than silently assumed to have worked.
//
// This must never block startup on a corrupt, locked, or otherwise
// unreadable file: any failure is logged and the normal (uncompacted)
// database is opened afterwards by the caller.
func maybeCompactOnStartup(dbPath string, logger *zap.SugaredLogger) {
	info, err := os.Stat(dbPath)
	if err != nil {
		return // no existing file yet, nothing to compact
	}
	if info.Size() < compactionThresholdBytes {
		return
	}

	logger.Infow("config.db exceeds compaction threshold, attempting one-time startup compaction",
		"path", dbPath, "size_bytes", info.Size(), "threshold_bytes", compactionThresholdBytes)

	if err := compactDBFile(dbPath, logger); err != nil {
		logger.Warnw("Startup compaction failed, continuing with uncompacted database", "error", err)
	}
}

// compactDBFile compacts dbPath into a temp file and swaps it into place.
// Safety properties, since this replaces the file holding OAuth tokens:
//   - any stale temp file from a prior crashed attempt is removed before
//     use, so reopening it can't fail with ErrBucketExists
//   - per-bucket key counts are verified equal between source and compacted
//     output before swapping; any mismatch aborts without touching dbPath
//   - the swap itself is a single atomic os.Rename, never a two-step
//     rename that leaves a window with no config.db on disk
//   - the tmp file name is unique per attempt (PID-suffixed), and srcDB's
//     flock on dbPath is held open until AFTER the rename succeeds, so a
//     second process racing its own concurrent maybeCompactOnStartup blocks
//     on that same flock (same inode, same path) until this attempt either
//     finishes or errors out -- it can never observe a half-swapped dbPath.
//   - the stale-tmp-file cleanup only runs AFTER this process holds that
//     flock, so it can never delete a live concurrent attempt's in-flight
//     tmp file: any match found once we hold the lock is provably left by a
//     dead process, not a running one (a running one would have blocked
//     above before reaching the glob).
//   - the flock guarantees above only serialize this attempt against another
//     compactDBFile call blocked on the *same* open (same inode). They do
//     NOT by themselves protect an ordinary, unrelated bbolt.Open(dbPath)
//     call (e.g. NewBoltDB's own open) whose open() happened just before
//     this rename and whose flock only unblocks after it: that caller would
//     be bound to the pre-rename inode this rename just orphaned. srcDB
//     itself is opened via openBoltDBAtStablePath for exactly this reason;
//     see its doc comment for the detect-and-retry mechanism, which callers
//     of dbPath (NewBoltDB included) must also use.
func compactDBFile(dbPath string, logger *zap.SugaredLogger) error {
	tmpPath := fmt.Sprintf("%s.compact-tmp.%d", dbPath, os.Getpid())

	srcDB, err := openBoltDBAtStablePath(dbPath, 0644, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open source for compaction: %w", err)
	}
	// Keep srcDB (and its flock on dbPath) open past compaction and
	// verification -- only release it after a successful rename below, or
	// immediately on any error path via this defer.
	closeSrc := true
	defer func() {
		if closeSrc {
			_ = srcDB.Close()
		}
	}()

	// Remove stale temp files from prior crashed attempts: this process's
	// own PID-suffixed name (recycled PID) plus any leftovers from other
	// attempts, matched by glob since the suffix varies per attempt. Safe to
	// do only now that srcDB's flock is held -- see doc comment above.
	if stale, globErr := filepath.Glob(dbPath + ".compact-tmp*"); globErr == nil {
		for _, path := range stale {
			_ = os.Remove(path)
		}
	}

	dstDB, err := bbolt.Open(tmpPath, 0644, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open compaction temp file: %w", err)
	}

	if err := bbolt.Compact(dstDB, srcDB, 64*1024*1024); err != nil {
		_ = dstDB.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: %w", err)
	}

	if err := verifyBucketCounts(srcDB, dstDB); err != nil {
		_ = dstDB.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("post-compaction verification failed: %w", err)
	}

	beforeInfo, _ := os.Stat(dbPath)

	if err := dstDB.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close compacted temp file: %w", err)
	}

	afterInfo, statErr := os.Stat(tmpPath)

	if err := os.Rename(tmpPath, dbPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("swap compacted file into place: %w", err)
	}

	// Only now release srcDB's flock -- the rename already succeeded, so
	// there's no remaining window for a second process to race a redundant
	// compaction against this one's in-flight swap.
	closeSrc = false
	if err := srcDB.Close(); err != nil {
		logger.Warnw("Failed to close source db handle after successful compaction swap", "error", err)
	}

	if beforeInfo != nil && statErr == nil {
		before, after := beforeInfo.Size(), afterInfo.Size()
		logger.Infow("Startup compaction complete", "before_bytes", before, "after_bytes", after)

		// Compaction only reclaims free pages, not live data (see
		// maybeCompactOnStartup's doc comment). A file still over the
		// threshold after compacting means it was full of live data at
		// compaction time, not free space -- expected on the first restart
		// after upgrading to the write-time retention caps, before they've
		// had a chance to shrink the live-data floor. Surface it so this
		// isn't mistaken for compaction not working.
		if after >= compactionThresholdBytes {
			logger.Infow("Startup compaction reclaimed little space; database is still mostly live data",
				"after_bytes", after, "threshold_bytes", compactionThresholdBytes,
				"note", "expect a second restart to reclaim more once write-time retention caps have shrunk live data")
		}
	}
	return nil
}

// verifyBucketCounts confirms src and dst have the same set of top-level
// buckets with identical key counts, so a compaction bug can never silently
// drop data (e.g. OAuth tokens) during the swap.
func verifyBucketCounts(src, dst *bbolt.DB) error {
	counts := map[string]int{}
	if err := src.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			counts[string(name)] = b.Stats().KeyN
			return nil
		})
	}); err != nil {
		return fmt.Errorf("read source bucket counts: %w", err)
	}

	return dst.View(func(tx *bbolt.Tx) error {
		seen := make(map[string]bool, len(counts))
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			seen[string(name)] = true
			want, ok := counts[string(name)]
			if !ok {
				return fmt.Errorf("unexpected bucket %q in compacted output", string(name))
			}
			if got := b.Stats().KeyN; got != want {
				return fmt.Errorf("bucket %q key count mismatch: src=%d dst=%d", string(name), want, got)
			}
			return nil
		}); err != nil {
			return err
		}
		for name := range counts {
			if !seen[name] {
				return fmt.Errorf("bucket %q missing from compacted output", name)
			}
		}
		return nil
	})
}
