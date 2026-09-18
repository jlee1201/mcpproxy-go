package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
)

// DefaultMaxResponseSize is the default maximum size for response truncation (64KB)
const DefaultMaxResponseSize = 64 * 1024

// activityKey generates a BBolt key for an activity record.
// Key format: {timestamp_ns}_{ulid} for natural reverse-chronological ordering.
// Using 20-digit nanosecond timestamp ensures consistent ordering.
func activityKey(timestamp time.Time, id string) []byte {
	return []byte(fmt.Sprintf("%020d_%s", timestamp.UnixNano(), id))
}

// parseActivityKey extracts the ULID from an activity key.
// Returns empty string if key format is invalid.
func parseActivityKey(key []byte) string {
	keyStr := string(key)
	// Key format: {20-digit timestamp}_{ulid}
	if len(keyStr) < 22 { // 20 digits + underscore + at least 1 char for id
		return ""
	}
	return keyStr[21:]
}

// truncateResponse truncates a response string if it exceeds maxSize.
// Returns the (potentially truncated) string and whether truncation occurred.
func truncateResponse(response string, maxSize int) (string, bool) {
	if maxSize <= 0 {
		maxSize = DefaultMaxResponseSize
	}
	if len(response) <= maxSize {
		return response, false
	}
	return response[:maxSize] + "...[truncated]", true
}

// MaxActivityRecordBytes caps the marshaled size of a single activity record
// before it's written, mirroring MaxToolCallRecordBytes/
// MaxDiagnosticRecordBytes for the tool_calls/diagnostics buckets.
// PruneActivitiesByBudget's trimBucketToByteBudget never evicts the
// just-written record (see its doc comment), so without this cap one
// oversized activity (a large tool response or arguments blob logged
// verbatim) could alone exceed the whole byte budget and, because the
// running total never resets once it crosses the threshold, cascade into
// evicting every older activity record too -- the exact bug class the
// tool-calls/diagnostics caps exist to prevent, never applied here (round-5
// finding). Sized independently of any caller's budget --
// PruneActivitiesByBudget's maxBytes is caller-supplied at runtime (see
// ActivityService.DefaultRetentionMaxBytes = 20MB), unlike the tool-calls/
// diagnostics buckets' fixed package-level defaults -- 4MB comfortably fits
// under the smallest sane byte budget while still capping any single record
// far below it.
const MaxActivityRecordBytes = 4 * 1024 * 1024

// truncateActivityRecordToFit marshals record, and if it exceeds maxBytes,
// clears its variable-size fields one at a time -- Response, then
// Arguments, then Metadata, then ErrorMessage -- re-measuring after each,
// until the result fits. Mirrors truncateToolCallRecordToFit's approach and
// rationale (see that function's doc comment for why clearing fields one at
// a time and re-measuring, rather than assuming the first clear is enough,
// matters). Falls back to a minimal fixed-shape record if every field is
// cleared and it's still over cap, so this function can never itself return
// a value over maxBytes.
func truncateActivityRecordToFit(record *ActivityRecord, maxBytes int) ([]byte, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(data) <= maxBytes {
		return data, nil
	}
	originalBytes := len(data)

	truncated := *record
	truncated.Response = fmt.Sprintf("[response omitted: %d bytes exceeds %d byte per-record cap]", originalBytes, maxBytes)
	truncated.ResponseTruncated = true
	if data, err = json.Marshal(&truncated); err != nil {
		return nil, err
	}
	if len(data) <= maxBytes {
		return data, nil
	}

	truncated.Arguments = nil
	if data, err = json.Marshal(&truncated); err != nil {
		return nil, err
	}
	if len(data) <= maxBytes {
		return data, nil
	}

	truncated.Metadata = nil
	if data, err = json.Marshal(&truncated); err != nil {
		return nil, err
	}
	if len(data) <= maxBytes {
		return data, nil
	}

	truncated.ErrorMessage = fmt.Sprintf("[error omitted: %d bytes exceeds %d byte per-record cap]", originalBytes, maxBytes)
	if data, err = json.Marshal(&truncated); err != nil {
		return nil, err
	}
	if len(data) <= maxBytes {
		return data, nil
	}

	minimal := &ActivityRecord{
		ID:                truncateFallbackField(record.ID),
		Type:              record.Type,
		ServerName:        truncateFallbackField(record.ServerName),
		ToolName:          truncateFallbackField(record.ToolName),
		Status:            truncateFallbackField(record.Status),
		ErrorMessage:      fmt.Sprintf("[record omitted: %d bytes exceeds %d byte per-record cap]", originalBytes, maxBytes),
		ResponseTruncated: true,
		Timestamp:         record.Timestamp,
		SessionID:         truncateFallbackField(record.SessionID),
		RequestID:         truncateFallbackField(record.RequestID),
	}
	data, err = json.Marshal(minimal)
	if err != nil {
		return nil, err
	}
	if len(data) > maxBytes {
		// Last resort: refuse to write rather than silently persist an
		// oversized record. See truncateToolCallRecordToFit's identical
		// check for why (trimBucketToByteBudget's protectedKey exemption
		// keeps the just-written key forever regardless of size).
		return nil, fmt.Errorf("minimal fallback activity record still exceeds %d byte cap (%d bytes), refusing to write", maxBytes, len(data))
	}
	return data, nil
}

// SaveActivity stores an activity record in BBolt.
// The record is stored with a composite key for efficient time-based queries.
func (m *Manager) SaveActivity(record *ActivityRecord) error {
	if record == nil {
		return fmt.Errorf("activity record cannot be nil")
	}

	// Generate ID if not set
	if record.ID == "" {
		record.ID = ulid.Make().String()
	}

	// Set timestamp if not set
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(ActivityRecordsBucket))
		if err != nil {
			return fmt.Errorf("failed to create activity bucket: %w", err)
		}

		data, err := truncateActivityRecordToFit(record, MaxActivityRecordBytes)
		if err != nil {
			return fmt.Errorf("failed to marshal activity record: %w", err)
		}

		key := activityKey(record.Timestamp, record.ID)
		if err := bucket.Put(key, data); err != nil {
			return fmt.Errorf("failed to store activity record: %w", err)
		}

		return nil
	})
}

// GetActivity retrieves an activity record by ID.
// Returns nil if the record is not found.
func (m *Manager) GetActivity(id string) (*ActivityRecord, error) {
	if id == "" {
		return nil, fmt.Errorf("activity ID cannot be empty")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var record *ActivityRecord

	err := m.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil // No activities yet
		}

		// Scan to find the record by ID (ID is in the key suffix)
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if parseActivityKey(k) == id {
				record = &ActivityRecord{}
				if err := record.UnmarshalBinary(v); err != nil {
					return fmt.Errorf("failed to unmarshal activity record: %w", err)
				}
				return nil
			}
		}

		return nil // Not found
	})

	if err != nil {
		return nil, err
	}

	return record, nil
}

// ListActivities returns paginated activity records matching the filter.
// Records are returned in reverse chronological order (newest first).
// Returns the records, total matching count, and any error.
func (m *Manager) ListActivities(filter ActivityFilter) ([]*ActivityRecord, int, error) {
	filter.Validate()

	m.mu.RLock()
	defer m.mu.RUnlock()

	var records []*ActivityRecord
	var total int

	err := m.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil // No activities yet
		}

		// Iterate in reverse order (newest first)
		cursor := bucket.Cursor()
		skipped := 0

		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			var record ActivityRecord
			if err := record.UnmarshalBinary(v); err != nil {
				m.logger.Warnw("Failed to unmarshal activity record",
					"key", string(k),
					"error", err)
				continue
			}

			// Check if record matches filter
			if !filter.Matches(&record) {
				continue
			}

			total++

			// Handle pagination
			if skipped < filter.Offset {
				skipped++
				continue
			}

			if len(records) < filter.Limit {
				records = append(records, &ActivityRecord{
					ID:                record.ID,
					Type:              record.Type,
					ServerName:        record.ServerName,
					ToolName:          record.ToolName,
					Arguments:         record.Arguments,
					Response:          record.Response,
					ResponseTruncated: record.ResponseTruncated,
					Status:            record.Status,
					ErrorMessage:      record.ErrorMessage,
					DurationMs:        record.DurationMs,
					Timestamp:         record.Timestamp,
					SessionID:         record.SessionID,
					RequestID:         record.RequestID,
					Metadata:          record.Metadata,
				})
			}
		}

		return nil
	})

	return records, total, err
}

// DeleteActivity deletes an activity record by ID.
// Returns nil if the record doesn't exist.
func (m *Manager) DeleteActivity(id string) error {
	if id == "" {
		return fmt.Errorf("activity ID cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil // No activities yet
		}

		// Find and delete the record by ID
		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if parseActivityKey(k) == id {
				return bucket.Delete(k)
			}
		}

		return nil // Not found, not an error
	})
}

// CountActivities returns the total number of activity records.
func (m *Manager) CountActivities() (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int

	err := m.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil
		}
		count = bucket.Stats().KeyN
		return nil
	})

	return count, err
}

// StreamActivities returns a channel that yields activity records matching the filter.
// The channel is closed when all matching records have been sent.
// This is useful for streaming large exports without loading all records into memory.
func (m *Manager) StreamActivities(filter ActivityFilter) <-chan *ActivityRecord {
	filter.Validate()
	ch := make(chan *ActivityRecord, 100)

	go func() {
		defer close(ch)

		m.mu.RLock()
		defer m.mu.RUnlock()

		err := m.db.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(ActivityRecordsBucket))
			if bucket == nil {
				return nil
			}

			cursor := bucket.Cursor()
			for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
				var record ActivityRecord
				if err := record.UnmarshalBinary(v); err != nil {
					continue
				}

				if !filter.Matches(&record) {
					continue
				}

				ch <- &record
			}

			return nil
		})

		if err != nil {
			m.logger.Errorw("Error streaming activities", "error", err)
		}
	}()

	return ch
}

// PruneOldActivities deletes activity records older than the specified duration.
// Returns the number of records deleted.
func (m *Manager) PruneOldActivities(maxAge time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-maxAge)
	cutoffKey := activityKey(cutoff, "")

	m.mu.Lock()
	defer m.mu.Unlock()

	var deleted int

	err := m.db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil
		}

		var keysToDelete [][]byte
		cursor := bucket.Cursor()

		// Keys before cutoff (older records have smaller keys)
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			// Compare keys lexicographically
			if string(k) < string(cutoffKey) {
				keysToDelete = append(keysToDelete, append([]byte{}, k...))
			} else {
				break // Keys are sorted, no more old records
			}
		}

		for _, key := range keysToDelete {
			if err := bucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete old activity: %w", err)
			}
			deleted++
		}

		return nil
	})

	if err != nil {
		return deleted, err
	}

	if deleted > 0 {
		m.logger.Infow("Pruned old activity records",
			"deleted", deleted,
			"max_age", maxAge.String())
	}

	return deleted, nil
}

// PruneExcessActivities deletes oldest records when count exceeds maxRecords.
// Deletes records until count is at targetPercent of maxRecords (default 90%).
// Returns the number of records deleted.
func (m *Manager) PruneExcessActivities(maxRecords int, targetPercent float64) (int, error) {
	if targetPercent <= 0 || targetPercent > 1 {
		targetPercent = 0.9 // Default to 90%
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var deleted int

	err := m.db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil
		}

		count := bucket.Stats().KeyN
		if count <= maxRecords {
			return nil
		}

		targetCount := int(float64(maxRecords) * targetPercent)
		toDelete := count - targetCount

		var keysToDelete [][]byte
		cursor := bucket.Cursor()

		// Delete oldest records (smallest keys)
		for k, _ := cursor.First(); k != nil && len(keysToDelete) < toDelete; k, _ = cursor.Next() {
			keysToDelete = append(keysToDelete, append([]byte{}, k...))
		}

		for _, key := range keysToDelete {
			if err := bucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete excess activity: %w", err)
			}
			deleted++
		}

		return nil
	})

	if err != nil {
		return deleted, err
	}

	if deleted > 0 {
		m.logger.Infow("Pruned excess activity records",
			"deleted", deleted,
			"max_records", maxRecords)
	}

	return deleted, nil
}

// PruneActivitiesByBudget deletes the oldest activity records once the
// bucket's total value bytes exceed maxBytes, via the same trim primitive
// RecordToolCall/RecordServerDiagnostic use (see trimBucketToByteBudget):
// the single most-recent record is always kept regardless of its own size,
// so one oversized activity can't cascade into wiping the whole bucket.
// This complements PruneOldActivities/PruneExcessActivities: a
// 7-day/10,000-record cap still permits ~100MB of legitimate stored bytes
// at ~10KB/record, so a byte budget is the cap that actually bounds
// config.db size.
func (m *Manager) PruneActivitiesByBudget(maxBytes int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var deleted int

	err := m.db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ActivityRecordsBucket))
		if bucket == nil {
			return nil
		}

		var err error
		deleted, err = trimBucketToByteBudget(bucket, maxBytes, nil)
		if err != nil {
			return fmt.Errorf("failed to delete over-budget activity: %w", err)
		}
		return nil
	})

	if err != nil {
		return deleted, err
	}

	if deleted > 0 {
		m.logger.Infow("Pruned activity records over byte budget",
			"deleted", deleted,
			"max_bytes", maxBytes)
	}

	return deleted, nil
}

// SaveActivityAsync saves an activity record asynchronously.
// This is non-blocking and suitable for recording tool calls without impacting latency.
func (m *Manager) SaveActivityAsync(record *ActivityRecord) {
	go func() {
		if err := m.SaveActivity(record); err != nil {
			m.logger.Errorw("Failed to save activity record async",
				"id", record.ID,
				"type", record.Type,
				"error", err)
		}
	}()
}

// GetActivityByIDScan performs a full scan to find activity by ID.
// This is less efficient than GetActivity but works when the timestamp is unknown.
func (m *Manager) GetActivityByIDScan(id string) (*ActivityRecord, error) {
	return m.GetActivity(id) // Our GetActivity already does a scan
}

// TruncateActivityResponse is a helper to truncate responses for storage.
func TruncateActivityResponse(response string, maxSize int) (string, bool) {
	return truncateResponse(response, maxSize)
}

// ActivityRecordFromJSON parses an activity record from JSON bytes.
func ActivityRecordFromJSON(data []byte) (*ActivityRecord, error) {
	var record ActivityRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to parse activity record: %w", err)
	}
	return &record, nil
}
