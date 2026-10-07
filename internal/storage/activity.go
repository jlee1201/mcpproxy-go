package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// DefaultMaxResponseSize is the default maximum size for response truncation (64KB)
const DefaultMaxResponseSize = 64 * 1024

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
// The byte-budget trim (SaveActivity's on-write pass and
// PruneActivitiesByBudget) never evicts the just-written record (see its doc comment), so without this cap one
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

// activityLogSpec indexes ActivityRecords for the history log. The lightened
// copy keeps everything ActivityFilter.Matches reads (including Metadata for
// the intent filter) and drops the payloads that make records large.
func activityLogSpec() recordLogSpec[ActivityRecord] {
	return recordLogSpec[ActivityRecord]{
		prefix: "activity",
		identify: func(r *ActivityRecord) (string, time.Time, string) {
			return r.ID, r.Timestamp, ""
		},
		lighten: func(r *ActivityRecord) *ActivityRecord {
			light := *r
			light.Arguments = nil
			light.Response = ""
			return &light
		},
	}
}

// SaveActivity appends an activity record to the history log. History lives in
// rotated files under <data-dir>/history, not config.db: it is high-volume,
// disposable, and bbolt never returns freed pages to the filesystem.
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

	data, err := truncateActivityRecordToFit(record, MaxActivityRecordBytes)
	if err != nil {
		return fmt.Errorf("failed to marshal activity record: %w", err)
	}
	// The stored form may have lost fields to the per-record cap; index that.
	stored, err := ActivityRecordFromJSON(data)
	if err != nil {
		return err
	}
	if err := m.activityLog.append(stored, data); err != nil {
		return fmt.Errorf("failed to store activity record: %w", err)
	}

	if max := m.activityMaxBytes.Load(); max > 0 && m.activityLog.bytesLive() > max {
		m.activityLog.trimToBudget("", true, int64(float64(max)*activityBudgetTrimTarget), record.ID)
	}
	return nil
}

// SetActivityByteBudget sets the history byte budget SaveActivity enforces on
// every write (0 disables it).
func (m *Manager) SetActivityByteBudget(maxBytes int64) {
	m.activityMaxBytes.Store(maxBytes)
}

// activityBudgetTrimTarget is the fraction of the budget a write-path trim
// cuts down to, so a log sitting at the budget doesn't trim on every write.
const activityBudgetTrimTarget = 0.9

// GetActivity retrieves an activity record by ID.
// Returns nil if the record is not found.
func (m *Manager) GetActivity(id string) (*ActivityRecord, error) {
	if id == "" {
		return nil, fmt.Errorf("activity ID cannot be empty")
	}
	e, ok := m.activityLog.lookup(id)
	if !ok {
		return nil, nil
	}
	record, err := m.activityLog.load(e)
	if errors.Is(err, errRecordGone) {
		return nil, nil
	}
	return record, err
}

// ListActivities returns paginated activity records matching the filter.
// Records are returned in reverse chronological order (newest first).
// Returns the records, total matching count, and any error.
func (m *Manager) ListActivities(filter ActivityFilter) ([]*ActivityRecord, int, error) {
	filter.Validate()

	matches := m.activityLog.snapshot(func(r *ActivityRecord) bool { return filter.Matches(r) })
	total := len(matches)
	if filter.Offset >= total {
		return nil, total, nil
	}
	page := matches[filter.Offset:]
	if len(page) > filter.Limit {
		page = page[:filter.Limit]
	}

	records := make([]*ActivityRecord, 0, len(page))
	for _, e := range page {
		record, err := m.activityLog.load(e)
		if errors.Is(err, errRecordGone) {
			continue
		}
		if err != nil {
			m.logger.Warnw("Failed to load activity record", "id", e.id, "error", err)
			continue
		}
		records = append(records, record)
	}
	return records, total, nil
}

// DeleteActivity deletes an activity record by ID.
// Returns nil if the record doesn't exist.
func (m *Manager) DeleteActivity(id string) error {
	if id == "" {
		return fmt.Errorf("activity ID cannot be empty")
	}
	_, err := m.activityLog.tombstone(id)
	return err
}

// CountActivities returns the total number of activity records.
func (m *Manager) CountActivities() (int, error) {
	return m.activityLog.count(), nil
}

// StreamActivities returns a channel that yields activity records matching the filter.
// The channel is closed when all matching records have been sent or ctx is done.
// This is useful for streaming large exports without loading all records into memory.
// It holds no storage lock while sending, so a consumer that stops reading
// only needs to cancel ctx to release the producer goroutine.
func (m *Manager) StreamActivities(ctx context.Context, filter ActivityFilter) <-chan *ActivityRecord {
	filter.Validate()
	ch := make(chan *ActivityRecord, 100)

	go func() {
		defer close(ch)

		matches := m.activityLog.snapshot(func(r *ActivityRecord) bool { return filter.Matches(r) })
		for _, e := range matches {
			if ctx.Err() != nil {
				return
			}
			record, err := m.activityLog.load(e)
			if err != nil {
				continue
			}
			select {
			case ch <- record:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// PruneOldActivities deletes activity records older than the specified duration.
// Returns the number of records deleted.
func (m *Manager) PruneOldActivities(maxAge time.Duration) (int, error) {
	deleted := m.activityLog.pruneOlderThan(time.Now().UTC().Add(-maxAge))
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
	deleted := m.activityLog.pruneExcess(maxRecords, targetPercent)
	if deleted > 0 {
		m.logger.Infow("Pruned excess activity records",
			"deleted", deleted,
			"max_records", maxRecords)
	}
	return deleted, nil
}

// PruneActivitiesByBudget deletes the oldest activity records once the total
// record bytes exceed maxBytes. The single most-recent record is always kept
// regardless of its own size, so one oversized activity can't cascade into
// wiping the whole log. This complements PruneOldActivities/
// PruneExcessActivities: a 7-day/10,000-record cap still permits ~100MB of
// legitimate stored bytes at ~10KB/record, so the byte budget is the cap that
// actually bounds disk use.
func (m *Manager) PruneActivitiesByBudget(maxBytes int64) (int, error) {
	deleted := m.activityLog.trimToBudget("", true, maxBytes, "")
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

// GetActivityByIDScan finds an activity by ID.
func (m *Manager) GetActivityByIDScan(id string) (*ActivityRecord, error) {
	return m.GetActivity(id)
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
