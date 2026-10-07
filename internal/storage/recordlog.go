package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// DefaultLogSegmentBytes is the size at which an append-only history segment
// is closed and a new one started. Retention deletes whole segments once
// every record in them has aged out, so smaller segments reclaim disk sooner.
const DefaultLogSegmentBytes = 4 * 1024 * 1024

// errRecordGone is returned by recordLog.load when the segment holding a
// record was deleted (retention) between listing and loading it.
var errRecordGone = errors.New("record no longer available")

// logEntry is the in-memory index row for one record in a segment file. It
// carries a lightened copy of the record (heavy fields such as arguments and
// responses dropped) so list/filter queries never touch disk.
type logEntry[T any] struct {
	id    string
	ts    time.Time
	group string // retention group (tool calls: server ID); "" for activity
	seg   uint64
	off   int64
	n     int // record bytes, excluding the trailing newline
	light *T
	dead  bool
}

// recordLogSpec tells a recordLog how to index a record type.
type recordLogSpec[T any] struct {
	prefix   string // segment file prefix, e.g. "activity"
	identify func(*T) (id string, ts time.Time, group string)
	lighten  func(*T) *T // copy of the record without arguments/responses
}

type segInfo struct {
	live      int   // live (not deleted/pruned) records in the segment
	liveBytes int64 // bytes of the live records
	tombs     int   // tombstone lines written into this segment
	size      int64 // bytes on disk
}

// recordLog is an append-only, size-rotated JSONL store with an in-memory
// index. It replaces storing request/response history in config.db: history is
// high-volume and disposable, config.db's copy-on-write file never shrinks.
//
// Retention is logical first (entries leave the index) and physical second (a
// segment file is removed once it has no live records). A record pruned but
// still sitting in a partly-live segment reappears after a restart until the
// next retention sweep, which runs at startup. Writes are not fsynced: this is
// diagnostic history, not source of truth.
//
// Line format: a JSON record per line, or "-<id>" as a delete tombstone.
type recordLog[T any] struct {
	mu     sync.RWMutex
	dir    string
	spec   recordLogSpec[T]
	segMax int64
	logger *zap.SugaredLogger

	entries   []*logEntry[T] // append order; sorted by ts on demand
	byID      map[string]*logEntry[T]
	segs      map[uint64]*segInfo
	deadN     int
	unsorted  bool
	liveBytes int64
	groupSize map[string]int64

	active     *os.File
	activeSeg  uint64
	activeSize int64
	nextSeg    uint64
	closed     bool
	loading    bool // index is being rebuilt from disk; never delete files meanwhile
}

func openRecordLog[T any](dir string, spec recordLogSpec[T], segMax int64, logger *zap.SugaredLogger) (*recordLog[T], error) {
	if segMax <= 0 {
		segMax = DefaultLogSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create history dir: %w", err)
	}
	l := &recordLog[T]{
		dir:       dir,
		spec:      spec,
		segMax:    segMax,
		logger:    logger,
		byID:      make(map[string]*logEntry[T]),
		segs:      make(map[uint64]*segInfo),
		groupSize: make(map[string]int64),
		nextSeg:   1,
	}
	if err := l.loadExisting(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *recordLog[T]) segPath(seg uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%s-%012d.jsonl", l.spec.prefix, seg))
}

func (l *recordLog[T]) listSegments() ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(l.dir, l.spec.prefix+"-*.jsonl"))
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, m := range matches {
		base := strings.TrimSuffix(filepath.Base(m), ".jsonl")
		num := strings.TrimPrefix(base, l.spec.prefix+"-")
		n, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, n)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

// loadExisting rebuilds the index from the segment files.
func (l *recordLog[T]) loadExisting() error {
	segs, err := l.listSegments()
	if err != nil {
		return fmt.Errorf("list history segments: %w", err)
	}
	// Segment files are never deleted mid-scan: a segment's live count can dip
	// to zero before a later line in the same file adds a record back.
	l.loading = true
	scanned := make(map[uint64]bool, len(segs))
	for _, seg := range segs {
		if err := l.scanSegment(seg); err != nil {
			l.logger.Warnw("Skipping unreadable history segment", "segment", l.segPath(seg), "error", err)
		} else {
			scanned[seg] = true
		}
		if seg >= l.nextSeg {
			l.nextSeg = seg + 1
		}
	}
	l.loading = false
	l.unsorted = true
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if info := l.segs[last]; scanned[last] && info != nil && info.size < l.segMax {
			f, err := os.OpenFile(l.segPath(last), os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				l.active, l.activeSeg, l.activeSize = f, last, info.size
			}
		}
	}
	// Drop segments that carry nothing worth keeping.
	l.gcLocked()
	return nil
}

func (l *recordLog[T]) scanSegment(seg uint64) error {
	path := l.segPath(seg)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info := &segInfo{}
	l.segs[seg] = info
	r := bufio.NewReaderSize(f, 256*1024)
	var off int64
	var bad int
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			l.indexLine(seg, off, bytes.TrimRight(line, "\n"), info, &bad)
			off += int64(len(line))
		} else if len(line) > 0 {
			// Partial trailing line from a crash mid-write. Drop it from the
			// active segment so the next append starts on a clean line.
			_ = os.Truncate(path, off)
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	info.size = off
	if bad > 0 {
		l.logger.Warnw("Ignored corrupt history lines", "segment", path, "lines", bad)
	}
	return nil
}

func (l *recordLog[T]) indexLine(seg uint64, off int64, line []byte, info *segInfo, bad *int) {
	if len(line) == 0 {
		return
	}
	if line[0] == '-' {
		info.tombs++
		if e := l.byID[string(line[1:])]; e != nil {
			l.removeLocked(e)
		}
		return
	}
	var rec T
	if err := json.Unmarshal(line, &rec); err != nil {
		*bad++
		return
	}
	l.addLocked(&rec, seg, off, len(line), info)
}

// addLocked indexes rec, replacing any live entry with the same ID.
func (l *recordLog[T]) addLocked(rec *T, seg uint64, off int64, n int, info *segInfo) {
	id, ts, group := l.spec.identify(rec)
	if id == "" {
		return
	}
	if old := l.byID[id]; old != nil {
		l.removeLocked(old)
	}
	e := &logEntry[T]{id: id, ts: ts, group: group, seg: seg, off: off, n: n, light: l.spec.lighten(rec)}
	if len(l.entries) > 0 && ts.Before(l.entries[len(l.entries)-1].ts) {
		l.unsorted = true
	}
	l.entries = append(l.entries, e)
	l.byID[id] = e
	if info == nil {
		info = l.segs[seg]
	}
	info.live++
	info.liveBytes += int64(n)
	l.liveBytes += int64(n)
	l.groupSize[group] += int64(n)
}

// removeLocked drops an entry from the index and garbage-collects segment
// files that no longer hold anything worth keeping.
func (l *recordLog[T]) removeLocked(e *logEntry[T]) {
	if e.dead {
		return
	}
	e.dead = true
	l.deadN++
	delete(l.byID, e.id)
	l.liveBytes -= int64(e.n)
	l.groupSize[e.group] -= int64(e.n)
	if l.groupSize[e.group] <= 0 {
		delete(l.groupSize, e.group)
	}
	if info := l.segs[e.seg]; info != nil {
		info.live--
		info.liveBytes -= int64(e.n)
		if info.live <= 0 {
			l.gcLocked()
		}
	}
}

// gcLocked deletes segment files with no live records, except the one being
// appended to. A segment holding tombstones is kept until no older segment
// remains: the tombstone may be the only thing stopping a deleted record in an
// older file from reappearing after a restart.
func (l *recordLog[T]) gcLocked() {
	if l.loading {
		return
	}
	order := make([]uint64, 0, len(l.segs))
	for seg := range l.segs {
		order = append(order, seg)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	olderRetained := false
	for _, seg := range order {
		info := l.segs[seg]
		isActive := l.active != nil && seg == l.activeSeg
		if info.live <= 0 && !isActive && (info.tombs == 0 || !olderRetained) {
			_ = os.Remove(l.segPath(seg))
			delete(l.segs, seg)
			continue
		}
		olderRetained = true
	}
}

// sortedLocked orders entries oldest to newest and drops dead ones. Caller
// holds the write lock.
func (l *recordLog[T]) sortedLocked() {
	if !l.needsSortLocked() {
		return
	}
	live := l.entries[:0]
	for _, e := range l.entries {
		if !e.dead {
			live = append(live, e)
		}
	}
	for i := len(live); i < len(l.entries); i++ {
		l.entries[i] = nil
	}
	l.entries = live
	l.deadN = 0
	sort.SliceStable(l.entries, func(i, j int) bool { return l.entries[i].ts.Before(l.entries[j].ts) })
	l.unsorted = false
}

func (l *recordLog[T]) needsSortLocked() bool {
	return l.unsorted || (l.deadN >= 256 && l.deadN*2 >= len(l.entries))
}

// writeLineLocked appends one line (data has no newline) to the active
// segment, rotating first when it would overflow, and returns where it landed.
func (l *recordLog[T]) writeLineLocked(data []byte) (seg uint64, off int64, err error) {
	need := int64(len(data)) + 1
	if l.active != nil && l.activeSize > 0 && l.activeSize+need > l.segMax {
		l.retireActiveLocked()
	}
	if err = l.ensureActiveLocked(); err != nil {
		return 0, 0, err
	}
	buf := make([]byte, 0, need)
	buf = append(buf, data...)
	buf = append(buf, '\n')
	if _, err = l.active.Write(buf); err != nil {
		// A short write may have left a partial line; abandon the segment
		// so later records start on a clean line in a new file.
		l.retireActiveLocked()
		return 0, 0, fmt.Errorf("write history record: %w", err)
	}
	off = l.activeSize
	l.activeSize += need
	l.segs[l.activeSeg].size = l.activeSize
	return l.activeSeg, off, nil
}

// append writes one already-marshaled record. data must contain no newline.
func (l *recordLog[T]) append(rec *T, data []byte) error {
	if id, _, _ := l.spec.identify(rec); id == "" {
		return errors.New("history record has no ID")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("history log is closed")
	}
	seg, off, err := l.writeLineLocked(data)
	if err != nil {
		return err
	}
	l.addLocked(rec, seg, off, len(data), nil)
	return nil
}

// ensureActiveLocked opens a fresh segment when nothing is being appended to.
func (l *recordLog[T]) ensureActiveLocked() error {
	if l.active != nil {
		return nil
	}
	seg := l.nextSeg
	f, err := os.OpenFile(l.segPath(seg), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open history segment: %w", err)
	}
	l.nextSeg = seg + 1
	l.active, l.activeSeg, l.activeSize = f, seg, 0
	l.segs[seg] = &segInfo{}
	return nil
}

// retireActiveLocked stops appending to the current segment; it is then
// subject to garbage collection like any other.
func (l *recordLog[T]) retireActiveLocked() {
	if l.active == nil {
		return
	}
	_ = l.active.Close()
	l.active = nil
	l.gcLocked()
}

// tombstone deletes a record by ID, durably across restarts.
func (l *recordLog[T]) tombstone(id string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.byID[id]
	if e == nil {
		return false, nil
	}
	// Write the tombstone before removing the entry: removal may delete the
	// record's segment, and the line must land in a segment that outlives it.
	seg, _, err := l.writeLineLocked([]byte("-" + id))
	if err == nil {
		l.segs[seg].tombs++
	}
	l.removeLocked(e)
	if err != nil {
		return true, fmt.Errorf("write tombstone: %w", err)
	}
	return true, nil
}

// snapshot returns copies of the index rows matching match, newest first.
func (l *recordLog[T]) snapshot(match func(*T) bool) []logEntry[T] {
	return l.snapshotLimit(match, -1)
}

// snapshotLimit is snapshot capped at limit rows (limit < 0 means no cap).
func (l *recordLog[T]) snapshotLimit(match func(*T) bool, limit int) []logEntry[T] {
	for {
		l.mu.RLock()
		if !l.needsSortLocked() {
			break
		}
		l.mu.RUnlock()
		l.mu.Lock()
		l.sortedLocked()
		l.mu.Unlock()
	}
	defer l.mu.RUnlock()
	var out []logEntry[T]
	for i := len(l.entries) - 1; i >= 0; i-- {
		if limit >= 0 && len(out) >= limit {
			break
		}
		e := l.entries[i]
		if e.dead || (match != nil && !match(e.light)) {
			continue
		}
		out = append(out, *e)
	}
	return out
}

// lookup returns the index row for id.
func (l *recordLog[T]) lookup(id string) (logEntry[T], bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e := l.byID[id]
	if e == nil {
		return logEntry[T]{}, false
	}
	return *e, true
}

// load reads the full record for an index row from disk. If the row went stale
// because compaction moved the record, it follows the record to its new home.
func (l *recordLog[T]) load(e logEntry[T]) (*T, error) {
	rec, err := l.loadAt(e)
	if errors.Is(err, errRecordGone) {
		if cur, ok := l.lookup(e.id); ok && (cur.seg != e.seg || cur.off != e.off) {
			return l.loadAt(cur)
		}
	}
	return rec, err
}

func (l *recordLog[T]) loadAt(e logEntry[T]) (*T, error) {
	f, err := os.Open(l.segPath(e.seg))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errRecordGone
		}
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, e.n)
	if _, err := f.ReadAt(buf, e.off); err != nil {
		return nil, fmt.Errorf("read history record %s: %w", e.id, err)
	}
	var rec T
	if err := json.Unmarshal(buf, &rec); err != nil {
		return nil, fmt.Errorf("decode history record %s: %w", e.id, err)
	}
	return &rec, nil
}

func (l *recordLog[T]) count() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.byID)
}

func (l *recordLog[T]) bytesLive() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.liveBytes
}

func (l *recordLog[T]) groupBytes(group string) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.groupSize[group]
}

// groups lists the retention groups that currently hold records.
func (l *recordLog[T]) groups() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]string, 0, len(l.groupSize))
	for g := range l.groupSize {
		out = append(out, g)
	}
	return out
}

// compactSparse rewrites mostly-dead segments. Segments are only deleted when
// nothing live remains, so one long-lived record (say, a quiet server's) would
// otherwise pin a whole segment of otherwise-dead data indefinitely. Live
// records in segments that are under half live are copied to the active
// segment and the old file is removed. Segments holding tombstones are left
// alone. Returns the number of segments reclaimed.
func (l *recordLog[T]) compactSparse() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0
	}
	cands := make(map[uint64][]*logEntry[T])
	for seg, info := range l.segs {
		if l.active != nil && seg == l.activeSeg {
			continue
		}
		if info.tombs == 0 && info.live > 0 && info.liveBytes*2 < info.size {
			cands[seg] = nil
		}
	}
	if len(cands) == 0 {
		return 0
	}
	for _, e := range l.entries {
		if _, ok := cands[e.seg]; ok && !e.dead {
			cands[e.seg] = append(cands[e.seg], e)
		}
	}
	reclaimed := 0
	for seg, entries := range cands {
		src, err := os.Open(l.segPath(seg))
		if err != nil {
			continue
		}
		moved := 0
		for _, e := range entries {
			buf := make([]byte, e.n)
			if _, err := src.ReadAt(buf, e.off); err != nil {
				break
			}
			newSeg, newOff, err := l.writeLineLocked(buf)
			if err != nil {
				break
			}
			from := l.segs[seg]
			from.live--
			from.liveBytes -= int64(e.n)
			to := l.segs[newSeg]
			to.live++
			to.liveBytes += int64(e.n)
			e.seg, e.off = newSeg, newOff
			moved++
		}
		_ = src.Close()
		if moved == len(entries) {
			reclaimed++
		}
	}
	l.gcLocked()
	return reclaimed
}

// pruneOlderThan removes records with a timestamp before cutoff.
func (l *recordLog[T]) pruneOlderThan(cutoff time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if !e.dead && e.ts.Before(cutoff) {
			l.removeLocked(e)
			n++
		}
	}
	return n
}

// pruneExcess removes the oldest records until count is at most
// int(max*targetPercent), when the count exceeds max.
func (l *recordLog[T]) pruneExcess(max int, targetPercent float64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := len(l.byID)
	if count <= max {
		return 0
	}
	l.sortedLocked()
	toDelete := count - int(float64(max)*targetPercent)
	n := 0
	for _, e := range l.entries {
		if n >= toDelete {
			break
		}
		if !e.dead {
			l.removeLocked(e)
			n++
		}
	}
	return n
}

// trimGroupToBudget removes the oldest records of group (all groups when
// all is true) until at most maxBytes remain. The newest record and protectID
// are always kept, so one oversized record cannot evict itself and then
// cascade into evicting everything older. Returns records removed.
func (l *recordLog[T]) trimToBudget(group string, all bool, maxBytes int64, protectID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sortedLocked()
	var cumulative int64
	var drop []*logEntry[T]
	first := true
	for i := len(l.entries) - 1; i >= 0; i-- {
		e := l.entries[i]
		if e.dead || (!all && e.group != group) {
			continue
		}
		if first || e.id == protectID {
			first = false
			continue
		}
		cumulative += int64(e.n)
		if cumulative > maxBytes {
			drop = append(drop, e)
		}
	}
	for _, e := range drop {
		l.removeLocked(e)
	}
	return len(drop)
}

// dropGroup removes every record in group.
func (l *recordLog[T]) dropGroup(group string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if !e.dead && e.group == group {
			l.removeLocked(e)
			n++
		}
	}
	return n
}

func (l *recordLog[T]) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.active != nil {
		err := l.active.Close()
		l.active = nil
		return err
	}
	return nil
}
