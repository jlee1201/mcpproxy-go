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
	live int   // live (not deleted/pruned) records in the segment
	size int64 // bytes on disk
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
	for i, seg := range segs {
		last := i == len(segs)-1
		if err := l.scanSegment(seg, last); err != nil {
			l.logger.Warnw("Skipping unreadable history segment", "segment", l.segPath(seg), "error", err)
		}
		if seg >= l.nextSeg {
			l.nextSeg = seg + 1
		}
	}
	// Segments whose every record was tombstoned or pruned carry no value.
	for seg, info := range l.segs {
		if info.live == 0 && (len(segs) == 0 || seg != segs[len(segs)-1]) {
			_ = os.Remove(l.segPath(seg))
			delete(l.segs, seg)
		}
	}
	l.unsorted = true
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if info := l.segs[last]; info != nil && info.size < l.segMax {
			f, err := os.OpenFile(l.segPath(last), os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				l.active, l.activeSeg, l.activeSize = f, last, info.size
			}
		}
	}
	return nil
}

func (l *recordLog[T]) scanSegment(seg uint64, last bool) error {
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
			if last {
				_ = os.Truncate(path, off)
			}
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
	l.liveBytes += int64(n)
	l.groupSize[group] += int64(n)
}

// removeLocked drops an entry from the index and deletes its segment file
// once nothing live remains in it (never the segment being appended to).
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
		if info.live <= 0 && (l.active == nil || e.seg != l.activeSeg) {
			_ = os.Remove(l.segPath(e.seg))
			delete(l.segs, e.seg)
		}
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

// append writes one already-marshaled record. data must contain no newline.
func (l *recordLog[T]) append(rec *T, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("history log is closed")
	}
	need := int64(len(data)) + 1
	if l.active != nil && l.activeSize > 0 && l.activeSize+need > l.segMax {
		l.retireActiveLocked()
	}
	if err := l.ensureActiveLocked(); err != nil {
		return err
	}
	buf := make([]byte, 0, need)
	buf = append(buf, data...)
	buf = append(buf, '\n')
	if _, err := l.active.Write(buf); err != nil {
		// A short write may have left a partial line; abandon the segment
		// so later records start on a clean line in a new file.
		l.retireActiveLocked()
		return fmt.Errorf("write history record: %w", err)
	}
	off := l.activeSize
	l.activeSize += need
	l.segs[l.activeSeg].size = l.activeSize
	l.addLocked(rec, l.activeSeg, off, len(data), nil)
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

// retireActiveLocked stops appending to the current segment, deleting it if
// nothing live remains in it.
func (l *recordLog[T]) retireActiveLocked() {
	if l.active == nil {
		return
	}
	_ = l.active.Close()
	l.active = nil
	if info := l.segs[l.activeSeg]; info != nil && info.live <= 0 {
		_ = os.Remove(l.segPath(l.activeSeg))
		delete(l.segs, l.activeSeg)
	}
}

// tombstone deletes a record by ID, durably across restarts.
func (l *recordLog[T]) tombstone(id string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.byID[id]
	if e == nil {
		return false, nil
	}
	l.removeLocked(e)
	if err := l.ensureActiveLocked(); err != nil {
		return true, err
	}
	if _, err := l.active.Write([]byte("-" + id + "\n")); err != nil {
		return true, fmt.Errorf("write tombstone: %w", err)
	}
	l.activeSize += int64(len(id)) + 2
	if info := l.segs[l.activeSeg]; info != nil {
		info.size = l.activeSize
	}
	return true, nil
}

// snapshot returns copies of the index rows matching match, newest first.
func (l *recordLog[T]) snapshot(match func(*T) bool) []logEntry[T] {
	l.mu.Lock()
	if l.needsSortLocked() {
		l.sortedLocked()
	}
	l.mu.Unlock()

	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []logEntry[T]
	for i := len(l.entries) - 1; i >= 0; i-- {
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

// load reads the full record for an index row from disk.
func (l *recordLog[T]) load(e logEntry[T]) (*T, error) {
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
