// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package spool keeps serialized OTLP trace requests on disk while the tail
// sampling processor decides, and delivers the kept spans from them.
//
// Each request is appended once to a segment file and registered in a log
// before it is acknowledged. A record associates the request with every trace
// it contains. The processor settles each association as keep or drop; once a
// record has no open association its kept spans are read back and handed to
// the next consumer, after which the record is committed and its segment
// space can be reclaimed.
package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// Config controls the on-disk layout and delivery limits.
type Config struct {
	Directory string
	// SegmentBytes is the size after which a new segment file is started.
	SegmentBytes int64
	// CheckpointBytes is the log size that triggers a compacting checkpoint.
	CheckpointBytes int64
	// SyncOnRotate fsyncs a segment when it is sealed.
	SyncOnRotate bool
	// MaxInflightRecords bounds records handed to the next consumer but not
	// yet committed, which bounds redelivery after a process kill.
	MaxInflightRecords int
	// MaxInflightBytes bounds the stored bytes of those records.
	MaxInflightBytes int64
	// KeepMemo and DropMemo bound the keep and drop decisions restored after
	// a restart.
	KeepMemo int
	DropMemo int

	// MaxUnprocessedMemory bounds the stored bytes of unprocessed records
	// whose decoded fragments are kept in memory; the others are read back
	// when the processor gets to them.
	MaxUnprocessedMemory int64
	// MaxUnprocessedRecords and MaxUnprocessedBytes reject new requests while
	// that many records or stored bytes wait for the processor. Zero means
	// no limit.
	MaxUnprocessedRecords int
	MaxUnprocessedBytes   int64
	// MinFreeDisk rejects new requests while the directory's file system has
	// less free space. Zero disables the check.
	MinFreeDisk int64
	// MinDeadlineRemaining rejects requests whose deadline is closer, as the
	// response may not reach the client in time.
	MinDeadlineRemaining time.Duration
}

// ErrBackpressure is returned by Register while the spool is over one of
// its limits. The request was not registered and may be retried.
var ErrBackpressure = errors.New("tail spool is over its limits")

type assocState uint8

const (
	// unsettled associations have not been processed by the sampler yet.
	unsettled assocState = iota
	// pending associations belong to an undecided trace instance.
	pending
	kept
	dropped
)

// Assoc identifies the spans of one trace within one ResourceSpans of a
// request. Each is settled separately, as the processor may handle them as
// different instances of the trace.
type Assoc struct {
	ID       pcommon.TraceID
	Resource uint32
}

// Record is a registered request.
type Record struct {
	seq     uint64
	seg     *segment
	off     int64
	length  uint32
	arrival int64
	assocs  []Assoc
	state   []assocState
	open    int
	keeps   int
	done    bool
	// recovered marks records registered by a previous run.
	recovered    bool
	readFailures int
	// payload is what the processor passed to Register, kept until the
	// record is processed if it fits in MaxUnprocessedMemory.
	payload any
}

// Seq returns the record's sequence number.
func (r *Record) Seq() uint64 { return r.seq }

// Assocs returns the record's associations.
func (r *Record) Assocs() []Assoc { return r.assocs }

// Arrival returns when the request was registered.
func (r *Record) Arrival() time.Time { return time.Unix(0, r.arrival) }

type assocRef struct {
	rec *Record
	idx int
}

// Spool is safe for concurrent use. Register is called by receivers; Attach,
// Inherit, Decide, Evict and Processed are called by the processor's loop.
type Spool struct {
	cfg    Config
	logger *zap.Logger
	next   consumer.Traces
	// lock keeps another process from opening the same directory.
	lock *os.File

	mu      sync.Mutex
	log     *logWriter
	segs    map[uint32]*segment
	active  *segment
	records map[uint64]*Record
	nextSeq uint64
	wm      uint64
	pending map[pcommon.TraceID][]assocRef
	memo    *memo
	err     error

	// unprocessed holds registered records in sequence order until the
	// processor begins them; wake signals that it is not empty. lastArrival
	// keeps their arrivals in sequence order too.
	unprocessed       []*Record
	lastArrival       int64
	unprocessedBytes  int64
	unprocessedMemory int64
	wake              chan struct{}
	freeDisk          int64
	freeDiskChecked   time.Time

	// checkpointing is set while a checkpoint is written outside the lock;
	// lastSnapshot is the size of the last one written.
	checkpointing bool
	lastSnapshot  int64
	// background tracks checkpoint and segment sync goroutines.
	background sync.WaitGroup

	ready       []*Record
	readyCond   *sync.Cond
	retryTimers map[*time.Timer]struct{}
	stopping    bool
	workers     sync.WaitGroup
	inflight    int64
	stats       Stats

	// readAt reads stored request bytes; tests replace it to inject faults.
	readAt func(seg *segment, buf []byte, off int64) (int, error)
	// snapshotWritten, if set, runs after a checkpoint's snapshot is written
	// and before it replaces the log; tests use it to hold the checkpoint.
	snapshotWritten func()
}

// Stats are cumulative counters exposed for tests and benchmarks.
type Stats struct {
	Registered       uint64
	RegisteredBytes  uint64
	Committed        uint64
	Delivered        uint64
	DeliveredSpans   uint64
	Redelivered      uint64
	DeliveryFailures uint64
	// ReadFailures counts failed reads of stored requests.
	ReadFailures uint64
	// ReleasedUnreadable counts records given up on because they could not
	// be read.
	ReleasedUnreadable      uint64
	ReleasedUnreadableBytes uint64
	// RejectedExpired and RejectedBackpressure count requests refused before
	// they were written.
	RejectedExpired      uint64
	RejectedBackpressure uint64
	// UnprocessedReads counts unprocessed records read back from disk.
	UnprocessedReads    uint64
	Evicted             uint64
	Checkpoints         uint64
	LiveRecords         int
	LiveSegments        int
	PendingTraces       int
	ReadyRecords        int
	RetryingRecords     int
	UnprocessedRecords  int
	UnprocessedBytes    int64
	UnprocessedInMemory int64
}

var errStopped = errors.New("tail spool is stopped")

func (s *Spool) fail(err error) error {
	if s.err == nil && err != nil {
		s.err = err
		s.logger.Error("Tail spool stopped accepting data", zap.Error(err))
	}
	return s.err
}

// Register appends raw and registers its associations, then queues the
// record for the processor with payload, which Next returns. The request may
// be acknowledged once Register returns without error; it returns ctx's error
// without writing anything if ctx is done by the time the record would be
// written.
func (s *Spool) Register(ctx context.Context, raw []byte, assocs []Assoc, arrival time.Time, payload any) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.stopping {
		return nil, errStopped
	}
	if s.active.size > 0 && s.active.size+int64(len(raw)) > s.cfg.SegmentBytes {
		if err := s.rotate(); err != nil {
			return nil, s.fail(err)
		}
	}
	if err := s.admitLocked(ctx, int64(len(raw))); err != nil {
		return nil, err
	}
	seg := s.active
	off := seg.size
	n, err := seg.f.Write(raw)
	seg.size += int64(n)
	if err == nil && n != len(raw) {
		err = fmt.Errorf("short write of %d/%d bytes", n, len(raw))
	}
	if err != nil {
		return nil, s.fail(fmt.Errorf("append to segment %d: %w", seg.id, err))
	}
	s.lastArrival = max(s.lastArrival, arrival.UnixNano())
	r := &Record{
		seq: s.nextSeq, seg: seg, off: off, length: uint32(len(raw)), arrival: s.lastArrival,
		assocs: assocs, state: make([]assocState, len(assocs)), open: len(assocs),
	}
	s.nextSeq++
	if err := s.log.append(&frame{kind: frameRecord, seq: r.seq, seg: seg.id, off: off, length: r.length, arrival: r.arrival, assocs: assocs}); err != nil {
		return nil, s.fail(err)
	}
	if err := s.log.flush(); err != nil {
		return nil, s.fail(err)
	}
	seg.refs++
	s.records[r.seq] = r
	s.stats.Registered++
	s.stats.RegisteredBytes += uint64(len(raw))
	if r.open == 0 {
		s.finishLocked(r)
	} else {
		if payload != nil && s.unprocessedMemory+int64(r.length) <= s.cfg.MaxUnprocessedMemory {
			r.payload = payload
			s.unprocessedMemory += int64(r.length)
		}
		s.unprocessed = append(s.unprocessed, r)
		s.unprocessedBytes += int64(r.length)
		s.pokeLocked()
	}
	s.maybeCheckpointLocked()
	return r, nil
}

// admitLocked refuses a request of n bytes that expired or would exceed a
// limit, before anything is written.
func (s *Spool) admitLocked(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		s.stats.RejectedExpired++
		return err
	}
	if d, ok := ctx.Deadline(); ok && time.Until(d) < s.cfg.MinDeadlineRemaining {
		s.stats.RejectedExpired++
		return fmt.Errorf("%w: less than %v left", context.DeadlineExceeded, s.cfg.MinDeadlineRemaining)
	}
	var reason string
	switch {
	case s.cfg.MaxUnprocessedRecords > 0 && len(s.unprocessed) >= s.cfg.MaxUnprocessedRecords:
		reason = "unprocessed records"
	// A single request larger than the limit is still taken when nothing waits.
	case s.cfg.MaxUnprocessedBytes > 0 && len(s.unprocessed) > 0 && s.unprocessedBytes+n > s.cfg.MaxUnprocessedBytes:
		reason = "unprocessed bytes"
	case s.cfg.MinFreeDisk > 0 && s.freeDiskLocked() < s.cfg.MinFreeDisk:
		reason = "free disk space"
	default:
		return nil
	}
	s.stats.RejectedBackpressure++
	return fmt.Errorf("%w: %s", ErrBackpressure, reason)
}

// freeDiskLocked returns the free space of the directory's file system,
// refreshed at most once a second.
func (s *Spool) freeDiskLocked() int64 {
	if now := time.Now(); now.Sub(s.freeDiskChecked) >= time.Second {
		s.freeDiskChecked = now
		free, err := freeDiskSpace(s.cfg.Directory)
		if err != nil {
			if s.freeDisk >= 0 {
				s.logger.Warn("Failed to check free disk space of the tail spool", zap.Error(err))
			}
			s.freeDisk = -1
		} else {
			s.freeDisk = free
		}
	}
	// An unknown amount does not block requests.
	if s.freeDisk < 0 {
		return s.cfg.MinFreeDisk
	}
	return s.freeDisk
}

// Wake receives a value when records may be waiting for Next.
func (s *Spool) Wake() <-chan struct{} { return s.wake }

// Poke makes Wake ready, so a processor that postponed a record retries it.
func (s *Spool) Poke() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pokeLocked()
}

func (s *Spool) pokeLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Next returns the oldest record not yet begun and the payload passed to
// Register, which is nil if it did not fit in memory. The record stays
// queued until Begin.
func (s *Spool) Next() (*Record, any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.unprocessed) == 0 {
		return nil, nil, false
	}
	r := s.unprocessed[0]
	return r, r.payload, true
}

// WaitingSince reports whether a record that arrived at or before t waits
// for Begin.
func (s *Spool) WaitingSince(t time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.unprocessed) > 0 && s.unprocessed[0].arrival <= t.UnixNano()
}

// ReadUnprocessed decodes a queued record whose payload was not kept. A
// record that does not decode, or still cannot be read after a few
// attempts, is released with its fragments dropped, as it holds back every
// later record; released reports it, and the queue moves on.
func (s *Spool) ReadUnprocessed(r *Record) (td ptrace.Traces, released bool, err error) {
	td, err = s.readRecord(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.stats.UnprocessedReads++
		return td, false, nil
	}
	s.stats.ReadFailures++
	r.readFailures++
	if !errors.Is(err, errUndecodable) && r.readFailures < unprocessedReadAttempts {
		return td, false, err
	}
	s.dequeueLocked(r)
	s.releaseUnreadableLocked(r, err)
	s.wm = max(s.wm, r.seq)
	return td, true, err
}

// Attach adds the record's association i to the trace's pending instance.
func (s *Spool) Attach(r *Record, i int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.state[i] != unsettled {
		return
	}
	r.state[i] = pending
	id := r.assocs[i].ID
	s.pending[id] = append(s.pending[id], assocRef{rec: r, idx: i})
}

// Inherit settles the record's association i by a decision made earlier for
// the trace.
func (s *Spool) Inherit(r *Record, i int, keep bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.state[i] != unsettled {
		return
	}
	s.appendLocked(&frame{kind: frameLate, seq: r.seq, idx: uint32(i), keep: keep})
	s.settleLocked(r, i, keep)
}

// Decide settles every association of the trace's pending instance.
func (s *Spool) Decide(id pcommon.TraceID, keep bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kind := byte(frameDrop)
	if keep {
		kind = frameKeep
	}
	s.logBeyondWatermarkLocked(id, keep)
	s.appendLocked(&frame{kind: kind, seq: s.wm, id: id})
	s.memo.put(id, keep)
	s.settlePendingLocked(id, keep)
}

// Evict ends the trace's instance without a decision; its pending spans are
// dropped, as the in-memory processor does when it removes a trace for
// capacity, and a decision it still held is forgotten.
func (s *Spool) Evict(id pcommon.TraceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logBeyondWatermarkLocked(id, false)
	s.appendLocked(&frame{kind: frameEvict, seq: s.wm, id: id})
	s.memo.remove(id)
	s.stats.Evicted++
	s.settlePendingLocked(id, false)
}

// logBeyondWatermarkLocked logs the settlement of associations attached in
// the record being processed, which replay does not attach by watermark.
func (s *Spool) logBeyondWatermarkLocked(id pcommon.TraceID, keep bool) {
	for _, ref := range s.pending[id] {
		if ref.rec.seq > s.wm {
			s.appendLocked(&frame{kind: frameLate, seq: ref.rec.seq, idx: uint32(ref.idx), keep: keep})
		}
	}
}

// Begin marks r as the record whose fragments the processor is handling.
// Records must be processed in sequence order. Decision frames carry the
// watermark of fully processed records, and replay attaches their unsettled
// associations to the pending instance of each trace.
func (s *Spool) Begin(r *Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wm = max(s.wm, r.seq-1)
	s.dequeueLocked(r)
}

// dequeueLocked removes r from the head of the unprocessed queue.
func (s *Spool) dequeueLocked(r *Record) {
	if len(s.unprocessed) == 0 || s.unprocessed[0] != r {
		return
	}
	s.unprocessed[0] = nil
	s.unprocessed = s.unprocessed[1:]
	s.unprocessedBytes -= int64(r.length)
	if r.payload != nil {
		r.payload = nil
		s.unprocessedMemory -= int64(r.length)
	}
	if len(s.unprocessed) > 0 {
		s.pokeLocked()
	}
}

// End marks every fragment of r as processed.
func (s *Spool) End(r *Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wm = max(s.wm, r.seq)
}

// Flush writes buffered log frames to the file.
func (s *Spool) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.flush(); err != nil {
		s.fail(err)
	}
	s.maybeCheckpointLocked()
}

// Stats returns a snapshot of the counters.
func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.LiveRecords = len(s.records)
	st.LiveSegments = len(s.segs)
	st.PendingTraces = len(s.pending)
	st.ReadyRecords = len(s.ready)
	st.RetryingRecords = len(s.retryTimers)
	st.UnprocessedRecords = len(s.unprocessed)
	st.UnprocessedBytes = s.unprocessedBytes
	st.UnprocessedInMemory = s.unprocessedMemory
	return st
}

func (s *Spool) appendLocked(f *frame) {
	// During replay the state is rebuilt from the log and needs no frames.
	if s.err != nil || s.log == nil {
		return
	}
	if err := s.log.append(f); err != nil {
		s.fail(err)
	}
}

func (s *Spool) settlePendingLocked(id pcommon.TraceID, keep bool) {
	refs := s.pending[id]
	delete(s.pending, id)
	for _, ref := range refs {
		s.settleLocked(ref.rec, ref.idx, keep)
	}
}

func (s *Spool) settleLocked(r *Record, i int, keep bool) {
	if r.state[i] == kept || r.state[i] == dropped {
		return
	}
	if keep {
		r.state[i] = kept
		r.keeps++
	} else {
		r.state[i] = dropped
	}
	r.open--
	if r.open == 0 {
		s.finishLocked(r)
	}
}

// finishLocked queues a record with kept spans for delivery, or commits it.
func (s *Spool) finishLocked(r *Record) {
	if r.keeps > 0 {
		s.ready = append(s.ready, r)
		s.readyCond.Signal()
		return
	}
	s.appendLocked(&frame{kind: frameCommit, seq: r.seq})
	s.releaseLocked(r)
}

func (s *Spool) releaseLocked(r *Record) {
	if r.done {
		return
	}
	r.done = true
	delete(s.records, r.seq)
	s.stats.Committed++
	r.seg.refs--
	if r.seg.refs == 0 && r.seg != s.active && s.log != nil {
		s.removeSegmentLocked(r.seg)
	}
}

// memo keeps the most recent keep and drop decisions in decision order, each
// bounded separately like the processor's two decision caches.
type memo struct {
	capacity [2]int
	order    [2]*list.List
	byID     map[pcommon.TraceID]*list.Element
	next     uint64
}

type memoEntry struct {
	ID   pcommon.TraceID
	Keep bool
	seq  uint64
}

func side(keep bool) int {
	if keep {
		return 1
	}
	return 0
}

func newMemo(keepCapacity, dropCapacity int) *memo {
	return &memo{
		capacity: [2]int{dropCapacity, keepCapacity},
		order:    [2]*list.List{list.New(), list.New()},
		byID:     map[pcommon.TraceID]*list.Element{},
	}
}

func (m *memo) put(id pcommon.TraceID, keep bool) {
	m.remove(id)
	i := side(keep)
	if m.capacity[i] <= 0 {
		return
	}
	order := m.order[i]
	m.next++
	m.byID[id] = order.PushBack(memoEntry{ID: id, Keep: keep, seq: m.next})
	for order.Len() > m.capacity[i] {
		front := order.Front()
		delete(m.byID, front.Value.(memoEntry).ID)
		order.Remove(front)
	}
}

func (m *memo) remove(id pcommon.TraceID) {
	if e, ok := m.byID[id]; ok {
		delete(m.byID, id)
		m.order[side(e.Value.(memoEntry).Keep)].Remove(e)
	}
}

// entries returns the remembered decisions, oldest first.
func (m *memo) entries() []memoEntry {
	out := make([]memoEntry, 0, len(m.byID))
	drops, keeps := m.order[0].Front(), m.order[1].Front()
	for drops != nil || keeps != nil {
		e := &drops
		if drops == nil || (keeps != nil && keeps.Value.(memoEntry).seq < drops.Value.(memoEntry).seq) {
			e = &keeps
		}
		out = append(out, (*e).Value.(memoEntry))
		*e = (*e).Next()
	}
	return out
}

func (s *Spool) sortedRecordsLocked() []*Record {
	recs := make([]*Record, 0, len(s.records))
	for _, r := range s.records {
		recs = append(recs, r)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].seq < recs[j].seq })
	return recs
}

func logPath(dir string) string { return filepath.Join(dir, "spool.log") }

func ensureDir(dir string) error {
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0o750); err != nil {
		return fmt.Errorf("create tail spool directory: %w", err)
	}
	return nil
}
