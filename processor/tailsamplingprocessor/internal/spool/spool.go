// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package spool keeps raw OTLP trace requests on disk while the tail sampling
// processor decides, and delivers the kept spans from the original bytes.
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	// MaxInflightBytes bounds the raw bytes of those records.
	MaxInflightBytes int64
	// KeepMemo and DropMemo bound the keep and drop decisions restored after
	// a restart.
	KeepMemo int
	DropMemo int
}

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

// Record is a registered raw request.
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

	ready     []*Record
	readyCond *sync.Cond
	stopping  bool
	workers   sync.WaitGroup
	inflight  int64
	stats     Stats
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
	Evicted          uint64
	Checkpoints      uint64
	LiveRecords      int
	LiveSegments     int
	PendingTraces    int
	ReadyRecords     int
}

var errStopped = errors.New("tail spool is stopped")

func (s *Spool) fail(err error) error {
	if s.err == nil && err != nil {
		s.err = err
		s.logger.Error("Tail spool stopped accepting data", zap.Error(err))
	}
	return s.err
}

// Register appends raw and registers its associations. The request may be
// acknowledged once Register returns without error.
func (s *Spool) Register(raw []byte, assocs []Assoc, arrival time.Time) (*Record, error) {
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
	r := &Record{
		seq: s.nextSeq, seg: seg, off: off, length: uint32(len(raw)), arrival: arrival.UnixNano(),
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
	}
	s.maybeCheckpointLocked()
	return r, nil
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
