// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type segment struct {
	id uint32
	f  *os.File
	// size includes the space reserved by writes still in progress.
	size int64
	refs int
	// writers counts Register calls writing into the segment outside the
	// lock. A sealed segment is synced and removed only once they are done.
	writers  int
	needSync bool
	syncing  bool
}

func readSegment(seg *segment, buf []byte, off int64) (int, error) {
	if seg.f == nil {
		return 0, fmt.Errorf("segment %d is missing", seg.id)
	}
	return seg.f.ReadAt(buf, off)
}

func writeSegment(seg *segment, buf []byte, off int64) (int, error) {
	return seg.f.WriteAt(buf, off)
}

func segmentPath(dir string, id uint32) string {
	return filepath.Join(dir, "segments", fmt.Sprintf("%08x.otlp", id))
}

func (s *Spool) openSegmentLocked(id uint32) (*segment, error) {
	f, err := os.OpenFile(segmentPath(s.cfg.Directory, id), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	seg := &segment{id: id, f: f, size: info.Size()}
	s.segs[id] = seg
	return seg, nil
}

func (s *Spool) rotate() error {
	old := s.active
	seg, err := s.openSegmentLocked(old.id + 1)
	if err != nil {
		return err
	}
	if seg.size != 0 {
		return fmt.Errorf("segment %d already has data", seg.id)
	}
	s.active = seg
	old.needSync = s.cfg.SyncOnRotate
	s.sealedLocked(old)
	return nil
}

// sealedLocked syncs a sealed segment once no write is in progress, or
// removes it if nothing references it.
func (s *Spool) sealedLocked(seg *segment) {
	if seg.writers > 0 {
		return
	}
	if seg.needSync {
		seg.needSync = false
		seg.syncing = true
		s.background.Add(1)
		go s.syncSegment(seg)
		return
	}
	if seg.refs == 0 && s.segs[seg.id] == seg {
		s.removeSegmentLocked(seg)
	}
}

// syncSegment fsyncs a sealed segment, which is no longer written.
func (s *Spool) syncSegment(seg *segment) {
	defer s.background.Done()
	err := seg.f.Sync()
	s.mu.Lock()
	defer s.mu.Unlock()
	seg.syncing = false
	if err != nil {
		s.fail(fmt.Errorf("sync segment %d: %w", seg.id, err))
	}
	if seg.refs == 0 && s.segs[seg.id] == seg {
		s.removeSegmentLocked(seg)
	}
}

func (s *Spool) removeSegmentLocked(seg *segment) {
	// syncSegment, or the last write through sealedLocked, removes it once
	// done.
	if seg.syncing || seg.writers > 0 {
		return
	}
	// The commits that released the segment must reach the file first, or a
	// restart would find registrations pointing into a removed segment.
	if s.log != nil {
		if err := s.log.flush(); err != nil {
			s.fail(err)
			return
		}
	}
	delete(s.segs, seg.id)
	seg.f.Close()
	if err := os.Remove(segmentPath(s.cfg.Directory, seg.id)); err != nil {
		s.logger.Warn("Failed to remove tail spool segment", zap.Uint32("segment", seg.id), zap.Error(err))
	}
}

// Recovered describes the state restored from disk.
type Recovered struct {
	// Refeed lists, in sequence order, records with associations that were
	// not decided before the restart. The processor must handle the listed
	// fragments again to rebuild its pending traces.
	Refeed []RefeedRecord
	// Memo lists recent decisions in decision order.
	Memo []memoEntry
}

// RefeedRecord is a record with associations to process again.
type RefeedRecord struct {
	Record *Record
	// Assocs are the indexes of the associations to process again.
	Assocs []int
}

// MemoEntry is a decision restored after a restart.
type MemoEntry = memoEntry

// Open replays the log in cfg.Directory and returns a spool that delivers
// kept spans to next once Start is called.
func Open(cfg Config, logger *zap.Logger, next consumer.Traces) (*Spool, *Recovered, error) {
	if err := ensureDir(cfg.Directory); err != nil {
		return nil, nil, err
	}
	lock, err := lockDir(cfg.Directory)
	if err != nil {
		return nil, nil, err
	}
	opened := false
	defer func() {
		if !opened {
			lock.Close()
		}
	}()
	s := &Spool{
		lock:    lock,
		cfg:     cfg,
		logger:  logger,
		next:    next,
		segs:    map[uint32]*segment{},
		records: map[uint64]*Record{},
		memo:    newMemo(cfg.KeepMemo, cfg.DropMemo),
		nextSeq: 1,
		wake:    make(chan struct{}, 1),
		stopped: make(chan struct{}),

		retryTimers: map[*time.Timer]struct{}{},
		readAt:      readSegment,
		writeAt:     writeSegment,
	}
	if cfg.MaxUnprocessed > 0 {
		s.room = make(chan struct{}, cfg.MaxUnprocessed)
	}
	s.readyCond = sync.NewCond(&s.mu)

	f, err := os.OpenFile(logPath(cfg.Directory), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, nil, err
	}
	rp := &replay{s: s, pending: map[pcommon.TraceID]*Pending{}}
	end, err := readLog(f, rp.apply)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("replay tail spool log: %w", err)
	}
	if err := f.Truncate(end); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	s.log = newLogWriter(f, end)

	rec, err := s.finishReplay(rp.pending)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	opened = true
	return s, rec, nil
}

type replay struct {
	s     *Spool
	order []*Record
	swept int
	// pending holds the instance of each trace as the log rebuilds it.
	pending map[pcommon.TraceID]*Pending
}

func (rp *replay) record(seq uint64) *Record {
	return rp.s.records[seq]
}

func (rp *replay) settle(id pcommon.TraceID, keep bool) {
	if p, ok := rp.pending[id]; ok {
		delete(rp.pending, id)
		rp.s.settlePendingLocked(p, keep)
	}
}

func (rp *replay) apply(f frame) error {
	s := rp.s
	switch f.kind {
	case frameHeader:
		s.nextSeq = max(s.nextSeq, f.seq)
	case frameRecord:
		// The segment of a record committed later in the log may already be
		// gone, so files are only opened for records still live at the end.
		seg, ok := s.segs[f.seg]
		if !ok {
			seg = &segment{id: f.seg}
			s.segs[f.seg] = seg
		}
		r := &Record{
			seq: f.seq, seg: seg, off: f.off, length: f.length, arrival: f.arrival,
			assocs: f.assocs, state: make([]assocState, len(f.assocs)), open: len(f.assocs),
		}
		seg.refs++
		s.records[r.seq] = r
		rp.order = append(rp.order, r)
		s.nextSeq = max(s.nextSeq, f.seq+1)
	case frameWatermark:
		rp.sweep(f.seq)
	case frameKeep, frameDrop:
		rp.sweep(f.seq)
		s.memo.put(f.id, f.kind == frameKeep)
		rp.settle(f.id, f.kind == frameKeep)
	case frameEvict:
		rp.sweep(f.seq)
		s.memo.remove(f.id)
		rp.settle(f.id, false)
	case frameLate:
		r := rp.record(f.seq)
		if r == nil {
			return nil
		}
		i := int(f.idx)
		if i >= len(r.assocs) {
			return fmt.Errorf("%w: association %d not in record %d", errCorrupt, i, f.seq)
		}
		if r.state[i] == pending {
			id := r.assocs[i].ID
			p := rp.pending[id]
			p.refs = slices.DeleteFunc(p.refs, func(ref assocRef) bool { return ref.rec == r && ref.idx == i })
			if len(p.refs) == 0 {
				delete(rp.pending, id)
				s.pendingTraces--
			}
			r.state[i] = unsettled
		}
		s.settleLocked(r, i, f.keep)
	case frameCommit:
		if r := rp.record(f.seq); r != nil {
			s.ready = slices.DeleteFunc(s.ready, func(x *Record) bool { return x == r })
			s.releaseLocked(r)
		}
	case frameMemo:
		s.memo.put(f.id, f.keep)
	}
	return nil
}

// sweep attaches the unsettled associations of records up to wm.
func (rp *replay) sweep(wm uint64) {
	s := rp.s
	if wm <= s.wm {
		return
	}
	s.wm = wm
	for rp.swept < len(rp.order) && rp.order[rp.swept].seq <= wm {
		r := rp.order[rp.swept]
		rp.swept++
		if r.done {
			continue
		}
		for i, a := range r.assocs {
			if r.state[i] != unsettled {
				continue
			}
			p, ok := rp.pending[a.ID]
			if !ok {
				p = &Pending{}
				rp.pending[a.ID] = p
			}
			s.attachLocked(p, r, i)
		}
	}
}

// finishReplay reopens segments, removes unreferenced files, and prepares
// the records that must be processed again.
func (s *Spool) finishReplay(instances map[pcommon.TraceID]*Pending) (*Recovered, error) {
	// A trace with associations attached after its last logged decision was
	// a new instance at runtime, so its old decision was no longer cached.
	for id := range instances {
		s.memo.remove(id)
	}
	s.pendingTraces = 0
	s.stats.Committed = 0

	for id, seg := range s.segs {
		if seg.refs == 0 {
			delete(s.segs, id)
			continue
		}
		f, err := os.OpenFile(segmentPath(s.cfg.Directory, id), os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("open segment %d: %w", id, err)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		seg.f, seg.size = f, info.Size()
	}
	for _, r := range s.sortedRecordsLocked() {
		if r.seg.f == nil {
			s.releaseUnreadableLocked(r, fmt.Errorf("segment %d is missing", r.seg.id))
		} else if r.off+int64(r.length) > r.seg.size {
			s.releaseUnreadableLocked(r, fmt.Errorf("record %d extends past segment %d", r.seq, r.seg.id))
		}
	}

	rec := &Recovered{Memo: s.memo.entries()}
	for _, r := range s.sortedRecordsLocked() {
		var open []int
		for i, st := range r.state {
			if st == pending || st == unsettled {
				r.state[i] = unsettled
				open = append(open, i)
			}
		}
		if len(open) > 0 {
			rec.Refeed = append(rec.Refeed, RefeedRecord{Record: r, Assocs: open})
		}
	}
	// Records to refeed count as unprocessed again.
	s.wm = s.nextSeq - 1
	if len(rec.Refeed) > 0 {
		s.wm = rec.Refeed[0].Record.seq - 1
	}

	var maxID uint32
	entries, err := os.ReadDir(filepath.Join(s.cfg.Directory, "segments"))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".otlp")
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(name, 16, 32)
		if err != nil {
			continue
		}
		maxID = max(maxID, uint32(id))
		if _, ok := s.segs[uint32(id)]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(s.cfg.Directory, "segments", e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	for id := range s.segs {
		maxID = max(maxID, id)
	}
	active, err := s.openSegmentLocked(maxID + 1)
	if err != nil {
		return nil, err
	}
	s.active = active
	s.checkpointLocked()
	return rec, s.err
}

// maybeCheckpointLocked starts a checkpoint once the log has grown well past
// the last one, which keeps a large live state from being rewritten over and
// over.
func (s *Spool) maybeCheckpointLocked() {
	if s.cfg.CheckpointBytes <= 0 || s.checkpointing || s.stopping || s.err != nil {
		return
	}
	if s.log.size <= max(s.cfg.CheckpointBytes, 2*s.lastSnapshot) {
		return
	}
	frames := s.snapshotLocked()
	s.checkpointing = true
	s.log.tail = make([]byte, 0, 64<<10)
	s.background.Add(1)
	go s.checkpoint(frames)
}

// checkpoint writes the snapshot outside the lock while the current log
// keeps receiving frames and a copy of them, then appends the copy to the
// snapshot and atomically replaces the log with it.
func (s *Spool) checkpoint(frames []frame) {
	defer s.background.Done()
	tmp := logPath(s.cfg.Directory) + ".tmp"
	w, err := writeSnapshot(tmp, frames)
	if s.snapshotWritten != nil {
		s.snapshotWritten()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tail := s.log.tail
	s.log.tail = nil
	s.checkpointing = false
	if err != nil {
		s.fail(fmt.Errorf("write checkpoint: %w", err))
		return
	}
	if s.stopping || s.err != nil {
		w.f.Close()
		os.Remove(tmp)
		return
	}
	snapshot := w.size
	if err := w.appendEncoded(tail); err != nil {
		w.f.Close()
		s.fail(fmt.Errorf("write checkpoint: %w", err))
		return
	}
	s.installLocked(w, tmp, snapshot)
}

// checkpointLocked rewrites the live state into a new log and atomically
// replaces the old one. Open uses it before anything else runs.
func (s *Spool) checkpointLocked() {
	if s.err != nil {
		return
	}
	tmp := logPath(s.cfg.Directory) + ".tmp"
	w, err := writeSnapshot(tmp, s.snapshotLocked())
	if err != nil {
		s.fail(fmt.Errorf("write checkpoint: %w", err))
		return
	}
	s.installLocked(w, tmp, w.size)
}

func (s *Spool) installLocked(w *logWriter, tmp string, snapshot int64) {
	if err := os.Rename(tmp, logPath(s.cfg.Directory)); err != nil {
		w.f.Close()
		s.fail(fmt.Errorf("write checkpoint: %w", err))
		return
	}
	if s.log != nil {
		s.log.f.Close()
	}
	s.log = w
	s.lastSnapshot = snapshot
	s.stats.Checkpoints++
}

// snapshotLocked returns the frames that rebuild the live state.
func (s *Spool) snapshotLocked() []frame {
	recs := s.sortedRecordsLocked()
	memo := s.memo.entries()
	frames := make([]frame, 0, 2+2*len(recs)+len(memo))
	frames = append(frames, frame{kind: frameHeader, seq: s.nextSeq})
	for _, r := range recs {
		frames = append(frames, frame{kind: frameRecord, seq: r.seq, seg: r.seg.id, off: r.off, length: r.length, arrival: r.arrival, assocs: r.assocs})
	}
	for _, r := range recs {
		for i, st := range r.state {
			if st == kept || st == dropped {
				frames = append(frames, frame{kind: frameLate, seq: r.seq, idx: uint32(i), keep: st == kept})
			}
		}
	}
	frames = append(frames, frame{kind: frameWatermark, seq: s.wm})
	for _, e := range memo {
		frames = append(frames, frame{kind: frameMemo, id: e.ID, keep: e.Keep})
	}
	return frames
}

func writeSnapshot(path string, frames []frame) (*logWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	w := newLogWriter(f, 0)
	for i := range frames {
		if err = w.append(&frames[i]); err != nil {
			break
		}
	}
	if err == nil {
		err = w.flush()
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}
