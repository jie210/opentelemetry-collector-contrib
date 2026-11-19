// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// Start launches the delivery workers. Each worker hands at most one record
// to the next consumer at a time, so at most MaxInflightRecords records can
// be redelivered after a process kill.
func (s *Spool) Start() {
	s.mu.Lock()
	s.stats.Redelivered = uint64(len(s.ready))
	s.mu.Unlock()
	for range max(1, s.cfg.MaxInflightRecords) {
		s.workers.Add(1)
		go s.worker()
	}
}

// Stop waits for in-progress deliveries and closes the files. Records not
// yet delivered stay on disk for the next start.
func (s *Spool) Stop() error {
	s.mu.Lock()
	s.stopping = true
	s.readyCond.Broadcast()
	s.mu.Unlock()
	s.workers.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.log.flush()
	s.log.f.Close()
	for _, seg := range s.segs {
		seg.f.Close()
	}
	if err != nil {
		return err
	}
	return s.err
}

func (s *Spool) overBudgetLocked(r *Record) bool {
	return s.cfg.MaxInflightBytes > 0 && s.inflight > 0 && s.inflight+int64(r.length) > s.cfg.MaxInflightBytes
}

func (s *Spool) worker() {
	defer s.workers.Done()
	for {
		s.mu.Lock()
		for !s.stopping && (len(s.ready) == 0 || s.overBudgetLocked(s.ready[0])) {
			s.readyCond.Wait()
		}
		if s.stopping {
			s.mu.Unlock()
			return
		}
		r := s.ready[0]
		s.ready = s.ready[1:]
		s.inflight += int64(r.length)
		keep := make(map[Assoc]struct{}, r.keeps)
		for i, st := range r.state {
			if st == kept {
				keep[r.assocs[i]] = struct{}{}
			}
		}
		s.mu.Unlock()

		delivered, err := s.deliver(r, keep)

		s.mu.Lock()
		s.inflight -= int64(r.length)
		switch {
		case delivered:
			s.appendLocked(&frame{kind: frameCommit, seq: r.seq})
			if err := s.log.flush(); err != nil {
				s.fail(err)
			}
			s.releaseLocked(r)
			s.maybeCheckpointLocked()
		case err != nil:
			s.readFailedLocked(r, err)
		default:
			s.ready = append(s.ready, r)
		}
		s.readyCond.Broadcast()
		s.mu.Unlock()
	}
}

// readFailedLocked schedules another delivery attempt of a record whose
// bytes could not be read. Records of the current run are retried until
// they can be read; records of the previous run are released after a few
// attempts.
func (s *Spool) readFailedLocked(r *Record, err error) {
	s.stats.ReadFailures++
	r.readFailures++
	if r.prevRun && (s.badSegs[r.seg.id] || r.readFailures >= prevRunReadAttempts) {
		s.badSegs[r.seg.id] = true
		s.releaseUnreadableLocked(r, err)
		return
	}
	delay := s.prevRunDelay
	if !r.prevRun {
		delay = min(s.minReadBackoff<<min(r.readFailures-1, 20), s.maxReadBackoff)
	}
	s.logger.Warn("Failed to read tail spool record, retrying",
		zap.Uint64("seq", r.seq), zap.Int("failures", r.readFailures), zap.Duration("delay", delay), zap.Error(err))
	s.retrying++
	time.AfterFunc(delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.retrying--
		if s.stopping || r.done {
			return
		}
		s.ready = append(s.ready, r)
		s.readyCond.Signal()
	})
}

// deliver reads the record, keeps the spans of kept traces and hands them to
// the next consumer, retrying the consumer until it succeeds or the spool
// stops. It reports whether the record was delivered, or the read error that
// prevented it.
func (s *Spool) deliver(r *Record, keep map[Assoc]struct{}) (bool, error) {
	td, err := s.readRecord(r)
	if err != nil {
		return false, err
	}
	spans := filterKept(td, keep)
	backoff := 50 * time.Millisecond
	for {
		err := s.next.ConsumeTraces(context.Background(), td)
		if err == nil {
			s.mu.Lock()
			s.stats.Delivered++
			s.stats.DeliveredSpans += uint64(spans)
			s.mu.Unlock()
			return true, nil
		}
		s.mu.Lock()
		s.stats.DeliveryFailures++
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			return false, nil
		}
		s.logger.Warn("Tail spool delivery failed, retrying", zap.Uint64("seq", r.seq), zap.Duration("backoff", backoff), zap.Error(err))
		time.Sleep(backoff)
		backoff = min(2*backoff, 5*time.Second)
		// A consumer may have modified td before failing; start from the
		// stored bytes again.
		if td, err = s.readRecord(r); err != nil {
			return false, err
		}
		spans = filterKept(td, keep)
	}
}

// ReadRefeed decodes a record of the previous run whose fragments must be
// processed again. A record that still cannot be read after a few attempts
// is released and the read error returned; its spans are lost.
func (s *Spool) ReadRefeed(r *Record) (ptrace.Traces, error) {
	for {
		td, err := s.readRecord(r)
		if err == nil {
			return td, nil
		}
		s.mu.Lock()
		s.stats.ReadFailures++
		r.readFailures++
		if s.badSegs[r.seg.id] || r.readFailures >= prevRunReadAttempts {
			s.badSegs[r.seg.id] = true
			s.releaseUnreadableLocked(r, err)
			s.mu.Unlock()
			return ptrace.Traces{}, err
		}
		delay := s.prevRunDelay
		s.mu.Unlock()
		time.Sleep(delay)
	}
}

func (s *Spool) readRecord(r *Record) (ptrace.Traces, error) {
	buf := make([]byte, r.length)
	read := s.readAt
	if read == nil {
		read = func(seg *segment, p []byte, off int64) (int, error) { return seg.f.ReadAt(p, off) }
	}
	if _, err := read(r.seg, buf, r.off); err != nil {
		return ptrace.Traces{}, fmt.Errorf("read record %d: %w", r.seq, err)
	}
	var u ptrace.ProtoUnmarshaler
	td, err := u.UnmarshalTraces(buf)
	if err != nil {
		return ptrace.Traces{}, fmt.Errorf("decode record %d: %w", r.seq, err)
	}
	return td, nil
}

// filterKept removes spans of associations not in keep, along with emptied
// scopes and resources, and returns the number of remaining spans.
func filterKept(td ptrace.Traces, keep map[Assoc]struct{}) int {
	var n int
	resource := uint32(0)
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		ri := resource
		resource++
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				_, ok := keep[Assoc{ID: span.TraceID(), Resource: ri}]
				return !ok
			})
			n += ss.Spans().Len()
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
	return n
}
