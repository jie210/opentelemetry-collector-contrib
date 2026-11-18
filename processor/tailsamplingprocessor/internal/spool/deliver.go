// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
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
		keep := make(map[pcommon.TraceID]struct{}, r.keeps)
		for i, st := range r.state {
			if st == kept {
				keep[r.ids[i]] = struct{}{}
			}
		}
		s.mu.Unlock()

		delivered := s.deliver(r, keep)

		s.mu.Lock()
		s.inflight -= int64(r.length)
		if delivered {
			s.appendLocked(&frame{kind: frameCommit, seq: r.seq})
			if err := s.log.flush(); err != nil {
				s.fail(err)
			}
			s.releaseLocked(r)
			s.maybeCheckpointLocked()
		} else {
			s.ready = append(s.ready, r)
		}
		s.readyCond.Broadcast()
		s.mu.Unlock()
	}
}

// deliver reads the record, keeps the spans of kept traces and hands them to
// the next consumer, retrying until it succeeds or the spool stops. It
// reports whether the record is finished.
func (s *Spool) deliver(r *Record, keep map[pcommon.TraceID]struct{}) bool {
	td, err := s.readRecord(r)
	if err != nil {
		s.logger.Error("Dropping unreadable tail spool record", zap.Uint64("seq", r.seq), zap.Error(err))
		s.mu.Lock()
		s.stats.DeliveryFailures++
		s.mu.Unlock()
		return true
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
			return true
		}
		s.mu.Lock()
		s.stats.DeliveryFailures++
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			return false
		}
		s.logger.Warn("Tail spool delivery failed, retrying", zap.Uint64("seq", r.seq), zap.Duration("backoff", backoff), zap.Error(err))
		time.Sleep(backoff)
		backoff = min(2*backoff, 5*time.Second)
		// A consumer may have modified td before failing; start from the
		// original bytes again.
		if td, err = s.readRecord(r); err != nil {
			return true
		}
		spans = filterKept(td, keep)
	}
}

// ReadRecord decodes the record's original request.
func (s *Spool) ReadRecord(r *Record) (ptrace.Traces, error) { return s.readRecord(r) }

func (s *Spool) readRecord(r *Record) (ptrace.Traces, error) {
	buf := make([]byte, r.length)
	if _, err := r.seg.f.ReadAt(buf, r.off); err != nil {
		return ptrace.Traces{}, fmt.Errorf("read record %d: %w", r.seq, err)
	}
	var u ptrace.ProtoUnmarshaler
	td, err := u.UnmarshalTraces(buf)
	if err != nil {
		return ptrace.Traces{}, fmt.Errorf("decode record %d: %w", r.seq, err)
	}
	return td, nil
}

// filterKept removes spans of traces not in keep, along with emptied scopes
// and resources, and returns the number of remaining spans.
func filterKept(td ptrace.Traces, keep map[pcommon.TraceID]struct{}) int {
	var n int
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				_, ok := keep[span.TraceID()]
				return !ok
			})
			n += ss.Spans().Len()
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
	return n
}
