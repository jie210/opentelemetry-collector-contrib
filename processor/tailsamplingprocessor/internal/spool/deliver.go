// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const (
	// recoveredReadAttempts bounds reads of a record left by a previous run
	// before it is released as unreadable.
	recoveredReadAttempts = 3
	// unprocessedReadAttempts bounds reads of the oldest unprocessed record,
	// which holds back every record registered after it.
	unprocessedReadAttempts = 5
)

// errUndecodable marks stored bytes that read fine but do not decode, such
// as a request nested deeper than the protobuf decoder accepts. Reading
// them again cannot succeed.
var errUndecodable = errors.New("stored request cannot be decoded")

var (
	recoveredReadInterval = time.Second
	readBackoffBase       = 100 * time.Millisecond
	maxReadBackoff        = 30 * time.Second
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
	for t := range s.retryTimers {
		t.Stop()
	}
	s.readyCond.Broadcast()
	s.mu.Unlock()
	s.workers.Wait()
	s.background.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.log.flush()
	s.log.f.Close()
	for _, seg := range s.segs {
		seg.f.Close()
	}
	s.lock.Close()
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

		delivered, readErr := s.deliver(r, keep)

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
		case readErr != nil:
			s.readFailedLocked(r, readErr)
		default:
			s.ready = append(s.ready, r)
		}
		s.readyCond.Broadcast()
		s.mu.Unlock()
	}
}

// readFailedLocked schedules another delivery attempt of a record that could
// not be read. A record that does not decode is released at once. Other
// failures of records of this run are retried until the process stops;
// records left by a previous run are released after a few attempts.
func (s *Spool) readFailedLocked(r *Record, err error) {
	s.stats.ReadFailures++
	r.readFailures++
	if errors.Is(err, errUndecodable) {
		s.releaseUnreadableLocked(r, err)
		return
	}
	if r.recovered && (r.readFailures >= recoveredReadAttempts || r.seg.unreadable) {
		r.seg.unreadable = true
		s.releaseUnreadableLocked(r, err)
		return
	}
	delay := recoveredReadInterval
	if !r.recovered {
		delay = min(maxReadBackoff, readBackoffBase<<min(r.readFailures-1, 10))
	}
	s.logger.Warn("Failed to read tail spool record, retrying",
		zap.Uint64("seq", r.seq), zap.Int("failures", r.readFailures), zap.Duration("backoff", delay), zap.Error(err))
	if s.stopping {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.retryTimers, t)
		if !r.done && !s.stopping {
			s.ready = append(s.ready, r)
			s.readyCond.Broadcast()
		}
	})
	s.retryTimers[t] = struct{}{}
}

// releaseUnreadableLocked gives up on a record that cannot be read: its open
// associations settle as drop and the record is committed so its segment can
// be reclaimed.
func (s *Spool) releaseUnreadableLocked(r *Record, err error) {
	if r.done {
		return
	}
	s.logger.Error("Releasing unreadable tail spool record",
		zap.Uint64("seq", r.seq), zap.Uint32("segment", r.seg.id), zap.Int64("offset", r.off),
		zap.Uint32("length", r.length), zap.Int("associations", len(r.assocs)), zap.Int("kept", r.keeps),
		zap.Error(err))
	for i, st := range r.state {
		if st == unsettled || st == pending {
			s.appendLocked(&frame{kind: frameLate, seq: r.seq, idx: uint32(i), keep: false})
			r.state[i] = dropped
		}
	}
	r.open = 0
	for i, x := range s.ready {
		if x == r {
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			break
		}
	}
	s.appendLocked(&frame{kind: frameCommit, seq: r.seq})
	if err := s.log.flush(); err != nil {
		s.fail(err)
	}
	s.stats.ReleasedUnreadable++
	s.stats.ReleasedUnreadableBytes += uint64(r.length)
	s.releaseLocked(r)
}

// deliver reads the record, keeps the spans of kept traces and hands them to
// the next consumer, retrying until it succeeds or the spool stops. It
// reports whether the record was delivered, or the error that prevented
// reading it.
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

// ReadRecovered decodes a record left by a previous run so the processor can
// process its fragments again. A record that does not decode, or cannot be
// read after a few attempts, is released and the error is returned.
func (s *Spool) ReadRecovered(r *Record) (ptrace.Traces, error) {
	for attempt := 1; ; attempt++ {
		td, err := s.readRecord(r)
		if err == nil {
			return td, nil
		}
		s.mu.Lock()
		s.stats.ReadFailures++
		if errors.Is(err, errUndecodable) {
			s.releaseUnreadableLocked(r, err)
			s.mu.Unlock()
			return ptrace.Traces{}, err
		}
		if attempt >= recoveredReadAttempts || r.seg.unreadable {
			r.seg.unreadable = true
			s.releaseUnreadableLocked(r, err)
			s.mu.Unlock()
			return ptrace.Traces{}, err
		}
		s.mu.Unlock()
		s.logger.Warn("Failed to read tail spool record, retrying",
			zap.Uint64("seq", r.seq), zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(recoveredReadInterval)
	}
}

// ReadRecord decodes the record's stored request.
func (s *Spool) ReadRecord(r *Record) (ptrace.Traces, error) { return s.readRecord(r) }

func (s *Spool) readRecord(r *Record) (ptrace.Traces, error) {
	buf := make([]byte, r.length)
	if _, err := s.readAt(r.seg, buf, r.off); err != nil {
		return ptrace.Traces{}, fmt.Errorf("read record %d: %w", r.seq, err)
	}
	var u ptrace.ProtoUnmarshaler
	td, err := u.UnmarshalTraces(buf)
	if err != nil {
		return ptrace.Traces{}, fmt.Errorf("record %d: %w: %w", r.seq, errUndecodable, err)
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
