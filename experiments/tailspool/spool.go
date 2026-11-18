// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type traceID = pcommon.TraceID
type segment struct {
	number  int
	file    *os.File
	size    int64
	records int
}
type record struct {
	seq             uint64
	due, inbox      int64
	fields          []byte
	seg             *segment
	offset          int64
	length, pending int
	keep            map[traceID]bool
}
type spool struct {
	journal                       *journal
	recovering                    map[traceID]bool
	restoredDone                  int64
	maxLateness                   int64
	mu                            sync.Mutex
	dir                           string
	limit                         int64
	syncSegments                  bool
	current                       *segment
	index                         map[traceID][]*record
	yes, no                       cache.Cache
	next                          int
	written, read, appends, reads int64
	sink                          func(ptrace.Traces) error
	err                           error
}

func newSpool(dir string, sink func(ptrace.Traces) error, yes, no cache.Cache) *spool {
	return &spool{dir: dir, limit: 64 << 20, index: make(map[traceID][]*record), sink: sink, yes: yes, no: no}
}

// The fixed ERROR policy examines span status only. Keep grouping IDs and parent
// identity for the upstream sampler; string-valued payloads never enter its queue.
func projection(td ptrace.Traces) (ptrace.Traces, map[traceID]bool) {
	out := ptrace.NewTraces()
	dest := out.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	ids := make(map[traceID]bool)
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, s := range ss.Spans().All() {
				p := dest.AppendEmpty()
				p.SetTraceID(s.TraceID())
				p.SetSpanID(s.SpanID())
				p.SetParentSpanID(s.ParentSpanID())
				p.Status().SetCode(s.Status().Code())
				ids[s.TraceID()] = ids[s.TraceID()] || s.Status().Code() == ptrace.StatusCodeError
			}
		}
	}
	return out, ids
}

func (s *spool) append(raw []byte, ids map[traceID]bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(raw, ids)
}
func (s *spool) appendLocked(raw []byte, ids map[traceID]bool) error {
	if s.err != nil {
		return s.err
	}
	if len(ids) == 0 {
		return nil
	}
	if s.current == nil || s.current.size+int64(len(raw)) > s.limit {
		old := s.current
		if old != nil && s.syncSegments {
			if err := old.file.Sync(); err != nil {
				s.err = err
				return err
			}
		}
		f, err := os.OpenFile(filepath.Join(s.dir, fmt.Sprintf("%08d.otlp", s.next)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		s.next++
		s.current = &segment{file: f, number: s.next - 1}
		if old != nil && old.records == 0 {
			if err = s.remove(old); err != nil {
				s.err = err
				return err
			}
		}
	}
	g := s.current
	r := &record{due: time.Now().Add(*decisionWait).UnixNano(), seg: g, offset: g.size, length: len(raw)}
	n, err := g.file.Write(raw)
	g.size += int64(n)
	if err != nil || n != len(raw) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.err = err
		return err
	}
	if s.journal != nil {
		if err = s.journal.appendRecord(s, r, ids); err != nil {
			s.err = err
			return err
		}
	}
	s.written += int64(n)
	s.appends++
	if s.journal == nil {
		s.register(r, ids)
	}
	if r.pending == 0 {
		s.finish(r)
	}
	if s.err == nil && s.journal != nil && *checkpointBytes > 0 && s.journal.delta >= *checkpointBytes {
		s.err = s.checkpoint()
	}
	return s.err
}

func (s *spool) register(r *record, ids map[traceID]bool) {
	r.seg.records++
	for id := range ids {
		if _, ok := s.yes.Get(id); ok {
			r.markKeep(id)
			continue
		}
		if _, ok := s.no.Get(id); ok {
			continue
		}
		r.pending++
		s.index[id] = append(s.index[id], r)
	}
}

func (r *record) markKeep(id traceID) {
	if r.keep == nil {
		r.keep = make(map[traceID]bool)
	}
	r.keep[id] = true
}

func (s *spool) remove(g *segment) error {
	if s.journal != nil {
		if err := s.journal.w.Flush(); err != nil {
			return err
		}
	}
	if err := g.file.Close(); err != nil {
		return err
	}
	return os.Remove(g.file.Name())
}

// Register the decision and detach its references in the same critical section
// as append, so a concurrently arriving late batch sees either path exactly once.
type observedCache struct {
	cache.Cache
	store *spool
	keep  bool
}

func (c observedCache) Put(id traceID, m cache.DecisionMetadata) {
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal != nil && !s.journal.replay && s.err == nil {
		s.err = s.journal.decision(id, c.keep)
	}
	c.apply(id, m)
}
func (c observedCache) apply(id traceID, m cache.DecisionMetadata) {
	s := c.store
	c.Cache.Put(id, m)
	for _, r := range s.index[id] {
		if c.keep {
			r.markKeep(id)
		}
		r.pending--
		if r.pending == 0 && (s.journal == nil || !s.journal.replay) {
			s.finish(r)
		}
	}
	delete(s.index, id)
}

func (s *spool) finish(r *record) {
	if s.err != nil {
		return
	}
	if len(r.keep) > 0 {
		raw := make([]byte, r.length)
		var stop func()
		if *probeReads && s.reads%32 == 0 {
			stop = readback.begin()
		}
		_, err := r.seg.file.ReadAt(raw, r.offset)
		if stop != nil {
			stop()
		}
		if err != nil {
			s.err = err
			return
		}
		s.read += int64(len(raw))
		s.reads++
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		if err != nil {
			s.err = err
			return
		}
		td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
			rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
				ss.Spans().RemoveIf(func(p ptrace.Span) bool { return !r.keep[p.TraceID()] })
				return ss.Spans().Len() == 0
			})
			return rs.ScopeSpans().Len() == 0
		})
		if err := s.sink(td); err != nil {
			s.err = err
			return
		}
	}
	if s.journal != nil {
		if err := s.journal.complete(r); err != nil {
			s.err = err
			return
		}
	}
	r.seg.records--
	if r.seg != s.current && r.seg.records == 0 {
		s.err = s.remove(r.seg)
	}
}
