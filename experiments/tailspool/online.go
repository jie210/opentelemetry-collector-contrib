// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// A successful early ACK owns a complete file. Adoption links its inode into
// the segment namespace. A registration carrying the inbox ID precedes unlink.
type startupGate struct {
	mu     sync.Mutex
	dir    string
	active endpoint
	last   int64
	staged int
}

func (g *startupGate) consume(ctx context.Context, raw []byte) error {
	g.mu.Lock()
	h := g.active
	if h != nil {
		g.mu.Unlock()
		return h(ctx, raw)
	}
	defer g.mu.Unlock()
	if _, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw); err != nil {
		return err
	}
	n := max(time.Now().UnixNano(), g.last+1)
	g.last = n
	p := filepath.Join(g.dir, fmt.Sprintf("%019d.inbox", n))
	if err := os.WriteFile(p+".part", raw, 0600); err != nil {
		return err
	}
	if err := os.Rename(p+".part", p); err != nil {
		return err
	}
	g.staged++
	return nil
}
func (g *startupGate) activate(s *spool, h endpoint) (spans, batches int, err error) {
	for {
		g.mu.Lock()
		paths, e := filepath.Glob(filepath.Join(g.dir, "*.inbox"))
		if e != nil {
			g.mu.Unlock()
			return spans, batches, e
		}
		if len(paths) == 0 {
			g.active = h
			g.mu.Unlock()
			return spans, batches, nil
		}
		g.mu.Unlock()
		for _, p := range paths {
			n, e := s.adopt(p)
			if e != nil {
				return spans, batches, e
			}
			spans += n
			batches++
		}
	}
}

func (s *spool) resolve(id traceID, now int64) {
	keep, ok := s.recovering[id]
	if !ok {
		return
	}
	lag := now - s.index[id][0].due
	c := observedCache{s.no, s, false}
	if keep {
		c = observedCache{s.yes, s, true}
	}
	if s.err == nil {
		s.err = s.journal.decision(id, keep)
	}
	c.apply(id, cache.DecisionMetadata{})
	delete(s.recovering, id)
	s.restoredDone++
	s.maxLateness = max(s.maxLateness, lag)
}
func (s *spool) adopt(path string) (int, error) {
	id, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), ".inbox"), 10, 64)
	if err != nil {
		return 0, err
	}
	if s.journal.adopted[id] {
		return 0, os.Remove(path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
	if err != nil {
		return 0, err
	}
	_, ids := projection(td)
	if len(ids) == 0 {
		return 0, os.Remove(path)
	}
	for k := range ids {
		if _, ok := s.recovering[k]; ok && s.index[k][0].due <= id {
			s.resolve(k, id)
		}
	}
	if s.err != nil {
		return 0, s.err
	}
	name := filepath.Join(s.dir, fmt.Sprintf("%08d.otlp", s.next))
	if err = os.Link(path, name); err != nil {
		return 0, err
	}
	f, err := os.Open(name)
	if err != nil {
		return 0, err
	}
	seg := &segment{file: f, number: s.next, size: int64(len(raw))}
	s.next++
	r := &record{inbox: id, due: id + int64(*decisionWait), seg: seg, length: len(raw)}
	if err = s.journal.appendRecord(s, r, ids); err != nil {
		return 0, err
	}
	for k, e := range ids {
		if len(s.index[k]) > 0 {
			s.recovering[k] = s.recovering[k] || e
		}
	}
	if r.pending == 0 {
		s.finish(r)
	}
	if s.err != nil {
		return 0, s.err
	}
	return td.SpanCount(), os.Remove(path)
}

// New batches for a recovering trace contribute to the same pending decision.
// Once the original deadline passes the decision is cached before registration.
func (s *spool) ingest(raw []byte, ids map[traceID]bool, td ptrace.Traces) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	for id := range ids {
		if _, ok := s.recovering[id]; ok {
			if s.index[id][0].due <= now {
				s.resolve(id, now)
			}
		}
	}
	if err := s.appendLocked(raw, ids); err != nil {
		return err
	}
	if len(s.recovering) > 0 {
		for id, e := range ids {
			if old, ok := s.recovering[id]; ok {
				s.recovering[id] = old || e
			}
		}
		for _, rs := range td.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				ss.Spans().RemoveIf(func(p ptrace.Span) bool { _, ok := s.recovering[p.TraceID()]; return ok })
			}
		}
	}
	return nil
}

type dueTrace struct {
	id  traceID
	due int64
}

func (s *spool) schedule() []dueTrace {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]dueTrace, 0, len(s.recovering))
	for id := range s.recovering {
		items = append(items, dueTrace{id, s.index[id][0].due})
	}
	slices.SortFunc(items, func(a, b dueTrace) int { return cmp.Compare(a.due, b.due) })
	return items
}
func (s *spool) expire(ctx context.Context, items []dueTrace) {
	for i := 0; i < len(items); {
		if ctx.Err() != nil {
			return
		}
		if d := time.Until(time.Unix(0, items[i].due)); d > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
		}
		s.mu.Lock()
		now := time.Now().UnixNano()
		end := min(i+32, len(items))
		for i < end && items[i].due <= now {
			s.resolve(items[i].id, now)
			i++
		}
		s.mu.Unlock()
	}
}
