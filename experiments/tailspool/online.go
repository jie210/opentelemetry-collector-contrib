// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"strconv"
	"strings"
	"sync"
	"time"

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
	parts, err := s.plan.split(td)
	if err != nil {
		return 0, err
	}
	ids := partIDs(parts)
	if len(ids) == 0 {
		return 0, os.Remove(path)
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
	r := &record{inbox: id, due: id + int64(s.wait), seg: seg, length: len(raw)}
	if err = s.journal.appendRecord(s, r, ids, parts); err != nil {
		return 0, err
	}
	s.attach(r, parts)
	for k := range ids {
		if len(s.index[k]) > 0 {
			s.recovering[k] = true
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

func partIDs(parts map[traceID]tracePart) map[traceID]bool {
	ids := make(map[traceID]bool, len(parts))
	for id := range parts {
		ids[id] = false
	}
	return ids
}
func (s *spool) ingestView(ctx context.Context, raw []byte, td ptrace.Traces) error {
	parts, e := s.plan.split(td)
	if e != nil || len(parts) == 0 {
		return e
	}
	ids := partIDs(parts)
	s.mu.Lock()
	for s.capacity > 0 && s.err == nil && len(s.index) > 0 && uint64(len(s.index)+len(ids)) > s.capacity {
		extra := 0
		for id := range ids {
			if len(s.index[id]) == 0 {
				if _, ok := s.yes.Get(id); ok {
					continue
				}
				if _, ok := s.no.Get(id); !ok {
					extra++
				}
			}
		}
		if extra == 0 || uint64(len(s.index)+extra) <= s.capacity {
			break
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if e = s.appendLocked(raw, ids, parts); e != nil {
		return e
	}
	s.attach(s.journal.live[s.journal.seq], parts)
	return nil
}
