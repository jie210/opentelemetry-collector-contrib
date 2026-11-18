// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"container/heap"
	"context"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type pendingTrace struct {
	id       traceID
	due      int64
	parts    [][]byte
	raw      []*record
	statuses uint8
}
type deadlineQueue []*pendingTrace

func (q deadlineQueue) Len() int           { return len(q) }
func (q deadlineQueue) Less(i, j int) bool { return q[i].due < q[j].due }
func (q deadlineQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *deadlineQueue) Push(v any)        { *q = append(*q, v.(*pendingTrace)) }
func (q *deadlineQueue) Pop() any {
	a := *q
	i := len(a) - 1
	v := a[i]
	a[i] = nil
	*q = a[:i]
	return v
}

func (s *spool) attach(r *record, parts map[traceID]tracePart) {
	if r == nil {
		return
	}
	for id, part := range parts {
		refs := s.index[id]
		if len(refs) == 0 || !slices.Contains(refs, r) {
			continue
		}
		v := s.views[id]
		if v == nil {
			if refs[0] != r {
				continue
			} // Decision submitted, late record inherits its result.
			v = &pendingTrace{id: id, due: r.due}
			s.views[id] = v
			heap.Push(&s.deadlines, v)
		} else if r.due-int64(s.wait) >= v.due {
			continue
		}
		if s.plan.full {
			v.raw = append(v.raw, r)
		}
		v.statuses |= part.statuses
		if len(part.wire) > 0 {
			v.parts = append(v.parts, part.wire)
		}
	}
}

// Read each live request once at restart, even when it contains many traces.
// This also rebuilds legacy journals that only recorded the ERROR bit.
func (s *spool) rebuildViews() error {
	seqs := make([]uint64, 0, len(s.journal.live))
	for seq := range s.journal.live {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	for _, seq := range seqs {
		r := s.journal.live[seq]
		parts := make(map[traceID]tracePart)
		if s.plan.full || s.plan.statusOnly {
			for i := 0; i < len(r.fields); i += 17 {
				mask := r.fields[i+16] >> 3
				if !s.plan.full && mask == 0 {
					parts = nil
					break
				}
				parts[traceID(r.fields[i:i+16])] = tracePart{statuses: mask}
			}
			if parts != nil {
				s.attach(r, parts)
				continue
			}
		}
		raw := make([]byte, r.length)
		if _, e := r.seg.file.ReadAt(raw, r.offset); e != nil {
			return e
		}
		td, e := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		if e != nil {
			return e
		}
		parts, e = s.plan.split(td)
		if e != nil {
			return e
		}
		s.attach(r, parts)
	}
	return nil
}
func (v *pendingTrace) appendTo(td ptrace.Traces) error {
	if v.statuses != 0 {
		spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
		for code := ptrace.StatusCodeUnset; code <= ptrace.StatusCodeError; code++ {
			if v.statuses&(1<<code) != 0 {
				p := spans.AppendEmpty()
				p.SetTraceID(v.id)
				p.Status().SetCode(code)
			}
		}
	}
	for _, part := range v.parts {
		t, e := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(part)
		if e != nil {
			return e
		}
		t.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
	}
	return nil
}

// Full-input policies keep disk references until evaluation. Read a shared
// record once per submitted group, retaining source resource/scope order.
func appendFullInputs(td ptrace.Traces, batch []*pendingTrace) error {
	selected := make(map[*record]map[traceID]bool)
	var order []*record
	for _, v := range batch {
		for _, r := range v.raw {
			if selected[r] == nil {
				selected[r] = map[traceID]bool{}
				order = append(order, r)
			}
			selected[r][v.id] = true
		}
	}
	slices.SortFunc(order, func(a, b *record) int { return cmp.Compare(a.seq, b.seq) })
	for _, r := range order {
		raw := make([]byte, r.length)
		if _, e := r.seg.file.ReadAt(raw, r.offset); e != nil {
			return e
		}
		t, e := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		if e != nil {
			return e
		}
		t.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
			rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
				ss.Spans().RemoveIf(func(p ptrace.Span) bool { return !selected[r][p.TraceID()] })
				return ss.Spans().Len() == 0
			})
			return rs.ScopeSpans().Len() == 0
		})
		t.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
	}
	return nil
}
func (s *spool) submitDue(ctx context.Context, all bool, consume func(context.Context, ptrace.Traces) error) error {
	for {
		s.mu.Lock()
		now := time.Now().UnixNano()
		var batch []*pendingTrace
		for len(s.deadlines) > 0 && len(batch) < 256 && (all || s.deadlines[0].due <= now) {
			v := heap.Pop(&s.deadlines).(*pendingTrace)
			delete(s.views, v.id)
			batch = append(batch, v)
		}
		s.mu.Unlock()
		if len(batch) == 0 {
			return nil
		}
		td := ptrace.NewTraces()
		for _, v := range batch {
			if e := v.appendTo(td); e != nil {
				return e
			}
		}
		if s.plan.full {
			if e := appendFullInputs(td, batch); e != nil {
				return e
			}
		}
		if e := consume(ctx, td); e != nil {
			return e
		}
	}
}
func (s *spool) dispatch(ctx context.Context, consume func(context.Context, ptrace.Traces) error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e := s.submitDue(ctx, false, consume); e != nil {
				s.mu.Lock()
				s.err = e
				s.mu.Unlock()
				return
			}
		}
	}
}
