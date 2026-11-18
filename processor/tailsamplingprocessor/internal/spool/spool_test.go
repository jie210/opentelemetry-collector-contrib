// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"context"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type spanSink struct {
	mu    sync.Mutex
	spans map[pcommon.SpanID]int
}

func (s *spanSink) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (s *spanSink) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				s.spans[span.SpanID()]++
			}
		}
	}
	return nil
}

// keepTrace is the model sampler's decision.
func keepTrace(id pcommon.TraceID) bool { return id[0]%2 == 0 }

type modelSampler struct {
	s       *Spool
	pending map[pcommon.TraceID]bool
	decided map[pcommon.TraceID]bool
}

func (m *modelSampler) process(r *Record, ids map[pcommon.TraceID]int) {
	m.s.Begin(r)
	for id, i := range ids {
		if keep, ok := m.decided[id]; ok {
			m.s.Inherit(r, i, keep)
			continue
		}
		m.s.Attach(r, i)
		m.pending[id] = true
	}
	m.s.End(r)
}

func (m *modelSampler) tick(r *rand.Rand) {
	for id := range m.pending {
		if r.IntN(3) == 0 {
			m.s.Decide(id, keepTrace(id))
			m.decided[id] = keepTrace(id)
			delete(m.pending, id)
		}
	}
	m.s.Flush()
}

func testConfig(dir string) Config {
	return Config{
		Directory:          dir,
		SegmentBytes:       4 << 10,
		CheckpointBytes:    8 << 10,
		MaxInflightRecords: 2,
		MaxInflightBytes:   1 << 20,
		KeepMemo:           1 << 20,
		DropMemo:           1 << 20,
	}
}

// crash abandons the spool as a killed process would: buffered log frames
// are lost and nothing is flushed.
func crash(s *Spool) {
	s.mu.Lock()
	s.stopping = true
	s.readyCond.Broadcast()
	s.mu.Unlock()
	s.workers.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.f.Close()
	for _, seg := range s.segs {
		seg.f.Close()
	}
}

func makeRequest(r *rand.Rand, traces []pcommon.TraceID, nextSpan *uint64) (ptrace.Traces, []pcommon.TraceID) {
	td := ptrace.NewTraces()
	var ids []pcommon.TraceID
	seen := map[pcommon.TraceID]bool{}
	for range 1 + r.IntN(2) {
		ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		for range 1 + r.IntN(4) {
			id := traces[r.IntN(len(traces))]
			span := ss.Spans().AppendEmpty()
			span.SetTraceID(id)
			*nextSpan++
			var sid pcommon.SpanID
			for i := range 8 {
				sid[i] = byte(*nextSpan >> (8 * i))
			}
			span.SetSpanID(sid)
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	return td, ids
}

func TestSpoolCrashRecoveryDeliversKeptSpans(t *testing.T) {
	for seed := range uint64(20) {
		r := rand.New(rand.NewPCG(seed, 99))
		dir := t.TempDir()
		sink := &spanSink{spans: map[pcommon.SpanID]int{}}
		expect := map[pcommon.SpanID]bool{}

		open := func() (*Spool, *modelSampler) {
			s, rec, err := Open(testConfig(dir), zap.NewNop(), sink)
			require.NoError(t, err)
			m := &modelSampler{s: s, pending: map[pcommon.TraceID]bool{}, decided: map[pcommon.TraceID]bool{}}
			for _, e := range rec.Memo {
				m.decided[e.ID] = e.Keep
			}
			s.Start()
			for _, rr := range rec.Refeed {
				m.process(rr.Record, rr.IDs)
			}
			return s, m
		}

		s, m := open()
		traces := make([]pcommon.TraceID, 40)
		for i := range traces {
			traces[i][0] = byte(i)
			traces[i][1] = byte(seed)
		}
		var nextSpan uint64
		var m2 sync.Mutex
		for step := range 400 {
			td, ids := makeRequest(r, traces, &nextSpan)
			var u ptrace.ProtoMarshaler
			raw, err := u.MarshalTraces(td)
			require.NoError(t, err)
			rec, err := s.Register(raw, ids, time.Now())
			require.NoError(t, err)
			for _, rs := range td.ResourceSpans().All() {
				for _, span := range rs.ScopeSpans().At(0).Spans().All() {
					m2.Lock()
					expect[span.SpanID()] = keepTrace(span.TraceID())
					m2.Unlock()
				}
			}
			idx := map[pcommon.TraceID]int{}
			for i, id := range ids {
				idx[id] = i
			}
			m.process(rec, idx)
			if r.IntN(4) == 0 {
				m.tick(r)
			}
			if step%97 == 96 {
				crash(s)
				s, m = open()
			}
		}
		for id := range m.pending {
			s.Decide(id, keepTrace(id))
		}
		require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 10*time.Second, 5*time.Millisecond)
		require.NoError(t, s.Stop())

		sink.mu.Lock()
		for sid, keep := range expect {
			if keep {
				require.GreaterOrEqual(t, sink.spans[sid], 1, "seed %d: kept span %v lost", seed, sid)
			} else {
				require.Zero(t, sink.spans[sid], "seed %d: dropped span %v delivered", seed, sid)
			}
		}
		sink.mu.Unlock()
	}
}

// snapshot returns the settlement of every live association, with pending
// and unsettled reported as unsettled, and the memo without IDs that have a
// pending instance.
func snapshot(s *Spool) (map[uint64][]assocState, []memoEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	states := map[uint64][]assocState{}
	open := map[pcommon.TraceID]bool{}
	for seq, r := range s.records {
		st := make([]assocState, len(r.state))
		for i, a := range r.state {
			st[i] = a
			if a == pending || a == unsettled {
				st[i] = unsettled
				open[r.ids[i]] = true
			}
		}
		states[seq] = st
	}
	var memo []memoEntry
	for _, e := range s.memo.entries() {
		if !open[e.ID] {
			memo = append(memo, e)
		}
	}
	return states, memo
}

func TestReplayMatchesRuntimeState(t *testing.T) {
	for seed := range uint64(30) {
		r := rand.New(rand.NewPCG(seed, 7))
		dir := t.TempDir()
		cfg := testConfig(dir)
		cfg.KeepMemo, cfg.DropMemo = 8, 5
		sink := &spanSink{spans: map[pcommon.SpanID]int{}}
		s, _, err := Open(cfg, zap.NewNop(), sink)
		require.NoError(t, err)
		m := &modelSampler{s: s, pending: map[pcommon.TraceID]bool{}, decided: map[pcommon.TraceID]bool{}}

		// process handles a record like the processor does, with capacity
		// evictions and size-limit drops in the middle of the record.
		process := func(rec *Record, ids map[pcommon.TraceID]int) {
			s.Begin(rec)
			for i, id := range rec.ids {
				if _, ok := ids[id]; !ok {
					continue
				}
				if r.IntN(8) == 0 && len(m.pending) > 0 {
					for victim := range m.pending {
						s.Evict(victim)
						delete(m.pending, victim)
						break
					}
				}
				if keep, ok := m.decided[id]; ok {
					s.Inherit(rec, i, keep)
					continue
				}
				s.Attach(rec, i)
				m.pending[id] = true
				if r.IntN(10) == 0 {
					s.Decide(id, false)
					m.decided[id] = false
					delete(m.pending, id)
				}
			}
			s.End(rec)
		}

		traces := make([]pcommon.TraceID, 12)
		for i := range traces {
			traces[i][0] = byte(i)
		}
		var next uint64
		for step := range 300 {
			td, ids := makeRequest(r, traces, &next)
			var u ptrace.ProtoMarshaler
			raw, err := u.MarshalTraces(td)
			require.NoError(t, err)
			rec, err := s.Register(raw, ids, time.Now())
			require.NoError(t, err)
			all := map[pcommon.TraceID]int{}
			for i, id := range ids {
				all[id] = i
			}
			process(rec, all)
			if r.IntN(3) == 0 {
				m.tick(r)
			}
			if r.IntN(5) == 0 {
				// Forget decisions like an LRU eviction would.
				for id := range m.decided {
					if r.IntN(3) == 0 {
						delete(m.decided, id)
					}
				}
			}
			if step%41 == 40 {
				s.Flush()
				wantStates, wantMemo := snapshot(s)
				crash(s)
				var got *Recovered
				s, got, err = Open(cfg, zap.NewNop(), sink)
				require.NoError(t, err)
				gotStates, gotMemo := snapshot(s)
				require.Equal(t, wantStates, gotStates, "seed %d step %d", seed, step)
				require.Equal(t, wantMemo, gotMemo, "seed %d step %d", seed, step)

				m = &modelSampler{s: s, pending: map[pcommon.TraceID]bool{}, decided: map[pcommon.TraceID]bool{}}
				for _, e := range got.Memo {
					m.decided[e.ID] = e.Keep
				}
				for _, rr := range got.Refeed {
					process(rr.Record, rr.IDs)
				}
			}
		}
		require.NoError(t, s.Stop())
	}
}

func TestSpoolRemovesSegmentsAfterCommit(t *testing.T) {
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	s.Start()
	r := rand.New(rand.NewPCG(1, 2))
	traces := []pcommon.TraceID{{2}, {3}}
	var next uint64
	for range 200 {
		td, ids := makeRequest(r, traces, &next)
		var u ptrace.ProtoMarshaler
		raw, err := u.MarshalTraces(td)
		require.NoError(t, err)
		rec, err := s.Register(raw, ids, time.Now())
		require.NoError(t, err)
		s.Begin(rec)
		for i, id := range ids {
			s.Inherit(rec, i, keepTrace(id))
		}
	}
	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, 5*time.Millisecond)
	st := s.Stats()
	require.Equal(t, 1, st.LiveSegments, "only the active segment remains")
	require.Positive(t, st.Checkpoints)
	require.NoError(t, s.Stop())
}

func TestSpoolIgnoresTornTail(t *testing.T) {
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	cfg := testConfig(dir)
	cfg.CheckpointBytes = 0
	s, _, err := Open(cfg, zap.NewNop(), sink)
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(3, 4))
	var next uint64
	td, ids := makeRequest(r, []pcommon.TraceID{{4}}, &next)
	var u ptrace.ProtoMarshaler
	raw, err := u.MarshalTraces(td)
	require.NoError(t, err)
	rec, err := s.Register(raw, ids, time.Now())
	require.NoError(t, err)
	s.Begin(rec)
	s.Attach(rec, 0)
	s.Flush()
	crash(s)

	f, err := os.OpenFile(logPath(dir), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{40, 0, 0, 0, 1, 2})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s, got, err := Open(cfg, zap.NewNop(), sink)
	require.NoError(t, err)
	require.Len(t, got.Refeed, 1)
	require.Equal(t, rec.Seq(), got.Refeed[0].Record.Seq())
	s.Start()
	s.Begin(got.Refeed[0].Record)
	s.Attach(got.Refeed[0].Record, 0)
	s.Decide(pcommon.TraceID{4}, true)
	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, s.Stop())
	require.Len(t, sink.spans, td.SpanCount())
}

func TestFrameRoundTrip(t *testing.T) {
	frames := []frame{
		{kind: frameHeader, seq: 7},
		{kind: frameRecord, seq: 9, seg: 3, off: 1 << 33, length: 77, arrival: -5, ids: []pcommon.TraceID{{1}, {2}}},
		{kind: frameKeep, seq: 4, id: pcommon.TraceID{9}},
		{kind: frameDrop, seq: 4, id: pcommon.TraceID{9}},
		{kind: frameEvict, seq: 4, id: pcommon.TraceID{9}},
		{kind: frameLate, seq: 4, id: pcommon.TraceID{9}, keep: true},
		{kind: frameCommit, seq: 11},
		{kind: frameWatermark, seq: 12},
		{kind: frameMemo, id: pcommon.TraceID{5}, keep: true},
	}
	for _, f := range frames {
		got, err := decodeFrame(f.encode(nil))
		require.NoError(t, err)
		if len(f.ids) == 0 && f.kind != frameRecord {
			got.ids = nil
		}
		require.Equal(t, f, got)
	}
	_, err := decodeFrame([]byte{frameKeep, 1, 2})
	require.ErrorIs(t, err, errCorrupt)
}
