// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var errInjected = errors.New("injected read failure")

func fastRetries(t *testing.T) {
	interval, base := recoveredReadInterval, readBackoffBase
	recoveredReadInterval, readBackoffBase = time.Millisecond, time.Millisecond
	t.Cleanup(func() { recoveredReadInterval, readBackoffBase = interval, base })
}

// failingReads makes the first n reads fail, or every read when n < 0.
func failingReads(s *Spool, n int64) *atomic.Int64 {
	var calls atomic.Int64
	s.readAt = func(seg *segment, buf []byte, off int64) (int, error) {
		if c := calls.Add(1); n < 0 || c <= n {
			return 0, errInjected
		}
		return readSegment(seg, buf, off)
	}
	return &calls
}

// registerKept registers count single-trace requests and keeps them.
func registerKept(t *testing.T, s *Spool, count int) (spans int) {
	r := rand.New(rand.NewPCG(5, 6))
	var next uint64
	for i := range count {
		id := pcommon.TraceID{byte(2 * (i + 1))}
		td, ids := makeRequest(r, []pcommon.TraceID{id}, &next)
		var u ptrace.ProtoMarshaler
		raw, err := u.MarshalTraces(td)
		require.NoError(t, err)
		rec, err := s.Register(context.Background(), raw, ids, time.Now(), nil)
		require.NoError(t, err)
		s.Begin(rec)
		for j := range ids {
			s.Attach(rec, j)
		}
		s.End(rec)
		s.Decide(id, true)
		spans += td.SpanCount()
	}
	s.Flush()
	return spans
}

func sinkSpans(s *spanSink) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spans)
}

func TestRuntimeReadFailureRetriesWithoutCommit(t *testing.T) {
	fastRetries(t)
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(testConfig(t.TempDir()), zap.NewNop(), sink)
	require.NoError(t, err)
	failingReads(s, 3)
	s.Start()
	spans := registerKept(t, s, 1)
	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, time.Millisecond)
	st := s.Stats()
	require.Equal(t, uint64(3), st.ReadFailures)
	require.Zero(t, st.ReleasedUnreadable)
	require.Equal(t, spans, sinkSpans(sink))
	require.NoError(t, s.Stop())
}

func TestPersistentReadFailureIsReleasedAfterRestart(t *testing.T) {
	fastRetries(t)
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}

	// A run that cannot read keeps retrying and never commits.
	s, _, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	failingReads(s, -1)
	s.Start()
	spans := registerKept(t, s, 2)
	require.Eventually(t, func() bool { return s.Stats().ReadFailures >= 6 }, 5*time.Second, time.Millisecond)
	require.Equal(t, 2, s.Stats().LiveRecords)
	crash(s)

	// A restart that can read delivers everything.
	readable := t.TempDir()
	require.NoError(t, os.CopyFS(readable, os.DirFS(dir)))
	s, _, err = Open(testConfig(readable), zap.NewNop(), sink)
	require.NoError(t, err)
	s.Start()
	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, time.Millisecond)
	require.Equal(t, spans, sinkSpans(sink))
	require.NoError(t, s.Stop())

	// A restart that still cannot read releases the records after a few
	// attempts each.
	empty := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err = Open(testConfig(dir), zap.NewNop(), empty)
	require.NoError(t, err)
	failingReads(s, -1)
	s.Start()
	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, time.Millisecond)
	st := s.Stats()
	require.Equal(t, uint64(2), st.ReleasedUnreadable)
	require.LessOrEqual(t, st.ReadFailures, uint64(2*recoveredReadAttempts))
	require.Zero(t, sinkSpans(empty))
	require.NoError(t, s.Stop())

	s, got, err := Open(testConfig(dir), zap.NewNop(), empty)
	require.NoError(t, err)
	require.Empty(t, got.Refeed)
	require.Zero(t, s.Stats().LiveRecords)
	require.NoError(t, s.Stop())
}

func TestReadRecoveredReleasesUnreadableRecord(t *testing.T) {
	fastRetries(t)
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(7, 8))
	var next uint64
	for range 2 {
		td, ids := makeRequest(r, []pcommon.TraceID{{4}}, &next)
		var u ptrace.ProtoMarshaler
		raw, err := u.MarshalTraces(td)
		require.NoError(t, err)
		rec, err := s.Register(context.Background(), raw, ids, time.Now(), nil)
		require.NoError(t, err)
		s.Begin(rec)
		s.Attach(rec, 0)
		s.End(rec)
	}
	s.Flush()
	crash(s)

	s, got, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	require.Len(t, got.Refeed, 2)
	require.Same(t, got.Refeed[0].Record.seg, got.Refeed[1].Record.seg)
	calls := failingReads(s, -1)
	_, err = s.ReadRecovered(got.Refeed[0].Record)
	require.ErrorIs(t, err, errInjected)
	require.Equal(t, int64(recoveredReadAttempts), calls.Load())
	// The other record of the same segment gets a single attempt.
	_, err = s.ReadRecovered(got.Refeed[1].Record)
	require.ErrorIs(t, err, errInjected)
	require.Equal(t, int64(recoveredReadAttempts+1), calls.Load())
	require.Equal(t, uint64(2), s.Stats().ReleasedUnreadable)
	require.Zero(t, s.Stats().LiveRecords)
	crash(s)

	s, got, err = Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	require.Empty(t, got.Refeed)
	require.NoError(t, s.Stop())
}

// deepRequest returns a single-span request whose attributes nest deeper
// than the protobuf decoder accepts, although it marshals fine.
func deepRequest(t *testing.T, id pcommon.TraceID) ([]byte, []Assoc) {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(id)
	span.SetSpanID(pcommon.SpanID{id[0]})
	m := span.Attributes().PutEmptyMap("k")
	for range 40 {
		m = m.PutEmptyMap("k")
	}
	var u ptrace.ProtoMarshaler
	raw, err := u.MarshalTraces(td)
	require.NoError(t, err)
	return raw, []Assoc{{ID: id}}
}

func TestUndecodableRecordIsReleasedAtDelivery(t *testing.T) {
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	s.Start()
	id := pcommon.TraceID{1}
	raw, ids := deepRequest(t, id)
	rec, err := s.Register(context.Background(), raw, ids, time.Now(), nil)
	require.NoError(t, err)
	s.Begin(rec)
	s.Attach(rec, 0)
	s.End(rec)
	s.Decide(id, true)
	spans := registerKept(t, s, 1)

	require.Eventually(t, func() bool { return s.Stats().LiveRecords == 0 }, 5*time.Second, time.Millisecond)
	st := s.Stats()
	require.Equal(t, uint64(1), st.ReadFailures, "not retried")
	require.Equal(t, uint64(1), st.ReleasedUnreadable)
	require.Equal(t, spans, sinkSpans(sink))
	require.NoError(t, s.Stop())

	s, got, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	require.Empty(t, got.Refeed)
	require.Zero(t, s.Stats().LiveRecords)
	require.NoError(t, s.Stop())
}

func TestReadRecoveredReleasesUndecodableRecordAtOnce(t *testing.T) {
	dir := t.TempDir()
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	raw, ids := deepRequest(t, pcommon.TraceID{1})
	r := rand.New(rand.NewPCG(11, 12))
	var next uint64
	td, okIDs := makeRequest(r, []pcommon.TraceID{{2}}, &next)
	var u ptrace.ProtoMarshaler
	okRaw, err := u.MarshalTraces(td)
	require.NoError(t, err)
	for _, req := range []struct {
		raw []byte
		ids []Assoc
	}{{raw, ids}, {okRaw, okIDs}} {
		rec, err := s.Register(context.Background(), req.raw, req.ids, time.Now(), nil)
		require.NoError(t, err)
		s.Begin(rec)
		s.Attach(rec, 0)
		s.End(rec)
	}
	s.Flush()
	crash(s)

	s, got, err := Open(testConfig(dir), zap.NewNop(), sink)
	require.NoError(t, err)
	require.Len(t, got.Refeed, 2)
	_, err = s.ReadRecovered(got.Refeed[0].Record)
	require.ErrorIs(t, err, errUndecodable)
	require.Equal(t, uint64(1), s.Stats().ReadFailures, "not retried")
	require.Equal(t, uint64(1), s.Stats().ReleasedUnreadable)
	// The segment is fine, so its other record is read normally.
	got1, err := s.ReadRecovered(got.Refeed[1].Record)
	require.NoError(t, err)
	require.Equal(t, td.SpanCount(), got1.SpanCount())
	require.NoError(t, s.Stop())
}

func TestReadUnprocessedReleasesUndecodableHead(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxUnprocessedMemory = 1
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	raw, ids := deepRequest(t, pcommon.TraceID{1})
	bad, err := s.Register(context.Background(), raw, ids, time.Now(), "payload")
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(13, 14))
	var next uint64
	good := newTestRequest(t, r, &next, pcommon.TraceID{2})
	goodRec, err := good.register(s, context.Background(), "payload")
	require.NoError(t, err)

	got, _, ok := s.Next()
	require.True(t, ok)
	require.Same(t, bad, got)
	<-s.Wake()
	_, released, err := s.ReadUnprocessed(bad)
	require.ErrorIs(t, err, errUndecodable)
	require.True(t, released, "released on the first attempt")
	require.Len(t, s.Wake(), 1, "the next record is announced")

	got, _, ok = s.Next()
	require.True(t, ok)
	require.Same(t, goodRec, got)
	td, released, err := s.ReadUnprocessed(goodRec)
	require.NoError(t, err)
	require.False(t, released)
	require.Equal(t, good.spans, td.SpanCount())
	s.Begin(goodRec)
	s.Attach(goodRec, 0)
	s.End(goodRec)
	s.Flush()
	st := s.Stats()
	require.Equal(t, uint64(1), st.ReleasedUnreadable)
	require.Zero(t, st.UnprocessedRecords)
	require.Zero(t, st.UnprocessedBytes)
	crash(s)

	// Only the readable record is processed again after a restart.
	s, rec, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	require.Len(t, rec.Refeed, 1)
	require.Equal(t, goodRec.Seq(), rec.Refeed[0].Record.Seq())
	require.NoError(t, s.Stop())
}

func TestReadUnprocessedReleasesHeadAfterAttempts(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxUnprocessedMemory = 1
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(15, 16))
	var next uint64
	first := newTestRequest(t, r, &next, pcommon.TraceID{1})
	second := newTestRequest(t, r, &next, pcommon.TraceID{2})
	firstRec, err := first.register(s, context.Background(), "payload")
	require.NoError(t, err)
	secondRec, err := second.register(s, context.Background(), "payload")
	require.NoError(t, err)
	failingReads(s, unprocessedReadAttempts)

	for attempt := 1; attempt < unprocessedReadAttempts; attempt++ {
		_, released, err := s.ReadUnprocessed(firstRec)
		require.ErrorIs(t, err, errInjected)
		require.False(t, released, "attempt %d", attempt)
	}
	_, released, err := s.ReadUnprocessed(firstRec)
	require.ErrorIs(t, err, errInjected)
	require.True(t, released)

	got, _, ok := s.Next()
	require.True(t, ok)
	require.Same(t, secondRec, got)
	_, released, err = s.ReadUnprocessed(secondRec)
	require.NoError(t, err)
	require.False(t, released)
	require.Equal(t, uint64(1), s.Stats().ReleasedUnreadable)
	require.NoError(t, s.Stop())
}

func TestOpenReleasesRecordsOfDamagedSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.CheckpointBytes = 0
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, _, err := Open(cfg, zap.NewNop(), sink)
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(9, 10))
	var next uint64
	segOf := map[uint64]uint32{}
	for range 60 {
		td, ids := makeRequest(r, []pcommon.TraceID{{6}, {8}}, &next)
		var u ptrace.ProtoMarshaler
		raw, err := u.MarshalTraces(td)
		require.NoError(t, err)
		rec, err := s.Register(context.Background(), raw, ids, time.Now(), nil)
		require.NoError(t, err)
		segOf[rec.Seq()] = rec.seg.id
		s.Begin(rec)
		for i := range ids {
			s.Attach(rec, i)
		}
		s.End(rec)
	}
	s.Flush()
	crash(s)

	// Remove the first segment and truncate the second.
	require.NoError(t, os.Remove(segmentPath(dir, 1)))
	require.NoError(t, os.Truncate(segmentPath(dir, 2), 10))
	var damaged int
	for _, seg := range segOf {
		if seg <= 2 {
			damaged++
		}
	}
	require.Positive(t, damaged)
	require.Less(t, damaged, len(segOf))

	s, got, err := Open(cfg, zap.NewNop(), sink)
	require.NoError(t, err)
	require.Equal(t, uint64(damaged), s.Stats().ReleasedUnreadable)
	require.Len(t, got.Refeed, len(segOf)-damaged)
	for _, rr := range got.Refeed {
		require.Greater(t, segOf[rr.Record.Seq()], uint32(2))
		_, err := s.ReadRecord(rr.Record)
		require.NoError(t, err)
	}
	require.NoError(t, s.Stop())
}
