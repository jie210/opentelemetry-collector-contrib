// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool

import (
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
		rec, err := s.Register(raw, ids, time.Now())
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
		rec, err := s.Register(raw, ids, time.Now())
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
		rec, err := s.Register(raw, ids, time.Now())
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
