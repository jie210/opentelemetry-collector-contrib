// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"context"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type testRequest struct {
	raw    []byte
	assocs []Assoc
	spans  int
}

func newTestRequest(t *testing.T, r *rand.Rand, next *uint64, traces ...pcommon.TraceID) testRequest {
	td, assocs := makeRequest(r, traces, next)
	var u ptrace.ProtoMarshaler
	raw, err := u.MarshalTraces(td)
	require.NoError(t, err)
	return testRequest{raw: raw, assocs: assocs, spans: td.SpanCount()}
}

func (q testRequest) register(s *Spool, ctx context.Context, payload any) (*Record, error) {
	return s.Register(ctx, q.raw, q.assocs, time.Now(), payload)
}

func TestRegisterChecksDeadlineWhenWriting(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(1, 1))
	var next uint64
	req := newTestRequest(t, r, &next, pcommon.TraceID{1})

	// The request expires while it waits for the lock.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s.mu.Lock()
	errc := make(chan error, 1)
	go func() {
		_, err := req.register(s, ctx, nil)
		errc <- err
	}()
	<-ctx.Done()
	s.mu.Unlock()
	require.ErrorIs(t, <-errc, context.DeadlineExceeded)

	short, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	s.mu.Lock()
	s.cfg.MinDeadlineRemaining = time.Second
	s.mu.Unlock()
	_, err = req.register(s, short, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	st := s.Stats()
	require.EqualValues(t, 2, st.RejectedExpired)
	require.Zero(t, st.Registered)
	require.Zero(t, s.active.size)

	_, err = req.register(s, context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, s.Stop())

	s, got, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	require.Len(t, got.Refeed, 1)
	require.NoError(t, s.Stop())
}

func TestRegisterBackpressure(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxUnprocessedRecords = 2
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(2, 2))
	var next uint64
	req := newTestRequest(t, r, &next, pcommon.TraceID{1})
	setLimits := func(records int, bytes, freeDisk int64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cfg.MaxUnprocessedRecords, s.cfg.MaxUnprocessedBytes, s.cfg.MinFreeDisk = records, bytes, freeDisk
		s.freeDiskChecked = time.Time{}
	}

	first, err := req.register(s, context.Background(), nil)
	require.NoError(t, err)
	_, err = req.register(s, context.Background(), nil)
	require.NoError(t, err)
	_, err = req.register(s, context.Background(), nil)
	require.ErrorIs(t, err, ErrBackpressure)
	s.Begin(first)
	_, err = req.register(s, context.Background(), nil)
	require.NoError(t, err)

	setLimits(0, 2*int64(len(req.raw)), 0)
	_, err = req.register(s, context.Background(), nil)
	require.ErrorIs(t, err, ErrBackpressure)

	registered := 3
	if slices.Contains([]string{"darwin", "freebsd", "linux"}, runtime.GOOS) {
		setLimits(0, 0, math.MaxInt64)
		_, err = req.register(s, context.Background(), nil)
		require.ErrorIs(t, err, ErrBackpressure)
		setLimits(0, 0, 1)
		_, err = req.register(s, context.Background(), nil)
		require.NoError(t, err)
		registered++
	}

	st := s.Stats()
	require.EqualValues(t, registered, st.Registered)
	require.EqualValues(t, st.Registered-1, st.UnprocessedRecords)
	require.NoError(t, s.Stop())

	s, got, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	require.Len(t, got.Refeed, registered)
	require.NoError(t, s.Stop())
}

func TestUnprocessedQueue(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	r := rand.New(rand.NewPCG(3, 3))
	var next uint64
	a := newTestRequest(t, r, &next, pcommon.TraceID{1})
	b := newTestRequest(t, r, &next, pcommon.TraceID{2})
	cfg.MaxUnprocessedMemory = int64(len(a.raw))
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)

	ra, err := a.register(s, context.Background(), "a")
	require.NoError(t, err)
	rb, err := b.register(s, context.Background(), "b")
	require.NoError(t, err)
	_, err = s.Register(context.Background(), a.raw, nil, time.Now(), "empty")
	require.NoError(t, err)
	require.Len(t, s.Wake(), 1)
	require.EqualValues(t, len(a.raw), s.Stats().UnprocessedInMemory)

	got, payload, ok := s.Next()
	require.True(t, ok)
	require.Same(t, ra, got)
	require.Equal(t, "a", payload)
	got, _, _ = s.Next()
	require.Same(t, ra, got, "a record stays queued until it is begun")
	<-s.Wake()
	s.Begin(ra)
	require.Len(t, s.Wake(), 1)

	got, payload, ok = s.Next()
	require.True(t, ok)
	require.Same(t, rb, got)
	require.Nil(t, payload, "over the memory budget")
	td, err := s.ReadUnprocessed(rb)
	require.NoError(t, err)
	require.Equal(t, b.spans, td.SpanCount())
	s.Begin(rb)

	_, _, ok = s.Next()
	require.False(t, ok, "a record without associations is not queued")
	st := s.Stats()
	require.Zero(t, st.UnprocessedRecords)
	require.Zero(t, st.UnprocessedBytes)
	require.Zero(t, st.UnprocessedInMemory)
	require.EqualValues(t, 1, st.UnprocessedReads)
	require.NoError(t, s.Stop())
}

func TestCheckpointKeepsFramesWrittenMeanwhile(t *testing.T) {
	for seed := range uint64(10) {
		r := rand.New(rand.NewPCG(seed, 11))
		dir := t.TempDir()
		cfg := testConfig(dir)
		sink := &spanSink{spans: map[pcommon.SpanID]int{}}
		s, _, err := Open(cfg, zap.NewNop(), sink)
		require.NoError(t, err)
		written := make(chan struct{})
		release := make(chan struct{})
		var hold sync.Once
		s.snapshotWritten = func() {
			hold.Do(func() {
				written <- struct{}{}
				<-release
			})
		}
		m := &modelSampler{s: s, pending: map[pcommon.TraceID]bool{}, decided: map[pcommon.TraceID]bool{}}
		traces := make([]pcommon.TraceID, 10)
		for i := range traces {
			traces[i][0] = byte(i)
		}
		var next uint64
		step := func() {
			req := newTestRequest(t, r, &next, traces...)
			rec, err := req.register(s, context.Background(), nil)
			require.NoError(t, err)
			m.process(rec, allAssocs(rec))
			if r.IntN(3) == 0 {
				m.tick(r)
			}
		}
		for s.Stats().Checkpoints < 2 {
			step()
			select {
			case <-written:
				// The log keeps changing while the snapshot waits.
				for range 30 {
					step()
				}
				close(release)
				require.Eventually(t, func() bool { return s.Stats().Checkpoints >= 2 }, 5*time.Second, time.Millisecond)
			default:
			}
		}

		s.Flush()
		wantStates, wantMemo := snapshot(s)
		crash(s)
		s, _, err = Open(cfg, zap.NewNop(), sink)
		require.NoError(t, err)
		gotStates, gotMemo := snapshot(s)
		require.Equal(t, wantStates, gotStates, "seed %d", seed)
		require.Equal(t, wantMemo, gotMemo, "seed %d", seed)
		require.NoError(t, s.Stop())
	}
}

func TestLargeSnapshotIsNotRewrittenOnEveryRequest(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.CheckpointBytes = 1 << 10
	s, _, err := Open(cfg, zap.NewNop(), &spanSink{spans: map[pcommon.SpanID]int{}})
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(4, 4))
	var next uint64
	// Undecided records keep the live state above checkpoint_size.
	for i := range 300 {
		req := newTestRequest(t, r, &next, pcommon.TraceID{byte(i), 1})
		_, err := req.register(s, context.Background(), nil)
		require.NoError(t, err)
	}
	s.background.Wait()
	s.mu.Lock()
	snapshot, size := s.lastSnapshot, s.log.size
	s.mu.Unlock()
	require.Greater(t, snapshot, 8*cfg.CheckpointBytes)
	require.Less(t, s.Stats().Checkpoints, uint64(10), "the threshold grows with the snapshot")
	require.LessOrEqual(t, size, 2*snapshot)
	require.NoError(t, s.Stop())
}
