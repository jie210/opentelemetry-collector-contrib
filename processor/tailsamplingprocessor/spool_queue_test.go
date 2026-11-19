// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"
)

// blockLoop requests a tick and leaves its completion unread, which holds the
// processor's loop until the returned function is called.
func blockLoop(c *testTSPController) func() {
	done := make(chan struct{})
	c.tickChan <- done
	return func() { <-done }
}

func singleSpan(id byte) ptrace.Traces {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{id})
	span.SetSpanID(pcommon.SpanID{id})
	return td
}

func TestSpoolAcknowledgesWhileLoopIsBusy(t *testing.T) {
	sink := newSpanCounter()
	p, c := newAlwaysSampleSpool(t, sink)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	unblock := blockLoop(c)
	for i := range 200 {
		require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(byte(i))))
	}
	require.Equal(t, 200, spoolOf(p).Stats().UnprocessedRecords)
	unblock()

	waitSpoolTaken(t, p)
	for range 2 {
		c.waitForTick()
	}
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return len(spans) == 200
	}, 5*time.Second, time.Millisecond)
}

func TestSpoolKeepsArrivalOfWaitingRecords(t *testing.T) {
	p, c := newAlwaysSampleSpool(t, newSpanCounter(), func(cfg *Config) {
		cfg.DecisionWait = 3 * time.Second
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	sp := spoolOf(p)

	unblock := blockLoop(c)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(1)))
	time.Sleep(2100 * time.Millisecond)
	unblock()
	waitSpoolTaken(t, p)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(2)))
	require.Eventually(t, func() bool { return sp.Stats().PendingTraces == 2 }, 5*time.Second, time.Millisecond)

	// The first trace waited two of its three seconds before it was
	// processed, the second just arrived.
	c.waitForTick()
	require.Equal(t, 1, sp.Stats().PendingTraces)
}

func TestSpoolRetriesUnreadableWaitingRecord(t *testing.T) {
	sink := newSpanCounter()
	dir := t.TempDir()
	p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
		cfg.Spool = &SpoolConfig{Directory: dir, MaxUnprocessedMemory: 1}
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	sp := spoolOf(p)

	unblock := blockLoop(c)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(1)))
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(2)))
	segments, err := filepath.Glob(filepath.Join(dir, "segments", "*.otlp"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	data, err := os.ReadFile(segments[0])
	require.NoError(t, err)
	require.NoError(t, os.Truncate(segments[0], 0))
	unblock()

	require.Eventually(t, func() bool { return sp.Stats().ReadFailures >= 2 }, 5*time.Second, time.Millisecond)
	st := sp.Stats()
	require.Equal(t, 2, st.UnprocessedRecords, "the record is not skipped")
	require.Zero(t, st.Committed)

	require.NoError(t, os.WriteFile(segments[0], data, 0o640))
	waitSpoolTaken(t, p)
	for range 2 {
		c.waitForTick()
	}
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return len(spans) == 2
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 2, sp.Stats().UnprocessedReads)
}

func TestSpoolReleasesWaitingRecordThatStaysUnreadable(t *testing.T) {
	sink := newSpanCounter()
	dir := t.TempDir()
	p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
		cfg.Spool = &SpoolConfig{Directory: dir, MaxUnprocessedMemory: 1}
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	sp := spoolOf(p)

	unblock := blockLoop(c)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(1)))
	segments, err := filepath.Glob(filepath.Join(dir, "segments", "*.otlp"))
	require.NoError(t, err)
	require.NoError(t, os.Truncate(segments[0], 0))
	unblock()

	require.Eventually(t, func() bool { return sp.Stats().ReleasedUnreadable == 1 }, 10*time.Second, time.Millisecond)
	require.Zero(t, sp.Stats().UnprocessedRecords)

	// Later requests are processed again.
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(2)))
	waitSpoolTaken(t, p)
	for range 2 {
		c.waitForTick()
	}
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return spans[pcommon.SpanID{2}] == 1
	}, 5*time.Second, time.Millisecond)
	spans, _ := sink.snapshot()
	require.Zero(t, spans[pcommon.SpanID{1}])
}

// deepSpan returns a span whose attributes nest deeper than the protobuf
// decoder accepts; it marshals fine.
func deepSpan(trace, span byte, isErr bool) ptrace.Traces {
	td := spanOf(trace, span, isErr)
	m := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutEmptyMap("k")
	for range 40 {
		m = m.PutEmptyMap("k")
	}
	return td
}

func spanOf(trace, span byte, isErr bool) ptrace.Traces {
	td := ptrace.NewTraces()
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetTraceID(pcommon.TraceID{trace})
	s.SetSpanID(pcommon.SpanID{span})
	if isErr {
		s.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

// An undecodable request is dropped once, waiting or kept, and does not
// hold back the fragments registered after it.
func TestSpoolDropsUndecodableRecord(t *testing.T) {
	for name, memory := range map[string]int64{"read back": 1, "kept in memory": 0} {
		t.Run(name, func(t *testing.T) {
			sink := newSpanCounter()
			p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
				cfg.DecisionWait = 2 * time.Second
				cfg.PolicyCfgs = []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}}}
				cfg.Spool = &SpoolConfig{Directory: t.TempDir(), MaxUnprocessedMemory: memory}
			})
			defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
			sp := spoolOf(p)

			require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 1, false)))
			waitSpoolTaken(t, p)
			unblock := blockLoop(c)
			require.NoError(t, p.ConsumeTraces(context.Background(), deepSpan(2, 2, true)))
			require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 3, true)))
			unblock()
			waitSpoolTaken(t, p)
			for range 4 {
				c.waitForTick()
			}

			require.Eventually(t, func() bool {
				st := sp.Stats()
				return st.LiveRecords == 0 && st.ReleasedUnreadable == 1
			}, 5*time.Second, time.Millisecond)
			require.Equal(t, uint64(1), sp.Stats().ReadFailures, "not retried")
			spans, _ := sink.snapshot()
			require.Equal(t, 1, spans[pcommon.SpanID{1}])
			require.Equal(t, 1, spans[pcommon.SpanID{3}])
			require.Zero(t, spans[pcommon.SpanID{2}])
		})
	}
}

func errorPolicy() []PolicyCfg {
	return []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}}}
}

func marshalTraces(t *testing.T, td ptrace.Traces) []byte {
	var m ptrace.ProtoMarshaler
	data, err := m.MarshalTraces(td)
	require.NoError(t, err)
	return data
}

// truncateSegmentOf empties the segment holding only the request want and
// returns its path.
func truncateSegmentOf(t *testing.T, dir string, want []byte) string {
	segments, err := filepath.Glob(filepath.Join(dir, "segments", "*.otlp"))
	require.NoError(t, err)
	for _, seg := range segments {
		data, err := os.ReadFile(seg)
		require.NoError(t, err)
		if bytes.Equal(data, want) {
			require.NoError(t, os.Truncate(seg, 0))
			return seg
		}
	}
	require.FailNow(t, "no segment holds only the request")
	return ""
}

// A trace due while fragments of it are queued waits for them, so the error
// span of trace 1, acknowledged within its decision window, counts however
// long the requests ahead of it take. Trace 1's deadline passes many times
// over meanwhile. Trace 4, with nothing queued, is decided on time.
func TestSpoolTickWaitsForQueuedFragments(t *testing.T) {
	cases := []struct {
		name string
		// fillers are requests of unrelated traces between the spans.
		fillers int
		// fault empties the segment of request b, the only filler;
		// recover restores it once its read failed.
		fault, recover bool
	}{
		{name: "read retried then released", fillers: 1, fault: true},
		{name: "read retried then recovered", fillers: 1, fault: true, recover: true},
		{name: "backlog read back", fillers: 20000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := newSpanCounter()
			dir := t.TempDir()
			p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
				cfg.Options = append(cfg.Options, withTickerFrequency(2*time.Millisecond))
				cfg.DecisionWait = time.Second
				cfg.NumTraces = 100000
				cfg.PolicyCfgs = errorPolicy()
				cfg.Spool = &SpoolConfig{Directory: dir, MaxUnprocessedMemory: 1}
				if tc.fault {
					cfg.Spool.SegmentSize = 1
				}
			})
			defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
			sp := spoolOf(p)

			fillers := make([]ptrace.Traces, tc.fillers)
			for i := range fillers {
				fillers[i] = ptrace.NewTraces()
				span := fillers[i].ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
				span.SetTraceID(pcommon.TraceID{2, byte(i >> 16), byte(i >> 8), byte(i)})
				span.SetSpanID(pcommon.SpanID{2, byte(i >> 16), byte(i >> 8), byte(i)})
			}
			bData := marshalTraces(t, fillers[0])

			unblock := blockLoop(c)
			require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 1, false)))
			require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(4, 4, true)))
			for _, td := range fillers {
				require.NoError(t, p.ConsumeTraces(context.Background(), td))
			}
			require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 3, true)))
			var seg string
			if tc.fault {
				seg = truncateSegmentOf(t, dir, bData)
			}
			unblock()
			if tc.recover {
				require.Eventually(t, func() bool { return sp.Stats().ReadFailures >= 1 }, 5*time.Second, time.Millisecond)
				require.NoError(t, os.WriteFile(seg, bData, 0o640))
			}
			var early map[pcommon.SpanID]int
			require.Eventually(t, func() bool {
				early, _ = sink.snapshot()
				return early[pcommon.SpanID{4}] == 1
			}, 5*time.Second, time.Millisecond)
			require.Zero(t, early[pcommon.SpanID{3}], "trace 4 is not held with trace 1")

			require.Eventually(t, func() bool {
				st := sp.Stats()
				return st.UnprocessedRecords == 0 && st.PendingTraces == 0 && st.LiveRecords == 0
			}, 10*time.Second, time.Millisecond)
			spans, _ := sink.snapshot()
			require.Equal(t, map[pcommon.SpanID]int{{1}: 1, {3}: 1, {4}: 1}, spans)
			if tc.fault && !tc.recover {
				require.EqualValues(t, 1, sp.Stats().ReleasedUnreadable)
			} else {
				require.Zero(t, sp.Stats().ReleasedUnreadable)
			}
		})
	}
}

// Waiting for space runs ticks while later requests are queued; they are
// not postponed, or the loop would wait for itself.
func TestSpoolTickWhileWaitingForSpace(t *testing.T) {
	sink := newSpanCounter()
	p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
		cfg.Options = append(cfg.Options, withTickerFrequency(20*time.Millisecond))
		cfg.NumTraces = 1
		cfg.BlockOnOverflow = true
		cfg.DecisionCache = DecisionCacheConfig{SampledCacheSize: 10, NonSampledCacheSize: 10}
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	unblock := blockLoop(c)
	for i := range 3 {
		require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(byte(i+1))))
	}
	unblock()
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return len(spans) == 3
	}, 10*time.Second, time.Millisecond)
}

func TestSpoolBackpressure(t *testing.T) {
	p, c := newAlwaysSampleSpool(t, newSpanCounter(), func(cfg *Config) {
		cfg.Spool.MaxUnprocessedRecords = 1
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	unblock := blockLoop(c)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(1)))
	require.ErrorIs(t, p.ConsumeTraces(context.Background(), singleSpan(2)), spool.ErrBackpressure)
	unblock()
	waitSpoolTaken(t, p)
	require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(2)))
	require.EqualValues(t, 1, spoolOf(p).Stats().RejectedBackpressure)
}
