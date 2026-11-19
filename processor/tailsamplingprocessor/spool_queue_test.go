// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// blockLoop requests a tick and leaves its completion unread, which holds the
// processor's loop until the returned function is called.
func blockLoop(c *testTSPController) func() {
	done := make(chan struct{})
	c.tickChan <- done
	return func() { <-done }
}

func singleSpan(id byte) ptrace.Traces {
	return numberedSpan(int(id))
}

func numberedSpan(i int) ptrace.Traces {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{byte(i >> 8), byte(i)})
	span.SetSpanID(pcommon.SpanID{byte(i >> 8), byte(i)})
	return td
}

// Like the in-memory processor, a request waits while as many requests as
// there are CPUs wait for the loop, and gives up when its client does.
func TestSpoolWaitsWhileLoopIsBusy(t *testing.T) {
	sink := newSpanCounter()
	p, c := newAlwaysSampleSpool(t, sink)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	sp := spoolOf(p)
	limit := runtime.NumCPU()

	unblock := blockLoop(c)
	for i := range limit {
		require.NoError(t, p.ConsumeTraces(context.Background(), numberedSpan(i)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.ConsumeTraces(ctx, numberedSpan(limit)), context.DeadlineExceeded)
	require.EqualValues(t, limit, sp.Stats().Registered)

	consumed := make(chan error, 1)
	go func() { consumed <- p.ConsumeTraces(context.Background(), numberedSpan(limit)) }()
	select {
	case err := <-consumed:
		require.FailNow(t, "acknowledged while the loop was behind", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-consumed)

	waitSpoolTaken(t, p)
	for range 2 {
		c.waitForTick()
	}
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return len(spans) == limit+1
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

// A request that cannot be read back for delivery is dropped once, and does
// not hold back the others.
func TestSpoolDropsUndecodableRecord(t *testing.T) {
	sink := newSpanCounter()
	p, c := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
		cfg.DecisionWait = 2 * time.Second
		cfg.PolicyCfgs = []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}}}
		cfg.Spool = &SpoolConfig{Directory: t.TempDir(), ReadRetries: 3}
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	sp := spoolOf(p)

	require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 1, false)))
	require.NoError(t, p.ConsumeTraces(context.Background(), deepSpan(2, 2, true)))
	require.NoError(t, p.ConsumeTraces(context.Background(), spanOf(1, 3, true)))
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
}

// While the loop waits for space, requests wait to be registered until its
// ticks free some.
func TestSpoolTickWhileWaitingForSpace(t *testing.T) {
	sink := newSpanCounter()
	p, _ := newAlwaysSampleSpool(t, sink, func(cfg *Config) {
		cfg.Options = append(cfg.Options, withTickerFrequency(20*time.Millisecond))
		cfg.NumTraces = 1
		cfg.BlockOnOverflow = true
		cfg.DecisionCache = DecisionCacheConfig{SampledCacheSize: 10, NonSampledCacheSize: 10}
	})
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	for i := range 3 {
		require.NoError(t, p.ConsumeTraces(context.Background(), singleSpan(byte(i+1))))
	}
	require.Eventually(t, func() bool {
		spans, _ := sink.snapshot()
		return len(spans) == 3
	}, 10*time.Second, time.Millisecond)
}
