// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
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
