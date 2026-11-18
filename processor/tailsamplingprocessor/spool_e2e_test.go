// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/rawrequest"
)

// withManualTicks makes ingestion synchronous and ticks explicit while
// keeping the real decision batcher, so decision_wait counts ticks.
func withManualTicks(c *testTSPController) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.tickChan = make(chan chan struct{})
		c.tickChan = tsp.tickChan
		tsp.workChan = make(chan []traceBatch)
		tsp.tickerFrequency = time.Hour
	}
}

type spanCounter struct {
	mu    sync.Mutex
	spans map[pcommon.SpanID]int
	body  map[pcommon.SpanID]string
}

func newSpanCounter() *spanCounter {
	return &spanCounter{spans: map[pcommon.SpanID]int{}, body: map[pcommon.SpanID]string{}}
}

func (*spanCounter) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (s *spanCounter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				s.spans[span.SpanID()]++
				s.body[span.SpanID()] = fmt.Sprintf("%v|%s|%v|%v|%d", rs.Resource().Attributes().AsRaw(), ss.Scope().Name(),
					span.Name(), span.Attributes().AsRaw(), span.Status().Code())
			}
		}
	}
	return nil
}

func (s *spanCounter) snapshot() (map[pcommon.SpanID]int, map[pcommon.SpanID]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spans := make(map[pcommon.SpanID]int, len(s.spans))
	for k, v := range s.spans {
		spans[k] = v
	}
	body := make(map[pcommon.SpanID]string, len(s.body))
	for k, v := range s.body {
		body[k] = v
	}
	return spans, body
}

// genSteps produces requests grouped by tick. Spans of one trace spread over
// several requests and ticks, some after the trace is decided.
func genSteps(r *rand.Rand, numTraces, numSteps int) [][]ptrace.Traces {
	type genSpan struct {
		trace   pcommon.TraceID
		span    pcommon.SpanID
		root    bool
		service int
	}
	perStep := make([][]genSpan, numSteps)
	var spanSeq uint64
	for range numTraces {
		var id pcommon.TraceID
		for i := range id {
			id[i] = byte(r.IntN(256))
		}
		first := r.IntN(numSteps)
		n := 1 + r.IntN(8)
		for k := range n {
			step := min(numSteps-1, first+r.IntN(4))
			spanSeq++
			var sid pcommon.SpanID
			for i := range 8 {
				sid[i] = byte(spanSeq >> (8 * i))
			}
			perStep[step] = append(perStep[step], genSpan{trace: id, span: sid, root: k == 0, service: r.IntN(3)})
		}
	}

	steps := make([][]ptrace.Traces, numSteps)
	for s, spans := range perStep {
		r.Shuffle(len(spans), func(i, j int) { spans[i], spans[j] = spans[j], spans[i] })
		for len(spans) > 0 {
			n := min(len(spans), 1+r.IntN(12))
			td := ptrace.NewTraces()
			for _, gs := range spans[:n] {
				rs := td.ResourceSpans().AppendEmpty()
				rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", gs.service))
				ss := rs.ScopeSpans().AppendEmpty()
				ss.Scope().SetName("scope")
				span := ss.Spans().AppendEmpty()
				span.SetTraceID(gs.trace)
				span.SetSpanID(gs.span)
				if !gs.root {
					span.SetParentSpanID(pcommon.SpanID{1})
				}
				span.SetName(fmt.Sprintf("op-%d", r.IntN(5)))
				start := time.Unix(1700000000, 0).Add(time.Duration(r.IntN(300)) * time.Millisecond)
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Duration(r.IntN(150)) * time.Millisecond)))
				if r.IntN(20) == 0 {
					span.Status().SetCode(ptrace.StatusCodeError)
				}
				span.Attributes().PutInt("http.status_code", int64(200+100*r.IntN(4)))
				span.Attributes().PutStr("env", []string{"prod", "dev", "test"}[r.IntN(3)])
				span.Attributes().PutBool("debug", r.IntN(10) == 0)
				span.Attributes().PutStr("payload", string(make([]byte, r.IntN(200))))
			}
			spans = spans[n:]
			steps[s] = append(steps[s], td)
		}
	}
	return steps
}

func newDiffProcessor(t *testing.T, cfg Config, next consumer.Traces) (processor.Traces, *testTSPController) {
	c := newTestTSPController()
	cfg.Options = append([]Option{withManualTicks(c)}, cfg.Options...)
	p, err := newTracesProcessor(context.Background(), processortest.NewNopSettings(metadata.Type), next, cfg)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
	return p, c
}

func consumeSpooled(t *testing.T, p processor.Traces, td ptrace.Traces) {
	var m ptrace.ProtoMarshaler
	raw, err := m.MarshalTraces(td)
	require.NoError(t, err)
	ctx := rawrequest.NewContext(context.Background(), &rawrequest.Request{Raw: raw, Traces: td})
	require.NoError(t, p.ConsumeTraces(ctx, td))
}

func onlinePolicies() []PolicyCfg {
	return []PolicyCfg{
		{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 100}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "sample", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 10}}},
	}
}

func richPolicies() []PolicyCfg {
	return []PolicyCfg{
		{sharedPolicyCfg: sharedPolicyCfg{Name: "prod-5xx", Type: And}, AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "prod", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "env", Values: []string{"prod"}}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "5xx", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "http.status_code", MinValue: 500, MaxValue: 599}}},
		}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "drop-debug", Type: Drop}, DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "debug", Type: BooleanAttribute, BooleanAttributeCfg: BooleanAttributeCfg{Key: "debug", Value: true}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "big", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 4}}},
		}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "not-svc0", Type: Not}, NotCfg: NotCfg{SubPolicy: NotSubPolicyCfg{
			sharedPolicyCfg: sharedPolicyCfg{Name: "svc0", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "service.name", Values: []string{"svc-0"}}},
		}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "ottl", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{
			ErrorMode: "ignore", SpanConditions: []string{`name == "op-3" and attributes["env"] == "test"`},
		}}},
		{sharedPolicyCfg: sharedPolicyCfg{Name: "composite", Type: Composite}, CompositeCfg: CompositeCfg{
			MaxTotalSpansPerSecond: 1 << 40,
			PolicyOrder:            []string{"slow-200", "errors"},
			SubPolicyCfg: []CompositeSubPolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow-200", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 200}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			RateAllocation: []RateAllocationCfg{{Policy: "slow-200", Percent: 50}, {Policy: "errors", Percent: 50}},
		}},
	}
}

func TestSpoolMatchesInMemoryProcessor(t *testing.T) {
	base := Config{
		DecisionWait:     2 * time.Second,
		NumTraces:        1000,
		SamplingStrategy: samplingStrategyTraceComplete,
		PolicyCfgs:       onlinePolicies(),
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		// pressure means the decision caches or num_traces evict entries.
		// The restart then only approximates their order, so the outputs
		// may differ for traces at the eviction boundary.
		pressure bool
	}{
		{name: "online", mutate: func(*Config) {}},
		{name: "capacity eviction", pressure: true, mutate: func(c *Config) {
			c.NumTraces = 40
		}},
		{name: "small caches and size limit", pressure: true, mutate: func(c *Config) {
			c.DecisionCache = DecisionCacheConfig{SampledCacheSize: 30, NonSampledCacheSize: 30}
			c.MaximumTraceSizeBytes = 1200
			c.DecisionWaitAfterRootReceived = time.Second
		}},
		{name: "caches and size limit", mutate: func(c *Config) {
			c.DecisionCache = DecisionCacheConfig{SampledCacheSize: 1000, NonSampledCacheSize: 1000}
			c.MaximumTraceSizeBytes = 1200
			c.DecisionWaitAfterRootReceived = time.Second
		}},
		{name: "rich policies", mutate: func(c *Config) {
			c.PolicyCfgs = richPolicies()
			c.DecisionCache = DecisionCacheConfig{SampledCacheSize: 1000}
		}},
		{name: "rich policies first match", mutate: func(c *Config) {
			c.PolicyCfgs = richPolicies()
			c.SampleOnFirstMatch = true
		}},
	}
	for _, tc := range cases {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s restart=%v", tc.name, restart), func(t *testing.T) {
				for seed := range uint64(5) {
					cfg := base
					tc.mutate(&cfg)
					runSpoolDiff(t, cfg, seed, restart, !(restart && tc.pressure))
				}
			})
		}
	}
}

func TestSpoolConfigValidation(t *testing.T) {
	valid := func() Config {
		return Config{
			DecisionWait:     time.Second,
			NumTraces:        10,
			SamplingStrategy: samplingStrategyTraceComplete,
			PolicyCfgs:       onlinePolicies(),
			Spool:            &SpoolConfig{Directory: t.TempDir()},
		}
	}
	require.NoError(t, func() error { c := valid(); return c.Validate() }())

	cases := map[string]func(*Config){
		"directory":   func(c *Config) { c.Spool.Directory = "" },
		"shards":      func(c *Config) { c.NumShards = 2 },
		"span ingest": func(c *Config) { c.SamplingStrategy = samplingStrategySpanIngest },
		"block without sampled cache": func(c *Config) {
			c.BlockOnOverflow = true
			c.DecisionCache.NonSampledCacheSize = 10
		},
		"block without non-sampled cache": func(c *Config) {
			c.BlockOnOverflow = true
			c.DecisionCache.SampledCacheSize = 10
		},
		"ottl now": func(c *Config) {
			c.PolicyCfgs = append(c.PolicyCfgs, PolicyCfg{sharedPolicyCfg: sharedPolicyCfg{Name: "and", Type: And}, AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "recent", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{SpanConditions: []string{`end_time > Now()`}}}},
			}}})
		},
	}
	for name, mutate := range cases {
		c := valid()
		mutate(&c)
		require.Error(t, c.Validate(), name)
	}
}

func TestSpoolRejectsRequestWithoutRawBytes(t *testing.T) {
	cfg := Config{
		DecisionWait:     time.Second,
		NumTraces:        10,
		SamplingStrategy: samplingStrategyTraceComplete,
		PolicyCfgs:       onlinePolicies(),
		Spool:            &SpoolConfig{Directory: t.TempDir()},
	}
	p, _ := newDiffProcessor(t, cfg, newSpanCounter())
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetTraceID(pcommon.TraceID{1})
	require.ErrorIs(t, p.ConsumeTraces(context.Background(), td), errSpoolNeedsRawRequest)

	// A processor in between would hand over a different pdata object.
	var m ptrace.ProtoMarshaler
	raw, err := m.MarshalTraces(td)
	require.NoError(t, err)
	other := ptrace.NewTraces()
	td.CopyTo(other)
	ctx := rawrequest.NewContext(context.Background(), &rawrequest.Request{Raw: raw, Traces: td})
	require.ErrorIs(t, p.ConsumeTraces(ctx, other), errSpoolNeedsRawRequest)

	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, p.ConsumeTraces(expired, td), context.DeadlineExceeded)
}

func runSpoolDiff(t *testing.T, cfg Config, seed uint64, restart, exact bool) {
	r := rand.New(rand.NewPCG(seed, 42))
	steps := genSteps(r, 300, 12)
	numBatches := int(cfg.DecisionWait.Seconds())

	wantSink := newSpanCounter()
	official, oc := newDiffProcessor(t, cfg, wantSink)

	spoolCfg := cfg
	spoolCfg.Spool = &SpoolConfig{Directory: t.TempDir(), SegmentSize: 16 << 10, CheckpointSize: 32 << 10}
	gotSink := newSpanCounter()
	spooled, sc := newDiffProcessor(t, spoolCfg, gotSink)

	for i, step := range steps {
		for _, td := range step {
			cp := ptrace.NewTraces()
			td.CopyTo(cp)
			require.NoError(t, official.ConsumeTraces(context.Background(), cp))
			consumeSpooled(t, spooled, td)
		}
		oc.waitForTick()
		sc.waitForTick()
		if restart && i == len(steps)/2 {
			// Pending traces get a full decision_wait after the restart, so
			// both sides first decide everything pending.
			require.NoError(t, spooled.Shutdown(context.Background()))
			spooled, sc = newDiffProcessor(t, spoolCfg, gotSink)
			for range numBatches + 1 {
				oc.waitForTick()
				sc.waitForTick()
			}
		}
	}
	for range numBatches + 1 {
		oc.waitForTick()
		sc.waitForTick()
	}

	sp := spooled.(*shardedProcessor).shards[0].spool
	require.Eventually(t, func() bool {
		st := sp.Stats()
		return st.ReadyRecords == 0 && st.LiveRecords == 0
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, official.Shutdown(context.Background()))
	require.NoError(t, spooled.Shutdown(context.Background()))

	want, wantBody := wantSink.snapshot()
	got, gotBody := gotSink.snapshot()
	require.NotEmpty(t, want)
	if !exact {
		var diff int
		for sid, n := range want {
			if got[sid] != n {
				diff++
			}
		}
		for sid := range got {
			if _, ok := want[sid]; !ok {
				diff++
			}
		}
		t.Logf("seed %d: %d of %d delivered spans differ after a restart under eviction pressure", seed, diff, len(want))
		return
	}
	require.Equal(t, want, got, "seed %d: span multiplicity differs", seed)
	require.Equal(t, wantBody, gotBody, "seed %d: span content differs", seed)
}
