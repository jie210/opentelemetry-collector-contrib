// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sampling

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

type leafCase struct {
	name string
	make func(t *testing.T) samplingpolicy.Evaluator
}

func int64Ptr(v int64) *int64 { return &v }

func foldLeafCases() []leafCase {
	set := componenttest.NewNopTelemetrySettings()
	must := func(e samplingpolicy.Evaluator, err error) func(*testing.T) samplingpolicy.Evaluator {
		return func(t *testing.T) samplingpolicy.Evaluator {
			require.NoError(t, err)
			return e
		}
	}
	plain := func(f func() samplingpolicy.Evaluator) func(*testing.T) samplingpolicy.Evaluator {
		return func(*testing.T) samplingpolicy.Evaluator { return f() }
	}
	ottlCase := func(span, event []string, mode ottl.ErrorMode) func(*testing.T) samplingpolicy.Evaluator {
		return func(t *testing.T) samplingpolicy.Evaluator {
			e, err := NewOTTLConditionFilter(set, span, event, mode)
			require.NoError(t, err)
			return e
		}
	}
	return []leafCase{
		{"status_error", must(NewStatusCodeFilter(set, []string{"ERROR"}))},
		{"status_ok_unset", must(NewStatusCodeFilter(set, []string{"OK", "UNSET"}))},
		{"string", must(NewStringAttributeFilter(set, "svc", []string{"b"}, false, 0, false))},
		{"string_invert", must(NewStringAttributeFilter(set, "svc", []string{"b"}, false, 0, true))},
		{"string_regex", must(NewStringAttributeFilter(set, "svc", []string{"^[ab]$"}, true, 8, false))},
		{"string_regex_invert", must(NewStringAttributeFilter(set, "svc", []string{"^[ab]$"}, true, 8, true))},
		{"numeric", plain(func() samplingpolicy.Evaluator {
			return NewNumericAttributeFilter(set, "n", int64Ptr(3), int64Ptr(5), false)
		})},
		{"numeric_min_only", plain(func() samplingpolicy.Evaluator { return NewNumericAttributeFilter(set, "n", int64Ptr(4), nil, false) })},
		{"numeric_invert", plain(func() samplingpolicy.Evaluator {
			return NewNumericAttributeFilter(set, "n", int64Ptr(3), int64Ptr(5), true)
		})},
		{"boolean", plain(func() samplingpolicy.Evaluator { return NewBooleanAttributeFilter(set, "flag", false, false) })},
		{"boolean_invert", plain(func() samplingpolicy.Evaluator { return NewBooleanAttributeFilter(set, "flag", true, true) })},
		{"trace_state", plain(func() samplingpolicy.Evaluator { return NewTraceStateFilter(set, "k", []string{"v1"}) })},
		{"trace_flags", plain(func() samplingpolicy.Evaluator { return NewTraceFlags(set) })},
		{"latency", plain(func() samplingpolicy.Evaluator { return NewLatency(set, 40, 0) })},
		{"latency_upper", plain(func() samplingpolicy.Evaluator { return NewLatency(set, 20, 60) })},
		{"ottl_span", ottlCase([]string{`attributes["svc"] == "c" and status.code == STATUS_CODE_ERROR`}, nil, ottl.PropagateError)},
		{"ottl_event", ottlCase(nil, []string{`name == "boom"`}, ottl.PropagateError)},
		{"ottl_error_propagate", ottlCase([]string{`Len(attributes["mix"]) > 3`}, nil, ottl.PropagateError)},
		{"ottl_error_ignore", ottlCase([]string{`Len(attributes["mix"]) > 3`}, nil, ottl.IgnoreError)},
	}
}

// randomTrace returns the fragments of one trace in arrival order. Each
// fragment is a single ResourceSpans, as produced by the processor's split.
func randomTrace(r *rand.Rand, id pcommon.TraceID) []ptrace.Traces {
	base := uint64(1_700_000_000_000_000_000)
	frags := make([]ptrace.Traces, 1+r.IntN(4))
	for i := range frags {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		putRandomAttrs(r, rs.Resource().Attributes())
		for range 1 + r.IntN(2) {
			ss := rs.ScopeSpans().AppendEmpty()
			for range 1 + r.IntN(4) {
				span := ss.Spans().AppendEmpty()
				span.SetTraceID(id)
				span.Status().SetCode(ptrace.StatusCode(r.IntN(3)))
				start := base + uint64(r.IntN(100))*uint64(time.Millisecond)
				if r.IntN(10) == 0 {
					start = 0
				}
				span.SetStartTimestamp(pcommon.Timestamp(start))
				span.SetEndTimestamp(pcommon.Timestamp(base + uint64(r.IntN(100))*uint64(time.Millisecond)))
				span.SetFlags(uint32(r.IntN(2)))
				switch r.IntN(3) {
				case 0:
					span.TraceState().FromRaw("k=v1")
				case 1:
					span.TraceState().FromRaw("k=v2,x=y")
				}
				putRandomAttrs(r, span.Attributes())
				for range r.IntN(2) {
					span.Events().AppendEmpty().SetName([]string{"boom", "calm"}[r.IntN(2)])
				}
			}
		}
		frags[i] = td
	}
	return frags
}

func putRandomAttrs(r *rand.Rand, m pcommon.Map) {
	if r.IntN(3) > 0 {
		m.PutStr("svc", []string{"a", "b", "c", ""}[r.IntN(4)])
	}
	if r.IntN(2) == 0 {
		m.PutInt("n", int64(r.IntN(8)))
	}
	if r.IntN(2) == 0 {
		m.PutBool("flag", r.IntN(2) == 0)
	}
	switch r.IntN(4) {
	case 0:
		m.PutStr("mix", "abcd")
	case 1:
		m.PutInt("mix", 7)
	}
}

func wholeTrace(frags []ptrace.Traces) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, f := range frags {
		f.ResourceSpans().At(0).CopyTo(td.ResourceSpans().AppendEmpty())
	}
	return td
}

func foldTrace(t *testing.T, plan *FoldPlan, id pcommon.TraceID, frags []ptrace.Traces) *samplingpolicy.FoldedTrace {
	ft := plan.NewTrace()
	for _, f := range frags {
		cp := ptrace.NewTraces()
		f.CopyTo(cp)
		plan.Add(t.Context(), id, ft, cp)
	}
	return ft
}

func TestFoldLeafMatchesWholeTrace(t *testing.T) {
	for _, tc := range foldLeafCases() {
		t.Run(tc.name, func(t *testing.T) {
			official := tc.make(t)
			plan := &FoldPlan{}
			folded, err := Fold(tc.make(t), plan)
			require.NoError(t, err)
			require.Equal(t, 1, plan.Len())

			r := rand.New(rand.NewPCG(1, uint64(len(tc.name))))
			seen := map[string]int{}
			for i := range 3000 {
				var id pcommon.TraceID
				id[0], id[15] = byte(i), byte(i>>8)
				frags := randomTrace(r, id)

				want, wantErr := official.Evaluate(t.Context(), id, &samplingpolicy.TraceData{ReceivedBatches: wholeTrace(frags)})
				ft := foldTrace(t, plan, id, frags)
				got, gotErr := folded.Evaluate(t.Context(), id, &samplingpolicy.TraceData{Folded: ft})

				require.Equal(t, want, got, "trace %d", i)
				require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr), "trace %d", i)
				seen[fmt.Sprint(want, wantErr != nil)]++
			}
			// Each case must exercise more than one outcome to be meaningful.
			require.Greater(t, len(seen), 1, "outcomes %v", seen)
			t.Logf("outcomes %v", seen)
		})
	}
}

func TestFoldTreeMatchesWholeTrace(t *testing.T) {
	logger := zap.NewNop()
	leaves := foldLeafCases()
	pick := func(t *testing.T, names ...string) []samplingpolicy.Evaluator {
		var out []samplingpolicy.Evaluator
		for _, n := range names {
			for _, l := range leaves {
				if l.name == n {
					out = append(out, l.make(t))
				}
			}
		}
		require.Len(t, out, len(names))
		return out
	}
	trees := map[string]func(t *testing.T) samplingpolicy.Evaluator{
		"and": func(t *testing.T) samplingpolicy.Evaluator {
			return NewAnd(logger, pick(t, "status_error", "latency_upper", "string_invert"))
		},
		"and_not": func(t *testing.T) samplingpolicy.Evaluator {
			subs := pick(t, "numeric", "boolean")
			return NewAnd(logger, []samplingpolicy.Evaluator{subs[0], NewNot(logger, subs[1])})
		},
		"drop": func(t *testing.T) samplingpolicy.Evaluator {
			return NewDrop(logger, pick(t, "trace_flags", "ottl_event"))
		},
		"composite": func(t *testing.T) samplingpolicy.Evaluator {
			subs := pick(t, "status_error", "latency", "ottl_span")
			params := make([]SubPolicyEvalParams, len(subs))
			for i, s := range subs {
				params[i] = SubPolicyEvalParams{Evaluator: s, MaxSpansPerSecond: 1 << 40, Name: fmt.Sprint(i)}
			}
			return NewComposite(logger, 1<<40, params, MonotonicClock{}, false)
		},
	}
	for name, build := range trees {
		t.Run(name, func(t *testing.T) {
			official := build(t)
			plan := &FoldPlan{}
			folded, err := Fold(build(t), plan)
			require.NoError(t, err)
			r := rand.New(rand.NewPCG(7, uint64(len(name))))
			for i := range 3000 {
				var id pcommon.TraceID
				id[0], id[15] = byte(i), byte(i>>8)
				frags := randomTrace(r, id)
				whole := wholeTrace(frags)
				var spans int64
				for _, rs := range whole.ResourceSpans().All() {
					for _, ss := range rs.ScopeSpans().All() {
						spans += int64(ss.Spans().Len())
					}
				}
				want, wantErr := official.Evaluate(t.Context(), id, &samplingpolicy.TraceData{ReceivedBatches: whole, SpanCount: spans})
				ft := foldTrace(t, plan, id, frags)
				got, gotErr := folded.Evaluate(t.Context(), id, &samplingpolicy.TraceData{Folded: ft, SpanCount: spans})
				require.Equal(t, want, got, "trace %d", i)
				require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr), "trace %d", i)
			}
		})
	}
}

func TestFoldTracesSizeMatchesBytesLimiting(t *testing.T) {
	plan := &FoldPlan{}
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 500 {
		var id pcommon.TraceID
		id[0] = byte(i)
		frags := randomTrace(r, id)
		ft := foldTrace(t, plan, id, frags)
		want := calculateTraceSize(&samplingpolicy.TraceData{ReceivedBatches: wholeTrace(frags)})
		require.Equal(t, want, calculateTraceSize(&samplingpolicy.TraceData{Folded: ft}))
	}
}

func TestFoldRejectsUnsupported(t *testing.T) {
	plan := &FoldPlan{}
	_, err := Fold(NewComposite(zap.NewNop(), 10, nil, MonotonicClock{}, true), plan)
	require.ErrorContains(t, err, "composite sub-policy recording")
	_, err = Fold(&budgetLimiter{}, plan)
	require.ErrorContains(t, err, "budget limiting")
	_, err = Fold(extensionEvaluator{&alwaysSample{}}, plan)
	require.ErrorContains(t, err, "not supported")
}

type extensionEvaluator struct{ *alwaysSample }

var _ = context.Background
