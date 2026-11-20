// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sampling // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

// FoldPlan accumulates the verdicts of span-reading leaf policies one trace
// fragment at a time, so a pending trace keeps a fixed amount of state
// instead of its spans.
//
// The built-in span-reading leaves return the first decisive result found
// while walking ReceivedBatches in arrival order. Evaluating each fragment on
// its own in the same order and keeping the first result that differs from
// the empty-trace result therefore reproduces the whole-trace verdict.
// Latency carries running bounds across spans and is folded explicitly.
//
// The other leaves evaluate fragments in Prepare, which receivers call
// concurrently, so they must be safe for concurrent use.
type FoldPlan struct {
	leaves []*foldLeaf
	// sized is set when a policy reads FoldedTrace.TracesSize.
	sized bool
}

// Len returns the number of folded leaves.
func (p *FoldPlan) Len() int { return len(p.leaves) }

// NewTrace returns the state of a trace that has not received any fragment.
func (p *FoldPlan) NewTrace() *samplingpolicy.FoldedTrace {
	ft := &samplingpolicy.FoldedTrace{Leaves: make([]samplingpolicy.FoldedLeaf, len(p.leaves))}
	for i, l := range p.leaves {
		ft.Leaves[i].Decision = l.empty
	}
	return ft
}

// Prepared holds what a fragment yields on its own, before it is folded into
// its trace.
type Prepared struct {
	size int64
	// verdicts holds each leaf's verdict on the fragment, except latency's.
	verdicts []leafVerdict
}

type leafVerdict struct {
	decision samplingpolicy.Decision
	err      error
}

// Prepare evaluates the leaves on the fragment, so that the receivers do this
// work concurrently and AddPrepared only merges the verdicts. frag must hold
// the spans of a single trace.
func (p *FoldPlan) Prepare(ctx context.Context, id pcommon.TraceID, frag ptrace.Traces) Prepared {
	pre := Prepared{verdicts: make([]leafVerdict, len(p.leaves))}
	if p.sized {
		var sizer ptrace.ProtoMarshaler
		pre.size = int64(sizer.TracesSize(frag))
	}
	td := &samplingpolicy.TraceData{ReceivedBatches: frag}
	for i, l := range p.leaves {
		if l.latency == nil {
			v := &pre.verdicts[i]
			v.decision, v.err = l.inner.Evaluate(ctx, id, td)
		}
	}
	return pre
}

// Add folds one fragment of the trace. frag must hold the spans of a single
// trace, in the order the official processor appends them to ReceivedBatches.
func (p *FoldPlan) Add(ctx context.Context, id pcommon.TraceID, ft *samplingpolicy.FoldedTrace, frag ptrace.Traces) {
	p.AddPrepared(ft, frag, p.Prepare(ctx, id, frag))
}

// AddPrepared folds one fragment of the trace, as Add does, with what Prepare
// returned for it.
func (p *FoldPlan) AddPrepared(ft *samplingpolicy.FoldedTrace, frag ptrace.Traces, pre Prepared) {
	ft.TracesSize += pre.size
	for i, l := range p.leaves {
		st := &ft.Leaves[i]
		if st.Done {
			continue
		}
		if l.latency != nil {
			l.latency.fold(st, frag)
			continue
		}
		if v := pre.verdicts[i]; v.err != nil || v.decision != l.empty {
			st.Decision, st.Err, st.Done = v.decision, v.err, true
		}
	}
}

// foldLeaf replaces a span-reading leaf in the policy tree. Without folded
// state it defers to the official evaluator.
type foldLeaf struct {
	inner   samplingpolicy.Evaluator
	latency *latency
	idx     int
	empty   samplingpolicy.Decision
}

var _ samplingpolicy.Evaluator = (*foldLeaf)(nil)

func (l *foldLeaf) Evaluate(ctx context.Context, id pcommon.TraceID, td *samplingpolicy.TraceData) (samplingpolicy.Decision, error) {
	if td.Folded == nil {
		return l.inner.Evaluate(ctx, id, td)
	}
	st := td.Folded.Leaves[l.idx]
	return st.Decision, st.Err
}

func (l *foldLeaf) IsStateful() bool { return l.inner.IsStateful() }

// Fold rewrites e so that its span-reading leaves read folded state, adding
// them to plan. It rejects evaluators whose verdict cannot be folded.
func Fold(e samplingpolicy.Evaluator, plan *FoldPlan) (samplingpolicy.Evaluator, error) {
	switch v := e.(type) {
	case *batchAnd:
		return nil, fmt.Errorf("batch-aware and policies are not supported by the tail spool")
	case *And:
		for i, sub := range v.subpolicies {
			folded, err := Fold(samplingpolicy.UnwrapThresholdEvaluator(sub), plan)
			if err != nil {
				return nil, err
			}
			v.subpolicies[i] = samplingpolicy.AsThresholdEvaluator(folded)
		}
		return v, nil
	case *not:
		folded, err := Fold(v.subPolicyEvaluator, plan)
		if err != nil {
			return nil, err
		}
		v.subPolicyEvaluator = folded
		return v, nil
	case *Drop:
		for i, sub := range v.subpolicies {
			folded, err := Fold(sub, plan)
			if err != nil {
				return nil, err
			}
			v.subpolicies[i] = folded
		}
		return v, nil
	case *Composite:
		if v.recordSubPolicy {
			return nil, fmt.Errorf("composite sub-policy recording is not supported by the tail spool")
		}
		for _, sub := range v.subpolicies {
			folded, err := Fold(sub.evaluator, plan)
			if err != nil {
				return nil, err
			}
			sub.evaluator = folded
		}
		return v, nil
	case *latency:
		return plan.add(v, v), nil
	case *statusCodeFilter, *stringAttributeFilter, *numericAttributeFilter, *booleanAttributeFilter,
		*traceStateFilter, *traceFlags, *ottlConditionFilter:
		return plan.add(e, nil), nil
	case *probabilisticSampler:
		if v.useTraceState {
			return nil, fmt.Errorf("tracestate-based probabilistic sampling is not supported by the tail spool")
		}
		return e, nil
	case *budgetLimiter:
		return nil, fmt.Errorf("tracestate-based budget limiting is not supported by the tail spool")
	case *bytesLimiting:
		plan.sized = true
		return e, nil
	case *alwaysSample, *spanCount, *rateLimiting:
		return e, nil
	default:
		return nil, fmt.Errorf("policy evaluator %T is not supported by the tail spool", e)
	}
}

func (p *FoldPlan) add(inner samplingpolicy.Evaluator, lat *latency) *foldLeaf {
	empty, _ := inner.Evaluate(context.Background(), pcommon.TraceID{}, &samplingpolicy.TraceData{ReceivedBatches: ptrace.NewTraces()})
	l := &foldLeaf{inner: inner, latency: lat, idx: len(p.leaves), empty: empty}
	p.leaves = append(p.leaves, l)
	return l
}

// fold applies Evaluate's per-span bounds update and check to one fragment,
// carrying the bounds in st.Lo and st.Hi.
func (l *latency) fold(st *samplingpolicy.FoldedLeaf, frag ptrace.Traces) {
	minTime, maxTime := pcommon.Timestamp(st.Lo), pcommon.Timestamp(st.Hi)
	defer func() { st.Lo, st.Hi = uint64(minTime), uint64(maxTime) }()
	for _, rs := range frag.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				if minTime == 0 || span.StartTimestamp() < minTime {
					minTime = span.StartTimestamp()
				}
				if maxTime == 0 || span.EndTimestamp() > maxTime {
					maxTime = span.EndTimestamp()
				}
				ms := int64(maxTime-minTime) / int64(time.Millisecond)
				var ok bool
				if l.upperThresholdMs == 0 {
					ok = ms > l.thresholdMs
				} else {
					ok = l.thresholdMs < ms && ms <= l.upperThresholdMs
				}
				if ok {
					st.Decision, st.Done = samplingpolicy.Sampled, true
					return
				}
			}
		}
	}
}
