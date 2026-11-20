// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sampling // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

type ottlConditionFilter struct {
	foldErr             error
	sampleSpanExpr      *ottl.ConditionSequence[*ottlspan.TransformContext]
	sampleSpanEventExpr *ottl.ConditionSequence[*ottlspanevent.TransformContext]
	errorMode           ottl.ErrorMode
	logger              *zap.Logger
}

var _ samplingpolicy.Evaluator = (*ottlConditionFilter)(nil)

// Inspect bound calls so nested converters and extension-created evaluators follow
// the same folding rules. The evaluator itself can always run at decision time.
type foldCheckedFactory[K any] struct {
	ottl.Factory[K]
	conditions []string
	filter     *ottlConditionFilter
}

func (f foldCheckedFactory[K]) CreateFunction(ctx ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
	fn, err := f.Factory.CreateFunction(ctx, args)
	if err == nil && f.filter.foldErr == nil {
		clock, ok := args.(interface{ MayReadCurrentTime() bool })
		if !ok || clock.MayReadCurrentTime() {
			f.filter.foldErr = fmt.Errorf("OTTL %s in conditions %q can produce different results at arrival and decision time", f.Name(), f.conditions)
		}
	}
	return fn, err
}

func checkFoldCalls[K any](funcs map[string]ottl.Factory[K], conditions []string, filter *ottlConditionFilter) map[string]ottl.Factory[K] {
	for _, name := range []string{"Now", "Time", "UUID", "UUIDv7"} {
		funcs[name] = foldCheckedFactory[K]{funcs[name], conditions, filter}
	}
	return funcs
}

// NewOTTLConditionFilter looks at the trace data and returns a corresponding SamplingDecision.
func NewOTTLConditionFilter(settings component.TelemetrySettings, spanConditions, spanEventConditions []string, errMode ottl.ErrorMode) (samplingpolicy.Evaluator, error) {
	filter := &ottlConditionFilter{
		errorMode: errMode,
		logger:    settings.Logger,
	}

	var err error

	if len(spanConditions) == 0 && len(spanEventConditions) == 0 {
		return nil, errors.New("expected at least one OTTL condition to filter on")
	}

	if len(spanConditions) > 0 {
		if filter.sampleSpanExpr, err = filterottl.NewBoolExprForSpanWithPathContextNames(spanConditions, checkFoldCalls(filterottl.StandardSpanFuncs(), spanConditions, filter), errMode, settings); err != nil {
			return nil, err
		}
	}

	if len(spanEventConditions) > 0 {
		if filter.sampleSpanEventExpr, err = filterottl.NewBoolExprForSpanEventWithPathContextNames(spanEventConditions, checkFoldCalls(filterottl.StandardSpanEventFuncs(), spanEventConditions, filter), errMode, settings); err != nil {
			return nil, err
		}
	}

	return filter, nil
}

func (ocf *ottlConditionFilter) Evaluate(ctx context.Context, traceID pcommon.TraceID, trace *samplingpolicy.TraceData) (samplingpolicy.Decision, error) {
	if ocf.logger.Core().Enabled(zap.DebugLevel) {
		ocf.logger.Debug("Evaluating with OTTL conditions filter", zap.String("traceID", traceID.String()))
	}

	if ocf.sampleSpanExpr == nil && ocf.sampleSpanEventExpr == nil {
		return samplingpolicy.NotSampled, nil
	}

	batches := trace.ReceivedBatches

	for i := 0; i < batches.ResourceSpans().Len(); i++ {
		rs := batches.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)

				var (
					ok  bool
					err error
				)

				// Now we reach span level and begin evaluation with parsed expr.
				// The evaluation will break when:
				// 1. error happened.
				// 2. "Sampled" decision made.
				// Otherwise, it will keep evaluating and finally exit with "NotSampled" decision.

				// Span evaluation
				if ocf.sampleSpanExpr != nil {
					tCtx := ottlspan.NewTransformContext(rs, ss, span)
					ok, err = ocf.sampleSpanExpr.Eval(ctx, tCtx)
					tCtx.Close()
					if err != nil {
						return samplingpolicy.Error, err
					}
					if ok {
						return samplingpolicy.Sampled, nil
					}
				}

				// Span event evaluation
				if ocf.sampleSpanEventExpr != nil {
					spanEvents := span.Events()
					for l := 0; l < spanEvents.Len(); l++ {
						tCtx := ottlspanevent.NewTransformContext(rs, ss, span, spanEvents.At(l))
						ok, err = ocf.sampleSpanEventExpr.Eval(ctx, tCtx)
						tCtx.Close()
						if err != nil {
							return samplingpolicy.Error, err
						}
						if ok {
							return samplingpolicy.Sampled, nil
						}
					}
				}
			}
		}
	}
	return samplingpolicy.NotSampled, nil
}

func (*ottlConditionFilter) IsStateful() bool {
	return false
}
