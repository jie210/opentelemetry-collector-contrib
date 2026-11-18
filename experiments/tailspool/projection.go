// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	tail "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"gopkg.in/yaml.v3"
)

func configureSampling(path string, cfg *tail.Config) error {
	if path == "" {
		var p tail.PolicyCfg
		p.Name = "keep-errors"
		p.Type = tail.StatusCode
		p.StatusCodeCfg.StatusCodes = []string{"ERROR"}
		cfg.PolicyCfgs = []tail.PolicyCfg{p}
		return nil
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var doc struct {
		Processors map[string]map[string]any `yaml:"processors"`
	}
	if e = yaml.Unmarshal(b, &doc); e != nil {
		return e
	}
	section := doc.Processors["tail_sampling"]
	if section["policies"] == nil {
		return fmt.Errorf("missing processors.tail_sampling.policies")
	}
	// A spool is one trace-complete backend. Keep transport/storage wiring in
	// the harness; decode policy options through the Collector's own decoder.
	if v, ok := section["tail_storage"]; ok {
		if v != "compact" {
			return fmt.Errorf("unsupported harness tail_storage %v", v)
		}
		delete(section, "tail_storage")
	}
	if v, ok := section["num_shards"]; ok {
		if v != 1 {
			return fmt.Errorf("the harness hosts one sampler shard")
		}
		delete(section, "num_shards")
	}
	for k := range section {
		switch k {
		case "policies", "decision_wait", "num_traces", "sampling_strategy", "block_on_overflow":
		default:
			return fmt.Errorf("unsupported harness processor setting %q", k)
		}
	}
	if e = confmap.NewFromStringMap(section).Unmarshal(cfg); e != nil {
		return e
	}
	if len(cfg.PolicyCfgs) == 0 || cfg.DecisionWait <= 0 || cfg.NumTraces == 0 || cfg.SamplingStrategy != "trace-complete" || !cfg.BlockOnOverflow {
		return fmt.Errorf("expected nonempty policies and a positive trace-complete window/capacity with block_on_overflow")
	}
	return nil
}

type projectionPlan struct {
	keys                                          map[string]struct{}
	status, times, state, flags, full, statusOnly bool
}

func derive(policies []tail.PolicyCfg) projectionPlan {
	p := projectionPlan{keys: map[string]struct{}{}, statusOnly: len(policies) > 0}
	for _, c := range policies {
		p.statusOnly = p.statusOnly && c.Type == tail.StatusCode
		// Fold composite members into the same field union. The evaluator keeps
		// the original policy tree and its configured comparison values.
		leaf := func(t tail.PolicyType, str, num, boolean string) {
			switch t {
			case tail.StringAttribute:
				p.keys[str] = struct{}{}
			case tail.NumericAttribute:
				p.keys[num] = struct{}{}
			case tail.BooleanAttribute:
				p.keys[boolean] = struct{}{}
			case tail.Latency:
				p.times = true
			case tail.StatusCode:
				p.status = true
			case tail.TraceState, tail.Probabilistic:
				p.state = true
			case tail.TraceFlags:
				p.flags = true
			case tail.AlwaysSample, tail.SpanCount, tail.RateLimiting:
			default:
				p.full = true
			}
		}
		and := func(subs []tail.AndSubPolicyCfg) {
			for _, s := range subs {
				if s.Type == tail.Not {
					n := s.NotCfg.SubPolicy
					leaf(n.Type, n.StringAttributeCfg.Key, n.NumericAttributeCfg.Key, n.BooleanAttributeCfg.Key)
				} else {
					leaf(s.Type, s.StringAttributeCfg.Key, s.NumericAttributeCfg.Key, s.BooleanAttributeCfg.Key)
				}
			}
		}
		switch c.Type {
		case tail.And:
			and(c.AndCfg.SubPolicyCfg)
		case tail.Drop:
			and(c.DropCfg.SubPolicyCfg)
		case tail.Not:
			n := c.NotCfg.SubPolicy
			leaf(n.Type, n.StringAttributeCfg.Key, n.NumericAttributeCfg.Key, n.BooleanAttributeCfg.Key)
		case tail.Composite:
			for _, s := range c.CompositeCfg.SubPolicyCfg {
				if s.Type == tail.And {
					and(s.AndCfg.SubPolicyCfg)
				} else {
					leaf(s.Type, s.StringAttributeCfg.Key, s.NumericAttributeCfg.Key, s.BooleanAttributeCfg.Key)
				}
			}
		default:
			leaf(c.Type, c.StringAttributeCfg.Key, c.NumericAttributeCfg.Key, c.BooleanAttributeCfg.Key)
		}
	}
	return p
}
func (p projectionPlan) attrs(src, dst pcommon.Map) {
	for k := range p.keys {
		if v, ok := src.Get(k); ok {
			v.CopyTo(dst.PutEmpty(k))
		}
	}
}

type tracePart struct {
	wire     []byte
	statuses uint8
}

func (p projectionPlan) split(td ptrace.Traces) (map[traceID]tracePart, error) {
	parts := make(map[traceID]tracePart)
	type builder struct {
		td              ptrace.Traces
		resource, scope int
		rs              ptrace.ResourceSpans
		spans           ptrace.SpanSlice
	}
	builders := map[traceID]*builder{}
	for i, rs := range td.ResourceSpans().All() {
		for j, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				id := sp.TraceID()
				if p.full {
					parts[id] = tracePart{}
					continue
				}
				if p.statusOnly && sp.Status().Code() >= 0 && sp.Status().Code() <= ptrace.StatusCodeError {
					v := parts[id]
					v.statuses |= 1 << sp.Status().Code()
					parts[id] = v
					continue
				}
				b := builders[id]
				if b == nil {
					b = &builder{td: ptrace.NewTraces(), resource: -1}
					builders[id] = b
				}
				if b.resource != i {
					b.rs = b.td.ResourceSpans().AppendEmpty()
					b.resource = i
					b.scope = -1
					p.attrs(rs.Resource().Attributes(), b.rs.Resource().Attributes())
				}
				if b.scope != j {
					scope := b.rs.ScopeSpans().AppendEmpty()
					b.scope = j
					b.spans = scope.Spans()
				}
				d := b.spans.AppendEmpty()
				d.SetTraceID(id)
				d.SetSpanID(sp.SpanID())
				d.SetParentSpanID(sp.ParentSpanID())
				p.attrs(sp.Attributes(), d.Attributes())
				if p.status {
					d.Status().SetCode(sp.Status().Code())
				}
				if p.times {
					d.SetStartTimestamp(sp.StartTimestamp())
					d.SetEndTimestamp(sp.EndTimestamp())
				}
				if p.state {
					d.TraceState().FromRaw(sp.TraceState().AsRaw())
				}
				if p.flags {
					d.SetFlags(sp.Flags())
				}
			}
		}
	}
	for id, b := range builders {
		wire, e := (&ptrace.ProtoMarshaler{}).MarshalTraces(b.td)
		if e != nil {
			return nil, e
		}
		v := parts[id]
		v.wire = wire
		parts[id] = v
	}
	return parts, nil
}

// Historical raw modes send each projected batch straight to the processor.
func (p projectionPlan) immediate(td ptrace.Traces) (ptrace.Traces, map[traceID]bool, error) {
	if p.statusOnly {
		out, ids := projection(td)
		return out, ids, nil
	}
	parts, e := p.split(td)
	if e != nil {
		return ptrace.Traces{}, nil, e
	}
	ids := partIDs(parts)
	if p.full {
		return td, ids, nil
	}
	out := ptrace.NewTraces()
	for _, part := range parts {
		v, e := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(part.wire)
		if e != nil {
			return out, nil, e
		}
		v.ResourceSpans().MoveAndAppendTo(out.ResourceSpans())
	}
	return out, ids, nil
}
