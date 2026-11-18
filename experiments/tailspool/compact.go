// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Only the ERROR status policy is configured. For that predicate a trace's
// sufficient statistic is the OR of the error flags across all its batches.
type errorStore struct {
	mu     sync.Mutex
	errors map[traceID]bool
}

func (*errorStore) Start(context.Context, component.Host) error { return nil }
func (*errorStore) Shutdown(context.Context) error              { return nil }
func (s *errorStore) Append(id traceID, td ptrace.Traces) error {
	for _, r := range td.ResourceSpans().All() {
		for _, c := range r.ScopeSpans().All() {
			for _, p := range c.Spans().All() {
				if p.Status().Code() == ptrace.StatusCodeError {
					s.mu.Lock()
					s.errors[id] = true
					s.mu.Unlock()
					return nil
				}
			}
		}
	}
	return nil
}
func statusSpan(spans ptrace.SpanSlice, id traceID, isError bool) {
	p := spans.AppendEmpty()
	p.SetTraceID(id)
	if isError {
		p.Status().SetCode(ptrace.StatusCodeError)
	}
}
func (s *errorStore) take(id traceID) ptrace.Traces {
	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	statusSpan(spans, id, s.errors[id])
	delete(s.errors, id)
	return td
}
func (s *errorStore) Take(id traceID) (ptrace.Traces, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.take(id), nil
}
func (s *errorStore) TakeMany(ids []traceID) ([]ptrace.Traces, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ptrace.Traces, len(ids))
	for i, id := range ids {
		out[i] = s.take(id)
	}
	return out, make([]error, len(ids))
}
func (s *errorStore) Delete(id traceID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.errors, id)
	return nil
}
