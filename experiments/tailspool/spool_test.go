// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestRestoreAndRelease(t *testing.T) {
	yes, _ := cache.NewLRUDecisionCache(10)
	no, _ := cache.NewLRUDecisionCache(10)
	var outputs []ptrace.Traces
	store := newSpool(t.TempDir(), func(td ptrace.Traces) error { outputs = append(outputs, td); return nil }, yes, no)
	store.limit = 1
	keep := observedCache{yes, store, true}
	drop := observedCache{no, store, false}
	bank := payloads()
	full := func(extra bool) ptrace.Traces {
		td := fixture(0, bank, true)
		rs := td.ResourceSpans().At(0)
		rs.SetSchemaUrl("rs-schema")
		rs.Resource().SetDroppedAttributesCount(4)
		ss := rs.ScopeSpans().At(0)
		ss.SetSchemaUrl("scope-schema")
		ss.Scope().SetVersion("v")
		span := ss.Spans().At(0)
		span.Events().AppendEmpty().SetName("event")
		span.Links().AppendEmpty().SetTraceID(tid(999))
		span.TraceState().FromRaw("tenant=abc")
		if extra {
			ss.Spans().AppendEmpty().SetTraceID(tid(42))
		}
		return td
	}
	m := &ptrace.ProtoMarshaler{}
	add := func(td ptrace.Traces) {
		p, ids := projection(td)
		if p.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Len() != 0 {
			t.Fatal("payload in projection")
		}
		raw, _ := m.MarshalTraces(td)
		if err := store.append(raw, ids); err != nil {
			t.Fatal(err)
		}
	}
	add(full(true))
	first := store.current.file.Name()
	add(full(false))
	drop.Put(tid(42), cache.DecisionMetadata{})
	keep.Put(tid(0), cache.DecisionMetadata{})
	add(full(false)) // Late batch inherits the existing keep decision.
	if len(outputs) != 3 || store.reads != 3 || store.appends != 3 || len(store.index) != 0 {
		t.Fatal("incomplete/repeated record", len(outputs), store)
	}
	want, _ := m.MarshalTraces(full(false))
	for _, got := range outputs {
		b, _ := m.MarshalTraces(got)
		if !bytes.Equal(want, b) {
			t.Fatal("restored content differs")
		}
	}
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatal("dead segment retained")
	}
	onlyDrop := ptrace.NewTraces()
	onlyDrop.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetTraceID(tid(42))
	add(onlyDrop)
	if store.reads != 3 {
		t.Fatal("dropped batch read")
	}
	if err := store.remove(store.current); err != nil {
		t.Fatal(err)
	}
}

func TestFixturePatchMatchesOriginal(t *testing.T) {
	bank := payloads()
	ts, err := templates(bank)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 24, 25, 312} {
		raw := ts[i%25].fill(nil, i)
		expected, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(fixture(i, bank, false))
		if !bytes.Equal(raw, expected) {
			t.Fatalf("request %d differs", i)
		}
	}
}
