// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestRecoveryWithPartialDecisionAndTornTail(t *testing.T) {
	dir := t.TempDir()
	open := func() (*spool, *os.File, map[traceID]bool) {
		f, e := os.OpenFile(filepath.Join(dir, "actual.sha256"), os.O_CREATE|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		yes, _ := cache.NewLRUDecisionCache(10)
		no, _ := cache.NewLRUDecisionCache(10)
		a := &audit{w: bufio.NewWriter(f)}
		s := newSpool(dir, a.consume, yes, no)
		pending, e := s.recover(f, a)
		if e != nil {
			t.Fatal(e)
		}
		return s, f, pending
	}
	s, f, _ := open()
	mk := func(ids ...int) ptrace.Traces {
		td := ptrace.NewTraces()
		spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
		for _, id := range ids {
			statusSpan(spans, tid(id), true)
		}
		return td
	}
	add := func(td ptrace.Traces) {
		_, ids := projection(td)
		b, e := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.append(b, ids); e != nil {
			t.Fatal(e)
		}
	}
	keep := func(id int) { observedCache{s.yes, s, true}.Put(tid(id), cache.DecisionMetadata{}) }
	add(mk(1))
	keep(1)
	add(mk(2, 3))
	keep(2)
	if e := s.journal.w.Flush(); e != nil {
		t.Fatal(e)
	}
	// A late record inherits keep and writes its manifest row. Kill before
	// its buffered commit reaches the journal. The legitimate duplicate input
	// must appear exactly twice across the restart.
	add(mk(1))
	if _, e := s.journal.f.Write([]byte{1, 2, 3}); e != nil {
		t.Fatal(e)
	}
	if _, e := f.WriteString("uncommitted output suffix"); e != nil {
		t.Fatal(e)
	}
	s.current.file.Close()
	s.journal.f.Close()
	f.Close()
	s, f, pending := open()
	if len(pending) != 1 || !pending[tid(3)] {
		t.Fatalf("recovered %v", pending)
	}
	observedCache{s.no, s, false}.Put(tid(3), cache.DecisionMetadata{})
	if s.err != nil {
		t.Fatal(s.err)
	}
	var buf bytes.Buffer
	oracle := audit{w: bufio.NewWriter(&buf)}
	for _, id := range []int{1, 1, 2} {
		if e := oracle.consume(mk(id)); e != nil {
			t.Fatal(e)
		}
	}
	oracle.w.Flush()
	got, e := os.ReadFile(f.Name())
	if e != nil || !bytes.Equal(got, buf.Bytes()) {
		t.Fatalf("manifest mismatch: %q %v", got, e)
	}
	s.journal.f.Close()
	f.Close()
	s, f, pending = open()
	defer f.Close()
	defer s.journal.f.Close()
	if len(pending) != 0 || s.journal.audit.count != 3 {
		t.Fatal("duplicate replay")
	}
}

func statusSpan(spans ptrace.SpanSlice, id traceID, isError bool) {
	p := spans.AppendEmpty()
	p.SetTraceID(id)
	if isError {
		p.Status().SetCode(ptrace.StatusCodeError)
	}
}
