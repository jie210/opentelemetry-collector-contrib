// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestCheckpointRestoreDeadlineAndIngress(t *testing.T) {
	dir := t.TempDir()
	open := func() (*spool, *os.File) {
		f, e := os.OpenFile(filepath.Join(dir, "actual.sha256"), os.O_CREATE|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		a := &audit{w: bufio.NewWriter(f)}
		s := newSpool(dir, a.consume, newDecisionLRU(), newDecisionLRU())
		s.recovering, e = s.recover(f, a)
		if e != nil {
			t.Fatal(e)
		}
		return s, f
	}
	s, f := open()
	raw := func(id int, status bool) ([]byte, ptrace.Traces, map[traceID]bool) {
		td := ptrace.NewTraces()
		statusSpan(td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans(), tid(id), status)
		b, e := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if e != nil {
			t.Fatal(e)
		}
		summary, ids := projection(td)
		return b, summary, ids
	}
	for _, n := range []int{1, 2, 3} {
		b, _, ids := raw(n, false)
		if e := s.append(b, ids); e != nil {
			t.Fatal(e)
		}
	}
	observedCache{s.yes, s, true}.Put(tid(3), cache.DecisionMetadata{})
	past := time.Now().Add(-time.Second).UnixNano()
	future := time.Now().Add(20 * time.Second).UnixNano()
	s.index[tid(1)][0].due = past
	s.index[tid(2)][0].due = future
	if e := s.checkpoint(); e != nil {
		t.Fatal(e)
	}
	s.current.file.Close()
	s.journal.f.Close()
	f.Close()
	s, f = open()
	defer f.Close()
	defer func() { s.journal.f.Close() }()
	if s.index[tid(1)][0].due != past || s.index[tid(2)][0].due != future {
		t.Fatal("original expiry lost")
	}
	if _, ok := s.yes.Get(tid(3)); !ok {
		t.Fatal("late-decision cache lost")
	}
	// A post-deadline ERROR stays dropped. A pre-deadline ERROR updates the
	// recovering trace, without starting an independent upstream decision.
	for _, n := range []int{1, 2} {
		b, summary, ids := raw(n, true)
		if e := s.ingest(b, ids, summary); e != nil {
			t.Fatal(e)
		}
		if n == 2 && summary.SpanCount() != 0 {
			t.Fatal("dispatched recovering trace twice")
		}
	}
	if _, ok := s.no.Get(tid(1)); !ok {
		t.Fatal("late error changed expired trace")
	}
	if !s.recovering[tid(2)] {
		t.Fatal("pre-expiry error lost")
	}
	s.resolve(tid(2), future)
	if s.journal.audit.count != 3 {
		t.Fatalf("exported %d", s.journal.audit.count)
	}
	gate := startupGate{dir: dir}
	b, _, _ := raw(4, true)
	if e := gate.consume(context.Background(), b); e != nil {
		t.Fatal(e)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.inbox"))
	if len(paths) != 1 {
		t.Fatal(paths)
	}
	// Simulate process death between WAL registration and unlinking the inbox.
	backup := paths[0] + ".saved"
	if e := os.Link(paths[0], backup); e != nil {
		t.Fatal(e)
	}
	if _, e := s.adopt(paths[0]); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(backup, paths[0]); e != nil {
		t.Fatal(e)
	}
	if s.current != nil {
		s.current.file.Close()
	}
	s.journal.f.Close()
	f.Close()
	s, f = open()
	defer f.Close()
	if _, e := s.adopt(paths[0]); e != nil {
		t.Fatal(e)
	}
	if len(s.index[tid(4)]) != 1 {
		t.Fatal("inbox adopted twice")
	}
	s.resolve(tid(4), time.Now().Add(*decisionWait).UnixNano())
	if s.journal.audit.count != 4 {
		t.Fatal("missing staged output")
	}
}
