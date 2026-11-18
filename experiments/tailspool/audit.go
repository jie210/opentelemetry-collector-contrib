// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type audit struct {
	mu    sync.Mutex
	w     *bufio.Writer
	count int64
	err   error
}

// A fixed-size digest per span includes all enclosing OTLP resource/scope
// metadata. Comparing sorted manifests preserves duplicates and ignores batching.
func (a *audit) consume(td ptrace.Traces) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				one := ptrace.NewTraces()
				r := one.ResourceSpans().AppendEmpty()
				r.SetSchemaUrl(rs.SchemaUrl())
				rs.Resource().CopyTo(r.Resource())
				scope := r.ScopeSpans().AppendEmpty()
				scope.SetSchemaUrl(ss.SchemaUrl())
				ss.Scope().CopyTo(scope.Scope())
				sp.CopyTo(scope.Spans().AppendEmpty())
				b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(one)
				if err != nil {
					a.err = err
					return err
				}
				h := sha256.Sum256(b)
				if _, err = fmt.Fprintf(a.w, "%x\n", h); err != nil {
					a.err = err
					return err
				}
				a.count++
			}
		}
	}
	return nil
}

func oracle() error {
	a := audit{w: bufio.NewWriter(os.Stdout)}
	for {
		var n uint32
		err := binary.Read(os.Stdin, binary.BigEndian, &n)
		if errors.Is(err, io.EOF) {
			return a.w.Flush()
		}
		if err != nil {
			return err
		}
		if n > 16<<20 {
			return fmt.Errorf("invalid frame size: %d", n)
		}
		b := make([]byte, n)
		if _, err = io.ReadFull(os.Stdin, b); err != nil {
			return err
		}
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
		if err != nil {
			return err
		}
		if err = a.consume(td); err != nil {
			return err
		}
	}
}
