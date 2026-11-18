// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package rawrequest passes the original OTLP request bytes from the tail
// spool receiver to the tail sampling processor within one ConsumeTraces call.
package rawrequest // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/rawrequest"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Request is a decoded request together with its encoded form.
type Request struct {
	// Raw is the ExportTraceServiceRequest protobuf. It must not be modified.
	Raw []byte
	// Traces is the decoded request passed to the next consumer.
	Traces ptrace.Traces
}

type ctxKey struct{}

// NewContext returns a context carrying req.
func NewContext(ctx context.Context, req *Request) context.Context {
	return context.WithValue(ctx, ctxKey{}, req)
}

// FromContext returns the request carried by ctx, or nil.
func FromContext(ctx context.Context) *Request {
	req, _ := ctx.Value(ctxKey{}).(*Request)
	return req
}
