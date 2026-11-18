// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailspoolreceiver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/rawrequest"
)

type rawCheckingConsumer struct {
	got     []*rawrequest.Request
	hasDL   bool
	failErr error
}

func (*rawCheckingConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (c *rawCheckingConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	req := rawrequest.FromContext(ctx)
	if req == nil || req.Traces != td {
		return errors.New("raw request missing")
	}
	_, c.hasDL = ctx.Deadline()
	c.got = append(c.got, req)
	return c.failErr
}

func freeEndpoint(t *testing.T) string {
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().String()
}

func TestReceiverPassesRawRequest(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.GRPC.NetAddr.Endpoint = freeEndpoint(t)
	sink := &rawCheckingConsumer{}
	r, err := NewFactory().CreateTraces(context.Background(), receivertest.NewNopSettings(componentType), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, r.Shutdown(context.Background())) }()

	conn, err := grpc.NewClient(cfg.GRPC.NetAddr.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := ptraceotlp.NewGRPCClient(conn)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3})
	span.SetSpanID(pcommon.SpanID{4})
	span.SetName("op")
	req := ptraceotlp.NewExportRequestFromTraces(td)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Export(ctx, req, grpc.UseCompressor("gzip"))
	require.NoError(t, err)

	require.Len(t, sink.got, 1)
	want, err := req.MarshalProto()
	require.NoError(t, err)
	require.Equal(t, want, sink.got[0].Raw)
	require.True(t, sink.hasDL, "client deadline must reach the processor")

	sink.failErr = errors.New("spool unavailable")
	_, err = client.Export(ctx, req)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestReceiverRejectsExpiredRequest(t *testing.T) {
	sink := &rawCheckingConsumer{}
	r := newReceiver(createDefaultConfig().(*Config), receivertest.NewNopSettings(componentType), sink)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	err := r.export(ctx, nil)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Empty(t, sink.got)
}
