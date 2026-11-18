// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package tailspoolreceiver receives OTLP/gRPC traces and passes the original
// request bytes along with the decoded traces, so that a tail_sampling
// processor configured with a spool can store requests without encoding them
// again. It must feed exactly one pipeline whose first processor is that
// tail_sampling processor.
package tailspoolreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tailspoolreceiver"

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/rawrequest"
)

type tailspoolReceiver struct {
	cfg      *Config
	settings receiver.Settings
	next     consumer.Traces

	server *grpc.Server
	wg     sync.WaitGroup
}

func newReceiver(cfg *Config, settings receiver.Settings, next consumer.Traces) *tailspoolReceiver {
	return &tailspoolReceiver{cfg: cfg, settings: settings, next: next}
}

func (r *tailspoolReceiver) Start(ctx context.Context, host component.Host) error {
	var err error
	r.server, err = r.cfg.GRPC.ToServer(ctx, host.GetExtensions(), r.settings.TelemetrySettings,
		configgrpc.WithGrpcServerOption(grpc.ForceServerCodecV2(rawCodec{})))
	if err != nil {
		return err
	}
	r.server.RegisterService(&traceServiceDesc, r)

	ln, err := r.cfg.GRPC.NetAddr.Listen(ctx)
	if err != nil {
		return err
	}
	r.settings.Logger.Info("Starting tailspool gRPC server", zap.String("endpoint", ln.Addr().String()))
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.server.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
		}
	}()
	return nil
}

func (r *tailspoolReceiver) Shutdown(context.Context) error {
	if r.server != nil {
		r.server.GracefulStop()
	}
	r.wg.Wait()
	return nil
}

// export handles one ExportTraceServiceRequest.
func (r *tailspoolReceiver) export(ctx context.Context, raw []byte) error {
	// A request registered after the client gave up would be registered
	// again when the client or load balancer retries it.
	if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		return status.Error(codes.DeadlineExceeded, "request deadline expired before registration")
	}
	var u ptrace.ProtoUnmarshaler
	td, err := u.UnmarshalTraces(raw)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "decode request: %v", err)
	}
	if td.SpanCount() == 0 {
		return nil
	}
	ctx = rawrequest.NewContext(ctx, &rawrequest.Request{Raw: raw, Traces: td})
	if err := r.next.ConsumeTraces(ctx, td); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

// rawCodec hands request messages to the handler as bytes and encodes the
// empty response.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	if _, ok := v.(*emptyResponse); !ok {
		return nil, status.Errorf(codes.Internal, "unexpected response type %T", v)
	}
	return mem.BufferSlice{}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return status.Errorf(codes.Internal, "unexpected request type %T", v)
	}
	// Materialize copies the pooled buffers that gRPC reuses after the call.
	*b = data.Materialize()
	return nil
}

// emptyResponse is an ExportTraceServiceResponse without partial success,
// which encodes to no bytes.
type emptyResponse struct{}

const exportMethod = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"

var traceServiceDesc = grpc.ServiceDesc{
	ServiceName: "opentelemetry.proto.collector.trace.v1.TraceService",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Export",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			var raw []byte
			if err := dec(&raw); err != nil {
				return nil, err
			}
			r := srv.(*tailspoolReceiver)
			handler := func(ctx context.Context, req any) (any, error) {
				if err := r.export(ctx, *req.(*[]byte)); err != nil {
					return nil, err
				}
				return &emptyResponse{}, nil
			}
			if interceptor == nil {
				return handler(ctx, &raw)
			}
			return interceptor(ctx, &raw, &grpc.UnaryServerInfo{Server: srv, FullMethod: exportMethod}, handler)
		},
	}},
	Metadata: "opentelemetry/proto/collector/trace/v1/trace_service.proto",
}
