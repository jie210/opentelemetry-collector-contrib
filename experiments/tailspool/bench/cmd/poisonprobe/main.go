// Command poisonprobe sends, over OTLP/HTTP JSON, one span whose attributes
// nest deeper than the protobuf decoder accepts, followed by ordinary spans,
// and reports which of them reach the sink.
package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

type sink struct {
	ptraceotlp.UnimplementedGRPCServer
	mu    sync.Mutex
	spans map[pcommon.SpanID]int
}

func (s *sink) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range req.Traces().ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				s.spans[sp.SpanID()]++
			}
		}
	}
	return ptraceotlp.NewExportResponse(), nil
}

func span(trace, id byte, depth int) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID{trace, 1})
	sp.SetSpanID(pcommon.SpanID{id, 1})
	sp.SetName("probe")
	if depth > 0 {
		m := sp.Attributes().PutEmptyMap("k")
		for range depth - 1 {
			m = m.PutEmptyMap("k")
		}
		m.PutStr("leaf", "v")
	}
	return td
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:4318/v1/traces", "collector OTLP/HTTP traces endpoint")
	listen := flag.String("sink", "127.0.0.1:5317", "sink OTLP gRPC address")
	depth := flag.Int("depth", 40, "attribute nesting depth of the poison span")
	wait := flag.Duration("wait", 8*time.Second, "time to wait for deliveries")
	flag.Parse()

	s := &sink{spans: map[pcommon.SpanID]int{}}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(1 << 30))
	ptraceotlp.RegisterGRPCServer(srv, s)
	go func() { _ = srv.Serve(ln) }()

	var jm ptrace.JSONMarshaler
	post := func(td ptrace.Traces) int {
		b, err := jm.MarshalTraces(td)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := http.Post(*endpoint, "application/json", bytes.NewReader(b))
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	log.Printf("poison span (depth %d): HTTP %d", *depth, post(span(0xEE, 0xEE, *depth)))
	codes := map[int]int{}
	for i := range byte(20) {
		codes[post(span(0x10+i, 0x10+i, 0))]++
	}
	log.Printf("20 ordinary spans: HTTP status counts %v", codes)

	time.Sleep(*wait)
	s.mu.Lock()
	defer s.mu.Unlock()
	ordinary := 0
	for i := range byte(20) {
		ordinary += s.spans[pcommon.SpanID{0x10 + i, 1}]
	}
	log.Printf("delivered after %s: poison=%d ordinary=%d/20", *wait, s.spans[pcommon.SpanID{0xEE, 1}], ordinary)
}
