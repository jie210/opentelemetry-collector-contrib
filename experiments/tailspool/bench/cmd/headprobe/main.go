// Command headprobe checks, over real OTLP, whether an unreadable waiting
// record lets the tail sampler decide later traces without their fragments.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (s *sink) count(id pcommon.SpanID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spans[id]
}

func id16(kind byte, n uint32) pcommon.TraceID {
	var t pcommon.TraceID
	_, _ = rand.Read(t[:])
	t[0] = kind
	t[12], t[13], t[14], t[15] = byte(n>>24), byte(n>>16), byte(n>>8), byte(n)
	return t
}

func sid(kind byte, n uint32) pcommon.SpanID {
	return pcommon.SpanID{kind, 0, 0, 0, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

func spanTraces(tid pcommon.TraceID, s pcommon.SpanID, isErr bool) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(tid)
	sp.SetSpanID(s)
	sp.SetName("probe")
	if isErr {
		sp.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

func main() {
	mode := flag.String("mode", "control", "control, transient, permanent (B undecodable) or io (B's segment truncated)")
	collector := flag.String("collector", "127.0.0.1:4317", "collector OTLP gRPC address")
	listen := flag.String("sink", "127.0.0.1:5317", "sink OTLP gRPC address")
	segments := flag.String("segments", "", "spool segments directory")
	wait := flag.Duration("decision-wait", 5*time.Second, "decision_wait of the collector")
	fillersAt := flag.Duration("fillers-at", 300*time.Millisecond, "time after A's first span when fillers are sent")
	fillers := flag.Int("fillers", 200, "filler requests sent before B")
	fillerSpans := flag.Int("filler-spans", 500, "spans per filler request")
	spansPerTrace := flag.Int("filler-spans-per-trace", 1, "filler spans sharing a trace")
	senders := flag.Int("senders", 8, "concurrent filler senders")
	flag.Parse()

	s := &sink{spans: map[pcommon.SpanID]int{}}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(1 << 30))
	ptraceotlp.RegisterGRPCServer(srv, s)
	go func() { _ = srv.Serve(ln) }()

	conn, err := grpc.NewClient(*collector, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	client := ptraceotlp.NewGRPCClient(conn)
	send := func(td ptrace.Traces) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td)); err != nil {
			log.Fatalf("export: %v", err)
		}
	}

	traceA, traceB := id16(0xA1, 0), id16(0xB2, 0)
	a1, a2, b1 := sid(0xA1, 1), sid(0xA1, 2), sid(0xB2, 1)

	var m ptrace.ProtoMarshaler
	bTraces := spanTraces(traceB, b1, false)
	bBytes, err := m.MarshalTraces(bTraces)
	if err != nil {
		log.Fatal(err)
	}

	t0 := time.Now()
	send(spanTraces(traceA, a1, false))
	log.Printf("A normal span acknowledged")
	time.Sleep(time.Until(t0.Add(*fillersAt)))

	fillerBase := id16(0xF0, 0)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range *senders {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= *fillers {
					return
				}
				td := ptrace.NewTraces()
				spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
				for j := range *fillerSpans {
					sp := spans.AppendEmpty()
					n := uint32(i**fillerSpans + j)
					tid := fillerBase
					binary.BigEndian.PutUint32(tid[12:], n/uint32(*spansPerTrace))
					sp.SetTraceID(tid)
					sp.SetSpanID(sid(0xF0, n))
					sp.SetName("filler")
				}
				send(td)
			}
		})
	}
	wg.Wait()
	log.Printf("%d filler requests acknowledged at +%s", *fillers, time.Since(t0).Round(time.Millisecond))

	send(bTraces)
	tB := time.Now()
	send(spanTraces(traceA, a2, true))
	log.Printf("B and A error span acknowledged at +%s", time.Since(t0).Round(time.Millisecond))

	var segFile string
	var offset int64 = -1
	if *mode != "control" {
		files, _ := filepath.Glob(filepath.Join(*segments, "*.otlp"))
		for i := len(files) - 1; i >= 0 && offset < 0; i-- {
			data, err := os.ReadFile(files[i])
			if err != nil {
				log.Fatal(err)
			}
			if at := bytes.Index(data, bBytes); at >= 0 {
				segFile, offset = files[i], int64(at)
			}
		}
		if offset < 0 {
			log.Fatal("B not found in segments")
		}
		if *mode == "io" {
			if offset != 0 {
				log.Fatal("B does not have a segment of its own; set segment_size to 1")
			}
			if err := os.Truncate(segFile, 0); err != nil {
				log.Fatal(err)
			}
			log.Printf("B's segment %s truncated, %s after its acknowledgement",
				filepath.Base(segFile), time.Since(tB).Round(time.Microsecond))
		}
	}
	if *mode == "transient" || *mode == "permanent" {
		f, err := os.OpenFile(segFile, os.O_WRONLY, 0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := f.WriteAt(make([]byte, len(bBytes)), offset); err != nil {
			log.Fatal(err)
		}
		_ = f.Close()
		log.Printf("B zeroed in %s at %d (%d bytes), %s after its acknowledgement",
			filepath.Base(segFile), offset, len(bBytes), time.Since(tB).Round(time.Microsecond))
	}

	time.Sleep(time.Until(t0.Add(*wait + 3*time.Second)))
	log.Printf("+%s, after A's deadline: A normal=%d A error=%d",
		time.Since(t0).Round(time.Millisecond), s.count(a1), s.count(a2))

	switch *mode {
	case "transient":
		f, err := os.OpenFile(segFile, os.O_WRONLY, 0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := f.WriteAt(bBytes, offset); err != nil {
			log.Fatal(err)
		}
		_ = f.Close()
		log.Printf("B restored at +%s", time.Since(t0).Round(time.Millisecond))
		time.Sleep(20 * time.Second)
	case "permanent":
		var cs []pcommon.SpanID
		for i := range uint32(50) {
			c := sid(0xC3, i)
			cs = append(cs, c)
			send(spanTraces(id16(0xC3, i), c, true))
		}
		log.Printf("50 unrelated error traces acknowledged at +%s", time.Since(t0).Round(time.Millisecond))
		time.Sleep(*wait + 5*time.Second)
		delivered := 0
		for _, c := range cs {
			delivered += s.count(c)
		}
		log.Printf("unrelated error spans delivered: %d of 50", delivered)
	default:
		time.Sleep(5 * time.Second)
	}
	log.Printf("final: A normal=%d A error=%d", s.count(a1), s.count(a2))
}
