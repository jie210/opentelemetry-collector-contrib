// Command tsbench drives the tail spool benchmark: a deterministic OTLP load
// generator, a sink that counts every received span, and a report comparing
// two sink dumps.
//
// Request i carries span k of each of G traces started at request i-k*D, for
// k in [0, K). Every span has a fixed slot i*S+j, S = K*G, which the sink
// counts, so a run's output can be compared span by span with another run.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
)

type workload struct {
	requests int
	k, g, d  int
	payload  int
}

func (w *workload) flags(fs *flag.FlagSet) {
	fs.IntVar(&w.requests, "requests", 12000, "number of requests")
	fs.IntVar(&w.k, "spans-per-trace", 8, "spans per trace, one per generation")
	fs.IntVar(&w.g, "traces-per-request", 16, "traces started per request")
	fs.IntVar(&w.d, "gap", 200, "requests between two spans of a trace")
	fs.IntVar(&w.payload, "payload", 512, "bytes of attribute payload per span")
}

func (w *workload) spansPerRequest() int { return w.k * w.g }

func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func traceID(q, m int) pcommon.TraceID {
	var id pcommon.TraceID
	n := uint64(q)<<16 | uint64(m)
	binary.BigEndian.PutUint64(id[0:8], mix(n))
	binary.BigEndian.PutUint64(id[8:16], mix(n^0x9e3779b97f4a7c15))
	return id
}

// build returns request i.
func (w *workload) build(i int, payload string) ptrace.Traces {
	td := ptrace.NewTraces()
	for svc := range 2 {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", svc))
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("tsbench")
		for k := range w.k {
			q := i - k*w.d
			if q < 0 {
				continue
			}
			for m := range w.g {
				if m%2 != svc {
					continue
				}
				h := mix(uint64(q)<<16 | uint64(m))
				span := ss.Spans().AppendEmpty()
				span.SetTraceID(traceID(q, m))
				var sid pcommon.SpanID
				binary.BigEndian.PutUint64(sid[:], uint64(i*w.spansPerRequest()+k*w.g+m)+1)
				span.SetSpanID(sid)
				if k > 0 {
					span.SetParentSpanID(pcommon.SpanID{1})
				}
				span.SetName(fmt.Sprintf("op-%d", k))
				// Timestamps follow the trace start, not the request
				// carrying the span, so the trace lasts tens of ms.
				start := time.Unix(1700000000, 0).Add(time.Duration(q)*time.Millisecond + time.Duration(k)*time.Millisecond)
				dur := time.Duration(h%50) * time.Millisecond
				if h%40 == 1 && k == w.k/2 {
					dur = 150 * time.Millisecond
				}
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(dur)))
				if h%50 == 0 && k == w.k-1 {
					span.Status().SetCode(ptrace.StatusCodeError)
				}
				span.Attributes().PutStr("payload", payload)
				span.Attributes().PutInt("http.status_code", 200)
			}
		}
	}
	return td
}

func runLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	var w workload
	w.flags(fs)
	endpoint := fs.String("endpoint", "127.0.0.1:4317", "collector endpoint")
	rate := fs.Float64("rate", 200, "requests per second")
	conc := fs.Int("concurrency", 16, "concurrent requests")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	start := fs.Int("start", 0, "first request")
	out := fs.String("attempts-out", "", "file receiving the number of attempts of each request")
	_ = fs.Parse(args)

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		log.Fatal(err)
	}
	client := ptraceotlp.NewGRPCClient(conn)
	payload := strings.Repeat("x", w.payload)

	attempts := make([]uint8, w.requests)
	work := make(chan int, *conc)
	var wg sync.WaitGroup
	var sent, failed, spans atomic.Int64
	for range *conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				req := ptraceotlp.NewExportRequestFromTraces(w.build(i, payload))
				backoff := 100 * time.Millisecond
				for {
					attempts[i]++
					ctx, cancel := context.WithTimeout(context.Background(), *timeout)
					_, err := client.Export(ctx, req)
					cancel()
					if err == nil {
						break
					}
					failed.Add(1)
					time.Sleep(backoff)
					backoff = min(2*backoff, 2*time.Second)
				}
				sent.Add(1)
				spans.Add(int64(req.Traces().SpanCount()))
			}
		}()
	}

	begin := time.Now()
	go func() {
		for range time.Tick(5 * time.Second) {
			el := time.Since(begin).Seconds()
			log.Printf("load: %d requests acked, %d failed attempts, %.0f spans/s", sent.Load(), failed.Load(), float64(spans.Load())/el)
		}
	}()
	for i := *start; i < w.requests; i++ {
		due := begin.Add(time.Duration(float64(i-*start) / *rate * float64(time.Second)))
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		work <- i
	}
	close(work)
	wg.Wait()
	el := time.Since(begin)
	log.Printf("load done: %d requests in %s, %d failed attempts, %.0f spans/s", sent.Load(), el.Round(time.Millisecond), failed.Load(), float64(spans.Load())/el.Seconds())
	if *out != "" {
		if err := os.WriteFile(*out, attempts, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

type sink struct {
	ptraceotlp.UnimplementedGRPCServer
	counts   []uint32
	spans    atomic.Int64
	outside  atomic.Int64
	requests atomic.Int64
	last     atomic.Int64
	delay    time.Duration
}

func (s *sink) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	time.Sleep(s.delay)
	s.requests.Add(1)
	for _, rs := range req.Traces().ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				sid := span.SpanID()
				slot := binary.BigEndian.Uint64(sid[:]) - 1
				if slot < uint64(len(s.counts)) {
					atomic.AddUint32(&s.counts[slot], 1)
				} else {
					s.outside.Add(1)
				}
				s.spans.Add(1)
			}
		}
	}
	s.last.Store(time.Now().UnixNano())
	return ptraceotlp.NewExportResponse(), nil
}

func runSink(args []string) {
	fs := flag.NewFlagSet("sink", flag.ExitOnError)
	var w workload
	w.flags(fs)
	listen := fs.String("listen", "127.0.0.1:5317", "OTLP gRPC listen address")
	httpAddr := fs.String("http", "127.0.0.1:5318", "stats and dump address")
	delay := fs.Duration("delay", 0, "delay before counting each export, to keep deliveries in flight")
	_ = fs.Parse(args)

	s := &sink{counts: make([]uint32, w.requests*w.spansPerRequest()), delay: *delay}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32))
	ptraceotlp.RegisterGRPCServer(srv, s)
	go func() { log.Fatal(srv.Serve(ln)) }()

	http.HandleFunc("/stats", func(rw http.ResponseWriter, _ *http.Request) {
		idle := time.Duration(0)
		if l := s.last.Load(); l > 0 {
			idle = time.Since(time.Unix(0, l))
		}
		fmt.Fprintf(rw, "spans=%d requests=%d outside=%d idle_ms=%d\n", s.spans.Load(), s.requests.Load(), s.outside.Load(), idle.Milliseconds())
	})
	http.HandleFunc("/dump", func(rw http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")
		buf := make([]byte, 4*len(s.counts))
		for i := range s.counts {
			binary.LittleEndian.PutUint32(buf[4*i:], atomic.LoadUint32(&s.counts[i]))
		}
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(rw, "dumped %d slots to %s\n", len(s.counts), path)
	})
	go func() { log.Fatal(http.ListenAndServe(*httpAddr, nil)) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func readCounts(path string) []uint32 {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	out := make([]uint32, len(b)/4)
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[4*i:])
	}
	return out
}

func runReport(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	baseline := fs.String("baseline", "", "sink dump of the reference run")
	got := fs.String("got", "", "sink dump of the run under test")
	attempts := fs.String("attempts", "", "attempts file of the run under test")
	spansPerRequest := fs.Int("spans-per-request", 128, "spans per request")
	_ = fs.Parse(args)

	want, have := readCounts(*baseline), readCounts(*got)
	var kept, missing, extra, dupSpans, dupCopies, wantDup int
	dupRequests := map[int]bool{}
	for i := range want {
		if want[i] > 1 {
			wantDup++
		}
		switch {
		case want[i] > 0 && have[i] == 0:
			missing++
		case want[i] == 0 && have[i] > 0:
			extra++
		}
		if want[i] > 0 {
			kept++
		}
		if have[i] > 1 {
			dupSpans++
			dupCopies += int(have[i]) - 1
			dupRequests[i / *spansPerRequest] = true
		}
	}
	retried := 0
	if *attempts != "" {
		b, err := os.ReadFile(*attempts)
		if err != nil {
			log.Fatal(err)
		}
		for _, a := range b {
			if a > 1 {
				retried++
			}
		}
	}
	fmt.Printf("baseline_kept=%d baseline_dup=%d missing=%d extra=%d dup_spans=%d dup_copies=%d dup_requests=%d retried_requests=%d\n",
		kept, wantDup, missing, extra, dupSpans, dupCopies, len(dupRequests), retried)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: tsbench load|sink|report [flags]")
	}
	switch os.Args[1] {
	case "load":
		runLoad(os.Args[2:])
	case "sink":
		runSink(os.Args[2:])
	case "report":
		runReport(os.Args[2:])
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}
