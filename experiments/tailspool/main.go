// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"

	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"

	"sync/atomic"
	"syscall"
	"time"

	pebble "github.com/open-telemetry/opentelemetry-collector-contrib/extension/tailstorage/pebbletailstorageextension"
	tail "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type host struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h host) GetExtensions() map[component.ID]component.Component { return h.extensions }

// Preserve optional method discovery while counting the actual storage calls.
type singleStore interface {
	component.Component
	Append(pcommon.TraceID, ptrace.Traces) error
	Take(pcommon.TraceID) (ptrace.Traces, error)
	Delete(pcommon.TraceID) error
}
type multiStore interface {
	TakeMany([]pcommon.TraceID) ([]ptrace.Traces, []error)
}
type observedSingle struct {
	singleStore
	calls atomic.Int64
}

func (s *observedSingle) Take(id pcommon.TraceID) (ptrace.Traces, error) {
	s.calls.Add(1)
	return s.singleStore.Take(id)
}

type observedMulti struct {
	*observedSingle
	multi      multiStore
	calls, ids atomic.Int64
}

func (s *observedMulti) TakeMany(ids []pcommon.TraceID) ([]ptrace.Traces, []error) {
	s.calls.Add(1)
	s.ids.Add(int64(len(ids)))
	if *probeReads && len(ids) > 0 {
		defer readback.begin()()
	}
	return s.multi.TakeMany(ids)
}

// Both arms use the same transport and take ownership of the decompressed
// request before grpc-go can release its receive buffer.
type wireCodec struct{}

func (wireCodec) Name() string                  { return "proto" }
func (wireCodec) Marshal(v any) ([]byte, error) { return v.([]byte), nil }
func (wireCodec) Unmarshal(data []byte, v any) error {
	p := v.(*[]byte)
	*p = append([]byte(nil), data...)
	return nil
}

type endpoint func(context.Context, []byte) error

func export(s any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	var raw []byte
	if err := decode(&raw); err != nil {
		return nil, err
	}
	if err := s.(endpoint)(ctx, raw); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return []byte{}, nil
}

var decisionWait = flag.Duration("decision-wait", 5*time.Second, "tail sampling decision wait")
var configPath = flag.String("config", "", "Collector YAML containing processors.tail_sampling.policies")
var traceCapacity = flag.Uint64("num-traces", 150000, "pending trace capacity")
var probeReads = flag.Bool("probe-reads", false, "sample Linux thread storage read_bytes during readback")
var readback readProbe

func run(mode, dir string) error {
	return runAt(mode, dir, "127.0.0.1:14317", "127.0.0.1:18888")
}

func listenRPC(addr string, h endpoint) (*grpc.Server, net.Listener, error) {
	g := grpc.NewServer(grpc.ForceServerCodec(wireCodec{}))
	g.RegisterService(&grpc.ServiceDesc{ServiceName: "opentelemetry.proto.collector.trace.v1.TraceService", HandlerType: (*any)(nil), Methods: []grpc.MethodDesc{{MethodName: "Export", Handler: export}}}, h)
	l, e := net.Listen("tcp", addr)
	return g, l, e
}
func runAt(mode, dir, rpcAddr, statsAddr string) error {
	return runObserved(mode, dir, rpcAddr, statsAddr, nil)
}
func runObserved(mode, dir, rpcAddr, statsAddr string, decision func(traceID, bool)) error {
	ctx := context.Background()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_WRONLY
	if mode != "compact" {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(filepath.Join(dir, "actual.sha256"), flags, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	a := &audit{w: bufio.NewWriter(f)}
	fac := tail.NewFactory()
	cfg := fac.CreateDefaultConfig().(*tail.Config)
	cfg.DecisionWait = *decisionWait
	cfg.NumTraces = *traceCapacity
	cfg.BlockOnOverflow = true
	err = configureSampling(*configPath, cfg)
	if err != nil {
		return err
	}
	yes, _ := cache.NewLRUDecisionCache(150000)
	no, _ := cache.NewLRUDecisionCache(150000)
	h := host{componenttest.NewNopHost(), make(map[component.ID]component.Component)}
	var sp *spool
	var restored map[traceID]bool
	var gate startupGate
	var g *grpc.Server
	var l net.Listener
	errs := make(chan error, 2)
	if mode == "compact" {
		lock, e := os.Open(dir)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return e
		}
		for _, pattern := range []string{".checkpoint-*", "*.inbox.part"} {
			files, e := filepath.Glob(filepath.Join(dir, pattern))
			if e != nil {
				return e
			}
			for _, p := range files {
				if e = os.Remove(p); e != nil {
					return e
				}
			}
		}
		yes, no = newDecisionLRU(), newDecisionLRU()
		gate.dir = dir
		g, l, err = listenRPC(rpcAddr, gate.consume)
		if err != nil {
			return err
		}
		defer g.Stop()
		go func() { errs <- g.Serve(l) }()
	}
	var single *observedSingle
	var multi *observedMulti
	var decided, accepted, requests atomic.Int64
	sink := func(_ context.Context, t ptrace.Traces) error { return a.consume(t) }
	switch mode {
	case "raw", "raw-sync", "compact":
		sp = newSpool(dir, a.consume, yes, no)
		sp.plan = derive(cfg.PolicyCfgs)
		sp.syncSegments = mode == "raw-sync"
		if mode == "compact" {
			sp.limit = 16 << 20
			sp.syncSegments = true
			sp.wait = cfg.DecisionWait
			sp.capacity = cfg.NumTraces
			sp.views = make(map[traceID]*pendingTrace)
			// The spool owns the configured collection window. Submit complete views
			// to the same official sampler for both live and recovered decisions.
			cfg.DecisionWait = time.Second
			restored, err = sp.recover(f, a)
			if err != nil {
				return err
			}
			defer func() { sp.journal.f.Close() }()
			sp.recovering = restored
			if err = sp.rebuildViews(); err != nil {
				return err
			}
		}
		yes = observedCache{yes, sp, true}
		no = observedCache{no, sp, false}
		sink = func(context.Context, ptrace.Traces) error { return nil }
	case "official": // The same processor, with its default storage and complete input.
	case "pebble":
		if err = featuregate.GlobalRegistry().Set("processor.tailsamplingprocessor.tailstorageextension", true); err != nil {
			return err
		}
		ef := pebble.NewFactory()
		ec := ef.CreateDefaultConfig().(*pebble.Config)
		ec.Directory = filepath.Join(dir, "db")
		ext, err := ef.Create(ctx, extensiontest.NewNopSettings(ef.Type()), ec)
		if err != nil {
			return err
		}
		if err = ext.Start(ctx, h); err != nil {
			return err
		}
		defer ext.Shutdown(ctx)
		id := component.NewID(ef.Type())
		single = &observedSingle{singleStore: ext.(singleStore)}
		h.extensions[id] = single
		if batch, ok := ext.(multiStore); ok {
			multi = &observedMulti{observedSingle: single, multi: batch}
			h.extensions[id] = multi
		}
		cfg.TailStorageID = &id
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	hook := func(keep bool) tail.DecisionHook {
		return func(_ context.Context, id traceID, _ *tail.TraceData) {
			decided.Add(1)
			if decision != nil {
				decision(id, keep)
			}
		}
	}
	cfg.Options = []tail.Option{tail.WithSampledDecisionCache(yes), tail.WithNonSampledDecisionCache(no), tail.WithSampledHooks(hook(true)), tail.WithNonSampledHooks(hook(false))}
	c, err := consumer.NewTraces(sink)
	if err != nil {
		return err
	}
	sampler, err := fac.CreateTraces(ctx, processortest.NewNopSettings(fac.Type()), cfg, c)
	if err != nil {
		return err
	}
	if err = sampler.Start(ctx, h); err != nil {
		return err
	}
	restoreCount := len(restored)
	handler := endpoint(func(ctx context.Context, raw []byte) error {
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		if err != nil {
			return err
		}
		n := td.SpanCount()
		if mode == "compact" {
			if err = sp.ingestView(ctx, raw, td); err != nil {
				return err
			}
		} else {
			if sp != nil {
				summary, ids, e := sp.plan.immediate(td)
				if e != nil {
					return e
				}
				if err = sp.append(raw, ids); err != nil {
					return err
				}
				td = summary
			}
			if err = sampler.ConsumeTraces(ctx, td); err != nil {
				return err
			}
		}
		accepted.Add(int64(n))
		requests.Add(1)
		return nil
	})
	m, err := net.Listen("tcp", statsAddr)
	if err != nil {
		return err
	}
	defer m.Close()
	var stopRecovery context.CancelFunc
	var recoveryDone chan struct{}
	if mode == "compact" {
		n, k, e := gate.activate(sp, handler)
		if e != nil {
			return e
		}
		accepted.Add(int64(n))
		requests.Add(int64(k))

		var rctx context.Context
		rctx, stopRecovery = context.WithCancel(ctx)
		recoveryDone = make(chan struct{})
		go func() { defer close(recoveryDone); sp.dispatch(rctx, sampler.ConsumeTraces) }()
	} else {
		g, l, err = listenRPC(rpcAddr, handler)
		if err != nil {
			return err
		}
		go func() { errs <- g.Serve(l) }()
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		stats := map[string]any{"restored_ids": restoreCount, "accepted": accepted.Load(), "decided": decided.Load(), "requests": requests.Load()}
		if *probeReads {
			stats["probed_calls"] = readback.calls.Load()
			stats["probed_cold_calls"] = readback.cold.Load()
			stats["probed_read_bytes"] = readback.bytes.Load()
			stats["probe_errors"] = readback.errors.Load()
		}
		if single != nil {
			stats["single_take"] = single.calls.Load()
		}
		if multi != nil {
			stats["take_many"] = multi.calls.Load()
			stats["batched_ids"] = multi.ids.Load()
		}
		if sp != nil {
			sp.mu.Lock()
			if sp.journal != nil {
				stats["staged"] = gate.staged
				stats["recovering"] = len(sp.recovering)
				stats["recovered_decisions"] = sp.restoredDone
				stats["max_recovery_lag_ms"] = sp.maxLateness / 1e6
				stats["checkpoints"] = sp.journal.checkpoints
			}
			stats["appends"] = sp.appends
			stats["written"] = sp.written
			stats["reads"] = sp.reads
			stats["read"] = sp.read
			stats["pending_ids"] = len(sp.index)
			if sp.err != nil {
				stats["error"] = sp.err.Error()
			}
			sp.mu.Unlock()
		}
		a.mu.Lock()
		stats["sent"] = a.count
		if err := a.w.Flush(); err != nil {
			a.err = err
		}
		if a.err != nil {
			stats["error"] = a.err.Error()
		}
		a.mu.Unlock()
		_ = json.NewEncoder(w).Encode(stats)
	})
	signals, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer cancel()

	web := http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { errs <- web.Serve(m) }()

	select {
	case <-signals.Done():
	case err = <-errs:
	}
	g.GracefulStop()
	if stopRecovery != nil {
		stopRecovery()
		<-recoveryDone
		if e := sp.submitDue(ctx, true, sampler.ConsumeTraces); e != nil {
			err = errors.Join(err, e)
		}
	}
	stopErr := sampler.Shutdown(ctx)
	_ = web.Close()
	if sp != nil && sp.current != nil {
		if sp.syncSegments {
			stopErr = errors.Join(stopErr, sp.current.file.Sync())
		}
		if sp.current.records == 0 {
			stopErr = errors.Join(stopErr, sp.remove(sp.current))
		} else {
			stopErr = errors.Join(stopErr, sp.current.file.Close())
		}
	}
	if sp != nil && sp.journal != nil {
		stopErr = errors.Join(stopErr, sp.journal.w.Flush())
	}
	return errors.Join(err, stopErr, a.w.Flush())
}

func main() {
	mode := flag.String("mode", "", "raw, raw-sync, pebble, load, expected or oracle")
	dir := flag.String("dir", "", "new benchmark directory")
	n := flag.Int("n", 1, "request count")
	rate := flag.Int("rate", 24, "target requests/s")
	flag.Parse()
	var err error
	if (*mode == "load" || *mode == "expected") && (*n <= 0 || *rate <= 0) {
		err = errors.New("request count and rate must be positive")
	} else if *mode == "load" {
		err = load(*n, *rate)
	} else if *mode == "expected" {
		err = expected(*n)
	} else if *mode == "oracle" {
		err = oracle()
	} else if *dir == "" {
		err = errors.New("missing -dir")
	} else {
		err = run(*mode, *dir)
	}
	if err != nil {
		log.Fatal(err)
	}
}
