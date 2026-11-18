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

func run(mode, dir string) error {
	return runAt(mode, dir, "127.0.0.1:14317", "127.0.0.1:18888")
}

func runAt(mode, dir, rpcAddr, statsAddr string) error {
	ctx := context.Background()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "actual.sha256"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	a := &audit{w: bufio.NewWriter(f)}
	fac := tail.NewFactory()
	cfg := fac.CreateDefaultConfig().(*tail.Config)
	cfg.DecisionWait = 5 * time.Second
	cfg.NumTraces = 150000
	cfg.BlockOnOverflow = true
	var policy tail.PolicyCfg
	policy.Name = "keep-errors"
	policy.Type = tail.StatusCode
	policy.StatusCodeCfg.StatusCodes = []string{"ERROR"}
	cfg.PolicyCfgs = []tail.PolicyCfg{policy}
	yes, _ := cache.NewLRUDecisionCache(150000)
	no, _ := cache.NewLRUDecisionCache(150000)
	h := host{componenttest.NewNopHost(), make(map[component.ID]component.Component)}
	var sp *spool
	var decided, accepted, requests atomic.Int64
	sink := func(_ context.Context, t ptrace.Traces) error { return a.consume(t) }
	switch mode {
	case "raw", "raw-sync":
		sp = newSpool(dir, a.consume, yes, no)
		sp.syncSegments = mode == "raw-sync"
		yes = observedCache{yes, sp, true}
		no = observedCache{no, sp, false}
		sink = func(context.Context, ptrace.Traces) error { return nil }
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
		h.extensions[id] = ext
		cfg.TailStorageID = &id
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	hook := func(context.Context, traceID, *tail.TraceData) { decided.Add(1) }
	cfg.Options = []tail.Option{tail.WithSampledDecisionCache(yes), tail.WithNonSampledDecisionCache(no), tail.WithSampledHooks(hook), tail.WithNonSampledHooks(hook)}
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
	g := grpc.NewServer(grpc.ForceServerCodec(wireCodec{}))
	handler := endpoint(func(ctx context.Context, raw []byte) error {
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		if err != nil {
			return err
		}
		n := td.SpanCount()
		if sp != nil {
			summary, ids := projection(td)
			if err = sp.append(raw, ids); err != nil {
				return err
			}
			td = summary
		}
		if err = sampler.ConsumeTraces(ctx, td); err != nil {
			return err
		}
		accepted.Add(int64(n))
		requests.Add(1)
		return nil
	})
	g.RegisterService(&grpc.ServiceDesc{ServiceName: "opentelemetry.proto.collector.trace.v1.TraceService", HandlerType: (*any)(nil), Methods: []grpc.MethodDesc{{MethodName: "Export", Handler: export}}}, handler)
	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	m, err := net.Listen("tcp", statsAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		stats := map[string]any{"accepted": accepted.Load(), "decided": decided.Load(), "requests": requests.Load()}
		if sp != nil {
			sp.mu.Lock()
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
	errs := make(chan error, 2)
	web := http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { errs <- web.Serve(m) }()
	go func() { errs <- g.Serve(l) }()
	select {
	case <-signals.Done():
	case err = <-errs:
	}
	g.GracefulStop()
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
