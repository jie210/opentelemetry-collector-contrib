// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBackendChild(t *testing.T) {
	if os.Getenv("SPOOL_TEST_DIR") == "" {
		t.Skip("subprocess helper")
	}
	var observe func(traceID, bool)
	if config := os.Getenv("SPOOL_TEST_CONFIG"); config != "" {
		*configPath = config
		*decisionWait = 3 * time.Second
		*checkpointBytes = 32 << 10
		f, err := os.OpenFile(filepath.Join(os.Getenv("SPOOL_TEST_DIR"), "decisions"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		observe = func(id traceID, keep bool) {
			if _, e := fmt.Fprintf(f, "%x %t\n", id[:], keep); e != nil {
				panic(e)
			}
		}
	}
	if err := runObserved(os.Getenv("SPOOL_TEST_MODE"), os.Getenv("SPOOL_TEST_DIR"), os.Getenv("SPOOL_TEST_RPC"), os.Getenv("SPOOL_TEST_STATS"), observe); err != nil {
		t.Fatal(err)
	}
}

func loopback(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestSplitTraceAndLateSpans(t *testing.T) {
	for _, mode := range []string{"pebble", "raw", "compact"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			rpc, statsAddr := loopback(t), loopback(t)
			proc := exec.Command(os.Args[0], "-test.run=^TestBackendChild$")
			proc.Env = append(os.Environ(), "SPOOL_TEST_MODE="+mode, "SPOOL_TEST_DIR="+dir, "SPOOL_TEST_RPC="+rpc, "SPOOL_TEST_STATS="+statsAddr)
			proc.Stderr = os.Stderr
			if err := proc.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				proc.Process.Signal(syscall.SIGTERM)
				if err := proc.Wait(); err != nil {
					t.Error(err)
				}
			}()
			client, err := grpc.NewClient(rpc, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(wireCodec{})))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			var manifest, legacyManifest bytes.Buffer
			legacyAudit := audit{w: bufio.NewWriter(&legacyManifest)}
			expectedAudit := audit{w: bufio.NewWriter(&manifest)}
			send := func(batch int, isLate bool) {
				full := ptrace.NewTraces()
				wanted := ptrace.NewTraces()
				for i := 0; i < 2; i++ {
					rs := full.ResourceSpans().AppendEmpty()
					rs.SetSchemaUrl("resource-v1")
					rs.Resource().Attributes().PutStr("tenant", "demo")
					rs.Resource().SetDroppedAttributesCount(3)
					ss := rs.ScopeSpans().AppendEmpty()
					ss.Scope().SetVersion("v1")
					ss.SetSchemaUrl("scope-v1")
					sp := ss.Spans().AppendEmpty()
					sp.SetTraceID(tid(i))
					sp.SetSpanID(sid(batch*2 + i))
					sp.SetName("part")
					sp.Attributes().PutStr("payload", strings.Repeat("body", 1024))
					sp.SetStartTimestamp(777)
					sp.SetEndTimestamp(888)
					sp.Events().AppendEmpty().SetName("event")
					sp.Links().AppendEmpty().SetTraceID(tid(99))
					sp.TraceState().FromRaw("x=1")
					if batch == 1 && i == 0 || isLate && i == 1 {
						sp.Status().SetCode(ptrace.StatusCodeError)
					}
					if i == 0 {
						rs.CopyTo(wanted.ResourceSpans().AppendEmpty())
					}
				}
				if err := expectedAudit.consume(wanted); err != nil {
					t.Fatal(err)
				}
				// v0.155.0 drops both enclosing schema URLs when grouping spans in the
				// upstream processor. Check that exact difference; newer versions
				// may already emit the original manifest.
				for _, rs := range wanted.ResourceSpans().All() {
					rs.SetSchemaUrl("")
					for _, ss := range rs.ScopeSpans().All() {
						ss.SetSchemaUrl("")
					}
				}
				if err := legacyAudit.consume(wanted); err != nil {
					t.Fatal(err)
				}
				b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(full)
				if err != nil {
					t.Fatal(err)
				}
				var response []byte
				if err = client.Invoke(ctx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export", b, &response, grpc.WaitForReady(true)); err != nil {
					t.Fatal(err)
				}
			}
			wait := func(n int) {
				for ctx.Err() == nil {
					r, e := http.Get("http://" + statsAddr + "/stats")
					if e == nil {
						var s struct {
							Sent  int
							Error string
						}
						e = json.NewDecoder(r.Body).Decode(&s)
						r.Body.Close()
						if e == nil && s.Error == "" && s.Sent == n {
							return
						}
					}
					time.Sleep(50 * time.Millisecond)
				}
				t.Fatal("timed out waiting for complete spans")
			}
			send(0, false)
			send(1, false)
			wait(2)
			send(2, true)
			wait(3)
			expectedAudit.w.Flush()
			actual, err := os.ReadFile(filepath.Join(dir, "actual.sha256"))
			if err != nil {
				t.Fatal(err)
			}
			sorted := func(s string) string {
				lines := strings.Fields(s)
				sort.Strings(lines)
				return strings.Join(lines, "\n")
			}
			if sorted(string(actual)) != sorted(manifest.String()) {
				legacyAudit.w.Flush()
				if mode != "pebble" || sorted(string(actual)) != sorted(legacyManifest.String()) {
					t.Fatal("span manifests differ")
				}
				t.Log("confirmed upstream resource and scope schema URL loss")
			}
		})
	}
}
