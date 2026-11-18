// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	tail "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const mixedRules = `      - name: pay
        type: and
        and:
          and_sub_policy:
            - {name: tenant, type: string_attribute, string_attribute: {key: tenant, values: ["^blue$"], enabled_regex_matching: true}}
            - {name: price, type: numeric_attribute, numeric_attribute: {key: price, min_value: 10}}
            - {name: ok, type: status_code, status_code: {status_codes: [OK]}}
      - {name: slow, type: latency, latency: {threshold_ms: 100}}
      - {name: priority, type: boolean_attribute, boolean_attribute: {key: priority, value: true}}
      - name: veto
        type: drop
        drop:
          drop_sub_policy:
            - {name: deny, type: string_attribute, string_attribute: {key: deny, values: [yes-value]}}
`
const ottlRules = `      - name: same-span
        type: ottl_condition
        ottl_condition:
          error_mode: ignore
          span:
            - 'span.attributes["route"] == "/pay" and span.status.code == STATUS_CODE_ERROR'
`
const metadataRules = `      - name: flagged-or-vendor
        type: and
        and:
          and_sub_policy:
            - {name: exact-count, type: span_count, span_count: {min_spans: 2, max_spans: 2}}
            - {name: vendor, type: trace_state, trace_state: {key: tenant, values: [blue]}}
            - {name: flagged, type: trace_flags}
      - {name: deterministic-subset, type: probabilistic, probabilistic: {sampling_percentage: 12, hash_salt: parity}}
`
const okRules = `      - {name: ok-only, type: status_code, status_code: {status_codes: [OK]}}
`

func parityInput(id, phase int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	tenant := "red"
	if id&1 != 0 {
		tenant = "blue"
	}
	rs.Resource().Attributes().PutStr("tenant", tenant)
	rs.Resource().Attributes().PutStr("not_for_sampling", "original")
	scope := rs.ScopeSpans().AppendEmpty()
	scope.Scope().SetVersion("2.1")
	scope.Scope().Attributes().PutInt("version", 12)
	p := scope.Spans().AppendEmpty()
	p.SetName("fixture")
	if id&1 != 0 {
		p.TraceState().FromRaw("tenant=blue")
	}
	if phase == 1 && id&2 != 0 {
		p.SetFlags(1)
	}
	p.SetTraceID(tid(id))
	p.SetSpanID(sid(id*4 + phase))
	p.Attributes().PutStr("payload", strings.Repeat("z", 1024))
	p.Events().AppendEmpty().SetName("saved")
	p.Links().AppendEmpty().SetTraceID(tid(2000))
	ts := uint64(1e9)
	p.SetStartTimestamp(pcommon.Timestamp(ts))
	if phase == 1 && id&16 != 0 {
		ts += 150e6
	}
	p.SetEndTimestamp(pcommon.Timestamp(ts + 1e6))
	if phase == 0 && id&2 != 0 {
		p.Attributes().PutInt("price", 12)
	}
	if phase == 1 && id&4 != 0 {
		p.Status().SetCode(ptrace.StatusCodeOk)
	}
	if phase == 1 && id&8 != 0 {
		p.Attributes().PutBool("priority", true)
	}
	if phase == 1 && id&32 != 0 {
		p.Attributes().PutStr("deny", "yes-value")
	}
	if phase == 0 && id&1 != 0 || phase == 1 && id&2 != 0 {
		p.Attributes().PutStr("route", "/pay")
	}
	if phase == 1 && id&4 == 0 {
		p.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

type parityProcess struct {
	cmd    *exec.Cmd
	client *grpc.ClientConn
	stats  string
	dir    string
	t      *testing.T
}

func (p *parityProcess) start(mode, config string) {
	rpc := loopback(p.t)
	p.stats = "http://" + loopback(p.t)
	p.cmd = exec.Command(os.Args[0], "-test.run=^TestBackendChild$")
	p.cmd.Env = append(os.Environ(), "SPOOL_TEST_MODE="+mode, "SPOOL_TEST_CONFIG="+config, "SPOOL_TEST_DIR="+p.dir, "SPOOL_TEST_RPC="+rpc, "SPOOL_TEST_STATS="+strings.TrimPrefix(p.stats, "http://"))
	p.cmd.Stderr = os.Stderr
	if e := p.cmd.Start(); e != nil {
		p.t.Fatal(e)
	}
	var e error
	p.client, e = grpc.NewClient(rpc, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(wireCodec{})))
	if e != nil {
		p.t.Fatal(e)
	}
	p.wait(func(s map[string]int64) bool { return true })
}
func (p *parityProcess) stop(kill bool) {
	if p.cmd == nil {
		return
	}
	p.client.Close()
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	p.cmd.Process.Signal(sig)
	e := p.cmd.Wait()
	p.cmd = nil
	if !kill && e != nil {
		p.t.Error(e)
	}
}
func (p *parityProcess) wait(ok func(map[string]int64) bool) {
	until := time.Now().Add(25 * time.Second)
	for time.Now().Before(until) {
		r, e := http.Get(p.stats + "/stats")
		if e == nil {
			var s map[string]int64
			e = json.NewDecoder(r.Body).Decode(&s)
			r.Body.Close()
			if e == nil && ok(s) {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	p.t.Fatal("stats timeout", p.stats)
}
func (p *parityProcess) send(first, last, phase int) {
	for base := first; base < last; base += 16 {
		td := ptrace.NewTraces()
		for id := base; id < min(base+16, last); id++ {
			part := parityInput(id, phase)
			part.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
		}
		raw, e := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if e != nil {
			p.t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var resp []byte
		e = p.client.Invoke(ctx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export", raw, &resp)
		cancel()
		if e != nil {
			p.t.Fatal(e)
		}
	}
}
func sortedLines(t *testing.T, path string) []string {
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	s := strings.Split(strings.TrimSpace(string(b)), "\n")
	slices.Sort(s)
	return s
}
func TestPolicyParity(t *testing.T) {
	for _, tc := range []struct{ name, rules string }{{"status-OK", okRules}, {"attribute-combination", mixedRules}, {"OTTL-same-span", ottlRules}, {"count-flags-state-probability", metadataRules}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := filepath.Join(t.TempDir(), "config.yaml")
			if e := os.WriteFile(config, []byte("processors:\n  tail_sampling:\n    decision_wait: 3s\n    num_traces: 500000\n    num_shards: 1\n    tail_storage: compact\n    policies:\n"+tc.rules), 0600); e != nil {
				t.Fatal(e)
			}
			const n = 512
			var decisions, output []string
			for _, arm := range []string{"official", "compact", "restart"} {
				mode := arm
				if arm == "restart" {
					mode = "compact"
				}
				p := &parityProcess{t: t, dir: t.TempDir()}
				defer p.stop(false)
				p.start(mode, config)
				p.send(0, 128, 0)
				p.send(0, 128, 1)
				p.wait(func(s map[string]int64) bool { return s["decided"] == 128 })
				p.send(128, n, 0)
				if arm == "restart" {
					p.stop(true)
					p.start(mode, config)
					p.wait(func(s map[string]int64) bool { return s["restored_ids"] == n-128 })
				}
				p.send(128, n, 1)
				p.send(0, 128, 2)
				target := int64(n)
				if arm == "restart" {
					target -= 128
				}
				p.wait(func(s map[string]int64) bool { return s["decided"] == target && s["pending_ids"] == 0 })
				p.stop(false)
				d := sortedLines(t, filepath.Join(p.dir, "decisions"))
				o := sortedLines(t, filepath.Join(p.dir, "actual.sha256"))
				if arm == "official" {
					decisions = d
					output = o
					var want bytes.Buffer
					a := audit{w: bufio.NewWriter(&want)}
					var kept int
					for id := 0; id < n; id++ {
						key := tid(id)
						prefix := fmt.Sprintf("%x ", key[:])
						_, found := slices.BinarySearch(d, prefix+"true")
						if !found {
							if _, ok := slices.BinarySearch(d, prefix+"false"); !ok {
								t.Fatal("missing decision", id)
							}
							continue
						}
						kept++
						phases := 2
						if id < 128 {
							phases++
						}
						for ph := 0; ph < phases; ph++ {
							if e := a.consume(parityInput(id, ph)); e != nil {
								t.Fatal(e)
							}
						}
					}
					a.w.Flush()
					w := strings.Fields(want.String())
					slices.Sort(w)
					if len(d) != n || kept == 0 || kept == n || !slices.Equal(w, o) {
						t.Fatalf("invalid control kept=%d decisions=%d output=%d", kept, len(d), len(o))
					}
					t.Logf("official keep=%d drop=%d full exported spans=%d", kept, n-kept, len(o))
				} else if !slices.Equal(d, decisions) || !slices.Equal(o, output) {
					t.Fatalf("%s decisions_match=%v spans_match=%v", arm, slices.Equal(d, decisions), slices.Equal(o, output))
				}
			}
		})
	}
}

func TestProjectionRetainsTypedValuesWithoutMatching(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collector.yaml")
	if err := os.WriteFile(path, []byte("processors:\n  tail_sampling:\n    policies:\n"+mixedRules), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := tail.NewFactory().CreateDefaultConfig().(*tail.Config)
	cfg.BlockOnOverflow = true
	if err := configureSampling(path, cfg); err != nil {
		t.Fatal(err)
	}
	p := derive(cfg.PolicyCfgs)
	td := parityInput(0, 0)
	sp := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	sp.Attributes().PutInt("price", 3)
	sp.Attributes().PutBool("priority", false)
	parts, err := p.split(td)
	if err != nil {
		t.Fatal(err)
	}
	view, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(parts[tid(0)].wire)
	if err != nil {
		t.Fatal(err)
	}
	rs := view.ResourceSpans().At(0)
	d := rs.ScopeSpans().At(0).Spans().At(0)
	tenant, ok := rs.Resource().Attributes().Get("tenant")
	if !ok || tenant.Str() != "red" || rs.Resource().Attributes().Len() != 1 {
		t.Fatal("resource key selection changed the value")
	}
	price, ok := d.Attributes().Get("price")
	if !ok || price.Type() != pcommon.ValueTypeInt || price.Int() != 3 {
		t.Fatal("nonmatching numeric value lost")
	}
	priority, ok := d.Attributes().Get("priority")
	if !ok || priority.Type() != pcommon.ValueTypeBool || priority.Bool() {
		t.Fatal("false value lost")
	}
	if d.Attributes().Len() != 2 || d.Events().Len() != 0 || d.Links().Len() != 0 || d.Name() != "" {
		t.Fatal("unneeded fields retained")
	}
}
