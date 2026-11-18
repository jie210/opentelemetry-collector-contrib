// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/grpc/backoff"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protowire"
)

func tid(n int) pcommon.TraceID {
	var a pcommon.TraceID
	binary.BigEndian.PutUint64(a[:8], uint64(n))
	binary.LittleEndian.PutUint64(a[8:], uint64(n)^0x953ad2bc6f8e1047)
	return a
}

func sid(n int) pcommon.SpanID {
	var a pcommon.SpanID
	binary.BigEndian.PutUint64(a[:], uint64(n)+0x1234567800000000)
	return a
}

type template struct {
	data              []byte
	tracePos, spanPos []int
}

func payloads() []string {
	bank := make([]string, 512)
	for i := range bank {
		var raw []byte
		for j := 0; j < 96; j++ {
			h := sha256.Sum256([]byte(fmt.Sprintf("payload:%d:%d", i, j)))
			raw = append(raw, h[:]...)
		}
		bank[i] = base64.StdEncoding.EncodeToString(raw)
	}
	return bank
}

func fixture(req int, bank []string, onlyKept bool) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "tail-bench")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 32; i++ {
		n := req*32 + i
		keep := n%100 == 0
		if onlyKept && !keep {
			continue
		}
		for j := 0; j < 4; j++ {
			s := ss.Spans().AppendEmpty()
			s.SetTraceID(tid(n))
			s.SetSpanID(sid(n*4 + j))
			s.SetName("bench")
			s.SetStartTimestamp(1700000000000000000)
			s.SetEndTimestamp(1700000000001000000)
			s.Attributes().PutStr("payload", bank[(req%25+4*i+j)%len(bank)])
			if keep && j == 3 {
				s.Status().SetCode(ptrace.StatusCodeError)
			}
		}
	}
	return td
}

func templates(bank []string) ([]template, error) {
	out := make([]template, 25)
	for i := range out {
		b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(fixture(i, bank, false))
		if err != nil {
			return nil, err
		}
		t := template{data: b}
		var walk func([]byte, int, int) error
		path := []protowire.Number{1, 2, 2}
		walk = func(msg []byte, base, level int) error {
			for len(msg) > 0 {
				num, typ, tag := protowire.ConsumeTag(msg)
				if tag < 0 {
					return protowire.ParseError(tag)
				}
				val := protowire.ConsumeFieldValue(num, typ, msg[tag:])
				if val < 0 {
					return protowire.ParseError(val)
				}
				if typ == protowire.BytesType {
					field, _ := protowire.ConsumeBytes(msg[tag:])
					offset := base + tag + val - len(field)
					if level < len(path) && num == path[level] {
						if err := walk(field, offset, level+1); err != nil {
							return err
						}
					} else if level == len(path) {
						switch num {
						case 1:
							t.tracePos = append(t.tracePos, offset)
						case 2:
							t.spanPos = append(t.spanPos, offset)
						}
					}
				}
				msg = msg[tag+val:]
				base += tag + val
			}
			return nil
		}
		if err = walk(b, 0, 0); err != nil {
			return nil, err
		}
		if len(t.tracePos) != 128 || len(t.spanPos) != 128 {
			return nil, fmt.Errorf("unexpected OTLP fixture shape %d %d", len(t.tracePos), len(t.spanPos))
		}
		out[i] = t
	}
	return out, nil
}

func (t template) fill(buf []byte, req int) []byte {
	buf = append(buf[:0], t.data...)
	for k, pos := range t.tracePos {
		a := tid(req*32 + k/4)
		z := sid(req*128 + k)
		copy(buf[pos:], a[:])
		copy(buf[t.spanPos[k]:], z[:])
	}
	return buf
}

func expected(n int) error {
	bank := payloads()
	a := audit{w: bufio.NewWriter(os.Stdout)}
	selected := make(map[int]bool)
	if *ackLog != "" {
		f, e := os.Open(*ackLog)
		if e != nil {
			return e
		}
		defer f.Close()
		cr := csv.NewReader(f)
		cr.FieldsPerRecord = 4
		for {
			row, e := cr.Read()
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			id, e := strconv.Atoi(row[0])
			if e != nil {
				return e
			}
			selected[id] = row[3] == *ackFilter
		}
		if len(selected) != n {
			return fmt.Errorf("outcome log has %d unique IDs, expected %d", len(selected), n)
		}
	}
	for i := 0; i < n; i++ {
		if *ackLog != "" && !selected[i] {
			continue
		}
		if err := a.consume(fixture(i, bank, true)); err != nil {
			return err
		}
	}
	return a.w.Flush()
}

var ackLog = flag.String("ack-log", "", "continuous load outcome CSV, or oracle input")
var ackFilter = flag.String("ack-filter", "1", "oracle outcome selector, 1 for ACK and 0 for failed")
var startRequest = flag.Int("start-request", 0, "first fixture request ID")

func load(n, rate int) error {
	ts, err := templates(payloads())
	if err != nil {
		return err
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(wireCodec{}))}
	var outcomes *os.File
	var logMu sync.Mutex
	if *ackLog != "" {
		outcomes, err = os.OpenFile(*ackLog, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		defer outcomes.Close()
		opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 20 * time.Millisecond, Multiplier: 1, MaxDelay: 20 * time.Millisecond}, MinConnectTimeout: time.Second}))
	}
	conn, err := grpc.NewClient("127.0.0.1:14317", opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n/rate+180)*time.Second)
	defer cancel()
	var next, total, done atomic.Int64
	var once sync.Once
	var first error
	var wg sync.WaitGroup
	start := time.Now()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf, resp []byte
			for {
				req := int(next.Add(1) - 1)
				if req >= n || ctx.Err() != nil {
					return
				}
				time.Sleep(time.Until(start.Add(time.Duration(float64(req) / float64(rate) * float64(time.Second)))))
				id := req + *startRequest
				buf = ts[id%25].fill(buf, id)
				begin := time.Now().UnixNano()
				err := conn.Invoke(ctx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export", buf, &resp)
				if outcomes != nil {
					end := time.Now().UnixNano()
					ack := 1
					if err != nil {
						ack = 0
					}
					logMu.Lock()
					_, writeErr := fmt.Fprintf(outcomes, "%d,%d,%d,%d\n", id, begin, end, ack)
					logMu.Unlock()
					if writeErr != nil {
						once.Do(func() { first = writeErr; cancel() })
						return
					}
					if err != nil {
						continue
					}
				}
				if err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
				done.Add(1)
				total.Add(int64(len(buf)))
			}
		}()
	}
	wg.Wait()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"seconds": time.Since(start).Seconds(), "requests": done.Load(), "bytes": total.Load()})
	return first
}
