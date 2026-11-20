// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// The parent uses these files to observe sink acceptance across process exit.
type barrierSink struct {
	dir     string
	workers int
	entered sync.WaitGroup
	mixed   bool
}

func (*barrierSink) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (s *barrierSink) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	id := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SpanID()
	n := int(id[0])
	withhold := s.mixed && n == s.workers
	if !withhold {
		path := filepath.Join(s.dir, fmt.Sprintf("accepted-%d", n))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	s.entered.Done()
	s.entered.Wait()
	if withhold || (s.mixed && n == s.workers-1 && n > 1) {
		select {}
	}
	return nil
}

// Write part of a frame into the real WAL, then report a storage error.
type partialWriter struct{ f *os.File }

func (w partialWriter) Write(b []byte) (int, error) {
	n, err := w.f.Write(b[:len(b)/2])
	if err != nil {
		return n, err
	}
	return n, io.ErrShortWrite
}

// Some storage errors are reported after one or more complete frames reached
// the file. The replay prefix then includes those frames despite the error.
type prefixFaultWriter struct {
	f         *os.File
	remaining int
}

func (w prefixFaultWriter) Write(b []byte) (int, error) {
	n, err := w.f.Write(b[:min(len(b), w.remaining)])
	if err != nil {
		return n, err
	}
	return n, io.ErrUnexpectedEOF
}

// Forward the decision and fail the following commit. These tests force
// individual writes at frame boundaries by choosing the bufio size.
type commitFaultWriter struct{ partialWriter }

func (w commitFaultWriter) Write(b []byte) (int, error) {
	if b[8] == frameCommit {
		return w.partialWriter.Write(b)
	}
	return w.f.Write(b)
}

func registerFaultRecord(t *testing.T, s *Spool, n byte) *Record {
	t.Helper()
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	id := pcommon.TraceID{n}
	span.SetTraceID(id)
	span.SetSpanID(pcommon.SpanID{n})
	raw, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	r, err := s.Register(context.Background(), raw, []Assoc{{ID: id}}, time.Now(), nil)
	require.NoError(t, err)
	return r
}

func faultConfig(dir string, workers int) Config {
	cfg := testConfig(dir)
	cfg.MaxInflightRecords = workers
	cfg.CheckpointBytes = 0
	cfg.SegmentBytes = 1
	return cfg
}

func TestPersistenceFailure(t *testing.T) {
	if dir := os.Getenv("TAILSPOOL_FAULT_DIR"); dir != "" {
		workers, err := strconv.Atoi(os.Getenv("TAILSPOOL_WORKERS"))
		require.NoError(t, err)
		runPersistenceFault(t, dir, os.Getenv("TAILSPOOL_FAULT"), workers)
		t.Fatal("persistence failure returned")
	}
	cases := map[string]string{
		"delivery-zero": "flush delivery commit", "delivery-half": "flush delivery commit", "delivery-mixed": "flush delivery commit", "decision-and-commit": "flush delivery commit", "decision-complete": "flush delivery commit",
		"T-append": "append registration", "T-flush": "flush registration", "T-complete": "flush registration",
		"K": "append WAL frame", "D": "append WAL frame", "V": "append WAL frame", "L": "append WAL frame",
		"drop-C": "append WAL frame", "drop-remove": "flush WAL before segment removal", "drop-flush": "flush WAL",
		"unreadable": "commit unreadable record", "write": "write segment", "short": "write segment", "rotate": "rotate segment", "sync": "sync segment",
		"checkpoint-sync": "install checkpoint", "checkpoint-create": "write checkpoint", "checkpoint-flush": "write checkpoint", "checkpoint-tail": "append checkpoint tail", "checkpoint-tail-write": "append checkpoint tail", "checkpoint-rename": "install checkpoint", "checkpoint-race": "append WAL frame",
	}
	for fault, op := range cases {
		limits := []int{1}
		if fault == "delivery-half" {
			limits = []int{1, 2, 16}
		}
		if fault == "delivery-mixed" {
			limits = []int{2, 16}
		}
		for _, workers := range limits {
			t.Run(fmt.Sprintf("%s-%d", fault, workers), func(t *testing.T) {
				if fault == "checkpoint-flush" {
					if _, err := os.Stat("/dev/full"); os.IsNotExist(err) {
						t.Skip("requires /dev/full")
					}
				}
				dir := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPersistenceFailure$")
				cmd.Env = append(os.Environ(), "TAILSPOOL_FAULT_DIR="+dir, "TAILSPOOL_FAULT="+fault, "TAILSPOOL_WORKERS="+strconv.Itoa(workers))
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", output)
				require.Equal(t, 1, exit.ExitCode(), "%s", output)
				require.Contains(t, string(output), "Fatal tail spool storage failure")
				if fault == "checkpoint-sync" {
					require.Contains(t, string(output), "sync checkpoint")
				}
				require.Contains(t, string(output), op)
				checkFaultRecovery(t, dir, fault, workers)
				t.Logf("fault=%s workers=%d pid=%d exit=%d", fault, workers, cmd.Process.Pid, exit.ExitCode())
			})
		}
	}
}

func runPersistenceFault(t *testing.T, dir, fault string, workers int) {
	cfg := faultConfig(dir, workers)
	sink := &barrierSink{dir: dir, workers: workers, mixed: fault == "delivery-mixed"}
	sink.entered.Add(workers)
	logger, err := zap.NewProduction()
	require.NoError(t, err)
	s, _, err := Open(cfg, logger, sink)
	require.NoError(t, err)
	for n := byte(1); n <= 24; n++ {
		r := registerFaultRecord(t, s, n)
		s.Begin(r)
		var keep Pending
		s.Attach(&keep, r, 0)
		s.End(r)
		s.Decide(&keep, r.assocs[0].ID, true)
	}
	r := registerFaultRecord(t, s, 25)
	s.Begin(r)
	var pending Pending
	if fault != "L" {
		s.Attach(&pending, r, 0)
	}
	s.End(r)
	s.Flush()
	// Keep the original inode accessible for the checkpoint race assertion.
	if strings.HasPrefix(fault, "checkpoint-") {
		require.NoError(t, os.Link(logPath(dir), filepath.Join(dir, "original")))
	}
	switch fault {
	case "delivery-zero", "delivery-half", "delivery-mixed", "decision-and-commit", "decision-complete":
		if fault == "delivery-zero" {
			require.NoError(t, s.log.f.Close())
		} else {
			s.log.w.Reset(partialWriter{s.log.f})
		}
		if fault == "decision-complete" {
			s.log.w.Reset(prefixFaultWriter{s.log.f, 33 + 8})
		}
		if strings.HasPrefix(fault, "decision-") {
			s.Decide(&pending, r.assocs[0].ID, true)
		}
		s.Start()
		s.workers.Wait()
	case "T-append", "T-flush", "T-complete":
		s.log.w.Reset(partialWriter{s.log.f})
		if fault == "T-complete" {
			s.log.w.Reset(prefixFaultWriter{s.log.f, 1 << 20})
		}
		if fault == "T-append" {
			s.log.w = bufio.NewWriterSize(partialWriter{s.log.f}, 1)
		}
		registerFaultRecord(t, s, 26)
	case "K", "D", "V", "L", "checkpoint-race":
		if fault == "checkpoint-race" {
			written, release := make(chan struct{}), make(chan struct{})
			s.snapshotWritten = func(*logWriter) {
				close(written)
				<-release
			}
			s.cfg.CheckpointBytes = 1
			s.Flush()
			<-written
			s.mu.Lock()
			close(release)
			s.log.w = bufio.NewWriterSize(partialWriter{s.log.f}, 1)
			s.appendLocked(&frame{kind: frameKeep, id: r.assocs[0].ID, seq: s.wm})
			s.mu.Unlock()
			return
		}
		s.log.w = bufio.NewWriterSize(partialWriter{s.log.f}, 1)
		switch fault {
		case "V":
			s.Evict(&pending, r.assocs[0].ID)
		case "L":
			s.Inherit(r, 0, true)
		default:
			s.Decide(&pending, r.assocs[0].ID, fault == "K")
		}
	case "drop-C", "drop-remove", "drop-flush":
		if fault == "drop-remove" {
			registerFaultRecord(t, s, 26)
		}
		// D is 33 bytes and bypasses either buffer. A 17-byte C
		// bypasses the smaller buffer, or waits for the subsequent Flush.
		size := 17
		if fault == "drop-C" {
			size = 1
		}
		s.log.w = bufio.NewWriterSize(commitFaultWriter{partialWriter{s.log.f}}, size)
		s.Decide(&pending, r.assocs[0].ID, false)
		if fault == "drop-flush" {
			s.Flush()
		}
	case "unreadable":
		s.log.w.Reset(partialWriter{s.log.f})
		s.mu.Lock()
		s.releaseUnreadableLocked(r, io.ErrUnexpectedEOF)
		s.mu.Unlock()
	case "write", "short":
		s.writeAt = func(*segment, []byte, int64) (int, error) {
			if fault == "write" {
				return 0, io.ErrClosedPipe
			}
			return 0, nil
		}
		registerFaultRecord(t, s, 26)
	case "rotate":
		require.NoError(t, os.Mkdir(segmentPath(dir, r.seg.id+1), 0o700))
		registerFaultRecord(t, s, 26)
	case "sync":
		require.NoError(t, r.seg.f.Close())
		s.cfg.SyncOnRotate = true
		registerFaultRecord(t, s, 26)
		s.background.Wait()
	case "checkpoint-sync", "checkpoint-create", "checkpoint-tail", "checkpoint-rename", "checkpoint-flush", "checkpoint-tail-write":
		tmp := logPath(dir) + ".tmp"
		if fault == "checkpoint-create" {
			require.NoError(t, os.Mkdir(tmp, 0o700))
		}
		if fault == "checkpoint-flush" {
			require.NoError(t, os.Symlink("/dev/full", tmp))
		}
		if fault == "checkpoint-sync" {
			s.snapshotWritten = func(w *logWriter) { require.NoError(t, w.f.Close()) }
		}
		if fault == "checkpoint-rename" {
			s.snapshotWritten = func(*logWriter) { require.NoError(t, os.Remove(tmp)) }
		}
		if strings.HasPrefix(fault, "checkpoint-tail") {
			written, release := make(chan struct{}), make(chan struct{})
			s.snapshotWritten = func(w *logWriter) {
				w.w.Reset(partialWriter{w.f})
				if fault == "checkpoint-tail-write" {
					w.w = bufio.NewWriterSize(partialWriter{w.f}, 1)
				}
				close(written)
				<-release
			}
			s.cfg.CheckpointBytes = 1
			s.Flush()
			<-written
			s.Decide(&pending, r.assocs[0].ID, true)
			s.Flush()
			close(release)
		} else {
			s.cfg.CheckpointBytes = 1
			s.Flush()
		}
		s.background.Wait()
	}
}

func checkFaultRecovery(t *testing.T, dir, fault string, workers int) {
	t.Helper()
	data, err := os.ReadFile(logPath(dir))
	require.NoError(t, err)
	registered := map[uint64]bool{}
	segments := map[uint32]bool{}
	var decisions []frame
	end, err := readLog(bytes.NewReader(data), func(f frame) error {
		switch f.kind {
		case frameRecord:
			registered[f.seq] = true
			segments[f.seg] = true
		case frameKeep, frameDrop, frameEvict, frameLate:
			decisions = append(decisions, f)
		case frameCommit:
			t.Fatalf("unexpected complete C for seq %d", f.seq)
		}
		return nil
	})
	require.NoError(t, err)
	for i := uint64(1); i <= 25; i++ {
		require.True(t, registered[i])
	}
	if fault == "delivery-half" || fault == "T-flush" || fault == "checkpoint-race" {
		require.Less(t, end, int64(len(data)))
	}
	if fault == "T-complete" {
		require.True(t, registered[26])
	}
	if fault == "decision-complete" {
		require.Len(t, decisions, 25)
	}
	if fault == "T-flush" || fault == "T-append" || fault == "write" || fault == "short" || fault == "rotate" {
		require.False(t, registered[26])
	}
	if strings.HasPrefix(fault, "checkpoint-") {
		old, err := os.Stat(filepath.Join(dir, "original"))
		require.NoError(t, err)
		live, err := os.Stat(logPath(dir))
		require.NoError(t, err)
		require.True(t, os.SameFile(old, live))
		if fault == "checkpoint-create" || fault == "checkpoint-flush" {
			require.NoError(t, os.Remove(logPath(dir)+".tmp"))
		}
	}
	for id := range segments {
		info, err := os.Stat(segmentPath(dir, id))
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
	}
	if fault == "checkpoint-race" {
		require.Len(t, decisions, 24)
	}
	if strings.HasPrefix(fault, "drop-") {
		require.Len(t, decisions, 25)
		require.Equal(t, byte(frameDrop), decisions[24].kind)
		require.Less(t, end, int64(len(data)))
	}
	if fault == "rotate" {
		require.NoError(t, os.Remove(segmentPath(dir, 26)))
	}
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	s, rec, err := Open(faultConfig(dir, workers), zap.NewNop(), sink)
	require.NoError(t, err)
	require.Zero(t, s.Stats().ReleasedUnreadable)
	wantKept := map[pcommon.SpanID]int{}
	unsettled := map[uint64]bool{}
	for seq := range registered {
		unsettled[seq] = true
	}
	for _, f := range decisions {
		seq := uint64(f.id[0])
		if f.kind == frameLate {
			seq = f.seq
		}
		delete(unsettled, seq)
		if f.keep || f.kind == frameKeep {
			wantKept[pcommon.SpanID{byte(seq)}] = 1
		}
	}
	require.Len(t, rec.Refeed, len(unsettled))
	for _, rr := range rec.Refeed {
		require.True(t, unsettled[rr.Record.seq])
		delete(unsettled, rr.Record.seq)
	}
	s.Start()
	require.Eventually(t, func() bool { return s.Stats().Delivered == uint64(len(wantKept)) }, 5*time.Second, time.Millisecond)
	require.NoError(t, s.Stop())
	require.Equal(t, wantKept, sink.spans)
	accepted, err := filepath.Glob(filepath.Join(dir, "accepted-*"))
	require.NoError(t, err)
	wantAccepted := 0
	if strings.HasPrefix(fault, "delivery-") || strings.HasPrefix(fault, "decision-") {
		wantAccepted = workers
	}
	if fault == "delivery-mixed" {
		wantAccepted--
	}
	require.Len(t, accepted, wantAccepted)
	for _, path := range accepted {
		n, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "accepted-"))
		require.NoError(t, err)
		require.Equal(t, 1, sink.spans[pcommon.SpanID{byte(n)}])
	}
	t.Logf("registered=%v segments=%v prefix=%d/%d sink_before_exit=%v recovered_keep=%v refeed=%d", registered, segments, end, len(data), accepted, wantKept, len(rec.Refeed))
}

func TestStartupWALBoundary(t *testing.T) {
	for _, fault := range []string{"short-header", "short-payload", "crc", "version", "open", "snapshot", "snapshot-sync"} {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			cfg := testConfig(dir)
			s, _, err := Open(cfg, zap.NewNop(), &spanSink{})
			require.NoError(t, err)
			registerFaultRecord(t, s, 1)
			require.NoError(t, s.Stop())
			path := logPath(dir)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			switch fault {
			case "short-header":
				data = append(data, 1, 0, 0)
			case "short-payload":
				data = append(data, 9, 0, 0, 0, 0, 0, 0, 0, 'C')
			case "crc":
				data[len(data)-1] ^= 1
			case "version":
				binary.LittleEndian.PutUint32(data[9:], walVersion+1)
				binary.LittleEndian.PutUint32(data[4:], crc32.Checksum(data[8:8+binary.LittleEndian.Uint32(data)], castagnoli))
			case "open":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Mkdir(path, 0o700))
			case "snapshot-sync":
				if _, err := os.Stat("/dev/null"); err != nil {
					t.Skip("requires /dev/null")
				}
				require.NoError(t, os.Symlink("/dev/null", path+".tmp"))
			case "snapshot":
				require.NoError(t, os.Mkdir(path+".tmp", 0o700))
			}
			if fault != "open" {
				require.NoError(t, os.WriteFile(path, data, 0o600))
			}
			s, rec, err := Open(cfg, zap.NewNop(), &spanSink{})
			if fault == "snapshot-sync" {
				require.ErrorContains(t, err, "sync checkpoint")
				old, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				require.Equal(t, data, old)
				require.NoError(t, os.Remove(path+".tmp"))
				s, rec, err = Open(cfg, zap.NewNop(), &spanSink{})
				require.NoError(t, err)
				require.Len(t, rec.Refeed, 1)
				require.NoError(t, s.Stop())
				return
			}
			if strings.HasPrefix(fault, "short-") {
				require.NoError(t, err)
				require.Len(t, rec.Refeed, 1)
				require.NoError(t, s.Stop())
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestTemporaryConsumerErrorRetries(t *testing.T) {
	var attempts atomic.Int32
	sink := &spanSink{spans: map[pcommon.SpanID]int{}}
	next, err := consumer.NewTraces(func(ctx context.Context, td ptrace.Traces) error {
		if attempts.Add(1) == 1 {
			return io.ErrClosedPipe
		}
		return sink.ConsumeTraces(ctx, td)
	})
	require.NoError(t, err)
	s, _, err := Open(testConfig(t.TempDir()), zap.NewNop(), next)
	require.NoError(t, err)
	want := registerKept(t, s, 1)
	s.Start()
	require.Eventually(t, func() bool { return s.Stats().Committed == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, s.Stop())
	require.EqualValues(t, 2, attempts.Load())
	require.EqualValues(t, 1, s.Stats().DeliveryFailures)
	require.Equal(t, want, sinkSpans(sink))
}
