package main

import (
	"bytes"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
)

// Linux charges storage reads, including read-ahead, to the initiating task.
// Pin the goroutine across the measured operation to exclude compaction threads.
type readProbe struct{ calls, cold, bytes, errors atomic.Int64 }

func threadReadBytes() (int64, bool) {
	data, err := os.ReadFile("/proc/thread-self/io")
	if err != nil {
		return 0, false
	}
	for line := range bytes.Lines(data) {
		if value, found := bytes.CutPrefix(line, []byte("read_bytes: ")); found {
			n, err := strconv.ParseInt(string(bytes.TrimSpace(value)), 10, 64)
			return n, err == nil
		}
	}
	return 0, false
}

func (p *readProbe) begin() func() {
	runtime.LockOSThread()
	before, ok := threadReadBytes()
	return func() {
		after, valid := threadReadBytes()
		runtime.UnlockOSThread()
		if !ok || !valid || after < before {
			p.errors.Add(1)
			return
		}
		p.calls.Add(1)
		if after > before {
			p.cold.Add(1)
			p.bytes.Add(after - before)
		}
	}
}
