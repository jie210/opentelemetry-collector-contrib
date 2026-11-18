// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Frame types of the registration log.
const (
	// frameHeader starts a checkpoint: version and next record sequence.
	frameHeader = 'H'
	// frameRecord registers a raw request: seq, segment, offset, length,
	// arrival time and the distinct trace IDs it contains.
	frameRecord = 'T'
	// frameKeep and frameDrop publish a decision for every association
	// attached to the trace's pending instance. They carry the processed
	// watermark.
	frameKeep = 'K'
	frameDrop = 'D'
	// frameEvict ends a pending instance removed for capacity; its
	// associations settle as drop. Carries the processed watermark.
	frameEvict = 'V'
	// frameLate settles one association by an inherited decision.
	frameLate = 'L'
	// frameCommit marks a record delivered or fully dropped.
	frameCommit = 'C'
	// frameWatermark attaches all unsettled associations up to seq.
	frameWatermark = 'W'
	// frameMemo restores a decision into the late-span caches only.
	frameMemo = 'M'
)

const walVersion = 1

var errCorrupt = errors.New("tail spool log is corrupt")

type frame struct {
	kind    byte
	seq     uint64
	seg     uint32
	off     int64
	length  uint32
	arrival int64
	id      pcommon.TraceID
	ids     []pcommon.TraceID
	keep    bool
}

func (f *frame) encode(b []byte) []byte {
	b = append(b, f.kind)
	switch f.kind {
	case frameHeader:
		b = binary.LittleEndian.AppendUint32(b, walVersion)
		b = binary.LittleEndian.AppendUint64(b, f.seq)
	case frameRecord:
		b = binary.LittleEndian.AppendUint64(b, f.seq)
		b = binary.LittleEndian.AppendUint32(b, f.seg)
		b = binary.LittleEndian.AppendUint64(b, uint64(f.off))
		b = binary.LittleEndian.AppendUint32(b, f.length)
		b = binary.LittleEndian.AppendUint64(b, uint64(f.arrival))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(f.ids)))
		for _, id := range f.ids {
			b = append(b, id[:]...)
		}
	case frameKeep, frameDrop, frameEvict:
		b = binary.LittleEndian.AppendUint64(b, f.seq)
		b = append(b, f.id[:]...)
	case frameLate:
		b = binary.LittleEndian.AppendUint64(b, f.seq)
		b = append(b, f.id[:]...)
		b = appendBool(b, f.keep)
	case frameCommit, frameWatermark:
		b = binary.LittleEndian.AppendUint64(b, f.seq)
	case frameMemo:
		b = append(b, f.id[:]...)
		b = appendBool(b, f.keep)
	}
	return b
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

func decodeFrame(p []byte) (frame, error) {
	if len(p) == 0 {
		return frame{}, errCorrupt
	}
	f := frame{kind: p[0]}
	r := reader{b: p[1:]}
	switch f.kind {
	case frameHeader:
		if v := r.u32(); v != walVersion && r.ok() {
			return f, fmt.Errorf("tail spool log version %d is not supported", v)
		}
		f.seq = r.u64()
	case frameRecord:
		f.seq = r.u64()
		f.seg = r.u32()
		f.off = int64(r.u64())
		f.length = r.u32()
		f.arrival = int64(r.u64())
		n := int(r.u32())
		if r.ok() && len(r.b) != n*16 {
			return f, errCorrupt
		}
		f.ids = make([]pcommon.TraceID, n)
		for i := range f.ids {
			f.ids[i] = r.id()
		}
	case frameKeep, frameDrop, frameEvict:
		f.seq = r.u64()
		f.id = r.id()
	case frameLate:
		f.seq = r.u64()
		f.id = r.id()
		f.keep = r.u8() == 1
	case frameCommit, frameWatermark:
		f.seq = r.u64()
	case frameMemo:
		f.id = r.id()
		f.keep = r.u8() == 1
	default:
		return f, fmt.Errorf("%w: unknown frame type %q", errCorrupt, f.kind)
	}
	if !r.ok() || len(r.b) != 0 {
		return f, fmt.Errorf("%w: malformed frame %q", errCorrupt, f.kind)
	}
	return f, nil
}

type reader struct {
	b   []byte
	bad bool
}

func (r *reader) ok() bool { return !r.bad }

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return make([]byte, n)
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() byte            { return r.take(1)[0] }
func (r *reader) u32() uint32         { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64         { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) id() pcommon.TraceID { return pcommon.TraceID(r.take(16)) }

// logWriter appends checksummed frames: length, CRC32-C of the payload,
// payload.
type logWriter struct {
	f    *os.File
	w    *bufio.Writer
	size int64
	buf  []byte
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func newLogWriter(f *os.File, size int64) *logWriter {
	return &logWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), size: size}
}

func (l *logWriter) append(fr *frame) error {
	l.buf = fr.encode(append(l.buf[:0], make([]byte, 8)...))
	payload := l.buf[8:]
	binary.LittleEndian.PutUint32(l.buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(l.buf[4:8], crc32.Checksum(payload, castagnoli))
	n, err := l.w.Write(l.buf)
	l.size += int64(n)
	return err
}

func (l *logWriter) flush() error { return l.w.Flush() }

// readLog calls fn for each complete frame and returns the offset after the
// last one. A trailing partial frame, left by a process killed between write
// calls, ends the log; a checksum mismatch is reported as corruption.
func readLog(r io.Reader, fn func(frame) error) (int64, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var off int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, nil
			}
			return off, err
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		if n > 1<<30 {
			return off, fmt.Errorf("%w: frame of %d bytes at offset %d", errCorrupt, n, off)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, nil
			}
			return off, err
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(hdr[4:8]) {
			return off, fmt.Errorf("%w: checksum mismatch at offset %d", errCorrupt, off)
		}
		f, err := decodeFrame(payload)
		if err != nil {
			return off, fmt.Errorf("at offset %d: %w", off, err)
		}
		if err := fn(f); err != nil {
			return off, err
		}
		off += 8 + int64(n)
	}
}
