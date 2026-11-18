// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
)

// Single writer under spool.mu. Frames order registrations, decisions and sink
// commits. A flushed frame and the earlier raw Write survive a process SIGKILL.
type journal struct {
	f           *os.File
	w           *bufio.Writer
	audit       *audit
	replay      bool
	seq         uint64
	live        map[uint64]*record
	adopted     map[int64]bool
	delta       int64
	checkpoints int64
}

var le = binary.LittleEndian

func (j *journal) frame(b []byte) error {
	j.delta += int64(len(b) + 8)
	var hdr [8]byte
	le.PutUint32(hdr[:4], uint32(len(b)))
	le.PutUint32(hdr[4:], crc32.ChecksumIEEE(b))
	if _, err := j.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := j.w.Write(b)
	return err
}
func (j *journal) appendRecord(s *spool, r *record, ids map[traceID]bool) error {
	j.seq++
	r.seq = j.seq
	b := make([]byte, 0, 17+17*len(ids))
	b = append(b, 'T')
	b = le.AppendUint64(b, r.seq)
	b = le.AppendUint64(b, uint64(r.due))
	b = le.AppendUint64(b, uint64(r.inbox))
	r.seg.records++
	b = le.AppendUint32(b, uint32(r.seg.number))
	b = le.AppendUint64(b, uint64(r.offset))
	b = le.AppendUint32(b, uint32(r.length))
	for id, e := range ids {
		b = append(b, id[:]...)
		var flags byte
		if e {
			flags = 1
		}
		if _, ok := s.yes.Get(id); ok {
			flags |= 2
			r.markKeep(id)
		} else if _, ok := s.no.Get(id); ok {
			flags |= 4
		} else {
			r.pending++
			s.index[id] = append(s.index[id], r)
		}
		b = append(b, flags)
	}
	r.fields = b[41:]
	j.live[r.seq] = r
	if err := j.frame(b); err != nil {
		return err
	}
	return j.w.Flush()
}
func (j *journal) decision(id traceID, keep bool) error {
	var b [17]byte
	b[0] = 'D'
	if keep {
		b[0] = 'K'
	}
	copy(b[1:], id[:])
	return j.frame(b[:])
}
func (j *journal) complete(r *record) error {
	delete(j.live, r.seq)
	a := j.audit
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.Flush(); err != nil {
		return err
	}
	var b [17]byte
	b[0] = 'C'
	le.PutUint64(b[1:], r.seq)
	le.PutUint64(b[9:], uint64(a.count))
	return j.frame(b[:])
}

func (j *journal) scan(apply func([]byte) error) error {
	reader := bufio.NewReaderSize(j.f, 256<<10)
	var offset int64
	var h [8]byte
	for {
		_, err := io.ReadFull(reader, h[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
		size := le.Uint32(h[:])
		if size == 0 || size > 64<<20 {
			return fmt.Errorf("bad journal frame size at %d", offset)
		}
		b := make([]byte, size)
		_, err = io.ReadFull(reader, b)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(b) != le.Uint32(h[4:]) {
			return fmt.Errorf("journal checksum at %d", offset)
		}
		if err = apply(b); err != nil {
			return err
		}
		offset += 8 + int64(size)
	}
	if err := j.f.Truncate(offset); err != nil {
		return err
	}
	_, err := j.f.Seek(offset, io.SeekStart)
	return err
}

// Rebuild indexes from metadata only. Open raw segments after all complete
// frames have been applied, since fully exported segments may be unlinked.
func (s *spool) recover(output *os.File, a *audit) (map[traceID]bool, error) {
	path := filepath.Join(s.dir, "records.wal")
	if _, e := os.Stat(path); os.IsNotExist(e) {
		files, e := filepath.Glob(filepath.Join(s.dir, "*.otlp"))
		if e != nil {
			return nil, e
		}
		st, e := output.Stat()
		if e != nil {
			return nil, e
		}
		if len(files) > 0 || st.Size() > 0 {
			return nil, fmt.Errorf("existing output or segments without recovery journal")
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()
	j := &journal{f: f, w: bufio.NewWriterSize(f, 256<<10), audit: a, replay: true}
	s.journal = j
	records := make(map[uint64]*record)
	j.live = records
	j.adopted = make(map[int64]bool)
	legacyDue := time.Now().Add(*decisionWait).UnixNano()
	segments := make(map[int]*segment)
	pending := make(map[traceID]bool)
	var count int64
	snapshot := false
	err = j.scan(func(b []byte) error {
		j.delta += int64(len(b) + 8)
		switch b[0] {
		case 'E':
			snapshot = false
			j.delta = 0
		case 'H':
			snapshot = true
			if len(b) != 25 {
				return fmt.Errorf("bad checkpoint header")
			}
			j.seq = le.Uint64(b[1:])
			count = int64(le.Uint64(b[9:]))
			j.checkpoints = int64(le.Uint64(b[17:]))
			j.delta = 0
		case 'Y', 'N':
			if (len(b)-1)%16 != 0 {
				return fmt.Errorf("bad checkpoint cache")
			}
			c := s.yes
			if b[0] == 'N' {
				c = s.no
			}
			for i := 1; i < len(b); i += 16 {
				c.Put(traceID(b[i:i+16]), cache.DecisionMetadata{})
			}
		case 'T', 'R':
			seq, due, inbox := j.seq+1, legacyDue, int64(0)
			if b[0] == 'T' {
				if len(b) < 42 {
					return fmt.Errorf("short timestamped record")
				}
				seq = le.Uint64(b[1:])
				due = int64(le.Uint64(b[9:]))
				inbox = int64(le.Uint64(b[17:]))
				b = b[24:]
			}
			if inbox != 0 {
				j.adopted[inbox] = true
			}
			j.seq = max(j.seq, seq)
			if len(b) < 34 || (len(b)-17)%17 != 0 {
				return fmt.Errorf("invalid registration")
			}
			n := int(le.Uint32(b[1:]))
			g := segments[n]
			if g == nil {
				g = &segment{number: n}
				segments[n] = g
			}
			r := &record{seq: seq, due: due, inbox: inbox, fields: b[17:], seg: g, offset: int64(le.Uint64(b[5:])), length: int(le.Uint32(b[13:]))}
			records[r.seq] = r
			g.records++
			for pos := 17; pos < len(b); pos += 17 {
				id := traceID(b[pos : pos+16])
				flags := b[pos+16]
				switch flags & 6 {
				case 2:
					if !snapshot {
						s.yes.Get(id)
					}
					r.markKeep(id)
				case 4:
					if !snapshot {
						s.no.Get(id)
					}
				case 0:
					r.pending++
					s.index[id] = append(s.index[id], r)
					pending[id] = pending[id] || flags&1 != 0
				default:
					return fmt.Errorf("invalid recorded decision")
				}
			}
			s.next = max(s.next, n+1)
		case 'D', 'K':
			if len(b) != 17 {
				return fmt.Errorf("invalid decision")
			}
			id := traceID(b[1:])
			c := observedCache{s.no, s, false}
			if b[0] == 'K' {
				c = observedCache{s.yes, s, true}
			}
			c.apply(id, cache.DecisionMetadata{})
			delete(pending, id)
		case 'C':
			if len(b) != 17 {
				return fmt.Errorf("invalid commit")
			}
			seq := le.Uint64(b[1:])
			r := records[seq]
			if r == nil || r.pending != 0 {
				return fmt.Errorf("unexpected commit %d", seq)
			}
			count = int64(le.Uint64(b[9:]))
			r.seg.records--
			delete(records, seq)
			if r.seg.records == 0 {
				delete(segments, r.seg.number)
			}
		default:
			return fmt.Errorf("unknown frame %d", b[0])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	st, err := output.Stat()
	if err != nil {
		return nil, err
	}
	end := 65 * count
	if st.Size() < end {
		return nil, fmt.Errorf("committed manifest is short: %d < %d", st.Size(), end)
	}
	if err = output.Truncate(end); err != nil {
		return nil, err
	}
	if _, err = output.Seek(end, io.SeekStart); err != nil {
		return nil, err
	}
	a.count = count
	for n, g := range segments {
		path := filepath.Join(s.dir, fmt.Sprintf("%08d.otlp", n))
		if g.file, err = os.Open(path); err != nil {
			return nil, err
		}
	}
	paths, err := filepath.Glob(filepath.Join(s.dir, "*.otlp"))
	if err != nil {
		return nil, err
	}
	for _, p := range paths {
		n, e := strconv.Atoi(strings.TrimSuffix(filepath.Base(p), ".otlp"))
		if e != nil {
			continue
		}
		s.next = max(s.next, n+1)
		if segments[n] == nil {
			if err = os.Remove(p); err != nil {
				return nil, err
			}
		}
	}
	j.replay = false
	var ready []uint64
	for seq, r := range records {
		if r.pending == 0 {
			ready = append(ready, seq)
		}
	}
	slices.Sort(ready)
	for _, seq := range ready {
		s.finish(records[seq])
		if s.err != nil {
			return nil, s.err
		}
	}
	success = true
	return pending, nil
}
