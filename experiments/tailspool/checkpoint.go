// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
)

var checkpointBytes = flag.Int64("checkpoint-bytes", 16<<20, "checkpoint after this many additional bytes of metadata")

type decisionLRU struct {
	*lru.Cache[traceID, cache.DecisionMetadata]
}

func newDecisionLRU() *decisionLRU {
	c, _ := lru.New[traceID, cache.DecisionMetadata](150000)
	return &decisionLRU{c}
}
func (c *decisionLRU) Put(id traceID, m cache.DecisionMetadata) { c.Add(id, m) }

// Serialize live records and the two bounded late-decision caches under the
// same lock as registrations and sink commits. Rename publishes the snapshot
// and switches the append log in one operation; a torn temp file is ignored.
func (s *spool) checkpoint() error {
	j := s.journal
	if err := j.w.Flush(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, "records.wal")
	f, err := os.CreateTemp(s.dir, ".checkpoint-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	out := journal{f: f, w: bufio.NewWriterSize(f, 256<<10)}
	header := le.AppendUint64([]byte{'H'}, j.seq)
	header = le.AppendUint64(header, uint64(j.audit.count))
	header = le.AppendUint64(header, uint64(j.checkpoints+1))
	if err = out.frame(header); err != nil {
		return err
	}
	for _, v := range []struct {
		tag byte
		c   cache.Cache
	}{{'Y', s.yes}, {'N', s.no}} {
		ids := v.c.(*decisionLRU).Keys()
		for len(ids) > 0 {
			n := min(len(ids), 4096)
			b := []byte{v.tag}
			for _, id := range ids[:n] {
				b = append(b, id[:]...)
			}
			if err = out.frame(b); err != nil {
				return err
			}
			ids = ids[n:]
		}
	}
	seqs := make([]uint64, 0, len(j.live))
	for seq := range j.live {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	for _, seq := range seqs {
		r := j.live[seq]
		b := le.AppendUint64([]byte{'T'}, seq)
		b = le.AppendUint64(b, uint64(r.due))
		b = le.AppendUint64(b, uint64(r.inbox))
		b = le.AppendUint32(b, uint32(r.seg.number))
		b = le.AppendUint64(b, uint64(r.offset))
		b = le.AppendUint32(b, uint32(r.length))
		for i := 0; i < len(r.fields); i += 17 {
			id := traceID(r.fields[i : i+16])
			flags := r.fields[i+16] & 1
			if r.keep[id] {
				flags |= 2
			} else if !slices.Contains(s.index[id], r) {
				flags |= 4
			}
			b = append(b, id[:]...)
			b = append(b, flags)
		}
		if err = out.frame(b); err != nil {
			return err
		}
	}
	if err = out.frame([]byte{'E'}); err != nil {
		return err
	}
	if err = out.w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	published = true
	old := j.f
	j.f = f
	j.w = out.w
	j.delta = 0
	j.checkpoints++
	return old.Close()
}
