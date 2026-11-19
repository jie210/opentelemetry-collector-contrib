// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/idbatcher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/telemetry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

// startSpool folds the policies, opens the spool and returns what must be
// restored before new data is processed.
func (tsp *tailSamplingSpanProcessor) startSpool() (*spool.Recovered, error) {
	if tsp.recordPolicy || tsp.useTracestate {
		return nil, errors.New("spool does not support the recordpolicy and usetracestate feature gates")
	}
	tsp.foldPlan = &sampling.FoldPlan{}
	for _, p := range tsp.policies {
		folded, err := sampling.Fold(samplingpolicy.UnwrapThresholdEvaluator(p.evaluator), tsp.foldPlan)
		if err != nil {
			return nil, fmt.Errorf("policy %q: %w", p.name, err)
		}
		p.evaluator = samplingpolicy.AsThresholdEvaluator(folded)
	}

	cfg := tsp.cfg.Spool.withDefaults()
	memoSize := func(cacheSize int) int {
		// Without a cache, decisions live as long as the trace stays in memory.
		if cacheSize > 0 {
			return cacheSize
		}
		return int(tsp.cfg.NumTraces)
	}
	sp, recovered, err := spool.Open(spool.Config{
		Directory:          cfg.Directory,
		SegmentBytes:       cfg.SegmentSize,
		CheckpointBytes:    cfg.CheckpointSize,
		SyncOnRotate:       cfg.SyncOnRotate,
		MaxInflightRecords: cfg.MaxInflightRecords,
		MaxInflightBytes:   cfg.MaxInflightBytes,
		KeepMemo:           memoSize(tsp.cfg.DecisionCache.SampledCacheSize),
		DropMemo:           memoSize(tsp.cfg.DecisionCache.NonSampledCacheSize),

		MaxUnprocessedMemory:  cfg.MaxUnprocessedMemory,
		MaxUnprocessedRecords: cfg.MaxUnprocessedRecords,
		MaxUnprocessedBytes:   cfg.MaxUnprocessedBytes,
		MinFreeDisk:           cfg.MinFreeDisk,
		MinDeadlineRemaining:  cfg.MinDeadlineRemaining,
	}, tsp.logger, tsp.nextConsumer)
	if err != nil {
		return nil, err
	}
	tsp.spool = sp
	sp.Start()
	tsp.logger.Info("Tail spool opened",
		zap.String("directory", cfg.Directory),
		zap.Int("refeed_records", len(recovered.Refeed)),
		zap.Int("restored_decisions", len(recovered.Memo)))
	return recovered, nil
}

// consumeSpooled registers the request, which is acknowledged once it is
// written; the loop processes it later. A request the client already gave
// up on is refused, as a retry would register it twice.
func (tsp *tailSamplingSpanProcessor) consumeSpooled(ctx context.Context, td ptrace.Traces) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Splitting moves the spans out of td.
	var m ptrace.ProtoMarshaler
	data, err := m.MarshalTraces(td)
	if err != nil {
		return err
	}
	var assocs []spool.Assoc
	var batches []traceBatch
	for ri, rss := range td.ResourceSpans().All() {
		for _, b := range splitResourceSpansByTrace(rss) {
			b.idx = len(assocs)
			assocs = append(assocs, spool.Assoc{ID: b.id, Resource: uint32(ri)})
			batches = append(batches, b)
		}
	}
	_, err = tsp.spool.Register(ctx, data, assocs, time.Now(), batches)
	return err
}

// processNextSpooled processes the oldest registered record not processed
// yet, reading it back if its fragments were not kept in memory.
func (tsp *tailSamplingSpanProcessor) processNextSpooled(tickChan <-chan time.Time) {
	if time.Now().Before(tsp.spoolRetryAt) {
		return
	}
	rec, payload, ok := tsp.spool.Next()
	if !ok {
		return
	}
	batch, _ := payload.([]traceBatch)
	if batch == nil {
		td, released, err := tsp.spool.ReadUnprocessed(rec)
		if released {
			tsp.spoolRetryDelay = 0
			return
		}
		if err != nil {
			// Skipping the record would move the watermark past its
			// fragments, so later records wait for it until the spool
			// gives up on it.
			tsp.spoolRetryDelay = min(max(2*tsp.spoolRetryDelay, 100*time.Millisecond), 30*time.Second)
			tsp.spoolRetryAt = time.Now().Add(tsp.spoolRetryDelay)
			time.AfterFunc(tsp.spoolRetryDelay, tsp.spool.Poke)
			tsp.logger.Warn("Failed to read tail spool record, retrying",
				zap.Uint64("seq", rec.Seq()), zap.Duration("delay", tsp.spoolRetryDelay), zap.Error(err))
			return
		}
		tsp.spoolRetryDelay = 0
		open := make(map[spool.Assoc]int, len(rec.Assocs()))
		for i, a := range rec.Assocs() {
			open[a] = i
		}
		batch = spooledFragments(td, open)
	}
	tsp.processSpooledRecord(rec, batch, tickChan)
}

// heldTrace is a trace due a decision while fragments of it were queued,
// last being the sequence number of the last record holding one then.
type heldTrace struct {
	id   pcommon.TraceID
	last uint64
}

// heldTraces is a min-heap of held traces by the record they wait for.
type heldTraces []heldTrace

func (h heldTraces) Len() int           { return len(h) }
func (h heldTraces) Less(i, j int) bool { return h[i].last < h[j].last }
func (h heldTraces) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *heldTraces) Push(x any)        { *h = append(*h, x.(heldTrace)) }

func (h *heldTraces) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// holdQueuedTraces moves the traces of batch with fragments still queued to
// the held ones. Fragments registered later do not hold them further.
func (tsp *tailSamplingSpanProcessor) holdQueuedTraces(batch idbatcher.Batch) {
	tsp.spool.Queued(batch, func(id pcommon.TraceID, last uint64) {
		delete(batch, id)
		heap.Push(&tsp.held, heldTrace{id: id, last: last})
	})
}

// decideReleasedTraces decides the held traces whose fragments queued when
// they were due have all been processed.
func (tsp *tailSamplingSpanProcessor) decideReleasedTraces() {
	var batch idbatcher.Batch
	for tsp.held.Len() > 0 && tsp.spool.Begun(tsp.held[0].last) {
		if batch == nil {
			batch = idbatcher.Batch{}
		}
		batch[heap.Pop(&tsp.held).(heldTrace).id] = struct{}{}
	}
	if batch != nil {
		tsp.decideBatch(batch)
	}
}

// spooledFragments splits a stored request into the fragments of the open
// associations, indexed by association.
func spooledFragments(td ptrace.Traces, open map[spool.Assoc]int) []traceBatch {
	var batch []traceBatch
	for ri, rss := range td.ResourceSpans().All() {
		for _, b := range splitResourceSpansByTrace(rss) {
			idx, ok := open[spool.Assoc{ID: b.id, Resource: uint32(ri)}]
			if !ok {
				continue
			}
			b.idx = idx
			batch = append(batch, b)
		}
	}
	return batch
}

// processSpooledRecord handles the fragments of one registered request. Its
// traces arrived when it was registered, however long it waited since.
func (tsp *tailSamplingSpanProcessor) processSpooledRecord(rec *spool.Record, batch []traceBatch, tickChan <-chan time.Time) {
	tsp.spool.Begin(rec)
	for _, b := range batch {
		b.rec = rec
		tsp.processSpooledFragment(b, tickChan, rec.Arrival())
	}
	tsp.spool.End(rec)
}

func (tsp *tailSamplingSpanProcessor) processSpooledFragment(b traceBatch, tickChan <-chan time.Time, arrival time.Time) {
	if _, ok := tsp.sampledIDCache.Get(b.id); ok {
		tsp.spool.Inherit(b.rec, b.idx, true)
		tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.Add(tsp.ctx, b.spanCount, attrSampledTrue)
		return
	}
	if _, ok := tsp.nonSampledIDCache.Get(b.id); ok {
		tsp.spool.Inherit(b.rec, b.idx, false)
		tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.Add(tsp.ctx, b.spanCount, attrSampledFalse)
		return
	}
	if _, ok := tsp.idToTrace[b.id]; !ok && uint64(len(tsp.idToTrace)) >= tsp.cfg.NumTraces {
		tsp.waitForSpace(tickChan)
	}
	tsp.processSpooledTrace(b, arrival)
}

// processSpooledTrace mirrors processTrace, folding the fragment into the
// trace's state instead of storing its spans. A new trace arrived when its
// record was registered, so a decision keeps its deadline however long the
// record waited, including across a restart.
func (tsp *tailSamplingSpanProcessor) processSpooledTrace(b traceBatch, arrival time.Time) {
	id := b.id
	var newTraceIDs int64
	defer func() {
		tsp.telemetry.ProcessorTailSamplingNewTraceIDReceived.Add(tsp.ctx, newTraceIDs)
	}()

	actualData, ok := tsp.idToTrace[id]
	if !ok {
		now := time.Now()
		if arrival.After(now) {
			arrival = now
		}
		actualData = &TraceData{
			arrivalTime: arrival,
			TraceData: samplingpolicy.TraceData{
				SpanCount:       b.spanCount,
				ReceivedBatches: ptrace.NewTraces(),
				Folded:          tsp.foldPlan.NewTrace(),
			},
		}
		tsp.idToTrace[id] = actualData
		tsp.tracesOnMemory.Add(1)
		newTraceIDs++
		actualData.batchID = tsp.decisionBatcher.AddToCurrentBatch(id)
		if waited := now.Sub(arrival); waited >= time.Second {
			left := max(0, tsp.cfg.DecisionWait-waited)
			actualData.batchID = tsp.decisionBatcher.MoveToEarlierBatch(id, actualData.batchID, uint64(left/time.Second))
		}
		if !tsp.blockOnOverflow {
			actualData.deleteElement = tsp.deleteTraceQueue.PushBack(id)
		}
	} else {
		actualData.SpanCount += b.spanCount
	}
	if b.hasRoot && tsp.cfg.DecisionWaitAfterRootReceived > 0 {
		actualData.batchID = tsp.decisionBatcher.MoveToEarlierBatch(id, actualData.batchID, uint64(tsp.cfg.DecisionWaitAfterRootReceived.Seconds()))
	}

	finalDecision := actualData.FinalDecision

	if tsp.maxTraceSizeBytes > 0 || telemetry.IsMetricStatCountBytesSampledEnabled() {
		var m ptrace.ProtoMarshaler
		actualData.SizeBytes += uint64(m.ResourceSpansSize(b.rss))
	}

	if finalDecision == samplingpolicy.Unspecified &&
		tsp.maxTraceSizeBytes > 0 &&
		actualData.SizeBytes > tsp.maxTraceSizeBytes {
		tsp.telemetry.ProcessorTailSamplingTracesDroppedTooLarge.Add(tsp.ctx, 1)
		actualData.FinalDecision = samplingpolicy.NotSampled
		actualData.Folded = nil
		tsp.decisionBatcher.RemoveFromBatch(id, actualData.batchID)
		tsp.spool.Attach(b.rec, b.idx)
		tsp.spool.Decide(id, false)
		tsp.releaseNotSampledTrace(id, actualData)
		return
	}

	switch finalDecision {
	case samplingpolicy.Unspecified:
		frag := ptrace.NewTraces()
		appendToTraces(frag, b.rss)
		tsp.foldPlan.Add(tsp.ctx, id, actualData.Folded, frag)
		tsp.spool.Attach(b.rec, b.idx)
		return
	case samplingpolicy.Sampled:
		tsp.spool.Inherit(b.rec, b.idx, true)
	case samplingpolicy.NotSampled:
		tsp.spool.Inherit(b.rec, b.idx, false)
		tsp.releaseNotSampledTrace(id, actualData)
	default:
		tsp.logger.Warn("Unexpected sampling decision", zap.Int("decision", int(finalDecision)))
		tsp.spool.Inherit(b.rec, b.idx, false)
	}

	if !actualData.decisionTime.IsZero() {
		tsp.telemetry.ProcessorTailSamplingSamplingLateSpanAge.Record(tsp.ctx, int64(time.Since(actualData.decisionTime)/time.Second))
	}
}

// restoreSpool rebuilds the decisions and pending traces of the previous
// run before new requests are processed.
func (tsp *tailSamplingSpanProcessor) restoreSpool(recovered *spool.Recovered, tickChan <-chan time.Time) {
	start := time.Now()
	for _, e := range recovered.Memo {
		c, size, decision := tsp.nonSampledIDCache, tsp.cfg.DecisionCache.NonSampledCacheSize, samplingpolicy.NotSampled
		if e.Keep {
			c, size, decision = tsp.sampledIDCache, tsp.cfg.DecisionCache.SampledCacheSize, samplingpolicy.Sampled
		}
		if size > 0 {
			c.Put(e.ID, cache.DecisionMetadata{})
			continue
		}
		if _, ok := tsp.idToTrace[e.ID]; ok || uint64(len(tsp.idToTrace)) >= tsp.cfg.NumTraces {
			continue
		}
		td := &TraceData{
			arrivalTime:   start,
			decisionTime:  start,
			FinalDecision: decision,
			TraceData:     samplingpolicy.TraceData{ReceivedBatches: ptrace.NewTraces()},
		}
		tsp.idToTrace[e.ID] = td
		tsp.tracesOnMemory.Add(1)
		if !tsp.blockOnOverflow {
			td.deleteElement = tsp.deleteTraceQueue.PushBack(e.ID)
		}
	}

	var fragments int
	for _, rr := range recovered.Refeed {
		// An unreadable record has been released by the spool.
		td, err := tsp.spool.ReadRecovered(rr.Record)
		if err != nil {
			continue
		}
		open := make(map[spool.Assoc]int, len(rr.Assocs))
		for _, idx := range rr.Assocs {
			open[rr.Record.Assocs()[idx]] = idx
		}
		batch := spooledFragments(td, open)
		if len(batch) == 0 {
			continue
		}
		fragments += len(batch)
		tsp.processSpooledRecord(rr.Record, batch, tickChan)
	}
	tsp.spool.Flush()
	tsp.logger.Info("Tail spool restored",
		zap.Int("refeed_records", len(recovered.Refeed)),
		zap.Int("refeed_fragments", fragments),
		zap.Int("pending_traces", len(tsp.idToTrace)),
		zap.Duration("duration", time.Since(start)))
}
