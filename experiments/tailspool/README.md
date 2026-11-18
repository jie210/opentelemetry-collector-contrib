# OTLP tail-sampling spool experiment

This standalone Go module pins the public tail-sampling processor and Pebble extension to v0.155.0. The executable hosts those components through their factories. The two arms share the gRPC transport, processor version, status-code policy, decision wait, decision cache sizes, and export verifier. No changes to upstream components are required. The harness binds to loopback and writes SHA-256 manifests for local verification.

## Data path

The gRPC codec owns a copy of the decompressed protobuf request while the handler decodes it. In raw mode the handler appends the original bytes to a 64 MiB segment, associates each distinct trace ID with the record's file, offset and length, and passes a new projection to the official processor. The default policy keeps traces containing ERROR. With a policy configuration file the projection follows the selected policies.

The decision-cache wrappers publish the official keep/drop decisions under the same lock used to register incoming records. Each record becomes eligible for release once all its trace IDs have decisions. A record with any kept IDs is read once, decoded, filtered at the span level and sent to the verifier. An entirely dropped record only releases a reference. A late record inherits decisions still present in the cache. Sealed segments are unlinked once all their records have been processed.

The raw-sync arm additionally calls File.Sync on each sealed segment, and on the last segment at orderly shutdown. In buffered raw mode Linux can reclaim unlinked dirty pages before issuing device writes. The application byte counters measure file writes and reads. The benchmark also measures device sectors and cgroup memory including page cache.

## Running

Build from this directory using Go 1.25 or later.

```sh
go test -race ./...
go build -o tailspool .
```

Run the benchmark in a fresh directory on the target data volume containing the Linux binary and benchmark.py. The driver uses transient systemd services, cgroup v2, an 8 GiB backend memory limit, and device counters for vdb. It writes per-second samples, sorted-manifest comparison results, and a JSON summary for each load point.

```sh
python3 benchmark.py
```

Use TAIL_BENCH_REFINE=1 to run the paired boundary tests and forced-writeback comparison. Use a separate fresh directory with TAIL_BENCH_SYNC=1 for the segment-sync arm. Each workload uses fixed, reproducible inputs. For each kept span, the verifier hashes a serialized single-span trace including its resource, instrumentation scope and both schema URLs. Sorted hash lists retain multiplicity. The fixture oracle constructs the expected kept spans from the generation rule independently of the processor decision. The integration test separately exercises cross-request decisions, late arrivals, events, links, trace state and schema URLs.

## Scope

The experiment measures one backend. It uses a hash sink and a standalone local gRPC endpoint. Production integration would need Collector receiver and exporter lifecycle wiring, explicit retry/acknowledgement semantics, crash recovery, quotas, and support for the configured set of policies and pre-sampling transformations. The raw and raw-sync modes use fresh experiment directories. The compact mode supports restart on the same filesystem. The single store lock includes readback and hashing. A mixed record waits for its last pending trace decision. These choices affect CPU, latency and the retention period.

Upgrading dependencies can change output semantics even when the APIs still compile. Run the full manifest and late-span tests on the target version. The v0.155.0 upstream processor omits resource and scope schema URLs during regrouping. The integration test identifies this specific legacy behavior and always requires exact preservation in the raw arm.

The measured staircase and long-run results are in [the benchmark report](RESULTS.md) and [the per-run measurements](benchmark-results.json).

## v0.165.1 and batch retrieval

The [v0.165.1 report](RESULTS-v165.md) compares the published modules with the same release plus upstream changes [#52954](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/52954) and [#52926](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/52926). The release and backport binaries are built with `build165.sh` using the separate `v165.mod` file. The storage observer preserves optional interface discovery and records actual single and batch reads. The [run summaries](benchmark-results-v165.json) include those counters and the digest of the sorted export manifest.

## Testing under page-cache pressure

Run `build165.sh` and copy its `batched` binary as `tailcold` into a fresh directory on the ext4 test volume. Copy `benchmark.py` and `pressure.py` alongside it and run `python3 -u pressure.py` there as root. The driver expects Linux cgroup v2 with systemd I/O and memory accounting, the device vdb, and unused loopback ports 14317 and 18888.

Both arms use a 30-second decision wait and a 512 MiB backend cgroup with swap disabled. The driver records cgroup device bytes, device-wide sectors, and sampled RSS. Probe mode measures calling-thread storage read bytes for every nonempty TakeMany and every 32nd raw ReadAt. This includes read-ahead issued by the calling thread. Compare these counters within each arm to establish that retrieval triggered physical reads. They measure different operations across arms. The raw arm uses buffered I/O with normal filesystem writeback.

## Policy projection and process restart

Use `-mode=compact -config collector.yaml` to select policies from the normal `processors.tail_sampling.policies` list. The field plan is derived at startup. For example:

```yaml
processors:
  tail_sampling:
    decision_wait: 30s
    num_traces: 500000
    policies:
      - name: important-services
        type: string_attribute
        string_attribute:
          key: service.name
          values: [checkout, payments]
      - name: errors
        type: status_code
        status_code:
          status_codes: [ERROR]
```

Attribute policies select the configured keys on both resources and spans, copying the original value and OTLP type. Latency selects timestamps, trace flags and trace state select the corresponding fields, and span-count policies retain span multiplicity. Nested policies select the union of their dependencies. A pure status-code list uses a mask of observed OTLP enum values. Values, regexes, thresholds, negation and policy precedence are evaluated by the official processor. For OTTL, byte-sensitive and unrecognized policies the whole list uses full-input evaluation. In that case pending traces reference raw records on disk, and the backend reads the complete input at evaluation time. Extension policies require their evaluator to be installed in the host.

The journal records raw segment positions, inherited decisions and sink commits. For status-only input it also records the observed status mask. At restart other key projections are rebuilt from each live raw record, or directly from its index when using full-input evaluation. Fresh and recovered traces share one deadline queue and official sampler. At the original cutoff the spool freezes the decision input; later records inherit the decision. Ready traces enter the processor with a one-second decision batch interval, so the decision and export can follow the cutoff by additional ticks. With an unmodified upstream processor, near-boundary arrivals, rate limits and batch-order-sensitive policies may depend on tick timing. Legacy registrations without timestamps use a fresh window. The compact harness supports a single trace-complete pipeline with blocking overflow. It can read the prior experimental settings for `decision_wait`, `num_traces`, `sampling_strategy`, `block_on_overflow`, `num_shards: 1` and `tail_storage: compact`.

The decision-cache hook supplies the trace ID. Once every trace in a record has a decision, the spool reads that original OTLP record if needed, retains all spans of kept traces and passes them to the downstream verification consumer. A restart truncates uncommitted local sink output and restores the committed decisions before applying new ones.

This prototype targets backend process death on the same filesystem. It relies on the kernel surviving SIGKILL. The hash sink can roll back to a byte offset. An external Kafka exporter needs its own atomic or idempotent delivery protocol. A checkpoint rewrites live records, their original deadlines, both late-decision caches and the committed output position into a checksummed snapshot. Atomic rename turns this file into the new append log. The default threshold is 16 MiB of additional metadata. Snapshot size depends on the live working set. The writer holds the spool mutex while taking the checkpoint. Raw segments are reclaimed as their records finish.

To reproduce, use the `batched` build from `build165.sh` as `tailcold`, alongside `pressure.py`, `benchmark.py`, and `restart.py` in a fresh test directory on vdb. Run the following serially. Each request carries 128 spans. The target in both tests is 24,576 spans/s.

```sh
python3 -c 'import pressure; pressure.run("compact", 24576 // 128, 150, "sustain")'
python3 restart.py
```

The restart driver sends two disjoint fixture ranges. It waits for gRPC ACKs from the first range, kills only the backend with SIGKILL while decisions are pending, and sends the second range after recovery. The expected manifest covers both ranges and preserves duplicate counts.

## Continuous ingress across SIGKILL

Build the TakeMany backport binary as above and copy it as `tail-online` alongside `continuous.py` to a fresh directory on the ext4 test volume. Run `python3 continuous.py` as root. The generator continues scheduling new request IDs throughout the test, records individual gRPC outcomes, and uses a 20 ms transport reconnect backoff. Each logical batch is attempted once. The test compares all actual export digests against the independently constructed ACK manifest and accounts separately for possible exports of calls that received a transport error.

The gRPC listener opens before WAL replay. During bootstrap each accepted batch is published via rename after writing its raw bytes once. The backend adopts this inode as a segment with a hard link. The journal records the inbox ID before unlinking the inbox name so that a second kill during adoption is recoverable. The client-facing gap includes process restart and gRPC reconnect. The stats endpoint becomes available after inbox adoption.
