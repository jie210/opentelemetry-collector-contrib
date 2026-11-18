# OTLP tail-sampling spool experiment

This standalone Go module pins the public tail-sampling processor and Pebble extension to v0.155.0. The executable hosts those components through their factories. The two arms share the gRPC transport, processor version, status-code policy, decision wait, decision cache sizes, and export verifier. No changes to upstream components are required. The harness binds to loopback and writes SHA-256 manifests for local verification.

## Data path

The gRPC codec owns a copy of the decompressed protobuf request while the handler decodes it. In raw mode the handler appends the original bytes to a 64 MiB segment, associates each distinct trace ID with the record's file, offset and length, and passes a new projection to the official processor. The projection contains trace ID, span ID, parent span ID and status code. The experiment fixes the policy to keep traces containing ERROR.

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

The experiment measures one backend. It uses a hash sink and a standalone local gRPC endpoint. Production integration would need Collector receiver and exporter lifecycle wiring, explicit retry/acknowledgement semantics, crash recovery, quotas, and support for the configured set of policies and pre-sampling transformations. Segment files have no restart index. They must be used with a fresh experiment directory. The single store lock includes readback and hashing. A mixed record waits for its last pending trace decision. These choices affect CPU, latency and the retention period.

Upgrading dependencies can change output semantics even when the APIs still compile. Run the full manifest and late-span tests on the target version. The v0.155.0 upstream processor omits resource and scope schema URLs during regrouping. The integration test identifies this specific legacy behavior and always requires exact preservation in the raw arm.

The measured staircase and long-run results are in [the benchmark report](RESULTS.md) and [the per-run measurements](benchmark-results.json).

## v0.165.1 and batch retrieval

The [v0.165.1 report](RESULTS-v165.md) compares the published modules with the same release plus upstream changes [#52954](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/52954) and [#52926](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/52926). The release and backport binaries are built with `build165.sh` using the separate `v165.mod` file. The storage observer preserves optional interface discovery and records actual single and batch reads. The [run summaries](benchmark-results-v165.json) include those counters and the digest of the sorted export manifest.
