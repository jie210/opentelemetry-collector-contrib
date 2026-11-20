# Tail-sampling spool experiments

Design notes, test-machine results and benchmark tooling for the disk spool of
the tail sampling processor (`processor/tailsamplingprocessor/internal/spool`).

- `PLAN.md`: design plan.
- `FIX-PLAN.md`: fixes and test-machine results.
- `bench/`: a separate Go module with the benchmark tooling.
  - `cmd/otelcol-tailspool`: collector with the OTLP receiver, tail sampling
    and the OTLP exporter, built from this repository.
  - `cmd/tsbench`: deterministic OTLP load generator, a sink that counts every
    span, and a report comparing two sink dumps span by span.
  - `cmd/poisonprobe`: sends a span whose attributes nest deeper than the
    protobuf decoder accepts over OTLP/HTTP JSON. The collector config needs an
    OTLP receiver with the HTTP protocol enabled.
  - `configs/official.yaml`, `configs/spool.yaml`: the two collector configs.
  - `run.sh`: runs one benchmark on the test machine; see its header.

Build with `GOWORK=off go build ./...` from `bench/`.

The earlier standalone prototype and its results were removed after
`8ec24bf6ef9`; use `git show 8ec24bf6ef9:experiments/tailspool/<file>` to read
them.
