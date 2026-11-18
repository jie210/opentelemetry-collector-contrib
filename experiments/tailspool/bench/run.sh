#!/bin/bash
# Runs one benchmark on the test machine.
#
#   run.sh NAME MODE RATE REQUESTS [KILL_AT_SECONDS...]
#
# MODE is official or spool. The sink dump, load attempts, collector log and
# a resource summary are written to $OUT/NAME.*. With kill times, the
# collector is killed with SIGKILL at each time (seconds after the load
# starts) and restarted at once; the load retries failed requests.
set -euo pipefail
name=$1 mode=$2 rate=$3 requests=$4
shift 4
kills=("$@")

BIN=${BIN:-/data/tailspool-fold/bin}
OUT=${OUT:-/data/tailspool-fold/out}
export SPOOL_DIR=${SPOOL_DIR:-/data/tailspool-fold/spool}
export SYNC_ON_ROTATE=${SYNC_ON_ROTATE:-true}
PAYLOAD=${PAYLOAD:-512}
TIMEOUT=${TIMEOUT:-30s}
# The official processor exports each sampled trace synchronously from its
# loop; without a queue it cannot keep up and client retries would add
# duplicates to the baseline. The spool delivers from its own workers.
if [[ $mode == official ]]; then
	export EXPORT_QUEUE=${EXPORT_QUEUE:-true}
else
	export EXPORT_QUEUE=${EXPORT_QUEUE:-false}
fi
mkdir -p "$OUT"
rm -rf "$SPOOL_DIR"
wl=(-requests "$requests" -payload "$PAYLOAD")

pkill -9 -f '[t]sbench sink' || true
pkill -9 -f '[o]telcol-tailspool' || true
sleep 1

"$BIN/tsbench" sink "${wl[@]}" -delay "${SINK_DELAY:-0}" >"$OUT/$name.sink.log" 2>&1 &
sink=$!

collector_pid=
start_collector() {
	local before
	before=$(grep -c 'Everything is ready' "$OUT/$name.collector.log" || true)
	"$BIN/otelcol-tailspool" --config "$BIN/$mode.yaml" >>"$OUT/$name.collector.log" 2>&1 &
	collector_pid=$!
	for _ in $(seq 600); do
		if (($(grep -c 'Everything is ready' "$OUT/$name.collector.log" || true) > before)); then
			return
		fi
		if ! kill -0 "$collector_pid" 2>/dev/null; then
			break
		fi
		sleep 0.1
	done
	echo "collector not ready" >&2
	tail -5 "$OUT/$name.collector.log" >&2
	pkill -f 'tsbench' || true
	exit 1
}

# sample records RSS, CPU time and spool size every second.
sample() {
	while kill -0 "$collector_pid" 2>/dev/null; do
		rss=$(awk '/VmRSS/{print $2}' /proc/"$collector_pid"/status 2>/dev/null || echo 0)
		cpu=$(awk '{print $14+$15}' /proc/"$collector_pid"/stat 2>/dev/null || echo 0)
		disk=$(du -sm "$SPOOL_DIR" 2>/dev/null | cut -f1 || echo 0)
		echo "$(date +%s.%N) $collector_pid $rss $cpu ${disk:-0}"
		sleep 1
	done
}

: >"$OUT/$name.collector.log"
: >"$OUT/$name.samples"
start_collector
sample >>"$OUT/$name.samples" &

t0=$(date +%s.%N)
"$BIN/tsbench" load "${wl[@]}" -rate "$rate" -timeout "$TIMEOUT" -attempts-out "$OUT/$name.attempts" >"$OUT/$name.load.log" 2>&1 &
load=$!

for k in "${kills[@]}"; do
	while (($(echo "$(date +%s.%N) - $t0 < $k" | bc))); do sleep 0.05; done
	kill -9 "$collector_pid"
	echo "killed at $(echo "$(date +%s.%N) - $t0" | bc)s" >>"$OUT/$name.events"
	wait "$collector_pid" 2>/dev/null || true
	restart=$(date +%s.%N)
	start_collector
	echo "ready after $(echo "$(date +%s.%N) - $restart" | bc)s" >>"$OUT/$name.events"
	sample >>"$OUT/$name.samples" &
done

wait "$load"
# Wait for the last decisions and deliveries: sink idle for 15s after the
# final traces passed decision_wait.
sleep 35
while :; do
	idle=$(curl -s 127.0.0.1:5318/stats | sed 's/.*idle_ms=//')
	((idle > 15000)) && break
	sleep 2
done
curl -s "127.0.0.1:5318/dump?path=$OUT/$name.counts" >/dev/null
curl -s 127.0.0.1:5318/stats >"$OUT/$name.stats"
grep -h 'VmHWM' /proc/"$collector_pid"/status >>"$OUT/$name.stats" || true
kill "$collector_pid"
wait "$collector_pid" 2>/dev/null || true
kill "$sink"
cat "$OUT/$name.stats"
tail -1 "$OUT/$name.load.log"
