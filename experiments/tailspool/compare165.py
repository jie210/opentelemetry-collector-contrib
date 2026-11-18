"""Run next to release, batched and benchmark.py on the original test volume."""
import json
from pathlib import Path

import benchmark

def measure(build, mode, rate, seconds, phase):
    benchmark.binary = str(benchmark.root / build)
    label = f"{phase}-{build}"
    row = benchmark.run(mode, rate, seconds, label)
    directory = Path(f"{label}-{mode}-{rate}")
    samples = json.loads((directory / "samples.json").read_text())
    last = samples[-1]
    if mode == "pebble":
        if build == "release":
            assert last["single_take"] == row["requests"] * 32 and not last.get("take_many")
        else:
            assert last["single_take"] == 0 and last["take_many"] and last["batched_ids"] == row["requests"] * 32
    return row["pass_load"]

def matched():
    """Compare memory at the fastest verified Pebble load of this build."""
    for build in ("release","batched"):
        verified=[]
        for path in Path(".").glob(f"*-{build}-pebble-*/summary.json"):
            r=json.loads(path.read_text())
            if r["duration"]==120 and r["pass_load"]:
                verified.append(r["rate"])
        if not verified:
            raise RuntimeError(f"no sustainable Pebble load for {build}")
        rate=max(verified)
        if build=="batched":
            failed=[]
            for path in Path(".").glob(f"*-{build}-pebble-*/summary.json"):
                r=json.loads(path.read_text())
                if not r["pass_load"] and r["rate"]>rate:
                    failed.append(r["rate"])
            if failed and min(failed)>1.3*rate:
                midpoint=(rate+min(failed))//2
                if measure(build,"pebble",midpoint,120,"boundary"):
                    rate=midpoint
        existing=Path(".").glob(f"*-{build}-raw-{rate}/summary.json")
        if not any(json.loads(path.read_text())["duration"]==120 for path in existing):
            measure(build,"raw",rate,120,"matched")
        # 120 requests/s narrowly missed the 95% threshold on the test disk.
        # Confirm the smaller headroom step before quoting the throughput ratio.
        if build=="batched" and rate==96 and Path("boundary-batched-pebble-120/summary.json").exists():
            if measure(build,"pebble",112,120,"near"):
                measure(build,"raw",112,120,"near")



def campaign():
    # Same 120-second reference point as the v0.155.0 campaign. It also
    # provides a confirmed fallback point for the subsequent staircase.
    passed = {}
    for build in ("release", "batched"):
        for mode in ("pebble", "raw"):
            passed[build,mode] = [21] if measure(build,mode,21,120,"pair") else []

    # Both Pebble arms get the same traffic up to their first failing step.
    for build in ("release","batched"):
        levels = passed[build,"pebble"]
        for rate in (24,48,96,192,288,384):
            if measure(build,"pebble",rate,30,"ramp"):
                levels.append(rate)
            else:
                low = levels[-1] if levels else 0
                mid = (rate+low)//2
                if mid>low and measure(build,"pebble",mid,30,"refine"):
                    levels.append(mid)
                break
        for rate in reversed(sorted(set(levels+[6,12,18]))):
            if rate==21 and 21 in levels:break
            if measure(build,"pebble",rate,120,"hold"):break

    # Recheck the old highest confirmed raw point and next upper point.
    for build in ("release","batched"):
        candidates = [192,96,48,24]
        if measure(build,"raw",288,120,"hold"):
            if not measure(build,"raw",384,30,"ramp"):continue
            candidates=[384]
        for rate in candidates:
            if measure(build,"raw",rate,120,"confirm"):break

    # Force all sealed segments to the device on the TakeMany build as well.
    for rate in (288,192,96,48,24):
        if measure("batched","raw-sync",rate,120,"sync"):break
    matched()


if __name__ == "__main__":
    campaign()
