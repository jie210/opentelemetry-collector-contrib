"""Exercise process death between two acknowledged producer ranges on the test vdb."""
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import time

from pressure import BIN, LIMIT, ROOT, read_stats

RATE_SPANS = 24576
SPANS_PER_REQUEST = 128
RATE = RATE_SPANS // SPANS_PER_REQUEST
HALF = 45 * RATE
DIR = ROOT / "restart-24576"
UNIT = f"tail-compact-restart-{os.getpid()}"


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def launch(suffix):
    unit = UNIT + suffix
    command("systemd-run", "--quiet", "--unit", unit, "-p", f"MemoryMax={LIMIT}",
            "-p", "MemorySwapMax=0", "-p", "IOAccounting=yes", "-E", "GOMEMLIMIT=320MiB",
            BIN, "-mode=compact", "-dir", str(DIR), "-decision-wait=30s",
            "-num-traces=500000", "-probe-reads")
    until = time.monotonic() + 160
    while time.monotonic() < until:
        if command("systemctl", "show", "-p", "ActiveState", "--value", unit) == "failed":
            raise RuntimeError(unit + " failed at startup")
        try:
            return read_stats()
        except OSError:
            time.sleep(.25)
    raise TimeoutError(unit)


def load(start):
    result = json.loads(command(BIN, "-mode=load", "-n", str(HALF), "-rate", str(RATE),
                                "-start-request", str(start)))
    assert result["requests"] == HALF
    return result


def main():
    DIR.mkdir()
    expected = DIR / "expected.sha256"
    with expected.open("wb") as out:
        subprocess.run([BIN, "-mode=expected", "-n", str(HALF * 2)], stdout=out, check=True)
    first, second = UNIT + "-a", UNIT + "-b"
    try:
        launch("-a")
        a = load(0)
        before = read_stats()
        assert before["pending_ids"] > 0 and before["sent"] > 0
        subprocess.run(["systemctl", "kill", "--signal=SIGKILL", first], check=True)
        for _ in range(80):
            if command("systemctl", "show", "-p", "MainPID", "--value", first) == "0":
                break
            time.sleep(.1)
        started = time.monotonic()
        restored = launch("-b")
        ready = time.monotonic() - started
        assert restored["restored_ids"] > 0
        b = load(HALF)
        for _ in range(180):
            after = read_stats()
            if after["pending_ids"] == 0:
                break
            time.sleep(.5)
        assert not after["pending_ids"] and not after.get("error")
        actual = (DIR / "actual.sha256").read_bytes().splitlines()
        wanted = expected.read_bytes().splitlines()
        diff = Counter(wanted)
        diff.subtract(actual)
        result = dict(target_spans_s=RATE_SPANS, ack_spans=(a["requests"]+b["requests"])*128,
                      before=before, restored=restored, after=after, restart_to_ready_s=ready,
                      load_before=a, load_after=b, expected=len(wanted), exported=len(actual),
                      missing=sum(n for n in diff.values() if n>0),
                      extra=sum(-n for n in diff.values() if n<0))
        (DIR/"summary.json").write_text(json.dumps(result,indent=2))
        print(json.dumps(result),flush=True)
        assert result["missing"]==result["extra"]==0
    finally:
        for unit in (first,second):
            subprocess.run(["systemctl","stop",unit],check=False)


if __name__ == "__main__":
    main()
