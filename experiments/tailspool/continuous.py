"""Run an uninterrupted 24,576 span/s producer through a backend process kill."""
from collections import Counter
import csv
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

root = Path.cwd()
binary = str(root / "tail-online")
unit = f"tail-online-{os.getpid()}"
backend = root / "backend"
backend.mkdir()
n = 192 * 155


def stats():
    with urllib.request.urlopen("http://127.0.0.1:18888/stats", timeout=2) as r:
        return json.load(r)


def property_value(name):
    return subprocess.check_output(["systemctl", "show", unit, "-p", name, "--value"], text=True).strip()


def expected(outcome):
    output = subprocess.check_output([binary, "-mode=expected", "-n", str(n),
                                      "-ack-log=outcomes.csv", "-ack-filter", outcome])
    return Counter(output.splitlines())


subprocess.run(["systemd-run","--quiet","--unit",unit,"-p","MemoryMax=512M","-p","MemorySwapMax=0",
                "-p","IOAccounting=yes","-p","Restart=on-failure","-p","RestartSec=50ms",
                "-E","GOMEMLIMIT=320MiB", binary,"-mode=compact","-dir",str(backend),
                "-decision-wait=30s","-num-traces=500000","-probe-reads","-checkpoint-bytes=8388608"],check=True)
load = None
try:
    for _ in range(100):
        try:
            stats()
            break
        except OSError:
            time.sleep(.1)
    with (root/"producer.json").open("w") as f:
        load = subprocess.Popen([binary,"-mode=load","-rate=192","-n",str(n),"-ack-log=outcomes.csv"],stdout=f)
    start = time.monotonic()
    killed = None
    samples = []
    while load.poll() is None:
        try:
            s = stats()
            t = time.time_ns()
            samples.append(dict(time_ns=t,wal_bytes=(backend/"records.wal").stat().st_size, **s))
            if killed is None and time.monotonic()-start>=77 and s["checkpoints"]>0:
                pid = int(property_value("MainPID"))
                killed = time.time_ns()
                os.kill(pid,signal.SIGKILL)
        except OSError:
            pass
        time.sleep(.25)
    assert load.returncode == 0 and killed
    for _ in range(180):
        s=stats()
        if not s["pending_ids"] and not s["recovering"]:
            break
        time.sleep(.5)
    assert not s.get("error") and not s["pending_ids"]
    (root/"samples.json").write_text(json.dumps(samples))
    rows=[tuple(map(int,r)) for r in csv.reader((root/"outcomes.csv").open())]
    assert len(rows)==n and {r[0] for r in rows}==set(range(n))
    ack=sorted(r[2] for r in rows if r[3]); pre=max(t for t in ack if t<killed);post=min(t for t in ack if t>=killed)
    want=expected("1"); ambiguous=expected("0")
    actual=Counter((backend/"actual.sha256").read_bytes().splitlines())
    rest=actual-want
    around=[t for t in ack if abs(t-killed)<2_000_000_000]
    gap=max(b-a for a,b in zip(around,around[1:]))
    post_call=min(end for _,begin,end,success in rows if begin>killed and success)
    intervals=[sum(a<=t<b for t in ack)*128/((b-a)/1e9) for a,b in
               ((killed-30_000_000_000,killed),(killed,killed+30_000_000_000),
                (killed+35_000_000_000,killed+65_000_000_000))]
    wal_reductions=[(a["wal_bytes"],b["wal_bytes"]) for a,b in zip(samples,samples[1:])
                    if b["checkpoints"]>a["checkpoints"]]
    before=[x for x in samples if x["time_ns"]<killed][-1]
    during=[x for x in samples if x["time_ns"]>killed and x["recovering"]]
    assert during
    result=dict(target_spans_s=24576, duration_s=155, attempted_batches=n, acknowledged_batches=len(ack),
                failed_batches=n-len(ack), kill_ns=killed,last_ack_ns=pre,first_ack_ns=post,
                gap_ms=(post-pre)/1e6,kill_to_ack_ms=(post-killed)/1e6,
                worst_ack_gap_around_kill_ms=gap/1e6, post_kill_call_ack_ms=(post_call-killed)/1e6,
                ack_spans_s_30s_windows=intervals,wal_checkpoint_sizes=wal_reductions,
                expected_acked=sum(want.values()),actual=sum(actual.values()),missing_acked=sum((want-actual).values()),
                extra_acked=sum((rest & want).values()),ambiguous_exports=sum((rest & ambiguous).values()),
                unexpected=sum((rest-ambiguous).values()),before=before,first_recovery_sample=during[0],
                last_recovery_sample=during[-1],final=s,restarts=int(property_value("NRestarts")),
                wal_bytes=(backend/"records.wal").stat().st_size)
    cg=Path("/sys/fs/cgroup")/property_value("ControlGroup").lstrip("/")
    process_start_ns=(int(property_value("ExecMainStartTimestampMonotonic"))*1000
                      + time.time_ns()-time.monotonic_ns())
    result["first_new_process_call_ack_ms"]=(min(end for _,begin,end,ok in rows if ok and begin>=process_start_ns)-killed)/1e6
    result["memory_peak_bytes"]=int((cg/"memory.peak").read_text())
    result["memory_events"]=(cg/"memory.events").read_text()
    (root/"summary.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    assert result["restarts"]==1 and not result["missing_acked"] and not result["unexpected"] and not result["extra_acked"]
finally:
    if load and load.poll() is None:
        load.terminate()
        load.wait()
    subprocess.run(["systemctl","stop",unit],check=False)
