"""Run in an empty test directory next to the Linux tailspool binary, as root."""
import hashlib
import json
import os
import pathlib
import statistics
import subprocess
import time
import urllib.request

root = pathlib.Path.cwd()
binary = str(root / "tailspool")
disk = pathlib.Path("/sys/block/vdb/stat")
clock_ticks = os.sysconf("SC_CLK_TCK")
page_size = os.sysconf("SC_PAGE_SIZE")

def device():
    fields = disk.read_text().split()
    return [int(fields[index]) * 512 for index in (2, 6)]

def stats():
    with urllib.request.urlopen("http://127.0.0.1:18888/stats", timeout=3) as r:
        return json.load(r)

def manifest(path):
    rows = sorted(path.read_bytes().splitlines())
    return len(rows), hashlib.sha256(b"\n".join(rows)).hexdigest()

def run(mode, rate, duration, phase):
    name = f"{phase}-{mode}-{rate}"
    folder = root / name
    folder.mkdir()
    n = rate * duration
    expected = root / f"expected-{n}.sha256"
    if not expected.exists():
        with expected.open("wb") as f:
            subprocess.run([binary, "-mode=expected", "-n", str(n)], stdout=f, check=True)
    wanted = manifest(expected)
    unit = f"otel-tail-compare-{os.getpid()}-{name}"
    subprocess.run(["systemd-run", "--quiet", "--unit", unit, "-p", "MemoryMax=8G",
                    "-p","CPUAccounting=yes","-p","MemoryAccounting=yes",binary,
                    "-mode",mode,"-dir",str(folder)],check=True)
    client = None
    try:
        for _ in range(60):
            try:
                stats()
                break
            except OSError:
                time.sleep(.25)
        pid = int(subprocess.check_output(["systemctl","show","-p","MainPID","--value",unit]))
        group = subprocess.check_output(["systemctl","show","-p","ControlGroup","--value",unit],text=True).strip()
        cg = pathlib.Path("/sys/fs/cgroup") / group.lstrip("/")
        ps = pathlib.Path(f"/proc/{pid}")
        def cpu():
            return sum(map(int,(ps/"stat").read_text().split()[13:15])) / clock_ticks
        def sample():
            return dict(t=round(time.monotonic()-start,3),**stats(),cpu=cpu(),
                        rss=int((ps/"statm").read_text().split()[1])*page_size,
                        memory=int((cg/"memory.current").read_text()))
        start_disk = device()
        start = time.monotonic()
        samples = [sample()]
        loadlog = (folder / "load.json").open("w")
        client = subprocess.Popen([binary,"-mode=load","-rate",str(rate),"-n",str(n)],stdout=loadlog)
        while client.poll() is None:
            try:
                samples.append(sample())
            except TimeoutError:
                pass
            time.sleep(1)
        loadlog.close()
        if client.returncode:
            raise RuntimeError(f"generator exited {client.returncode}")
        sent = json.loads((folder/"load.json").read_text())
        while time.monotonic()-start < sent["seconds"]+90:
            row=sample()
            samples.append(row)
            if (row["decided"]==n*32 and row["sent"]==wanted[0] and row.get("pending_ids",0)==0):
                break
            time.sleep(.5)
        final=samples[-1]
        found=manifest(folder/"actual.sha256")
        cpu_elapsed=final["cpu"]-samples[0]["cpu"]
        peak_cg=int((cg/"memory.peak").read_text())
        end_disk=device()
        complete=(found==wanted and final["accepted"]==n*128 and final["decided"]==n*32 and not final.get("error"))
        delay=max(0,final["t"]-sent["seconds"])
        # Compare pending state late vs early in the steady part, measured
        # in seconds of target traffic (one sample per second, 5s decision wait).
        def pending(lo,hi):
            return [max(0,r["accepted"]/4-r["decided"])/(rate*32) for r in samples if lo<=r["t"]<hi]
        a=pending(duration*.33,duration*.55)
        b=pending(duration*.73,duration*.95)
        growth=statistics.median(b)-statistics.median(a) if a and b else None
        result=dict(mode=mode,rate=rate,duration=duration,requests=n,accepted_spans=final["accepted"],
                    ingest_spans_s=n*128/sent["seconds"],MiB_s=sent["bytes"]/2**20/sent["seconds"],
                    drain_s=delay,cpu_cores=cpu_elapsed/final["t"],rss_MiB=max(r["rss"] for r in samples)/2**20,
                    cgroup_MiB=peak_cg/2**20,device_read_MiB=(end_disk[0]-start_disk[0])/2**20,
                    device_write_MiB=(end_disk[1]-start_disk[1])/2**20,exported=found[0],
                    digest_matches=found==wanted,complete=complete,backlog_growth_s=growth,
                    pass_load=complete and n/sent["seconds"]>=rate*.95 and delay<=10 and growth is not None and growth<=2,
                    raw_appends=final.get("appends"),raw_write_bytes=final.get("written"),raw_read_bytes=final.get("read"),
                    raw_reads=final.get("reads"),input_bytes=sent["bytes"])
        if mode.startswith("raw") and (final["appends"]!=n or final["written"]!=sent["bytes"]):raise RuntimeError("write-once mismatch")
        (folder/"samples.json").write_text(json.dumps(samples))
    finally:
        if client and client.poll() is None:
            client.terminate()
            client.wait()
        subprocess.run(["systemctl","stop",unit],check=False)
    subprocess.run(["sync","-f",str(folder)],check=True)
    flushed=device()
    result["device_write_flushed_MiB"]=(flushed[1]-start_disk[1])/2**20
    (folder/"summary.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    if not complete:raise RuntimeError("incomplete output: "+name)
    return result

def refine():
    # Use identical requests and run length for the paired load near Pebble's limit.
    passing=6
    for rate in (12,18,21):
        row=run("pebble",rate,30,"refine")
        if not row["pass_load"]:
            break
        passing=rate
    for mode in ("pebble","raw"):
        run(mode,passing,120,"pair")

    # Measure the gap between the coarse raw staircase points.
    if run("raw",288,30,"refine")["pass_load"]:
        run("raw",288,120,"confirm")

    # Charge every sealed segment to the device, including its last segment.
    sync_best=0
    for rate in (24,192,288,384):
        if not run("raw-sync",rate,30,"sync")["pass_load"]:
            break
        sync_best=rate
    if sync_best:
        run("raw-sync",sync_best,120,"confirm")


if __name__=="__main__" and os.environ.get("TAIL_BENCH_REFINE")=="1":
    refine()
elif __name__=="__main__":
    best={}
    forced=os.environ.get("TAIL_BENCH_SYNC")=="1"
    active={"raw-sync"} if forced else {"raw","pebble"}
    for i,rate in enumerate((6,24,48,96,192,384,768,1536)):
        for mode in (("raw-sync",) if forced else (("pebble","raw") if i%2==0 else ("raw","pebble"))):
            if mode not in active:continue
            row=run(mode,rate,30,"ramp")
            if row["pass_load"]:best[mode]=rate
            else:active.remove(mode)
        if not active:break
    for mode,rate in best.items():
        run(mode,rate,120,"hold")
