"""30s tail sampling with natural file-cache eviction in a backend-only cgroup."""
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import urllib.request

from benchmark import device, manifest

ROOT = Path.cwd()
BIN = str(ROOT / "tailcold")
DEV = Path("/sys/class/block/vdb/dev").read_text().strip()
MIB = 2**20
WAIT = 30
LIMIT = 512 * MIB


def counters(path):
    return {k:int(v) for k,v in (line.split() for line in path.read_text().splitlines())}


def cg_io(cg):
    for line in (cg/"io.stat").read_text().splitlines():
        dev, *fields = line.split()
        if dev == DEV:
            return {k:int(v) for k,v in (pair.split("=") for pair in fields)}
    return {"rbytes":0,"wbytes":0}


def read_stats():
    with urllib.request.urlopen("http://127.0.0.1:18888/stats",timeout=4) as response:
        return json.load(response)


def slope(samples, field):
    t = [s["t"] for s in samples]
    y = [s[field] for s in samples]
    mean = statistics.mean(t)
    return sum((x-mean)*v for x,v in zip(t,y)) / sum((x-mean)**2 for x in t)


def run(mode,rate,duration,phase):
    directory = ROOT / f"{phase}-{mode}-{rate}"
    directory.mkdir()
    n = rate*duration
    expected = ROOT / f"expected-{n}.sha256"
    if not expected.exists():
        with expected.open("wb") as f:
            subprocess.run([BIN,"-mode=expected","-n",str(n)],stdout=f,check=True)
    wanted = manifest(expected)
    unit = f"tail-cold-{os.getpid()}-{phase}-{mode}-{rate}"
    subprocess.run(["sync","-f",str(ROOT)],check=True)
    subprocess.run(["systemd-run","--quiet","--unit",unit,"-p",f"MemoryMax={LIMIT}",
                    "-p","IOAccounting=yes","-p","MemorySwapMax=0","-p","TimeoutStopSec=25","-E","GOMEMLIMIT=320MiB",
                    BIN,"-mode",mode,"-dir",str(directory),"-decision-wait=30s",
                    "-num-traces=500000","-probe-reads"],check=True)
    client = None
    samples = []
    result = dict(mode=mode,rate=rate,duration=duration,phase=phase,pass_load=False,complete=False)
    try:
        for _ in range(80):
            try:
                read_stats()
                break
            except OSError:
                time.sleep(.25)
        cg = Path("/sys/fs/cgroup") / subprocess.check_output(
            ["systemctl","show","-p","ControlGroup","--value",unit],text=True).strip().lstrip("/")
        pid = subprocess.check_output(["systemctl","show","-p","MainPID","--value",unit],text=True).strip()
        proc = Path("/proc") / pid
        if not (cg/"io.stat").is_file():
            raise AssertionError("backend cgroup I/O accounting unavailable")
        assert int((cg/"memory.max").read_text())==LIMIT and int((cg/"memory.swap.max").read_text())==0
        start = time.monotonic()
        def sample():
            s=read_stats()
            mem=counters(cg/"memory.stat")
            io=cg_io(cg)
            r,w=device()
            return dict(**s,t=time.monotonic()-start, anon=mem["anon"],file=mem["file"],
                        refault=mem["workingset_refault_file"],mem=int((cg/"memory.current").read_text()),
                        rss=int((proc/"statm").read_text().split()[1])*os.sysconf("SC_PAGE_SIZE"),
                        cpu=counters(cg/"cpu.stat")["usage_usec"]/1e6, cr=io["rbytes"],cw=io["wbytes"],dr=r,dw=w)
        samples.append(sample())
        with (directory/"load.json").open("w") as f:
            client = subprocess.Popen([BIN,"-mode=load","-rate",str(rate),"-n",str(n)],stdout=f)
            while client.poll() is None:
                try:
                    samples.append(sample())
                except TimeoutError:
                    pass
                time.sleep(.8)
            if client.returncode:
                raise RuntimeError(f"load exited {client.returncode}")
        sent=json.loads((directory/"load.json").read_text())
        while time.monotonic()-start < sent["seconds"]+150:
            s=sample()
            samples.append(s)
            if s["decided"]==n*32 and s["sent"]==wanted[0] and not s.get("pending_ids",0):
                break
            time.sleep(.5)
        z=samples[-1]
        first=samples[0]
        found=manifest(directory/"actual.sha256")
        window=[s for s in samples if WAIT+6<=s["t"]<=duration-2]
        if len(window)<8:
            raise RuntimeError("insufficient steady-state observations")
        half=len(window)//2
        def lag(part):
            return statistics.median((s["accepted"]/4-s["decided"])/(rate*32) for s in part)
        growth=lag(window[half:])-lag(window[:half])
        delay=max(0,z["t"]-sent["seconds"])
        speed=n/sent["seconds"]
        steady=slope(window,"accepted")/128
        decisions=slope(window,"decided")/32
        events=counters(cg/"memory.events")
        complete=(sent["requests"]==n and z["accepted"]==n*128 and z["decided"]==n*32 and
                  found==wanted and not z.get("error") and z["probe_errors"]==0)
        if mode=="raw":
            complete &= z["appends"]==n and z["written"]==sent["bytes"] and z["reads"]==sum(
                ((32*i+99)//100)*100 <32*(i+1) for i in range(n))
        else:
            complete &= z["batched_ids"]==n*32 and z["single_take"]==0 and z["take_many"]>0
        result.update(complete=complete,pass_load=bool(complete and speed>=.95*rate and steady>=.95*rate
                      and decisions>=.95*rate and delay<=WAIT+5 and growth<=2 and not events["oom_kill"]),
                      requests=n,accepted=z["accepted"],sent=z["sent"],digest=found[1],input_bytes=sent["bytes"],
                      MiB_s=sent["bytes"]/MIB/sent["seconds"],steady_rps=steady,decision_rps=decisions,
                      drain_s=delay,growth_s=growth,cpu=(z["cpu"]-first["cpu"])/(z["t"]-first["t"]),
                      peak_rss_MiB=max(s["rss"] for s in samples)/MIB,peak_cgroup_MiB=int((cg/"memory.peak").read_text())/MIB,
                      steady_rss_MiB=statistics.median(s["rss"] for s in window)/MIB,
                      steady_anon_MiB=statistics.median(s["anon"] for s in window)/MIB,
                      steady_file_MiB=statistics.median(s["file"] for s in window)/MIB,
                      cg_read_MiB=(z["cr"]-first["cr"])/MIB,cg_write_MiB=(z["cw"]-first["cw"])/MIB,
                      steady_read_MiB_s=slope(window,"cr")/MIB,steady_write_MiB_s=slope(window,"cw")/MIB,
                      dev_read_MiB=(z["dr"]-first["dr"])/MIB,
                      refault_pages=z["refault"]-first["refault"],memory_events=events,
                      probed=z["probed_calls"],probed_cold=z["probed_cold_calls"],probe_read_MiB=z["probed_read_bytes"]/MIB,
                      logical_read_MiB=z.get("read",0)/MIB if mode=="raw" else None,
                      take_many=z.get("take_many"),batched_ids=z.get("batched_ids"))
    except (OSError,RuntimeError) as error:
        result["error"]=str(error)
        if 'cg' in locals() and (cg/"memory.events").exists():
            result["memory_events"]=counters(cg/"memory.events")
        result["unit_result"]=subprocess.check_output(["systemctl","show",unit,"-p","Result","--value"],text=True).strip()
    finally:
        if client is not None and client.poll() is None:
            client.terminate()
            client.wait()
        subprocess.run(["systemctl","stop",unit],check=False)
    subprocess.run(["sync","-f",str(ROOT)],check=True)
    if samples:
        result["dev_write_flushed_MiB"]=(device()[1]-samples[0]["dw"])/MIB
    (directory/"samples.json").write_text(json.dumps(samples))
    (directory/"summary.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    return result


def campaign():
    results={}
    def measure(mode,rate,duration,phase):
        row=run(mode,rate,duration,phase)
        results[mode,rate,duration]=row
        return row["pass_load"]
    for mode in ("pebble","raw"):
        measure(mode,48,150,"pair")
    best={}
    for mode in ("pebble","raw"):
        good=48 if results[mode,48,150]["pass_load"] else 0
        levels=(72,96,144,192,256,320) if good else (24,32,40)
        high=48 if not good else None
        previous=[]
        if good:previous.append(good)
        for rate in levels:
            if measure(mode,rate,90,"ramp"):
                previous.append(rate)
                good=rate
            else:
                high=rate
                break
        if high and high-good>=8:
            mid=(high+good)//2
            if measure(mode,mid,90,"refine"):previous.append(mid)
        for rate in sorted(set(previous+[12,18]),reverse=True):
            if (mode,rate,150) in results:
                passed=results[mode,rate,150]["pass_load"]
            else:
                passed=measure(mode,rate,150,"hold")
            if passed:
                best[mode]=rate
                break
    if set(best)!={"pebble","raw"}:
        raise RuntimeError("no confirmed sustainable point in one arm")
    common=min(best.values())
    for mode in ("pebble","raw"):
        if (mode,common,150) not in results:
            measure(mode,common,150,"matched")


if __name__=="__main__":
    campaign()
