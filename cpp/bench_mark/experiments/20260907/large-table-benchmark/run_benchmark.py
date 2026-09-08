# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# License); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


import argparse
import datetime
import json
from pathlib import Path
import shlex
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
BUILD = ROOT / "cpp/target/writer-throughput-xcode21"
CXX = "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/clang++"
SDK = "/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"
BINARY = OUT / "table_write_throughput_xcode21"

def build():
    flags = {line.split(" = ")[0]: shlex.split(line.split(" = ")[1])
             for line in (BUILD / "src/CMakeFiles/tsfile.dir/flags.make").read_text().splitlines()
             if " = " in line}
    command = [CXX, "-std=c++11", "-O3", "-DNDEBUG", "-mcpu=native", "-march=native", "-flto",
               "-isysroot", SDK,
               *[x for x in flags["CXX_DEFINES"] if x not in ["-DTSFILE_BUILDING", "-Dtsfile_EXPORTS"]],
               *flags["CXX_INCLUDES"], str(OUT / "table_write_throughput.cc"),
               "-L" + str(BUILD / "lib"), "-ltsfile", "-Wl,-rpath," + str(BUILD / "lib"),
               "-o", str(BINARY)]
    (OUT / "compile-command-xcode21.json").write_text(json.dumps(command, indent=2))
    with (OUT / "compile-xcode21.log").open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
    print("BUILD COMPLETE", flush=True)

def run(run_dir, label, config, rows, verify_all=False, retain=False):
    output = run_dir / (label + ".tsfile")
    command = [str(BINARY), "--output", str(output), "--rows-per-device", str(rows)]
    for key, value in config.items():
        command += ["--" + key.replace("_", "-"), str(value)]
    if verify_all:
        command.append("--verify-all")
    (run_dir / (label + ".command.json")).write_text(json.dumps(command, indent=2))
    result = None
    print(json.dumps({"run": label, "config": config, "rows_per_device": rows}), flush=True)
    with (run_dir / (label + ".log")).open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log.write(line)
            log.flush()
            try:
                event = json.loads(line)
            except ValueError:
                print(line.rstrip(), flush=True)
                continue
            if event.get("event") == "result":
                result = event
            if event.get("event") in ["progress", "result", "write_complete"]:
                print(line.rstrip(), flush=True)
        status = process.wait()
    if status != 0 or result is None:
        raise RuntimeError(f"{label} failed ({status}); see log")
    assert result["rows"] == rows * 10
    assert result["numeric_points"] == rows * 50
    assert result["tablets"] == rows // 10000
    assert result["verification"] == "passed"
    result["config"] = config
    result["label"] = label
    result["output"] = str(output)
    (run_dir / (label + ".result.json")).write_text(json.dumps(result, indent=2))
    if not retain:
        output.unlink()  # Only this run's generated, successfully verified fixture.
    return result

def tune(run_dir):
    results = []
    for codec in ["plain_none", "plain_lz4", "default_lz4"]:
        for threads in [0, 6, 15]:
            config = dict(codec=codec, threads=threads, page_rows=10000, chunk_mib=512)
            results.append(run(run_dir, f"scan-{codec}-{threads}", config, 500000))
    top = sorted(results, key=lambda r: r["end_to_end_s"])[:2]
    for index, candidate in enumerate(top):
        for page_rows in [50000, 100000]:
            config = dict(candidate["config"], page_rows=page_rows)
            results.append(run(run_dir, f"pages-{index}-{page_rows}", config, 500000))
    finalists = sorted(results, key=lambda r: r["end_to_end_s"])[:2]
    repeated = []
    for index, candidate in enumerate(finalists):
        trials = []
        for repeat in range(3):
            trials.append(run(run_dir, f"confirm-{index}-{repeat}", candidate["config"], 2000000))
        repeated.append(dict(config=candidate["config"],
                             median_s=statistics.median(r["end_to_end_s"] for r in trials),
                             trials=trials))
    chosen = min(repeated, key=lambda r: r["median_s"])
    payload = dict(scan=results, confirmations=repeated, chosen=chosen)
    (run_dir / "tuning.json").write_text(json.dumps(payload, indent=2))
    (OUT / "selected-config.json").write_text(json.dumps(chosen["config"], indent=2))
    print("SELECTED " + json.dumps(chosen), flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["build", "smoke", "tune", "full"])
    parser.add_argument("--run-dir")
    args = parser.parse_args()
    if args.phase == "build":
        build()
    else:
        run_dir = Path(args.run_dir) if args.run_dir else OUT / (args.phase + "-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
        run_dir.mkdir(parents=True, exist_ok=True)
        print("RUN_DIR " + str(run_dir), flush=True)
        if args.phase == "smoke":
            for codec in ["plain_none", "plain_lz4", "default_lz4"]:
                run(run_dir, "smoke-" + codec, dict(codec=codec, threads=15, page_rows=10000, chunk_mib=4), 20000, verify_all=True)
            run(run_dir, "minimum-rows", dict(codec="plain_none", threads=0, page_rows=10000, chunk_mib=4), 10000)
        elif args.phase == "tune":
            tune(run_dir)
        else:
            config = json.loads((OUT / "selected-config.json").read_text())
            run(run_dir, "table-10-devices-2-billion-rows", config, 200000000, retain=True)
