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
import subprocess
import sys

HERE = Path(__file__).resolve().parent

def build(build_dir):
    build_dir = Path(build_dir).resolve()
    flags = {line.split(" = ")[0]: shlex.split(line.split(" = ")[1])
             for line in (build_dir / "src/CMakeFiles/tsfile.dir/flags.make").read_text().splitlines()
             if " = " in line}
    cache = (build_dir / "CMakeCache.txt").read_text().splitlines()
    compiler = next(line.split("=", 1)[1] for line in cache
                    if line.startswith("CMAKE_CXX_COMPILER:") and "=" in line)
    command = [compiler, *flags["CXX_FLAGS"],
               *[x for x in flags["CXX_DEFINES"] if x not in ["-DTSFILE_BUILDING", "-Dtsfile_EXPORTS"]],
               *flags["CXX_INCLUDES"], str(HERE / "table_write_phases.cc"),
               "-L" + str(build_dir / "lib"), "-ltsfile", "-Wl,-rpath," + str(build_dir / "lib"),
               "-pthread", "-o", str(HERE / "table_write_phases")]
    (HERE / "compile-command.json").write_text(json.dumps(command, indent=2))
    with (HERE / "compile.log").open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
    print("BUILD COMPLETE", flush=True)

def run(label, threads, rows, verify_all=False):
    run_dir = HERE / label
    run_dir.mkdir()
    command = [str(HERE / "table_write_phases"), "--output", str(run_dir / "data.tsfile"),
               "--rows-per-device", str(rows), "--codec", "default_lz4", "--threads", str(threads),
               "--page-rows", "50000", "--chunk-mib", "512"]
    if verify_all:
        command.append("--verify-all")
    (run_dir / "command.json").write_text(json.dumps(command, indent=2))
    receipt = {"label": label, "threads": threads, "rows_per_device": rows,
               "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    (run_dir / "receipt.json").write_text(json.dumps(receipt, indent=2))
    result = None
    with (run_dir / "run.log").open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line.rstrip(), flush=True)
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") == "result":
                result = event
        status = process.wait()
    receipt["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    receipt["exit_code"] = status
    (run_dir / "receipt.json").write_text(json.dumps(receipt, indent=2))
    if status != 0 or result is None:
        raise RuntimeError("Benchmark failed; see " + str(run_dir / "run.log"))
    assert result["rows"] == rows * 10 and result["numeric_points"] == rows * 50
    assert result["tablets"] == rows // 10000 and result["verification"] == "passed"
    assert result["parallel_write"] == (threads != 0)
    assert result["ingestion_s"] > 0 and result["prepare_s"] > 0
    assert abs(result["prepare_s"] + result["ingestion_s"] + result["other_s"] - result["end_to_end_s"]) < 0.001
    assert result["peak_rss_bytes"] > 10000000
    result["label"] = label
    result["config"] = {"codec": "default_lz4", "threads": threads, "page_rows": 50000, "chunk_mib": 512}
    (run_dir / "result.json").write_text(json.dumps(result, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["build", "run"])
    parser.add_argument("--build-dir")
    parser.add_argument("--label")
    parser.add_argument("--threads", type=int, default=0)
    parser.add_argument("--rows-per-device", type=int, default=200000000)
    parser.add_argument("--verify-all", action="store_true")
    args = parser.parse_args()
    if args.phase == "build":
        build(args.build_dir)
    else:
        run(args.label, args.threads, args.rows_per_device, args.verify_all)
