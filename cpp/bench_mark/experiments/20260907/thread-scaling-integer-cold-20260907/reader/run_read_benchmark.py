# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# License); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess

HERE = Path(__file__).resolve().parent


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + "\n")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def check_runtime(manifest):
    for name, expected in manifest["library_sha256"].items():
        require(sha(Path(manifest["runtime_dir"]) / name) == expected,
                "runtime library changed: " + name)
    require(sha(HERE / "table_read_benchmark") == manifest["binary_sha256"],
            "benchmark binary changed")


def build(build_dir):
    directory = Path(build_dir).resolve()
    flags_file = directory / "src/CMakeFiles/tsfile.dir/flags.make"
    flags = {
        line.split(" = ", 1)[0]: shlex.split(line.split(" = ", 1)[1])
        for line in flags_file.read_text().splitlines()
        if " = " in line
    }
    cache = (directory / "CMakeCache.txt").read_text().splitlines()
    compiler = next(line.split("=", 1)[1] for line in cache
                    if line.startswith("CMAKE_CXX_COMPILER:"))
    source = Path(next(line.split("=", 1)[1] for line in cache
                       if line.startswith("CMAKE_HOME_DIRECTORY:")))
    require("-O3" in flags["CXX_FLAGS"] and "-flto" in flags["CXX_FLAGS"], "O3/LTO missing")
    require("-march=native" in flags["CXX_FLAGS"], "native optimization missing")
    for macro in ("ENABLE_THREADS", "ENABLE_SIMD"):
        require("-D" + macro in flags["CXX_DEFINES"], "missing " + macro)
    require("-DENABLE_ASAN" not in flags["CXX_DEFINES"], "ASAN enabled")
    require("-DENABLE_MEM_STAT" not in flags["CXX_DEFINES"], "memory instrumentation enabled")
    libraries = sorted((directory / "lib").glob("libtsfile.*"))
    require(bool(libraries), "no shared library")
    runtime = HERE / ("runtime-" + sha(libraries[0])[:16])
    runtime.mkdir(exist_ok=True)
    for library in libraries:
        target = runtime / library.name
        if not target.exists():
            shutil.copyfile(library, target)
            target.chmod(0o444)
        require(sha(target) == sha(library), "runtime snapshot collision")
    command = [compiler, *flags["CXX_FLAGS"],
               *[x for x in flags["CXX_DEFINES"]
                 if x not in ("-DTSFILE_BUILDING", "-Dtsfile_EXPORTS")],
               *flags["CXX_INCLUDES"], str(HERE / "table_read_benchmark.cc"),
               "-L" + str(runtime), "-ltsfile",
               "-Wl,-rpath," + str(runtime), "-pthread",
               "-o", str(HERE / "table_read_benchmark")]
    save(HERE / "compile-command.json", command)
    (HERE / "library-flags.make").write_text(flags_file.read_text())
    with (HERE / "compile.log").open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    manifest = {
        "platform": platform.platform(), "uname": list(platform.uname()),
        "logical_cpus": os.cpu_count(), "source_cpp": str(source),
        "build_dir": str(directory), "compiler": compiler,
        "runtime_dir": str(runtime),
        "binary_sha256": sha(HERE / "table_read_benchmark"),
        "benchmark_source_sha256": sha(HERE / "table_read_benchmark.cc"),
        "library_sha256": {p.name: sha(p) for p in libraries if p.is_file()},
        "source_sha256": {str(p.relative_to(source)): sha(p)
                          for p in sorted((source / "src").rglob("*"))
                          if p.suffix in (".cc", ".h")},
        "cache_policy": "Input file page cache evicted and mincore verified zero before every run; normal buffered reads during the run.",
    }
    save(HERE / "manifest.json", manifest)
    print("BUILD_COMPLETE", flush=True)


def run(args):
    target = HERE / args.label
    target.mkdir()
    file = Path(args.file).resolve()
    manifest = json.loads((HERE / "manifest.json").read_text())
    check_runtime(manifest)
    save(target / "manifest.json", manifest)
    shutil.copyfile(HERE / "compile-command.json", target / "compile-command.json")
    shutil.copyfile(HERE / "library-flags.make", target / "library-flags.make")
    before = file.stat()
    command = [str(HERE / "table_read_benchmark"), "--file", str(file),
               "--threads", str(args.threads), "--batch-size", str(args.batch_size),
               "--rows-per-device", str(args.rows_per_device)]
    if args.verify_all:
        command.append("--verify-all")
    save(target / "command.json", command)
    receipt = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "file": str(file), "input_bytes": before.st_size,
               "input_mtime_ns": before.st_mtime_ns,
               "binary_sha256": sha(HERE / "table_read_benchmark"),
               "library_sha256": manifest["library_sha256"],
               "runtime_dir": manifest["runtime_dir"],
               "threads": args.threads, "parallel_read": args.threads > 1,
               "query_count": 1, "table": "benchmark",
               "columns": ["device", "i32", "i64", "i32_2", "i64_2", "i64_3"],
               "time_min": -(2**63), "time_max": 2**63 - 1,
               "tag_filter": None, "field_filter": None,
               "row_limit": None, "row_offset": None,
               "cache_policy": "Input file page cache evicted and mincore verified zero before every run; normal buffered reads during the run."}
    save(target / "receipt.json", receipt)
    # Evict only this input file, outside the C++ benchmark timer.
    cache_command = [str(HERE / "file_cache"), "evict", str(file)]
    cache_result = subprocess.run(cache_command, text=True, capture_output=True)
    (target / "cache-command.json").write_text(json.dumps(cache_command) + "\n")
    (target / "cache.log").write_text(cache_result.stdout + cache_result.stderr)
    require(cache_result.returncode == 0, "file cache eviction failed")
    cache = json.loads(cache_result.stdout)
    save(target / "cache-before.json", cache)
    require(cache["cold_verified"] and cache["resident_pages_after"] == 0,
            "input file has cached pages")
    require(cache["file_bytes"] == before.st_size, "wrong cache target size")
    receipt["cache_before"] = cache
    receipt["cache_tool_sha256"] = sha(HERE / "file_cache")
    save(target / "receipt.json", receipt)
    print("COLD_VERIFIED " + args.label + " pages=0/" + str(cache["total_pages"]), flush=True)
    result = None
    # Prevent inherited loader overrides from selecting a different library.
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("DYLD_")
                   and key not in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT")}
    with (target / "run.log").open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, env=environment)
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
    cache_after = json.loads(subprocess.check_output(
        [str(HERE / "file_cache"), "inspect", str(file)], text=True))
    save(target / "cache-after.json", cache_after)
    receipt["exit_code"] = status
    receipt["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    after = file.stat()
    check_runtime(manifest)
    receipt["runtime_unchanged"] = True
    receipt["input_unchanged"] = (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    save(target / "receipt.json", receipt)
    if status != 0 or result is None:
        raise RuntimeError("Benchmark failed: " + str(target / "run.log"))
    require(receipt["input_unchanged"], "input file changed")
    require(result["rows"] == args.rows_per_device * 10, "wrong row count")
    require(result["numeric_points"] == args.rows_per_device * 50, "wrong point count")
    require(result["device_rows"] == [args.rows_per_device] * 10, "wrong device counts")
    require(result["verification"] == "passed", "verification failed")
    require(result["returned_column_buffer_bytes"] == result["rows"] * 53, "wrong buffer bytes")
    require(result["threads"] == args.threads and result["batch_size"] == args.batch_size,
            "wrong reader configuration")
    require(result["parallel_read"] == (args.threads > 1), "wrong parallel read mode")
    require(abs(result["read_total_s"] + result["validation_s"] + result["other_s"]
                - result["end_to_end_s"]) < 0.001, "phase timing mismatch")
    if args.verify_all:
        require(result["sampled_rows"] == result["rows"], "incomplete full verification")
    result["label"] = args.label
    save(target / "result.json", result)
    print("RUN_VERIFIED " + args.label, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("build", "run", "matrix"))
    parser.add_argument("--build-dir")
    parser.add_argument("--label")
    parser.add_argument("--file")
    parser.add_argument("--threads", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=100000)
    parser.add_argument("--rows-per-device", type=int, default=200000000)
    parser.add_argument("--verify-all", action="store_true")
    args = parser.parse_args()
    if args.phase == "build":
        build(args.build_dir)
    elif args.phase == "matrix":
        require(args.threads > 1, "matrix requires the multithread pool size")
        require(bool(args.label), "matrix requires a host label")
        # ABBA order; input file page cache is evicted before every run.
        for mode, threads, repeat in (("single", 1, 1), ("multi", args.threads, 1),
                                      ("multi", args.threads, 2), ("single", 1, 2)):
            case = argparse.Namespace(**vars(args))
            case.label = "{}-{}-{}".format(args.label, mode, repeat)
            case.threads = threads
            run(case)
    else:
        run(args)
