# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
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
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
TARGET = HERE.parent
BUILD = TARGET / "writer-throughput-xcode21"
PROTECTED = TARGET / "large-table-benchmark/integer-20260907/local-single/data.tsfile"
JAVA_CP = str(HERE / "java") + ":" + (
    TARGET / "large-table-benchmark/java-reference-classpath.txt"
).read_text().strip()
EXPECTED_BYTES = 41956356651


def save(path, value):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


writer = module("writer_benchmark", HERE / "writer/matrix_runner.py")
reader = module("reader_benchmark", HERE / "reader/run_read_benchmark.py")
cache_control = module("cache_control", HERE / "cache_control.py")


def state(phase, threads=None, repeat=None):
    value = {"phase": phase, "threads": threads, "repeat": repeat, "utc": now()}
    save(HERE / "status.json", value)
    print("SWEEP_STATUS " + json.dumps(value), flush=True)


def build():
    reader.build(BUILD)
    command = json.loads((HERE / "reader/compile-command.json").read_text())
    write_command = [
        part.replace(str(HERE / "reader/table_read_benchmark.cc"),
                     str(HERE / "writer/table_write_phases.cc"))
        .replace(str(HERE / "reader/table_read_benchmark"),
                 str(HERE / "writer/table_write_phases"))
        for part in command
    ]
    save(HERE / "writer/compile-command.json", write_command)
    with (HERE / "writer/compile.log").open("w") as log:
        subprocess.run(write_command, check=True, stdout=log, stderr=subprocess.STDOUT)
    sdk = command[command.index("-isysroot") + 1]
    cache_command = [command[0], "-std=c++11", "-O3", "-DNDEBUG", "-isysroot", sdk,
                     str(HERE / "reader/file_cache.cc"), "-o", str(HERE / "reader/file_cache")]
    save(HERE / "reader/cache-compile-command.json", cache_command)
    subprocess.run(cache_command, check=True)
    subprocess.run(["javac", "-cp", JAVA_CP, str(HERE / "java/ReferenceVerify.java")], check=True)
    input_stat = PROTECTED.stat()
    assert input_stat.st_size == EXPECTED_BYTES
    manifest = {
        "created_utc": now(), "host": "local", "cpu": "Apple M5 Pro",
        "logical_cpus": 15, "max_threads": 16, "memory_bytes": 25769803776,
        "thread_counts": [1, 2, 4, 8, 16], "write_repeats": 1, "read_repeats": 2,
        "rows_per_device": 200000000, "devices": 10,
        "rows": 2000000000, "numeric_points": 10000000000,
        "tablet_rows": 100000, "rows_per_device_per_tablet": 10000,
        "read_batch_rows": 100000,
        "schema": {"time": "INT64", "device": "STRING TAG", "i32": "INT32",
                   "i64": "INT64", "i32_2": "INT32", "i64_2": "INT64", "i64_3": "INT64"},
        "encoding": "TS2DIFF for all numeric fields and time; LZ4 compression",
        "writer_configuration": {"page_rows": 50000, "page_memory_mib": 16,
                                 "chunk_memory_mib": 512, "sync_on_close": True},
        "execution_order": "1,2,4,8,16: one write, Java sample verification, two cold full reads, remove generated file",
        "read_query": "One query, all columns, INT64_MIN..INT64_MAX, no filters/limit/offset",
        "single_thread": "Parallel encoding/decoding disabled; writer CLI threads=0 maps to one thread",
        "multithread": "One writer/reader and a shared worker pool of N; no parallel files or clients",
        "write_cache": "Before every write: macOS system file-cache purge, verified by warm-then-zero-residency probe, excluded from timing",
        "read_cache": "Before every read: same system file-cache purge, then target-file eviction and mincore zero-residency check, excluded from timing",
        "write_timing": "End-to-end includes preparation, writer setup, writes, flush, fsync, close, and F_FULLFSYNC. Post-write validation excluded.",
        "read_timing": "End-to-end includes initialization, open, full query/TSBlock scan, lightweight per-block validation, cleanup and close.",
        "data_file_policy": "Preserve the previously retained file; remove only each newly generated file after both full reads and all validation pass.",
        "protected_file": str(PROTECTED), "protected_size": input_stat.st_size,
        "protected_mtime_ns": input_stat.st_mtime_ns,
        "reader_manifest": json.loads((HERE / "reader/manifest.json").read_text()),
        "file_sha256": {},
    }
    paths = [HERE / "writer/table_write_phases", HERE / "reader/table_read_benchmark",
             HERE / "reader/file_cache", HERE / "writer/table_write_phases.cc",
             HERE / "reader/table_read_benchmark.cc", HERE / "reader/file_cache.cc",
             HERE / "java/ReferenceVerify.java", HERE / "java/ReferenceVerify.class", HERE / "cache_control.py"]
    manifest["file_sha256"] = {str(p.relative_to(HERE)): sha(p) for p in paths}
    save(HERE / "manifest.json", manifest)
    print("SWEEP_BUILD_PASSED", flush=True)


def check_runtime():
    manifest = json.loads((HERE / "manifest.json").read_text())
    for name, expected in manifest["file_sha256"].items():
        assert sha(HERE / name) == expected, name
    reader.check_runtime(manifest["reader_manifest"])
    current = PROTECTED.stat()
    assert (current.st_size, current.st_mtime_ns) == (
        manifest["protected_size"], manifest["protected_mtime_ns"])
    return manifest


def run_read(label, file, threads, rows, full=False):
    args = argparse.Namespace(label=label, file=str(file), threads=threads,
                              rows_per_device=rows, batch_size=100000, verify_all=full)
    cache_control.clear(HERE / "cache-events" / (label + "-read.json"))
    reader.run(args)
    result = json.loads((HERE / "reader" / label / "result.json").read_text())
    assert result["verification"] == "passed"
    return result


def smoke():
    check_runtime()
    checksums = []
    for threads in [1, 16]:
        label = "smoke-" + str(threads)
        state("smoke", threads)
        cache_control.clear(HERE / "cache-events" / (label + "-write.json"))
        writer.run(label, 0 if threads == 1 else threads, 20000, True)
        file = HERE / "writer" / label / "data.tsfile"
        with file.open("rb") as stream:
            stream.read(1024 * 1024)
        warm = json.loads(subprocess.check_output(
            [str(HERE / "reader/file_cache"), "inspect", str(file)], text=True))
        assert warm["resident_pages_after"] > 0
        save(HERE / "writer" / label / "cache-warm-check.json", warm)
        result = run_read(label, file, threads, 20000, True)
        assert result["sampled_rows"] == 200000
        checksums.append(result["sample_checksum"])
        file.unlink()
    assert len(set(checksums)) == 1
    check_runtime()
    save(HERE / "smoke-verification.json", {"verification": "passed", "threads": [1, 16],
         "full_rows_per_case": 200000, "checksums": checksums, "cache_positive_control": "passed"})
    state("smoke_passed")


def sweep():
    manifest = check_runtime()
    assert json.loads((HERE / "smoke-verification.json").read_text())["verification"] == "passed"
    # Remove inherited loader overrides so all runs use the recorded snapshot.
    for key in list(os.environ):
        if key.startswith("DYLD_") or key in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT"):
            del os.environ[key]
    completed = []
    for threads in manifest["thread_counts"]:
        check_runtime()
        label = "threads-{:02d}".format(threads)
        summary_path = HERE / (label + ".json")
        if summary_path.exists():
            summary = json.loads(summary_path.read_text())
            assert summary["verification"] == "passed" and summary["generated_file_deleted"]
            completed.append(summary)
            continue
        assert shutil.disk_usage(HERE).free > 85 * 1024**3, "less than 85 GiB free before a full write"
        state("write", threads)
        cache_control.clear(HERE / "cache-events" / (label + "-write.json"))
        writer.run(label, 0 if threads == 1 else threads, 200000000)
        directory = HERE / "writer" / label
        file = directory / "data.tsfile"
        write_result = json.loads((directory / "result.json").read_text())
        assert file.stat().st_size == EXPECTED_BYTES
        state("java_verification", threads)
        with (directory / "java-verify.log").open("w") as log:
            subprocess.run(["java", "-cp", JAVA_CP, "ReferenceVerify", str(file)],
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        assert "REFERENCE_VERIFICATION_PASSED" in (directory / "java-verify.log").read_text()
        reads = []
        for repeat in [1, 2]:
            state("cold_read", threads, repeat)
            result = run_read(label + "-r" + str(repeat), file, threads, 200000000)
            assert result["blocks"] == 20000 and result["sampled_rows"] == 60000
            reads.append(result)
        assert reads[0]["sample_checksum"] == reads[1]["sample_checksum"]
        if completed:
            assert reads[0]["sample_checksum"] == completed[0]["reads"][0]["sample_checksum"]
        check_runtime()
        summary = {"threads": threads, "writer": write_result, "reads": reads,
                   "verification": "passed", "finished_utc": now(),
                   "generated_file": str(file), "file_bytes": file.stat().st_size,
                   "generated_file_deleted": False}
        assert file.resolve().parent == directory.resolve() and file.resolve() != PROTECTED.resolve()
        assert file.stat().st_size == EXPECTED_BYTES
        file.unlink()
        summary["generated_file_deleted"] = True
        save(summary_path, summary)
        completed.append(summary)
        save(HERE / "results.json", completed)
        print("THREAD_COMPLETE " + json.dumps({"threads": threads,
              "write_end_to_end_s": write_result["end_to_end_s"],
              "read_end_to_end_s": [r["end_to_end_s"] for r in reads]}), flush=True)
    assert len(completed) == 5
    check_runtime()
    save(HERE / "results.json", completed)
    cache_control.request("stop")
    state("complete")
    print("INTEGER_THREAD_SWEEP_PASSED", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["build", "smoke", "sweep"])
    args = parser.parse_args()
    {"build": build, "smoke": smoke, "sweep": sweep}[args.phase]()
