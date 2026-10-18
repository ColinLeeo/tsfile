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
#

"""Fail closed if instrumentation, artifact selection, or leak detection is missing."""

import ctypes
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def tool(args):
    env = os.environ.copy()
    env.pop("LD_PRELOAD", None)
    return subprocess.run(
        args, env=env, check=True, text=True, capture_output=True
    ).stdout


def main():
    if sys.flags.optimize:
        raise RuntimeError("Run sanitizer verification without Python -O")
    build = Path(sys.argv[1]).resolve()
    import tsfile

    package = Path(tsfile.__file__).resolve().parent
    expected = (build.parent / "python" / "tsfile").resolve()
    assert package == expected, f"Wrong TsFile package: {package}; expected {expected}"
    marker = json.loads((package / "_sanitizers.json").read_text())
    assert "address" in marker["sanitizers"], marker
    assert os.environ.get("TSFILE_SANITIZER_TESTS") == "1"
    assert "sitecustomize" in sys.modules, "Python exit-time leak hook is missing"
    ctypes.CDLL(None).__asan_init
    for name in (
        "tsfile.tsfile_py_cpp",
        "tsfile.tsfile_reader",
        "tsfile.tsfile_writer",
        "tsfile.dataset._merge",
        "tsfile.dataset._index",
    ):
        module = importlib.import_module(name)
        symbols = tool(["nm", "-D", module.__file__])
        assert "__asan_" in symbols, f"Extension is not instrumented: {module.__file__}"
    libraries = list(package.glob("libtsfile.so*"))
    assert libraries and all("__asan_" in tool(["nm", "-D", str(p)]) for p in libraries)
    maps = Path("/proc/self/maps").read_text()
    assert str(package / "libtsfile.so") in maps, "Packaged C++ library not loaded"
    commands = json.loads((build / "compile_commands.json").read_text())
    own = [c for c in commands if "/cpp/src/" in c["file"]]
    assert own and all("-fsanitize=address" in c["command"] for c in own)

    # A real native shared library loaded through Python exercises runtime
    # interception and process-exit LSan. Never suppress this allocation site.
    with tempfile.TemporaryDirectory(prefix="tsfile-sanitizer-control-") as tmp:
        source = Path(tmp) / "control.cc"
        library = Path(tmp) / "control.so"
        source.write_text("""
#include <stdlib.h>
extern "C" __attribute__((noinline)) void tsfile_lsan_positive_control() {
    void *p = malloc(123);
    asm volatile("" : : "r"(p) : "memory");
}
extern "C" __attribute__((noinline)) int tsfile_asan_positive_control() {
    volatile int *p = (int *)malloc(sizeof(int));
    *p = 42;
    free((void *)p);
    return *p;
}
""")
        tool(
            [
                os.environ.get("CXX", "g++"),
                "-shared",
                "-fPIC",
                "-O1",
                "-g",
                "-fsanitize=address",
                *(
                    ["-shared-libasan"]
                    if "clang"
                    in tool([os.environ.get("CXX", "g++"), "--version"]).lower()
                    else []
                ),
                "-fno-omit-frame-pointer",
                str(source),
                "-o",
                str(library),
            ]
        )
        for function, diagnostic in (
            ("tsfile_lsan_positive_control", "detected memory leaks"),
            ("tsfile_asan_positive_control", "heap-use-after-free"),
        ):
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    f"import ctypes; ctypes.CDLL({str(library)!r}).{function}()",
                ],
                text=True,
                capture_output=True,
                timeout=60,
            )
            assert (
                result.returncode != 0
                and diagnostic in result.stderr
                and function in result.stderr
            ), f"Sanitizer control did not fail: {function}\n{result.stdout}\n{result.stderr}"
            print(f"PASS: {function} failed with {diagnostic}", flush=True)
    print(
        "PASS: all Python extensions and C++ objects are instrumented and selected",
        flush=True,
    )


if __name__ == "__main__":
    main()
