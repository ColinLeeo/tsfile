<!--


    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Python binding sanitizer tests

The opt-in Linux GCC/Clang profile instruments the C++ shared library and all five
Cython extensions, including the C++ random-access adapter and the dataset
index/merge extensions. It uses a shared ASan runtime in the Python process;
the existing standalone C++ `ENABLE_ASAN` profile is separate.

## Run

Use a clean checkout and a dedicated virtual environment. Install the normal
build dependencies from `python/requirements.txt`, plus pytest, then run from
the repository root:

```bash
python3 -m venv build/sanitizer-venv
build/sanitizer-venv/bin/python -m pip install -r python/requirements.txt pytest
PYTHON="$PWD/build/sanitizer-venv/bin/python" bash python/tools/run_sanitizers.sh
```

`TSFILE_SANITIZER_BUILD` selects an absolute build/output directory (default:
`build/python-sanitizers`). `CC=clang` and `CXX=clang++` are defaults; install Clang,
LLVM (including llvm-symbolizer), CMake, binutils and uuid development headers.
Use matching compiler/runtime versions (or set `CC=gcc CXX=g++`).
Clang 18 is recommended and pinned in CI; use
`CC=clang-18 CXX=clang++-18` when installed under versioned names.
`CMAKE_BUILD_PARALLEL_LEVEL` defaults to 4. Test selectors are passed
to pytest, for example `bash python/tools/run_sanitizers.sh -k ownership`.
No sanitizer runtime is preloaded into compilers or dependency installers.
The runner forces verified BUNDLED dependencies: an existing system static
library can itself contain sanitizer instrumentation and contaminate an
otherwise ordinary build. Dependency versions/checksums remain owned by CMake.

For combined ASan/UBSan, set `TSFILE_PY_SANITIZERS=address,undefined`.
LSan is provided by ASan and remains enabled for the tests in either mode.
The initial profile intentionally supports Linux GCC and Clang only. Normal macOS and
Windows builds remain unchanged; they do not silently run uninstrumented tests.

## Separate build steps

Configure C++ with `TSFILE_SANITIZERS=address`, `TSFILE_BUILD_SHARED=ON`,
`TSFILE_DEPENDENCY_SOURCE=BUNDLED` and `ENABLE_ASAN=OFF`. Set `TSFILE_CPP_BUILD` to that output directory and
`TSFILE_PY_SANITIZERS=address` when building Python. The CMake-generated
`sanitizers.json` is checked by setup.py before copying native libraries.
A mismatch is a build error. Both compiler and linker flags must be applied;
merely preloading libasan into an ordinary wheel is not full instrumentation.

The runner force-rebuilds extensions into a separate Python package tree and
runs outside the source package, preventing stale in-place extensions from
shadowing the instrumented build. Do not publish these artifacts as release
wheels. Use separate output directories for normal and sanitizer builds.

## Verify detection

Before pytest, the runner checks the actual imported package, all extension
symbols, loaded libtsfile, and C++ compile commands. It compiles a tiny temporary
shared library and requires both an intentional native leak and a use-after-free
called from Python to fail in child processes. A disabled or ineffective
sanitizer therefore fails the job before test results are accepted.

`test_sanitizer_ownership.py` uses synthetic Tree/Table files and independent
interpreters to exercise string/binary return ownership, nulls, early exit,
exception cleanup, dataframe reads, and Arrow buffer lifetime after result
closure, partial schema conversion, and failed schema registration. Ordinary
builds also run their functional assertions; sanitizer builds add exit-time
leak checks. Both suites must pass; do not disable LSan or suppress TsFile to
obtain a green run.

The `Python-Sanitizers` workflow runs on every push and pull request that
changes C++ code, Python code, or the sanitizer workflow, across all branches.
It can also be run manually. This stays enabled alongside the ordinary Python
matrix, without instrumenting release artifacts or starting a persistent daemon.

The runner uses `PYTHONMALLOC=malloc`, `detect_leaks=1`, frame pointers and debug
information. Processes must exit normally: SIGKILL and application calls to `os._exit()`
skip the Python exit hook. The test runner installs a private `sitecustomize`
hook which collects Python garbage and explicitly invokes
`__lsan_do_recoverable_leak_check()` before interpreter global teardown. This
avoids shutdown-only CPython/NumPy bookkeeping reports from stripped Python
binaries. ASan keeps `detect_leaks=1`; its additional post-`Py_Finalize` scan is
disabled with `leak_check_at_exit=0`. A detected leak prints the native stack and
forces exit status 23. The native positive control verifies this policy. The
runner discovers llvm-symbolizer alongside the compiler or on PATH; override
`ASAN_SYMBOLIZER_PATH` when necessary. Fast frame-pointer unwinding is used for
allocation and fatal-error stacks; older runtimes can stall while slowly
unwinding Python/libffi frames. For deeper interpreter stacks in a focused
reproduction, set `ASAN_OPTIONS=fast_unwind_on_malloc=0`; this is substantially
slower when `PYTHONMALLOC=malloc`. Reports are emitted to stderr and retained
in CI logs.

`tools/lsan.supp` is intentionally empty. Establish an import-only baseline
before adding any allocation-site-specific suppression, explain the allocation,
and retain the positive controls. Never suppress Python allocators, TsFile,
Cython, or entire modules/libraries just to obtain a green run.

Full CPython instrumentation is optional follow-up work; a standard interpreter
with the matching compiler runtime preloaded is sufficient to exercise instrumented
bindings and libtsfile. Python-level reference retention additionally needs
Python allocation/reference tools; it is not all classified as a native leak.

On older GCC and Clang runtimes, LSan can crash while scanning dynamic TLS after native
threads start (`Tracer caught signal 11`). Prefer a newer matching toolchain
or Clang; do not mistake this infrastructure failure for a successful leak
check. The default runner keeps TLS scanning enabled. `use_tls=0` is only a
diagnostic workaround and can create false leak reports for TLS-owned objects.
