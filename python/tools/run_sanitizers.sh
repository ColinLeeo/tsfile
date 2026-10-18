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

# Build an isolated native/Python test tree; never publish this as a wheel.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
out=${TSFILE_SANITIZER_BUILD:-"$root/build/python-sanitizers"}
mkdir -p "$out"
out=$(cd "$out" && pwd)
python=${PYTHON:-python3}
export CC=$(command -v "${CC:-clang}")
export CXX=$(command -v "${CXX:-clang++}")
export TSFILE_PY_SANITIZERS=${TSFILE_PY_SANITIZERS:-address}
cmake -S "$root/cpp" -B "$out/cpp" \
    -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=OFF \
    -DTSFILE_SANITIZERS="$TSFILE_PY_SANITIZERS" \
    -DTSFILE_BUILD_SHARED=ON -DBUILD_TEST=OFF -DBUILD_TOOLS=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DTSFILE_DEPENDENCY_CACHE="${TSFILE_DEPENDENCY_CACHE:-$out/cpp/dependency-cache}" \
    -DTSFILE_DEPENDENCY_OFFLINE="${TSFILE_DEPENDENCY_OFFLINE:-OFF}" \
    -DTSFILE_DEPENDENCY_SOURCE=BUNDLED
cmake --build "$out/cpp" --target tsfile --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-4}"
export TSFILE_CPP_BUILD="$out/cpp"
(
    cd "$root/python"
    "$python" setup.py build_py --build-lib "$out/python" \
        build_ext --build-lib "$out/python" --build-temp "$out/python-temp" --force
)
# Only preload into tests, not cmake/compiler/pip child processes.
if "$CC" --version | grep -qi clang; then
    asan=$("$CC" -print-file-name="libclang_rt.asan-$(uname -m).so")
else
    asan=$("$CC" -print-file-name=libasan.so)
fi
if [[ ! -f "$asan" ]]; then
    echo "Cannot locate the matching ASan runtime: $asan" >&2
    exit 1
fi
if [[ -z "${ASAN_SYMBOLIZER_PATH:-}" ]]; then
    symbolizer=$(command -v llvm-symbolizer || true)
    if [[ -z "$symbolizer" ]]; then
        symbolizer="$(dirname "$(readlink -f "$CC")")/llvm-symbolizer"
    fi
    if [[ -x "$symbolizer" ]]; then
        export ASAN_SYMBOLIZER_PATH="$symbolizer"
    fi
fi
export LD_PRELOAD="$asan${LD_PRELOAD:+:$LD_PRELOAD}"
export TSFILE_SANITIZER_PACKAGE_ROOT="$out/python"
export PYTHONPATH="$root/python/tools/sanitizer_runtime:$out/python"
export PYTHONMALLOC=malloc
export ASAN_OPTIONS="detect_leaks=1:leak_check_at_exit=0:halt_on_error=1:fast_unwind_on_malloc=1:fast_unwind_on_fatal=1${ASAN_OPTIONS:+:$ASAN_OPTIONS}"
export LSAN_OPTIONS="exitcode=23:suppressions=$root/python/tools/lsan.supp${LSAN_OPTIONS:+:$LSAN_OPTIONS}"
export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
export TSFILE_SANITIZER_TESTS=1
# Run outside the source package so an old in-place extension cannot shadow it.
cd "$out"
"$python" "$root/python/tools/check_sanitizers.py" "$out/cpp"
"$python" -m pytest --import-mode=importlib "$root/python/tests" "$@"
