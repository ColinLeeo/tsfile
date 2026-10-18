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

"""Exercise setup.py's sanitizer gate without compiling or touching source artifacts."""

import json
from pathlib import Path
import runpy
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux sanitizer profile"
)


def configure(tmp_path, monkeypatch, requested, native, legacy=False, compiler="GNU"):
    import Cython.Build
    import setuptools

    package = tmp_path / "python"
    (package / "tsfile").mkdir(parents=True)
    setup = package / "setup.py"
    setup.write_text((Path(__file__).parents[1] / "setup.py").read_text())
    cpp = tmp_path / "cpp"
    (cpp / "include").mkdir(parents=True)
    (cpp / "lib").mkdir()
    (cpp / "lib" / "libtsfile.so").write_bytes(b"not loaded by setup")
    (cpp / "sanitizers.json").write_text(
        json.dumps(
            {"sanitizers": native, "compiler_id": compiler, "legacy_asan": legacy}
        )
    )
    monkeypatch.setenv("TSFILE_CPP_BUILD", str(cpp))
    monkeypatch.setenv("TSFILE_PY_SANITIZERS", requested)
    captured = {}
    monkeypatch.setattr(
        Cython.Build, "cythonize", lambda extensions, **kwargs: extensions
    )
    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: captured.update(kwargs))
    runpy.run_path(str(setup))
    return captured["ext_modules"]


@pytest.mark.parametrize(
    "requested,native",
    [("address", []), ("", ["address"]), ("undefined", ["undefined"])],
)
def test_reject_mismatched_or_unsupported_sanitizers(
    tmp_path, monkeypatch, requested, native
):
    with pytest.raises(RuntimeError):
        configure(tmp_path, monkeypatch, requested, native)


@pytest.mark.parametrize("compiler", ["GNU", "Clang"])
def test_all_extensions_receive_compile_and_link_flags(tmp_path, monkeypatch, compiler):
    extensions = configure(
        tmp_path, monkeypatch, "address", ["address"], compiler=compiler
    )
    assert len(extensions) == 5
    for extension in extensions:
        assert "-fsanitize=address" in extension.extra_compile_args
        assert "-fsanitize=address" in extension.extra_link_args
        assert ("-shared-libasan" in extension.extra_link_args) == (compiler == "Clang")
        assert extension.extra_compile_args.index(
            "-O1"
        ) > extension.extra_compile_args.index("-O3")
    assert (tmp_path / "python" / "tsfile" / "_sanitizers.json").exists()


def test_normal_build_does_not_enable_sanitizers(tmp_path, monkeypatch):
    for extension in configure(tmp_path, monkeypatch, "", []):
        assert not any(
            "sanitize" in flag
            for flag in extension.extra_compile_args + extension.extra_link_args
        )
    assert not (tmp_path / "python" / "tsfile" / "_sanitizers.json").exists()


def test_reject_legacy_static_runtime_profile(tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="standalone ENABLE_ASAN"):
        configure(tmp_path, monkeypatch, "", [], legacy=True)
