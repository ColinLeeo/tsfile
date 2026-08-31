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

import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "set_python_version.py"
SPEC = importlib.util.spec_from_file_location("set_python_version", SCRIPT_PATH)
set_python_version = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(set_python_version)


def test_read_maven_version_uses_project_version(tmp_path):
    pom = tmp_path / "pom.xml"
    pom.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <parent>
    <groupId>org.apache</groupId>
    <artifactId>apache</artifactId>
    <version>33</version>
  </parent>
  <version>2.4.1-SNAPSHOT</version>
</project>
""",
        encoding="utf-8",
    )

    assert set_python_version.read_maven_version(pom) == "2.4.1-SNAPSHOT"


@pytest.mark.parametrize(
    ("maven_version", "dev_number", "expected"),
    [
        ("2.4.1-SNAPSHOT", "0", "2.4.1.dev0"),
        ("2.4.1-SNAPSHOT", "2", "2.4.1.dev2"),
        ("2.4.1-260806-SNAPSHOT", "12", "2.4.1.dev12"),
        ("2.4.1", "1", "2.4.1.dev1"),
    ],
)
def test_resolve_dev_version(maven_version, dev_number, expected):
    assert set_python_version.resolve_dev_version(maven_version, dev_number) == expected


@pytest.mark.parametrize("dev_number", ["", "-1", "01", "1.0", "dev1"])
def test_resolve_dev_version_rejects_invalid_dev_number(dev_number):
    with pytest.raises(ValueError, match="development release number"):
        set_python_version.resolve_dev_version("2.4.1-SNAPSHOT", dev_number)


@pytest.mark.parametrize(
    "maven_version", ["2.4-SNAPSHOT", "2.4.1-RC1", "2.4.1-feature-SNAPSHOT"]
)
def test_resolve_dev_version_rejects_unsupported_maven_version(maven_version):
    with pytest.raises(ValueError, match="Maven version must be"):
        set_python_version.resolve_dev_version(maven_version, "1")


def test_synchronize_python_version_only_updates_python_metadata(tmp_path):
    python_dir = tmp_path / "python"
    cpp_dir = tmp_path / "cpp"
    python_dir.mkdir()
    cpp_dir.mkdir()
    pyproject = python_dir / "pyproject.toml"
    setup = python_dir / "setup.py"
    cmake = cpp_dir / "CMakeLists.txt"
    pyproject.write_text('[project]\nversion = "2.4.1.dev"\n', encoding="utf-8")
    setup.write_text('version = "2.4.1.dev"\n', encoding="utf-8")
    cmake.write_text("set(TsFile_CPP_VERSION 2.4.1.dev)\n", encoding="utf-8")

    set_python_version.synchronize_python_version(tmp_path, "2.4.1.dev2")

    assert 'version = "2.4.1.dev2"' in pyproject.read_text(encoding="utf-8")
    assert 'version = "2.4.1.dev2"' in setup.read_text(encoding="utf-8")
    assert cmake.read_text(encoding="utf-8") == ("set(TsFile_CPP_VERSION 2.4.1.dev)\n")
