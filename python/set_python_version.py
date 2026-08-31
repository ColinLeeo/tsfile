#!/usr/bin/env python3
#
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

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_VERSION_COMPONENT = r"(?:0|[1-9][0-9]*)"
_MAVEN_VERSION = re.compile(
    rf"^({_VERSION_COMPONENT}\.{_VERSION_COMPONENT}\.{_VERSION_COMPONENT})"
    rf"(?:(?:-[0-9]+)?-SNAPSHOT)?$"
)
_DEV_NUMBER = re.compile(r"^(?:0|[1-9][0-9]*)$")
_VERSION_ASSIGNMENT = re.compile(r'(?m)^(version\s*=\s*")[^"\r\n]+(")')


def read_maven_version(pom_path: Path) -> str:
    root = ET.parse(pom_path).getroot()
    namespace = root.tag.partition("}")[0] + "}" if "}" in root.tag else ""
    version = root.find(f"{namespace}version")
    if version is None or not version.text:
        raise ValueError(f"project version is missing from {pom_path}")
    return version.text.strip()


def resolve_dev_version(maven_version: str, dev_number: str) -> str:
    if not _DEV_NUMBER.fullmatch(dev_number):
        raise ValueError(
            "development release number must be 0 or a positive integer "
            "without leading zeros"
        )

    match = _MAVEN_VERSION.fullmatch(maven_version)
    if match is None:
        raise ValueError(
            "Maven version must be X.Y.Z, X.Y.Z-SNAPSHOT, or "
            "X.Y.Z-<date>-SNAPSHOT; got "
            f"{maven_version!r}"
        )
    return f"{match.group(1)}.dev{dev_number}"


def _updated_version_text(path: Path, version: str) -> tuple[str, str]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        original = stream.read()
    updated, replacements = _VERSION_ASSIGNMENT.subn(rf"\g<1>{version}\g<2>", original)
    if replacements != 1:
        raise ValueError(
            f"expected exactly one version assignment in {path}, found {replacements}"
        )
    return original, updated


def synchronize_python_version(project_root: Path, version: str) -> None:
    paths = (
        project_root / "python" / "pyproject.toml",
        project_root / "python" / "setup.py",
    )
    updates = [(path, *_updated_version_text(path, version)) for path in paths]
    for path, original, updated in updates:
        if updated == original:
            continue
        with path.open("w", encoding="utf-8", newline="") as stream:
            stream.write(updated)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set a Python development version derived from the root Maven POM."
    )
    parser.add_argument(
        "--dev-number",
        required=True,
        help="PEP 440 development release number, for example 0, 1, or 2.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        project_root = args.project_root.resolve()
        maven_version = read_maven_version(project_root / "pom.xml")
        python_version = resolve_dev_version(maven_version, args.dev_number)
        synchronize_python_version(project_root, python_version)
    except (OSError, ET.ParseError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(python_version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
