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

"""Binding ownership tests also serve as exit-time LSan regression cases."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
import tsfile


@pytest.mark.parametrize(
    "case", ["tree", "table", "early", "exception", "arrow", "schema_errors"]
)
def test_binding_ownership_subprocess(case, tmp_path):
    script = Path(__file__).resolve().parents[1] / "tools" / "sanitizer_smoke.py"
    env = os.environ.copy()
    package_root = str(Path(tsfile.__file__).resolve().parent.parent)
    env["PYTHONPATH"] = os.pathsep.join(
        [package_root, *filter(None, [env.get("PYTHONPATH")])]
    )
    result = subprocess.run(
        [sys.executable, str(script), case, str(tmp_path)],
        text=True,
        capture_output=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
