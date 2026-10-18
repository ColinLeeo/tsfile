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

"""Check native leaks at Python exit, before interpreter global teardown.

Only placed on PYTHONPATH by the sanitizer runner. Normal packages do not ship
this module. Checking before Py_Finalize avoids reporting interpreter/import
bookkeeping as leaks when a distribution's Python binary lacks debug symbols.
"""

import atexit
import ctypes
import gc
import os
from pathlib import Path
import sys

if os.environ.get("TSFILE_SANITIZER_TESTS") == "1":
    try:
        root = Path(os.environ["TSFILE_SANITIZER_PACKAGE_ROOT"]).resolve()
        sys.path.insert(0, str(root))
        # Pin the selected package before scripts with a source-directory cwd
        # can shadow it. Subsequent submodule imports reuse its __path__.
        import tsfile

        if Path(tsfile.__file__).resolve().parent != root / "tsfile":
            raise RuntimeError("The sanitizer package was shadowed")
        check = ctypes.PyDLL(None).__lsan_do_recoverable_leak_check
        check.argtypes = []
        check.restype = ctypes.c_int
    except Exception as exc:
        sys.stderr.write(f"Cannot initialize TsFile leak checking: {exc}\n")
        sys.stderr.flush()
        os._exit(2)

    def check_native_leaks():
        gc.collect()
        if check():
            sys.stdout.flush()
            sys.stderr.flush()
            # The leak check has run and printed its report. Prevent later
            # interpreter teardown from overriding the failure status.
            os._exit(23)

    atexit.register(check_native_leaks)
