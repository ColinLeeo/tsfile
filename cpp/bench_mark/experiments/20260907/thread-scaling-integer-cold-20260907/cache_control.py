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
import json
import os
from pathlib import Path
import select
import shlex
import stat
import subprocess
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
CONTROL = HERE / "cache-service.json"
PROBE = HERE / "cache-probe.bin"


def setup():
    assert not CONTROL.exists(), "cache service already configured"
    directory = Path(tempfile.mkdtemp(prefix="tsfile-cache-", dir="/private/tmp"))
    for name in ["request", "response"]:
        os.mkfifo(directory / name, 0o600)
    CONTROL.write_text(json.dumps({"directory": str(directory), "max_requests": 32,
        "idle_timeout_s": 600, "command": "/usr/sbin/purge"}, indent=2) + "\n")
    with PROBE.open("xb") as stream:
        block = b"p" * (1024 * 1024)
        for _ in range(16):
            stream.write(block)
        stream.flush()
        os.fsync(stream.fileno())


def launch():
    control = json.loads(CONTROL.read_text())
    directory = Path(control["directory"])
    # The privileged worker accepts only fixed purge/ping/stop operations.
    # Benchmarks and all result-file writes remain in the ordinary user process.
    code = (
        "exec 3<> " + shlex.quote(str(directory / "request")) + "; "
        "exec 4<> " + shlex.quote(str(directory / "response")) + "; "
        "count=0; "
        "while [ \"$count\" -lt 32 ] && IFS=' ' read -r -t 600 operation token <&3; do "
        "case \"$operation\" in "
        "purge) /usr/sbin/purge; rc=$?; count=$((count+1));; "
        "ping) rc=0;; "
        "stop) printf '%s 0\\n' \"$token\" >&4; exit 0;; "
        "*) rc=64;; esac; "
        "printf '%s %s\\n' \"$token\" \"$rc\" >&4; "
        "done"
    )
    command = "/bin/bash -c " + shlex.quote(code)
    script = "do shell script " + json.dumps(command) + " with administrator privileges"
    (HERE / "cache-service-policy.json").write_text(json.dumps({
        "privileged_command": command, "max_purges": 32, "idle_timeout_s": 600,
        "operations": ["purge", "ping", "stop"]}, indent=2) + "\n")
    subprocess.run(["osascript", "-e", script], check=True)


def request(operation):
    assert operation in ["purge", "ping", "stop"]
    control = json.loads(CONTROL.read_text())
    directory = Path(control["directory"])
    descriptors = []
    try:
        for name in ["request", "response"]:
            path = directory / name
            info = path.lstat()
            assert stat.S_ISFIFO(info.st_mode) and info.st_uid == os.getuid()
            descriptors.append(os.open(path, os.O_RDWR | os.O_NONBLOCK))
        token = uuid.uuid4().hex
        start = time.monotonic()
        os.write(descriptors[0], (operation + " " + token + "\n").encode())
        data = b""
        while b"\n" not in data:
            remaining = 120 - (time.monotonic() - start)
            assert remaining > 0, "cache service response timed out"
            ready, _, _ = select.select([descriptors[1]], [], [], remaining)
            assert ready, "cache service unavailable or administrator authorization pending"
            data += os.read(descriptors[1], 4096)
        received, code = data.decode().strip().split()
        assert received == token and int(code) == 0, data
        return {"operation": operation, "request_id": token, "exit_code": int(code),
                "elapsed_s": time.monotonic() - start,
                "utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


def inspect_probe():
    return json.loads(subprocess.check_output(
        [str(HERE / "reader/file_cache"), "inspect", str(PROBE)], text=True))


def clear(receipt):
    # A dedicated file proves the global purge also clears an unrelated file.
    with PROBE.open("rb") as stream:
        while stream.read(1024 * 1024):
            pass
    before = inspect_probe()
    assert before["resident_pages_after"] > 0, "cache positive control failed"
    result = request("purge")
    after = inspect_probe()
    result.update({"scope": "system file cache", "probe_before": before, "probe_after": after})
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps(result, indent=2) + "\n")
    assert after["resident_pages_after"] == 0, "global purge did not evict cache probe"
    print("SYSTEM_CACHE_CLEARED " + str(receipt), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "launch", "ping", "stop", "check"])
    action = parser.parse_args().action
    if action == "setup":
        setup()
    elif action == "launch":
        launch()
    elif action == "check":
        clear(HERE / "cache-global-positive-control.json")
    else:
        print(json.dumps(request(action)))
