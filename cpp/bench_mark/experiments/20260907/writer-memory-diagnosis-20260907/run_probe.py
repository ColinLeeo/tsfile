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
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import cache_control

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent / 'thread-scaling-integer-cold-20260907'


def main():
    args = argparse.ArgumentParser()
    args.add_argument('--rows-per-device', type=int, required=True)
    args.add_argument('--threads', type=int, nargs='+', required=True)
    args.add_argument('--label', required=True)
    o = args.parse_args()
    manifest = json.loads((ORIGINAL / 'manifest.json').read_text())
    library = Path(manifest['reader_manifest']['runtime_dir']) / 'libtsfile.dylib'
    assert hashlib.sha256(library.read_bytes()).hexdigest() == manifest['reader_manifest']['library_sha256']['libtsfile.dylib']
    environment = {k: v for k, v in os.environ.items() if not k.startswith('DYLD_') and k not in ('LD_PRELOAD', 'LD_LIBRARY_PATH')}
    for index, thread in enumerate(o.threads):
        label = f'{o.label}-{index+1}-threads-{thread:02d}'
        output = HERE / label
        output.mkdir()
        assert shutil.disk_usage(HERE).free > 85 * 1024**3
        cache_control.clear(output / 'cache-before.json')
        (output / 'system-before.json').write_text(json.dumps({
            'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'vm_stat': subprocess.check_output(['/usr/bin/vm_stat'], text=True),
            'swap': subprocess.check_output(['/usr/sbin/sysctl', 'vm.swapusage'], text=True),
        }, indent=2))
        file = output / 'data.tsfile'
        command = [str(HERE / 'table_write_memory'), '--output', str(file),
                   '--rows-per-device', str(o.rows_per_device), '--codec', 'default_lz4',
                   '--threads', str(thread), '--page-rows', '50000', '--chunk-mib', '512']
        (output / 'command.json').write_text(json.dumps(command, indent=2))
        print('RUNNING', label, flush=True)
        with (output / 'run.log').open('w') as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=environment)
            (output / 'pid').write_text(str(process.pid))
            status = process.wait()
        assert status == 0, (label, status)
        events = []
        for line in (output / 'run.log').read_text().splitlines():
            if line.startswith('{'):
                events.append(json.loads(line))
        result = next(e for e in events if e['event'] == 'result')
        assert result['verification'] == 'passed'
        assert result['rows'] == o.rows_per_device * 10
        samples = [e for e in events if e['event'] == 'memory']
        assert len(samples) > 5
        write_samples = [e for e in samples if e['phase'] in ('setup', 'write', 'before_final_flush', 'after_final_flush', 'after_close')]
        report = {'label': label, 'threads': thread, 'rows_per_device': o.rows_per_device,
                  'result': result, 'samples': len(samples),
                  'peak_sampled_rss': max(write_samples, key=lambda s: s['rss_bytes']),
                  'max_heap_in_use': max(write_samples, key=lambda s: s['heap_in_use_bytes']),
                  'max_writer_chunks': max(write_samples, key=lambda s: s['writer_chunk_estimate_bytes']),
                  'close_samples': [e for e in samples if e['phase'] in ('after_final_flush', 'after_close', 'after_allocator_relief')],
                  'allocator_relief': next(e for e in events if e['event'] == 'allocator_relief'),
                  'file_bytes': file.stat().st_size, 'generated_file_deleted': False,
                  'timing_use': 'diagnostic only: includes memory sampling overhead; do not replace formal throughput results'}
        assert file.resolve().parent == output.resolve()
        file.unlink()
        report['generated_file_deleted'] = True
        (output / 'summary.json').write_text(json.dumps(report, indent=2)+'\n')
        print('COMPLETE', json.dumps({k:report[k] for k in ['label','samples','peak_sampled_rss','allocator_relief']}), flush=True)


if __name__ == '__main__':
    main()
