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

# 实验源码清单

源码来自本地 `cpp/target/` 下的同名相对路径。仅清理部分 Python 许可头的行末空白，保留实验行为和数据生成公式。

| 相对路径 | 原文件 SHA-256 | 本分支文件 SHA-256 |
| --- | --- | --- |
| `thread-scaling-integer-cold-20260907/writer/table_write_phases.cc` | `7a7c37095b568666a0ec85eb0d44c3731e3e69c2e39d62c2ae5dec652895035d` | `7a7c37095b568666a0ec85eb0d44c3731e3e69c2e39d62c2ae5dec652895035d` |
| `thread-scaling-integer-cold-20260907/writer/matrix_runner.py` | `fce02505f6c68fa13f961d11b14d7d936c264429e86bc1594a6d6f64d3daf7a0` | `8b9887c2304ce6f4770e358a88b8a4d6300f21e6794f4cb3bf56cb98fd472089` |
| `thread-scaling-integer-cold-20260907/reader/table_read_benchmark.cc` | `12b3f0d3a892787e2b4f2af6bca37e2ebf764a37261d086600a1f79625816edc` | `12b3f0d3a892787e2b4f2af6bca37e2ebf764a37261d086600a1f79625816edc` |
| `thread-scaling-integer-cold-20260907/reader/run_read_benchmark.py` | `715452320048ac302e82b01a4bead13e21e1a5c4f04b11624c1a5871f46dbce1` | `715452320048ac302e82b01a4bead13e21e1a5c4f04b11624c1a5871f46dbce1` |
| `thread-scaling-integer-cold-20260907/reader/file_cache.cc` | `c34ef1b758f2c7bba8d1808ebe781ae1d7ff65529303dd8fe17d7356c8576cbf` | `c34ef1b758f2c7bba8d1808ebe781ae1d7ff65529303dd8fe17d7356c8576cbf` |
| `thread-scaling-integer-cold-20260907/java/ReferenceVerify.java` | `6bcd7f42bdaa4fc7bb012b63cf37a499e9c9233d350cf17f4cb42a3d98bf5052` | `6bcd7f42bdaa4fc7bb012b63cf37a499e9c9233d350cf17f4cb42a3d98bf5052` |
| `thread-scaling-integer-cold-20260907/run_sweep.py` | `f150293bb932001e8cc50d66b669d3687f8ca8b9dea0829c1d183f1dc72bd87f` | `f150293bb932001e8cc50d66b669d3687f8ca8b9dea0829c1d183f1dc72bd87f` |
| `thread-scaling-integer-cold-20260907/cache_control.py` | `1a9546ed32db83c6f9c61aa8cd8eb2dc0d84063ea5dce12789148410e95c2357` | `1a9546ed32db83c6f9c61aa8cd8eb2dc0d84063ea5dce12789148410e95c2357` |
| `thread-scaling-integer-cold-20260907/make_report.py` | `baefcf3287d659e5d77837bfcd5af3f6d0886148e1c7af7fff065d65fd9e8e4c` | `baefcf3287d659e5d77837bfcd5af3f6d0886148e1c7af7fff065d65fd9e8e4c` |
| `thread-scaling-integer-cold-20260907/make_waterline_report.py` | `75ec7786d01dc9903b005811051e9eb59c920edbfba3ae0d32c31b973391eae7` | `75ec7786d01dc9903b005811051e9eb59c920edbfba3ae0d32c31b973391eae7` |
| `writer-memory-diagnosis-20260907/table_write_memory.cc` | `9d2ae591f4f2eb20fbf48d2c727e5e769a99d9a6311d4e9e451de29e00eeb07c` | `9d2ae591f4f2eb20fbf48d2c727e5e769a99d9a6311d4e9e451de29e00eeb07c` |
| `writer-memory-diagnosis-20260907/run_probe.py` | `e8a3411c3cbdd523ac7898a4e56bd5e2dccc53fffe04f0198ebc6343a2d7e04a` | `e8a3411c3cbdd523ac7898a4e56bd5e2dccc53fffe04f0198ebc6343a2d7e04a` |
| `writer-memory-diagnosis-20260907/cache_control.py` | `18f4c6699e35f618de69a49a2f6842895489cacc7f174a1f937779e965ff3ebd` | `18f4c6699e35f618de69a49a2f6842895489cacc7f174a1f937779e965ff3ebd` |
| `writer-memory-diagnosis-20260907/make_diagnosis_report.py` | `01f80557c0a2e6ce4eb307f33f518ac3a917948612b97b0181b186545e220fae` | `01f80557c0a2e6ce4eb307f33f518ac3a917948612b97b0181b186545e220fae` |
| `schema-cache-validation/benchmark.cc` | `958e75b6b4fa55edc47ffb7b6320c794c3daade3ac114980e3e89c59fcb70aa4` | `958e75b6b4fa55edc47ffb7b6320c794c3daade3ac114980e3e89c59fcb70aa4` |
| `schema-cache-validation/pr934-java-fixture.cc` | `e34844981e1e81b66627836cab122270e7af0096ec49a092c427793785ba3c18` | `e34844981e1e81b66627836cab122270e7af0096ec49a092c427793785ba3c18` |
| `schema-cache-validation/sparse-aligned-reproduction.cc` | `b901f47a9529d3b88c5d5a7cd1761b96ac8e29c4ea1d0fe1a6f33a56f90ed318` | `b901f47a9529d3b88c5d5a7cd1761b96ac8e29c4ea1d0fe1a6f33a56f90ed318` |
| `large-table-benchmark/table_write_throughput.cc` | `343353ea0103ac29d0a61f6371d9fb42ab349d3e53173c8955c06724eb72d249` | `343353ea0103ac29d0a61f6371d9fb42ab349d3e53173c8955c06724eb72d249` |
| `large-table-benchmark/run_benchmark.py` | `ea7235a3a5b4f4a6c6159b6aef49aa7064354a4d932aadc4679b19698af51090` | `1d21e362559c178dbeb64d3e8714426cefd89846bec35f25349deeafddda0764` |
| `large-table-benchmark/matrix-20260907/table_write_phases.cc` | `bdd66156d0469675732aa7927fa9365a0b387b7b1dcaf207fcf0272eac507f92` | `bdd66156d0469675732aa7927fa9365a0b387b7b1dcaf207fcf0272eac507f92` |
| `large-table-benchmark/matrix-20260907/matrix_runner.py` | `fce02505f6c68fa13f961d11b14d7d936c264429e86bc1594a6d6f64d3daf7a0` | `8b9887c2304ce6f4770e358a88b8a4d6300f21e6794f4cb3bf56cb98fd472089` |
| `large-table-benchmark/matrix-20260907/ReferenceVerify.java` | `37989b4b4e82ff3b4a80bfb332585478e529dd402c4ef77125e9dfb5bf94deaa` | `37989b4b4e82ff3b4a80bfb332585478e529dd402c4ef77125e9dfb5bf94deaa` |
