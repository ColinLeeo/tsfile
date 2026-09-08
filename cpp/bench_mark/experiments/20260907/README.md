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

# 2026-09-07 TsFile C++ 性能实验源码

本目录保存实际用于实验的 benchmark 源码、运行脚本、内存诊断和 Java 校验代码。源码整理自本地 `cpp/target/`，仅清理部分许可头的行末空白；来源与 SHA-256 见 [SOURCE-MANIFEST.md](SOURCE-MANIFEST.md)。生成的 TsFile、二进制、依赖库和运行结果不属于源码包。

本分支基于实验分支 `pr934` 的 `2d3bf750b`，同时保存实验时未提交的 writer 缓存、稀疏列对齐、INT64 SIMD 统计类型兼容性，以及 TS2DIFF 宽位解码修复和回归测试。缓存机制另有 [PR #945](https://github.com/apache/tsfile/pull/945)，该 PR 基于更新的 develop 并另含空页恢复修复；本分支保留原实验源码快照。

## 代码入口

| 内容 | 路径 |
| --- | --- |
| 整型写入：分阶段计时、持久化、RSS、结果校验 | `thread-scaling-integer-cold-20260907/writer/table_write_phases.cc` |
| 整型批量读取：全列扫描、吞吐、RSS、结果校验 | `thread-scaling-integer-cold-20260907/reader/table_read_benchmark.cc` |
| 文件缓存驱逐与 mincore 检查 | `thread-scaling-integer-cold-20260907/reader/file_cache.cc` |
| 原实验 1、2、4、8、16 线程编排 | `thread-scaling-integer-cold-20260907/run_sweep.py` |
| macOS 系统缓存清理服务 | `thread-scaling-integer-cold-20260907/cache_control.py` |
| Java 元数据与抽样值校验 | `thread-scaling-integer-cold-20260907/java/ReferenceVerify.java` |
| 内存诊断：RSS、allocator、writer 缓冲采样 | `writer-memory-diagnosis-20260907/` |
| 缓存微基准、稀疏列复现、跨语言数据生成 | `schema-cache-validation/` |
| 早期混合数值类型写入与分阶段实验 | `large-table-benchmark/` |
| 单元回归测试 | 仓库的 `cpp/test/writer/tsfile_writer_schema_cache_test.cc`、`cpp/test/encoding/ts2diff_codec_test.cc` |

`run_sweep.py`、内存诊断编排和报告脚本保留了原机的目录、classpath、数据文件大小及 manifest 检查；它们是历史实验脚本，直接运行需重建当时的目录和运行产物。下面的独立编译与执行方式不依赖原机的大文件或报告 manifest。

`schema-cache-validation/` 中的 `pr934-java-fixture.cc` 和 `sparse-aligned-reproduction.cc` 是 GoogleTest fixture，需链接 GoogleTest；`benchmark.cc` 是独立微基准。

## Schema 与数据规模

表名 `benchmark`：`time` 为 INT64 时间戳，`device` 为 STRING TAG；5 个 FIELD 依次为 `i32 INT32`、`i64 INT64`、`i32_2 INT32`、`i64_2 INT64`、`i64_3 INT64`。

10 个设备，每设备 2 亿时间行，共 20 亿行、100 亿数值点。每个 Tablet 共 10 万行，包含 10 个设备；每设备连续 1 万行，填完当前设备再填下一个。通过列数组准备数据并批量提交 Tablet。数值与时间使用 TS2DIFF，压缩 LZ4；page 5 万行，page 内存阈值 16 MiB，chunk group 内存阈值 512 MiB。

线程数为 1、2、4、8、16。每轮只有一个 writer/reader；多线程由内部线程池承担编码/解码。写入 CLI 的 `--threads 0` 表示禁用并行（实验的 1 线程），读取 CLI 的 `--threads 1` 表示禁用并行。内存诊断程序使用 macOS 的 Mach 和 malloc 接口。

## 编译与运行

以下命令在仓库根目录执行，适用于 macOS。CMake 使用 Unix Makefiles，因为原有 Python 编译脚本读取 `flags.make`。需要 CMake、C++ 编译器和 Python 3；系统缓存清理需要管理员权限。

```bash
cmake -S cpp -B cpp/target/bench-release -G 'Unix Makefiles' \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TEST=OFF \
  -DENABLE_MEM_STAT=OFF -DENABLE_ASAN=OFF \
  -DENABLE_THREADS=ON -DENABLE_SIMD=ON \
  -DTSFILE_ENABLE_NATIVE_ARCH=ON -DENABLE_LZMA2=ON
cmake --build cpp/target/bench-release -j 8

BENCH="$PWD/cpp/bench_mark/experiments/20260907/thread-scaling-integer-cold-20260907"
python3 "$BENCH/writer/matrix_runner.py" build --build-dir cpp/target/bench-release
python3 "$BENCH/reader/run_read_benchmark.py" build --build-dir cpp/target/bench-release
c++ -std=c++11 -O3 "$BENCH/reader/file_cache.cc" -o "$BENCH/reader/file_cache"
```

下面示例执行 1 线程全规模写入和两次冷读取。其他轮次将写入线程设为 2、4、8、16，读取线程设为相同值，使用新的 label。每份全规模文件约 39.1 GiB，需要预留足够空间。每次写入、读取前均清理系统缓存；读取脚本还会驱逐目标文件缓存并确认驻留页数为零。缓存清理不计入数据时间。

```bash
sudo /usr/sbin/purge
python3 "$BENCH/writer/matrix_runner.py" run \
  --label threads-01 --threads 0 --rows-per-device 200000000

sudo /usr/sbin/purge
python3 "$BENCH/reader/run_read_benchmark.py" run \
  --label threads-01-r1 --threads 1 --rows-per-device 200000000 \
  --file "$BENCH/writer/threads-01/data.tsfile"

sudo /usr/sbin/purge
python3 "$BENCH/reader/run_read_benchmark.py" run \
  --label threads-01-r2 --threads 1 --rows-per-device 200000000 \
  --file "$BENCH/writer/threads-01/data.tsfile"
```

小规模正确性检查可将每设备行数改为 `20000`，使用新 label，并给读写程序加 `--verify-all`。Java 校验程序的常量针对完整的每设备 2 亿行实验；运行时另行提供包含 TsFile 及依赖的 Java classpath。

## 输出口径

每次运行保存 `run.log`、`result.json`、`command.json`；读取还保存库与二进制 SHA-256、缓存检查记录。

- `prepare_s`：数据准备时间，单位秒。
- `ingestion_s`：writer 建立、批量数据提交、flush、close、fsync 与持久化屏障的时间，单位秒。
- `end_to_end_s`：完整数据流程时间，单位秒；写后验证在计时区间之外。
- 端到端行吞吐 = 总行数 / `end_to_end_s`，单位行/秒；端到端数值点吞吐 = 总行数 × 5 / `end_to_end_s`，单位点/秒。TAG 与时间列不计入数值点。
- `peak_rss_bytes`：进程峰值常驻内存，单位字节。诊断程序额外采样内存，耗时不应替代正式吞吐结果。

3KW 水位线为 3000 万数值点/秒，相当于本 schema 下的 600 万行/秒。

## 本次整理验证

在本分支的实验源码上重建并运行 C++ 测试：847 项通过，3 项因外部 fixture 未配置而跳过，10 项原有 disabled 测试未运行。读写 benchmark、内存诊断、缓存微基准、混合类型写入和文件缓存工具均重新编译；Python 脚本通过语法检查，Java 校验器编译通过，额外 GoogleTest fixture 通过语法检查。本次整理未重新执行 20 亿行性能实验。
