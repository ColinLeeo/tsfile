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

import csv
import hashlib
import json
import statistics
import tarfile
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    results = json.loads((HERE / "results.json").read_text())
    assert [r["threads"] for r in results] == [1, 2, 4, 8, 16]
    manifest = json.loads((HERE / "manifest.json").read_text())
    for name, digest in manifest["file_sha256"].items():
        assert hashlib.sha256((HERE / name).read_bytes()).hexdigest() == digest
    rows = []
    for result in results:
        threads = result["threads"]
        writer = result["writer"]
        for name in [f"threads-{threads:02d}-write.json", f"threads-{threads:02d}-r1-read.json", f"threads-{threads:02d}-r2-read.json"]:
            cache_event = json.loads((HERE / "cache-events" / name).read_text())
            assert cache_event["exit_code"] == 0
            assert cache_event["probe_before"]["resident_pages_after"] > 0
            assert cache_event["probe_after"]["resident_pages_after"] == 0
        readers = result["reads"]
        assert result["verification"] == "passed" and result["generated_file_deleted"]
        assert writer["rows"] == 2000000000 and writer["numeric_points"] == 10000000000
        assert writer["writer_threads"] == threads
        assert writer["parallel_write"] == (threads > 1)
        assert len(readers) == 2
        for repeat, reader in enumerate(readers, 1):
            assert reader["rows"] == 2000000000 and reader["numeric_points"] == 10000000000
            assert reader["blocks"] == 20000 and reader["returned_column_buffer_bytes"] == 106000000000
            assert reader["device_rows"] == [200000000] * 10
            assert reader["threads"] == threads and reader["sampled_rows"] == 60000
            assert reader["sample_checksum"] == results[0]["reads"][0]["sample_checksum"]
            cache = json.loads((HERE / "reader" / f"threads-{threads:02d}-r{repeat}" / "cache-before.json").read_text())
            assert cache["cold_verified"] and cache["resident_pages_after"] == 0
        read_mean = statistics.mean(r["end_to_end_s"] for r in readers)
        record = {"threads": threads, "prepare_s": writer["prepare_s"],
                  "ingestion_s": writer["ingestion_s"], "write_e2e_s": writer["end_to_end_s"],
                  "write_rows_per_s": 2000000000 / writer["end_to_end_s"],
                  "write_points_per_s": 10000000000 / writer["end_to_end_s"],
                  "read_r1_e2e_s": readers[0]["end_to_end_s"],
                  "read_r2_e2e_s": readers[1]["end_to_end_s"], "read_e2e_mean_s": read_mean,
                  "read_rows_per_s": 2000000000 / read_mean,
                  "read_points_per_s": 10000000000 / read_mean,
                  "file_bytes": result["file_bytes"],
                  "write_cpu_s": writer["cpu_user_s"] + writer["cpu_system_s"],
                  "read_cpu_mean_s": statistics.mean(r["cpu_user_s"] + r["cpu_system_s"] for r in readers)}
        rows.append(record)
    for row in rows:
        row["write_speedup"] = rows[0]["write_e2e_s"] / row["write_e2e_s"]
        row["read_speedup"] = rows[0]["read_e2e_mean_s"] / row["read_e2e_mean_s"]
    (HERE / "summary.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n")
    with (HERE / "results.csv").open("w", newline="") as stream:
        csv_writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        csv_writer.writeheader()
        csv_writer.writerows(rows)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties
    font = Path("/System/Library/Fonts/PingFang.ttc")
    if not font.exists():
        font = Path("/System/Library/Fonts/STHeiti Light.ttc")
    if font.exists():
        from matplotlib import font_manager
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"] = FontProperties(fname=str(font)).get_name()
    plt.rcParams.update({"font.size": 11, "axes.unicode_minus": False, "svg.fonttype": "path"})
    x = [r["threads"] for r in rows]
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5), layout="constrained")
    ax = axes[0]
    for key, label, color in [("write", "写入端到端（一次）", "#1769aa"),
                              ("read", "读取端到端（两次冷缓存）", "#16805d")]:
        ax.plot(x, [r[key + "_rows_per_s"] / 10000 for r in rows],
                marker="o", linewidth=2, label=label, color=color)
    low = [2000000000 / max(r["read_r1_e2e_s"], r["read_r2_e2e_s"]) / 10000 for r in rows]
    high = [2000000000 / min(r["read_r1_e2e_s"], r["read_r2_e2e_s"]) / 10000 for r in rows]
    ax.fill_between(x, low, high, color="#16805d", alpha=.14)
    ax.set(ylabel="端到端吞吐（万行/秒）", title="整型数据：线程数与端到端吞吐", ylim=(0, None))
    secondary = ax.secondary_yaxis("right", functions=(lambda v: v * 5, lambda v: v / 5))
    secondary.set_ylabel("端到端吞吐（万数值点/秒）")
    ax.legend(loc="best")
    for key, label, color in [("write", "写入", "#1769aa"), ("read", "读取", "#16805d")]:
        axes[1].plot(x, [r[key + "_speedup"] for r in rows], marker="o", linewidth=2, label=label, color=color)
    axes[1].axhline(1, color="#777777", linestyle="--", linewidth=1)
    axes[1].set(ylabel="相对 1 线程的加速比（倍）", title="线程增加带来的性能变化", ylim=(0, None))
    axes[1].legend(loc="best")
    for ax in axes:
        ax.set(xlabel="线程数", xticks=x, xlim=(.6, 16.4))
        ax.grid(axis="y", alpha=.22)
        ax.spines[["top"]].set_visible(False)
    fig.savefig(HERE / "thread-scaling.png", dpi=150)
    fig.savefig(HERE / "thread-scaling.svg")
    plt.close(fig)

    best_write = min(rows, key=lambda r: r["write_e2e_s"])
    best_read = min(rows, key=lambda r: r["read_e2e_mean_s"])
    header = (TARGET_REPORT := HERE.parent / "large-table-benchmark/integer-20260907/benchmark-summary.md").read_text().split("-->", 1)[0] + "-->\n\n"
    report = header + "# 本机整型数据：1、2、4、8、16 线程读写性能\n\n"
    report += f"本轮写入端到端最高为 **{best_write['threads']} 线程，{best_write['write_rows_per_s']/10000:.2f} 万行/秒、{best_write['write_points_per_s']/10000:.2f} 万数值点/秒**，相对单线程 {best_write['write_speedup']:.2f} 倍。\n\n"
    report += f"冷缓存读取端到端最高为 **{best_read['threads']} 线程，{best_read['read_rows_per_s']/10000:.2f} 万行/秒、{best_read['read_points_per_s']/10000:.2f} 万数值点/秒**，相对单线程 {best_read['read_speedup']:.2f} 倍。\n\n"
    report += f"从 8 到 16 线程，写入端到端吞吐增加 {(rows[-1]['write_rows_per_s']/rows[-2]['write_rows_per_s']-1)*100:.2f}%；读取平均耗时为 {rows[-2]['read_e2e_mean_s']:.2f} 秒和 {rows[-1]['read_e2e_mean_s']:.2f} 秒，两次实测范围有重叠，本轮读取性能基本持平。\n\n"
    report += f"![线程数与吞吐变化]({HERE}/thread-scaling.png)\n\n"
    report += "## 表结构与实验配置\n\n"
    report += "本机 Apple M5 Pro，15 核、24 GiB 内存，macOS 26.6.2，Apple SSD。C++ TsFile，Apple Clang 21，O3、native、LTO、SIMD；读写使用同一固定版本的动态库。\n\n"
    report += "| 列 | 类型 |\n|---|---|\n| time | INT64 时间列 |\n| device | STRING TAG |\n| i32、i32_2 | INT32 |\n| i64、i64_2、i64_3 | INT64 |\n\n"
    report += "每档一个文件、一张 benchmark 表、10 个设备，每设备 2 亿行，共 **20 亿行、100 亿整型数值点**。1 行 = 5 个数值点，时间和 TAG 不计点。数据公式与上一轮整型实验一致，无空值。所有数值列与时间列采用 TS2DIFF＋LZ4。\n\n"
    report += "写入使用列数组批量接口，每个 Tablet 10 万行：按设备顺序连续放入每设备 1 万行，共 20,000 次批量调用。每页上限 5 万行、页内存阈值 16 MiB、ChunkGroup 阈值 512 MiB，开启关闭时磁盘同步。\n\n"
    report += "读取每档刚生成的完整文件，单次整表查询全部 7 列，时间 INT64_MIN～INT64_MAX，无过滤、limit 或 offset，TSBlock 每批 10 万行。每轮解码并返回全部 20 亿行、100 亿数值点，累计列缓冲区 106 GB。\n\n"
    report += "1 线程关闭内部并行；2、4、8、16 线程设置单个 writer/reader 的工作线程池大小。每档写入一次、读取两次；按 1、2、4、8、16 顺序串行执行，读写不重叠。每次写入及读取前均执行 macOS 系统文件缓存 purge，并验证预热的独立缓存探针驻留页归零；每次读取再清理目标文件页缓存并确认其驻留页为 0。清缓存不计时，正式扫描保留正常缓冲 I/O 和预读。本机有 15 个逻辑核，16 线程为本次指定的线程池配置。\n\n"
    report += "## 写入结果\n\n数据写入时间包括库/文件初始化、批量写入、编码压缩、自动及最终 flush、close/fsync、F_FULLFSYNC。端到端另外包括数据准备。写后回读校验不计入写入时间。\n\n"
    report += "| 线程 | 准备时间（秒） | 数据写入时间（秒） | 端到端时间（秒） | 端到端吞吐（万行/秒） | 端到端吞吐（万数值点/秒） | 加速比 |\n|---:|---:|---:|---:|---:|---:|---:|\n"
    for r in rows:
        report += f"| {r['threads']} | {r['prepare_s']:.2f} | {r['ingestion_s']:.2f} | {r['write_e2e_s']:.2f} | {r['write_rows_per_s']/10000:.2f} | {r['write_points_per_s']/10000:.2f} | {r['write_speedup']:.2f}× |\n"
    report += "\n## 冷缓存读取结果\n\n端到端包括线程池初始化、open、query、全部 TSBlock 返回、逐块检查及抽样值校验、关闭及清理。平均时间为两次耗时的算术平均，吞吐为总行/点数除以平均时间。图中读取阴影表示两次实测范围。\n\n"
    report += "| 线程 | 第一次（秒） | 第二次（秒） | 端到端平均（秒） | 端到端吞吐（万行/秒） | 端到端吞吐（万数值点/秒） | 加速比 |\n|---:|---:|---:|---:|---:|---:|---:|\n"
    for r in rows:
        report += f"| {r['threads']} | {r['read_r1_e2e_s']:.2f} | {r['read_r2_e2e_s']:.2f} | {r['read_e2e_mean_s']:.2f} | {r['read_rows_per_s']/10000:.2f} | {r['read_points_per_s']/10000:.2f} | {r['read_speedup']:.2f}× |\n"
    report += "\n全部 5 次写入和 10 次全量读取均通过校验。写入后 C++、Java 分别抽样 100,110 行；每次全量读取逐块检查类型、空值、缓冲区尺寸、设备累计行数，并校验 60,000 行的全部列，所有轮次抽样校验和一致。\n\n"
    report += "每档写入一次、读取两次，结果表示本轮实测；固定执行顺序和系统负载、温度、SSD 状态可能影响相邻档位的小幅差异，不把小幅差异解释为稳定的优化收益。冷缓存指文件页缓存，未清 CPU、文件系统全部元数据或 SSD 控制器缓存。\n\n"
    report += f"[CSV 数据]({HERE}/results.csv) · [完整原始结果]({HERE}/results.json) · [配置]({HERE}/manifest.json) · [运行脚本]({HERE}/run_sweep.py) · [矢量图]({HERE}/thread-scaling.svg)\n\n"
    report += f"每档新增大文件校验后已清理，原有加载用文件继续保留：\n\n```text\n{manifest['protected_file']}\n```\n"
    (HERE / "benchmark-summary.md").write_text(report)
    with tarfile.open(HERE / "evidence.tar.gz", "w:gz") as archive:
        for path in sorted(HERE.rglob("*")):
            if path.is_file() and path.suffix in (".md", ".json", ".log", ".csv", ".cc", ".py", ".java", ".make", ".svg", ".png"):
                archive.add(path, arcname=str(Path(HERE.name) / path.relative_to(HERE)))
    print(json.dumps({"best_write": best_write, "best_read": best_read}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
