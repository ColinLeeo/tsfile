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

import json
from pathlib import Path
import zipfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter, MultipleLocator

HERE = Path(__file__).resolve().parent
DIAG = HERE.parent / "writer-memory-diagnosis-20260907"
WATERLINE = 30_000_000


def main():
    rows = json.loads((HERE / "summary.json").read_text())
    raw = json.loads((HERE / "results.json").read_text())
    audit = json.loads((HERE / "final-audit.json").read_text())
    assert audit["verification"] == "passed"
    assert [row["threads"] for row in rows] == [1, 2, 4, 8, 16]
    for row, result in zip(rows, raw):
        assert row["threads"] == result["threads"]
        assert abs(row["write_points_per_s"] - 10_000_000_000 / result["writer"]["end_to_end_s"]) < .001
        read_mean = sum(r["end_to_end_s"] for r in result["reads"]) / 2
        assert abs(row["read_points_per_s"] - 10_000_000_000 / read_mean) < .001
        for operation in ["write", "read"]:
            assert abs(row[operation + "_points_per_s"] - 5 * row[operation + "_rows_per_s"]) < .001

    for path in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Light.ttc"]:
        if Path(path).exists():
            font_manager.fontManager.addfont(path)
            plt.rcParams["font.family"] = font_manager.FontProperties(fname=path).get_name()
            break
    plt.rcParams.update({"font.size": 11, "axes.unicode_minus": False, "svg.fonttype": "path"})
    colors = {"write": "#2563eb", "read": "#0f766e", "waterline": "#dc2626"}
    x = [r["threads"] for r in rows]
    fig, ax = plt.subplots(figsize=(13, 6.9))
    fig.subplots_adjust(left=.10, right=.89, bottom=.15, top=.79)
    fig.suptitle("整型数据：端到端吞吐随线程数的变化", x=.10, y=.985,
                 ha="left", fontsize=19, fontweight="bold")
    fig.text(.10, .925, "本机 M5 Pro  ·  20 亿行 / 100 亿数值点  ·  每次写入、读取前均清理系统文件缓存",
             fontsize=11, color="#475569")
    for operation, label in [("write", "写入端到端"), ("read", "读取端到端（两次平均）")]:
        y = [r[operation + "_points_per_s"] / 10000 for r in rows]
        ax.plot(x, y, color=colors[operation], label=label, marker="o", markersize=7,
                linewidth=2.6, zorder=4)
        for xi, yi in zip(x, y):
            ax.annotate(f"{yi:,.0f}", (xi, yi), xytext=(0, 11), textcoords="offset points",
                        ha="center", color=colors[operation], fontsize=11)
    lower = [10_000_000_000 / max(r["read_r1_e2e_s"], r["read_r2_e2e_s"]) / 10000 for r in rows]
    upper = [10_000_000_000 / min(r["read_r1_e2e_s"], r["read_r2_e2e_s"]) / 10000 for r in rows]
    ax.fill_between(x, lower, upper, color=colors["read"], alpha=.12, linewidth=0, zorder=2)
    ax.axhline(WATERLINE / 10000, color=colors["waterline"], linestyle=(0, (6, 4)),
               linewidth=2, label="3KW 水位线", zorder=3)
    ax.annotate("3KW = 3000 万数值点/秒 = 600 万行/秒",
                xy=(.99, WATERLINE / 10000), xycoords=ax.get_yaxis_transform(),
                xytext=(0, -10), textcoords="offset points", ha="right", va="top",
                fontsize=11, color=colors["waterline"],
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": .9, "pad": 2})
    ax.set_xscale("log", base=2)
    ax.set_xticks(x, labels=[str(v) for v in x])
    ax.set(xlim=(.8, 20), ylim=(0, 33000), xlabel="线程数（倍增档位）",
           ylabel="端到端吞吐（万数值点/秒）")
    ax.yaxis.set_major_locator(MultipleLocator(5000))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    right = ax.secondary_yaxis("right", functions=(lambda value: value / 5, lambda value: value * 5))
    right.set_ylabel("端到端吞吐（万行/秒）", labelpad=12)
    right.yaxis.set_major_locator(MultipleLocator(1000))
    right.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    ax.grid(axis="y", color="#e2e8f0", linewidth=.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#94a3b8")
    ax.spines["bottom"].set_color("#94a3b8")
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.035), ncol=3, frameon=False,
              borderaxespad=0, handlelength=2.5, columnspacing=2)
    fig.text(.10, .025, "写入：每档单次实测；读取：每档两次，阴影为实测范围。1 行 = 5 个数值点，时间与 TAG 不计点。",
             fontsize=10, color="#64748b")
    fig.savefig(HERE / "throughput-waterline.png", dpi=180)
    fig.savefig(HERE / "throughput-waterline.svg")
    plt.close(fig)

    best_write = max(rows, key=lambda r: r["write_points_per_s"])
    best_read = max(rows, key=lambda r: r["read_points_per_s"])
    threshold = {
        "waterline_label": "3KW", "numeric_points_per_s": WATERLINE,
        "equivalent_rows_per_s": WATERLINE / 5, "points_per_row": 5,
        "results": [{"threads": r["threads"],
                     "write_ratio": r["write_points_per_s"] / WATERLINE,
                     "read_ratio": r["read_points_per_s"] / WATERLINE,
                     "write_above": r["write_points_per_s"] >= WATERLINE,
                     "read_above": r["read_points_per_s"] >= WATERLINE} for r in rows],
    }
    assert all(r["write_above"] and r["read_above"] for r in threshold["results"])
    (HERE / "waterline-results.json").write_text(json.dumps(threshold, ensure_ascii=False, indent=2) + "\n")
    original = (HERE / "benchmark-summary.md").read_text()
    header, content = original.split("-->", 1)
    details = content[content.index("## 表结构与实验配置"):]
    details = details[:details.index("## 写入结果")]
    report = header + "-->\n\n# 本机整型读写性能报告\n\n"
    report += "本轮测试 1、2、4、8、16 线程。**所有档位的写入、读取端到端吞吐均超过 3KW 水位线。**\n\n"
    report += "水位线为 **3000 万数值点/秒（30,000,000 点/秒）**，按每行 5 个数值点计算，等价于 **600 万行/秒**。\n\n"
    report += f"![端到端吞吐随线程数变化及 3KW 水位线]({HERE}/throughput-waterline.png)\n\n"
    report += "## 数据结果\n\n所有吞吐采用端到端口径；读取时间取两次实测的平均值。\n\n"
    report += "| 线程数 | 写入时间（秒） | 写入（万行/秒） | 写入（万数值点/秒） | 读取时间（秒） | 读取（万行/秒） | 读取（万数值点/秒） |\n|---:|---:|---:|---:|---:|---:|---:|\n"
    for r in rows:
        report += f"| {r['threads']} | {r['write_e2e_s']:.2f} | {r['write_rows_per_s']/10000:.2f} | {r['write_points_per_s']/10000:.2f} | {r['read_e2e_mean_s']:.2f} | {r['read_rows_per_s']/10000:.2f} | {r['read_points_per_s']/10000:.2f} |\n"
    report += "\n## 内存占用\n\n"
    report += "每次运行记录进程峰值常驻内存（Peak RSS），通过 `getrusage(RUSAGE_SELF).ru_maxrss` 获取，并从字节换算为 MiB（1 MiB = 1,048,576 字节）。下表读取两次分别列出，均为各次运行的峰值。\n\n"
    report += "| 线程数 | 写入峰值（MiB） | 读取第 1 次峰值（MiB） | 读取第 2 次峰值（MiB） |\n|---:|---:|---:|---:|\n"
    for result in raw:
        write_rss = result['writer']['peak_rss_bytes'] / 1024**2
        read_rss = [r['peak_rss_bytes'] / 1024**2 for r in result['reads']]
        assert write_rss > 0 and len(read_rss) == 2 and min(read_rss) > 0
        report += f"| {result['threads']} | {write_rss:.2f} | {read_rss[0]:.2f} | {read_rss[1]:.2f} |\n"
    report += "\n写入峰值覆盖进程启动、数据准备、批量写入、flush、落盘同步及 close，在写后抽样校验前获取；读取峰值覆盖进程启动、全量读取、读取中的校验及关闭。RSS 是进程截至采样时的历史峰值，不是结束时的当前内存，也不等于整机总内存占用或系统文件缓存总量。\n\n"
    report += "原始吞吐实验未连续采集内存随时间变化、系统文件缓存总量、内存压力或 Swap，无法还原当时峰值的精确构成。4 线程写入峰值约 1.96 GiB，明显高于其他档位；每档仅一次写入，不能据此认定内存占用随线程数的稳定变化规律。\n\n"
    if (DIAG / "diagnostic-results.json").exists():
        diagnostic = json.loads((DIAG / "diagnostic-results.json").read_text())
        four, eight = diagnostic
        assert [r["threads"] for r in diagnostic] == [4, 8]
        report += f"追加相同 20 亿行规模的内存诊断：4、8 线程 RSS 峰值分别为 **{four['peak_rss_mib']:.2f}、{eight['peak_rss_mib']:.2f} MiB**，采样最大在用堆内存分别为 **{four['sampled_heap_peak_mib']:.2f}、{eight['sampled_heap_peak_mib']:.2f} MiB**。flush、close 后，在用堆内存明显回落，但大量已释放、被系统标记为可回收的页仍计入 RSS。原始的 2011.16 MiB 不是稳定的四线程内存需求；复测支持分配器保留页与系统回收时机影响 RSS 的解释，不能据此量化原始峰值的各项构成。\n\n"
        report += f"诊断运行含额外采样开销，未混入正式吞吐结果。详见[内存诊断与过程曲线]({DIAG}/memory-diagnosis.md)。\n\n"
    report += "\n## 趋势与水位线\n\n"
    report += f"单线程写入为 **{rows[0]['write_points_per_s']/10000:.2f} 万数值点/秒**，高于水位线 **{(rows[0]['write_points_per_s']/WATERLINE-1)*100:.2f}%**。\n\n"
    report += f"写入端到端吞吐随线程数增加而提高，16 线程达到 **{best_write['write_points_per_s']/10000:.2f} 万数值点/秒**，为水位线的 **{best_write['write_points_per_s']/WATERLINE:.2f} 倍**、单线程的 **{best_write['write_speedup']:.2f} 倍**。8 到 16 线程只再提升 **{(rows[-1]['write_points_per_s']/rows[-2]['write_points_per_s']-1)*100:.2f}%**。\n\n"
    report += f"读取在 8 线程附近趋于平稳。本轮 8 线程平均为 **{best_read['read_points_per_s']/10000:.2f} 万数值点/秒**，为水位线的 **{best_read['read_points_per_s']/WATERLINE:.2f} 倍**、单线程的 **{best_read['read_speedup']:.2f} 倍**。8 和 16 线程的两次实测范围重叠，本轮读取性能基本持平。\n\n"
    report += details
    report += "写入端到端时间包括数据准备、初始化、批量写入、编码压缩、flush、落盘同步和 close；读取端到端包括初始化、open、query、全部 TSBlock 返回、逐块检查、抽样值校验及关闭。清缓存不计入端到端时间。\n\n"
    report += "全部 5 次写入、10 次全量读取以及 15 次正式运行前的系统清缓存检查均通过。写入后 C++、Java 分别抽样 100,110 行；每次全量读取检查全部设备的行数、每块列类型和缓冲区，并校验 60,000 行的全部列。\n\n"
    report += "每档写入一次、读取两次，结果代表本轮实测；相邻档位的小幅差异不代表稳定的性能差异。\n\n"
    report += f"[分阶段耗时、详细校验与完整配置]({HERE}/benchmark-summary.md) · [原始结果 JSON]({HERE}/results.json) · [CSV 数据]({HERE}/results.csv)\n"
    report += f"\n[下载折线图 PNG]({HERE}/throughput-waterline.png) · [下载矢量图 SVG]({HERE}/throughput-waterline.svg) · [水位线对照数据]({HERE}/waterline-results.json)\n"
    (HERE / "throughput-report.md").write_text(report)
    # Bundle relative Markdown links and their assets for use on another machine.
    bundle_files = ["throughput-report.md", "throughput-waterline.png",
                    "throughput-waterline.svg", "benchmark-summary.md",
                    "thread-scaling.png", "thread-scaling.svg", "results.json",
                    "results.csv", "summary.json", "waterline-results.json",
                    "manifest.json", "run_sweep.py"]
    with zipfile.ZipFile(HERE / "tsfile-integer-performance-report.zip", "w",
                         compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle_sources = [HERE / name for name in bundle_files]
        if (DIAG / "diagnostic-results.json").exists():
            bundle_sources += [DIAG / name for name in ["memory-diagnosis.md",
                "memory-diagnosis.png", "memory-diagnosis.svg",
                "diagnostic-results.json", "memory-samples.csv"]]
        for source in bundle_sources:
            name = source.name
            if source.suffix == ".md":
                bundle.writestr(name, source.read_text().replace(str(HERE) + "/", "").replace(str(DIAG) + "/", ""))
            else:
                bundle.write(source, name)
    print(json.dumps(threshold, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
