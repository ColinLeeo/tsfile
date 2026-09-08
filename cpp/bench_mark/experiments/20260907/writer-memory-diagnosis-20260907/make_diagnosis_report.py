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
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

HERE = Path(__file__).resolve().parent
MIB = 1024**2


def main():
    cases = []
    for directory in sorted(HERE.glob('full-diagnostic-*-threads-*')):
        summary = json.loads((directory / 'summary.json').read_text())
        samples = [json.loads(line) for line in (directory / 'run.log').read_text().splitlines()
                   if line.startswith('{') and '"event":"memory"' in line]
        assert summary['result']['rows'] == 2_000_000_000
        assert summary['result']['verification'] == 'passed'
        assert summary['generated_file_deleted'] and not (directory / 'data.tsfile').exists()
        cache = json.loads((directory / 'cache-before.json').read_text())
        assert cache['probe_before']['resident_pages_after'] > 0 and cache['probe_after']['resident_pages_after'] == 0
        series = [s for s in samples if s['phase'] == 'write']
        close = next(s for s in samples if s['phase'] == 'after_close')
        entry = {'threads': summary['threads'], 'rows': summary['result']['rows'],
                 'peak_rss_mib': summary['result']['peak_rss_bytes']/MIB,
                 'sampled_heap_peak_mib': max(s['heap_in_use_bytes'] for s in samples)/MIB,
                 'sampled_writer_chunk_peak_mib': max(s['writer_chunk_estimate_bytes'] for s in samples)/MIB,
                 'physical_footprint_peak_mib': close['physical_footprint_peak_bytes']/MIB,
                 'close_mib': {k.removesuffix('_bytes'): v/MIB if v >= 0 else None for k,v in close.items() if k.endswith('_bytes')},
                 'source': str(directory/'run.log')}
        cases.append((entry, series))
    assert [c['threads'] for c,_ in cases] == [4,8]
    font = '/System/Library/Fonts/STHeiti Light.ttc'
    font_manager.fontManager.addfont(font)
    plt.rcParams.update({'font.family':font_manager.FontProperties(fname=font).get_name(),
                         'font.size':10, 'axes.unicode_minus':False, 'svg.fonttype':'path'})
    fig,axes=plt.subplots(2,1,figsize=(12,8),sharex=True,sharey=True)
    fig.subplots_adjust(left=.09,right=.98,bottom=.11,top=.84,hspace=.28)
    fig.suptitle('整型写入内存诊断：RSS、堆内存与可回收页',x=.09,y=.98,ha='left',fontsize=18,fontweight='bold')
    fig.text(.09,.93,'每档 20 亿行；同一冻结版本动态库；每次写入前清缓存；每 100 万行采样',color='#475569')
    metrics=[('rss_bytes','RSS','#2563eb'),('heap_in_use_bytes','仍在使用的堆内存','#dc2626'),
             ('reusable_bytes','系统标记的可回收页','#0f766e')]
    for axis,(entry,series) in zip(axes,cases):
        x=[s['rows']/1e8 for s in series]
        for field,label,color in metrics:
            axis.plot(x,[s[field]/MIB for s in series],label=label,color=color,lw=1.35,alpha=.85)
        axis.set_title(f"{entry['threads']} 线程：进程 RSS 峰值 {entry['peak_rss_mib']:.2f} MiB",loc='left')
        axis.set_ylabel('内存（MiB）')
        axis.grid(axis='y',color='#e2e8f0')
        axis.spines[['top','right']].set_visible(False)
        axis.set_xlim(0,20)
    axes[0].legend(loc='lower left',bbox_to_anchor=(0,1.10),ncol=3,frameon=False)
    axes[-1].set_xlabel('累计写入行数（亿行）')
    fig.text(.09,.025,'堆内存按批次涨落；已释放的页可能继续计入 RSS。诊断采样会增加开销，本图不用于替换正式吞吐数据。',color='#64748b')
    fig.savefig(HERE/'memory-diagnosis.png',dpi=160)
    fig.savefig(HERE/'memory-diagnosis.svg')
    plt.close(fig)
    (HERE/'diagnostic-results.json').write_text(json.dumps([e for e,_ in cases],ensure_ascii=False,indent=2)+'\n')
    with (HERE/'memory-samples.csv').open('w') as out:
        fields=['threads','rows','elapsed_s','rss_bytes','heap_in_use_bytes','reusable_bytes','physical_footprint_bytes','writer_chunk_estimate_bytes']
        writer=csv.DictWriter(out,fieldnames=fields)
        writer.writeheader()
        for entry,series in cases:
            for sample in series:
                writer.writerow({'threads':entry['threads'],**{k:sample[k] for k in fields if k!='threads'}})
    header=(HERE.parent/'thread-scaling-integer-cold-20260907/throughput-report.md').read_text().split('-->',1)[0]+'-->\n\n'
    report=header+'# 4 线程写入内存峰值诊断\n\n'
    report+='原始实验记录的 2011.16 MiB 是进程 Peak RSS。追加复测显示，RSS 包含大量已释放并被系统标记为可回收的页，不能用它直接代表 writer 仍在使用的缓冲区大小。原始实验只有最终峰值，没有逐时记录，因此无法还原当时 2011.16 MiB 的精确构成。\n\n'
    report+='## 原规模复测结果\n\n每档 10 个设备、每设备 2 亿行，Tablet 10 万行、5 个整型数值列，其余写入配置与原实验相同；每次启动前清理系统文件缓存并验证探针驻留页归零。\n\n'
    report+='| 线程数 | 进程 Peak RSS（MiB） | 采样最大在用堆内存（MiB） | 采样最大 writer 缓冲估算（MiB） | 系统记账 footprint 峰值（MiB） |\n|---:|---:|---:|---:|---:|\n'
    for e,_ in cases:
        report+=f"| {e['threads']} | {e['peak_rss_mib']:.2f} | {e['sampled_heap_peak_mib']:.2f} | {e['sampled_writer_chunk_peak_mib']:.2f} | {e['physical_footprint_peak_mib']:.2f} |\n"
    report+='\n在用堆内存包括已分配、尚未释放的对象与缓冲；writer 缓冲是其内置估算。二者每 10 个 Tablet（100 万行）采样一次，可能遗漏批次内部瞬时峰值；Peak RSS 和 footprint 峰值由系统累计。\n\n'
    report+='## close 后仍然计入 RSS 的空间\n\n'
    report+='| 线程数 | RSS（MiB） | 可回收页（MiB） | 在用堆内存（MiB） | footprint（MiB） |\n|---:|---:|---:|---:|---:|\n'
    for e,_ in cases:
        c=e['close_mib'];report+=f"| {e['threads']} | {c['rss']:.2f} | {c['reusable']:.2f} | {c['heap_in_use']:.2f} | {c['physical_footprint']:.2f} |\n"
    report+='\nflush、close 后，在用堆内存已明显回落，但大量可回收页仍计入 RSS。这直接解释了为何 RSS 可能明显高于程序当前仍在使用的内存。页的复用、回收时机和线程调度可能影响其规模；本次没有追踪分配调用栈，不能进一步量化各因素对原始 4 线程峰值的贡献。\n\n'
    report+='代码中 512 MiB 阈值由整个 writer 共用，每个 Tablet 都等待本批并行任务完成后再继续；没有按线程各配置一份 512 MiB 缓冲，也没有跨 Tablet 持续提交任务。此次观察不支持将峰值差异归因于 4 线程持有了更多活跃写入数据；本次也不构成完整的内存泄漏检测。\n\n'
    report+='![写入过程内存曲线](memory-diagnosis.png)\n\n'
    report+='## 口径与证据\n\n'
    report+='原报告的 Peak RSS 数据保留，补充说明其口径。正式吞吐结果保持原值，追加诊断运行含采样开销，不混入吞吐对比。原始运行未采集的 footprint、可回收页和 Swap 不做事后补填。\n\n'
    report+='诊断通过 macOS `task_info(TASK_VM_INFO)` 获取当前 RSS、可回收页和 footprint，通过 `malloc_zone_statistics(nullptr, ...)` 获取全部 malloc zone 的在用与保留空间。Apple 对 footprint 与堆内存统计的区别见 [Analyze heap memory](https://developer.apple.com/videos/play/wwdc2024/10173/)，字段定义见 [task_info.h](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/mach/task_info.h)。\n\n'
    report+='[诊断汇总](diagnostic-results.json) · [连续采样 CSV](memory-samples.csv)\n'
    (HERE/'memory-diagnosis.md').write_text(report)
    print(json.dumps([e for e,_ in cases],ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
