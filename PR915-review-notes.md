<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# PR 915 Review Notes

审阅目标：对照 `/Users/colin/Downloads/V2.5.0-TsFile-命令行工具-需求文档.md` 检查 PR 915 的实现。

PR 快照：`origin/pr/915`，提交 `909984b24`。评审文档分支为 `doc/pr915-review-notes`，worktree 为 `/Users/colin/dev/tsfile/worktrees/pr915-review-notes`，其基点与 PR 提交一致。

结论：当前实现尚未满足需求文档，建议在合入前处理下列问题。其中 `[P1]` 表示会造成主要命令语义错误、结果不可靠、交付不安全或无法处理大文件；`[P2]` 表示边界契约或严格输入校验不符合设计。

## 当前仍成立的问题

<!-- tag-comment-scope-A-start mode="block" hash="sha256:cb3ba39ba1ee024f2eac5efe41509437c44cabc4d10c25e108f461495546f43e" -->
### [P1] `head` / `cat` / `export` 的 `offset` 越界没有返回参数错误

`cpp/tools/format/result_set_format.cc:75-114` 只是在结果集遍历时跳过 `offset` 行；匹配行数小于 `offset` 时仍输出空结果并返回成功。无时间过滤时 `cpp/tools/commands/row_query.cc:191-198` 还会把窗口直接下推给 reader，同样没有区分“小于 offset”和“恰好等于 offset”。需求 D-247 要求前者返回 `1`、stdout 为空，且 `export` 不提交目标。
<!-- tag-comment-scope-A-end mode="block" -->
<!-- tag-comment-thread-A
cmd1 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:08:07.562Z","body":"这里应该有几个问题：\n1. offset 应该下推到reader，无论多少都需要吧？\n2. 即便offset 比较大，操作上也是合法的， 只不过超过一定offset之后数据肯定不存在了。 \n3. 而且用户其实也不知道offset 究竟应该多大。 "}
-->

### [P1] 多对象 `export` 的失败清理不完整

`cpp/tools/commands/cmd_export.cc:298-333` 先提交编号文件，再更新 `_manifest.json`。编号文件成功但 Manifest 更新失败时直接返回，没有删除未记录编号文件，也没有在清理失败时报告路径。需求 D-240/D-249 要求正常失败清理未记录文件，清理失败返回 `3`。

<!-- tag-comment-scope-B-start mode="block" hash="sha256:a7eeeb36b06ffa5bdac8fe3c66731f1d5ddb202867be13d0cef3e91190d32713" -->
### [P1] `sketch` 仍是占位实现

`cpp/tools/commands/cmd_export.cc:349-363` 只输出硬编码边框、文件路径和模型，没有调用绑定版本的 `printSketch`，不包含 Page/Chunk/ChunkGroup 等物理结构。需求 D-136/D-204 要求内容、层级、顺序和文本格式原样遵循 `printSketch`。
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd2 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:11:27.346Z","body":"这里后面有这个pr：\nhttps://github.com/apache/tsfile/pull/838"}
-->

### [P1] `stats` 的 tree 模式输出结构和统计口径不完整

<!-- tag-comment-scope-C-start mode="block" hash="sha256:518b2a08f95afae2f1e5e5d54e0fc36da5e38e5bc5844e5abe1202ba0eb7a43a" -->
`cpp/tools/commands/cmd_stats.cc:432-457` 把 `data_type` 固定写成 null，把 `null_count` 固定写成 `0`，并直接把 series statistic 的 count 当作 `non_null_count`。需求 D-178/D-194/D-222 要求真实类型，并以 device 逻辑行总数为分母计算空值。
<!-- tag-comment-scope-C-end mode="block" -->
<!-- tag-comment-thread-C
cmd3 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:12:00.449Z","body":"这里是留下了占位吗？"}
-->

### [P1] `count` 的 tree 模式统计口径不对

`cpp/tools/commands/cmd_count.cc:245-253` 把每个序列的统计点数同时当成 device 的 `row_count` 和该列的 `non_null_count`，并把 `null_count` 固定为 `0`。不同 FIELD 时间点不一致时，各行会得到不同的作用域行数，违反 D-187/D-198/D-222。

### [P1] `stats` / `count` 在多表文件上仍然强制要求 `-t`

<!-- tag-comment-scope-D-start mode="block" hash="sha256:4cc41770f9bc9390c8f8fc57d33b6fb385c73f18b921a4b9231b3c2c027bfb21" -->
`cpp/tools/commands/cmd_stats.cc:225-238` 和 `cpp/tools/commands/cmd_count.cc:85-99` 在表数量不等于 1 时直接返回参数错误；帮助文本也宣称省略作用域仅适用于单对象。需求 D-180/D-188/D-231/D-232 要求省略作用域时覆盖纯模型文件中的全部访问作用域。
<!-- tag-comment-scope-D-end mode="block" -->
<!-- tag-comment-thread-D
cmd4 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:13:22.801Z","body":"我们的评审文档里面应该是都不再要求了吧。 即便是全部表或者全部tree 都能正常显示。 "}
-->

### [P2] 表模型列名投影仍使用字面匹配

`cpp/tools/commands/cmd_schema.cc:68-72`、`cpp/tools/commands/cmd_stats.cc:219-222,274-285`、`cpp/tools/commands/cmd_count.cc:125-135` 都用原始字符串比较 `-m`。table 名虽然被转为小写，TAG/FIELD 名没有按 ASCII 大小写不敏感规则解析并映射回 schema 规范名称，违反 D-031。

### [P2] `schema` 对不存在的作用域或列静默成功

`cpp/tools/commands/cmd_schema.cc:49-89,94-157` 只在输出循环中筛选 table/device/column，不验证请求对象是否命中。`schema -t missing`、`schema -d missing` 或 `schema -m missing` 都可能只输出表头并返回 `0`。需求 D-174/D-175 要求不存在对象或列返回 `1`。

### [P2] tree 模式的 `stats` / `count` 对不存在的 device 或 measurement 静默成功

`cpp/tools/commands/statistics.cc:139-181` 过滤不到元数据就返回空 rows，随后两个命令正常 finish 并返回 `0`。需求 D-181/D-182/D-189/D-190 要求请求名称完全不存在时返回 `1`。

### [P1] `head` / `cat` / `export` 的对象和投影预校验不完整

`cpp/tools/commands/row_query.cc:165-199` 直接把 table `-m` 交给 reader，没有先验证列存在且类别为 FIELD；tree 分支在对象或 FIELD 过滤为空时于 `221-224` 返回执行失败 `3`。需求要求对象不存在、列不存在或类别不匹配在扫描前返回参数错误 `1`。

### [P1] `meta.format_version` 使用编译期常量，不是文件实际版本

`cpp/tools/commands/cmd_meta.cc:30-40` 输出 `storage::VERSION_NUM_BYTE`，这是当前绑定库的格式常量，不是当前输入文件读取到的版本。需求 D-177/D-226 要求返回文件实际版本，并在版本信息无法识别或矛盾时返回 `2`。

### [P1] `export` / `sketch -o` 的目标保护和原子替换不成立

`cpp/tools/commands/cmd_export.cc:42-123` 使用会跟随链接的 `stat`、固定的 `<target>.tmp` 和普通 `rename`。它没有检查目标与源 TsFile 是否为同一 inode，没有拒绝符号链接/FIFO 等特殊目标，可能截断用户已有的 `.tmp` 文件；非 `--force` 的“先检查再 rename”还有竞态，目标并发出现时 Unix `rename` 会直接替换。close/flush 失败也未检查。需求 D-120/D-204/D-223 要求 same-file 保护、特殊目标拒绝和真正的 no-replace/原子提交。

### [P1] `write` 不是“全部成功后再提交正式目标”的原子交付

`cpp/tools/commands/cmd_write.cc:612-640` 在读取完表头后直接以 `O_CREAT | O_TRUNC` 创建正式目标，后续数据错误再于 `776-779` 删除。执行期间目标已可见；检查与创建之间还有竞态，可截断并发创建的文件。需求 D-102/D-103/D-227 要求先写私有暂存文件，全部 flush/close 成功后再以 no-replace 原子提交。

### [P2] `write` 的 DATE 词法不够严格

`cpp/tools/commands/cmd_write.cc:153-170` 使用 `sscanf("%4d-%2d-%2d%c")`；字段宽度是最大宽度，不要求月、日恰好两位，因此 `2024-1-1` 可被接受。需求 D-051 要求严格 `YYYY-MM-DD`。

### [P2] `write` 的类型关键字接受大小写变体

`cpp/tools/format/input_format.cc:67-69,92-128` 通过会规范化大小写的 `common::parse_data_type_name` 解析类型，因此 `--field s1 int64` 会被接受；`cpp/test/tools/input_format_test.cc` 还把小写 category/type 作为合法输入。需求 D-080 要求类型参数只接受规范大写名称。

### [P1] tree 模式 `stats` / `count` 没有在统计缺失或不可靠时扫描补算

`cpp/tools/commands/statistics.cc:165-177` 在 `get_statistic()` 返回 null 时只构造全空值行；现有 statistic 也未经可靠性判断直接使用。`cmd_stats` 随后把有 count 的行标为 `statistics`，`cmd_count` 同样没有 scan 路径。需求 D-178/D-183/D-184/D-198 要求缺失或不可靠时扫描完整作用域，失败则整个命令失败。

### [P1] `stats` 的值统计矩阵和 NDJSON 类型不符合契约

`cpp/tools/commands/statistics.cc:75-95` 为 tree 的 INT64/DATE/TIMESTAMP 输出了 `sum`，但 D-185 要求三者为 null；对应单测 `cpp/test/tools/statistics_test.cc:26-38` 还固化了错误预期。table 扫描中 `cpp/tools/commands/cmd_stats.cc:85-105` 把 DATE 视为 numeric，却没有读取 DATE 的数值，导致 DATE min/max 比较恒用 0。两种模式又把动态的 min/max/first/last/sum 列统一声明为 STRING（`cmd_stats.cc:368-381,432-440`），使 NDJSON 中 INT32/FLOAT/DOUBLE/BOOLEAN 值被错误加引号，违反 D-185/D-199/D-221。

### [P1] `schema` 没有展开列的实际多组物理参数

`cpp/tools/commands/cmd_schema.cc:49-89,118-153` 每个列最多输出一行，只取 table/measurement schema 上的一组 encoding/compression；没有遍历文件中的 Chunk 并按首次出现顺序去重。因此同一列存在多组物理组合时结果会丢失信息，违反 D-172；相关测试也只覆盖单一组合。

### [P1] stdout 写入和序列化失败不会转换成退出码 `3`

`cpp/tools/format/output_format.cc:293-413` 的 `RowWriter` 写入后不检查 stream 状态，`finish()` 也返回 void；`cpp/tools/format/result_set_format.cc:75-114` 最终只返回 reader 的迭代状态。因此 EPIPE、短写或其他 ostream 错误可能仍返回 `0`，也没有立即停止读取。需求 D-122 及各命令错误表要求捕获 stdout 交付失败并返回 `3`。

### [P1] `cat` / `export` 没有满足大结果流式处理边界

`cpp/tools/format/output_format.cc:312-317,370-413` 会为 table 格式缓存全部行后再排版；`cpp/tools/commands/cmd_export.cc:314-346` 对所有格式先把完整结果写入 `std::ostringstream`，再一次性写目标，多对象模式还再次扫描完整字符串计算行数。结果内存随导出数据量线性增长，违反 D-216 的流式要求。

### [P1] `write` 的结构与名称校验不完整

`cpp/tools/cli/run_cli.cc:251-263` 只检查 `columns` 非空，所以只有 TAG、没有 FIELD 的结构也会通过；`cpp/tools/format/input_format.cc:92-128` 不限制 TAG 必须为 STRING，只按大小写敏感方式检查重复列，也不拒绝任意大小写的保留名 `time`。`cpp/tools/commands/cmd_write.cc:322-370` 虽检测表头大小写冲突，却按原字符串查找声明列，无法完成表头与 table/TAG/FIELD 的 ASCII 大小写不敏感映射。此外没有合法 UTF-8 和控制字符名称校验。上述行为违反 D-062/D-072/D-074/D-078/D-091/D-092。

### [P2] `write` 的严格 CSV 输入契约还有多处缺口

`cpp/tools/commands/cmd_write.cc:687-691` 静默跳过空逻辑记录，违反 D-089；`cpp/tools/format/input_format.cc:131-145` 接受 `1/0` 以及任意大小写 BOOLEAN，而 D-051 要求规范小写文本；输入没有合法 UTF-8 检查，也没有移除文档允许的单个开头 UTF-8 BOM。错误位置只维护物理行累计值，没有同时报告一基逻辑记录号和多行记录范围，未满足 D-088/D-097。

## 已讨论但不再作为当前问题

### 同时包含 tree 和 table 结构的文件是否必须拒绝

先前按文档原文提出过，但你明确说明产品意图是“有表优先，否则按树”，因为 table 是 tree 上的增量配置。后续以这个产品语义为准，不再作为实现缺陷记录。

### `head` / `cat` 在树模型多 device 文件上默认扫全文件

早先版本存在该问题；当前快照的 `cpp/tools/commands/row_query.cc` 已经在多 device 且未指定 `-d` 时返回 usage error。

### `export` 混用 `-d` 和 `-t` 未被拒绝

早先版本存在该问题；当前快照的 `cpp/tools/cli/run_cli.cc` 已在通用参数校验里拒绝 `-d/--device` 与 `-t/--table` 同时出现。

### 顶层 `--help` / 子命令 `--help` 的独立性问题

当前已有测试覆盖，不再作为缺陷记录。

### `stats` 的 NDJSON 数字引号问题

先前讨论的是 count/time 等 64 位数量应按字符串输出，这部分实现正确；值级统计的动态类型问题仍成立，已单独记录在上文。

## 验证状态

- 已逐项检查 PR 相对 `origin/develop` 修改的 CLI 参数、命令实现、格式化层、测试和配套 Skill 文档。
- GitHub 上 PR 915 当前公开 CI 全部通过，包括 C++/Java/Python 分析、跨语言 fixture 验证及各平台单元测试。
- 本轮未在本机重新构建；当前 worktree 没有现成测试二进制。CI 通过只说明现有测试通过，其中若干测试本身固化了与需求不一致的行为，不能消除上述评审问题。
