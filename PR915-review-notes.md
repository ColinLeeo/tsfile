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

结论：当前实现尚未满足需求文档，合入前应按下列建议修改代码并补充测试。其中 `[P1]` 表示会造成主要命令语义错误、结果不可靠、交付不安全或无法处理大文件；`[P2]` 表示边界契约或严格输入校验不符合设计。

## 当前仍成立的问题



### [P1] 多对象 `export` 的失败清理不完整

`cpp/tools/commands/cmd_export.cc:298-333` 先提交编号文件，再更新 `_manifest.json`。编号文件成功但 Manifest 更新失败时直接返回，没有删除未记录编号文件，也没有在清理失败时报告路径。应记录本次新提交但尚未写入 Manifest 的编号文件；Manifest 提交失败时逐一删除这些文件，清理成功后返回原始失败码，清理失败则返回 `3` 并在 stderr 列出所有残留路径。Manifest 自身也应复用原子输出 helper，避免留下半写文件。



### [P1] `stats` 的 tree 模式输出结构和统计口径不完整

`cpp/tools/commands/cmd_stats.cc:432-457` 把 `data_type` 固定写成 null，把 `null_count` 固定写成 `0`，并直接把 series statistic 的 count 当作 `non_null_count`，因此 tree 模式无法输出真实类型和空值数量。应从 measurement schema 取得 `data_type`，以 device 的逻辑行总数为分母，用 `null_count = row_count - non_null_count` 计算空值，并为零行、全空列和不同 FIELD 时间点不一致的情况补充测试。


### [P1] `count` 的 tree 模式统计口径不对

`cpp/tools/commands/cmd_count.cc:245-253` 把每个序列的统计点数同时当成 device 的 `row_count` 和该列的 `non_null_count`，并把 `null_count` 固定为 `0`。不同 FIELD 时间点不一致时，各行会得到不同的作用域行数。应先按 device 计算所有 FIELD 时间戳的并集作为统一 `row_count`，每列使用自身统计点数作为 `non_null_count`，再计算 `null_count = row_count - non_null_count`；如果底层没有能精确给出时间戳并集的元数据，只扫描时间列/索引补算，不重复解码 FIELD 值。

### [P1] `stats` / `count` 在多表文件上仍然强制要求 `-t`

`cpp/tools/commands/cmd_stats.cc:225-238` 和 `cpp/tools/commands/cmd_count.cc:85-99` 在表数量不等于 1 时直接返回参数错误，帮助文本也把省略作用域限制为单对象。应在未指定 `-t` 或 `-d` 时遍历文件中的全部 table 或全部 tree device，按 `ls` 的对象顺序分别输出统计结果；显式指定作用域时仍只处理该对象。


### [P2] 表模型列名投影仍使用字面匹配

`cpp/tools/commands/cmd_schema.cc:68-72`、`cpp/tools/commands/cmd_stats.cc:219-222,274-285` 和 `cpp/tools/commands/cmd_count.cc:125-135` 在 reader 查询前按原字符串比较 `-m`，因此不会受 reader 的小写规范化保护，`S1` 可能在上层被判定为不存在。应对 table 模型的 TAG/FIELD 名执行 ASCII 小写规范化后匹配，并映射回 schema 中的规范名称用于输出；tree 模型继续按其既有名称规则处理。


### [P2] `schema` 对不存在的作用域或列静默成功

`cpp/tools/commands/cmd_schema.cc:49-89,94-157` 只在输出循环中筛选 table/device/column，没有验证请求是否命中，导致 `schema -t missing`、`schema -d missing` 或 `schema -m missing` 可能只输出表头并返回 `0`。应在创建 writer 和输出表头前验证 table/device 存在，并确认每个显式 `-m` 至少命中一个合法列；任一请求未命中时向 stderr 报告具体对象或列并返回 `1`，stdout 保持为空。


### [P2] tree 模式的 `stats` / `count` 对不存在的 device 或 measurement 静默成功

`cpp/tools/commands/statistics.cc:139-181` 在 tree 模式过滤不到元数据时返回空 rows，随后 `stats/count` 仍正常结束并返回 `0`。应在输出前区分“存在但没有数据的合法作用域”和“请求的 device 或 measurement 不存在”；后者返回参数错误 `1` 并指出未命中的名称，前者才输出对应的零值或空统计结果。


### [P1] `head` / `cat` / `export` 的对象和投影预校验不完整

table reader 对不存在的列会返回 `E_COLUMN_NOT_EXIST`，但 `cpp/tools/commands/row_query.cc` 当前把 query 的非零返回统一映射为执行失败 `3`，且 reader 不负责验证 `-m` 是否属于 FIELD。应在扫描前基于 reader schema 一次性校验对象、列存在性和 FIELD 类别，失败时返回参数错误 `1` 且 stdout 为空；tree 分支也应使用相同的预校验和退出码口径，只把真正的读取、解码或执行失败映射为 `2` 或 `3`。


### [P1] `meta.format_version` 使用编译期常量，不是文件实际版本

`cpp/tools/commands/cmd_meta.cc:30-40` 输出 `storage::VERSION_NUM_BYTE`，这是当前绑定库的格式常量，不是输入 TsFile 文件头中的实际格式版本，因此不同版本文件可能得到相同结果。按当前 `meta` 契约，应由 reader 暴露打开文件时读取并校验过的版本字节，`meta.format_version` 输出该值；版本不支持或文件头矛盾时返回 `2`。顶层 `--version` 仍单独输出 Maven/CMake 注入的 CLI 和绑定 TsFile 代码版本，不能与文件格式版本混用。


### [P1] `export` / `sketch -o` 的目标保护和原子替换不成立

<!-- tag-comment-scope-J-start mode="block" hash="sha256:074cc419072ff54ab82a4b67897a63b97c58f5e423319386664a23d0cc31e9df" -->
`cpp/tools/commands/cmd_export.cc:42-123` 使用会跟随链接的 `stat`、固定的 `<target>.tmp` 和普通 `rename`，没有检查源与目标是否为同一 inode，也没有拒绝符号链接、FIFO 等特殊目标；非 `--force` 的先检查再 rename 还存在并发覆盖竞态，flush/close 失败也未处理。应抽取统一的原子输出 helper：使用 `lstat` 校验目标类型和 same-file，创建同目录且唯一的私有临时文件，检查 write/flush/close，最后使用真正的 no-replace 提交；`--force` 只允许原子替换普通文件，失败时清理并报告残留临时路径。

<!-- tag-comment-scope-J-end mode="block" -->
<!-- tag-comment-thread-J
cmd10 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:32:15.935Z","body":"这里似乎有点复杂。 "}
cmd30 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:06.897Z","body":"回复 cmd10：确实涉及三层，但可以拆开实现：先用 `lstat` 拒绝链接和特殊目标并检查源/目标 inode；再用唯一、同目录的私有临时文件写入并检查 flush/close；最后使用真正的 no-replace 提交，`\u002d\u002dforce` 时只允许替换普通文件。多对象 Manifest 可以复用同一个原子文件 helper。复杂度主要在跨平台提交原语，语义本身可以集中到一个 helper 中。"}
-->

### [P1] `write` 不是“全部成功后再提交正式目标”的原子交付

`cpp/tools/commands/cmd_write.cc:612-640` 在读取完表头后直接以 `O_CREAT | O_TRUNC` 创建正式目标，后续数据错误再于 `776-779` 删除。执行期间目标已可见，检查与创建之间还有竞态，也可能截断并发创建的文件。应把 TsFile 写入目标同目录的唯一私有暂存文件，只有在全部 CSV 校验、writer flush/close 和文件 close 成功后才以 no-replace 原子提交；任一步失败都删除暂存文件且不得触碰既有正式目标，并为并发创建目标、close 失败和异常中止补充测试。

### [P2] `write` 的 DATE 词法不够严格

`cpp/tools/commands/cmd_write.cc:153-170` 使用 `sscanf("%4d-%2d-%2d%c")` 解析 DATE，字段宽度只表示最大宽度，因此会接受 `2024-1-1`。应先验证输入严格匹配十位 `YYYY-MM-DD` 结构，再解析并校验真实日历日期，包括闰年和各月天数；任何额外字符、缺少前导零或非法日期都应在写入前返回参数错误。






### [P1] `stats` 的值统计矩阵和 NDJSON 类型不符合契约

<!-- tag-comment-scope-N-start mode="block" hash="sha256:af01feb4f5978d69a4b6803f725a0f0dac2cd4f474b95643054f269592a4f034" -->
`cpp/tools/commands/cmd_stats.cc:85-105` 把 DATE 纳入 numeric 比较，却没有从 DATE 单元读取其 int32 天数值，导致 table 模式的 DATE min/max 比较恒使用 `0`；`cmd_stats.cc:368-381,432-440` 又把动态的 min/max/first/last/sum 列统一声明为 STRING，使 NDJSON 中的数值和 BOOLEAN 被错误加引号。应按底层 Statistic 实际提供的字段直接输出，不额外扫描补算或人为屏蔽 DATE/TIMESTAMP/INT64 的统计项；DATE 使用 int32 内部值参与比较并按 DATE 词法序列化，其他统计值在 RowSchema 中保留真实数据类型。统计对象不存在时按全空处理，底层统计读取失败时整个命令失败。

<!-- tag-comment-scope-N-end mode="block" -->
<!-- tag-comment-thread-N
cmd14 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:19:54.324Z","body":"但 D-185 要求三者为 null 这里为啥会要求三者为null呢？\ndate 不应该用numeric吗？ 这里有点不懂？"}
cmd34 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:08.963Z","body":"回复 cmd14：这里的“三者为 null”只指 `sum`，不是 min/max/first/last。DATE 可以按内部天数值做先后比较，所以 min/max 应正常计算；当前 table 扫描的问题恰好是把 DATE 纳入 numeric，却没有在 `numeric_value` 里读取 DATE，导致比较值恒为 0。DATE/TIMESTAMP 的求和缺少业务含义，INT64 求和还有溢出风险，所以 D-185 选择让三者的 `sum` 为 null；如果产品希望沿用底层 INT64 statistic 的 sum，需要重新明确溢出和 DATE/TIMESTAMP 语义。"}
cmd53 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:58:38.122Z","body":"date 内部应该是按照int 来处理的吧。 我想的是， 这里的底层是什么统计数据我们就返回什么统计数据， 如果底层没有， 那就不返回或者返回错误。 我们不会专门去扫描所谓的统计信息的。"}
cmd72 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:53.515Z","body":"回复 cmd53：是，底层 `DATE` 使用继承自 `Int32Statistic` 的 `DateStatistic`，`TIMESTAMP` 使用继承自 `Int64Statistic` 的统计对象，所以底层确实已经决定了可返回的统计字段。按你描述的口径，CLI 应只序列化 `get_statistic()` 实际提供的数据，不自行扫描补算；统计对象不存在时沿用前面确认的“全空”，底层读取失败则返回输入错误。这样还需要同步调整设计中强制扫描、固定禁用 INT64/DATE/TIMESTAMP sum 的规则。当前评审里独立存在的实现问题仍是 table 路径没有正确读取 DATE 的 int32 值，以及 NDJSON 把数值/布尔统计统一序列化成字符串。你这条是在进一步明确口径，没有直接表示认同当前正文，所以我先只回复，不改正文。"}
-->

<!-- tag-comment-scope-O-start mode="block" hash="sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" -->

<!-- tag-comment-scope-O-end mode="block" -->
<!-- tag-comment-thread-O
cmd15 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:22:57.897Z","body":"这里不用处理， 我们就只能汇报在schema 里面的类型， 毕竟我们可以按照chunk 进行数据编码压缩。 "}
cmd35 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:09.504Z","body":"回复 cmd15：收到。按你的产品定义，`schema` 只汇报 schema 层声明的 encoding/compression，不展开各 Chunk 的实际组合；Chunk 级物理信息交给 `sketch`。据此这条评审不再成立，设计里的 D-172/D-218 后续需要改为 schema 层口径。"}
cmd54 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:58:53.003Z","body":"同意"}
cmd73 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:54.033Z","body":"回复 cmd54：已修复。正文已撤销该实现问题，明确 `schema` 只报告 schema 层配置，Chunk 级信息交给 `sketch`。"}
-->

### [P1] stdout 写入和序列化失败不会转换成退出码 `3`

<!-- tag-comment-scope-P-start mode="block" hash="sha256:8aac79ac894169fbe310bc65370f85786c2149a6f4499191e02da7b61cd86510" -->
`cpp/tools/format/output_format.cc:293-413` 的 `RowWriter` 写入后不检查 stream 状态，`finish()` 也不返回状态，导致 EPIPE、短写或其他 ostream 错误可能仍返回 `0` 并继续读取。应让 `RowWriter::write/finish` 返回可传播的错误状态，每次写入后检查 ostream，在首次失败时停止 reader 遍历，并由命令入口统一映射为退出码 `3`；同时处理 SIGPIPE，避免进程直接以 `141` 结束。

<!-- tag-comment-scope-P-end mode="block" -->
<!-- tag-comment-thread-P
cmd16 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:23:29.069Z","body":"这个确实需要修一下。"}
cmd36 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:10.019Z","body":"回复 cmd16：同意。建议让 `RowWriter::write/finish` 返回状态，逐次检查 ostream，并在首次失败时停止 reader 遍历；入口层把该状态统一映射成退出码 `3`，同时处理 SIGPIPE，避免进程直接以 141 结束。"}
cmd55 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:59:05.609Z","body":"同意。 "}
cmd74 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:54.551Z","body":"回复 cmd55：已修复。正文已明确 stdout 状态检查、停止读取、退出码映射和 SIGPIPE 处理；代码仍待修改。"}
-->

### [P1] `cat` / `export` 没有满足大结果流式处理边界

<!-- tag-comment-scope-Q-start mode="block" hash="sha256:b077a393ae9ff88a3707255a431b180ee1ca605489f5517814d933b629e65a03" -->
`cpp/tools/format/output_format.cc:312-317,370-413` 会为 table 格式缓存全部行，`cpp/tools/commands/cmd_export.cc:314-346` 又对所有格式先写入完整 `std::ostringstream`，使内存随结果量线性增长。CSV 和 NDJSON 应逐行写入同目录临时输出文件，边写边计数，全部成功后原子提交；table 格式若需要全局列宽，可使用有界 spool 或两遍处理。多对象 Manifest 不应再次扫描完整字符串统计行数。

<!-- tag-comment-scope-Q-end mode="block" -->
<!-- tag-comment-thread-Q
cmd17 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:24:05.844Z","body":"问题在于 tsfile 不能追加写， 所以我们必须缓存。 "}
cmd37 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:10.513Z","body":"回复 cmd17：这里缓存的是导出的 CSV/NDJSON/可读文本，不是继续写 TsFile，所以不受 TsFile 不能追加写的限制。可以把 ResultSet 逐行写到同目录临时输出文件，全部成功后原子 rename；行数也可边写边计数。table 可读格式为了计算列宽可能需要额外 spool 或两遍处理，但不应让 CSV/NDJSON 也整体进入内存。因此这条问题对 `export` 仍成立。"}
cmd56 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:59:22.725Z","body":"同意。 "}
cmd75 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:55.072Z","body":"回复 cmd56：已修复。正文已明确导出文本可以流式写入临时文件，并保留 CSV/NDJSON 整体缓存的实现问题；代码仍待修改。"}
-->

### [P1] `write` 的结构与名称校验不完整

<!-- tag-comment-scope-R-start mode="block" hash="sha256:e53aca2f7d0d5ed7127eb7e4fa396463af2d0c306ca7ea421ad624d350e603b7" -->
`cpp/tools/cli/run_cli.cc:251-263` 只检查 `columns` 非空，因此只有 TAG、没有 FIELD 的结构也会通过；输入层也没有完整落实 TAG 类型、保留名、名称和表头映射规则。应在读取 CSV 前要求至少一个 FIELD，并限制 `--tag` 类型为 STRING；table、TAG、FIELD 和表头名称统一按 ASCII 小写规范化后检查保留名 `time`、跨类别重复与大小写冲突，再映射到规范 schema 名称。还应拒绝非法 UTF-8 和控制字符名称，并为每项首错添加测试。

<!-- tag-comment-scope-R-end mode="block" -->
<!-- tag-comment-thread-R
cmd18 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:00:13.738Z","body":"我们逐个来看：\n1. 只有tag，没有field 的不能过， 这种文件对应着写入都只创建了时间序列， 没有写入数据。 \n2. 这里应该按照string来解析， 在输入的数据中似乎没办法区分是否为string，保留名按照time 全小写。 表模型里面的列名都是自动转换成小写的。 \n3. 后面的也应该修一修"}
cmd38 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.002Z","body":"回复 cmd18：逐项确认：第一，只有 TAG 没有 FIELD 必须在读 CSV 前拒绝；第二，`\u002d\u002dtag` 声明的类型参数应要求 STRING，CSV 单元格按字符串词法解析即可，不需要从数据推断，名称先规范成小写后再检查 `time` 和跨类别重复；第三，UTF-8/控制字符、大小写冲突和表头映射等剩余校验也需要补齐。因此这条评审保留，但实现时可以按这三个子项拆分测试。"}
cmd57 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:59:45.966Z","body":"同意。 "}
cmd76 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:55.590Z","body":"回复 cmd57：已修复。正文已按三个确认项重写结构、TAG/名称和其余输入校验要求；代码仍待修改。"}
-->

### [P2] `write` 缺少 UTF-8 和 BOM 输入处理

<!-- tag-comment-scope-S-start mode="block" hash="sha256:664c7f1b7fe69187d1f9a745d62653ddc8c5ae9aeb95fa3eccff3f34939ffc2c" -->
`write` 输入路径没有验证合法 UTF-8，也没有移除文档允许的单个开头 UTF-8 BOM，可能把非法字节或带 BOM 的首列表头交给后续名称匹配。应在 CSV 解析入口验证完整输入的 UTF-8 字节序列，只允许并剥离文件起始处的一个 `EF BB BF`，其他位置的 BOM 或非法序列按输入错误返回，并确保失败时不提交正式 TsFile 目标。

<!-- tag-comment-scope-S-end mode="block" -->
<!-- tag-comment-thread-S
cmd19 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:02:09.928Z","body":"这里的话：\n1. 静默跳过空逻辑记录指的是跳过行还是跳过列？\n2. 这个允许， 可以接受1/0 或者大小写的boolean， 只要他声明的是boolean类型即可。\n3. 这个合法utf8 指的是什么检查？\n4. 这个可以举一个例子吗？"}
cmd39 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.478Z","body":"回复 cmd19：第一，静默跳过的是整条空逻辑记录，也就是空行，代码命中 `if (line.empty()) continue`，不是跳过列。第二，收到，BOOLEAN 按你确认的语义允许 `1/0` 和大小写变体，这个子项不再是问题。第三，合法 UTF-8 检查是验证输入字节序列能否解码，例如字节 `C3 28` 不是合法 UTF-8；开头的 `EF BB BF` 则是允许且应移除的 BOM。第四，位置例子：第二条逻辑记录是一个跨物理第 2-3 行的带引号字段，若第 4 列 `temp` 非法，期望诊断类似“logical record 2, physical lines 2-3, column temp (4)”，当前只会报告累计到的物理行 3。"}
cmd58 {"author":"colin","kind":"human","createdAt":"2026-08-25T08:01:53.341Z","body":"第一个可以跳过。 \n第二个同意。\n第三个也行。\n第四个这个会复杂一些吧，如果能实现当然也很好。 "}
cmd77 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:56.132Z","body":"回复 cmd58：已修复。正文已移除空记录和 BOOLEAN 两个问题，保留 UTF-8/BOM 为待修项，并把完整逻辑记录位置降为非阻塞的诊断改进建议。"}
-->

<!-- tag-comment-scope-T-start mode="block" hash="sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" -->

<!-- tag-comment-scope-T-end mode="block" -->
<!-- tag-comment-thread-T
cmd20 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:03:25.048Z","body":"我们的设计文档似乎没有禁止这件事情。"}
cmd40 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.961Z","body":"回复 cmd20：当前设计正文其实明确禁止：通用规则写着“存在多个对象时必须显式选择，否则返回 1”，`head` 章节也写着“存在多个对象时必须显式用 `-d` 或 `-t` 选择”，D-205 重复了同一规则。因此当前实现与现有文档一致，这条早期问题被移入“不再作为当前问题”是合理的。如果产品希望默认读取全部 device，需要先修改这几处设计，并重新定义多 device 结果的列合并和行顺序。"}
cmd59 {"author":"colin","kind":"human","createdAt":"2026-08-25T08:02:49.297Z","body":"但是我们的文档里面中间的例子又存在那种不输入对象的情况， 所以这里似乎是没有禁止？"}
cmd78 {"author":"codex","kind":"codex","createdAt":"2026-08-25T08:07:56.653Z","body":"回复 cmd59：这些省略对象的示例使用的是 `tree.tsfile`，文档在测试数据约定中明确它只有一个 device：`root.factory.d1`，因此按“唯一对象可省略”的现有规则是合法的，并不表示多 device 文件也能省略对象。正文“存在多个对象时必须显式选择”和 D-205 仍明确禁止多作用域默认读取。文档确实缺少一个多 device 的 `head/cat` 反例，容易让这里看起来含混；但你这条尚未明确要改成多 device 默认扫描，所以我先只解释，不改正文。"}
-->

## 验证状态

- 已逐项检查 PR 相对 `origin/develop` 修改的 CLI 参数、命令实现、格式化层、测试和配套 Skill 文档。
- GitHub 上 PR 915 当前公开 CI 全部通过，包括 C++/Java/Python 分析、跨语言 fixture 验证及各平台单元测试。
- 本轮未在本机重新构建；当前 worktree 没有现成测试二进制。CI 通过只说明现有测试通过，其中若干测试本身固化了与需求不一致的行为，不能消除上述评审问题。
