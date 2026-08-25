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
cmd21 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:02.269Z","body":"回复 cmd1：你说得对。`offset` 的数值本身应当合法，超过匹配结果时返回合法空结果，用户不需要预先知道总行数。下推方面，无时间等前置过滤时可以直接用 `queryByRow`；有过滤条件时必须确保 `offset` 作用于过滤后的逻辑行，能否下推取决于 reader 接口是否同时支持这些条件。按你确认的产品语义，这条评审不再成立，后续还应同步撤销 D-247 的越界报错约束。"}
cmd41 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:51:42.250Z","body":"同意，我们更新这个问题。 "}
-->

### [P1] 多对象 `export` 的失败清理不完整

`cpp/tools/commands/cmd_export.cc:298-333` 先提交编号文件，再更新 `_manifest.json`。编号文件成功但 Manifest 更新失败时直接返回，没有删除未记录编号文件，也没有在清理失败时报告路径。需求 D-240/D-249 要求正常失败清理未记录文件，清理失败返回 `3`。

<!-- tag-comment-scope-B-start mode="block" hash="sha256:a7eeeb36b06ffa5bdac8fe3c66731f1d5ddb202867be13d0cef3e91190d32713" -->
### [P1] `sketch` 仍是占位实现

`cpp/tools/commands/cmd_export.cc:349-363` 只输出硬编码边框、文件路径和模型，没有调用绑定版本的 `printSketch`，不包含 Page/Chunk/ChunkGroup 等物理结构。需求 D-136/D-204 要求内容、层级、顺序和文本格式原样遵循 `printSketch`。
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd2 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:11:27.346Z","body":"这里后面有这个pr：\nhttps://github.com/apache/tsfile/pull/838"}
cmd22 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:02.773Z","body":"回复 cmd2：确认。PR 838 当前仍为 OPEN，目标正是把 Java `TsFileSketchTool` 的能力迁入 C++ `tsfile-cli sketch`，并声明了大 metadata/index 区域的已知限制。它合入或被 PR 915 依赖后，这个占位问题可由 PR 838 处理；就 PR 915 当前快照本身而言，`sketch` 仍是占位实现。"}
cmd42 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:52:24.409Z","body":"同意，更新这里的描述，或者直接去掉。 "}
-->

### [P1] `stats` 的 tree 模式输出结构和统计口径不完整

<!-- tag-comment-scope-C-start mode="block" hash="sha256:518b2a08f95afae2f1e5e5d54e0fc36da5e38e5bc5844e5abe1202ba0eb7a43a" -->
`cpp/tools/commands/cmd_stats.cc:432-457` 把 `data_type` 固定写成 null，把 `null_count` 固定写成 `0`，并直接把 series statistic 的 count 当作 `non_null_count`。需求 D-178/D-194/D-222 要求真实类型，并以 device 逻辑行总数为分母计算空值。
<!-- tag-comment-scope-C-end mode="block" -->
<!-- tag-comment-thread-C
cmd3 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:12:00.449Z","body":"这里是留下了占位吗？"}
cmd23 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:03.282Z","body":"回复 cmd3：是的，这里实际留下的是占位值。代码没有 TODO 标记，但 `data_type` 直接写空，`null_count` 直接写 `0`，并没有从 schema 或 device 逻辑行数计算，所以行为上就是尚未完成的占位实现。"}
cmd43 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:52:55.082Z","body":"同意，我们后面会修改代码来实现。 "}
-->

### [P1] `count` 的 tree 模式统计口径不对

`cpp/tools/commands/cmd_count.cc:245-253` 把每个序列的统计点数同时当成 device 的 `row_count` 和该列的 `non_null_count`，并把 `null_count` 固定为 `0`。不同 FIELD 时间点不一致时，各行会得到不同的作用域行数，违反 D-187/D-198/D-222。

### [P1] `stats` / `count` 在多表文件上仍然强制要求 `-t`

<!-- tag-comment-scope-D-start mode="block" hash="sha256:4cc41770f9bc9390c8f8fc57d33b6fb385c73f18b921a4b9231b3c2c027bfb21" -->
`cpp/tools/commands/cmd_stats.cc:225-238` 和 `cpp/tools/commands/cmd_count.cc:85-99` 在表数量不等于 1 时直接返回参数错误；帮助文本也宣称省略作用域仅适用于单对象。需求 D-180/D-188/D-231/D-232 要求省略作用域时覆盖纯模型文件中的全部访问作用域。
<!-- tag-comment-scope-D-end mode="block" -->
<!-- tag-comment-thread-D
cmd4 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:13:22.801Z","body":"我们的评审文档里面应该是都不再要求了吧。 即便是全部表或者全部tree 都能正常显示。 "}
cmd24 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:03.801Z","body":"回复 cmd4：是的，当前设计是不指定作用域时也要覆盖全部 table 或全部 tree device。这里的评审指出的正是实现仍在多表时强制 `-t`，所以这条问题成立；不是要求用户重新显式选择，而是要求实现支持全部作用域输出。"}
cmd44 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:53:57.828Z","body":"同意"}
-->

### [P2] 表模型列名投影仍使用字面匹配

<!-- tag-comment-scope-E-start mode="block" hash="sha256:9fe049394c38dcc389e36b7494a61f4da937d0990d5bd1e6a8de34e83b63cd48" -->
`cpp/tools/commands/cmd_schema.cc:68-72`、`cpp/tools/commands/cmd_stats.cc:219-222,274-285`、`cpp/tools/commands/cmd_count.cc:125-135` 都用原始字符串比较 `-m`。table 名虽然被转为小写，TAG/FIELD 名没有按 ASCII 大小写不敏感规则解析并映射回 schema 规范名称，违反 D-031。
<!-- tag-comment-scope-E-end mode="block" -->
<!-- tag-comment-thread-E
cmd5 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:18:42.621Z","body":"这里是否转换都无所谓， 底层的reader 会转换， 除非他在上层再次处理才会有问题。 "}
cmd25 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:04.306Z","body":"回复 cmd5：reader 查询路径确实会把表列名转成小写，所以 `head/cat` 直接交给 reader 的列名不受影响。但这里列出的 `schema/stats/count` 都在 reader 查询前用 `std::find`、`set` 等按原字符串自行筛选和校验，因此会在上层把 `S1` 当成不存在。问题只成立于这些上层再次处理的路径。"}
-->

### [P2] `schema` 对不存在的作用域或列静默成功

<!-- tag-comment-scope-F-start mode="block" hash="sha256:47e1b693a0238fd518b13e99509ef923cf770e328fcdc4b440910c8828b9ae7d" -->
`cpp/tools/commands/cmd_schema.cc:49-89,94-157` 只在输出循环中筛选 table/device/column，不验证请求对象是否命中。`schema -t missing`、`schema -d missing` 或 `schema -m missing` 都可能只输出表头并返回 `0`。需求 D-174/D-175 要求不存在对象或列返回 `1`。
<!-- tag-comment-scope-F-end mode="block" -->
<!-- tag-comment-thread-F
cmd6 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:25:14.992Z","body":"这里确实需要改一下"}
cmd26 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:04.837Z","body":"回复 cmd6：同意。这里需要在输出前分别确认请求的 table/device 存在，并确认每个 `-m` 至少命中一个合法列；未命中时返回参数错误，而不是空表头加成功状态。"}
-->

### [P2] tree 模式的 `stats` / `count` 对不存在的 device 或 measurement 静默成功

<!-- tag-comment-scope-G-start mode="block" hash="sha256:bcc208048fdf82e687cfe949dcdd8b8cb4e0b20defa198dea2f56b83494ad9db" -->
`cpp/tools/commands/statistics.cc:139-181` 过滤不到元数据就返回空 rows，随后两个命令正常 finish 并返回 `0`。需求 D-181/D-182/D-189/D-190 要求请求名称完全不存在时返回 `1`。
<!-- tag-comment-scope-G-end mode="block" -->
<!-- tag-comment-thread-G
cmd7 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:25:27.126Z","body":"同样需要处理。 "}
cmd27 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:05.354Z","body":"回复 cmd7：同意。tree 的 `stats/count` 也需要区分“合法空作用域”和“请求对象或 measurement 不存在”，后者应在输出前返回参数错误。"}
-->

### [P1] `head` / `cat` / `export` 的对象和投影预校验不完整

<!-- tag-comment-scope-H-start mode="block" hash="sha256:8bd3b715bac1f7afab0562f515bba3f0658a26d53296157c8a09fb098f4a7443" -->
`cpp/tools/commands/row_query.cc:165-199` 直接把 table `-m` 交给 reader，没有先验证列存在且类别为 FIELD；tree 分支在对象或 FIELD 过滤为空时于 `221-224` 返回执行失败 `3`。需求要求对象不存在、列不存在或类别不匹配在扫描前返回参数错误 `1`。
<!-- tag-comment-scope-H-end mode="block" -->
<!-- tag-comment-thread-H
cmd8 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:26:54.390Z","body":"这里确实应该验证一下是否是reader的schema， 如果查询了非法的column， 会返回什么呢？ 我记得内部会直接报告notexist？内部肯定是严格的。 "}
cmd28 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:05.886Z","body":"回复 cmd8：底层确实是严格的。`TableQueryExecutor` 会把列名转小写，找不到时返回 `E_COLUMN_NOT_EXIST`。但当前 `run_row_query` 把所有 query 非零返回统一映射为退出码 `3`，而不是参数错误 `1`；并且 reader 只检查列存在，不会替 CLI 检查 `-m` 是否为 FIELD，TAG 列也可能被接受。因此 CLI 仍需先基于 reader schema 做存在性和类别校验。"}
-->

### [P1] `meta.format_version` 使用编译期常量，不是文件实际版本

<!-- tag-comment-scope-I-start mode="block" hash="sha256:1ccff6064abc93ec348d76f3852732b86e3d1fd11eda48b10b7d3e30ecfabbbb" -->
`cpp/tools/commands/cmd_meta.cc:30-40` 输出 `storage::VERSION_NUM_BYTE`，这是当前绑定库的格式常量，不是当前输入文件读取到的版本。需求 D-177/D-226 要求返回文件实际版本，并在版本信息无法识别或矛盾时返回 `2`。
<!-- tag-comment-scope-I-end mode="block" -->
<!-- tag-comment-thread-I
cmd9 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:27:48.064Z","body":"这部分版本应该是获取外部的tsfile 版本， 现在tsfile 版本应该是又mvn给定的 ，这里仿照对应的逻辑即可。 "}
cmd29 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:06.390Z","body":"回复 cmd9：这里需要区分两种版本。`\u002d\u002dversion` 中的 `tsfile=\u003ctsfile-version>` 是 Maven/CMake 注入的绑定库版本；`meta.format_version` 是输入 TsFile 文件头中的格式版本字节。当前 reader 没有公开该值的 accessor，所以不能直接照搬 Maven 版本，需要从 reader/file metadata 暴露已读取的文件格式版本；否则会把“库版本”和“文件版本”混为一项。"}
-->

### [P1] `export` / `sketch -o` 的目标保护和原子替换不成立

<!-- tag-comment-scope-J-start mode="block" hash="sha256:df8c0f2d700f4afbc75a6b300095073ab0edce3d575d71bec6fa05484699096e" -->
`cpp/tools/commands/cmd_export.cc:42-123` 使用会跟随链接的 `stat`、固定的 `<target>.tmp` 和普通 `rename`。它没有检查目标与源 TsFile 是否为同一 inode，没有拒绝符号链接/FIFO 等特殊目标，可能截断用户已有的 `.tmp` 文件；非 `--force` 的“先检查再 rename”还有竞态，目标并发出现时 Unix `rename` 会直接替换。close/flush 失败也未检查。需求 D-120/D-204/D-223 要求 same-file 保护、特殊目标拒绝和真正的 no-replace/原子提交。
<!-- tag-comment-scope-J-end mode="block" -->
<!-- tag-comment-thread-J
cmd10 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:32:15.935Z","body":"这里似乎有点复杂。 "}
cmd30 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:06.897Z","body":"回复 cmd10：确实涉及三层，但可以拆开实现：先用 `lstat` 拒绝链接和特殊目标并检查源/目标 inode；再用唯一、同目录的私有临时文件写入并检查 flush/close；最后使用真正的 no-replace 提交，`\u002d\u002dforce` 时只允许替换普通文件。多对象 Manifest 可以复用同一个原子文件 helper。复杂度主要在跨平台提交原语，语义本身可以集中到一个 helper 中。"}
-->

### [P1] `write` 不是“全部成功后再提交正式目标”的原子交付

`cpp/tools/commands/cmd_write.cc:612-640` 在读取完表头后直接以 `O_CREAT | O_TRUNC` 创建正式目标，后续数据错误再于 `776-779` 删除。执行期间目标已可见；检查与创建之间还有竞态，可截断并发创建的文件。需求 D-102/D-103/D-227 要求先写私有暂存文件，全部 flush/close 成功后再以 no-replace 原子提交。

### [P2] `write` 的 DATE 词法不够严格

<!-- tag-comment-scope-K-start mode="block" hash="sha256:92b0cfb0757bd758cfd3caac516ce06cc06e307bbf18a790abcf52d97a49931c" -->
`cpp/tools/commands/cmd_write.cc:153-170` 使用 `sscanf("%4d-%2d-%2d%c")`；字段宽度是最大宽度，不要求月、日恰好两位，因此 `2024-1-1` 可被接受。需求 D-051 要求严格 `YYYY-MM-DD`。
<!-- tag-comment-scope-K-end mode="block" -->
<!-- tag-comment-thread-K
cmd11 {"author":"colin","kind":"human","createdAt":"2026-08-25T04:34:30.271Z","body":"除了D-051 上面还有什么约束？ 我们应该忽略后续的约束记录。 "}
cmd31 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:07.396Z","body":"回复 cmd11：即使忽略附录里的后续 D-xxx 记录，正文“结果格式与序列化”已经规定 DATE 使用 `YYYY-MM-DD`，创建章节又规定 CSV 输入复用公共数据词法。因此固定两位月、日并不只依赖 D-051。当前 `%2d` 允许单数字段，确实比正文宽松；如果产品决定接受 `2024-1-1`，需要先同步放宽正文中的 DATE 词法。"}
-->

### [P2] `write` 的类型关键字接受大小写变体

<!-- tag-comment-scope-L-start mode="block" hash="sha256:bfb616cd8914b5f9ec6ef0fc81bec8e60c404df8090d23aba198e8addf2d0082" -->
`cpp/tools/format/input_format.cc:67-69,92-128` 通过会规范化大小写的 `common::parse_data_type_name` 解析类型，因此 `--field s1 int64` 会被接受；`cpp/test/tools/input_format_test.cc` 还把小写 category/type 作为合法输入。需求 D-080 要求类型参数只接受规范大写名称。
<!-- tag-comment-scope-L-end mode="block" -->
<!-- tag-comment-thread-L
cmd12 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:17:22.186Z","body":"这里可以接受小写"}
cmd32 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:07.896Z","body":"回复 cmd12：收到，按你确认的产品语义，类型关键字允许小写，当前实现无需因此修改，这条评审不再成立。设计正文和 D-080 中“只接受规范大写名称”的表述后续需要同步调整，避免验收口径冲突。"}
-->

### [P1] tree 模式 `stats` / `count` 没有在统计缺失或不可靠时扫描补算

<!-- tag-comment-scope-M-start mode="block" hash="sha256:1084d83a39a27b8f40dec46fd77533271da2a987c74f4dc7900f35930b3acdc7" -->
`cpp/tools/commands/statistics.cc:165-177` 在 `get_statistic()` 返回 null 时只构造全空值行；现有 statistic 也未经可靠性判断直接使用。`cmd_stats` 随后把有 count 的行标为 `statistics`，`cmd_count` 同样没有 scan 路径。需求 D-178/D-183/D-184/D-198 要求缺失或不可靠时扫描完整作用域，失败则整个命令失败。
<!-- tag-comment-scope-M-end mode="block" -->
<!-- tag-comment-thread-M
cmd13 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:18:17.114Z","body":"忽略需求D-xxx， 统计信息缺失就意味着全空"}
cmd33 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:08.407Z","body":"回复 cmd13：收到。按你确认的新语义，`get_statistic() == nullptr` 直接解释为该序列全空，不再要求扫描补算；在这个前提下，这条评审不成立。需要确保底层不会用 nullptr 同时表达“统计未加载、缺失或损坏”，否则三种状态会被误合并为全空。"}
-->

### [P1] `stats` 的值统计矩阵和 NDJSON 类型不符合契约

<!-- tag-comment-scope-N-start mode="block" hash="sha256:281030acea9e6c4b6508cb54e0388682a5ca9bbdbd4cc9b9b0e9856247c8ebd6" -->
`cpp/tools/commands/statistics.cc:75-95` 为 tree 的 INT64/DATE/TIMESTAMP 输出了 `sum`，但 D-185 要求三者为 null；对应单测 `cpp/test/tools/statistics_test.cc:26-38` 还固化了错误预期。table 扫描中 `cpp/tools/commands/cmd_stats.cc:85-105` 把 DATE 视为 numeric，却没有读取 DATE 的数值，导致 DATE min/max 比较恒用 0。两种模式又把动态的 min/max/first/last/sum 列统一声明为 STRING（`cmd_stats.cc:368-381,432-440`），使 NDJSON 中 INT32/FLOAT/DOUBLE/BOOLEAN 值被错误加引号，违反 D-185/D-199/D-221。
<!-- tag-comment-scope-N-end mode="block" -->
<!-- tag-comment-thread-N
cmd14 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:19:54.324Z","body":"但 D-185 要求三者为 null 这里为啥会要求三者为null呢？\ndate 不应该用numeric吗？ 这里有点不懂？"}
cmd34 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:08.963Z","body":"回复 cmd14：这里的“三者为 null”只指 `sum`，不是 min/max/first/last。DATE 可以按内部天数值做先后比较，所以 min/max 应正常计算；当前 table 扫描的问题恰好是把 DATE 纳入 numeric，却没有在 `numeric_value` 里读取 DATE，导致比较值恒为 0。DATE/TIMESTAMP 的求和缺少业务含义，INT64 求和还有溢出风险，所以 D-185 选择让三者的 `sum` 为 null；如果产品希望沿用底层 INT64 statistic 的 sum，需要重新明确溢出和 DATE/TIMESTAMP 语义。"}
-->

### [P1] `schema` 没有展开列的实际多组物理参数

<!-- tag-comment-scope-O-start mode="block" hash="sha256:d7363856ba19b5dfb7936757c9be5f4f1c1512bc109f94a8624c5715295c2df6" -->
`cpp/tools/commands/cmd_schema.cc:49-89,118-153` 每个列最多输出一行，只取 table/measurement schema 上的一组 encoding/compression；没有遍历文件中的 Chunk 并按首次出现顺序去重。因此同一列存在多组物理组合时结果会丢失信息，违反 D-172；相关测试也只覆盖单一组合。
<!-- tag-comment-scope-O-end mode="block" -->
<!-- tag-comment-thread-O
cmd15 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:22:57.897Z","body":"这里不用处理， 我们就只能汇报在schema 里面的类型， 毕竟我们可以按照chunk 进行数据编码压缩。 "}
cmd35 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:09.504Z","body":"回复 cmd15：收到。按你的产品定义，`schema` 只汇报 schema 层声明的 encoding/compression，不展开各 Chunk 的实际组合；Chunk 级物理信息交给 `sketch`。据此这条评审不再成立，设计里的 D-172/D-218 后续需要改为 schema 层口径。"}
-->

### [P1] stdout 写入和序列化失败不会转换成退出码 `3`

<!-- tag-comment-scope-P-start mode="block" hash="sha256:d43050ad88f0196246df0d4438bd7005f3eb9e8e430e4fa27ebfbb4829aea27b" -->
`cpp/tools/format/output_format.cc:293-413` 的 `RowWriter` 写入后不检查 stream 状态，`finish()` 也返回 void；`cpp/tools/format/result_set_format.cc:75-114` 最终只返回 reader 的迭代状态。因此 EPIPE、短写或其他 ostream 错误可能仍返回 `0`，也没有立即停止读取。需求 D-122 及各命令错误表要求捕获 stdout 交付失败并返回 `3`。
<!-- tag-comment-scope-P-end mode="block" -->
<!-- tag-comment-thread-P
cmd16 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:23:29.069Z","body":"这个确实需要修一下。"}
cmd36 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:10.019Z","body":"回复 cmd16：同意。建议让 `RowWriter::write/finish` 返回状态，逐次检查 ostream，并在首次失败时停止 reader 遍历；入口层把该状态统一映射成退出码 `3`，同时处理 SIGPIPE，避免进程直接以 141 结束。"}
-->

### [P1] `cat` / `export` 没有满足大结果流式处理边界

<!-- tag-comment-scope-Q-start mode="block" hash="sha256:29125f98f7cb68a6165e5d574e98931f85b51d36954b86be5aecf45f80532992" -->
`cpp/tools/format/output_format.cc:312-317,370-413` 会为 table 格式缓存全部行后再排版；`cpp/tools/commands/cmd_export.cc:314-346` 对所有格式先把完整结果写入 `std::ostringstream`，再一次性写目标，多对象模式还再次扫描完整字符串计算行数。结果内存随导出数据量线性增长，违反 D-216 的流式要求。
<!-- tag-comment-scope-Q-end mode="block" -->
<!-- tag-comment-thread-Q
cmd17 {"author":"colin","kind":"human","createdAt":"2026-08-25T06:24:05.844Z","body":"问题在于 tsfile 不能追加写， 所以我们必须缓存。 "}
cmd37 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:10.513Z","body":"回复 cmd17：这里缓存的是导出的 CSV/NDJSON/可读文本，不是继续写 TsFile，所以不受 TsFile 不能追加写的限制。可以把 ResultSet 逐行写到同目录临时输出文件，全部成功后原子 rename；行数也可边写边计数。table 可读格式为了计算列宽可能需要额外 spool 或两遍处理，但不应让 CSV/NDJSON 也整体进入内存。因此这条问题对 `export` 仍成立。"}
-->

### [P1] `write` 的结构与名称校验不完整

<!-- tag-comment-scope-R-start mode="block" hash="sha256:578b54cd49f4f53d62879b3fa0204f8749a5db21c2f873d78fd996688c91d268" -->
`cpp/tools/cli/run_cli.cc:251-263` 只检查 `columns` 非空，所以只有 TAG、没有 FIELD 的结构也会通过；`cpp/tools/format/input_format.cc:92-128` 不限制 TAG 必须为 STRING，只按大小写敏感方式检查重复列，也不拒绝任意大小写的保留名 `time`。`cpp/tools/commands/cmd_write.cc:322-370` 虽检测表头大小写冲突，却按原字符串查找声明列，无法完成表头与 table/TAG/FIELD 的 ASCII 大小写不敏感映射。此外没有合法 UTF-8 和控制字符名称校验。上述行为违反 D-062/D-072/D-074/D-078/D-091/D-092。
<!-- tag-comment-scope-R-end mode="block" -->
<!-- tag-comment-thread-R
cmd18 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:00:13.738Z","body":"我们逐个来看：\n1. 只有tag，没有field 的不能过， 这种文件对应着写入都只创建了时间序列， 没有写入数据。 \n2. 这里应该按照string来解析， 在输入的数据中似乎没办法区分是否为string，保留名按照time 全小写。 表模型里面的列名都是自动转换成小写的。 \n3. 后面的也应该修一修"}
cmd38 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.002Z","body":"回复 cmd18：逐项确认：第一，只有 TAG 没有 FIELD 必须在读 CSV 前拒绝；第二，`\u002d\u002dtag` 声明的类型参数应要求 STRING，CSV 单元格按字符串词法解析即可，不需要从数据推断，名称先规范成小写后再检查 `time` 和跨类别重复；第三，UTF-8/控制字符、大小写冲突和表头映射等剩余校验也需要补齐。因此这条评审保留，但实现时可以按这三个子项拆分测试。"}
-->

### [P2] `write` 的严格 CSV 输入契约还有多处缺口

<!-- tag-comment-scope-S-start mode="block" hash="sha256:9deaeb64f715082971e6ad6299b7dce9f7fe2df59af9b1ee34fe16126e96fd5a" -->
`cpp/tools/commands/cmd_write.cc:687-691` 静默跳过空逻辑记录，违反 D-089；`cpp/tools/format/input_format.cc:131-145` 接受 `1/0` 以及任意大小写 BOOLEAN，而 D-051 要求规范小写文本；输入没有合法 UTF-8 检查，也没有移除文档允许的单个开头 UTF-8 BOM。错误位置只维护物理行累计值，没有同时报告一基逻辑记录号和多行记录范围，未满足 D-088/D-097。
<!-- tag-comment-scope-S-end mode="block" -->
<!-- tag-comment-thread-S
cmd19 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:02:09.928Z","body":"这里的话：\n1. 静默跳过空逻辑记录指的是跳过行还是跳过列？\n2. 这个允许， 可以接受1/0 或者大小写的boolean， 只要他声明的是boolean类型即可。\n3. 这个合法utf8 指的是什么检查？\n4. 这个可以举一个例子吗？"}
cmd39 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.478Z","body":"回复 cmd19：第一，静默跳过的是整条空逻辑记录，也就是空行，代码命中 `if (line.empty()) continue`，不是跳过列。第二，收到，BOOLEAN 按你确认的语义允许 `1/0` 和大小写变体，这个子项不再是问题。第三，合法 UTF-8 检查是验证输入字节序列能否解码，例如字节 `C3 28` 不是合法 UTF-8；开头的 `EF BB BF` 则是允许且应移除的 BOM。第四，位置例子：第二条逻辑记录是一个跨物理第 2-3 行的带引号字段，若第 4 列 `temp` 非法，期望诊断类似“logical record 2, physical lines 2-3, column temp (4)”，当前只会报告累计到的物理行 3。"}
-->

## 已讨论但不再作为当前问题

### 同时包含 tree 和 table 结构的文件是否必须拒绝

先前按文档原文提出过，但你明确说明产品意图是“有表优先，否则按树”，因为 table 是 tree 上的增量配置。后续以这个产品语义为准，不再作为实现缺陷记录。

<!-- tag-comment-scope-T-start mode="block" hash="sha256:4086e667d25786e82175e8cb8162bf1fd478effb4269420f98f074bd94124321" -->
### `head` / `cat` 在树模型多 device 文件上默认扫全文件

早先版本存在该问题；当前快照的 `cpp/tools/commands/row_query.cc` 已经在多 device 且未指定 `-d` 时返回 usage error。
<!-- tag-comment-scope-T-end mode="block" -->
<!-- tag-comment-thread-T
cmd20 {"author":"colin","kind":"human","createdAt":"2026-08-25T07:03:25.048Z","body":"我们的设计文档似乎没有禁止这件事情。"}
cmd40 {"author":"codex","kind":"codex","createdAt":"2026-08-25T07:09:11.961Z","body":"回复 cmd20：当前设计正文其实明确禁止：通用规则写着“存在多个对象时必须显式选择，否则返回 1”，`head` 章节也写着“存在多个对象时必须显式用 `-d` 或 `-t` 选择”，D-205 重复了同一规则。因此当前实现与现有文档一致，这条早期问题被移入“不再作为当前问题”是合理的。如果产品希望默认读取全部 device，需要先修改这几处设计，并重新定义多 device 结果的列合并和行顺序。"}
-->

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
