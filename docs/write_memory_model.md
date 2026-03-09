# TsFile 写入内存模型与 Tablet 大小推导

## 背景

TsFile 在嵌入式/受限内存场景下运行时，峰值堆内存由两类性质截然不同的数据决定：

| 类别 | 特征 | 说明 |
|------|------|------|
| 数据内存 `M_data` | **周期性** —— 每次 flush 后释放并重用 | Tablet 缓冲区 + ChunkWriter 缓冲区 |
| 元数据内存 `M_meta` | **单调递增** —— 累积到 `close()` 才写入文件 | 每次 flush 后在堆上保留的 `chunk_meta` / `chunk_group_meta` |

因此总峰值为：

```
M_total(R) = M_init + M_data(R) + M_meta(N_flush)
```

其中 `R` 为每次 tablet 写入的行数，`N_flush = ⌈total_rows / R⌉`。

---

## 内存各项的计算公式

### 1. 基线开销 `M_init`

程序启动、完成 Schema 注册后的初始堆占用，与写入参数无关。

```
M_init ≈ 900 KB   （实测值，嵌入式平台可能偏低）
```

### 2. 数据内存 `M_data(R)`

每行数据在内存中存在于两个地方：

```
row_bytes_tablet      = 8 + Σ sizeof(value_col_i)   // Tablet：共享时间戳 + 所有列值
row_bytes_chunkwriter = 取决于写入模式（见下）
```

| 写入模式 | ChunkWriter 每行字节 | 说明 |
|----------|----------------------|------|
| **Table 模式**（对齐序列） | `8 + Σ sizeof(value_col_i)` | 时间列独立存储一次，值列各自存储 |
| **Tree 模式**（非对齐） | `N_col × (8 + avg_value_bytes)` | 每条序列各自携带时间戳 |

```
A = row_bytes_tablet + row_bytes_chunkwriter   // 每行的总驻留内存代价
M_data(R) = A × R
```

> **注**：`M_data` 在每次 flush 后被释放，下一个 Tablet 写入时重新申请。因此它是**固定的上限**，不随写入进度增长。

### 3. 元数据内存 `M_meta(N_flush)`

每次 flush 在堆上留下如下结构（直到 writer.close()）：

```
每次 flush 新增字节 B = N_devices_per_flush × (N_col × 104 + 96)
```

| 结构 | 大小 | 生命周期 |
|------|------|----------|
| `chunk_meta` | 104 bytes × N_col | writer.close() 时序列化到文件末尾 |
| `chunk_group_meta` | 96 bytes | writer.close() 时序列化 |

```
N_flush = ⌈total_rows / R⌉
M_meta  = N_flush × B
```

---

## 最优 Tablet 大小的推导

### 优化目标

最小化峰值内存 `M_total(R)`（`M_init` 为常量，忽略）：

```
min  f(R) = A·R + (total_rows / R)·B
```

这是经典的调和–线性权衡问题。对 `R` 求导并令其为零：

```
df/dR = A - total_rows·B / R² = 0

     ┌─────────────────────────┐
     │  R* = √(total_rows·B/A) │   ← 最优 Tablet 行数
     └─────────────────────────┘
```

此时达到最低可能峰值：

```
M_min = M_init + 2·√(total_rows · A · B)
```

> **直觉**：`R` 太大 → `M_data` 过高；`R` 太小 → flush 次数多、`M_meta` 过高。
> `R*` 恰好让两者相等：`A·R* = (total_rows/R*)·B`。

### 给定内存上限求可行域

令 `M_avail = memory_limit - M_init`，可行条件 `f(R) ≤ M_avail` 化为：

```
A·R² − M_avail·R + total_rows·B ≤ 0

       M_avail ± √(M_avail² − 4·A·total_rows·B)
R ∈ [ ─────────────────────────────────────────── ]
                      2·A
```

**可行的充要条件**：`M_avail ≥ 2·√(total_rows·A·B)`，即预算不低于 `M_min`。

---

## 实验验证

以 PDF 实测数据对照（50 devices × 50 measurements × int32，总行数 100,000/device）：

| `max_rows` | flush 次数 | M_data（模型） | M_meta（模型） | M_total（模型） | 实测 | 误差 |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 10,000 | 10 | 7.71 MB | 2.53 MB | 11.07 MB | 11.7 MB | 5.4% |
| 8,000  | 13 | 6.16 MB | 3.28 MB | 10.28 MB | 10.8 MB | 5.1% |
| 6,000  | 17 | 4.62 MB | 4.29 MB | 9.75 MB  | 10.6 MB | 8.7% |
| 5,000  | 20 | 3.85 MB | 5.05 MB | 9.74 MB  | 11.2 MB | 13%  |

最优 `R* = √(100,000 × 264,800 / 808) ≈ 5,725 rows`，对应最低理论峰值 ≈ 9.67 MB，与图表吻合。

> 误差来源：OS 内存块对齐分配、字符串变长开销、list 结构 node 开销（Massif 统计实际分配而非请求量）。

---

## C++ API 使用方法

```cpp
#include "writer/tsfile_writer.h"

// 示例：写入 500,000 行，内存预算 32 MB，10 个 field 列（double 类型）
TableSchema schema("sensors", {
    ColumnSchema("device_id", TSDataType::STRING, ColumnCategory::TAG),
    ColumnSchema("temp",      TSDataType::DOUBLE, ColumnCategory::FIELD),
    ColumnSchema("pressure",  TSDataType::DOUBLE, ColumnCategory::FIELD),
    // ... 更多列
});

WriteMemoryPlan plan = TsFileWriter::plan_write_memory(
    500'000,          // total_rows
    32 * 1024 * 1024, // memory_limit_bytes = 32 MB
    schema,
    1,                // n_devices_per_flush（每个 tablet 包含 1 个 device）
    true              // table_mode
);

if (!plan.feasible) {
    // 预算不足，plan.min_peak_bytes 是最低需求
    printf("Budget too small! Minimum required: %lld bytes\n",
           plan.min_peak_bytes);
}

printf("Recommended tablet rows : %lld\n", plan.recommended_tablet_rows);
printf("Expected flush count    : %lld\n", plan.flush_count);
printf("Estimated peak memory   : %.2f MB\n",
       plan.peak_memory_bytes / 1048576.0);
printf("  data  (cyclic)        : %.2f MB\n",
       plan.data_memory_bytes  / 1048576.0);
printf("  meta  (cumulative)    : %.2f MB\n",
       plan.meta_memory_bytes  / 1048576.0);

// 按推荐大小写入
TsFileWriter writer;
writer.open("output.tsfile");
writer.register_table(std::make_shared<TableSchema>(schema));

int64_t cur_row = 0;
while (cur_row < total_rows) {
    int64_t batch = std::min(plan.recommended_tablet_rows,
                             total_rows - cur_row);
    Tablet tablet(schema.get_table_name(), batch);
    // ... 填充 tablet ...
    writer.write_table(tablet);
    writer.flush();
    cur_row += batch;
}
writer.close();
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `total_rows` | `int64_t` | 计划写入的总行数 |
| `memory_limit_bytes` | `int64_t` | 峰值内存预算（字节）；传 `0` 则仅返回最优值 |
| `schema` | `const TableSchema&` | 已注册的表 Schema，用于推算列宽 |
| `n_devices_per_flush` | `int32_t` | 每次 tablet 写入涉及的 device 数（默认 1） |
| `table_mode` | `bool` | `true` = table 模式（对齐），`false` = tree 模式（非对齐） |
| `m_init_bytes` | `int64_t` | 基线堆开销（默认 900 KB，嵌入式可实测后传入） |

### 返回值 `WriteMemoryPlan`

| 字段 | 说明 |
|------|------|
| `recommended_tablet_rows` | 推荐的 tablet 行数 |
| `flush_count` | 预期 flush 次数 |
| `peak_memory_bytes` | 峰值内存估算（bytes） |
| `data_memory_bytes` | 数据部分（周期性，每 flush 后释放） |
| `meta_memory_bytes` | 元数据部分（累积到 close） |
| `init_memory_bytes` | 基线开销 |
| `feasible` | `false` 表示预算不足 |
| `min_peak_bytes` | 最低可能峰值（在 `R*` 处） |

---

## 误差说明

模型误差约 5–13%，主要来源：

1. **OS 内存块对齐**：`malloc` 实际分配可能大于请求量。
2. **变长字段**：STRING / TEXT / BLOB 列的实际长度可能偏离估算值（默认 16 字节）。
3. **容器开销**：`std::list` node、`std::string` SSO 等数据结构自身的元数据。
4. **整除余量**：当 `total_rows % tablet_rows ≠ 0` 时，最后一批实际行数偏少但元数据仍按完整块计。

实际部署时建议将预算留 15–20% 的余量：`memory_limit_bytes = target_usage * 0.85`。
