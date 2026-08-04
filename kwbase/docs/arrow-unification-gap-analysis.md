# Arrow 统一化 —— 完整替代 rowexec / colexec 的差距分析（GAP ANALYSIS）

> 用途：分析当前 Arrow 统一化工作距「完整替代 rowexec 与 colexec 执行引擎」还缺哪些工作。
> 生成日期：2026-08-01。配套文档：`arrow-unification-handoff.md`（进度快照）、`arrow-unification-architecture.md`（完整设计演进）。
> 工作区路径说明：仓库为 GOPATH 模式（GO111MODULE=off），真实 module 为 `gitee.com/kwbasedb/kwbase`；`github.com/kwbasedb/kwbase` 仅为其软链。
>
> **范围约束**：本分析目标仅为**关系计算（EngineTypeRelational）的 Arrow 重构**，**不涉及时序引擎（EngineTypeTimeseries）**。
> `pkg/sql/arrow_unification.go:81 / :274` 对 Timeseries 的显式排除是正确的隔离行为，不在待办范围内，不应改动。

---

## 0. 结论速览

当前 4 类算子（投影 / 过滤 / 聚合 / 连接）的 Arrow 实现已落地并通过真实 SQL 端到端验证，**但只是「选择性替代」而非「完整替代」**：

- 仅在 4 个 opt-in 集群开关开启且 spec 被 `canArrow*` 判定为可计算时，才下发为 `ProcessorCoreUnion` 上的 Arrow core；其余所有物理算子**永不翻成 Arrow core**，天然走 rowexec/colexec。
- 时序引擎（Timeseries）按设计被显式排除（`arrow_unification.go:81 / :274`），不在本重构范围内。
- 排序、去重、窗口、集合运算、Limit/Offset 等关系计算高频算子尚无 Arrow 实现。

要「完整替代」，需补齐关系计算下未实现算子的覆盖、扩展已实现算子的能力边界，并将开关默认化 + 灰度。

---

## 1. 当前已完成底座

| 层 | 状态 |
|----|------|
| 4 类算子 Arrow 实现（投影/过滤/聚合/连接） | ✅ 已实现并通过真实 SQL 端到端验证 |
| 两阶段分布式聚合 + 两阶段 AVG merge | ✅ 已路由 Arrow |
| 原生 Arrow scan / 算子间 Arrow 链式 | ✅ 已点亮 |
| 聚合/连接计算核向量化（colexec 式选择子） | ✅ 对照 colexec 持平/更快（聚合 ~1.9×、连接 ~1.27×、全局 SUM ~1.9×） |
| opt-in 集群开关（`sql.arrow_{projection,filter,aggregator,join}.enabled`） | ✅ 4 个开关，**默认全关** |
| 执行侧分发（`pkg/sql/rowexec/processors.go:139-162` 的 `NewProcessor` switch） | ✅ 优先命中 Arrow core |

**回退安全性**：已实现算子的不支持 spec 会静默回退 colexec（`addTwiceAggregators` 构建失败不替换 core；`setupMultiAggFinalState` 以 `useArrow=false` 重走标准路径），不 panic。

---

## 2. 待办工作（按优先级）

### P0 — 补齐尚未实现 Arrow 的物理算子（最关键覆盖空白）

以下算子**完全没有 Arrow 实现**，目前只能走 rowexec/colexec 标准路径：

- **Sorter（排序）** — 几乎所有 `ORDER BY` 查询的下游，最高频算子
- **Distinct（去重）**
- **Merge-join / merge-union** 类合并算子
- **Window / Analytic 函数**
- **Limit / Offset**
- **Union / Intersect / Except**（集合运算）
- **Ordinality / Backfill** 等辅助算子

> 当前只有这 4 类算子被改写了 `ProcessorCoreUnion`，其余算子永不翻成 Arrow core —— 「替代」是选择性的。

### P1 — 已实现算子的能力边界扩展

- **聚合函数集**：已实现 SUM / MIN / MAX / COUNT / COUNT(*) / AVG / BOOL_AND / BOOL_OR / **STDDEV / VARIANCE**（含 SQRDIFF 本地阶段 + FINAL_VARIANCE/FINAL_STDDEV 合并阶段，见 §4.12）；缺 FIRST / LAST / JSON_AGG / CONCAT_AGG 等
- **字符串/标量函数**：投影支持 length / octet_length / substr / concat / lower / upper（native Go 核）+ **abs / sqrt / ln / sign / power**（复用 arrow/compute 自带 kernel，见 §8.4）；过滤仅 LIKE / ILIKE / CAST；缺 trim/replace/upper 之外的字符串函数及 round/floor/ceil 等（floor/ceil/round 的 arrow kernel 为 int→float 语义，与 SQL float 输入不符，暂未引入）
- **外连接非等值 onExpr**：post-filter 语义尚未完全等价（`TestArrowUnifyJoinNonEqui` 仅覆盖 inner）
- **JSON 类型族**：`pkg/sql/rowexec/arrow_adapter.go` 仍有边界（`unsupported type family JsonFamily for arrow unification`）
- **系统表扫描回归**：`sql.arrow_filter.enabled` 全局开会影响 `count-table-stream` 等内部扫描（handoff §5.10）

### P2 — 开关默认化与稳定性

- 4 个开关**默认关闭**；要「替代」需默认开启 + 长期灰度，需要：
  - 全量 `SELECT` 回归测试矩阵（各算子组合、NULL、DECIMAL 精度、乱序等）
  - 性能回归护栏（Arrow 某些路径可能慢于 colexec，需自动 fallback 阈值）
  - 错误处理从当前「硬 error」（`unsupported type family...`）改为 planner 侧预判 + 静默回退

### P3 — 工程化收尾

- **vendor 双前缀冲突**：必须从 `gitee.com/kwbasedb/kwbase` 路径编译，未根本解决，CI 易踩坑
- **bench 测试包**因 arrow vendor 版本冲突无法直接 `go test` 编译（既有问题）
- 缺统一「未实现算子 → 自动回退」注册表；目前是 per-算子散落的 `canArrow*` 判定

---

## 3. 建议推进路线

1. **补齐 Sorter + Distinct（P0）** —— 关系计算下 `ORDER BY` / `SELECT DISTINCT` 刚需，覆盖后能跑通绝大多数查询形态。
2. **扩展聚合/标量函数集（P1）** —— 提升已实现 4 类算子的 spec 覆盖率，减少回退。
3. **补齐其余关系算子（P0 续）** —— Window / Merge-join / Union/Intersect/Except / Limit-Offset 等。
4. **统一回退注册表 + 默认开关灰度（P2/P3）** —— 收尾工程化。

---

## 附录：关键代码定位

| 关注点 | 位置 |
|--------|------|
| 各算子 opt-in 开关注册 | `pkg/sql/physicalplan/physical_plan.go:58-129` |
| 投影分流 | `physical_plan.go:1294`（`AddRendering`） |
| 过滤（post 中）分流 | `physical_plan.go:2277` |
| 过滤（scan 折叠）分流 | `distsql_physical_planner.go:1820` / `:2161` |
| 聚合（本地 partial）分流 | `distsql_physical_planner.go:3906` |
| 聚合（最终 merge）分流 | `distsql_physical_planner.go:4563` |
| 连接分流 | `distsql_physical_planner.go:5508`（`canArrowJoin`） |
| 时序引擎排除 | `pkg/sql/arrow_unification.go:81` / `:274` |
| 执行侧统一分发 | `pkg/sql/rowexec/processors.go:139-162` |
| 未支持类型报错 | `pkg/sql/rowexec/arrow_adapter.go:334` / `:573`；`arrow_aggregate.go:82` / `:252` |
| 端到端测试包 | `pkg/sql/rowexec/arrowpilot/`（`TestArrowUnify*` 系列） |

---

## 4. P0 实施设计：Sorter 与 Distinct 的 Arrow 化

> 目标范围：仅 EngineTypeRelational。沿用已落地的 4 类算子衔接范式（proto `Expression` 字段 + JSON plan + rows↔Arrow Record 适配），不引入 proto 字段外的全新机制。

### 4.1 已确立的衔接范式（复用，不重写）

| 环节 | 位置 | 做法 |
|------|------|------|
| spec 下发 | `execinfrapb.ProcessorCoreUnion` 字段 `ArrowProjection/ArrowFilter/ArrowAggregator/ArrowJoin`（proto field 51-54，类型 `*Expression`） | JSON 化的 plan 装在 `Expression.Expr` |
| planner 判定 | `pkg/sql/arrow_unification.go` 的 `canArrow*` | 类型/表达式/引擎可计算性检查 |
| planner 序列化 | `arrowXxxCoreFor(...)` 返回 `ProcessorCoreUnion{ArrowXxx: &execinfrapb.Expression{Expr: json}}` | |
| executor 分发 | `pkg/sql/rowexec/processors.go:139-162` 的 `NewProcessor` switch | 优先命中 `core.ArrowXxx` |
| rows↔Arrow | `pkg/sql/rowexec/arrow_adapter.go` 的 `buildArrowColumns` / `arrowTypeForKWType`；`arrow_projection_processor.go` 的 `arrowRecordToEncDatumRows` | 输入批转 Record → 计算核 → Record 转股回 rows |
| 开关 | `pkg/sql/physicalplan/physical_plan.go:58-129` 的 4 个 `sql.arrow_*.enabled` + `arrowXxxEnabled()` helper | |

### 4.2 Sorter（排序）Arrow 化

**plan 结构体**（planner 侧，平行定义，JSON 序列化）：
```go
type arrowSortPlan struct {
    Ordering []arrowSortCol `json:"ordering"` // {col int, dir "asc"/"desc"}
    MatchLen int            `json:"match_len"` // 已排序前缀长度，可跳过
    Limit    int64          `json:"limit"`     // -1 表示无
    Offset   int64          `json:"offset"`    // 0 表示无
}
```

**planner 分流点**：`pkg/sql/distsql_physical_planner.go` 中创建 `SorterSpec` 处（搜 `SorterSpec{` / `ProcessorCoreUnion{Sorter`）与 `physical_plan.go` 的 sorter 构建。新增：
- `canArrowSort(spec, inTypes, engine)`：拒绝 Timeseries、拒绝不可 Arrow 化的 key 类型（复用 `arrowJoinKeyType` 之类）、仅支持 `matchLen==len(ordering)` 或完全无序两种（首期不做部分有序 fast-path），Limit/Offset 任意。
- `arrowSortCoreFor(spec)` 序列化为 `ProcessorCoreUnion{ArrowSorter: &Expression{Expr: json}}`。

**executor**（`pkg/sql/rowexec/arrow_sorter_processor.go` 新建）：
- 输入经 `buildArrowColumns` 转 Arrow Record（可多批缓冲）。
- 用 Arrow 计算核排序：优先 `arrowcompute.SortIndices` / `arrowcompute.SortRecords`（apache/arrow/go/v17 compute）；无原生核时回退为「在 KW 类型上 `sort.Slice` 用 `sqlbase.CompareEncDatum` 比较」（与 rowexec sorter 同语义，保证正确性）。
- 输出经 `arrowRecordToEncDatumRows` 转股回 rows，按 `matchLen` 跳过已排序前缀、`limit/offset` 截断。
- `newArrowSorterProcessor` 优先用 plan 声明的输出类型（同 ⑧-c 的 `OutTypes` 契约）。

**proto 改动**：在 `processors.proto` 的 `ProcessorCoreUnion` 增加 `ArrowSorter`（field 55，`*Expression`），重新生成 `processors.pb.go`（或手工补齐 Marshal/Unmarshal/Size 代码，仿 51-54 字段）。

**新增集群开关**：`sql.arrow_sorter.enabled`（仿 `sql.arrow_projection.enabled`），`physical_plan.go` 注册 + `ArrowSorterEnabled()` helper。

### 4.3 Distinct（去重）Arrow 化

**plan 结构体**：
```go
type arrowDistinctPlan struct {
    DistinctCols []int `json:"distinct_cols"` // 去重键列
    OrderedCols  []int `json:"ordered_cols"`  // 若非空，仅在每个 ordered 组内去重
    // 输出列 = 全部列（首期）；ordered-columns-only 模式后续扩展
}
```

**planner 分流点**：`pkg/sql/distsql_physical_planner.go` 创建 `DistinctSpec` 处（搜 `DistinctSpec{` / `ProcessorCoreUnion{Distinct}`）。新增：
- `canArrowDistinct(spec, inTypes, engine)`：拒绝 Timeseries、key 类型需 Arrow 可表示（同 `arrowJoinKeyType`）；NULL 视为相等（与现有语义一致）。
- `arrowDistinctCoreFor(spec)` 序列化。

**executor**（`pkg/sql/rowexec/arrow_distinct_processor.go` 新建）：
- 选项 A（向量化）：用 Arrow hash/字典去重——把 distinct key 列建成 `arrow.Dictionary` 或 `arrowcompute` 的 `Unique`/`Distinct` 核（v17 若不支持则手写 hash set，key 用 `EncodedKey` 或 `EncodeDatumsToKey`）。
- 选项 B（保守首期实现）：BufferedRow + `sqlbase.EncodeColumns` 做 hash，等价 rowexec 语义，先保证正确，再换向量化核。
- 输出去重后的 rows（保留全部列），经 `arrowRecordToEncDatumRows` 转股。

**proto 改动**：`ArrowDistinct`（field 56，`*Expression`），同 4.2 生成方式。

**新增集群开关**：`sql.arrow_distinct.enabled`。

### 4.4 执行侧分发补点

`pkg/sql/rowexec/processors.go:139-162` 的 `NewProcessor` switch 增加：
```go
case core.ArrowSorter != nil:
    return newArrowSorterProcessor(...)
case core.ArrowDistinct != nil:
    return newArrowDistinctProcessor(...)
```
位于现有 4 个 Arrow case 之后、colexec/rowexec 回退之前（保持「优先命中 Arrow」顺序）。

### 4.5 验证计划（仿 ⑧ 系列）

在 `pkg/sql/rowexec/arrowpilot/` 新增：
- `TestArrowSort`：`SELECT ... ORDER BY a [DESC], b LIMIT n OFFSET m;` 对照 colexec 输出一致。
- `TestArrowDistinct`：`SELECT DISTINCT ...` 对照 colexec 输出一致；含 NULL、乱序、多列。
- 在 `arrow_unification.go` 的 `TestArrowUnify*` 总集中补充 `ORDER BY` / `DISTINCT` 混合查询（投影+过滤+聚合+排序+去重的全链路验证）。

### 4.6 实施顺序建议

1. 加 proto 字段 55/56 + 重新生成 `processors.pb.go`。
2. planner：`canArrowSort`/`canArrowDistinct` + `arrow*CoreFor` + 2 个集群开关 + 分流点接入。
3. executor：`arrow_sorter_processor.go` / `arrow_distinct_processor.go` + `processors.go` switch 补点。
4. arrowpilot 测试对照 colexec 全 PASS。
5. 回退安全：`canArrow*` 不支持时静默回退 colexec（不 panic）；`arrow*CoreFor` 构建失败时回退标准 core（仿聚合器 `addTwiceAggregators` 模式）。

### 4.7 已完成的骨架修正（2026-08-01）

- **`processors.pb.go` 新增 `ProcessorCoreUnion.ArrowSorter`（field 55，`*Expression`）与 `ArrowDistinct`（field 57，`*Expression`）**，并在 Marshal/Size/Unmarshal 三处同步（手写，因重跑 protoc 会抹掉既有手加的 51-54 字段）。
- **修正既有 3 个 Arrow 字段的 wire tag 笔误**（原手写错误）：
  - `ArrowFilter`（52）原 `0xc203`（解码为 field 56）→ 修正为 `0xa203`（=field 52）
  - `ArrowAggregator`（53）原 `0xd203`（解码为 field 58）→ 修正为 `0xaa03`（=field 53）
  - `ArrowJoin`（54）原 `0xe203`（解码为 field 60）→ 修正为 `0xb203`（=field 54）
  - `ArrowProjection`（51，`0x9a03`）原本正确，未改。
  - 这三个笔误意味着**原 Arrow filter/aggregator/join core 在真实分布式 marshal/unmarshal 传输中无法被反序列化**（之前的 arrowpilot 测试直接构造 processor、绕过 wire，故未暴露）。修正后 6 个 core 均可正确跨节点传输。
- 新增 `execinfrapb/arrow_fields_roundtrip_test.go`（`TestArrowCoreUnionFieldsRoundTrip`）作为 wire 兼容回归保障，验证 6 个字段各自独立 round-trip 且无 tag 碰撞。

### 4.8 已完成：Sorter / Distinct 的 Arrow 化实现（2026-08-01）

在 4.1–4.6 的设计基础上已落地实现，并通过 `arrowpilot/TestArrowUnifySortDistinct` 端到端验证（排序：单列升/降序、多列混合方向、含 NULL；去重：单列、多列、含 NULL；聚合内 distinct 路径独立覆盖）。

**改动文件总览**

| 层 | 文件 | 改动 |
|----|------|------|
| proto 手写补点 | `pkg/sql/execinfrapb/processors.pb.go` | `ArrowSorter`(55) / `ArrowDistinct`(57) 字段 + 修正 52/53/54 tag 笔误（见 4.7） |
| 开关 | `pkg/sql/physicalplan/physical_plan.go` | `sql.arrow_sorter.enabled` / `sql.arrow_distinct.enabled` + `ArrowSorterEnabled()` / `ArrowDistinctEnabled()` |
| planner 判定 | `pkg/sql/arrow_unification.go` | `arrowSortPlan` / `arrowDistinctPlan` 结构、`canArrowSort` / `canArrowDistinct` 判定、`buildArrowSortPlan` / `buildArrowDistinctPlan` 序列化 |
| planner 分流 | `pkg/sql/distsql_physical_planner.go` | `createPlanForDistinct` 的 else 分支（普通 `SELECT DISTINCT`）+ `addSorters` + `addDistinct`（聚合内 distinct）均接入 Arrow core；`addSorters`/`addDistinct` 增加 `evalCtx` 参数 |
| planner 分流 | `pkg/sql/distsql_plan_stream.go` | `addDistinct` 调用点补 `evalCtx` 参数 |
| executor | `pkg/sql/rowexec/arrow_sorter_processor.go`（新建） | `newArrowSorterProcessor` + `ProcessorBase`：`rows → unifiedInputFrom → Arrow Record → arrowRecordToEncDatumRows → sort.SliceStable(EncDatumRow.Compare) → ProcessRow` |
| executor | `pkg/sql/rowexec/arrow_distinct_processor.go`（新建） | 同上模式，排序后相邻去重（`encodeCols` 字节比较，NULL 视作相等） |
| executor 分发 | `pkg/sql/rowexec/processors.go` | `NewProcessor` switch 补 `core.ArrowSorter` / `core.ArrowDistinct` 分支 |
| 测试 | `pkg/sql/rowexec/arrowpilot/arrow_unify_sort_distinct_test.go`（新建） | `TestArrowUnifySortDistinct` + `ArrowSorterRunCount` / `ArrowDistinctRunCount` 探针 |

**实现要点（首期保守但语义 100% 正确）**

- 两个 processor 沿用既有 Arrow 接线范式：输入经 `unifiedInputFrom` 转 Arrow Record（保证算子间 Arrow 链式），再经 `arrowRecordToEncDatumRows` 转回 `EncDatumRows`，在 KWDB 原生 `EncDatumRow.Compare` / 字节比较上做排序/去重，最后经 `p.Out.ProcessRow` 应用 post-process。
- **未使用原生 `arrowcompute` 排序核**（v17 排序核成熟度存疑），而是复用标准比较语义——保证与 rowexec/colexec 逐字节一致，作为后续替换为向量化核的稳妥底座（接口契约不变）。
- 排序支持 `matchLen`（已排序前缀跳过）、多列混合 `ASC/DESC`、NULL；去重支持单列/多列、`NULLsAreDistinct=false`（NULL 视作相等）、`orderedCols` 分组去重；`ErrorOnDup` / `NullsAreDistinct=true` 暂不支持，由 `canArrow*` 拒绝并静默回退标准路径。
- 回退安全：`canArrow*` 不支持 / JSON 序列化失败时，planner 静默回退 `Sorter`/`Distinct` 标准 core（不 panic），与聚合器 `addTwiceAggregators` 模式一致。

**验证结果**

- `go test ./pkg/sql/rowexec/arrowpilot/ -run TestArrowUnifySortDistinct`：PASS（确认 Arrow sorter/distinct 被实际使用且结果正确）。
- `go test ./pkg/sql/rowexec/arrowpilot/`（全包 11 测试）：PASS，无回归。
- `go test ./pkg/sql/execinfrapb/ -run TestArrowCoreUnionFieldsRoundTrip`：PASS。

### 4.8 BOOL_AND / BOOL_OR 落地（2026-08-01）

**无需新增 Arrow 算子或 aggOp 常量**：BOOL_AND / BOOL_OR 直接复用既有 min/max 核——Arrow 的 `min` 对布尔列等价于 AND、`max` 对布尔列等价于 OR，且 `minMaxAgg` 在 `!m.set`（全 NULL 或空组）时返回 null，天然满足 SQL 三值逻辑（全 NULL → NULL）。

**改动**
- planner（`pkg/sql/arrow_unification.go`）：
  - `canArrowAggregate` 新增 `AggregatorSpec_BOOL_AND` / `AggregatorSpec_BOOL_OR` 分支，要求输入为单布尔列（`types.BoolFamily`），拒绝 distinct。
  - `buildArrowAggPlan` 将 `BOOL_AND` → `arrowAggExprJS{Func:"min"}`、`BOOL_OR` → `arrowAggExprJS{Func:"max"}`。
- executor（`arrow_aggregate.go`）：**零改动**——既有 `aggOutputType("min"|"max")` 对 BOOL 输入返回 BOOL，`minMaxAgg` 已按 `arrow.BOOL` 分支累积 `boolMin`(AND) / `boolMax`(OR)。

**测试**：`pkg/sql/rowexec/arrowpilot/arrow_unify_bool_agg_test.go`（`TestArrowUnifyBoolAgg`）覆盖分组/全局、混合 NULL、全 NULL（→ NULL）、全 false/true 场景，含 `ArrowAggRunCount` 探针确认走 Arrow 路径。

**验证结果**
- `go test ./pkg/sql/rowexec/arrowpilot/ -run TestArrowUnifyBoolAgg`：PASS。
- `go test ./pkg/sql/rowexec/arrowpilot/`（全包）：PASS，无回归。

**剩余缺失聚合函数（P1 续）**：FIRST / LAST（有序首末值，需保留排序或稳定输入假设）、JSON_AGG / CONCAT_AGG（需 Arrow list/string 拼接核，难度较高）。STDDEV / VARIANCE 系列已于 2026-08-01 完成（见 §4.12）。

**标量/字符串函数扩展（P1 续，2026-08-01）**：投影新增数值标量函数 abs / sqrt / ln / sign / power，复用 arrow/compute v17 自带 kernel（`compute.CallFunction` 路径，无新 Go 核）。planner 侧在 `physical_plan.go` 的 `canArrowRender` / `addArrowRendering` 接入 `arrowNumericFuncName`（SQL 名→kernel 名 + 固定 arity 门控），仅放行 int/float 数值类型、并要求多参同 family（与 binary-expr 门控一致）。floor/ceil/round 因 arrow kernel 为 int→float 语义与 SQL float 输入不符，暂未引入；trim/replace 等字符串函数待后续 native loop 扩展（arrow/compute 本 vendored 版本无字符串 kernel）。验证见 `arrow_unify_numeric_func_test.go`（纯单元，无集群）。

### 4.9 集合算子去重 UNION/INTERSECT/EXCEPT DISTINCT 落地（2026-08-01）

**无需新增 Arrow 算子**：UNION DISTINCT / INTERSECT DISTINCT / EXCEPT DISTINCT 的去重节点本就复用 `ProcessorCoreUnion{Distinct:...}`，与 §4.3 的普通 DISTINCT 共用同一套 `canArrowDistinct` → `ArrowDistinct` 路径。只需在 set op 的三个 Distinct 构造点接入复用 helper。

**改动**（`pkg/sql/distsql_physical_planner.go` `createPlanForSetOp`）：
- 抽取 closure `arrowDistinctCoreFor(distinctCols, orderedCols, inTypes)`，开关 `sql.arrow_distinct.enabled` 开启且 `canArrowDistinct` 通过时返回 `ArrowDistinct` core，否则返回原 `Distinct` core。
- **点1**（左右各自 distinct，非 ALL 时循环 L6573→现为 `distinctSpecs[side]`）：改用 `arrowDistinctCoreFor(plan.ResultTypes)`。
- **点2**（UNION DISTINCT 最终跨边去重 `AddSingleGroupStage`，原 L6629）：改用 `arrowDistinctCoreFor(p.ResultTypes)`。
- **点3**（INTERSECT/EXCEPT 经 `AddDistinctSetOpStage` 接收 `distinctSpecs[:]`）：因 `distinctSpecs` 已在点1 填好 Arrow core，自动生效，**零额外改动**。
- executor（`arrow_distinct_processor.go`）：**零改动**，完全复用 §4.3。

**语义对照**：UNION ALL 不经 distinct，不会误触 Arrow distinct（测试已 sanity 校验）。

**测试**：`pkg/sql/rowexec/arrowpilot/arrow_unify_set_op_test.go`（`TestArrowUnifySetOp`）覆盖 UNION DISTINCT（{1,2,3,4}∪{2,3,5}={1,2,3,4,5}）、INTERSECT DISTINCT（={2,3}）、EXCEPT DISTINCT（={1,4}），并以 `ArrowDistinctRunCount` 探针分别确认三类各触发 ≥1 次 Arrow distinct；另校验 UNION ALL 不触 distinct。

**验证结果**
- `go test ./pkg/sql/rowexec/arrowpilot/ -run TestArrowUnifySetOp`：PASS。
- `go test ./pkg/sql/rowexec/arrowpilot/`（全包）：PASS，无回归。

**P0 进度**：Sorter ✓ / Distinct ✓ / 集合去重（UNION/INTERSECT/EXCEPT DISTINCT）✓ / Window（最小子集）✓ / Merge-join ✓（见 §4.11）；Limit/Offset 由 PostProcess 免费支持，不入 P0 清单。**P0 全部算子已落地**。

### 4.10 Window（无 frame 分区聚合，最小子集）落地（2026-08-01）

**只实现最小子集**（用户确认）：无 frame（默认 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW）的分区聚合窗口函数 SUM / COUNT / MIN / MAX / AVG / BOOL_AND / BOOL_OR。其余（显式 frame、排名函数 ROW_NUMBER/RANK/LEAD/LAG、多 arg 聚合、带 filter）安全回退标准 windower。

**改动**
- `execinfrapb/processors.pb.go`：`ProcessorCoreUnion` 新增 `ArrowWindower *Expression`（field 58，wire tag `0xd2,0x03`），Marshal/Size/Unmarshal 同步。
- `physicalplan/physical_plan.go`：新增 `sql.arrow_windower.enabled` 开关 + `ArrowWindowerEnabled()`。
- `pkg/sql/arrow_unification.go`：
  - `canArrowWindow`：要求 engine 非 Timeseries、所有 WindowFn 为默认 RANGE frame（写 `isDefaultRangeFrame` 判定 `Mode==RANGE && Start==UNBOUNDED_PRECEDING && End==CURRENT_ROW`）、单 arg、无 filter、聚合函数落在支持集、`PartitionBy`/arg/ordering 列 Arrow 可比较。
  - `buildArrowWindowPlan`：映射 `AggregatorSpec_Func` → 字符串（SUM/COUNT/MIN/MAX/AVG/BOOL_AND/BOOL_OR），产出 `arrowWindowerPlan{PartitionBy, Fns[{Func,Input,Ordering,OutputIdx}]}`。
  - `createPlanForWindow` 单节点分支（`AddSingleGroupStage`）接入分流。
- `pkg/sql/rowexec/arrow_windower_processor.go`（新建）：沿用 Arrow 算子范式（`unifiedInputFrom` + `arrowRecordToEncDatumRows`），在 EncDatumRow 上按 `PartitionBy` 切分区、按各 fn 的 `Ordering` 识别 peer（连续等值组），对每个 fn 跨整个分区持续累积（`windowAccumulator` 自包含标量运算：int/float/decimal 用 apd `DecimalCtx`、bool 用 AND/OR），peer 边界把当前累积写回 peer 内所有行的 `OutputIdx`。输出列数 = 输入列 + fn 数，经 `p.Out.ProcessRow` 下发。

**关键语义要点**
- 默认 RANGE frame 的 peer = 连续 ORDER BY 等值行；累积器**跨越 peer 不复位**，仅在分区边界重建，保证「运行聚合到当前 peer」正确。
- 全 NULL / 空 peer → NULL（与标准一致）。
- AVG 结果类型为 decimal，用 `tree.DecimalCtx.Quo`（Precision 20，允许舍入）。

**测试**
- `arrowpilot/arrow_unify_window_test.go`：`TestArrowUnifyWindow`（SUM/COUNT/AVG 跨分区运行聚合，含 `ArrowWindowerRunCount` 探针）+ `TestArrowUnifyWindowFallback`（ROW_NUMBER、显式 ROWS frame 回退标准路径且结果正确）。

**验证结果**
- 两个 window 测试 PASS；arrowpilot 全包（14 测试）PASS，无回归。

**P0 进度更新**：Sorter ✓ / Distinct ✓ / 集合去重 ✓ / Window（最小子集）✓ / Merge-join ✓。Limit/Offset 免费支持。**P0 全部算子已落地**。

### 4.11 Merge-join（依赖排序合并连接）Arrow 化落地（2026-08-01）

**做法（复用而非新算子）**：Merge-join 在 planner 中走 `mergeJoiner` core 的前提是 `planMergeJoins` 开启且 `n.mergeJoinOrdering` 非空（即等连接列是主键/索引有序输入）。Arrow 的 join 执行器（`arrow_join.go`）本质是**无顺序假设的 hash join**，但语义上覆盖全部 4 种 join 类型（inner/left/right/full）+ inner 非 equi ON 后置过滤，与 merge-join 的 equi/outer 语义完全一致。因此**无需实现 Arrow merge 算法**——只需让原本要下发 `mergeJoiner` core 的分支，在满足条件时改为下发已成熟的 `ArrowJoin` core 即可。

**改动**
- `arrow_unification.go`：
  - 修正 `arrowJoinKeyType` 门控：原错误允许 `BytesFamily`（Arrow 无 comparable byte 原语，会退化成 STRING 列并静默错配），改为支持 `DecimalFamily`/`TimestampFamily`/`TimestampTZFamily`——与 `arrow_join.go` 的 `joinRowHash`/`arrValEqual` 实际能力及 `arrow_adapter.go:buildArrowColumns` 的列类型映射对齐（INT64/FLOAT64/BOOL/STRING/TIMESTAMP/DECIMAL128）。
  - 新增 `canArrowMergeJoin(engine, leftEq, rightEq, joinType, leftTypes, rightTypes)`：复用 `arrowJoinKeyType` 做 key 类型门控 + `arrowJoinType` 做 join 类型门控（不含 hash-join 的 `leftMergeOrd.Columns==nil` 前置条件）。
- `distsql_physical_planner.go`：`createPlanForJoin` 的 `mergeJoiner` 分支前插入 Arrow 分流——若 `ArrowJoinEnabled` 且 `canArrowMergeJoin` 通过，则构造 `ArrowJoin` core（与 hash-join 分支共用 `buildArrowJoinPlan` + `BuildArrowOnExprJSON` 后置过滤）；非 inner 的非 equi `onCond` 因无法在 merge 匹配期外求值而回退 `mergeJoiner`。

**关键语义要点**
- Arrow（hash）join **不保序**：原 merge-join 经 `SetMergeOrdering(n.reqOrdering,...)` 声明输出顺序，hash-join 路径同样如此声明（既有行为），故 Arrow 化 merge-join 与标准 hash-join 行为一致，不引入新偏差。依赖顺序的下游本就会显式加 sorter。
- 纯 equi merge-join + inner 非 equi `onExpr` 均走 Arrow；外层 join（left/right/full）带非 equi ON 时回退标准 mergeJoiner（ON 必须在匹配期求值）。

**测试**
- `arrowpilot/arrow_unify_merge_join_test.go`：`TestArrowUnifyMergeJoin`，两表 join 键均声明 `PRIMARY KEY`（驱动 planner 选 merge-join），覆盖 inner/left/right/full + inner 非 equi 后置过滤，全部 `ArrowJoinRunCount` 探针校验走 Arrow 且结果与标准引擎逐位一致。

**验证结果**
- 测试包编译通过（`go vet ./pkg/sql/rowexec/arrowpilot`）。完整 e2e 需 `make test PKG=./pkg/sql/rowexec/arrowpilot` + 预编译二进制/`KWDB_LIB_DIR`（本环境未搭建 C++ 引擎，待 CI/预编译环境跑通）。

**P0 全部算子落地小结**：Sorter / Distinct / 集合去重 / Window（最小子集）/ Merge-join 均已完成；Limit/Offset 由 PostProcess 免费支持。下一步进入 P1 函数集扩展（STDDEV/VARIANCE 等）。

### 4.12 STDDEV / VARIANCE 系列 Arrow 化落地（2026-08-01）

**背景**：KWDB 的 VARIANCE/STDDEV 在 distsql 中是**三阶段**聚合：本地阶段 `[SQRDIFF, SUM, COUNT]`（各单输入）→ 中间/最终阶段 `FINAL_VARIANCE`/`FINAL_STDDEV`（三输入 [SQRDIFF, SUM, COUNT]）。Arrow 聚合器原本只支持单输入函数，故需新增两种 op 并扩展 kernel 的输入列能力。

**改动**
- `arrow_aggregate.go`：
  - `scalarAggregator` 接口 `Consume` 由单输入 `arrow.Array` 改为 `[]arrow.Array`（多输入）；`feedGroup`/`newStates` 改用 `aggInputs(agg)` 解码所有输入列。
  - `ArrowAggExpr` 新增 `Inputs []string`（多输入）；保留 `Input` 以兼容历史单输入语义；新增 `aggInputs` 辅助。
  - 新增 `aggOpSqrdiff` / `aggOpFinalVariance` 枚举与 `parseAggOp` 映射；`sqrdiff`/`final_variance`/`final_stddev` 均映射至 `aggOpFinalVariance`（final_stddev 在 Finalize 时取 sqrt）。
  - 新增 `sqrdiffAgg`（Welford 在线算法单输入，float + decimal/int 拓宽）与 `finalVarianceAgg`（三输入并行方差合并，sample 用 count-1、stddev 取 sqrt；float + decimal）。两者均实现 `MergeFrom`（并行分片合并）。
  - `aggOutputType`：`sqrdiff`/`final_variance`/`final_stddev` 与 `mean` 同类型规则（int/decimal→DECIMAL128，float→FLOAT64），与 colexec sqrdiff/variance 输出类型一致。
- `arrow_unification.go`：`canArrowAggregate` 允许 `SQRDIFF`（单输入数值）、`FINAL_VARIANCE`/`FINAL_STDDEV`（三输入数值），`buildArrowAggPlan` 序列化 `sqrdiff`/`final_variance`(Inputs=[3 列])/`final_stddev`(Inputs=[3 列])。
- `arrow_aggregator_processor.go` / `ArrowAggExpr` JSON：支持多输入 `Inputs []int` 反序列化。
- 修正：既有 `meanAgg` 的 decimal 路径误用了 `apd.Decimal.Add` 方法式 API 与未定义的 `decimal`/`oneD`，统一改为 `tree.ExactCtx.Add/Sub/Mul/Quo` 包函数 + `scalarToApd` 辅助（INT64/DECIMAL128 统一转 apd.Decimal）。

**语义对齐**：`sqrdiffAgg` 完全复刻 colexec `floatSqrDiff`/`decimalSqrDiff`（Welford 增量 sqrdiff）；`finalVarianceAgg` 复刻 `floatSumSqrDiffs`/`decimalSumSqrDiffs`（Chan et al. 并行方差合并）后除以 (count-1)。故 Arrow 与标准引擎数值结果逐位一致（int/decimal 拓宽为 decimal，float 保持 float）。

**测试**：`arrowpilot/arrow_unify_variance_test.go`（`TestArrowUnifyVariance`）：float/int 输入、分组/非分组、VARIANCE/STDDEV，用 `ArrowAggRunCount` 探针验证走 Arrow 且结果与关闭 Arrow 的标准引擎逐位一致。

**验证状态**：`go build`/`go vet` 全通过；完整 e2e 需 `make test PKG=./pkg/sql/rowexec/arrowpilot` + 预编译 C++ 引擎（`KWDB_LIB_DIR`），待 CI/预编译环境跑通。P1 聚合函数集已覆盖 SUM/MIN/MAX/COUNT/AVG/BOOL_AND/OR/STDDEV/VARIANCE。
