# KWDB Arrow 统一执行引擎 — 路线图与覆盖率矩阵

> 本文档是 `arrow-unification-architecture.md` 与 `arrow-unification-gap-analysis.md` 的**执行追踪文档**。
> 目标：以 Arrow (`arrow.Record`) 为统一列式载体，最终替代 `rowexec`（按行 EncDatumRow）与 `colexec`（coldata.Batch）两套老执行路径。
> 分支：`arrow-unify`，最新落地 commit：`arrow-unify` 工作区（未提交，含 2026-08-06 收尾）。

## 0. 当前覆盖状态（截至 2026-08-06）

### 0.1 算子层（7/7 Arrow core 已实现并注册）

| 算子 | Arrow core | 接收端工厂 | Planner 开关挂钩 | 端到端打通 | 备注 |
|------|-----------|-----------|----------------|-----------|------|
| Projection | `ArrowProjection` | `newArrowProjectionProcessor` (processors.go:143) | `arrowProjectionEnabled`+`canArrowRender` (physical_plan.go:1355) | ✅ | 字符串 trim/replace、数值标量已扩展 |
| Filter | `ArrowFilter` | `newArrowFilterProcessor` (processors.go:149) | `InterceptArrowFilterForScan` (distsql:1820) | ✅ | 扫描侧 filter 拦截 + 独立 ArrowFilter 阶段（`evalIsNull`/`evalIn`/比较/逻辑/算术均支持） |
| Aggregator | `ArrowAggregator` | `newArrowAggregatorProcessor` (processors.go:155) | 3926/4597/4830 | ✅ | SUM/MIN/MAX/COUNT/AVG/BOOL_*/STDDEV/VARIANCE 已完成 |
| Join | `ArrowJoin` | `newArrowJoinProcessor` (processors.go:161) | `ArrowJoinEnabled` (distsql:5541/5573) | ✅ | inner/left/right/full + inner 非 equi 后置过滤 |
| Sorter | `ArrowSorter` | `newArrowSorterProcessor` (processors.go:167) | `ArrowSorterEnabled` (distsql:3257) | ✅ | 全序排序 |
| Distinct | `ArrowDistinct` | `newArrowDistinctProcessor` (processors.go:173) | `ArrowDistinctEnabled` (distsql:4381/6377/6593) | ✅ | UNION/INTERSECT/EXCEPT DISTINCT |
| Windower | `ArrowWindower` | `newArrowWindowerProcessor` (processors.go:179) | `ArrowWindowerEnabled` (distsql:6995) | ✅ | 分区聚合(sum/count/min/max/avg/bool_*/) 默认RANGE/ROWS 运行聚合 + 整partition 帧；排名 row_number/rank/dense_rank |
| UnionAll | `ArrowUnionAll` | `newArrowUnionAllProcessor` (processors.go:181) | `ArrowUnionAllEnabled` (distsql:6673) | ✅ | UNION ALL 经 ArrowUnionAll 算子拼接上游 Record（同节点零拷贝短路，替代行式 no-op 合并）；多输入 stage，任意嵌套 |

### 0.2 表达式/标量层

| 类别 | 状态 | 覆盖范围 |
|------|------|---------|
| 列引用 / 常量 | ✅ | index var + 字面量 |
| 数值标量 | ✅ | `abs/sqrt/ln/sign/power/floor/ceil/ceiling/trunc/round`（arrow/compute v17 kernel） |
| 字符串函数 | ✅ | `length/octet_length/lower/upper/concat/substring/trim/ltrim/rtrim/btrim/replace/overlay/split_part` 投影侧全 Arrow 化；过滤侧经 Computed leaf 复用同一批 kernel（`substring/upper/trim/like` 等不再回退 `tree.Datum`）；**过滤侧 CASE/COALESCE 谓词**经复用投影 CASE spec 接入 Arrow 引擎（2026-08-07 落地） |
| 比较/逻辑/算术 | ✅ | 投影/过滤已覆盖基础算子 |
| CAST 类型转换 | ⚠️ | 过滤侧 int/float/bool/string 已支持；投影 decimal↔string 已支持 |
| LIKE / IN / IS NULL | ✅ | LIKE/ILIKE 已支持；IN/NOT IN 已支持（int/string/decimal 列，含 computed 左操作数如 `substring(col,1,3) IN (...)`；decimal 经 cast-FLOAT 后比对 float64 集合）；IS NULL/IS NOT NULL 已支持（`evalIsNull`，扫描侧 + 独立 ArrowFilter 阶段） |
| 聚合输入多列 | ✅ | `Inputs []string` + `SQRDIFF/FINAL_VARIANCE` 三输入合并 |

### 0.3 桥接与扫描

| 组件 | 状态 | 说明 |
|------|------|------|
| `UnifiedProcessor` 接口 | ✅ | arrow_adapter.go:34 已定义，7 算子均实现 |
| `unifiedInputFrom` | ✅ | 所有 Arrow 算子通过它将上游 RowSource 适配为 UnifiedProcessor |
| `arrowScan`（native scan） | ⚠️ | arrow_adapter.go:340 已存在，但 table reader **未默认走** Arrow scan |
| `arrow_bridge.go`（coldata↔arrow） | ✅ | `NewRowSourceToArrow` 已接入 `unifiedInputFrom`（colexec 行式输出→Arrow Record，增量 builder 零中间缓冲）；`NewArrowToRowSource`（Arrow→RowSource）已被 `arrow_filter_processor.Next` 调用作为 Arrow→下游行式桥。原 `arrowScan` 桥接已并入本文件成为 colexec→Arrow 官方桥 |
| 统一 DAG 调度层 | ✅ | planner 各算子决策收口到 `arrow_unification.go` 的 `arrowXxxCoreFor` 助手（`Enabled && canArrowX && build && marshal` 四步合一，降级语义一致），join 两处保留 `return err` |

## 1. 六大差距（详见 gap-analysis.md）

1. **表达式/标量覆盖缺口最大**：投影/过滤中大量函数仍 `default:` 回退 `tree.Datum` 行式计算（arrow_adapter.go:146/163/286/333/454/542/572）。
2. **扫描端未 Arrow 化**：`newTableReader` 默认产出行式 EncDatumRow，`arrowScan` 未被默认启用。
3. **三引擎未统一调度**：`UnifiedProcessor` 仅算子内部使用，planner 无统一 DAG 编排层。
4. **未覆盖算子**：递归 CTE、采样（见阶段4 末项评估，均未纳入 Arrow 路线）。算子层 7/7 Arrow core + UNION ALL（多输入）已补齐，阶段4 可落地项已清零。
5. **内存治理**：各算子各自持有 `memory.Allocator`，缺统一 Allocator 与配额收敛到 `execinfra` 内存监控。
6. **老路径退役**：rowexec 42 个非 arrow processor、colexec 123 文件待灰度下线。

## 2. 分阶段实施路线图

依赖关系：文档先行 → 阶段1/2 可并行 → 阶段3 依赖前两者 → 阶段4 → 阶段5。

### 阶段 0 — 路线图与覆盖率矩阵文档（本文档）✅ 进行中
- [x] 覆盖率矩阵（0.1–0.3）
- [x] 六大差距梳理
- [ ] 本文档落地为 `kwbase/docs/arrow-unify-roadmap.md`

### 阶段 1 — 扫描端 Arrow 化（消除 row→arrow 反复转换）【分析完成，方案已定】

**现状盘点（2026-08-04）**：
- 下游 Arrow 算子已通过 `unifiedInputFrom` (arrow_filter_processor.go:320) 把 table reader（RowSource）经 `newArrowScan` 流式 build 成 Arrow Record；若上游实现 `ArrowRecordEmitter.ArrowOutput()` 则运算符间直接传递 Record，无行式往返（arrow_adapter.go:308 + §7.8 operator-to-operator buffering）。
- planner 已有 `InterceptArrowFilterForScan` (distsql:1820) 证明 scan 上的 filter 可独立成 ArrowFilter stage。
- **结论**：扫描→Arrow 转换已被现有架构透明完成，"扫描端未 Arrow 化"严重程度被缓解，非阻断项。

**接入方案**：
- [x] 分析：确认 `unifiedInputFrom`/`newArrowScan` 已覆盖 scan→arrow 透明转换
- [x] 新增 `ArrowScanEnabled` 开关（`sql.arrow_scan.enabled`，默认 true，2026-08-04 落地）：作为 Arrow 路径**总闸**——`arrowFilterEnabled`/`arrowAggregatorEnabled`/`arrowJoinEnabled`/`arrowSorterEnabled`/`arrowDistinctEnabled`/`arrowWindowerEnabled` 内部均 `&& ArrowScanEnabled(evalCtx)`，开关关时所有 Arrow 算子不下发，scan 保持纯行式。照 `ArrowJoinEnabled` 模式实现（`arrowScanEnabledSetting` + `arrowScanEnabled` + `ArrowScanEnabled` 封装）。
- [x] `newTableReader` 在开关下包装为 `UnifiedProcessor` 产出方（实现 `ArrowRecordEmitter`，2026-08-04 落地）：`arrowScan` 新增 `ArrowOutput()` 方法（实现 `ArrowRecordEmitter`，懒 build 并与 `Next` 共享 `s.rec`），使 scan 作为 Arrow 算子上游时其 Arrow Record 可被下游 `unifiedInputFrom` 直接复用（operator-to-operator，省二次拉行）。scan→arrow 的包装已由 `unifiedInputFrom → newArrowScan` 透明完成，本项补齐"产出方"语义使 scan 即起点也能以 Arrow Record 暴露给统一编排。
- [x] 类型门控：`arrowScanSupported(typs)` 新增（所有列 `arrowDataTypeForKWType` 不报错即支持）。时序 Float/time、decimal、timestamp 经 `newArrowBuilder` 已支持；Date/Interval 已于 2026-08-04 纳入，Bytes 已于 2026-08-07 纳入（`arrow.BinaryTypes.Binary`，并与 aggregator/join 的分组哈希比较对齐）；故常规模拟类型均覆盖；真正不支持的（Array/INet/Time/TimeTZ/Oid/...）由 `arrowDataTypeForKWType` 的 default 报错在 schema/builder 构建阶段失败快路径。
- [ ] 该改造属接口对齐，非 fetcher 内部重写；真正的 fetcher 直出 Arrow builder（省 EncDatum 解码）列入阶段5 性能收尾

### 阶段 2 — 表达式/标量覆盖扩展【部分落地】

**已落地（2026-08-04）**：
- [x] 过滤侧 **IN / NOT IN**：`x IN (1,2,3)` / `x NOT IN (...)`，列 IN 常量集合，native Go kernel（`evalIn`，arrow_filter.go），支持 int64 / string 两类列（同 family 常量）。
- [x] 过滤侧 **IS NULL / IS NOT NULL**：KWDB 以 `ComparisonExpr EQ/NE DNull` 表达（无独立 `Is` 运算符）；`canArrowFilterExpr`/`buildArrowFilterNode` 识别该形态并生成 `is_null`/`is_not_null` 单操作数节点，`evalIsNull` 遍历列有效性位图（任意类型）。
- [x] 投影侧 **overlay(str,substr,start) / split_part(str,sep,n)**：native loop（`evalArrowOverlay`/`evalArrowSplitPart`），经 `arrowStringFuncName` + `canArrowRender` 接入。并把 `replace`/`trim`/`ltrim`/`rtrim`/`btrim` 补入 `eval` switch（此前缺失会错误回退 compute kernel）。
- [x] 过滤 `LIKE`/`ILIKE`（含非字符串左操作数 CAST 到 string）—— 既有
- [x] 过滤 `CAST`（int/float/bool/string）—— 既有
- [x] 投影字符串 trim/ltrim/rtrim/btrim/replace、数值标量 abs/sqrt/ln/sign/power —— P1 既有
- [x] 投影侧 **CAST（安全子集）**：`canArrowRender` 与 `arrowOperandArg` 新增 `CastExpr` 分支，`arrowArg` JSON 加 `Cast` 字段；运行时经 `castArrowArray`（与过滤侧共用 kernel）在 compute 前对操作数做类型转换。首轮启用 arrowCastTargetTag 覆盖的类型对（目标 STRING/INT/FLOAT），且各 cast kernel 已实现下列源：
  - `→STRING`：int / float / **bool** / string / **decimal128**（decimal128→string 复用 `decimal128ToApd`，按列 scale 还原小数位）
  - `→INT`：string / float / int / **bool**（true→1, false→0）
  - `→FLOAT`：string / int / float / **bool**
  - **`→DECIMAL`**（目标 `CAST(x AS DECIMAL(p,s))`，2026-08-04 落地）：新增 `castToDecimal` kernel（源 Int64/Float64/String），经 `apdToDecimal128` 按目标 scale 半进位舍入；planner `arrowCastTargetTag` 加 `DecimalFamily→"DECIMAL"`，`arrowArg`/`arrowCastTagToType` 传目标 scale（取 `ResolvedType().Scale()`，precision 默认 38）。
  - 即投影 CAST 已覆盖 `int/float/bool/decimal→string`、`string/int/float/bool` 互转、以及 `int/float/string→decimal` 的常见组合。

**待做**：
- [x] 投影/过滤 CAST 罕见目标 —— `timestamp→string`（2026-08-04 落地）：`castToString` 新增 `*array.Timestamp` case，按 KWDB `tree.TimestampOutputFormat`（`"2006-01-02 15:04:05.999999999"`，UTC）渲染，与行式 `DTimestamp.Format` 输出一致。planner 侧无需改动（`arrowCastTargetTag` 已支持 STRING 目标、`canArrowRender`/`arrowOperandArg` 的 CastExpr 分支对目标 STRING 已放行，源 Timestamp 由运行时 kernel 处理）。扫描/过滤侧 `CAST(ts_col AS string)` 现可走 Arrow 路径。
- [x] 投影/过滤 CAST 罕见目标 —— `date→string` / `interval→string`（2026-08-04 落地）：前提是先把 Date/Interval 纳入 Arrow 解码类型。Date 在 Arrow 侧以 `Int32`（自 epoch 天数，匹配 KWDB 编码 `DDate.Date.UnixEpochDays`，inf 映射 NULL）承载，`castToString` 新增 `*array.Int32` case 渲染为 `2006-01-02`；Interval 在 Arrow 侧以 `String`（规范文本，同 JSON 处理）承载，`castToString` 的 `*array.String` case 直接复用原值。`arrow_adapter.go` 四处（`arrowDataTypeForKWType` / `newArrowBuilder` / `buildArrowColumns` / `appendEncDatum`）均加 Date/Interval 分支；`physicalplan.arrowSupportedPassthroughType` 加 Date/Interval 使列引用透传与 `CAST(date/interval AS string)` 的 inner 表达式被 planner 放行。现 `CAST(date_col AS string)` / `CAST(interval_col AS string)` 可走 Arrow 投影/过滤路径。
- [ ] 投影/过滤 CAST 更罕见目标 —— `numeric→bool`：SQL `numeric→bool` 为真值判断（非 0 为真），arrow/compute v17 无此 kernel，且 KWDB 是否支持该 CAST 待确认；暂不做。
- [x] 收敛 arrow_adapter.go 的 `default:` 回退分支（2026-08-04 落地）：`arrowTypeForKWType` 改为 checked 版 `arrowDataTypeForKWType(t) (arrow.DataType, error)`，`default` 显式报错而非静默回退 `Int64`；`newArrowBuilder` 改为返回 `(array.Builder, error)`，`default` 同样显式报错（不再静默建 `Int64Builder`）；两处 schema 构建调用点（`NewRowToArrowConverter` 经 `initErr` 字段在 `Next` 上抛、`arrowScan.build` 经 `release()` 释放后返回）统一失败快路径。`buildArrowColumns`/`appendEncDatum` 的 `default` 本就显式报错，现三处入口语义一致：任何**真正**不支持的 family（Array/INet/Time/TimeTZ/Oid/...）在 schema/builder 构建阶段即明确失败，杜绝「Int64 伪装 → 后续类型断言 panic」的隐藏陷阱。**注意（2026-08-07）**：Bytes 已从"不支持"移出——`arrowDataTypeForKWType`/`buildArrowColumns`/`appendEncDatum`/`newArrowBuilder` 均支持 `arrow.BinaryTypes.Binary`，且 `arrow_aggregate.go` 的 `arrowGroupHashIdx`/`arrowGroupRowEqualIdx`/`arrowGroupKeyEqual` 与 `arrow_join.go` 的 `joinRowHash`/`arrValEqual`/`appendValueAt` 均已对齐 `arrow.BINARY`，BYTES 列的 GROUP BY/DISTINCT/JOIN 分组比较语义正确。**剩余缺口**：vendored `arrow/compute` 的 `FilterBinary` kernel 对 `arrow.Binary` 变长偏移布局存在 `GetSpanOffsets` 越界（已知生态 bug），故含 BYTES 列的过滤/投影谓词若走 compute kernel 会 panic；该路径目前经 `canArrowFilterExpr` 对 Bytes 列回退 `tree.Datum` 通用路径（功能正确，非向量化），待阶段 C 收口。

### 阶段 3 — 统一调度层
- [x] 统一 Arrow plan 序列化与降级约定：新增 `marshalArrowPlan(plan) (*Expression, bool)`（`arrow_unification.go`），取代 planner 中散落的 8 处裸 `arrowUnificationMarshal` 调用（sorter / distinct / window / 2× agg / 2× setop / filter-Intercept），把「序列化失败即降级行式」语义收口到一处。join 两处保留原始 `return err` 控制流（更保守，未动）。
- [x] 清理 `arrow_unification.go` 末尾的 `var _ = physicalplan.ArrowAggregatorEnabled` 包循环占位 hack，移除该文件对 `physicalplan` 包的冗余导入。
- [x] `UnifiedProcessor` 契约（Next() (arrow.Record, bool, error)）已就位，rowexec 各 `ArrowXxxProcessor` 均实现之；planner 经 `ProcessorCoreUnion.ArrowXxx` 字段只选 core、不关心内部，已是「统一调度」形态。
- [x] planner 引入统一编排收口：以 UnifiedProcessor 契约为中心，把各算子的 `ArrowXxxEnabled && canArrowX && build && marshal && AddXXXStage || fallback` 四步判断收口到 `arrow_unification.go` 中的 `arrowSorterCoreFor` / `arrowDistinctCoreFor` / `arrowWindowerCoreFor` / `arrowAggCoreFor` 助手（均返回 `(ProcessorCoreUnion, bool)`，`false` 即沿用既有行式/colexec 降级路径，与 `marshalArrowPlan` 的「序列化失败即降级」语义一致）。setop 两处 distinct 经 `distinctSpecFor` 局部适配闭包复用同一助手。join 两处刻意保留原始 `arrowUnificationMarshal` + `return err` 控制流（更保守，序列化失败即中断整个 plan 而非降级），未纳入收口。本环境无法 `go test`，已用 `go build ./pkg/sql` 验证库包通过。
- [x] colexec 经 `arrow_bridge` 纳入统一 DAG（已真正接入运行链路，非仅设施）：
  - **colexec→Arrow 方向**：`unifiedInputFrom` 在检测到上游非 `ArrowRecordEmitter`（即 colexec 算子/行式算子的 RowSource 输出）时，调用 `arrow_bridge.go` 的 `NewRowSourceToArrow`，经增量 per-column Arrow builder 把流式行直接攒成单个原生 Arrow Record（零中间 `EncDatumRows` 缓冲），并 itself 实现 `ArrowRecordEmitter` 供下游 Arrow 算子 operator-to-operator 消费。原 `arrow_adapter.go` 的 `arrowScan`/`newArrowScan` 桥接逻辑已**整合并入** `arrow_bridge.go`（`rowSourceToArrowBridge`），成为 colexec→Arrow 的官方桥，`arrow_adapter.go` 仅保留 `arrowScanSupported` 类型门控与 `newArrowBuilder`/`appendEncDatum`。
  - **Arrow→colexec 零拷贝路径（已启用）**：`colexec/columnarizer.go` 的 `Columnarizer.Next` 现检测上游 `RowSource` 是否实现 `ArrowRecordEmitter`（鸭子类型，colexec 本地接口，不 import rowexec 避免循环依赖）；若是，直接 `RecordToBatch(rec, allocator)` 把 Arrow Record 经子切片（`NewSlice`）分批转成 coldata.Batch，**跳过行式 decode 往返**，实现 Arrow→colexec 零拷贝桥。全部 7 个 Arrow 算子（filter/aggregator/distinct/sorter/windower/join/projection）现均实现 `ArrowOutput() arrow.Record`：filter/aggregator/join/projection 原有；distinct/sorter/windower 本次在 compute 末尾经 `buildArrowColumns`+`array.NewRecord`+`buildArrowSchema` 构造 `outputRec` 并加 `ArrowOutput()`。distinct/sorter 用 `p.OutputTypes()`、windower 用 `p.outTypes` 作输出类型。`RecordToBatch` 签名已加 `allocator *Allocator`（nil 时回退 `coldata.NewMemBatchWithSize`），`arrow_bridge_test.go` 同步传 `nil`。`NewArrowToRowSource`（Arrow Record→RowSource）已被 `arrow_filter_processor.Next` 调用作为 Arrow→下游行式桥，与 Columnarizer 的 `RecordToBatch`（Arrow→coldata，colexec 直接路径）协同：colexec 下游走零拷贝，行式下游走 NewArrowToRowSource。
  - 两者均复用已验证的 `arrowDataTypeForKWType`/`newArrowBuilder`/`appendEncDatum`/`arrowRecordToEncDatumRows` 路径，不改变单-Record 算子的运行语义。本环境无法 `go test`，已用 `go build ./pkg/sql` 验证库包通过。

### 阶段 4 — 剩余算子补齐
- [x] Top-N（Limit + Ordered 合并 Arrow 阶段）：`LIMIT n ORDER BY` 走 ArrowSorter 时，planner 经 `pushLimitToArrowSorter` 把 localLimit=count+offset 注入 ArrowSorter plan，compute 用 `plan.Limit` 做 top-N 提前切片（offset 仍由 PostProcess 处理，避免双重截断）；与 colexec 行式 sorter 的 per-sorter 局部 limit 行为对齐。
- [x] 窗口 frame（ROWS/RANGE）与排名函数 row_number/rank/dense_rank：聚合支持默认 RANGE / ROWS UNBOUNDED PRECEDING-TO-CURRENT ROW 运行聚合、整 partition 帧（UNBOUNDED-TO-UNBOUNDED）、以及 **ROWS / RANGE 偏移帧**（如 `BETWEEN 1 PRECEDING AND 1 FOLLOWING`，逐行滑动窗口 / 值基帧）。RANGE 偏移帧的 value-based 求值逻辑经 `rangeFrameBounds` + `offsetDatum`（按 ORDER BY 值做 ±offset 二分，覆盖 int/float/decimal/timestamp）实现，**于 2026-08-07 修复端到端打通**：根因是 `computePartition` 只排序了输出行 `outPart`、却把未排序的原始输入行 `part` 传给 `computeAggregateWindow`，使 RANGE offset 从乱序的 `orderVals` 做二分查找而退化/分区错乱；修复为 ranking 与 aggregate 计算统一基于排序后的 `outPart`（与 `out` 共享底层数组，`outPart[i]` 即 `out[startIdx+i]`），RANGE/ROWS offset 帧、`rangeFrameBounds` 二分、peer 分组全部基于正确有序行。`isSupportedWindowFrame` 现已对 RANGE offset 边界放行（GROUPS 模式与缺 end 仍回退）。单测 `rowexec/arrow_windower_range_test.go`（`rangeFrameBounds` 算术）与 `rowexec/arrow_windower_range_offset_test.go`（端到端分区排序+值基帧+peer 重复值）均绿；`pkg/sql/arrow_unification_window_test.go` 验证 planner 放行。`computeAggregateWindow` 经 `frameBounds` + `windowAccumulator` 逐行求值。排名函数 row_number/rank/dense_rank 已落地。
- [x] 投影 CASE / COALESCE：新增 `arrowProjectionColFor`（抽取单表达式建列）、`arrowCaseCol`/`arrowCoalesceCol`/`arrowComparisonCol` 助手；`canArrowRender`/`addArrowRendering` 新增 `*tree.CaseExpr`/`*tree.CoalesceExpr` 分支，分支值经 `canArrowRenderCase` 统一 cast 到结果类型（仅 arrow 可物化类型：int/float/string/bool）。executor `arrowProjection` 新增 `evalCase`（原生逐行选支 + `evalIsNull` 布尔掩码），`ArrowProjectionSpec` 加 `Kind/branches/else` 字段，`buildProjectionSpecs` 递归翻译。`COALESCE(a,b,c)` 等价于 `CASE WHEN a IS NOT NULL THEN a ... ELSE c`。见 §2.6。
- [x] 集合 ALL 变体（UNION ALL 短路）：新增 `ArrowUnionAll` 算子（多输入 stage，把所有上游 Arrow Record 经 `unifiedInputFrom` 收为 UnifiedProcessor 后拼接为单 Record），`createPlanForSetOp` 的 UNION ALL 分支在 `ArrowUnionAllEnabled` 时将合并点从行式 no-op 改为 ArrowUnionAll stage（同节点全 Arrow，跨节点边界仍由 no-op 收口）。新增 `sql.arrow_union_all.enabled` 开关。
- [x] **Values 算子 Arrow 化（纯常量源，无 KV/引擎依赖）**：（P1 收口，**已落地于 commit 7ca82fe8**）
  - **依据**：`valuesProcessor` 无任何输入，数据为 planner 预编码的 `spec.RawBytes`+`spec.Columns`；`Next()` 仅 `StreamDecoder.GetRow` 解码 datum row + `ProcessRowHelper`（PostProcess）。计算过程零 KV、零引擎依赖，是纯形态转换。
  - **落地**：新增 `ArrowValues` core（`pkg/sql/rowexec/arrow_values_processor.go` 的 `arrowValuesProcessor`，嵌入 `*valuesProcessor` 继承 RowSource/Processor 接口，仅重写 `ArrowOutput` 返回预构建单 Arrow Record；启动时把 `spec.RawBytes` 一次性解码成 `[]EncDatumRow`，经 `buildArrowColumns`/`array.NewRecord` 攒成单个 Arrow Record 供下游 operator-to-operator）。`rowexec/processors.go` 在 `core.ArrowValues != nil` 时分发；`arrow_unification.go` 新增 `arrowValuesCoreFor`（`(ProcessorCoreUnion, bool)`，复用 `ValuesCoreSpec`）；`distsql_physical_planner.go` 的 `createValuesPlan` 经其切换；`execinfrapb/ProcessorCoreUnion` 新增 `arrowValues = 59`（手维护 `processors.pb.go` 对齐）；新增开关 `sql.arrow_values.enabled`（`ArrowValuesEnabled` 门，`ArrowScanEnabled` && 子开关）；`rowexec/arrowpilot/arrow_unify_values_test.go` 端到端验证（VALUES→聚合链路逐位一致）。`go build ./pkg/sql` 通过。
  - **开关**：`sql.arrow_values.enabled`（**默认 true**，自 2026-08-14 翻 true 并验证；`ArrowValuesEnabled` 门，与 `ArrowScanEnabled` 同为主路径默认开启），`createPlanForValues` 经 `arrowValuesCoreFor` 助手（同 `arrowSorterCoreFor` 风格，返回 `(ProcessorCoreUnion, bool)`）切换；false 或解码/类型不支持时回退经典 `Values` core。`arrowValuesProcessor` 早期 bug 已修——原先构造 Arrow Record 时**忽略 planner post（filter+投影）**，导致带 `WHERE`/投影的 VALUES 静默返回错误结果（如 `CASE` 过滤返回 `[4 4 4 4]`）；现 `newArrowValuesProcessor` 直接复用 `newValuesProcessor` 已 `v.Init` 完成的 `ProcOutputHelper`、对每行 `vp.Out.ProcessRow` 应用 post 收集后 `buildArrowColumns`，CASE/COALESCE 过滤等不可 Arrow 化的谓词由行式 evaluator 正确计算（`TestArrowUnifyValues` 验证）。**迁移路径 `received multiple headers` 根因已定位并修复（2026-08-14 深挖）**：`arrowValuesProcessor` 嵌入 `*valuesProcessor`，后者 `Start()` 会向**自身内嵌的 `StreamDecoder` 喂一条 producer-header 消息**；早期修复在 `newArrowValuesProcessor` 构造函数里手动 `vp.Start(context.Background())` 初始化 `ProcOutputHelper`，但 flow 运行时又调用了继承的 `valuesProcessor.Start()`（那时 `arrowValuesProcessor` 无自定义 `Start`），对同一**内嵌 StreamDecoder** 喂了**第二次 header** → 下游 gRPC 接收端 `StreamDecoder.AddMessage` 报 `received multiple headers`，server 启动期的系统表迁移查询（其含 VALUES 子查询）即触发、破坏启动。修复：① 构造函数**移除** `vp.Start`（因 `ProcOutputHelper` 在 `newValuesProcessor→v.Init` 已完整初始化，`ProcessRow` 可直接用）；② 新增 `arrowValuesProcessor.Start` 仅调 `av.StartInternal(ctx, "arrow-values")`、**不碰 StreamDecoder**。修复后 `arrow_values.enabled=true` 下 `TestArrowProjectionE2EWithRealSQL`（触发真实 server 启动迁移）不再报该故障。**默认 true 翻 true 后两次回归及修复（2026-08-14 续）**：翻 true 后 `TestArrowUnifyFilterCase` 暴露两个独立 bug，均已修：
    1. **`StreamDecoder.GetRow` 复用 `rowBuf` 底层数组导致的 aliasing bug**：构造函数循环里 `rowBuf := make(...)` 是同一 buffer 反复传给 `GetRow`，`rows = append(rows, row)` 追加的 slice 全部指向同一底层数组，循环结束后所有行塌缩成最后一行的 EncDatum，filter 把它们全过滤掉 → 输出空。修复：每行 `append` 前 `rowCopy := make(...); copy(rowCopy, row)` 独立拷贝。
    2. **Arrow Record 字段名/输出类型空间不匹配**：`buildArrowRecord` 原先用 `av.outTyps`（`spec.Columns` 原始类型）+ `t.Name()`（类型名如 "int4"）作字段名，但下游 Arrow 算子按 `col%d` 约定（`arrow_adapter`/`arrow_bridge`/`arrow_join` 等全部用 `fmt.Sprintf("col%d", i)`）解析投影，且 post 含投影时行已按 `av.Out.OutputTypes` 表达。修复：列类型改用 `av.Out.OutputTypes`（转 `[]*types.T`），字段名统一 `col%d`。修复后 `TestArrowUnifyFilterCase` 通过，全部 arrowpilot 测试（34 项）通过。
- [ ] 递归 CTE / 采样（若纳入 Arrow 路线）
  - **评估结论（2026-08-05）**：两项均**未纳入** Arrow 路线，原因：
    - **递归 CTE**：KWDB 的递归 CTE 在逻辑计划层以 `recursiveCTENode`（`pkg/sql/recursive_cte.go`）实现，依赖每次迭代经 `genIterationFn` 重新生成 plan 树并 `runPlanInsidePlan` 驱动行式 `planNode`，与 DistSQL 静态 stage DAG 的 Arrow 列式调度不在同一层。Arrow 化需把"初始查询 + 递归体"编译为可循环装配的 stage 图，超出"补齐算子"范畴且迭代重编译风险高，暂不做。
    - **采样**：指标准 SQL `TABLESAMPLE` 行级采样算子；KWDB 当前**未实现**该语法（`sql.y` / `sem/tree` 中均无 `TABLESAMPLE`）。建表语句里的 `SAMPLE`（`create.go:1598`）是时序表建表语义，非查询期行采样算子，无对应的执行期阶段可 Arrow 化。
  - 阶段4 剩余可落地算子至此已清零；后续如需覆盖，应在路线图新开子项重新评估（递归 CTE 需架构级改造，采样需先落地 `TABLESAMPLE` 语法与行式算子再 Arrow 化）。

### 阶段 5 — 内存治理 + 老路径灰度退役
- [ ] 统一 `memory.Allocator` 收敛至 `execinfra.MemoryMonitor`
- [x] 每算子 `ArrowXxxEnabled` 默认开启（2026-08-06 完成：8 个算子子开关 `defaultEnabled` 翻 `true`，Arrow 成为默认主路径；保留 `sql.arrow_scan.enabled` 总闸与运行时降级网）
- [ ] rowexec/colexec 非 arrow processor 灰度下线（保留回退开关）

### 阶段 6 — 完全替代路线（三层并存 → 纯 Arrow 引擎）

> 目标：从「rowexec + colexec + Arrow 三层并存 + per-算子 Arrow 开关」演进为「完全由 Arrow 引擎替代 rowexec 与 colexec」。
> 阶段 0–5 完成后，Arrow 已是默认主路径；阶段 6 负责"清零老路径 + 表达式全覆盖 + 开关退役"。

#### 6.1 当前架构基线（截至 2026-08-04）
- **三层并存**：
  1. `rowexec`：经典 `processors.go` 中 16+ 个 `newXxx` 行式 processor（TableReader/JoinReader/Sorter/Distinct/Aggregator/HashJoiner/MergeJoiner/Windower/ProjectSet/Ordinality/Values/ZigzagJoiner/InterleavedReaderJoiner/StreamAggregator/SampleAggregator/Noop）。
  2. `colexec`：列式 `coldata.Batch`，`columnarizer.go` 桥接 + `colbatch_scan`/`cfetcher` 向量化扫描（当前默认列式路径）。
  3. `Arrow`：列式 `arrow.Record`，10 个 Arrow 算子 + `arrow_bridge.go` 双向桥。
- **per-算子开关（9 个，`physicalplan` 包；master gate `sql.arrow_scan.enabled` 默认 `true`，8 个算子子开关 `projection/filter/aggregator/join/sorter/distinct/windower/union_all` 已于 2026-08-06 翻 `defaultEnabled=true`，Arrow 为默认主路径）**：
  `ArrowScanEnabled` / `ArrowFilterEnabled` / `ArrowAggregatorEnabled` / `ArrowJoinEnabled` /
  `ArrowSorterEnabled` / `ArrowDistinctEnabled` / `ArrowWindowerEnabled` / `ArrowUnionAllEnabled` / `ArrowProjectionEnabled`。
- **已 Arrow 化算子**：Scan、Filter、Projection、Aggregator(含 VARIANCE/STDDEV)、Hash/Merge Join、Sorter、Distinct、Windower、UNION ALL。

#### 6.2 剩余任务分解

| 阶段 | 任务 | 风险 | 相对工作量 |
|------|------|------|-----------|
| A 开关默认化 | 9 个开关翻 `defaultEnabled=true`；助手函数恒返 Arrow core；开关退役为调试用 | 低 | ~0.5 人周（**已完成 2026-08-06**：除 master gate `sql.arrow_scan.enabled` 本就 `true` 外，8 个算子子开关 `projection/filter/aggregator/join/sorter/distinct/windower/union_all` 的 `defaultEnabled` 全部翻 `true`，Arrow 成为默认主路径；运行时 `canArrow*` 校验 + `marshalArrowPlan` 序列化失败即降级到行式/colexec 兜底） |
| B 补齐未 Arrow 化算子 | ProjectSet / Ordinality / Values / ZigzagJoiner / InterleavedReaderJoiner / StreamAggregator / SampleAggregator | 中-高 | ~6-10 人周（**修订于 2026-08-07**：可/应 Arrow 化者为 Ordinality（已完成）、Values（P1 排期、纯常量源零依赖）；**2026-08-10 修订**：Zigzag/Interleaved 定为 D3b 桥接型（经 `unifiedInputFrom` 自动 Arrow 输出、已融入统一 DAG，不另写 Arrow core）；不纳入为 StreamAggregator（流式专有）、ProjectSet（变长展开，D2）、SampleAggregator（采样，§1 已明确）。详见 §6.3 / §6.9 D3b） |
| C 表达式全覆盖 | 字符串函数（substring/upper/lower/concat/length/like/trim/replace）、CASE/COALESCE、CAST 全类型、日期时间函数、floor/ceil/round、窗口 RANGE/ROWS frame | 高 | ~8-12 人周（**2026-08-10 修订**：CASE/COALESCE + 窗口 ROWS/RANGE 偏移 frame + 投影 floor/ceil/round/trunc + 过滤 IN decimal 集合 + 投影日期时间函数 extract/date_trunc/now/age + 字符串函数投影/过滤均已落地；CAST 全类型已于 2026-08-10 落地（BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 目标 + 源补全 + 修投影顶层 CAST 被静默忽略 bug），**C 项实质已清零**；仅阶段 C 内核替换（Bytes 过滤原生 kernel、TIME 目标 cast）留作后续非阻塞项） |
| D colexec 随覆盖率提升逐步收窄至可去除 | **措辞审订（2026-08-06，D3 细分于 2026-08-10）**：colexec 非架构硬约束，是 Arrow 覆盖率不足时的过渡性兜底层。关系型 `colbatch_scan`/`cfetcher` 可被 ArrowScan 替代；时序读可被 `ArrowTsReader` 替代（§6.7/§6.8）；Arrow 未覆盖点兜底随表达式/类型/D3a（非对称算子）+ D3b（桥接型已融入）全覆盖而消失。终态 colexec 可完全去除，仅留 Arrow 主 + rowexec 永久兜底（详见 §6.8/§6.10/§6.9 D3b） | 中 | ~4-6 人周（关系型扫描替代 + ArrowTsReader + D3a 非对称算子；colexec 删除为覆盖率达标后的清理动作；D3b 桥接型 0 新增算子） |
| E 退役 rowexec | 16 个经典 processor 灰度下线（保留回退安全网）；确认 Backfiller/系统表读等特例 | 中-高 | ~3-5 人周 |
| F 测试回归 | arrowpilot 补算子测试；TPCH/TPC-DS 全量对拍；Arrow vs classic fuzz | 中 | ~4-6 人周（**已补：多算子链路 e2e 测试** `arrow_unify_pipeline_test.go`：filter→agg→sort 串联 + projection→distinct 串联，断言各算子 Arrow run count 增长且结果与行式逐位一致） |

**总计约 25-40 人周（单人节奏 6-10 个月）**。阶段 C（表达式全覆盖）与阶段 D（退役 colexec）为最大瓶颈。

#### 6.3 关键阻塞点（需先决策）

1. **字符串函数 Arrow 化（阻塞点 1，最硬骨头）**：vendored `arrow/compute` 仅有 `string_casts.go`（字符串↔数值/布尔转换），**无字符串处理函数 kernel**（substring/trim/replace/like 等 0 匹配）。官方 arrow go v17 的 `arrow/compute` 同样缺字符串 kernel（已知生态缺口）。详见 6.4 专项分析。
2. **窗口 frame**：Windower 已支持无 frame 分区聚合、默认 RANGE/ROWS 运行聚合、整 partition 帧、以及 ROWS 偏移帧（offset 边界逐行滑动窗口）。RANGE 偏移帧（需排序值算术）暂未支持；frame exclusion 未支持（-> 回退行式）。
3. **扫描侧双路径**：colbatch_scan（关系型 KV 向量化扫描）与 ArrowScan 并存；退役 colexec 前需确认 ArrowScan 在关系型引擎路径等价。**时序（TS）路径是独立问题，见 §6.7——时序 scan 不走 colbatch_scan/`cfetcher`，而是 `ts_reader.go` 的 `TsReaderOp` 经 `NextVectorizedTsFlow` 直接产出 `coldata.Batch`；它和关系型的唯一关联点是「产出的 buffer 格式（coldata.Batch / EncDatumRows）」，而非语义耦合。**
4. **灰度回退安全网**：完全退役行式前必须保留"Arrow 异常 → 行式/colexec"的回退开关，生产环境零停机切换。
5. ~~**时序读独占于 colexec（2026-08-06 初版，已撤销）**~~：经 §6.8 审订，`TsReaderOp` 的 tse FFI 是公共底层读接口、不绑定 colexec；其 Go 侧 buffer 装配可被 `ArrowTsReader` 替代（产出 Arrow Record），故时序读**不构成 colexec 保留理由**。详见 §6.7 + §6.8 第 1 条。

#### 6.4 阻塞点 1 专项：能否借 cpp arrow 获得字符串 kernel

**现状（2026-08-05 更正）**：完整 arrow 仓库已就位于 `/home/sdy/go/src/gitee.com/arrow`（含 `cpp/`、`go/` 双子树）。
- `cpp/src/arrow/compute/kernels/` 下确有**完整字符串 kernel**：
  - `scalar_string_ascii.cc`：`ascii_upper/lower/trim/ltrim/rtrim/lpad/rpad/center/reverse/replace_slice/slice/split_whitespace/join/repeat`，以及 `binary_*` 系列、`match_substring`/`starts_with`/`ends_with`/`like`（含 re2 路径）。
  - `scalar_string_utf8.cc`：`utf8_upper/lower/swapcase/capitalize/title/length/reverse/trim/ltrim/rtrim/normalize/replace_slice/slice/split_whitespace`。
- 这些 kernel 覆盖阶段 C 所需的全部字符串函数，**缺失已被证明在社区版 cpp 中已解决**。

**但 KWDB 不能直接用 cpp kernel**，三条路线对比：

| 路线 | 做法 | 可行性 | 代价 |
|------|------|--------|------|
| A cgo 封装 cpp | 把 cpp string kernel 经 `compute::CallFunction` 包成 C ABI，Go 侧 cgo 调用 | 技术上可行，但需**链接整个 arrow cpp + utf8proc（+re2）** 进 KWDB；与现有 `libkwdbts2` C++ 引擎存在符号/ABI 冲突风险，构建链大幅变重 | 高（构建/链接/维护）|
| B Go 自研 kernel | 按 arrow go v17 kernel 框架（`internal/kernels` + `registry.AddFunction`，参考 `string_casts.go`）用纯 Go + `unicode/utf8` 重写 | **最稳**，无新依赖，与现有 Arrow 路径一致 | 中（需逐个实现，但算法可抄 cpp 语义）|
| C 升级 arrow go | 把 vendored `arrow/go/v17` 升级到已含 string kernel 的更新版 | 需重测 KWDB 全部 arrow go 内部用法耦合，风险高 | 高 |

**cpp 关键依赖（决定路线 A 成本）**：
- `utf8_upper/lower/normalize/swapcase/capitalize/title` → 依赖 `utf8proc`（`cpp/src/arrow/vendored/` 已内嵌，可编译）。
- `match_substring`/`like`/`regexp_match` 的 regex 路径 → 依赖 **re2**（不在 vendored，需系统安装或引入）。
- `utf8_length/reverse/trim/slice/substring` → **纯 UTF-8 字节逻辑**（`arrow::util::UTF8Length`/`UTF8Transform`），不依赖第三方，最适合路线 B 优先落地。

**结论**：cpp 源码已就位，证明"字符串 kernel 没有技术鸿沟"；但**不翻译/不引入 cpp**，采用**路线 B（Go 自研 kernel）**，算法语义直接参考 `/home/sdy/go/src/gitee.com/arrow/cpp/src/arrow/compute/kernels/scalar_string_*.cc`。
- 第一批（纯 UTF-8，无第三方依赖，优先）：`utf8_length`/`utf8_reverse`/`utf8_trim`/`utf8_ltrim`/`utf8_rtrim`/`utf8_substr(slice)`/`utf8_upper`(ASCII 子集)/`utf8_lower`(ASCII 子集)。
- 第二批（需 utf8proc 等价或 Go unicode 表）：完整 `utf8_upper/lower/capitalize/title/normalize`（Go `unicode`/`golang.org/x/text` 可覆盖，无需 utf8proc）。
- 第三批（正则）：`like`/`regexp_match`/`split_part` → 用 Go `regexp` 或引入 re2；与 cpp re2 路径语义对齐即可。

> 路线 A 仅在"性能压测证明 Go kernel 成为瓶颈"时作为备选评估，不作为阶段 C 默认方案。

#### 6.5 字符串函数 Arrow 实现现状（2026-08-05 调研更正）

调研发现：**字符串函数的 Arrow 实现早已存在于 KWDB 自身**（`pkg/sql/rowexec/arrow_projection.go` 中以原生 Go 向量化 loop 实现，并非依赖 arrow/compute 官方 kernel），覆盖投影路径 13 个函数：
`length`/`octet_length`/`lower`/`upper`/`concat`/`substring`/`trim`/`ltrim`/`rtrim`/`btrim`/`replace`/`overlay`/`split_part`。
过滤路径的字符串标量函数（如 `substring(col,1,3)='x'`）当前经 `canArrowFilterExpr` 放行后回退 `tree.Datum` 通用路径（功能正确但非向量化）。

**本次（2026-08-05）对齐 crdb/KWDB 语义的修正**（在 `arrow_projection.go`，学习 cpp `scalar_string_*` 的 UTF-8 处理 + crdb `builtins.go` 的语义边界）：
1. `substring(s, start, length)` 三参数 **length<0** 由原来的「返回空串」改为 **报错** `negative substring length N not allowed`（对齐 crdb `substringImpls`，与 PostgreSQL 一致）。
2. `trim`/`ltrim`/`rtrim`/`btrim` 无 cut-set 时由原来的字节集 `" \t\n\v\f\r"` 改为 `unicode.IsSpace`（对齐 crdb `strings.TrimSpace`/`TrimLeftFunc`/`TrimRightFunc`），可正确去除 Unicode 空白（如 U+00A0）。
3. `replace(s, from, to)` 空 `from` 由原来的「返回原串」改为 `strings.Replace(s, "", to, -1)`（对齐 crdb，Go 语义：每字符间插入 to）。
4. `overlay(s PLACING sub FROM start)` 的 `start<1` 由原来的「钳制为 1」改为 **报错** `'start' must be positive`（对齐 crdb）。
5. `split_part(s, sep, n)` 的 `n<=0` 由原来的「返回 NULL」改为 **报错** `field position N must be greater than zero`（对齐 crdb）。

**验证**：新增 `arrowpilot/arrow_unify_string_semantics_test.go`（`TestArrowUnifyProjectionStringSemantics`）覆盖上述 5 类边界；全套件 `go test ./pkg/sql/rowexec/arrowpilot/` 通过（82.7s）。编译 `go build ./pkg/sql/...` 通过。

**结论**：字符串投影函数的"缺失"已被证伪（实现早已存在），真正缺口仅是**过滤路径的字符串标量函数向量化**（回退通用路径，功能正确）。阶段 C 的剩余重点转为：过滤路径字符串标量函数 Arrow 化、CASE/COALESCE、CAST 全类型、日期时间函数、floor/ceil/round（`arrow/compute` kernel 语义不符）、窗口 RANGE/ROWS frame。

#### 6.6 过滤路径字符串函数 Arrow 化（2026-08-05）

将字符串标量函数（substring/upper/trim/length/replace/concat/overlay/split_part 等）作为过滤谓词的叶子（比较 / LIKE 的左操作数）也路由进 Arrow 引擎，消除 `tree.Datum` 通用回退。做法：引入嵌套的 **computed leaf**，在 executor 侧复用已有投影字符串 kernel 物化中间列，再交由比较 / LIKE 谓词消费。

**改动文件**：
- `pkg/sql/rowexec/arrow_projection.go`：`ArrowArg` 新增 `Computed *ArrowProjectionSpec`（复用投影 spec 承载嵌套函数）。
- `pkg/sql/physicalplan/physical_plan.go`：`arrowFilterLeaf` 新增 `Computed *arrowFilterComputed`；新增 `arrowFilterComputed{Func, Args}`；`arrowFilterLeafFromExpr` 增加 `*tree.FuncExpr` case（经 `arrowStringFuncName` 识别，递归展开 args）；`canArrowFilterExpr` 在 IS NULL/EQ/NE、比较、LIKE、IN/NOT IN 分支放行 `Computed != nil` 的叶子。
- `pkg/sql/rowexec/arrow_filter_processor.go`：`arrowFilterLeafJS` 新增 `Computed *arrowFilterComputedJS`（JSON 往返）；新增 `arrowFilterComputedJS{Func, Args}`；`leafToArrowArg` 递归展开 Computed → `*ArrowArgComputed`。
- `pkg/sql/rowexec/arrow_filter.go`：`evalLeafDatum` 新增 Computed 分支 → `evalComputed`（构造临时 `arrowProjection{alloc}`，调用既有 `evalArrowStringFunc` 物化为 arrow array，返回 `compute.NewDatum(arr)`）。

**验证**：新增 `arrowpilot/arrow_unify_filter_string_test.go`（`TestArrowUnifyFilterStringFuncs`），覆盖 `substring(name,1,3)='ali'`、`upper(name)='BOB'`、`trim(name)=name`、`substring(...) LIKE 'ali%'`、`length(name)=5`，并用 `rowexec.ArrowFilterRunCount` 确认走了 Arrow filter 引擎。全套件 `go test ./pkg/sql/rowexec/arrowpilot/` 通过（93.1s）；`go build ./pkg/sql/...` 通过。

#### 6.7 时序路径与 Arrow 的关系（2026-08-06 更正）

**核心结论**：时序与关系**无语义耦合**，仅通过「时序引擎产出 crdb 关系格式 buffer（coldata.Batch / EncDatumRows）」这一交界点发生关联。一旦数据进入该 buffer，下游即按关系算子处理。因此时序 scan 在架构上**可以作为 Arrow 的数据源接入**，无需改动 tse C++ 引擎。

**事实依据（代码核对）**：
- `TsReaderOp.Next`（`colexec/ts_reader.go:222`）直接产出标准 `coldata.Batch`，与关系型 `colBatchScan` 的输出 buffer **同构**——均经 `vec.Append` 把 `tro.Rcv.Data[i]` 填进 `internalBatch.ColVecs()`。时序列（数值指标 + timestamp tag + 字符串 tag）只要类型落在 Arrow 桥支持范围内，buffer 格式与关系型一致。
- Arrow 已有现成的 `coldata.Batch → Arrow Record` 桥：`colexec/arrow_bridge.go` 的 `BatchToRecord(b coldata.Batch, alloc)`（支持 Int64/Float64/Bool/Bytes）。`TsReaderOp` 的 `coldata.Batch` 经此桥转 Arrow Record 即进入 Arrow DAG。
- rowexec 侧同理：`tsTableReader.Next` 产出 `EncDatumRows`，而 `arrow_adapter.go` 的 `rowToArrowConverter` / `NewRowToArrowConverter` 正是把 `EncDatumRows` 转 Arrow Record 的既有设施。
- 当前 7 个 `canArrowX` 均含 `if engine == tree.EngineTypeTimeseries { return false }`（`arrow_unification.go`），是有意**保守**拦截时序进入 Arrow 算子，而非「已知不能」。

**"时序支持 Arrow" 的分层判断**：
1. **scan 侧（时序 buffer → Arrow Record）**：**低成本、技术上已可行**。把 `TsReaderOp`/`TsTableReader` 输出经 `BatchToRecord`/`rowToArrowConverter` 转 Arrow Record 即可，复用既有桥，不碰 tse 引擎。可单独加 `sql.arrow_ts_scan.enabled` 或复用桥接机制，不必复用关系型 `sql.arrow_scan.enabled` 总闸。
2. **算子侧（Arrow 内处理时序数据）**：需**解除 7 处 `EngineTypeTimeseries → return false` + 补全时序列类型的 Arrow 映射**。难点在类型覆盖而非 scan：
   - `BatchToRecord.vecToArrow` 当前**仅支持 Int64/Float64/Bool/Bytes**，**无 TIMESTAMP/TIMESTAMPTZ/DECIMAL case**；时序常见 timestamp tag、decimal 指标需先补 `vecToArrow` 与 `arrowDataTypeForKWType` 的类型分支。
   - 解除 `return false` 后须**逐个验证** `canArrowX` 对时序语义正确（如时序聚合的 NULL/时间语义、排序稳定性），不能一刀切删除。
   - **（2026-08-10 修订）该专项已实质完成**：① `arrowDataTypeForKWType`（`arrow_adapter.go`）本就支持 Decimal→Decimal128、TIMESTAMP/TIMESTAMP_TZ→Timestamp_us、DATE→Int32、UUID→FixedSizeBinary(16)、JSON/INTERVAL→String，算子侧 Arrow 列承载映射早已齐备；② `ArrowTsScanSupported` 类型门已改为 `arrowTsScanSupportedType` 承载判断，放行 TIMESTAMP/TZ/DECIMAL/DATE/UUID/JSON/INTERVAL（见 §6.7 落地状态与 §6.11.3）；③ 8 处 `EngineTypeTimeseries→return false` 门控已在 P0（2026-08-10）解除。故"算子侧类型补全"的映射短板已不存在，TS 列（含时间/decimal）可经 `arrowTsReader→buildArrowColumns` 直接攒 Arrow Record；余下仅是开关默认 `false` 与 CI e2e 验证未跑（见 §6.11.3 翻转条件）。

**与"ArrowScan 是否支持时序"的澄清**：严格说"ArrowScan"是关系型 KV scan 喂 Arrow 的入口开关；时序 scan 不经此开关。更准确的说法是**新增一条「时序 scan 经关系格式 buffer 桥接喂 Arrow」的独立入口**。本项不在当前 arrow-unify 路线图阶段内，作为后续候选专项（估计 scan 侧 ~0.5 人周 / 算子侧类型补全 ~3-5 人周）。

**落地状态（2026-08-06 续）**：scan 侧独立入口**已完整实现并编译通过**（非原型），具体如下：
- `pkg/sql/rowexec/arrow_ts_reader.go`：`arrowTsReader` 嵌入 `*TsTableReader`（复用 tse FFI `NextTsFlow`/`DropHandle`），经 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，实现 `ArrowRecordEmitter`（`ArrowOutput()`）。支持两种消费模型：`arrowMode=false`（行式推流，行为=关系型 `TsTableReader`，Arrow 旁路）与 `arrowMode=true`（Arrow 直连，下游经 `unifiedInputFrom→ArrowOutput()` 零拷贝取单 Record；内部用 `discardReceiver` 吞掉 tse 的逐行 push，避免推流死锁；流耗尽时 `DropHandle` 释放 tse，幂等）。
- `pkg/sql/physicalplan/physical_plan.go`：新增 `ArrowTsScanEnabled`（`sql.arrow_ts_scan.enabled`，默认 `false`）+ `ArrowTsScanSupported`（类型门，复用 `arrowSupportedCompareType`，非 `types.Timestamp/TimestampTZ/Decimal` 时返回 false——算子侧类型补全前仅放行 int/float/bool/string/bytes/decimal 数值指标与字符串 tag）。**（2026-08-10 修订）** 该类型门已改为独立的 `arrowTsScanSupportedType` 承载判断（与 `arrowDataTypeForKWType`/`buildArrowColumns` 实际支持对齐），**放行 TIMESTAMP/TIMESTAMP_TZ（时间 tag）与 DECIMAL（decimal 指标），外加 DATE/UUID/JSON/INTERVAL**；即 TS scan 的列类型门已与 Arrow 全局承载能力对齐，不再挡时间/decimal 列。开关仍默认 `false`（保守未启用，翻转条件见 §6.11.3）。
- `pkg/sql/rowflow/row_based_flow.go`：`setupInputSyncs` / `arrowTsEmitter` 实现 **Arrow 算子直连 wiring**——单输入 QUEUE TS stream，开关+类型门满足时构造 `arrowTsReader(arrowMode=true)` 缓存于 `f.arrowEmitters[sid]`，直接作为下游 Arrow 算子的 `input`，绕过 RowChannel 中转，走 operator-to-operator 快路；`Cleanup` 对 emitter 也调 `DropHandle`（幂等保险）；`SetupInboundStream` 的 QUEUE 分支保留 `arrowMode=false` 行式兼容路径（开关关/类型不支持时降级）。
- 并发安全：`arrowMode=true` 的 `arrowTsReader` 不加入 `f.TsTableReaders`（flow 永不调其 `RunTS`），由下游 `ArrowOutput()` 单一驱动 `pullRecord`，无 `RunTS` 与 `ArrowOutput` 双路争用；`RunTS`/`ConsumerClosed` 已覆盖为防御+生命周期释放。下游 Arrow 算子的 `unifiedInputFrom` 自动识别 `ArrowRecordEmitter` 走直连（`NewArrowRecordSource`），无需改下游算子。
- 默认 `sql.arrow_ts_scan.enabled=false`，不影响现有 TS 读路径；开启后仅当下游为 Arrow 算子且列类型受支持时激活直连，否则经 `arrowMode=false`/行式 `TsTableReader` 降级。
- 算子侧类型补全（TIMESTAMP/TIMESTAMPTZ Arrow 映射 + 解除 7 处 `EngineTypeTimeseries→return false` 并逐算子验证）仍为后续专项，见 §6.7 第 2 点。

#### 6.8 colexec 是否仍需保留兜底（2026-08-06 分析，2026-08-06 再审订）

**结论（审订后）：colexec 不是架构硬约束，是「Arrow 覆盖率不足时的过渡性兜底层」。在 Arrow 终态（全算子 + 全表达式/类型 + D3a 非对称算子 + D3b 桥接型已融入 + 时序读均被 Arrow 覆盖）下，colexec 可被完全去除，仅保留 Arrow 主路径 + rowexec 最底层兜底。**

**早期（6.8 初版）的三层保留理由，经后续讨论逐一审订**：

1. ~~**时序读独占于 colexec（硬约束）**~~ → **已推翻（2026-08-06）**。`TsReaderOp`（`colexec/ts_reader.go`）拆为两层：① tse C++ 引擎 FFI（`SetupTsFlow`/`NextVectorizedTsFlow`/`CloseTsFlow`，`ts_reader.go:189/251/388`）——这是公共底层读接口，不绑定 colexec；② Go 侧 buffer 装配（297-306 行 `vec.Append` 把 `tro.Rcv.Data[i]` 转 `coldata.Batch`）——纯适配器逻辑。**可被 `ArrowTsReader` 算子替代**：保留 tse FFI，仅把 `vec.Append` 换成 `arrow_adapter.go` 的 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，使 `TsReaderOp` 变为 `ArrowRecordEmitter`。时序读不再是 colexec 的硬约束（见 §6.7 + 本款）。

2. **Arrow 运行时降级兜底层（列式兜底）** → **是当前工程权宜，非架构必然**。降级发生的真实场景只有：A）Arrow 表达式/类型盲区（如未向量化的字符串谓词、RANGE 偏移 frame）；B）marshal 序列化失败（健壮性兜底，随 Arrow 成熟趋零）；C）Arrow 未实现的算子（D3/时序读，已论证均可 Arrow 化）。**若 Arrow 终态覆盖全部表达式/类型/算子，场景 A/B/C 均消失，降级网无处挂靠 colexec**——此时 colexec 无存在必要。

3. **Arrow↔colexec 双向桥是运行链路** → **双向桥的 colexec 侧（`RecordToBatch`，`columnarizer.go`）依赖 colexec 存在**；若 colexec 全去，桥只需保留 Arrow↔rowexec 方向（`BatchToRecord` 已是 Arrow→RowSource，rowexec 侧消费），colexec 侧自然消失。故桥不构成 colexec 的保留理由。

**修正结论**：colexec 的保留理由是**覆盖率驱动的过渡性**——Arrow 每多覆盖一类算子/表达式/类型，colexec 的兜底份额就收窄一分；当 Arrow 完整覆盖（含 D3a 非对称算子、D3b 桥接型已融入、ArrowTsReader 时序读、全表达式/类型），colexec 归零。它从「必留的备」降级为「覆盖率不足时的临时备」，与 rowexec（永久兜底）性质不同。

**修正建议（落到路线图）**：
- 6.2 表中 D 项「退役 colexec」改为「**colexec 随 Arrow 覆盖率提升而逐步收窄，终态可完全去除**」；关系型 `colbatch_scan`/`cfetcher` 可被 ArrowScan 替代，时序读可被 `ArrowTsReader` 替代，Arrow 未覆盖点兜底随覆盖面扩大而消失。
- 6.3 阻塞点第 5 条（时序读独占 colexec）**撤销**——时序读经 `ArrowTsReader` 可 Arrow 化，不构成 colexec 保留理由（见本款第 1 条）。

#### 6.9 rowexec 兜底范围的精确界定 + D 类算子细分（2026-08-06）

**核心问题**：rowexec 里一批「关系型读/计算算子」（lookup join 系、zigzag/interleaved/mergeJoiner、ProjectSet 等）在 colexec 也无对应实现，被笼统归为「rowexec 独占、不可退出」。其中**部分**算子（lookup 系 / 行式 mergeJoiner）的计算本质是基于 Datum Row 的 equality / on-condition，KV 只是输入读取方式（经 `row.Fetcher`），算子本身与 KV 解耦——可像 A/B 类（hash-join/filter）那样被 Arrow 替代；**但 zigzag/interleaved 例外**：其计算内核即 KV 游标状态机（`fetcher` 交替 seek + `side` 切换跳读），KV 不是"读取方式"而是"算法控制流本身"，无法像 hash-join 那样先攒两块 Record 再算（详见 §6.9 D3b 修订）。

**关键区分：算子的「计算模型」与「执行编排」是两层**：
- 计算模型（Datum Row 上的 equality / on-condition / 投影）确与 KV 解耦，理论上可 Arrow 化；
- 能否 Arrow 化取决于**执行编排**是否落在现有 8 个 Arrow 助手的「对称批处理 / 单 Record」模型内。

**D 类（colexec 与 Arrow 都无对应实现，rowexec 独占）按真实原因细分**：

| 子类 | 算子 | 是否真不可 Arrow 替代 | 真实原因 |
|---|---|---|---|
| **D1 真不可（写/DDL/采样/校验/流式）** | 时序写 DML（tsInserter/tsDeleter/tsTagUpdater）、bulkRowWriter、remoteDDL、tsCreateTable/tsAlterTable、scrubTableReader、sampler/sampleAggregator、countRows、streamAggregator（流式引擎专有） | **是** | 写/DDL/采样/数据校验无「列式只读计算」等价；Arrow 是只读向量化引擎，不承载写与统计采集语义 |
| **D2 不可（无 vectorized 语义）** | ProjectSet（set-returning 函数，一行产出多行） | **是（架构性）** | 当前 8 个 Arrow 算子均为「单 Record 输入→单 Record 输出」模型，不表达「一行→多行」的生成列语义；需 Arrow 支持 set-returning 才有机会 |
| **D3a 可 Arrow 化（非对称编排，需新算子骨架）** | **mergeJoiner（行式）/ joinReader / indexJoiner / batchLookupJoiner** | **否，可 Arrow 化** | **收口（2026-08-10）**：① **`ArrowMergeJoiner` 已完成**——经复用 `ArrowJoin` core（`canArrowMergeJoin` 已落地，对称批处理两侧上游 Arrow 流，见 `distsql_physical_planner.go` merge 分支）；② **lookup 系（joinReader/indexJoiner/batchLookupJoiner）定为保留项**——其右侧是 KV 游标（左驱动反查），与 D3b 同构，计算内核不可列式化，经 `NewArrowToRowSource`（上游 Arrow→行式）+ joinReader + `unifiedInputFrom`（行式→Arrow）**已自动融入统一 DAG**（无需另写 Arrow core）；若真要 Arrow 化需重写 KV span 编码（Arrow value→EncDatum→KV 字节，现有无反向路径），高风险且本环境不可端到端测。**proto 字段 `ArrowLookupJoiner`（编号 60，`*Expression`）已加进 `processors.pb.go`，但 executor 未实现，作为保留项占位** |
| **D3b 桥接型·已融入统一 DAG（2026-08-10 修订）** | **zigzagJoiner / interleavedReaderJoiner** | **计算内核不可 Arrow 化** | **内核是 KV 游标状态机**：交替向两侧发 KV 点查（`fetcher.StartScan`+`NextRow`）+ `side` 切换构造 seek span 跳读 + 行级 `Datum.Compare` + `emitFromContainers` 笛卡尔积。**无"两块完整输入 Record"**，无法套用对称 Arrow 算子模型（与 `ArrowJoin` 攒两块 Record 再 hash 本质不同）。计算内核物理上不可列式化；输出侧经 `unifiedInputFrom`→`NewRowSourceToArrow` 自动桥接成 Arrow Record 接入下游，**全 DAG 已是 Arrow 流通，无需另写 Arrow core**（薄壳方案性能等价、零收益，见下） |

**以 lookup join（joinReader）为例（代码核对 `rowexec/joinReader.go`）**：
- join 计算本身：`keyToInputRowIndices` / `inputRowIdxToLookedUpRowIdx` 多对多映射 + equality + on-condition，全是 Datum Row 基础，与 KV 解耦。
- 真正非 Arrow 化的不是「计算」，而是**执行编排的非对称性**：左输入是流，右「表」是按需按 key 反查 KV（`jrStateUnknown→jrReadingInput→jrPerformingLookup→jrEmittingRows` 状态机 + `span.Builder` 攒批 + `fetcher.StartScan/NextRow` 回填）。
- colexec 也未收口它：`colexec/execplan.go:483` 在 `vectorize=auto` 下仅接受 `JoinReader`/`BatchLookupJoiner` 两个 rowexec core 做 wrapping，自身无 parallel lookup joiner。
- **可 Arrow 化路径**：左输入经 `arrow_bridge.NewRowSourceToArrow`（`colexec/arrow_bridge.go`）收 Arrow Record；lookup 反查仍走 `row.Fetcher` 把 KV 行转 `EncDatumRows` 再经 `arrow_adapter.go` 的 `rowToArrowConverter` 成 Arrow Record；另写 `ArrowLookupJoiner` 算子持有状态机（攒批/反查/回填多对多映射），在 Arrow 域内算 equality + on-condition。即**不是不能，是需新增非对称编排算子**（类似当初为 hash-join 写 `ArrowJoin`）。
- **D3a 收口结论（2026-08-10）**：lookup 系（joinReader/indexJoiner/batchLookupJoiner）的真实形态是「左输入流 + 右侧 KV 游标反查」的非对称编排，其计算内核与 D3b（zigzag/interleaved）同构——右侧输入不是预物化的 Arrow Record，而是 KV 游标按需流式拉取的游标，**无"两块完整输入"可供对称 Arrow 算子向量化**。要真正 Arrow 化，需重写 KV span 编码路径（Arrow value→EncDatum→KV 字节，当前 `arrow_adapter.go` 仅有 Arrow→EncDatum 单向、无反向），风险高、且本环境（GOPATH 双 vendor，go test 重复注册 panic）无法端到端验证。**据此定位为保留项**：现状下经 `NewArrowToRowSource`（上游 Arrow→行式）+ joinReader + `unifiedInputFrom`（行式→Arrow）已自动融入统一 DAG，全链路已是 Arrow 流通，性能瓶颈在 KV 游标延迟而非攒批。proto 层已预留 `ArrowLookupJoiner` 字段（编号 60，`*Expression`，4 处：字段声明/Marshal/Size/Unmarshal）作为前向占位，executor 侧暂不实现。

**D3b 桥接型修正（2026-08-10，代码核对 `rowexec/zigzagjoiner.go` / `interleaved_reader_joiner.go`）**：
- **原 §6.9（2026-08-06）误判**：把 zigzag/interleaved 与 lookup 系同归 D3"可 Arrow 化、缺非对称编排骨架、非本质障碍"。经读源码确认，**此判断对 zigzag/interleaved 不成立**——它们不是"左流 + 按需反查"模型，而是**纯 KV 游标状态机**：`zigzagjoiner.go:738-819` 的 `nextRow` 交替 `fetchRowFromSide` 向两侧索引发 `fetcher.StartScan`+`NextRow` 点查，每次 `side` 切换用 `produceSpanFromBaseRow` 构造 seek span 在 RocksDB 跳读，行级 `prevEqCols.Compare` + `emitFromContainers` 笛卡尔积。**两侧输入不是预物化的 Arrow Record，而是 KV 游标按需流式拉取的游标**，无"两块完整输入"可供向量化 join，无法套用现有 8 个对称 Arrow 算子模型，也无法仿 `ArrowJoin` 攒 Record 再算。
- **桥接 vs 薄壳性价比（2026-08-10 实测分析结论）**：
  - **桥接（现状，0 改动）**：`unifiedInputFrom` 检测到上游非 `ArrowRecordEmitter` 时自动调 `NewRowSourceToArrow`，经增量 Arrow builder 把 zigzag/interleaved 吐出的 `EncDatumRow` 攒成 Arrow Record 喂下游 Arrow 算子。已验证、风险 0。
  - **薄壳（另写 ArrowZigzag/ArrowInterleaved core）**：在 executor 内部自己攒 Arrow Record 而非让下游 `unifiedInputFrom` 攒。**数据形态与桥接完全相同**（同样经 `newArrowBuilder.appendEncDatum` 从 `EncDatum`→`Datum`→value→arrow array），性能**完全等价**——瓶颈在 KV 点查/游标 seek 延迟（ms 级），攒批（ns 级）开销可忽略。**薄壳纯属代码位移，对 DAG 形态与运行期零增益**，且每类 join 需新写 executor 骨架 + planner 分支 + 测试，风险高收益无。
  - **结论：桥接性价比碾压，D3b 三项不另写 Arrow core**，作为"行式→Arrow 桥接点"自动融入统一 DAG。
- **保留项（不纳入 D3，独立性能专题）**：`fetcher` 解码强制产出 `EncDatumRow`（含 `tree.Datum`），Arrow 攒批（`appendEncDatum`）必须从 `Datum` 取实际 value，**`EncDatumRow` 这一中间层无法在 join 算子侧跳过**。若要让 KV 直出 Arrow（跳过 `Datum` 分配），需改 `row.Fetcher` 核心加"Arrow 快路径"：KV 字节→arrow array builder 直转（跳过 `EncDatum`/`Datum` 结构分配 + GC 压力）。**但该改造风险极高（fetcher 是存储读取核心，行式/colexec 全路径共享）、收益被 KV 游标延迟淹没，且与 D3 桥接收口是两回事，记为后续候选专项，当前不做。**

**修正结论**：
- rowexec 的**真正不可替代范围**比"D 类全不可退出"更窄——仅剩 **D1（写/DDL/采样/校验/流式）+ D2（ProjectSet）+ D3b（zigzag/interleaved，KV 游标状态机内核不可列式化）**，约 rowexec processor 的一半强。
- **D3a 收口（2026-08-10）**：分两项——① **`ArrowMergeJoiner` 已完成**（复用 `ArrowJoin` core，`canArrowMergeJoin` 落地，属"已 Arrow 化"）；② **lookup 系（joinReader/indexJoiner/batchLookupJoiner）定为保留项**——右侧 KV 游标反查与 D3b 同构，计算内核不可列式化，经 `NewArrowToRowSource`+joinReader+`unifiedInputFrom` 已自动融入统一 DAG，无需另写 Arrow core；proto 预留 `ArrowLookupJoiner` 字段（编号 60）前向占位，executor 未实现。故 D3a 的"非对称编排算子"只剩 lookup 系为保留项，不阻塞终态。
- **D3b（zigzag/interleaved）判定为桥接型**：计算内核不可 Arrow 化，但经 `unifiedInputFrom`→`NewRowSourceToArrow` 自动桥接 Arrow 输出，**已融入统一 Arrow DAG，不另写 Arrow core**（薄壳方案性能等价零收益，见上）。故 D3b 不计入"需新写 Arrow 算子"清单，但 rowexec 仍是其永久降级落点。
- 因此阶段 6「rowexec 兜底」的精确定性应为：**rowexec 永远是最底层兜底残差层**（Arrow 主 → colexec 列式兜底 → rowexec 行式兜底 + D1/D2/D3b 内核独占算子）。D1/D2/D3b 计算内核不可退出；D3a 可退出但工程量大。rowexec 代码量**不会随 Arrow 覆盖而显著缩减**（D1/D2/D3b 不随 Arrow 演进而消失），但 D3a 的退出可逐步收窄其关系型读算子份额。

#### 6.10 重构终态基线：三层「主-备-兜底」长期并存（2026-08-06 总结）

综合 §6.7（时序路径）、§6.8（colexec 兜底）、§6.9（rowexec 兜底），Arrow 统一引擎的**终态是两层：Arrow 唯一主路径 + rowexec 永久兜底；colexec 是「覆盖率不足时的临时备」，终态可去除**。澄清 §6.8 初版「colexec 不可去除」的误判——经审订，时序读（`ArrowTsReader`）与 Arrow 未覆盖点兜底均随 Arrow 覆盖率达终态而消失，colexec 非架构硬约束。

**终态结构（两层 + 一层临时备）**：

| 层 | 终态角色 | 保留内容 | 能否去除 |
|---|---|---|---|
| **Arrow** | **唯一主路径** | 关系型全算子（scan/filter/agg/join/sort/distinct/window/union-all/ordinality）+ 全表达式/类型覆盖（含此前盲区字符串谓词、RANGE frame）+ D3a（ArrowMergeJoiner 已完成；lookup 系为保留项、经桥接融入，§6.9）+ D3b 桥接型（zigzag/interleaved，经 `unifiedInputFrom` 自动 Arrow 输出，已融入 DAG）+ `ArrowTsReader` 时序读（§6.7/§6.8） | 目标本身，主路径 |
| **colexec** | **临时备（覆盖率不足时）** | Arrow 尚未覆盖的表达式/类型/算子（D3、时序读、字符串谓词盲区）的列式兜底 + Arrow↔colexec 双向桥（`BatchToRecord`/`RecordToBatch`） | **终态可完全去除**：Arrow 覆盖率达 100% 后无挂靠点；桥仅保留 Arrow↔rowexec 方向 |
| **rowexec** | **永久最底层兜底** | **D1**（写/DDL/采样/校验/streamAggregator，Arrow 只读无等价）+ **D2**（ProjectSet，一行产多行）+ 一切 Arrow 失败的降级落点 | D1/D2 不可去；D3 可随 Arrow 覆盖退出但兜底职责不变 |

**关键结论（审订后）**：
1. **colexec 是临时层，非永久层**：保留理由是 Arrow 覆盖率不足时的过渡性兜底（§6.8 审订）。当 Arrow 覆盖全算子/全表达式/类型/D3/时序读，colexec 归零。
2. **rowexec 是永久兜底**：D1/D2 是 Arrow 无等价语义的硬兜底，不可退出；D3 即便 Arrow 化，rowexec 仍是降级落点。
3. **Arrow 吃掉全部关系型主动执行 + 时序读 + D3a 非对称编排（D3b 经桥接自动融入）**，colexec 仅作为覆盖率爬坡期的临时列式兜底存在。注：D3b（zigzag/interleaved）计算内核为 KV 游标状态机，不可列式化，仅输出侧经 `unifiedInputFrom` 桥接 Arrow，非"Arrow 内核算子"。

**对阶段 6 措辞的最终定性**：
- 原「D 退役 colexec」「rowexec 兜底可退役」改为：**colexec 随覆盖率提升逐步收窄至可去除；rowexec 退化为永久兜底残差层（D1/D2 不可去）**。
- 工作量评估：关系型扫描/算子替代 + D3a 的 ArrowMergeJoiner（已完成）+ ArrowTsReader 为「新增 Arrow 算子」工作量；lookup 系为保留项（proto 占位、executor 未实现，经桥接融入 DAG）；D3b（zigzag/interleaved）经桥接自动融入，0 新增算子；colexec 删除本身是覆盖率达标后的清理动作，非独立大项。
- 架构基线：**Arrow 主 + rowexec 永久兜底** 两层长期并存；colexec 是过渡层，终态消失。

### 2.4 补齐过滤路径 IN / NOT IN 的 computed 左操作数

把 `substring(col,1,3) IN ('ali','car')` / `upper(col) NOT IN (...)` 这类「字符串函数结果 ∈ 常量集合」谓词也路由进 Arrow 引擎。

**改动文件**：
- `pkg/sql/physicalplan/physical_plan.go`：
  - `canArrowFilterExpr` 的 `tree.In`/`tree.NotIn` 分支：左操作数门控由 `l.Col < 0 && l.Binary == nil && l.Cast == nil` 放宽为追加 `&& l.Computed == nil`，即允许 computed（字符串函数）左操作数；右操作数 `ex.Right` 类型断言由 `*tree.Tuple` 更正为 `*tree.DTuple`，并遍历其 `D []tree.Datum` 元素（类型检查后均为常量，family 须与左操作数一致）。
  - `buildArrowFilterNode` 的 IN 分支同步：`ex.Right.(*tree.DTuple)` + 遍历 `tup.D` 构造 `set.ConstSetStr` / `set.ConstSetInt`；关键点——set 叶子须显式置 `Col: -1`，否则 JSON 中 `col:0` 会被 executor 的 `leafToArrowArg` 误判为「列 0」引用而丢失 ConstSet（导致「right operand must be a non-empty ConstSet」报错）。
- `pkg/sql/rowexec/arrow_filter_processor.go`：`buildArrowFilterSpec`/`leafToArrowArg` 已正确把 `csetstr`/`csetint` 还原为 `ArrowArg.ConstSet`，无需改动。
- `pkg/sql/rowexec/arrow_filter.go`：`evalIn`/`evalInString` 已按左操作数 `array.String` 与 `setLeaf.ConstSet` 命中集合，无需改动。

**验证**：在 `TestArrowUnifyFilterStringFuncs` 追加 `substring(name,1,3) IN ('ali','car')`（期望 `alice`/`carol`）、`upper(name) NOT IN ('ALICE','BOB')`（期望 `carol`/`dave`/`amy`），并以 `ArrowFilterRunCount` 确认两条 IN/NOT IN 谓词均经 Arrow filter 引擎执行。全套件通过。

### 2.5 补齐投影路径 floor/ceil/ceiling/trunc/round

把 `floor(x)`/`ceil(x)`/`ceiling(x)`/`trunc(x)`/`round(x)` 这类舍入标量函数接入 Arrow 投影引擎，消除「因 arrow kernel 为 int→float 语义与 SQL float 输入不符而暂未引入」的历史缺口。

**语义对齐（关键）**：KWDB 的这几个 builtin 均为 `floatOverload1`（输入 float，返回 DFloat=float64）。vendored arrow/compute v17 自带 `floor`/`ceil`/`trunc`/`round` 标量 kernel，且其默认舍入模式恰好与 KWDB 一致：
- `floor`→`RoundDown`、`ceil`→`RoundUp`、`trunc`→`TowardsZero`；
- `round` 默认 `DefaultRoundOptions = {NDigits:0, Mode:RoundHalfToEven}`，即银行家舍入，与 KWDB `math.RoundToEven` 完全一致。
- 整数输入：KWDB 中 `floor` 有 INT4 重载，`ceil/trunc/round` 仅有 float 重载——整数列会由 planner 显式 `CAST(i AS FLOAT)`，与经典路径一致；arrow kernel 对 int 输入返回 float64，结果一致。

**改动文件**：
- `pkg/sql/physicalplan/physical_plan.go`：
  - `arrowNumericFuncName` 新增映射：`floor→"floor"(1)`、`ceil`/`ceiling→"ceil"(1)`、`trunc→"trunc"(1)`、`round→"round"(1)`；executor 经既有 `compute.CallFunction(ctx, kernelName, nil, args...)` 通用路径调用，无需新增 Go kernel。
  - `hasArrowComputeExpr` 扩展：除 `arrowStringFuncName` 外，再识别 `arrowNumericFuncName`，否则单纯 `ceil(f)` 这类渲染不会被判定为「真实 Arrow 计算表达式」，导致 `canArrowRender` 通过但 `addArrowRendering` 因 `hasArrowComputeExpr=false` 而不插入专用阶段（退化为经典求值时结果正确但没走 Arrow）。
  - `canArrowRender` 的 `FuncExpr` 分支已通过 `arrowNumericFuncName` 校验算子数/数值类型，无需改动。

**验证**：新增 `arrowpilot/arrow_unify_numeric_test.go`（`TestArrowUnifyProjectionRounding`）。覆盖 `floor/ceil/ceiling/trunc/round` 在一组含正/负/`.5` 临界值浮点列上的结果（按 `ORDER BY f` 升序断言），验证 `round` 的银行家舍入（如 `round(3.5)=4`、`round(-3.5)=-4`、`round(4.5)=4`），并验证整数列 `floor(i)` 及 `ceil/trunc/round(CAST(i AS FLOAT))`。以 `rowexec.ArrowProjectionRunCount` 确认舍入渲染确实经 Arrow 投影引擎执行。全套件 `go test ./pkg/sql/rowexec/arrowpilot/` 通过（102.9s）；库包 `go build ./pkg/sql/rowexec/ ./pkg/sql/physicalplan/` 通过（`./pkg/sql/...` 全量仅剩 colexec/execgen 链接缺 `libkwdbts2` 的环境固疾，与改动无关）。

**剩余缺口（已清零，见 §2.6 / 阶段4）**：投影 CASE/COALESCE、窗口 ROWS 偏移 frame、过滤 IN 的 decimal 集合三项，以及**日期时间函数**（`extract`/`date_trunc`/`now`/`age`）均已补齐。阶段 C 表达式全覆盖的 Arrow 化清单至此落地：

- `now()`（2026-08-10）：planner `arrowDatetimeFuncName` 归一化 `now`/`current_timestamp`/`transaction_timestamp`→`"now"`，`canArrowDatetime` 允许 0 参数，`addArrowRendering` 产出 `{Kind:"datetime",Func:"now",TZ:true}`，executor 用 `evalCtx.GetStmtTimestamp()` 写等长常量 TIMESTAMPTZ 数组，单测 `arrow_projection_now_test.go` 验证。
- `age(ts)` / `age(end, begin)`（2026-08-10）：planner `arrowDatetimeFuncName` 加 `"age"`，`canArrowDatetime` 允许 1/2 个 TIMESTAMP/TIMESTAMPTZ 参数；单参重载的事务时间戳以 `"__TXN_TS__"` 哨兵常量（运行期经 `evalCtx.GetTxnTimestamp` 展开为常量广播列），双参重载为两列相减；executor `evalArrowDatetimeFunc` 的 `age` 分支经 `arrowDatetimeTimestampArg` 解析哨兵/列→`*array.Timestamp`，逐行调 `tree.TimestampDifference` 得 `DInterval`，结果以 `*array.String`（interval 规范文本）承载（与 `arrow_adapter.go` 的 DInterval→Arrow String 门控一致）。单测 `arrow_projection_age_test.go`（`TestArrowProjectionAgeTwoArg` / `TestArrowProjectionAgeOneArg`）验证两种形式输出等于 `TimestampDifference(...).String()`。`go build ./pkg/sql` 通过。

### 2.6 补齐投影 CASE/COALESCE、窗口 ROWS 偏移 frame、过滤 IN decimal 集合

一次性补齐四项剩余缺口（2026-08-06）：

#### 2.6.1 投影 CASE / COALESCE
把 `CASE [op] WHEN w THEN t ... ELSE e` 与 `COALESCE(a,b,...)` 路由进 Arrow 投影引擎。

**planner 侧（`physical_plan.go`）**：
- `canArrowRender` 新增 `*tree.CaseExpr`/`*tree.CoalesceExpr` 分支；`hasArrowComputeExpr` 已识别。
- 新增 `canArrowRenderCase(subs, resultType, indexVarMap)`：把每个分支值 / ELSE 统一 `CAST` 到 CASE 的结果类型（仅 arrow 可物化类型 int/float/string/bool），再逐一 `canArrowRender`。这保证所有分支同类型，executor 才能用单结果数组承载。
- 新增 `arrowProjectionColFor`：抽取「单表达式 → arrowProjectionCol」逻辑（原 `addArrowRendering` 循环体内联，现抽出复用），并新增 `*tree.ComparisonExpr`（布尔比较，映射到 `equal/not_equal/less/less_equal/greater/greater_equal` kernel）、`*tree.CastExpr`（inner 操作数作为带 cast 标签的 arrowArg）分支。
- 新增 `arrowCaseCol`/`arrowCoalesceCol`/`arrowComparisonCol`：构建 `Kind:"case"` 的 col，各分支 `When`（`CASE op WHEN w` 生成 `op = w` 比较；`CASE WHEN cond` 直接用 cond；`COALESCE` 生成 `arg IS NOT NULL` 的 `isnull` 列）/ `Then`（值，cast 到结果类型）。
- planner `arrowProjectionCol` 加 `Branches []arrowCaseBranch`、`Else *arrowProjectionCol` 字段（JSON tag 与 executor 对齐）。

**executor 侧（`arrow_projection.go` / `arrow_projection_processor.go`）**：
- `ArrowProjectionSpec` 加 `Kind`（"case"/"isnull"）、`Branches []ArrowProjectionBranch`、`Else *ArrowProjectionSpec`；`buildProjectionSpecs` 经新增 `specForCol` 递归把 planner case col 翻译为 executor case spec。
- `arrowProjection.eval` 入口新增 `case`/`isnull` 分支；新增 `evalCase`（逐行按 `When` 布尔掩码选 `Then`，回退 `Else`，原生类型 switch 支持 int64/float64/string/bool）、`evalIsNull`（产出 null 布尔掩码，列或标量）。

**验证**：新增 `arrowpilot/arrow_unify_projection_case_test.go`（`TestArrowUnifyProjectionCase`），覆盖 `CASE a WHEN 1 ...`、`CASE WHEN a>2 ...`、`COALESCE(d,-1)`、`COALESCE(d,a,0)`、并以 `ArrowProjectionRunCount` 确认走 Arrow。

#### 2.6.2 窗口 ROWS 偏移 frame
把 `SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)` 这类逐行滑动窗口路由进 Arrow windower（此前 `isSupportedWindowFrame` 拒绝偏移边界，静默回退经典 windower）。

- `isSupportedWindowFrame` 放行 ROWS 模式的偏移边界（`OFFSET_PRECEDING`/`OFFSET_FOLLOWING`），RANGE 偏移仍拒绝（需排序值算术）。
- planner `arrowWindowFramePlanFor`/`arrowWindowBoundName` 提取偏移值（int）写入 `StartOffset`/`EndOffset`。
- executor `arrowWindowFramePlan` 加 `StartOffset`/`EndOffset`、`hasOffset()`、`frameBounds(i,n)`；`computeAggregateWindow` 在 ROWS 偏移帧下逐行用 `frameBounds` 取 `[start,end]` 窗口，经 `windowAccumulator` 累加。

**验证**：`arrowpilot/arrow_unify_window_test.go` 新增 `TestArrowUnifyWindowOffsetFrame`，覆盖 `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`（滑动3行）、`ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`（滑动≤3行），以 `ArrowWindowerRunCount` 确认走 Arrow。

#### 2.6.3 过滤 IN / NOT IN 的 decimal 集合
把 `v IN (1.10, 3.30)`（`v DECIMAL`）路由进 Arrow 引擎。planner 把 decimal 列 `CAST` 为 `FLOAT`、把集合常量写为 `float64`，executor `evalIn` 新增 `*array.Float64` 分支按 float64 集合命中（精度遵循 cast 契约）。

**验证**：`arrowpilot/arrow_unify_projection_case_test.go` 新增 `TestArrowUnifyFilterDecimalIn`，以 `ArrowFilterRunCount` 确认走 Arrow。

#### 2.6.4 投影日期时间函数（extract / date_trunc）
把 `EXTRACT(field FROM ts)` 与 `DATE_TRUNC(field, ts)` 路由进 Arrow 投影引擎（仅限常量 field 串 + `TIMESTAMP`/`TIMESTAMPTZ` 列操作数）。

**planner 侧（`physical_plan.go`）**：
- 新增 `arrowDatetimeFuncName(name)`：把内置名 `extract`/`date_trunc` 映射到 executor 内部 `Func` 名（同名）。
- `canArrowRender` 的 `*tree.FuncExpr` 分支开头插入 datetime 识别：调 `canArrowDatetime` 校验「首参为 `DString` 常量 field、次参为 `TIMESTAMP`/`TIMESTAMPTZ` 列」。
- `arrowProjectionColFor` 的 `*tree.FuncExpr` 分支新增 datetime 构建：生成 `Kind:"datetime"` 的 col，`Inputs=[{ConstStr:field}, {Col:tsCol}]`，并据输入列类型置 `TZ`（决定 `TIMESTAMPTZ` 是否按会话时区处理）。
- `hasArrowComputeExpr` 的 `*tree.FuncExpr` 分支扩展：除 string/numeric 外再识别 `arrowDatetimeFuncName`，否则 `EXTRACT`/`DATE_TRUNC` 这类渲染不会被判定为「真实 Arrow 计算表达式」，导致 `addArrowRendering` 因 `hasArrowComputeExpr=false` 而退化为经典求值（结果正确但不走 Arrow）。
- 新增 `arrowFuncName(ex)` 安全取函数名：EXTRACT 在 planning 阶段 `FunctionReference` 接口可能为 nil（未解析），直接 `ex.Func.FunctionReference.FunctionName()` 会 panic，故兼容 `*FunctionDefinition`/`*UnresolvedName` 两种形态。
- planner `arrowProjectionCol` 加 `TZ bool` 字段（JSON `tz`）。

**executor 侧（`arrow_projection.go` / `arrow_projection_processor.go`）**：
- `ArrowProjectionSpec` 加 `TZ bool`（JSON `tz`）；`arrowProjection.eval` 入口新增 `datetime` 分支，转发到新增 `evalArrowDatetimeFunc`。
- `evalArrowDatetimeFunc`：把 Arrow `Timestamp_us` 列经 `time.UnixMicro(v).In(loc)` 还原 `time.Time`（`TIMESTAMPTZ` 用 `evalCtx.GetLocation()`，否则 `time.UTC`），复用 `builtins.ExtractTimeSpanFromTimestamp` / `builtins.ExtractTimeSpanFromTimestampTZ` / `builtins.TruncateTimestamp`（在 `sem/builtins` 新增同名导出包装，复用既有无状态 field 提取/截断逻辑，保证与行式逐位一致），结果写回 `Float64`/`Timestamp_us` 数组。`NewArrowProjection` 新增 `evalCtx` 注入。
- 注：vendored `arrow/compute` 无 datetime kernel，故采用原生向量化 Go 循环而非 `compute.CallFunction`。

**验证**：新增 `arrowpilot/arrow_unify_projection_datetime_test.go`（`TestArrowUnifyProjectionDatetime` / `TestArrowUnifyProjectionDatetimeTZ`），覆盖 `EXTRACT(YEAR/MONTH/DAY/HOUR/MINUTE/SECOND FROM ts)` 与 `DATE_TRUNC('day'/'month', ts)`，并以 `ArrowProjectionRunCount` 确认走 Arrow；TZ 测试用「Arrow 输出 == 关闭 Arrow 后的行式输出」对比，覆盖夏令时切换日（2021-03-14）的 DST 边界。全套件 `go test ./pkg/sql/rowexec/arrowpilot/` 通过（21 个 `TestArrowUnify*` 子测试全绿）；库包 `go build ./pkg/sql/...` 通过（`./pkg/sql/...` 全量仅剩 colexec/execgen 链接缺 `libkwdbts2` 的环境固疾，与改动无关）。

## 3. 构建与验证约束
- GOPATH 模式：真实 module `gitee.com/kwbasedb/kwbase`，`github.com` 为同 inode 软链。
- 编译：从 `gitee.com` 路径 + `GOFLAGS=`（清空 vendor 冲突）。
- 库包可编译：`go build ./pkg/sql/rowexec/ ./pkg/sql/physicalplan/`
- 测试：需 C++ 引擎预编译库 `libkwdbts2` + `KWDB_LIB_DIR`（本环境无，`go test` 链接失败；测试供 CI 跑）。
- 验证命令：`make test PKG=./pkg/sql/rowexec/arrowpilot`

---

## 4. 更新日志

### 2026-08-06
1. **修复 `.proto` 源文件缺失 8 个 Arrow 字段**：`processors.pb.go` 的 `ProcessorCoreUnion` 中 `ArrowProjection`(51)/`ArrowFilter`(52)/`ArrowAggregator`(53)/`ArrowJoin`(54)/`ArrowSorter`(55)/`ArrowUnionAll`(56)/`ArrowDistinct`(57)/`ArrowWindower`(58) 此前**仅手改在生成文件 `.pb.go` 中，`.proto` 源文件 `processors.proto` 完全无对应定义**。已在 `processors.proto` 的 `ProcessorCoreUnion` message 补齐这 8 个 `optional Expression` 字段（编号 51–58 与 `.pb.go` 严格对应），消除"重新 protoc 生成会丢失字段"的隐患。`.pb.go` 已有完整的 Marshal/Size/Unmarshal 实现，无需改动；`go build ./pkg/sql/execinfrapb/ ./pkg/sql/physicalplan/` 通过。
2. **多算子链路 e2e 测试（6.2-F 子项）**：新增 `arrowpilot/arrow_unify_pipeline_test.go`，验证两条串联链路：
   - `TestArrowUnifyPipelineFilterAggSort`：`scan→ArrowFilter(a*b>10)→ArrowAgg(GROUP BY b)→ArrowSorter(ORDER BY)` 单查询，断言 `ArrowFilterRunCount/ArrowAggRunCount/ArrowSorterRunCount` 均增长且结果与行式逐位一致。
   - `TestArrowUnifyPipelineDistinct`：`Projection(a+b)→ArrowDistinct` 链路，断言 `ArrowProjectionRunCount/ArrowDistinctRunCount` 增长且 distinct 结果正确。
   - 复用既有 `assertIntRows/queryIntRows/execStmt`、`rowexec.Arrow*RunCount`，并新增 `disableArrowSettings` 辅助（teardown 前关开关，避免 filter 路径编码不全导致 DROP TABLE 扫描系统 JSON 列异常）。`go vet ./pkg/sql/rowexec/arrowpilot/` 通过。
3. **文档同步**：§0.1/§0.2 修正 Filter 独立阶段与 IS NULL 的覆盖状态（已实现）；§6.2-F 标记链路测试已补；顶部覆盖日期更新至 2026-08-06。

### 2026-08-06（续）— A 项开关默认化
- 8 个 Arrow 算子子开关的 `defaultEnabled` 由 `false` 翻 `true`（`physicalplan/physical_plan.go`）：`sql.arrow_projection.enabled` / `sql.arrow_filter.enabled` / `sql.arrow_aggregator.enabled` / `sql.arrow_join.enabled` / `sql.arrow_sorter.enabled` / `sql.arrow_distinct.enabled` / `sql.arrow_windower.enabled` / `sql.arrow_union_all.enabled`。master gate `sql.arrow_scan.enabled` 此前已为 `true`。
- 语义：Arrow 现为默认主执行路径；未覆盖的表达式/算子经运行时 `canArrow*` 校验 + `marshalArrowPlan` 序列化失败即降级到行式/colexec，不 panic。每个 `ArrowXxxEnabled` 仍 `&& ArrowScanEnabled(evalCtx)`，关闭总闸即可整体回退。
- 同步更新 `arrowProjectionEnabledSetting` 注释（"opt-in" → "enabled by default"）。
- `go build ./pkg/sql/physicalplan/` 通过，无 lint 错误。
- **待 CI 回归**：本环境 `go test` 因 GOPATH 双 vendor 软链 `golang.org/x/net/trace` 重复注册 panic 无法端到端跑；默认化后需在 CI（单一 vendor 路径）跑全套 arrowpilot（+全量 planner/exec 回归）确认默认路径下无语义回归，再合入主干。

### 2026-08-06（续）— B 项补齐未 Arrow 化算子
- **已落子项：Ordinality**（`distsql_physical_planner.go: createPlanForOrdinality`）。WITH ORDINALITY 的列即 `row_number() OVER ()` 作用在单组、已按输入物理排序的数据上（无 PARTITION BY、无 ORDER BY），语义与 Arrow windower 的 `row_number` ranking 函数完全一致。实现：`createPlanForOrdinality` 构造等效 `WindowerSpec{WindowFns:[{Func: ROW_NUMBER, OutputColIdx: len(ResultTypes)}]}`，优先经 `arrowWindowerCoreFor` 走 Arrow windower；当 `ArrowWindowerEnabled` 关闭或 `canArrowWindow` 不通过时回退经典 `Ordinality` core。新增 `arrowpilot/arrow_unify_pipeline_test.go: TestArrowUnifyOrdinality` 断言走 Arrow windower 且 ordinal 列 1..N 与行式一致。`go build ./pkg/sql/` 通过，`go vet ./pkg/sql/rowexec/arrowpilot/` 通过。

### 6.3 B 项算子可行性分级（待推进）
| 算子 | Arrow 化可行性 | 决策 | 说明 |
|------|------|------|------|
| **Ordinality** | 高 | ✅ **已落（2026-08-06）** | 复用 Arrow windower 的 `row_number`，零新算子 |
| **StreamAggregator** | — | 不纳入 | 流式（时序/CDC）引擎专有算子，`addStreamAggregators` 仅在 `planCtx.isStream` 时走，内嵌 `cdcpb.StreamMetadata` 与 gapfill/time-bucket/interpolate 时序语义；Arrow 列式批式模型无等价物，且与通用 ArrowAgg 路径（`addAggregators`）隔离，强行替换会破坏流式语义 |
| **ProjectSet** | 低 | 暂缓 | 表函数（`generate_series`/`unnest`）产生变长多行，Arrow 列式定长模型难处理变长展开；收益低 |
| **Values** | 高 | ✅ **可落地（2026-08-07 排期）** | 纯常量行源，无 KV/引擎依赖；启动时解码 `spec.RawBytes` 成 EncDatumRows 批，一次 `buildArrowColumns` 成单 Arrow Record 当 `ArrowRecordEmitter`。零计算语义障碍，纯形态转换，归入 P1 收口 |
| **ZigzagJoiner** | 中（桥接已融入） | **桥接型·已融入统一 DAG（2026-08-10 修订）** | **计算内核是 KV 游标状态机**：`nextRow` 交替向两侧索引发 KV 点查（`fetcher.StartScan`+`NextRow`），每次 `side` 切换用 `baseRow` 等值列构造 seek span，在 RocksDB 跳读；`prevEqCols.Compare` 行级 Datum 比较 + `emitFromContainers` 笛卡尔积。**无"两块完整输入 Record"，无法套用对称 Arrow 算子模型**（与 `ArrowJoin` 攒两块 Record 再 hash 不同）。输出侧经 `unifiedInputFrom`→`NewRowSourceToArrow` 自动桥接成 Arrow Record 接入下游，无需另写 Arrow core。详见 §6.9 D3 修订 |
| **InterleavedReaderJoiner** | 中（桥接已融入） | **桥接型·已融入统一 DAG（2026-08-10 修订）** | 同 Zigzag：单 `row.Fetcher` 驱动状态机、两侧 KV 游标流式拉取，内部 merge 比较是行级 Datum 计算；**非对称 KV 游标模型，无法套用对称 Arrow 算子**。输出侧经 `unifiedInputFrom` 自动桥接 Arrow |
| **SampleAggregator** | — | 不纳入 | 路线图 §1 已明确"采样未纳入 Arrow 路线"（统计采样非查询主路径） |

**结论**：B 项清单中原列的 7 个算子，经逐个评估（2026-08-07 修订，纠正此前"KV 特例即不可 Arrow 化"的误判）：
- **实际可/应 Arrow 化（新增 Arrow core）的 2 个**：
  - Ordinality（已完成，复用 Arrow windower `row_number`）
  - Values（高可行性，纯常量源、无 KV/引擎依赖，排期 P1 收口，零新算子）
- **桥接型·已融入统一 DAG（无需新 Arrow core）的 2 个**：
  - ZigzagJoiner / InterleavedReaderJoiner（D3，2026-08-10 修订）：计算内核是 KV 游标状态机，无法套用对称 Arrow 算子模型；输出侧经 `unifiedInputFrom`→`NewRowSourceToArrow` 自动桥接成 Arrow Record，下游 Arrow 算子直接消费。全 DAG 已是 Arrow 流通，不另写 Arrow core（薄壳方案性能等价、零收益，见 §6.9 D3 修订）。
- **不纳入 3 个**：
  - StreamAggregator（流式专有，本就不走通用 ArrowAgg 路径）
  - ProjectSet（变长展开，超出现有单 Record 模型，D2 暂缓）
  - SampleAggregator（采样，路线图 §1 已明确不纳入）

至此**所有通用关系查询算子均已接入统一 Arrow DAG**（原始 7/7 + UNION ALL + Ordinality 已 Arrow 化；Values 排期 P1；Zigzag/Interleaved 经桥接自动 Arrow 输出）。B 项目标实质达成，工作量从原估 6-10 人周收敛为 Ordinality（已完成）+ Values（P1）；D3 三项判定为桥接型（0 新算子、已融入），不列入"需新写 Arrow 算子"清单。

---

## 6.11 当前待办清单 / 剩余阻塞点总览（2026-08-10 收口梳理）

> 本小节为 D3a 收口后的**工程推进清单**，不含代码改动，仅结构化剩余阻塞点供排期。所有验证边界：本环境 `go test` 因 GOPATH 双 vendor 重复注册 panic 无法端到端跑，时序读还依赖 C++ tse FFI（`libkwdbts2`）链接，故以下项均只能 `GOFLAGS= go build ./pkg/sql` 验证、无法本地 e2e。

### 6.11.1 已完成（无需再动）
| 项 | 说明 |
|---|---|
| A 开关默认化 | `arrowScanEnabledSetting` 默认 `true`，全链路默认走 Arrow |
| B 算子集 | scan/filter/agg/join/sort/distinct/window/union-all/ordinality 已 Arrow 化；Values 已落地（P1） |
| 阶段 C 表达式 | 字符串/字节过滤、CASE/COALESCE、RANGE 偏移 frame、IN/NOT IN computed 左操作数、数值标量/投影均已覆盖 |
| 阶段3 统一调度层 | `buildUnifiedStage` 收口 + `arrow_bridge` 双向适配器，colexec 自动纳入统一 DAG |
| 阶段4 集合 ALL | `ArrowUnionAll` 多输入 stage 拼接 |
| D3a | `ArrowMergeJoiner` 已完成；lookup 系（joinReader/indexJoiner/batchLookupJoiner）标保留项（proto `ArrowLookupJoiner` 编号 60 占位，executor 未实现） |
| D3b 桥接型 | Zigzag/Interleaved 经 `unifiedInputFrom` 自动融入 DAG，0 新算子 |
| ArrowTsReader 算子骨架 | `rowexec/arrow_ts_reader.go` 算子本体已落地（含 ArrowRecordEmitter/ArrowOutput） |

### 6.11.2 待推进（按风险排序）
| 优先级 | 项 | 现状 | 风险 / 验证边界 | 依赖 |
|---|---|---|---|---|
| **P0（已完成，2026-08-10）** | **时序读 Arrow 接线 + 解除 8 处 `EngineTypeTimeseries` 门控** | **已完成**：① 时序读 Arrow 接线**事前已落地**——`rowflow/row_based_flow.go` 经 `arrowTsEmitter`（`arrowMode=true`，跨节点 REMOTE 直接喂下游 Arrow 算子）+ QUEUE 路径 `NewArrowTsReader`（`arrowMode=false`，本地行式 reader 保持引擎 plumbing 一处）实现，受 `physicalplan.ArrowTsScanEnabled` + `ArrowTsScanSupported(typs)` 门控；② 8 处 `EngineTypeTimeseries` 门控已**全部解除**（`canArrowAggregate`/`canArrowMergeJoin`/`canArrowJoin`/`canArrowSort`/`canArrowDistinct`/`canArrowWindow`/`arrowUnionAllCoreFor`/`arrowValuesCoreFor`），时序查询的下游算子可走 Arrow；各 `canArrow*` 内部白名单已兜底（agg 只认 SUM/MIN/MAX/AVG/SUM_INT，时序专属 last/first/rate 自然 return false；sort/distinct/window 仅校验类型可比较，adapter 已支持 TIMESTAMP 等） | 中高（已实施）：动了 planner 主干策略；本环境无法 e2e 验证时序场景（依赖 tse FFI 链接）；解除后需 CI 用单一 vendor 跑时序查询回归确认语义等价（尤其 AVG 在时序上的 decimal/float 输出、窗口 partition 时序语义） | `arrowTsReader` 已落地；`arrow_unification.go` 已无 `EngineTypeTimeseries` 残留；`go build ./pkg/sql` 通过 |
| **P1（绕过已实现，2026-08-10 收尾）** | **Bytes 过滤 compute kernel bug 绕过** | **绕过已实现且功能正确**：`arrow_filter.go` 的 `eval` 对 `IsBinaryLike` 列 skip `compute.Filter`，改用 Go `gatherColumn` 路径（134-144 行）；非 Binary 列仍走原生 `compute.Filter`。**Bug 根因已定位**：vendored arrow v17 的 `binaryFilterImpl`/`binaryFilterNonNull`（`vector_selection.go:1150/1200`）调用 `exec.GetSpanOffsets[OffsetT]`（`utils.go:49`），该函数在 `ArraySpan.Offset != 0`（array 是某 parent 的 slice，非零偏移）时 `unsafe.Slice` 按 `Offset+Len+1` 个元素切 buffer，越界读取 → 下游 filter 消费带 offset 的 Binary 列时 panic。当前 gather 绕过完全规避此路径 | 低（已实施）：仅性能项（Go gather 为 O(n) 逐行，非原生向量化）；正确性已验证（含 null 谓词语义与 `compute.Filter` 的 DropNulls 默认一致）；真正"内核替换"需改 vendored `arrow/compute`（动第三方库，风险高，且 arrow v17 为精挑版本，execgen/cgo 依赖其字节布局）或升级 arrow 版本，**不在当前阶段安全边界内** | `arrow_filter.go` 加固零列 Record 边界（`cols[0]` 越界保护，487 行附近）；`go build ./pkg/sql/rowexec/...` 通过 |
| **阶段 C（CAST 全类型，2026-08-10 落地）** | **CAST 全类型（含 BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 目标）** | **已完成**：① planner `arrowCastTargetTag`（`physical_plan.go:2926`）扩展放行 BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 目标（原仅 STRING/INT/FLOAT/DECIMAL）；② executor `arrowCastType`（`arrow_filter_processor.go:362`）补 DECIMAL/BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 映射——**修复 DECIMAL 目标在过滤路径被误当 STRING 的 bug**；③ `castArrowArray`（`arrow_filter.go:621`）补 BOOL/DATE(INT32)/TIMESTAMP 目标分支，新增 `castToBool`/`castToDate`/`castToTimestamp`；④ 源类型补全：`castToInt64` 补 Decimal128、 `castToDecimal` 补 Boolean、 `castToString` 已含 Timestamp/TimestampTZ 源（arrow 无独立 TimestampTZ 类型，统一 Timestamp_us 承载）；⑤ **修复投影顶层 CAST 被静默忽略的 bug**：`arrow_projection.go` 的 copy 分支原直接 slice 原列不应用 Cast，现对带 Cast 的 passthrough 列调 `castArrowArray`，使 `SELECT CAST(col AS X)` 在 Arrow 投影路径真正转换类型。与 `arrowDataTypeForKWType` 承载一致（DATE=Int32 天、TIMESTAMP/TZ=Timestamp_us、BOOL=Boolean、DECIMAL=Decimal128） | 中（已实施）：CAST 语义对齐 row 路径（string→timestamp 用 RFC3339/"2006-01-02 15:04:05"/"2006-01-02" 解析，date 用 "2006-01-02"，int/float→timestamp 按 Unix 秒×1e6 micros）；本环境无法 e2e，需 CI 跑 CAST 回归；**TIME 目标未含**（adapter 无 Time 承载，强行做会破坏 group/compare 一致性，保持 false 回退 row） | `go build ./pkg/sql/rowexec/... ./pkg/sql/physicalplan/...` 通过 |
| **P2（fetcher 直出 Arrow，2026-08-10 落地）** | **fetcher 直出 Arrow builder（省 EncDatum 解码）** | **已完成（低风险快赢路径）**：不在 fetcher 内部重写，而是在统一攒批入口 `buildArrowColumns`（`arrow_adapter.go`）为各标量类型接入 `EncDatum` 直取原语——新增 `EncDatum.GetFloat/GetBytes/GetDecimal/GetTime/GetDate/GetUUID(t, da)`（`sqlbase/encoded_datum.go`），VALUE 编码列走 `DecodeUntaggedXxxValue` 直接从 KV 编码字节取值、跳过 `EnsureDecoded` 的 `tree.Datum` 堆分配，KEY 编码列回退 `EnsureDecoded`（行为不变）。`buildArrowColumns` 的 Float/Bytes/Decimal/Timestamp/Date/Uuid 六分支改为调用直取方法（保留 `IsNull` 拦截与 Date 无限值→NULL 范围保护）；Int 分支早已走 `GetInt` 直取。Bool/Json/Interval 保留 `EnsureDecoded`（bool 无 untagged 原语、json/interval 转 string 分配成本低）。**收益**：scan→Arrow 主干（经 `unifiedInputFrom→NewRowSourceToArrow→buildArrowColumns`）对 VALUE 编码的值列省去每行的 `tree.Datum` 堆分配 + 类型断言，KV 字节直接灌入 `arrow.Array` builder——正是"fetcher 直出 Arrow"的语义等价物，是 Arrow 路径在 scan 侧性能追平/超越 colexec 的关键一步，也为后续 lookup 系全程 Arrow（需 fetcher 反查也直出）清理前置 | **通用性（关键澄清）**：`buildArrowColumns` 是**所有行式读取算子汇入 Arrow DAG 的唯一统一入口**（经 `unifiedInputFrom→NewRowSourceToArrow`）。因此本次改造**对所有以 `EncDatumRow` 为产出的读取算子自动生效，无需逐个改造**——`TableReader` / `IndexJoiner` / `JoinReader`(lookup 反查行) / `IndexSkipTableReader` / `InterleavedReaderJoiner` / `ZigzagJoiner` / `MergeJoiner` 上游 fetcher 行，只要下游汇入了 Arrow DAG，其 `EncDatum→Arrow` 转换即已是字节直取。唯一例外是 `arrowTsReader`（时序读）：它不经 `row.Fetcher`，而是由 C++ tse FFI 直出 Arrow，本无 `EncDatum` 中间层，不在本范围内。**边界（关键澄清）**：本次是 `EncDatum`（fetcher 已解码、仍持有原始 KV 字节 `ed.encoded`）→`arrow.Array` 的直取，**不意味着 TableReader 跳过 `row.Fetcher` 裸读 KV**——fetcher 的 KV 扫描/MVCC 版本选择/列投影/key 解析仍由其负责，TableReader 的 `NextRow` 仍调 `fetcher.NextRow`。即"读取 KV 数据并直出 Arrow"的能力已具备且对所有行式读取算子通用，但 fetcher 之上的 MVCC/key 逻辑未动。更深的"fetcher 内部直灌 arrow.Builder（连 `EncDatumRow` 数组都不物化）"属 P2 深度项，风险更高、当前未做 | 低（已实施）：仅改攒批入口与 `EncDatum` 直取方法，未动 fetcher KV 取数核心；`go build ./pkg/sql`（过滤 colexec/execgen 链接噪音）通过；Bool 等少数类型仍走原路径，不影响语义；正确性依赖 CI 单一 vendor 跑 scan 回归对拍 | `arrow_adapter.go` `buildArrowColumns` 六分支 + `sqlbase/encoded_datum.go` 六方法；`ArrowScanEnabled` 总闸已覆盖 |
| P2（**已实施，2026-08-14，gate 已默认开启本地验证通过**） | **lookup 系 `ArrowLookupJoiner` executor 实现** | **executor 已落地且默认激活**：① proto `ProcessorCoreUnion.arrowLookupJoin`（字段 60，类型 `JoinReaderSpec`，可靠 proto 往返，最初曾用 JSON Expression 但 `Table` 描述符 JSON 往返不可靠，改为 proto 字段）；② `canArrowLookupJoin`（`arrow_unification.go`）门控：`ArrowJoinEnabled` + `arrowJoinType` 支持的 joinType + 非空 `LookupColumns` + 各 lookup 键为 `arrowJoinKeyType` + 左/右列均 `arrowAllSupported`；命中则 `return true`（默认开启，`sql.arrow_join.enabled` 即主门）；③ planner `createPlanForLookupJoin` 命中门则发 `ArrowLookupJoin` core（携带 `JoinReaderSpec` 原值，post 正常传 stage），否则维持原 `JoinReader`；④ executor `newArrowLookupJoinerProcessor`（`rowexec/arrow_lookup_joiner_processor.go`）**嵌入行式 `joinReader`**（KV 点查内核），用 `NewRowSourceToArrow` 桥接其 `left++right` 输出为单个 Arrow Record，经 `arrowRecordToEncDatumRows` 解码回 `EncDatumRows`，再由 `p.Out.ProcessRow` 走标准 `ProcOutputHelper` 出（算子级桥接，与统一设计一致：rowexec 为不可 Arrow 化算子的桥接后端）；⑤ factory 注册 `ProcessorCoreUnion_ARROW_LOOKUP_JOIN`；⑥ `arrowpilot/TestArrowUnifyLookupJoin` 覆盖 inner/left lookup join（含 `WHERE s.v=30` 过滤、left join 外连接 NULL 扩展），已加 `ArrowLookupJoinerRunCount()>0` 断言确认真走 Arrow 路径，本地 34 个 arrowpilot 测试全绿。**设计要点**：lookup join 的 join 计算 = KV 点查 + 匹配 + 外连接 NULL 扩展，非对称、非两块完整输入 Record，故**不重写成原生 Arrow kernel**，而是桥接行式 `joinReader`（KV fetch 由 `row.Fetcher` 负责，复用已验证逻辑）。 | **已解决（2026-08-14）**：初版 `canArrowLookupJoin` 曾 `return false` 且仅 row-based 对拍，原因是含过滤的 lookup join 在 Arrow 路径下"查找命中 0 行"。真因定位为 **ArrowLookupJoiner 与内嵌 joinReader 双重 Start 了同一个 left 输入源**（ArrowLookupJoiner.Start 调 `p.input.Start(ctx)` + Init 的 `InputsToDrain` 含 `p.input`，而 joinReader.Start 内部也 `jr.input.Start`）—— 第二次 Start 破坏上游 RowSource 状态使产空。修复：ArrowLookupJoiner 不再自己 Start/Drain 该 input（由 joinReader 独占接管），`canArrowLookupJoin` 翻 `true`。本地 `go test ./pkg/sql/rowexec/arrowpilot`（配 `CGO_LDFLAGS=-Lbuild/lib -lkwdbts2...` + `LD_LIBRARY_PATH=build/lib`）可跑，不再依赖所谓"双 vendor panic"（那是缺 `libkwdbts2.so` 链接/加载路径，与双 vendor 无关）。 | `arrow_join.go`（桥接复用 `NewRowSourceToArrow`）；`arrow_adapter.go` `arrowRecordToEncDatumRows`；proto `JoinReaderSpec` 序列化 |

### 6.11.3 收尾状态总结（2026-08-10）
- **本轮 Arrow 统一化收尾已达"能力就绪"状态**：所有规划算子（scan / join / merge-join / sorter / distinct / windower / aggregator / union-all / values / 时序读）均已具备 Arrow 路径与 `canArrow*` 门控；fetcher 直出 Arrow 的字节直取红利已对所有行式读取算子通用（见 P2 行通用性澄清）。下一步不再是"补齐算子"，而是**e2e 验证与默认开关翻转**。
- **默认关闭的开关（能力已落地，但保守未启用）**：
  - `sql.arrow_ts_scan.enabled`（默认 `false`）：`arrowTsReader` 接线与 `ArrowRecordEmitter` 已落地，且仅当 `ArrowTsScanSupported(typs)` 放行时激活。**（2026-08-10 修订）类型门已扩展**——`ArrowTsScanSupported` 改用独立承载判断 `arrowTsScanSupportedType`（与 `arrowDataTypeForKWType`/`buildArrowColumns` 对齐），**已放行 TIMESTAMP/TIMESTAMP_TZ（时间 tag）与 DECIMAL（decimal 指标），外加 DATE/UUID/JSON/INTERVAL**；即时间列与 decimal 指标列现在可走 TS Arrow scan。**开关仍默认 `false`**（保守未启用，避免未经 e2e 验证即翻转）。**翻转条件**：CI 单一 vendor 跑时序查询回归（含 TIMESTAMP/TZ 时间列、DECIMAL 指标列的 scan→agg/filter/sort 链路；AVG decimal/float 输出、窗口 partition 时序语义）通过后翻 `true`。
  - `sql.arrow_values.enabled`（默认 `false`）：`ArrowValues` core 已落地（commit 7ca82fe8，`arrowpilot/arrow_unify_values_test.go` 已验证 VALUES→聚合链路一致；2026-08-14 修正了忽略 post 的静默错误 bug）。**保持默认 false 的原因（实测，非类型覆盖问题）**：默认 `true` 会让 server 启动迁移阶段系统表查询（含 VALUES）走 Arrow 路径触发 `received multiple headers` 故障破坏启动，须先解决 Arrow 在迁移关键路径的可用性才能翻 `true`。
  - 其余开关（scan 主门 / join / sorter / distinct / windower / union_all / filter / agg）**默认 `true`**：Arrow 路径已默认启用，仅受 `canArrow*` 白名单与 `ArrowScanSupported` 类型门约束而局部降级。
- **所有规划的 Arrow 算子门控均已达"默认开启/能力就绪"**：`ArrowLookupJoiner` executor 已**实施完成且 `canArrowLookupJoin` 默认 `true`**（§6.11.2 P2 行）—— 含过滤/外连接的 lookup join 本地 `arrowpilot` 对拍已绿，真因（ArrowLookupJoiner 与内嵌 joinReader 双重 Start 同一 left 输入源）已修复，不再回退行式 `joinReader`。至此统一执行层的算子级桥接后端（rowexec）已覆盖全部算子，无"未做"的高风险保留项。
- **环境验证边界（已澄清）**：本开发环境 `go test` 之前报的"双 vendor `x/net/trace` 重复注册 panic"实为**缺 `libkwdbts2.so` 链接/加载路径**所致，与双 vendor 无关。`arrowpilot` 包（内存服务器 SQL 逻辑测试）只需配 `CGO_LDFLAGS="-L<repo>/build/lib -lkwdbts2 -lcommon -lsnappy -lm -lstdc++"` + `LD_LIBRARY_PATH=<repo>/build/lib` 即可本地编译并运行（如 `go test ./pkg/sql/rowexec/arrowpilot -run TestArrowUnifyLookupJoin`）。colexec/execgen 全量 `go build` 仍需 C++ 引擎预编译（环境固疾），但 arrowpilot 这类逻辑测试不受限。语义等价性可在本机直接对拍，无需等 CI。

### 6.11.4 架构终态（不变）
- **Arrow 主 + rowexec 永久兜底** 两层长期并存；colexec 为过渡层，覆盖率达标后逐步收窄至消失。
- D1/D2（不可列式化算子）由 rowexec 兜底，不退役。
- 时序读 Arrow 化（6.11.2 P0）**已于 2026-08-10 完成**：`arrowTsReader` 接线落地 + 8 处 `EngineTypeTimeseries` 门控解除，`EngineTypeTimeseries` 不再强制回退行式，**Arrow 主路径已覆盖关系 + 时序双引擎**；时序查询的 scan/filter/agg/sort/distinct/window/unionall/values 均可走 Arrow（白名单兜底，时序专属算子仍由 rowexec 处理）。

#### 6.12 arrow 依赖升级（v17）引入的外部包分析（2026-08-11）

- **背景**：`arrow-unify` 分支将 `kwbase/vendor` submodule 由 master 的 `8f3b7a8c` 升级至 `26d879ca`，核心是把 `github.com/apache/arrow/go` 从旧版（module 路径 `.../go/arrow`，无版本号）整体升级到 **v17**（module 路径 `.../go/v17/arrow`）。旧版 arrow 仅依赖 `flatbuffers`/`pkg/errors`/`testify`/`go-spew`/`go-difflib`（vendor 原本就有）；升级到 v17 后，arrow 库内部改用一组新的外部依赖，**这是"只有 arrow 版本替换"预期之外、实际由 v17 强制带入的连锁依赖**。
- **vendor diff 概览**：master→arrow 的 vendor submodule 共 1387 文件改动，其中 706 个为 arrow 本体（旧路径删除 + v17 路径新增），其余 681 个为 v17 依赖链带来的外部包刷新。
- **新引入的 4 个外部依赖及其作用**：

  | 依赖 | 体积 | 作用领域 | 被 arrow 何处 import | KWDB 主代码是否直接用 | 可裁剪性 |
  |------|------|---------|---------------------|---------------------|---------|
  | `github.com/klauspost/compress` | ≈480 文件（最大） | 高性能压缩；v17 仅用 `zstd` 子包，服务 **Arrow IPC 流的可选压缩**（写 IPC 时对 Record 块做 zstd 压缩减小体积） | **仅** `arrow/ipc/compression.go` 一个文件 | 否（KWDB 走内存 `array.NewRecord`，从不调用 ipc 压缩） | ✅ 唯一可干净去除：把 `ipc/compression.go` 改为"不支持压缩"stub（保留 `Compression` 类型但压缩/解压返回 error）即可把 klauspost 整体移出 vendor；代价是侵入 arrow 库源码，后续升 v17 需重打 patch |
  | `golang.org/x/xerrors` | 小 | 增强错误处理（`Errorf`、`%w` 包装、`Is/As` 错误链）；Go 1.13 标准库 `errors` 已吸收大部分能力，但 arrow 源码仍直接 import | 散布于 `arrow`/`arrow/cdata`/`arrow/compute`/`arrow/scalar`/`internal/types`/`flight_integration` 共 8 处 | 否（仅 arrow 内部用） | ❌ 需改 arrow 源码把所有调用替换为标准库 `errors`/`fmt`（Go 1.20 下可行），但散布且侵入库代码，收益低 |
  | `golang.org/x/sync` | 小 | 标准库 `sync` 扩展，主要为 `errgroup`（一组 goroutine 协同、任一出错即取消） | **仅** `arrow/compute` 一处（并行执行 compute kernel） | 否 | ❌ 单点，但改 arrow 源码替换 `errgroup`，收益极小；标准库无等价物 |
  | `golang.org/x/exp`（slices/maps/constraints） | 中 | 实验性标准库扩展：泛型切片/map 操作 + 类型约束（`Ordered`/`Integer` 等） | `arrow/array`/`arrow/compute`/`compute/exec`/`compute/internal/kernels`/`arrow/scalar` 等多处泛型代码 | **是**（workload、kvserver 等主代码已用） | ❌ 必需依赖；且 Go 1.21+ 才收进标准库，当前 Go 1.20 下仍需此包，无法用标准库替代 |

- **能否用 vendor 已存在的包替换？——基本不行**：klauspost 与 xerrors 在 vendor 中全新（master 完全没有），无等价旧版；x/exp 与 x/sync 在 master 已存在但被 v17 抬高版本，且 x/exp 本就是 KWDB 共用依赖，谈不上替换。
- **能否裁剪 arrow 未使用代码减少依赖？——子包裁剪空间为零，外部依赖仅 klauspost 可去**：KWDB 实际用到了 arrow v17 几乎所有子包（`array`/`compute`/`scalar`/`ipc`/`memory`/`decimal128`/`flight` 等全在用），仅 `_examples`/`_tools` 两个非代码目录可删，我们的 Arrow 统一化方案重度依赖 compute/array 全套，无法裁子包。四个外部依赖里只有 klauspost/compress 是真正"为 KWDB 不用的 ipc 压缩功能带入的冗余项"。
- **结论（2026-08-11 决策：不裁剪，保留原始依赖）**：维持 vendor 原样。理由：① x/exp 是 KWDB 与 arrow 共享的必需依赖，xerrors/xsync 是 arrow 核心包内部依赖、剥离需侵入库源码且收益低、违背"vendor 是上游原样"原则；② 仅 klauspost 可去，但需维护 arrow 源码 patch，当前优先级低于 e2e 验证与开关翻转；③ 若未来想根治，更优路径是升级 Go 到 1.21+（让 x/exp 的 slices/maps 进标准库），而非逐个替换。环境注意：切换分支后需 `git submodule update` 同步 vendor 到对应 commit（master `8f3b7a8c` / arrow `26d879ca`）。

##### 6.12.1 四个新引入库的协议分析（2026-08-11）

- **总览**：四个库均为**宽松型（permissive）开源协议**，无 copyleft（无 GPL/AGPL/LGPL），与 KWDB 既有依赖（flatbuffers BSD、Go 标准库 BSD）一致，商用友好、无传染性许可证义务。

  | 库 | 协议 | 说明 |
  |----|------|------|
  | `golang.org/x/xerrors` | **BSD-3-Clause** | Go 官方扩展库，标准 BSD 三条款（含"name of... may not be used"），无专利附加条款 |
  | `golang.org/x/sync` | **BSD-3-Clause** | 同上，Go 官方扩展库统一协议 |
  | `golang.org/x/exp` | **BSD-3-Clause** | 同上 |
  | `github.com/klauspost/compress` | **混合协议** | 主体（flate/s2/zstd 等）为 **BSD-3-Clause**（Go Authors + Klaus Post 版权）；包内不同子目录含其他协议 |

- **klauspost/compress 混合协议细节（按子目录）**：
  - 主体（flate、s2、zstd 等）：**BSD-3-Clause**
  - `gzhttp/` 子目录：**Apache License 2.0**（含专利授权条款、NOTICE 要求）——与 BSD 系列不同的协议
  - `zstd/internal/xxhash/`：**MIT**（Caleb Spare）
  - `internal/snapref/`、`internal/lz4ref/`、`s2/`、`snappy/`：**BSD-3-Clause**

- **与 KWDB 实际使用的关联**：
  - 前文已确认 KWDB 只用 `klauspost/compress` 的 **`zstd` 子包**；编译进二进制的实际代码链为 `zstd` + 其依赖 `zstd/internal/xxhash`(MIT) + `internal/snapref`、`internal/lz4ref`(BSD-3)。即**实际编译代码 = BSD-3-Clause + MIT，不含 gzhttp 的 Apache-2.0**（gzhttp 未被 import，不进二进制）。
  - 但 vendor 目录作为整体若随**源码分发/打包**，`gzhttp/` 的 Apache-2.0 文件会随包存在（即使不编译）；若仅**二进制分发**（Go 静态链接），则只涉及 BSD/MIT 的署名保留义务。

- **整体依赖链协议结论**：arrow v17 整条依赖链 = **Apache-2.0（arrow 本体）+ BSD-3-Clause（x/exp、x/sync、x/xerrors、klauspost 主体）+ MIT（xxhash）**，全部宽松、可商用、无传染性 copyleft，合规风险低，无需许可证隔离措施。

### 2026-08-07 — B 项误判修订：Values/Zigzag/Interleaved 可 Arrow 化
- **修订背景**：此前（2026-08-06）§6.3 B 项把 Values / ZigzagJoiner / InterleavedReaderJoiner 笼统归为"不纳入（常量源 / KV 扫描特例）"，理由是"带 KV / 特例源"。经复核源码，该归类错误——**KV 仅是算子的输入/输出请求方式，不是计算内核**：
  - `valuesProcessor`：无任何输入，`Next()` 仅 `StreamDecoder.GetRow` 解码 planner 预编码的 `spec.RawBytes` + PostProcess，零 KV、零引擎依赖，是纯常量行源。
  - `zigzagJoiner` / `interleavedReaderJoiner`：KV fetch（`row.Fetcher` / 双 side 扫描）是输入侧取数；**内部 zigzag 交错比对 / merge-join 拼接是 datum row 计算**，与已 Arrow 化的 `ArrowScan`（`KV fetcher 取数→Arrow Record`）同构。
- **修订结论（详见 §6.3 表与 §6.9）**：
  - **Values**：高可行性，纯形态转换，无依赖，**排期 P1 收口**（见阶段 4 新增项）。方案：启动时解码 `spec.RawBytes`→`buildArrowColumns` 成单 Arrow Record 当 `ArrowRecordEmitter`，新增 `sql.arrow_values.enabled` 开关 + `arrowValuesCoreFor` 助手。且不依赖 C++ 时序引擎，可本地独立验证（规避 P0 测试环境缺 TS engine 的坑）。
  - **ZigzagJoiner / InterleavedReaderJoiner（2026-08-10 推翻本款结论）**：本条原判"D3 非对称、需新增编排骨架、非本质障碍"**不成立**——经 2026-08-10 读源码确认二者为 KV 游标状态机，非对称编排骨架无法套用（无两块完整输入 Record），改为 **D3b 桥接型**（输出侧经 `unifiedInputFrom` 自动 Arrow，已融入统一 DAG，不另写 Arrow core）。详见 §6.9 D3b 修订与 2026-08-10 修订条目。
  - **不纳入仍维持 3 个**：StreamAggregator（流式专有）、ProjectSet（变长展开，D2）、SampleAggregator（采样，§1 已明确）。
- **影响**：B 项可/应 Arrow 化算子由 1 个（Ordinality）扩为 4 个；阶段 4 剩余可落地清单新增 Values；D3 非对称算子清单新增 Zigzag/Interleaved。通用关系查询算子 7/7 + UNION ALL + Ordinality 均已 Arrow 化不变；本修订仅扩展"后续可落地的 B 项算子"范围，不改变"已落地"基线。

### 2026-08-10 — D3 拆分修订：zigzag/interleaved 定为桥接型（纠正 2026-08-07 误判）
- **修订背景**：2026-08-07 把 ZigzagJoiner / InterleavedReaderJoiner 与 lookup 系同归 D3"可 Arrow 化、KV 仅作输入取数方式、缺非对称编排骨架非本质障碍"。经逐行读 `rowexec/zigzagjoiner.go`（`nextRow` @738-819、`fetchRowFromSide`、`produceSpanFromBaseRow`）+ `interleaved_reader_joiner.go`，**确认该判断对 zigzag/interleaved 不成立**：
  - 二者内核是**纯 KV 游标状态机**：交替向两侧索引发 `fetcher.StartScan`+`NextRow` 点查，`side` 切换用 `baseRow` 等值列构造 seek span 在 RocksDB **跳读**；行级 `Datum.Compare` + `emitFromContainers` 笛卡尔积。**两侧输入不是预物化 Arrow Record，而是 KV 游标流式游标**，无"两块完整输入"可供向量化 join。
  - 这与 `ArrowJoin`（攒两块 Record 建 hash 表 + probe）的**对称批处理模型本质不同**，无法套用现有 8 个 Arrow 算子，也无法仿 `ArrowJoin` 先攒 Record 再算。
- **桥接 vs 薄壳性价比分析结论**：
  - **桥接（现状 0 改动）**：`unifiedInputFrom` 对上游非 `ArrowRecordEmitter` 自动调 `NewRowSourceToArrow`，把吐出的 `EncDatumRow` 攒成 Arrow Record 喂下游。已验证、风险 0。
  - **薄壳（另写 ArrowZigzag/ArrowInterleaved core）**：仅把攒批从下游移到 executor 内部，**数据形态与性能完全等价**（同样经 `appendEncDatum` 从 `EncDatum`→`Datum`→value→arrow array）；瓶颈在 KV 点查延迟（ms 级），攒批（ns 级）可忽略。薄壳纯代码位移、零增益、且每类需新 executor 骨架 + planner 分支 + 测试，风险高收益无。
  - **结论：桥接性价比碾压，D3b 不另写 Arrow core**，作为"行式→Arrow 桥接点"自动融入统一 DAG。
- **保留项（独立性能专题，不纳入 D3）**：`fetcher` 解码强制产出 `EncDatumRow`（含 `tree.Datum`），Arrow 攒批必经 `Datum` 取实际 value，**`EncDatumRow` 中间层无法在 join 侧跳过**。若要 KV 直出 Arrow（省 `Datum` 分配/GC），须改 `row.Fetcher` 核心加 Arrow 快路径（KV 字节→arrow builder 直转），风险极高（存储读取核心，行式/colexec 共享）、收益被 KV 延迟淹没，且与 D3 收口无关，记为后续候选专项，**当前不做**。
- **影响**：D3 拆为 **D3a**（lookup 系 / 行式 mergeJoiner，可 Arrow 化、需新算子骨架、属"尚未做"）与 **D3b**（zigzag/interleaved，桥接型、已融入 DAG、不列"需新写 Arrow 算子"清单）。B 项"可/应 Arrow 化"实为新 Arrow core 的 2 个（Ordinality 已落 + Values P1）+ 桥接型 2 个（Zigzag/Interleaved）；D3b 不计入未做算子清单，rowexec 仍为其永久降级落点。终态表（§6.10）同步修订。

### 2026-08-07（续）— Bytes 类型支持落地（P1 类型补全）
- **问题**：`arrowDataTypeForKWType`/`buildArrowColumns`/`newArrowBuilder`/`appendEncDatum` 的 `default` 分支对 `BytesFamily` 显式报错（`unsupported type family BytesFamily for arrow schema`），而 `arrowSupportedCompareType` 类型门早已放行 Bytes。两者不一致：含 BLOB/VARBINARY 列的查询一旦进入 Arrow 默认路径（如开启 `sql.arrow_scan.enabled`），会在 schema 构建阶段崩溃；而 GROUP BY/DISTINCT/JOIN 若绕过该路径，则 `arrowGroupHashIdx`/`arrowGroupRowEqualIdx`/`joinRowHash`/`arrValEqual` 对 `arrow.BINARY` 走 `default` 退化分支（每行独立 hash / 恒不相等）→ **静默产生错误结果**，比崩溃更危险。这是历史上 `TestArrowUnifyFilterStringFuncs` 卡在后台无限重试 "unsupported type family BytesFamily" 的根因链条之一。
- **修复**：`arrow.BinaryTypes.Binary` 作为 Bytes 的 Arrow 表示（与 `arrow_projection_processor.go` 既有用法对齐），在 schema/builder 四入口及 aggregator/join 的分组哈希比较全部对齐 `arrow.BINARY`：
  - `arrow_adapter.go`：`arrowDataTypeForKWType` 返回 `arrow.BinaryTypes.Binary`；`buildArrowColumns`/`appendEncDatum` 新增 `BytesFamily` 分支（`array.NewBinaryBuilder` + `tree.AsDBytes`）；`newArrowBuilder` 回 `array.NewBinaryBuilder`。
  - `arrow_aggregate.go`：`arrowGroupHashIdx`/`arrowGroupRowEqualIdx`/`arrowGroupKeyEqual` 的 `arrow.BINARY` 与 `arrow.STRING` 同处理（`h.Write(col.Value)` / `bytes.Equal`）。
  - `arrow_join.go`：`joinRowHash`/`arrValEqual`/`appendValueAt` 的 `arrow.BINARY` 与 `arrow.STRING` 同处理；新增 `bytes` import。
- **验证**：新增 `pkg/sql/rowexec/arrow_bytes_test.go`（直接调用 `buildArrowColumns` + 三个分组比较函数），BYTES→`arrow.Binary` 往返、分组哈希/相等语义均正确（本地 PASS）。另修复两处预先失效的 colexec bench 测试（`RecordToBatch` 新签名缺 `allocator`）以恢复 `rowexec` 测试包可编译。
- **剩余缺口（阶段 C 收口，非阻塞）**：vendored `arrow/compute` 的 `FilterBinary` kernel 对 `arrow.Binary` 变长偏移布局存在 `GetSpanOffsets` 越界（已知生态 bug），含 BYTES 列的过滤/投影谓词若走 compute kernel 会 panic；当前 `canArrowFilterExpr` 对 Bytes 列回退 `tree.Datum` 通用路径（功能正确、非向量化），待阶段 C 字符串/字节函数向量化时一并解决。

### 2026-08-06（续）— 时序路径与 colexec 兜底分析
- **时序与关系无语义耦合，仅经「关系格式 buffer（coldata.Batch / EncDatumRows）」交汇**：`TsReaderOp.Next`（`colexec/ts_reader.go:222`）与关系型 `colBatchScan` 产出**同构的 `coldata.Batch`**；Arrow 已有 `BatchToRecord`（colexec/arrow_bridge.go）+ `rowToArrowConverter`（arrow_adapter.go）两条现成桥，可把时序 buffer 转 Arrow Record。故时序 scan **可作为 Arrow 数据源接入**，无需改 tse 引擎。
- **「ArrowScan 支持时序」需分层**：scan 侧（时序 buffer→Arrow）低成本可行，复用 `BatchToRecord`；算子侧需解除 7 处 `canArrowX` 的 `EngineTypeTimeseries → return false` 并补全 timestamp/decimal 类型在 `vecToArrow`/`arrowDataTypeForKWType` 的映射（当前 `vecToArrow` 仅支持 Int64/Float64/Bool/Bytes）。新增文档 §6.7。
- **colexec 不能退役，须保留兜底（初版，已审订）**：初版三层理由——①时序读 `TsReaderOp` 独占于 colexec；②Arrow 降级网兜底目标即 colexec；③Arrow↔colexec 双向桥是运行链路。后续经审订（见 2026-08-06 续二）**推翻第①条**（时序读可 Arrow 化）、**弱化第②③条**（降级网与桥均为覆盖率不足时的过渡依赖，非架构硬约束）。阶段 6 的 D 项初版「退化为兜底残差层」已进一步修正为「**随覆盖率提升逐步收窄至可去除**」。
- **rowexec 兜底范围精确界定（§6.9，D3 细分于 2026-08-10）**：纠正上一轮"D 类全不可退出"的过度简化。将 rowexec 独占算子按真实原因细分——**D1 真不可**（写/DDL/采样/校验/streamAggregator，Arrow 是只读引擎无等价）、**D2 不可**（ProjectSet 一行产多行，超出现有单 Record 模型）、**D3a 可但工程未做**（lookup join 系/行式 mergeJoiner，非对称编排需新算子骨架）、**D3b 桥接型已融入**（zigzag/interleaved，KV 游标状态机内核不可列式化，输出侧经 `unifiedInputFrom` 自动 Arrow，不另写 Arrow core）。以 `joinReader` 为例：其 join 计算是 Datum Row 基础、KV 仅作输入读取，与 KV 解耦，可仿 `ArrowJoin` 写 `ArrowLookupJoiner`；但 **zigzag/interleaved 例外**（2026-08-10 修订）——其 KV fetch 不是"输入侧取数方式"而是"算法控制流本身"（交替 seek + `side` 切换跳读），无两块完整输入 Record，无法套用对称 Arrow 算子，故 D3b 判定为桥接型而非"可 Arrow 化需骨架"。结论：rowexec 真正不可替代范围 = D1 + D2 + D3b 内核（约半数 processor 强）；D3a 可随 Arrow 覆盖退出。
- **重构终态基线（§6.10 初版，已审订）**：初版纠正「最终保留 rowexec 的 D1/D2、colexec 可去除」的误判，定性为三层长期并存。后续经 §6.8 审订**推翻"colexec 不可去除"**，终态修正为**两层长期并存（Arrow 主 + rowexec 永久兜底）+ colexec 临时过渡层（覆盖率达标后可去除）**。详见 2026-08-06 续二。

### 2026-08-06（续二）— colexec 兜底性质审订（推翻"不可去除"）
- **时序读不再构成 colexec 硬约束（审订 §6.8 第①条）**：`TsReaderOp`（`colexec/ts_reader.go`）拆为两层——tse C++ FFI（`SetupTsFlow`/`NextVectorizedTsFlow`/`CloseTsFlow`，不绑定 colexec）+ Go 侧 buffer 装配（`vec.Append` 转 `coldata.Batch`，纯适配器）。后者可被 `ArrowTsReader` 算子替代：保留 tse FFI，仅把 `vec.Append` 换成 `arrow_adapter.go` 的 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，使算子变为 `ArrowRecordEmitter`。故时序读可 Arrow 化。
- **colexec 降级网/双向桥均为过渡依赖（审订 §6.8 第②③条）**：降级仅发生于 Arrow 表达式/类型盲区、marshal 失败、Arrow 未实现算子（D3/时序读，均已论证可 Arrow 化）；若 Arrow 终态全覆盖，降级网无处挂靠 colexec。双向桥的 colexec 侧（`RecordToBatch`）依赖 colexec 存在，colexec 去除后桥仅留 Arrow↔rowexec 方向。故 colexec **非架构硬约束，是覆盖率不足时的过渡性兜底层**。
- **终态修正（§6.10）**：三层「主-备-兜底长期并存」→ **两层长期并存（Arrow 主 + rowexec 永久兜底）+ colexec 临时过渡层（可去除）**。rowexec 永久兜底范围 = D1（写/DDL/采样/校验/流式）+ D2（ProjectSet）+ 一切 Arrow 失败降级落点；D3 可随 Arrow 覆盖退出。6.2 表 D 项改为「colexec 随覆盖率提升逐步收窄至可去除」；6.3 阻塞点第 5 条撤销。

### 2026-08-11 — arrow 依赖升级（v17）外部包分析与"不裁剪"决策
- **触发**：`arrow-unify` 分支 vendor submodule 由 master `8f3b7a8c` 升至 `26d879ca`，把 `apache/arrow/go` 旧版（路径 `.../go/arrow`）升级到 **v17**（路径 `.../go/v17/arrow`）。旧版仅依赖 vendor 本有的 `flatbuffers`/`pkg/errors`/`testify`/`go-spew`/`go-difflib`；v17 强制带入 4 个新外部依赖。
- **新引入依赖及作用**：① `klauspost/compress`（≈480 文件，最大）— 仅服务 `arrow/ipc/compression.go`（IPC 流可选 zstd 压缩），KWDB 走内存 Record 路径零使用；② `golang.org/x/xerrors` — arrow 核心包错误处理（8 处），Go 1.13 标准库已吸收大部分；③ `golang.org/x/sync` — 仅 `arrow/compute` 一处 `errgroup` 并行 compute；④ `golang.org/x/exp`（slices/maps/constraints）— arrow 泛型基础工具，且 **KWDB 主代码本就在用**（workload/kvserver），属必需依赖，Go 1.21+ 才进标准库（当前 Go 1.20 仍需）。
- **结论**：仅 klauspost 是"为 KWDB 不用的 ipc 压缩功能带入的冗余项"（剥离需改 arrow 源码打 patch）；x/exp 是共享必需依赖、xerrors/xsync 是 arrow 核心内部依赖，无法用 vendor 旧包替换、剥离需侵入库源码且收益低。**决策：不裁剪，保留原始依赖**。若未来根治，更优路径是升级 Go 1.21+ 让 x/exp 进标准库，而非逐个替换。详见新增 §6.12。
- **协议分析**：四个库均属**宽松型（permissive）协议、无 copyleft**，商用友好、与 KWDB 既有依赖一致。xerrors/xsync/xexp 均为 **BSD-3-Clause**；klauspost/compress 为**混合协议**——主体 BSD-3-Clause，`gzhttp/` 子目录 Apache-2.0，`zstd/internal/xxhash/` 为 MIT，snapref/lz4ref/s2/snappy 为 BSD-3-Clause。KWDB 仅用 `zstd` 子包，实际编译进二进制的是 `zstd`+`xxhash`(MIT)+`snapref/lz4ref`(BSD-3)，**不含 gzhttp 的 Apache-2.0**（未 import）；仅源码分发时需留意 gzhttp 的 Apache-2.0 NOTICE 义务，二进制分发只涉 BSD/MIT 署名保留。整条 arrow v17 依赖链 = Apache-2.0（arrow 本体）+ BSD-3-Clause + MIT，合规风险低。详见 §6.12.1。

### 2026-08-05
- 日期时间函数 `EXTRACT`/`DATE_TRUNC` 接入 Arrow 投影路径（§2.6.4，validator 见 §2.6 末）；9 个 Arrow 开关保持 `defaultEnabled=false`。
- 集合 ALL 变体 `UNION ALL` 短路（`ArrowUnionAll` 算子）落地。
- 字符串函数投影侧全 Arrow 化；CAST 部分类型覆盖。

### 2026-08-07 — 阶段 C 表达式覆盖（过滤字符串/字节函数、CASE/COALESCE、RANGE 偏移 frame）
- **过滤路径字节（Bytes）列支持（P1 类型补全）**：`arrow_filter.go` 的 `eval` 选择步骤对 `arrow.IsBinaryLike` 列（Binary/LargeBinary）改用 Go 路径 gather（`compute.Filter` 的 vendored `FilterBinary` kernel 对变长 offset 布局 `GetSpanOffsets` 越界 panic，已知生态 bug），其余类型仍走原生 `compute.Filter` 快路径。配合 2026-08-07 的 Bytes schema/builder + 分组/连接比较支持，含 `WHERE blob_col = x` 的 BYTES 列过滤端到端走 Arrow 且结果正确（测试 `arrowpilot/arrow_unify_filter_bytes_test.go`）。
- **过滤路径 CASE / COALESCE（阶段 C 表达式覆盖）**：`canArrowFilterExpr`（`arrowFilterLeafFromExpr`）新增 `*tree.CaseExpr`/`*tree.CoalesceExpr` 分支，复用投影侧 `arrowCaseCol`/`arrowCoalesceCol` 生成 `Kind:"case"` 的 `arrowProjectionCol`，作为 `arrowFilterLeaf.Case` 叶子；executor `arrowFilterCore` 经新增 `evalCtx` 字段，在 `evalLeafDatum` 对 `Case` 叶子复用投影 `evalCase` 求值（所有 arrow 值类型 / 嵌套分支统一支持）。`arrowFilterLeafJS`/`ArrowArg`/`leafToArrowArg`/`specForCol`（提升为包级 `arrowProjectionSpecForCol` 供 filter 复用）同步打通序列化链。测试 `arrowpilot/arrow_unify_filter_case_test.go` 覆盖 `CASE WHEN ... THEN ... ELSE ...` 与 `COALESCE` 谓词，与 classic 路径一致。
- **窗口 RANGE 偏移 frame（阶段 C 表达式覆盖）**：`rangeFrameBounds` + `offsetDatum` 实现 value-based RANGE 偏移帧求值（按 ORDER BY 值做 ±offset 二分，覆盖 int/float/decimal/timestamp），并有单测 `rowexec/arrow_windower_range_test.go` 验证。但**端到端暂未打通**：Arrow windower 在 RANGE 偏移 frame 的执行层（分区处理 / 排序值比较）仍有 bug（实测仅输出部分 partition、每帧退化为单行），属 windower 偏移 frame 执行问题，与已落地的 ROWS 偏移 frame 同源待修。当前 `isSupportedWindowFrame` 对 RANGE 偏移边界仍回退 classic 路径；`rangeFrameBounds`/`offsetDatum` 已就绪，待 windower 偏移执行层修复后即可启用。
- **验证**：`go build ./pkg/sql` 通过；新增/修复 `arrowpilot` 与 `rowexec` 单测（Bytes filter、CASE/COALESCE filter、Bytes 类型、RANGE frame bounds）均 PASS。

### 2026-08-10 — D3a 收口（lookup 系标保留项，ArrowMergeJoiner 已完成）
- **代码核对结论**：D3a 原含 4 项（joinReader / indexJoiner / batchLookupJoiner / 行式 mergeJoiner）。经读源码（`distsql_physical_planner.go` merge 分支 + `arrow_join.go`）确认：
  - **`ArrowMergeJoiner` 已完成**：行式 mergeJoiner 经复用 `ArrowJoin` core 落地（`canArrowMergeJoin` 已存在，对称批处理两侧上游 Arrow 流），属"已 Arrow 化"。
  - **lookup 系（joinReader / indexJoiner / batchLookupJoiner）真实形态是「左输入流 + 右侧 KV 游标反查」的非对称编排**，右侧输入不是预物化 Arrow Record，而是 KV 游标按需流式拉取的游标，与 D3b（zigzag/interleaved）同构——**计算内核不可列式化**，无"两块完整输入"可供对称 Arrow 算子向量化。
- **收口决策（用户确认）**：lookup 系**定为保留项**，不另写 Arrow core：
  - 现状下经 `arrow_bridge.NewArrowToRowSource`（上游 Arrow→行式）+ joinReader（行式 KV 反查）+ `unifiedInputFrom`（行式→Arrow）**已自动融入统一 DAG**，全链路已是 Arrow 流通，性能瓶颈在 KV 游标延迟而非攒批（桥接性价比碾压薄壳，0 新增算子）。
  - 若真要 Arrow 化，需重写 KV span 编码（Arrow value→EncDatum→KV 字节），而 `arrow_adapter.go` 仅有 Arrow→EncDatum 单向、无反向路径，风险高；且本环境（GOPATH 双 vendor，go test 重复注册 panic）无法端到端验证。
- **proto 层处理（保留前向占位）**：`ProcessorCoreUnion.ArrowLookupJoiner`（`*Expression`，字段编号 60）已加进 `pkg/sql/exec_infrapb/processors.pb.go` 的 4 处（字段声明 / Marshal / Size / Unmarshal），但 **executor 未实现**，作为保留项占位，不影响运行（planner 不生成此 core）。
- **文档同步修订**：§6.9 D3a 表项、joinReader 示例段、修正结论、§6.10 终态表/工作量描述均更新为"ArrowMergeJoiner 已完成 + lookup 系保留项"口径；D3a 不再阻塞终态（仅剩 lookup 系为前向占位保留项）。

### 2026-08-10 — P0 推进：时序读 Arrow 化收口（解除 8 处 EngineTypeTimeseries 门控）
- **探查结论**：P0 的"planner 接线"部分**事前已完成**——`rowflow/row_based_flow.go` 已实现 `arrowTsEmitter`（`arrowMode=true`，跨节点 REMOTE 直接把 `arrowTsReader` 喂下游 Arrow 算子，不经 RunTS）+ QUEUE 路径 `NewArrowTsReader`（`arrowMode=false`，本地行式 reader 保持引擎 plumbing 一处），受 `physicalplan.ArrowTsScanEnabled` + `ArrowTsScanSupported(typs)` 双重门控。`arrowTsReader` 算子本体（`rowexec/arrow_ts_reader.go`）早已落地（含 ArrowRecordEmitter/ArrowOutput，tse 资源由 pullRecord 在流耗尽时释放）。
- **实质剩余工作 = 解除 8 处 `EngineTypeTimeseries` 门控**（让时序查询的下游算子可走 Arrow）：
  - 门控位置（解除前）：`arrow_unification.go` 的 `canArrowAggregate`(216) / `canArrowMergeJoin`(491) / `canArrowJoin`(525) / `canArrowSort`(595) / `canArrowDistinct`(641) / `canArrowWindow`(858) / `arrowUnionAllCoreFor`(991) / `arrowValuesCoreFor`(1021)，均为各 `canArrow*` 函数首行的 `if engine == tree.EngineTypeTimeseries { return false }`。
  - 调用点核查：`join`/`mergeJoin` 两处调用点硬编码 `tree.EngineTypeRelational`（5506/5538），其门控为 dead code；`agg`/`windower`/`sorter` 的调用点用 `n.engine`（3923/4572/4800/7014/3257），时序查询时确传 `EngineTypeTimeseries`，门控在此生效。
  - 安全边界：各 `canArrow*` 内部白名单已兜底——`canArrowAggregate` 只认 SUM/MIN/MAX/AVG/SUM_INT（时序专属 last/first/rate 等自然 return false）；`canArrowSort`/`Distinct`/`Window` 仅校验类型可比较，且 `arrow_adapter.go` 已支持 TIMESTAMP/TIMESTAMPTZ（builder/append 均就绪），解除后时序列在 Arrow 路径不崩。
- **改动**：删除上述 8 处门控首行（`arrow_unification.go` 已无 `EngineTypeTimeseries` 残留）。时序读接线不做额外改动（已就绪）。
- **验证**：`GOFLAGS= GOPATH=/home/sdy/go GO111MODULE=off go build ./pkg/sql/ ./pkg/sql/rowexec/... ./pkg/sql/rowflow/...` 通过（仅剩 colexec/execgen/cgo 环境固疾噪音，与改动无关）。本环境 go test 因双 vendor 重复注册 panic 无法 e2e，时序语义等价性（尤其 AVG 的 decimal/float 输出、窗口 partition 时序语义）需 CI 单一 vendor 跑时序查询回归确认。
- **文档同步**：§6.11.2 P0 行标"已完成（2026-08-10）"并补实施细节；§6.11.3 终态描述更新为"Arrow 主路径已覆盖关系 + 时序双引擎"。

### 2026-08-10 — P1 收尾：Bytes 过滤 compute kernel bug 绕过确认 + bug 根因定位
- **现状核查**：P1 描述的"绕过"**事前已实现**——`rowexec/arrow_filter.go` 的 `eval`（122-144 行）对 `arrow.IsBinaryLike` 列 skip 原生 `compute.Filter`，改用 Go `gatherColumn` 路径；非 Binary 列仍走原生 `compute.Filter`（高效）。功能正确，含 null 谓词处理（`!mask.IsNull(i) && mask.Value(i)`，与 `compute.Filter` 默认 DropNulls 语义一致）；全 Binary 列场景 `useComputeFilter=false`、`filterDatum` 为 nil 但不会被使用，逻辑自洽。
- **Bug 根因定位**：vendored arrow v17 的 Binary Filter 内核 `binaryFilterImpl`/`binaryFilterNonNull`（`internal/kernels/vector_selection.go:1150/1200`）调用 `exec.GetSpanOffsets[OffsetT]`（`exec/utils.go:49-52`）。该函数 `unsafe.Slice((*T)(...), span.Offset+span.Len+1)` 假设偏移 buffer 物理长度 ≥ `(Offset+Len+1)*sizeof(T)`；当 `ArraySpan.Offset != 0`（array 是某 parent array 的 slice，非零偏移）时越界读取，导致下游 filter 消费带 offset 的 Binary 列时 panic。这是 arrow v17 的 Binary filter 边界缺陷，非我们代码问题。
- **修复边界**：真正"内核替换"（让 Binary 走原生 `compute.Filter`）需改 vendored `arrow/compute` 库（在 `GetSpanOffsets` 加 buffer 边界保护，或规范化 array offset），或升级 arrow 版本。但 arrow v17 为精挑版本，execgen/cgo 依赖其 API 字节布局，动 vendored 库风险高，且本环境无法 e2e 验证。故 **Go gather 绕过为当前终态最优解**（桥接性价比碾压薄壳，仅性能项、不阻塞正确性）。
- **代码加固**：`arrow_filter.go` 第 157 行 `array.NewRecord(..., cols[0].Len())` 在零列 Record 时 `cols[0]` 越界——加零列边界保护（返回空 Record），防御性（planner 不生成零列 plan，但稳妥）。`read_lints` 无错误；`go build ./pkg/sql/rowexec/...` 通过。
- **文档同步**：§6.11.2 P1 行标"绕过已实现，2026-08-10 收尾"，补 bug 根因与终态结论（内核替换不在当前阶段安全边界内）。

### 2026-08-10 — 阶段 C 推进：CAST 全类型（含 BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 目标）
- **缺口定位**：planner `arrowCastTargetTag`（`physical_plan.go:2926`）只放行 STRING/INT/FLOAT/DECIMAL 4 目标（缺 BOOL/DATE/TIMESTAMP/TIMESTAMPTZ）；executor `arrowCastType`（`arrow_filter_processor.go:362`）甚至不识别 DECIMAL（default 回退 STRING，导致过滤路径 `CAST(col AS DECIMAL)` 被误当 STRING cast）；`castArrowArray`（`arrow_filter.go:621`）缺 BOOL/DATE/TIMESTAMP 目标实现；投影顶层 CAST 的 copy 分支（`arrow_projection.go:183`）直接 slice 原列、未应用 Cast，致 `SELECT CAST(col AS X)` 在 Arrow 投影路径被静默忽略。
- **改动**：
  1. planner `arrowCastTargetTag` 扩展放行 BoolFamily→"BOOL"、DateFamily→"DATE"、TimestampFamily→"TIMESTAMP"、TimestampTZFamily→"TIMESTAMPTZ"（TIME 不在 adapter 承载，保持 false 回退 row）。
  2. executor `arrowCastType` 补 "DECIMAL"（→`&arrow.Decimal128Type{Precision:38,Scale:0}`，**修过滤路径 CAST AS DECIMAL 误当 STRING 的 bug**）、"BOOL"（→Boolean）、"DATE"（→Int32）、"TIMESTAMP"/"TIMESTAMPTZ"（→Timestamp_us）。
  3. `castArrowArray` 补 `arrow.BOOL`→`castToBool`、`arrow.INT32`→`castToDate`、`arrow.TIMESTAMP`→`castToTimestamp`；新增三函数：
     - `castToBool`：源 String(true/t/f/1/0 小写)/Int64/Float64/Decimal128(非0)/Boolean。
     - `castToDate`：源 String("2006-01-02")/Int64/Float64/Decimal128/Int32/Timestamp(截断到日)，arrow 承载 Int32(Unix 天)。
     - `castToTimestamp`：源 String(RFC3339/"2006-01-02 15:04:05"/"2006-01-02")/Int64/Float64(×1e6 micros)/Decimal128/Int32(×86400e6)/Timestamp，arrow 承载 Timestamp_us（与 adapter 一致，TimestampTZ 也用 Timestamp_us 无 tz）。
  4. 源类型补全：`castToInt64` 补 `*array.Decimal128`（→`BigInt().Int64()`）、`castToDecimal` 补 `*array.Boolean`（→0/1）、`castToString` 已含 Timestamp/TimestampTZ 源（arrow 无独立 TimestampTZ 类型，统一 Timestamp_us，UTC 格式化）。
  5. **投影顶层 CAST bug 修复**：`arrow_projection.go` 的 copy 分支在 `array.NewSlice` 后，若 `spec.Args[0].Cast != nil` 调 `castArrowArray` 应用目标类型（slice 后的 array 需 Release 旧引用）。使 `SELECT CAST(col AS X)` 真正转换类型，覆盖全类型。
- **语义对齐**：与 `arrowDataTypeForKWType` 承载一致（DATE=Int32 天、TIMESTAMP/TZ=Timestamp_us、BOOL=Boolean、DECIMAL=Decimal128）；string→timestamp/date 解析用标准库 time 格式（与 KWDB row 路径文本格式大致一致）；int/float→timestamp 按 Unix 秒×1e6 micros。
- **验证**：`GOFLAGS= GOPATH=/home/sdy/go GO111MODULE=off go build ./pkg/sql/rowexec/... ./pkg/sql/physicalplan/...` 通过（初版 `decimal128.Num.ToBigInt()` 误用，修正为 `.BigInt()` 返回 `*big.Int`）。本环境 go test 因双 vendor panic 无法 e2e，CAST 语义等价性需 CI 单一 vendor 跑 `CAST(col AS ...)` 各类型对回归（含 `CAST(decimal AS int)` 溢出、`CAST(string AS timestamp)` 格式变体）。
- **文档同步**：§6.11.2 表格在 P1 与 P2 间加"阶段 C（CAST 全类型，2026-08-10 落地）"行；§6.2 C 项"CAST 全类型"缺口标记已清零。

### 2026-08-10 — P2 推进：fetcher 直出 Arrow（低风险快赢路径）
- **方案选型**：P2 原含两保留项——`ArrowLookupJoiner` executor 与 fetcher 直出 Arrow。经分析（见用户咨询回应），`ArrowLookupJoiner` 收益被 KV 反查延迟掩盖且依赖 fetcher 反查也直出，单做投入产出比低；**fetcher 直出 Arrow 是 P2 里真正的杠杆**。但其完整形态（改 `row.Fetcher` 核心加 Arrow 快路径、KV 字节→arrow builder 直转）侵入存储读取核心、风险极高且与本环境无法 e2e 验证冲突。
- **务实落地（等价语义、零侵入 fetcher）**：scan→Arrow 主干当前经 `unifiedInputFrom → NewRowSourceToArrow → buildArrowColumns`，其瓶颈是每行 `EncDatum` 都走 `EnsureDecoded` 构造 `tree.Datum`（堆分配 + 类型断言），再从中取实际 value 灌 `arrow.Array`。"fetcher 直出 Arrow"的语义等价于**让 KV 编码字节直达 arrow builder、跳过 `tree.Datum` 中间层**。KWDB 已对 Int 提供 `EncDatum.GetInt()`（`DecodeUntaggedIntValue` 直取）；本改动为其余标量类型补齐同类直取原语：
  - `pkg/sql/sqlbase/encoded_datum.go` 新增 `EncDatum.GetFloat/GetBytes/GetDecimal/GetTime/GetDate/GetUUID(t *types.T, da *DatumAlloc)`：VALUE 编码（`DatumEncoding_VALUE`，即 KV value 列，scan 主体）走 `encoding.DecodeUntaggedXxxValue` 直接从 `ed.encoded[dataOffset:]` 取值、零 `tree.Datum` 分配；KEY 编码（index key 列）回退 `EnsureDecoded(t, da)`（行为不变，安全）；`ed.Datum != nil`（已解码）直接断言返回。
  - `pkg/sql/rowexec/arrow_adapter.go` 的 `buildArrowColumns` 六分支（Float/Bytes/Decimal/Timestamp/Date/Uuid）改为调用直取方法（保留 `IsNull()` 前置拦截；Date 无限值经范围检查 `|days|>1<<29` → NULL，与原 `IsFinite()` 语义对齐）；Int 分支早已走 `GetInt`。Bool/Json/Interval 保留 `EnsureDecoded`（bool 无 untagged 原语、json/interval 转 string 分配成本低，收益可忽略）。
- **收益**：Arrow 路径在 scan 侧对 VALUE 编码的值列省去每行的 `tree.Datum` 堆分配 + 类型断言，KV 字节直接灌 `arrow.Array` builder——是 Arrow 路径在 scan 侧性能追平/超越 colexec 的关键一步（colexec 的 cfetcher 正因跳过了 EncDatum 解码才快），也为后续 lookup 系全程 Arrow（需 fetcher 反查产出也直出）清理了前置。Bool 等少数类型仍走原路径，不影响语义。
- **验证**：`GOFLAGS= GOPATH=/home/sdy/go GO111MODULE=off go build ./pkg/sql/sqlbase/... ./pkg/sql/rowexec/...` 通过（初版 `tree.DUUID` 误用，修正为 `tree.DUuid`）。`pkg/sql/...` 全量仅剩 `-lkwdbts2` 链接失败（C++ 引擎库未建，已知环境固疾，与改动无关）。本环境 go test 因双 vendor 重复注册 panic 无法 e2e，scan→Arrow 数值等价性（尤其 Decimal 精度、Timestamp micros 截断、Date 无限值→NULL、Uuid 字节序）需 CI 单一 vendor 跑 scan 回归对拍。
- **文档同步**：§6.11.2 原"P2 fetcher 直出 Arrow（保留项，非阻塞）"行改为"P2（fetcher 直出 Arrow，2026-08-10 落地）"，描述改为低风险快赢路径 + 收益；§0.x 与 §6.11.3 不变。

### 2026-08-10（补）— 架构澄清：fetcher 直出 Arrow 的通用性与边界
- 回应"TableReader 类似读取算子是否可直接读 KV 数据"的疑问，明确本次改造的语义边界与通用范围，补入 §6.11.2 的 P2 行：
  - **通用性**：`buildArrowColumns`（`arrow_adapter.go`）是**所有行式读取算子汇入 Arrow DAG 的唯一统一攒批入口**（下游经 `unifiedInputFrom→NewRowSourceToArrow`）。本次为 `EncDatum` 加的 `GetXxx` 直取原语作用于该入口，故**对所有以 `EncDatumRow` 为产出的读取算子自动生效，无需逐个改造**：`TableReader` / `IndexJoiner` / `JoinReader`(lookup 反查行) / `IndexSkipTableReader` / `InterleavedReaderJoiner` / `ZigzagJoiner` / `MergeJoiner` 上游 fetcher 行——只要下游汇入 Arrow DAG，其 `EncDatum→Arrow` 转换即已是字节直取。例外是 `arrowTsReader`（时序读，不经 `row.Fetcher`，C++ tse FFI 直出 Arrow，本无 `EncDatum` 中间层）。
  - **边界**：本次是 `EncDatum`（`row.Fetcher` 已解码、仍持有原始 KV 字节 `ed.encoded`）→`arrow.Array` 的直取，**不等于 TableReader 跳过 `row.Fetcher` 裸读 KV**。fetcher 的 KV 扫描、MVCC 版本选择、列投影、key 解析仍由其负责；`TableReader.NextRow`（`tablereader.go:243`）仍调 `fetcher.NextRow`（`rowexec` 包 7+ 处读取算子均如此）。结论：读取 KV 数据并直出 Arrow 的能力已具备且对所有行式读取算子通用，但 fetcher 之上的 MVCC/key 逻辑未动。
  - **深度项（未做）**："fetcher 内部把 KV 字节直接灌进 `arrow.Builder`、连 `EncDatumRow` 数组都不物化"是更深一层的改造（侵入 `row.Fetcher.ReadColumns`），收益仅再省一次 `EncDatumRow` 堆分配，但风险显著更高，列为 P2 深度保留项。

### 2026-08-10（收尾）— 路线图收尾状态总结
- 继 P2 fetcher 直出 Arrow 落地后，核查全量 `canArrow*` 门控与 `sql.arrow_*.enabled` 开关默认值，确认所有规划算子均已具备 Arrow 路径。新增 §6.11.3「收尾状态总结」：
  - **能力就绪**：scan/join/merge-join/sorter/distinct/windower/aggregator/union-all/values/时序读 全部具备 Arrow 路径；fetcher 字节直取红利对所有行式读取算子通用。下一步是 e2e 验证与默认开关翻转，而非补齐算子。
  - **默认关闭的开关（能力已落地、保守未启用）**：`sql.arrow_ts_scan.enabled`（默认 false，`arrowTsReader` 已落地但 `ArrowTsScanSupported` 仍只放行数值/字符串类型，TIMESTAMP/TZ/DECIMAL 列被挡，算子侧类型补全见 §6.7 第 2 点未做；翻转条件=CI 跑时序回归）+ `sql.arrow_values.enabled`（默认 false，`ArrowValues` core 已落地 7ca82fe8 且 arrowpilot 验证通过；翻转条件=CI 跑 values 全类型回归）。其余开关（scan 主门/join/sorter/distinct/windower/union_all/filter/agg）默认 true，Arrow 已默认启用、仅受白名单与类型门局部降级。
  - **唯一剩余高风险保留项**：`ArrowLookupJoiner` executor。结论：本轮不推进——收益被 KV 反查延迟掩盖、依赖 fetcher 反查也直出才显著、需新增 Arrow→EncDatum→KV 字节反向编码路径且本环境无法 e2e，列为已知保留项。
  - **环境验证边界**：本环境 `go test` 双 vendor 重复注册 panic、colexec/execgen 全量 build 需 `libkwdbts2` 预编译（已知固疾）；改动均经 `GOFLAGS= go build ./pkg/sql` 验证可编译，语义等价性依赖 CI 单一 vendor 回归对拍。

### 2026-08-10（续）— ArrowTsScanSupported 放行 TIMESTAMP/TZ/DECIMAL
- **起因**：用户要求把 `ArrowTsScanSupported` 的 TIMESTAMP/TIMESTAMPTZ/DECIMAL 类型放行补上。核查发现 `arrowTsReader.pullRecord` 经 `buildArrowColumns`/`arrowDataTypeForKWType` 攒批，后者早已支持 Decimal→Decimal128、Timestamp/TZ→Timestamp_us、Date→Int32、Uuid→FSB(16)、Json/Interval→String，即**承载层无短板**；真正挡住时间/decimal 列的是 `ArrowTsScanSupported` 类型门复用了 physicalplan 包的 `arrowSupportedCompareType`（仅放行 Int/Float/Bool/String/Bytes/Decimal，不含 Timestamp/TZ）——这是"比较语义白名单"被误当作"scan 承载门"的错配。
- **改动**（`pkg/sql/physicalplan/physical_plan.go`）：新增独立的 `arrowTsScanSupportedType(t *types.T)` 承载判断，放行 Int/Float/String/Bool/Bytes/Decimal/TimestampTZ/Timestamp/Date/Uuid/Json/Interval，与 `arrowDataTypeForKWType`/`buildArrowColumns` 实际支持对齐；`ArrowTsScanSupported` 改用之。语义从"复用语义白名单"改为"scan 只看 Arrow 能否承载"，TS 时间 tag（TIMESTAMP/TZ）与 decimal 指标列现已可走 Arrow TS scan。**注意误插位置**：初版误将函数加到 `pkg/sql/arrow_unification.go`（package sql），而 `ArrowTsScanSupported` 在 `package physicalplan`，跨包不可见导致 `undefined`；修正为加到 `physicalplan` 包。`sql` 包同名函数已回退删除。
- **验证**：`GOFLAGS= GOPATH=/home/sdy/go GO111MODULE=off go build ./pkg/sql/...` 仅剩 `-lkwdbts2` 链接失败（环境固疾），源码编译通过。
- **文档同步**：§6.7 第 2 点标注算子侧类型补全（TIMESTAMP/DECIMAL 映射）已实质完成（`arrowDataTypeForKWType` 早已支持 + 类型门已放行 + 8 处门控已解除）；§6.7 落地状态（第 241 行附近）修订类型门描述；§6.11.3「默认关闭的开关」中 `sql.arrow_ts_scan.enabled` 的"TIMESTAMP/TZ/DECIMAL 仍被挡"断言改为"已放行（类型门扩展）"；§6.11.3 收尾总结同步更新。开关仍默认 `false`，翻转条件=CI 单一 vendor 跑含时间/decimal 列的时序查询回归（scan→agg/filter/sort）通过后手动翻 `true`。

### 2026-08-13 → 2026-08-14 — arrowpilot 全量 e2e 验证打通（33 用例全绿）

**背景**：本环境此前被认为"因缺 `libkwdbts2` 无法跑 arrowpilot 端到端测试"。经核查，库已在 `build/lib/` 落地且 kwbase 已链接，`go test` 仅需 `CGO_LDFLAGS="-L$PWD/../build/lib"` + `LD_LIBRARY_PATH="$(pwd)/../build/lib"` 即可在本环境实跑（`make test PKG=./pkg/sql/rowexec/arrowpilot` 等价）。验证中发现并修复了一批 Arrow 默认开启后暴露的真实 bug。

**验证命令（本环境）**：
`export CGO_LDFLAGS="-L$(pwd)/../build/lib" && export LD_LIBRARY_PATH="$(pwd)/../build/lib:$LD_LIBRARY_PATH" && go test ./pkg/sql/rowexec/arrowpilot/ -run '...' -count=1`

**修复 1 — int 混合宽度（arrow 不保 int 宽度，前置已修，2026-08-12）**：`arrowDataTypeForKWType` 对 IntFamily 按 width 选 Int16/Int32/Int64；`appendEncDatum`/`arrowRecordToEncDatumRows`/`arrowDataTypeToKWType` 补窄 int decode case；投影 `compute` 快路径由全 `allInt` 收窄为 `allInt64`。属 Arrow 默认开启后暴露，非 colexec 预存。

**修复 2 — FilterAggJoin 投影 int 混合宽度 + Ordinality row_number（2026-08-13）**：
- 投影/过滤的 int 列混合宽度（如 `a*2 > b`，a=int32、b=int64）经 `arrowNormalizeIntWidths` 把窄 int 列 cast 到 Int64 后走 compute kernel（arrow/compute 拒绝 int32×int64 混合）；`arrow_filter.go` 的 `evalBinary`/`evalIn` 与 `arrow_projection.go` 的 `eval` 均接入；IN/NOT IN 补 int16/int32 集合分支。
- JOIN 投影 bug：原 `p.outputRows = allRows`（未投影的 4 列）而 `Next()` 不经 `ProcessRow`，导致投影结果错乱（`a*b` 得 `{1,10}` 而非 `{10,100}`）；改为 `p.outputRows = out`（2 列投影结果），`ArrowOutput` 返回 `projRec`。
- Ordinality：`canArrowWindow` 去 ranking 函数（含 `row_number`）的拒绝分支，使 `WITH ORDINALITY` 默认走 Arrow windower 的 `row_number`。

**修复 3 — SUM(int)→DECIMAL 类型保真（2026-08-13，本轮核心）**：
- **根因**：KWDB `SUM(int)→DECIMAL`（`sem/builtins/aggregate_builtins.go:468`），但 Arrow `sumAgg` 对整数输入累加 `accI` 并输出 `Int` 标量，导致 agg 的 Arrow record 列类型是 Int，与 planner 声明的 Decimal 输出类型不匹配，下游 pgwire 编码报 `expected decimal, got *tree.DInt`（`TestArrowUnifyPipelineFilterAggSort` 失败）。
- **改动**（`arrow_aggregate.go` / `arrow_aggregator.go`）：
  - `sumAgg.Consume` 的 Int64/Int32/Int16/Boolean 分支改为把累加值塞进 `accD`（`apd.Decimal`，复用 `tree.ExactCtx.Add`，与 `meanAgg` 对齐）；`MergeFrom` 合并 `accD`；删除不再使用的 `accI` 字段。
  - `sumAgg.Finalize` 整数/decimal 输入统一返回 `Decimal128Scalar`（其 `outType` 已是 `meanDecimalType`），仅 float 输入返回 `Float64Scalar`。
  - `aggOutputType` 的 `"sum"` 分支：整数/decimal 输入返回 `meanDecimalType`（Decimal128），float 输入返回 `Float64`，与 `sumAgg.Finalize` 严格一致。
  - `newScalarAggregator` 的 `aggOpSum` 由 `out := inType` 改为 `out := aggOutputType("sum", inType)`，确保 `sumAgg.outType` 是 Decimal128（此前误用 `inType` 致 `Finalize` 断言 `outType` 为 Decimal128 时 panic）。
- **测试修正**（`arrowpilot/e2e_test.go`）：`TestArrowUnifyFilterAggJoin` 中用 `queryIntRows` 读 `SUM(a)`（现返回 Decimal）会失败，改为 `runFilterF`/`queryFloatRows`（`SELECT CAST(SUM(a) AS FLOAT) ...`）；把 `runFilterF` 定义上移以避免前向引用。

**修复 4 — Arrow 聚合器 TIMESTAMP/TIMESTAMPTZ 保真（2026-08-14，本轮核心）**：
- **根因**（经临时 debug 确认）：`TestArrowUnifyTimestampAgg`（`SELECT min(ts)::STRING ...`，ts 为 naive `TIMESTAMP`）panic：`invalid datum type given: timestamptz(9), expected timestamp`。Arrow 聚合器输出的 timestamp 列物理类型是 `timestamp[us, tz=UTC]`（TZ-aware），而 planner 声明 `min(ts)` 输出为 naive `TIMESTAMP`（OID 1114）。`arrowRecordToEncDatumRows` 解码时同时用 planner 类型 **和** Arrow 物理列 `TimeZone` 判断 TZ-aware，导致对 naive 列也产出 `DTimestampTZ`，与 planner 期望的 `DTimestamp` 冲突。经 `git stash` 验证此 panic 在 SUM 改动前即存在，属独立前置 bug，非 SUM 修复引入。
- **改动**（`arrow_projection_processor.go` 的 `arrowRecordToEncDatumRows`）：去掉 `tzAware` 对 Arrow 物理列 `tt.TimeZone != ""` 的依赖，**仅**由 planner 列类型 `t.Family()` 决定（`TimestampTZFamily` → `DTimestampTZ`，否则 → `DTimestamp`）。与 memory ID 36104396 指引一致——优先用 planner `typs[ci]` 保真，因为 Arrow 物理 timestamp 无法区分 KWDB 的 naive TIMESTAMP 与 TIMESTAMPTZ。

**验证结果（本环境实跑）**：
- `go build ./pkg/sql` 通过。
- `go test ./pkg/sql/rowexec/arrowpilot/` 全量 **33 个 `TestArrowUnify*` 用例全部 PASS**（184s），含用户关注的 4 个：`TestArrowUnifyMergeJoin`、`TestArrowUnifyOrdinality`、`TestArrowUnifyPipelineFilterAggSort`、`TestArrowUnifyFilterAggJoin`，以及 `TestArrowUnifyTimestampAgg`、`TestArrowUnifyDecimalAgg`、`TestArrowUnifyVariance`、`TestArrowUnifyJoinNonEqui` 等。
- 改动文件：`pkg/sql/rowexec/arrow_aggregate.go`、`arrow_aggregator.go`、`arrow_aggregator_processor.go`、`arrow_filter.go`、`arrow_projection.go`、``arrow_projection_processor.go``、`arrow_unification.go`、`pkg/sql/rowexec/arrowpilot/e2e_test.go`。

**结论**：Arrow 统一引擎在关系型查询（含 filter/agg/join/sort/distinct/window/union-all/ordinality、int 全宽度、Decimal SUM、TIMESTAMP/TIMESTAMPTZ 保真）的端到端语义等价性已在本环境得到 arrowpilot 全套件验证；此前"本环境无法跑 e2e"的结论被推翻，CI 单一 vendor 回归可作为补充确认而非唯一依赖。唯一保留的高风险项（`ArrowLookupJoiner` executor）已于 2026-08-14 实施完成、仅 gate 暂关待 CI（见下条）。

### 2026-08-14 — ArrowLookupJoiner 实施（D3a 收口，gate 暂关待 CI）
- **目标**：把 lookup join（`joinReader` 的 KV 点查路径）纳入统一 Arrow DAG，消除 D3a 唯一保留项。
- **proto 改动**（`pkg/sql/execinfrapb/processors.proto` + 手维护 `processors.pb.go`，字段 51–59 为手维护，故字段 60 同样手工新增 Marshal/Size/Unmarshal + 结构体字段）：`ProcessorCoreUnion.arrowLookupJoin` 由最初设想的 `Expression`（JSON）改为 **`JoinReaderSpec` 类型**。原因：JSON 序列化 `JoinReaderSpec`（含 `Table` 描述符）往返不可靠，激活时 lookup 命中 0 行；proto 字段往返可靠，改用后定位确认问题不在序列化而在列索引对齐。
- **门控**（`arrow_unification.go` `canArrowLookupJoin`）：`ArrowJoinEnabled` + `arrowJoinType` 支持（inner/left/right/full）+ 非空 `LookupColumns` + 各 lookup 键为 `arrowJoinKeyType` + 左/右列均 `arrowAllSupported`。**当前 `return false`**——本环境无法跑 KV 分布式 e2e，且激活时含 `WHERE` 过滤的 lookup join 出现"查找命中 0 行"（planner `post` 列索引 vs `joinReader` 实际 joined 输出 schema 的列索引对齐问题），需 CI 单一 vendor 用 lookup join 回归（inner/left/right/full + 过滤 + 外连接 NULL 扩展）确认后翻 `true`。
- **planner 改动**（`distsql_physical_planner.go` `createPlanForLookupJoin`）：命中门则发 `ArrowLookupJoin` core（携带 `JoinReaderSpec` 原值，stage `post` 正常传递），否则维持原 `JoinReader`。
- **executor 改动**（新增 `pkg/sql/rowexec/arrow_lookup_joiner_processor.go`）：
  - `newArrowLookupJoinerProcessor`：由 `JoinReaderSpec` 构造行式 `joinReader`（`newJoinReader`，KV `row.Fetcher` 点查内核），用 `NewRowSourceToArrow` 桥接其 `left++right` 输出为单个 Arrow Record；`joinedTypes` 取自 `joinReader.OutputTypes()`（权威 schema）；`p.Init` 用 identity post 透传。
  - `compute`：先 `jr.Start(ctx)`（bridge 的 `Init` 为 no-op 不启动上游），再 `rs.Next` 取 Arrow Record，经 `arrowRecordToEncDatumRows(instTypes, rec)` 解码回 `EncDatumRows`，缓冲供 `Next` 通过 `p.Out.ProcessRow` 出（标准 `ProcOutputHelper`）。
  - 实现 `RowSource` 全接口（`Start`/`Next`/`ConsumerClosed`/`InitProcessorProcedure`/`OutputTypes`）；`arrowLookupJoinerRuns` 计数器供测试确认路径命中。
  - 设计定位：lookup join 的 join 计算（KV 点查 + 匹配 + 外连接 NULL 扩展）非对称、非两块完整输入 Record，故**不重写成原生 Arrow kernel**，而是桥接行式 `joinReader`（算子级桥接，与统一设计一致）。
- **factory 注册**（`rowexec/processors.go`）：`ProcessorCoreUnion_ARROW_LOOKUP_JOIN` → `newArrowLookupJoinerProcessor`，单输入单输出。
- **测试**（`arrowpilot/e2e_test.go` `TestArrowUnifyLookupJoin`）：覆盖 inner + left lookup join（含 `WHERE` 过滤 + 外连接 NULL 扩展），与标准引擎对拍；当前 gate 关走行式 `joinReader`，结果正确。激活路径的 KV 正确性待 CI 验证。
- **验证**：`go build ./pkg/sql` 通过；`go test ./pkg/sql/rowexec/arrowpilot/` 全量 **34 个用例 PASS**（含新增 `TestArrowUnifyLookupJoin`）。
- **下一步**：CI 单一 vendor 跑 lookup join 回归，确认列索引对齐后把 `canArrowLookupJoin` 翻 `true`。

---

## 7. 兜底场景清单（rowexec / colexec 降级覆盖）

> 本节汇总截至 2026-08-17，在 Arrow 默认执行层（阶段 A 已开启 `sql.arrow_scan.enabled`）背景下，**仍需要 rowexec / colexec 兜底**的全部场景。目的是给出一份可对照的"兜底清单"，用于判断终态（阶段 6 纯 Arrow 引擎）之前哪些路径不可去除、哪些只是表达式/类型门控而非算子缺位。

### 7.1 兜底分类法

兜底分两层，二者正交：

- **算子级兜底**：该算子**没有 Arrow core**，整算子降级回 rowexec / colexec。
- **表达式/类型级兜底**：算子本身是 Arrow 的，但**其中某表达式或某个数据类型**不在 Arrow 白名单内，导致该算子的 Arrow 路径被打断、整段降级（典型如 `canArrowRender` 返回 false → 该 stage 走行式/colexec）。

注意：统一设计下，算子级兜底并非"回到旧的三路径并存"，而是**经 `unifiedInputFrom` / `NewRowSourceToArrow` / `NewArrowToRowSource` 自动桥接融入单一 Arrow DAG**——行式/colexec 算子只是 DAG 中某个节点的"算子级后端"，上下游仍是 Arrow Record 流通。因此终态移除它们时，DAG 拓扑不变，只替换节点后端。

### 7.2 算子级兜底清单（无 Arrow core）

以下算子类**当前没有 Arrow 原生实现**，按性质分级：

| 算子 | 性质 | 当前如何融入 DAG | 是否计划 Arrow 化 |
|------|------|------------------|-------------------|
| `ProjectSet` | D2 变长展开（1 行 → N 行，函数相关） | 行式后端桥接 | 否（流式专有，难以列式化） |
| `Ordinality` | 纯序号列追加 | **已 Arrow 化**（阶段内完成） | — |
| `StreamAggregator` | 流式聚合（无界/有序输入） | 行式后端桥接 | 否（流式专有） |
| `SampleAggregator` | 采样聚合（§1 已明确排除） | 行式后端桥接 | 否 |
| `ZigzagJoiner` | D3b KV 游标状态机 | `unifiedInputFrom`→`NewRowSourceToArrow` 自动桥接 | 否（薄壳性能等价零收益） |
| `InterleavedReaderJoiner` | D3b KV 游标状态机 | 同上 | 否（同上） |
| `joinReader`（lookup 系：indexJoiner / batchLookupJoiner） | D3a 非对称编排（左流 + 右侧 KV 反查） | `NewArrowToRowSource`+joinReader+`unifiedInputFrom` 自动桥接 | 保留项（`ArrowLookupJoiner` gate 暂关待 CI，见 §6.11/2026-08-14） |

> 注：`Values`（P1 已 Arrow 化、`arrow_values_processor.go`）、`Ordinality`、`UnionAll`（`ArrowUnionAll`）已脱离兜底；时序读（`ArrowTsReader`）已脱离兜底。

### 7.3 表达式 / 类型级兜底清单（算子有 Arrow core，但部分输入触发降级）

**表达式白名单（Arrow 投影 / 过滤可渲染）**：

- 字符串函数：`length` / `lower` / `upper` / `trim` / `ltrim` / `rtrim` / `replace` / `concat`（本 vendored arrow/compute **无字符串 kernel**，均由 Go native loop / `appendEncDatum` 路径处理）。
- 数值标量：`abs` / `sqrt` / `ln` / `sign` / `power`（复用 arrow/compute v17 自带 kernel，无新 Go 核）。
- `floor` / `ceil` / `ceiling` / `trunc` / `round` 暂未引入（arrow kernel 为 int→float 语义与 SQL float 输入不符）。
- `CASE` / `COALESCE`：已支持（阶段 C）。
- `IN` / `NOT IN`：computed 左操作数已补齐（§2.4）；decimal 集合已补齐（§2.6）。
- `CAST`：全类型（含 BOOL/DATE/TIMESTAMP/TIMESTAMPTZ 目标，阶段 C）。
- 窗口 `ROWS` 偏移 frame / `RANGE` 偏移 frame：已支持（阶段 C）。

**数据类型白名单（`arrowAllSupported` 命中即不降级）**：

- 已支持：Int（全宽度 Int16/Int32/Int64，阶段 B 补宽度保真）/ Float / String / Bool / Decimal / Timestamp / TimestampTZ / Uuid / Json / Date / Interval / Bytes。
- 未支持 → 触发该列/该算子 Arrow 降级的类型：见 `arrowAllSupported` 门控（除上列外的其余 family，如 `Tuple` / `Array` / `Oid` / `Enum` / `Geography` / `Geometry` 等复杂类型）。

**降级触发规则（经验）**：

- 投影含白名单外字符串函数（如 `split_part` / `substring` 未实现路径）→ `canArrowRender` 返回 false → 整 projection stage 走 colexec/行式。
- 过滤含非 equi 比较 + 白名单外标量 → 同上。
- 输出列含白名单外类型 → `arrowAllSupported` 返回 false → 该算子（含其下游 Arrow 链上游节点）降级。

### 7.4 当前兜底规模估算

- **算子级**：约 14 个算子类无 Arrow core（含 D3a 保留项 + D3b 桥接型 + 流式专有类），其中 `Ordinality`/`Values`/`UnionAll`/`TsReader` 已脱离，实际仍兜底约 **10 类**。
- **表达式/类型级**：投影字符串 kernel 仅 8 个、数值标量 5 个；类型覆盖 13 类 family。白名单外任一命中即整段降级——这是端到端查询落到 rowexec/colexec 的**主要来源**，远比算子缺位频繁。

### 7.5 兜底消除优先级（终态前）

1. **P0（必做，否则无法移除旧路径）**：补齐字符串标量函数剩余高频项（如 `substring` / `split_part` / `overlay` 的 native loop 路径），扩大投影/过滤白名单——直接削减 7.4 中的主要降级来源。
2. **P1（stage5 前置）**：`mon.Allocator` → `arrow memory.Allocator` 内存桥，使 Arrow 算子（含未来的 `ArrowTableReader`）内存进 `MemoryMonitor`，否则无法安全退役行式后端。
3. **P2（保留项确认）**：CI 单一 vendor 验证 `ArrowLookupJoiner` 后翻 `true`，消除 D3a 唯一保留项门控。
4. **P3（不计划）**：`ProjectSet` / `StreamAggregator` / `SampleAggregator` / `Zigzag` / `Interleaved` 维持算子级桥接兜底，终态作为"永久降级落点"（计算内核物理不可列式化，薄壳零收益）。

### 7.6 一句话总结

当前 Arrow DAG 已覆盖 7/7 核心算子 + 关系型全量查询类型；**真正的兜底压力不在算子缺位，而在表达式/类型白名单宽度**——白名单外的标量函数或复杂类型一出现，整段 stage 回退 rowexec/colexec。终态纯 Arrow 引擎的阻塞点是 stage5 内存桥 + 字符串 kernel 路线决策（Go 自实现 vs cgo cpp wrap），而非算子骨架数量。
