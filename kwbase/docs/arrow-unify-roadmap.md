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
  - **开关**：`sql.arrow_values.enabled`（默认 false，`ArrowValuesEnabled` 门），`createPlanForValues` 经 `arrowValuesCoreFor` 助手（同 `arrowSorterCoreFor` 风格，返回 `(ProcessorCoreUnion, bool)`）切换；false 或解码/类型不支持时回退经典 `Values` core。
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
| B 补齐未 Arrow 化算子 | ProjectSet / Ordinality / Values / ZigzagJoiner / InterleavedReaderJoiner / StreamAggregator / SampleAggregator | 中-高 | ~6-10 人周（**修订于 2026-08-07**：经重新评估，可/应 Arrow 化者为 Ordinality（已完成）、Values（P1 排期、纯常量源零依赖）、ZigzagJoiner / InterleavedReaderJoiner（D3 非对称、输出侧可 Arrow 化，KV 仅作输入取数方式）；不纳入为 StreamAggregator（流式专有）、ProjectSet（变长展开，D2）、SampleAggregator（采样，§1 已明确）。详见 §6.3） |
| C 表达式全覆盖 | 字符串函数（substring/upper/lower/concat/length/like/trim/replace）、CASE/COALESCE、CAST 全类型、日期时间函数、floor/ceil/round、窗口 RANGE/ROWS frame | 高 | ~8-12 人周（CASE/COALESCE + 窗口 ROWS 偏移 frame + 投影 floor/ceil/round/trunc + 过滤 IN decimal 集合 + 投影日期时间函数 extract/date_trunc 已落地；剩字符串函数投影已存在仅过滤路径待向量化、CAST 全类型、now/age 等时间函数、窗口 RANGE 偏移 frame） |
| D colexec 随覆盖率提升逐步收窄至可去除 | **措辞审订（2026-08-06）**：colexec 非架构硬约束，是 Arrow 覆盖率不足时的过渡性兜底层。关系型 `colbatch_scan`/`cfetcher` 可被 ArrowScan 替代；时序读可被 `ArrowTsReader` 替代（§6.7/§6.8）；Arrow 未覆盖点兜底随表达式/类型/D3 全覆盖而消失。终态 colexec 可完全去除，仅留 Arrow 主 + rowexec 永久兜底（详见 §6.8/§6.10） | 中 | ~4-6 人周（关系型扫描替代 + ArrowTsReader + D3 非对称算子；colexec 删除为覆盖率达标后的清理动作） |
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

**与"ArrowScan 是否支持时序"的澄清**：严格说"ArrowScan"是关系型 KV scan 喂 Arrow 的入口开关；时序 scan 不经此开关。更准确的说法是**新增一条「时序 scan 经关系格式 buffer 桥接喂 Arrow」的独立入口**。本项不在当前 arrow-unify 路线图阶段内，作为后续候选专项（估计 scan 侧 ~0.5 人周 / 算子侧类型补全 ~3-5 人周）。

**落地状态（2026-08-06 续）**：scan 侧独立入口**已完整实现并编译通过**（非原型），具体如下：
- `pkg/sql/rowexec/arrow_ts_reader.go`：`arrowTsReader` 嵌入 `*TsTableReader`（复用 tse FFI `NextTsFlow`/`DropHandle`），经 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，实现 `ArrowRecordEmitter`（`ArrowOutput()`）。支持两种消费模型：`arrowMode=false`（行式推流，行为=关系型 `TsTableReader`，Arrow 旁路）与 `arrowMode=true`（Arrow 直连，下游经 `unifiedInputFrom→ArrowOutput()` 零拷贝取单 Record；内部用 `discardReceiver` 吞掉 tse 的逐行 push，避免推流死锁；流耗尽时 `DropHandle` 释放 tse，幂等）。
- `pkg/sql/physicalplan/physical_plan.go`：新增 `ArrowTsScanEnabled`（`sql.arrow_ts_scan.enabled`，默认 `false`）+ `ArrowTsScanSupported`（类型门，复用 `arrowSupportedCompareType`，非 `types.Timestamp/TimestampTZ/Decimal` 时返回 false——算子侧类型补全前仅放行 int/float/bool/string/bytes/decimal 数值指标与字符串 tag）。
- `pkg/sql/rowflow/row_based_flow.go`：`setupInputSyncs` / `arrowTsEmitter` 实现 **Arrow 算子直连 wiring**——单输入 QUEUE TS stream，开关+类型门满足时构造 `arrowTsReader(arrowMode=true)` 缓存于 `f.arrowEmitters[sid]`，直接作为下游 Arrow 算子的 `input`，绕过 RowChannel 中转，走 operator-to-operator 快路；`Cleanup` 对 emitter 也调 `DropHandle`（幂等保险）；`SetupInboundStream` 的 QUEUE 分支保留 `arrowMode=false` 行式兼容路径（开关关/类型不支持时降级）。
- 并发安全：`arrowMode=true` 的 `arrowTsReader` 不加入 `f.TsTableReaders`（flow 永不调其 `RunTS`），由下游 `ArrowOutput()` 单一驱动 `pullRecord`，无 `RunTS` 与 `ArrowOutput` 双路争用；`RunTS`/`ConsumerClosed` 已覆盖为防御+生命周期释放。下游 Arrow 算子的 `unifiedInputFrom` 自动识别 `ArrowRecordEmitter` 走直连（`NewArrowRecordSource`），无需改下游算子。
- 默认 `sql.arrow_ts_scan.enabled=false`，不影响现有 TS 读路径；开启后仅当下游为 Arrow 算子且列类型受支持时激活直连，否则经 `arrowMode=false`/行式 `TsTableReader` 降级。
- 算子侧类型补全（TIMESTAMP/TIMESTAMPTZ Arrow 映射 + 解除 7 处 `EngineTypeTimeseries→return false` 并逐算子验证）仍为后续专项，见 §6.7 第 2 点。

#### 6.8 colexec 是否仍需保留兜底（2026-08-06 分析，2026-08-06 再审订）

**结论（审订后）：colexec 不是架构硬约束，是「Arrow 覆盖率不足时的过渡性兜底层」。在 Arrow 终态（全算子 + 全表达式/类型 + D3 非对称算子 + 时序读均被 Arrow 覆盖）下，colexec 可被完全去除，仅保留 Arrow 主路径 + rowexec 最底层兜底。**

**早期（6.8 初版）的三层保留理由，经后续讨论逐一审订**：

1. ~~**时序读独占于 colexec（硬约束）**~~ → **已推翻（2026-08-06）**。`TsReaderOp`（`colexec/ts_reader.go`）拆为两层：① tse C++ 引擎 FFI（`SetupTsFlow`/`NextVectorizedTsFlow`/`CloseTsFlow`，`ts_reader.go:189/251/388`）——这是公共底层读接口，不绑定 colexec；② Go 侧 buffer 装配（297-306 行 `vec.Append` 把 `tro.Rcv.Data[i]` 转 `coldata.Batch`）——纯适配器逻辑。**可被 `ArrowTsReader` 算子替代**：保留 tse FFI，仅把 `vec.Append` 换成 `arrow_adapter.go` 的 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，使 `TsReaderOp` 变为 `ArrowRecordEmitter`。时序读不再是 colexec 的硬约束（见 §6.7 + 本款）。

2. **Arrow 运行时降级兜底层（列式兜底）** → **是当前工程权宜，非架构必然**。降级发生的真实场景只有：A）Arrow 表达式/类型盲区（如未向量化的字符串谓词、RANGE 偏移 frame）；B）marshal 序列化失败（健壮性兜底，随 Arrow 成熟趋零）；C）Arrow 未实现的算子（D3/时序读，已论证均可 Arrow 化）。**若 Arrow 终态覆盖全部表达式/类型/算子，场景 A/B/C 均消失，降级网无处挂靠 colexec**——此时 colexec 无存在必要。

3. **Arrow↔colexec 双向桥是运行链路** → **双向桥的 colexec 侧（`RecordToBatch`，`columnarizer.go`）依赖 colexec 存在**；若 colexec 全去，桥只需保留 Arrow↔rowexec 方向（`BatchToRecord` 已是 Arrow→RowSource，rowexec 侧消费），colexec 侧自然消失。故桥不构成 colexec 的保留理由。

**修正结论**：colexec 的保留理由是**覆盖率驱动的过渡性**——Arrow 每多覆盖一类算子/表达式/类型，colexec 的兜底份额就收窄一分；当 Arrow 完整覆盖（含 D3 非对称算子、ArrowTsReader 时序读、全表达式/类型），colexec 归零。它从「必留的备」降级为「覆盖率不足时的临时备」，与 rowexec（永久兜底）性质不同。

**修正建议（落到路线图）**：
- 6.2 表中 D 项「退役 colexec」改为「**colexec 随 Arrow 覆盖率提升而逐步收窄，终态可完全去除**」；关系型 `colbatch_scan`/`cfetcher` 可被 ArrowScan 替代，时序读可被 `ArrowTsReader` 替代，Arrow 未覆盖点兜底随覆盖面扩大而消失。
- 6.3 阻塞点第 5 条（时序读独占 colexec）**撤销**——时序读经 `ArrowTsReader` 可 Arrow 化，不构成 colexec 保留理由（见本款第 1 条）。

#### 6.9 rowexec 兜底范围的精确界定 + D 类算子细分（2026-08-06）

**核心问题**：rowexec 里一批「关系型读/计算算子」（lookup join 系、zigzag/interleaved/mergeJoiner、ProjectSet 等）在 colexec 也无对应实现，被笼统归为「rowexec 独占、不可退出」。但其中多数**算子的计算本质是基于 Datum Row 的 equality / on-condition，KV 只是输入读取方式（经 `row.Fetcher`），算子本身与 KV 无关**——那么能否像 A/B 类（hash-join/filter）那样被 Arrow 替代？

**关键区分：算子的「计算模型」与「执行编排」是两层**：
- 计算模型（Datum Row 上的 equality / on-condition / 投影）确与 KV 解耦，理论上可 Arrow 化；
- 能否 Arrow 化取决于**执行编排**是否落在现有 8 个 Arrow 助手的「对称批处理 / 单 Record」模型内。

**D 类（colexec 与 Arrow 都无对应实现，rowexec 独占）按真实原因细分**：

| 子类 | 算子 | 是否真不可 Arrow 替代 | 真实原因 |
|---|---|---|---|
| **D1 真不可（写/DDL/采样/校验/流式）** | 时序写 DML（tsInserter/tsDeleter/tsTagUpdater）、bulkRowWriter、remoteDDL、tsCreateTable/tsAlterTable、scrubTableReader、sampler/sampleAggregator、countRows、streamAggregator（流式引擎专有） | **是** | 写/DDL/采样/数据校验无「列式只读计算」等价；Arrow 是只读向量化引擎，不承载写与统计采集语义 |
| **D2 不可（无 vectorized 语义）** | ProjectSet（set-returning 函数，一行产出多行） | **是（架构性）** | 当前 8 个 Arrow 算子均为「单 Record 输入→单 Record 输出」模型，不表达「一行→多行」的生成列语义；需 Arrow 支持 set-returning 才有机会 |
| **D3 可但工程未做（非对称编排）** | **joinReader / indexJoiner / batchLookupJoiner / zigzagJoiner / interleavedReaderJoiner / mergeJoiner（行式）** | **否，可 Arrow 化** | 计算是 Datum Row 基础，KV 只是读；缺的是「Arrow 非对称 lookup 编排算子」骨架，非本质障碍 |

**以 lookup join（joinReader）为例（代码核对 `rowexec/joinReader.go`）**：
- join 计算本身：`keyToInputRowIndices` / `inputRowIdxToLookedUpRowIdx` 多对多映射 + equality + on-condition，全是 Datum Row 基础，与 KV 解耦。
- 真正非 Arrow 化的不是「计算」，而是**执行编排的非对称性**：左输入是流，右「表」是按需按 key 反查 KV（`jrStateUnknown→jrReadingInput→jrPerformingLookup→jrEmittingRows` 状态机 + `span.Builder` 攒批 + `fetcher.StartScan/NextRow` 回填）。
- colexec 也未收口它：`colexec/execplan.go:483` 在 `vectorize=auto` 下仅接受 `JoinReader`/`BatchLookupJoiner` 两个 rowexec core 做 wrapping，自身无 parallel lookup joiner。
- **可 Arrow 化路径**：左输入经 `arrow_bridge.NewRowSourceToArrow`（`colexec/arrow_bridge.go`）收 Arrow Record；lookup 反查仍走 `row.Fetcher` 把 KV 行转 `EncDatumRows` 再经 `arrow_adapter.go` 的 `rowToArrowConverter` 成 Arrow Record；另写 `ArrowLookupJoiner` 算子持有状态机（攒批/反查/回填多对多映射），在 Arrow 域内算 equality + on-condition。即**不是不能，是需新增非对称编排算子**（类似当初为 hash-join 写 `ArrowJoin`）。

**修正结论**：
- rowexec 的**真正不可替代范围**比"D 类全不可退出"更窄——仅剩 **D1（写/DDL/采样/校验/流式）+ D2（ProjectSet）**，约 rowexec processor 的一半。
- **D3（含 lookup join 系、zigzag/interleaved/行式 mergeJoiner）可随 Arrow 覆盖而退出 rowexec**，但需新增对应非对称编排算子，属"尚未 Arrow 化"而非"不可 Arrow 化"。
- 因此阶段 6「rowexec 兜底」的精确定性应为：**rowexec 永远是最底层兜底残差层**（Arrow 主 → colexec 列式兜底 → rowexec 行式兜底 + D1/D2 独占算子）。D1/D2 不可退出；D3 可退出但工程量大（每个需新写 Arrow 算子骨架）。rowexec 代码量**不会随 Arrow 覆盖而显著缩减**（D1/D2 不随 Arrow 演进而消失），但 D3 的退出可逐步收窄其关系型读算子份额。

#### 6.10 重构终态基线：三层「主-备-兜底」长期并存（2026-08-06 总结）

综合 §6.7（时序路径）、§6.8（colexec 兜底）、§6.9（rowexec 兜底），Arrow 统一引擎的**终态是两层：Arrow 唯一主路径 + rowexec 永久兜底；colexec 是「覆盖率不足时的临时备」，终态可去除**。澄清 §6.8 初版「colexec 不可去除」的误判——经审订，时序读（`ArrowTsReader`）与 Arrow 未覆盖点兜底均随 Arrow 覆盖率达终态而消失，colexec 非架构硬约束。

**终态结构（两层 + 一层临时备）**：

| 层 | 终态角色 | 保留内容 | 能否去除 |
|---|---|---|---|
| **Arrow** | **唯一主路径** | 关系型全算子（scan/filter/agg/join/sort/distinct/window/union-all/ordinality）+ 全表达式/类型覆盖（含此前盲区字符串谓词、RANGE frame）+ D3 非对称算子（ArrowLookupJoiner 等，§6.9）+ `ArrowTsReader` 时序读（§6.7/§6.8） | 目标本身，主路径 |
| **colexec** | **临时备（覆盖率不足时）** | Arrow 尚未覆盖的表达式/类型/算子（D3、时序读、字符串谓词盲区）的列式兜底 + Arrow↔colexec 双向桥（`BatchToRecord`/`RecordToBatch`） | **终态可完全去除**：Arrow 覆盖率达 100% 后无挂靠点；桥仅保留 Arrow↔rowexec 方向 |
| **rowexec** | **永久最底层兜底** | **D1**（写/DDL/采样/校验/streamAggregator，Arrow 只读无等价）+ **D2**（ProjectSet，一行产多行）+ 一切 Arrow 失败的降级落点 | D1/D2 不可去；D3 可随 Arrow 覆盖退出但兜底职责不变 |

**关键结论（审订后）**：
1. **colexec 是临时层，非永久层**：保留理由是 Arrow 覆盖率不足时的过渡性兜底（§6.8 审订）。当 Arrow 覆盖全算子/全表达式/类型/D3/时序读，colexec 归零。
2. **rowexec 是永久兜底**：D1/D2 是 Arrow 无等价语义的硬兜底，不可退出；D3 即便 Arrow 化，rowexec 仍是降级落点。
3. **Arrow 吃掉全部关系型主动执行 + 时序读 + D3 非对称编排**，colexec 仅作为覆盖率爬坡期的临时列式兜底存在。

**对阶段 6 措辞的最终定性**：
- 原「D 退役 colexec」「rowexec 兜底可退役」改为：**colexec 随覆盖率提升逐步收窄至可去除；rowexec 退化为永久兜底残差层（D1/D2 不可去）**。
- 工作量评估：关系型扫描/算子替代 + D3 非对称算子 + ArrowTsReader 为「新增 Arrow 算子」工作量；colexec 删除本身是覆盖率达标后的清理动作，非独立大项。
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
| **ZigzagJoiner** | 中 | 可 Arrow 化（D3） | KV 仅作两 side 的输入取数请求方式，**zigzag 交错比对 + join 拼接本身是 datum row 计算**；输出侧可 Arrow 化（fetcher 取数→攒/流式 Arrow Record），输入侧暂保留 KV，与 ArrowScan 同构 |
| **InterleavedReaderJoiner** | 中 | 可 Arrow 化（D3） | 同 Zigzag：KV fetch 是输入侧、`fetcher row.Fetcher` 取行，内部 merge-join 比较是 datum row 计算；输出侧可 Arrow 化接入统一 DAG |
| **SampleAggregator** | — | 不纳入 | 路线图 §1 已明确"采样未纳入 Arrow 路线"（统计采样非查询主路径） |

**结论**：B 项清单中原列的 7 个算子，经逐个评估（2026-08-07 修订，纠正此前"KV 特例即不可 Arrow 化"的误判）：
- **实际可/应 Arrow 化的 4 个**：
  - Ordinality（已完成，复用 Arrow windower `row_number`）
  - Values（高可行性，纯常量源、无 KV/引擎依赖，排期 P1 收口，零新算子）
  - ZigzagJoiner / InterleavedReaderJoiner（D3 非对称，输出侧可 Arrow 化，KV 仅作输入取数方式）
- **不纳入 3 个**：
  - StreamAggregator（流式专有，本就不走通用 ArrowAgg 路径）
  - ProjectSet（变长展开，超出现有单 Record 模型，D2 暂缓）
  - SampleAggregator（采样，路线图 §1 已明确不纳入）

至此**所有通用关系查询算子均已 Arrow 化**（原始 7/7 + UNION ALL + Ordinality）；Values 作为无依赖纯形态源排期 P1 收口，Zigzag/Interleaved 并入 D3 非对称算子清单与 `ArrowLookupJoiner` 同批规划。B 项目标实质达成，工作量从原估 6-10 人周收敛为 Ordinality（已完成）+ Values（P1）+ D3 非对称骨架（与 lookup join 同批）。

### 2026-08-07 — B 项误判修订：Values/Zigzag/Interleaved 可 Arrow 化
- **修订背景**：此前（2026-08-06）§6.3 B 项把 Values / ZigzagJoiner / InterleavedReaderJoiner 笼统归为"不纳入（常量源 / KV 扫描特例）"，理由是"带 KV / 特例源"。经复核源码，该归类错误——**KV 仅是算子的输入/输出请求方式，不是计算内核**：
  - `valuesProcessor`：无任何输入，`Next()` 仅 `StreamDecoder.GetRow` 解码 planner 预编码的 `spec.RawBytes` + PostProcess，零 KV、零引擎依赖，是纯常量行源。
  - `zigzagJoiner` / `interleavedReaderJoiner`：KV fetch（`row.Fetcher` / 双 side 扫描）是输入侧取数；**内部 zigzag 交错比对 / merge-join 拼接是 datum row 计算**，与已 Arrow 化的 `ArrowScan`（`KV fetcher 取数→Arrow Record`）同构。
- **修订结论（详见 §6.3 表与 §6.9）**：
  - **Values**：高可行性，纯形态转换，无依赖，**排期 P1 收口**（见阶段 4 新增项）。方案：启动时解码 `spec.RawBytes`→`buildArrowColumns` 成单 Arrow Record 当 `ArrowRecordEmitter`，新增 `sql.arrow_values.enabled` 开关 + `arrowValuesCoreFor` 助手。且不依赖 C++ 时序引擎，可本地独立验证（规避 P0 测试环境缺 TS engine 的坑）。
  - **ZigzagJoiner / InterleavedReaderJoiner**：D3 非对称，输出侧可 Arrow 化（fetcher 取数→Arrow Record），输入侧暂保留 KV，与 `ArrowLookupJoiner` 同批规划（需新增非对称编排算子骨架，非本质障碍）。
  - **不纳入仍维持 3 个**：StreamAggregator（流式专有）、ProjectSet（变长展开，D2）、SampleAggregator（采样，§1 已明确）。
- **影响**：B 项可/应 Arrow 化算子由 1 个（Ordinality）扩为 4 个；阶段 4 剩余可落地清单新增 Values；D3 非对称算子清单新增 Zigzag/Interleaved。通用关系查询算子 7/7 + UNION ALL + Ordinality 均已 Arrow 化不变；本修订仅扩展"后续可落地的 B 项算子"范围，不改变"已落地"基线。

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
- **rowexec 兜底范围精确界定（§6.9）**：纠正上一轮"D 类全不可退出"的过度简化。将 rowexec 独占算子按真实原因细分——**D1 真不可**（写/DDL/采样/校验/streamAggregator，Arrow 是只读引擎无等价）、**D2 不可**（ProjectSet 一行产多行，超出现有单 Record 模型）、**D3 可但工程未做**（lookup join 系/行式 mergeJoiner/**zigzag/interleaved**）。以 `joinReader` 为例：其 join 计算是 Datum Row 基础、KV 仅作输入读取，与 KV 解耦；`zigzag`/`interleaved` 同理——KV fetch 是输入侧取数方式、内部 zigzag 交错比对 / merge-join 拼接是 datum row 计算，真正未 Arrow 化的是「左流 + 按需 KV 反查 / 双 side 交错」的非对称编排，需新增 `ArrowLookupJoiner` / `ArrowZigzag` / `ArrowInterleaved` 算子骨架（类似 ArrowJoin），非本质障碍。结论：rowexec 真正不可替代范围仅 D1+D2（约半数 processor），D3 可随 Arrow 覆盖退出。
- **重构终态基线（§6.10 初版，已审订）**：初版纠正「最终保留 rowexec 的 D1/D2、colexec 可去除」的误判，定性为三层长期并存。后续经 §6.8 审订**推翻"colexec 不可去除"**，终态修正为**两层长期并存（Arrow 主 + rowexec 永久兜底）+ colexec 临时过渡层（覆盖率达标后可去除）**。详见 2026-08-06 续二。

### 2026-08-06（续二）— colexec 兜底性质审订（推翻"不可去除"）
- **时序读不再构成 colexec 硬约束（审订 §6.8 第①条）**：`TsReaderOp`（`colexec/ts_reader.go`）拆为两层——tse C++ FFI（`SetupTsFlow`/`NextVectorizedTsFlow`/`CloseTsFlow`，不绑定 colexec）+ Go 侧 buffer 装配（`vec.Append` 转 `coldata.Batch`，纯适配器）。后者可被 `ArrowTsReader` 算子替代：保留 tse FFI，仅把 `vec.Append` 换成 `arrow_adapter.go` 的 `buildArrowColumns`/`array.NewRecord` 攒成 Arrow Record，使算子变为 `ArrowRecordEmitter`。故时序读可 Arrow 化。
- **colexec 降级网/双向桥均为过渡依赖（审订 §6.8 第②③条）**：降级仅发生于 Arrow 表达式/类型盲区、marshal 失败、Arrow 未实现算子（D3/时序读，均已论证可 Arrow 化）；若 Arrow 终态全覆盖，降级网无处挂靠 colexec。双向桥的 colexec 侧（`RecordToBatch`）依赖 colexec 存在，colexec 去除后桥仅留 Arrow↔rowexec 方向。故 colexec **非架构硬约束，是覆盖率不足时的过渡性兜底层**。
- **终态修正（§6.10）**：三层「主-备-兜底长期并存」→ **两层长期并存（Arrow 主 + rowexec 永久兜底）+ colexec 临时过渡层（可去除）**。rowexec 永久兜底范围 = D1（写/DDL/采样/校验/流式）+ D2（ProjectSet）+ 一切 Arrow 失败降级落点；D3 可随 Arrow 覆盖退出。6.2 表 D 项改为「colexec 随覆盖率提升逐步收窄至可去除」；6.3 阻塞点第 5 条撤销。

### 2026-08-05
- 日期时间函数 `EXTRACT`/`DATE_TRUNC` 接入 Arrow 投影路径（§2.6.4，validator 见 §2.6 末）；9 个 Arrow 开关保持 `defaultEnabled=false`。
- 集合 ALL 变体 `UNION ALL` 短路（`ArrowUnionAll` 算子）落地。
- 字符串函数投影侧全 Arrow 化；CAST 部分类型覆盖。

### 2026-08-07 — 阶段 C 表达式覆盖（过滤字符串/字节函数、CASE/COALESCE、RANGE 偏移 frame）
- **过滤路径字节（Bytes）列支持（P1 类型补全）**：`arrow_filter.go` 的 `eval` 选择步骤对 `arrow.IsBinaryLike` 列（Binary/LargeBinary）改用 Go 路径 gather（`compute.Filter` 的 vendored `FilterBinary` kernel 对变长 offset 布局 `GetSpanOffsets` 越界 panic，已知生态 bug），其余类型仍走原生 `compute.Filter` 快路径。配合 2026-08-07 的 Bytes schema/builder + 分组/连接比较支持，含 `WHERE blob_col = x` 的 BYTES 列过滤端到端走 Arrow 且结果正确（测试 `arrowpilot/arrow_unify_filter_bytes_test.go`）。
- **过滤路径 CASE / COALESCE（阶段 C 表达式覆盖）**：`canArrowFilterExpr`（`arrowFilterLeafFromExpr`）新增 `*tree.CaseExpr`/`*tree.CoalesceExpr` 分支，复用投影侧 `arrowCaseCol`/`arrowCoalesceCol` 生成 `Kind:"case"` 的 `arrowProjectionCol`，作为 `arrowFilterLeaf.Case` 叶子；executor `arrowFilterCore` 经新增 `evalCtx` 字段，在 `evalLeafDatum` 对 `Case` 叶子复用投影 `evalCase` 求值（所有 arrow 值类型 / 嵌套分支统一支持）。`arrowFilterLeafJS`/`ArrowArg`/`leafToArrowArg`/`specForCol`（提升为包级 `arrowProjectionSpecForCol` 供 filter 复用）同步打通序列化链。测试 `arrowpilot/arrow_unify_filter_case_test.go` 覆盖 `CASE WHEN ... THEN ... ELSE ...` 与 `COALESCE` 谓词，与 classic 路径一致。
- **窗口 RANGE 偏移 frame（阶段 C 表达式覆盖）**：`rangeFrameBounds` + `offsetDatum` 实现 value-based RANGE 偏移帧求值（按 ORDER BY 值做 ±offset 二分，覆盖 int/float/decimal/timestamp），并有单测 `rowexec/arrow_windower_range_test.go` 验证。但**端到端暂未打通**：Arrow windower 在 RANGE 偏移 frame 的执行层（分区处理 / 排序值比较）仍有 bug（实测仅输出部分 partition、每帧退化为单行），属 windower 偏移 frame 执行问题，与已落地的 ROWS 偏移 frame 同源待修。当前 `isSupportedWindowFrame` 对 RANGE 偏移边界仍回退 classic 路径；`rangeFrameBounds`/`offsetDatum` 已就绪，待 windower 偏移执行层修复后即可启用。
- **验证**：`go build ./pkg/sql` 通过；新增/修复 `arrowpilot` 与 `rowexec` 单测（Bytes filter、CASE/COALESCE filter、Bytes 类型、RANGE frame bounds）均 PASS。
