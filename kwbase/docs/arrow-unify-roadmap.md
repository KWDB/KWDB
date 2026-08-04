# KWDB Arrow 统一执行引擎 — 路线图与覆盖率矩阵

> 本文档是 `arrow-unification-architecture.md` 与 `arrow-unification-gap-analysis.md` 的**执行追踪文档**。
> 目标：以 Arrow (`arrow.Record`) 为统一列式载体，最终替代 `rowexec`（按行 EncDatumRow）与 `colexec`（coldata.Batch）两套老执行路径。
> 分支：`arrow-unify`，最新落地 commit：`d400646f`。

## 0. 当前覆盖状态（截至 2026-08-04）

### 0.1 算子层（7/7 Arrow core 已实现并注册）

| 算子 | Arrow core | 接收端工厂 | Planner 开关挂钩 | 端到端打通 | 备注 |
|------|-----------|-----------|----------------|-----------|------|
| Projection | `ArrowProjection` | `newArrowProjectionProcessor` (processors.go:143) | `arrowProjectionEnabled`+`canArrowRender` (physical_plan.go:1355) | ✅ | 字符串 trim/replace、数值标量已扩展 |
| Filter | `ArrowFilter` | `newArrowFilterProcessor` (processors.go:149) | `InterceptArrowFilterForScan` (distsql:1820) | ⚠️ | 仅扫描侧 filter 拦截；独立 filter 阶段/多谓词组合待补 |
| Aggregator | `ArrowAggregator` | `newArrowAggregatorProcessor` (processors.go:155) | 3926/4597/4830 | ✅ | SUM/MIN/MAX/COUNT/AVG/BOOL_*/STDDEV/VARIANCE 已完成 |
| Join | `ArrowJoin` | `newArrowJoinProcessor` (processors.go:161) | `ArrowJoinEnabled` (distsql:5541/5573) | ✅ | inner/left/right/full + inner 非 equi 后置过滤 |
| Sorter | `ArrowSorter` | `newArrowSorterProcessor` (processors.go:167) | `ArrowSorterEnabled` (distsql:3257) | ✅ | 全序排序 |
| Distinct | `ArrowDistinct` | `newArrowDistinctProcessor` (processors.go:173) | `ArrowDistinctEnabled` (distsql:4381/6377/6593) | ✅ | UNION/INTERSECT/EXCEPT DISTINCT |
| Windower | `ArrowWindower` | `newArrowWindowerProcessor` (processors.go:179) | `ArrowWindowerEnabled` (distsql:6995) | ⚠️ | 仅无 frame 分区聚合最小子集；缺 frame/排名函数 |

### 0.2 表达式/标量层

| 类别 | 状态 | 覆盖范围 |
|------|------|---------|
| 列引用 / 常量 | ✅ | index var + 字面量 |
| 数值标量 | ✅ | `abs/sqrt/ln/sign/power`（arrow/compute v17 kernel） |
| 字符串函数 | ⚠️ | `trim/ltrim/rtrim/btrim/replace`（P1 已做）；`substring/upper/lower/concat/length/like` 等回退 `tree.Datum` |
| 比较/逻辑/算术 | ✅ | 投影/过滤已覆盖基础算子 |
| CAST 类型转换 | ⚠️ | 过滤侧 int/float/bool/string 已支持；投影 decimal↔string 等待补 |
| LIKE / IN / IS NULL | ⚠️ | LIKE/ILIKE 已支持；IN/NOT IN 已支持（int/string 列）；IS NULL 待补 |
| 聚合输入多列 | ✅ | `Inputs []string` + `SQRDIFF/FINAL_VARIANCE` 三输入合并 |

### 0.3 桥接与扫描

| 组件 | 状态 | 说明 |
|------|------|------|
| `UnifiedProcessor` 接口 | ✅ | arrow_adapter.go:34 已定义，7 算子均实现 |
| `unifiedInputFrom` | ✅ | 所有 Arrow 算子通过它将上游 RowSource 适配为 UnifiedProcessor |
| `arrowScan`（native scan） | ⚠️ | arrow_adapter.go:340 已存在，但 table reader **未默认走** Arrow scan |
| `arrow_bridge.go`（coldata↔arrow） | ✅ | 零拷贝桥已存在，colexec 旁路可接入 |
| 统一 DAG 调度层 | ❌ | planner 仍按算子逐个散落 `useArrow` 分支决策，无统一编排 |

## 1. 六大差距（详见 gap-analysis.md）

1. **表达式/标量覆盖缺口最大**：投影/过滤中大量函数仍 `default:` 回退 `tree.Datum` 行式计算（arrow_adapter.go:146/163/286/333/454/542/572）。
2. **扫描端未 Arrow 化**：`newTableReader` 默认产出行式 EncDatumRow，`arrowScan` 未被默认启用。
3. **三引擎未统一调度**：`UnifiedProcessor` 仅算子内部使用，planner 无统一 DAG 编排层。
4. **未覆盖算子**：Top-N、递归 CTE、采样、窗口 frame、排名函数（row_number/rank/dense_rank）、集合运算的 ALL 变体。
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
- [x] 类型门控：`arrowScanSupported(typs)` 新增（所有列 `arrowDataTypeForKWType` 不报错即支持）。时序 Float/time、decimal、timestamp 经 `newArrowBuilder` 已支持；Date/Interval 已于 2026-08-04 纳入，故常规模拟类型均覆盖；真正不支持的（Bytes/Array/INet/Time/TimeTZ/Oid/...）由 `arrowDataTypeForKWType` 的 default 报错在 schema/builder 构建阶段失败快路径。
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
- [x] 收敛 arrow_adapter.go 的 `default:` 回退分支（2026-08-04 落地）：`arrowTypeForKWType` 改为 checked 版 `arrowDataTypeForKWType(t) (arrow.DataType, error)`，`default` 显式报错而非静默回退 `Int64`；`newArrowBuilder` 改为返回 `(array.Builder, error)`，`default` 同样显式报错（不再静默建 `Int64Builder`）；两处 schema 构建调用点（`NewRowToArrowConverter` 经 `initErr` 字段在 `Next` 上抛、`arrowScan.build` 经 `release()` 释放后返回）统一失败快路径。`buildArrowColumns`/`appendEncDatum` 的 `default` 本就显式报错，现三处入口语义一致：任何**真正**不支持的 family（Bytes/Array/INet/Time/TimeTZ/Oid/...）在 schema/builder 构建阶段即明确失败，杜绝「Int64 伪装 → 后续类型断言 panic」的隐藏陷阱。

### 阶段 3 — 统一调度层
- [x] 统一 Arrow plan 序列化与降级约定：新增 `marshalArrowPlan(plan) (*Expression, bool)`（`arrow_unification.go`），取代 planner 中散落的 8 处裸 `arrowUnificationMarshal` 调用（sorter / distinct / window / 2× agg / 2× setop / filter-Intercept），把「序列化失败即降级行式」语义收口到一处。join 两处保留原始 `return err` 控制流（更保守，未动）。
- [x] 清理 `arrow_unification.go` 末尾的 `var _ = physicalplan.ArrowAggregatorEnabled` 包循环占位 hack，移除该文件对 `physicalplan` 包的冗余导入。
- [x] `UnifiedProcessor` 契约（Next() (arrow.Record, bool, error)）已就位，rowexec 各 `ArrowXxxProcessor` 均实现之；planner 经 `ProcessorCoreUnion.ArrowXxx` 字段只选 core、不关心内部，已是「统一调度」形态。
- [ ] planner 引入 `buildUnifiedStage`：以 UnifiedProcessor 契约为中心把 `ArrowXxxEnabled && canArrowX && build && marshal && AddXXXStage || fallback` 收成单一编排入口。**待 CI 验证后做**：各算子 `AddXXXStage` 类型不同（NoGrouping/Noop/SingleGroup）且 return/continue 语义各异，强行泛型化风险高、本环境无法 `go test`，留待带 `libkwdbts2` 的 CI 环境批量替换并回归。
- [ ] colexec 经 `arrow_bridge` 纳入统一 DAG（跨引擎统一路由，独立于上述收口）

### 阶段 4 — 剩余算子补齐
- [ ] Top-N（Limit + Ordered 合并 Arrow 阶段）
- [ ] 窗口 frame（ROWS/RANGE）与排名函数 row_number/rank/dense_rank
- [ ] 集合 ALL 变体（UNION ALL 短路）
- [ ] 递归 CTE / 采样（若纳入 Arrow 路线）

### 阶段 5 — 内存治理 + 老路径灰度退役
- [ ] 统一 `memory.Allocator` 收敛至 `execinfra.MemoryMonitor`
- [ ] 每算子 `ArrowXxxEnabled` 默认开启（先 shadow，后 cut-over）
- [ ] rowexec/colexec 非 arrow processor 灰度下线（保留回退开关）

## 3. 构建与验证约束
- GOPATH 模式：真实 module `gitee.com/kwbasedb/kwbase`，`github.com` 为同 inode 软链。
- 编译：从 `gitee.com` 路径 + `GOFLAGS=`（清空 vendor 冲突）。
- 库包可编译：`go build ./pkg/sql/rowexec/ ./pkg/sql/physicalplan/`
- 测试：需 C++ 引擎预编译库 `libkwdbts2` + `KWDB_LIB_DIR`（本环境无，`go test` 链接失败；测试供 CI 跑）。
- 验证命令：`make test PKG=./pkg/sql/rowexec/arrowpilot`
