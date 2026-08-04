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
- [ ] 新增 `ArrowScanEnabled` 开关（`sql.arrow_scan.enabled`，默认 true）
- [ ] `newTableReader` 在开关下包装为 `UnifiedProcessor` 产出方（实现 `ArrowRecordEmitter`），使 scan 即起点时也能以 Arrow Record 暴露给 planner 统一编排
- [ ] 类型门控：时序 Float/time、decimal、timestamp 经 `newArrowBuilder` 已支持，无需额外改
- [ ] 该改造属接口对齐，非 fetcher 内部重写；真正的 fetcher 直出 Arrow builder（省 EncDatum 解码）列入阶段5 性能收尾

### 阶段 2 — 表达式/标量覆盖扩展【部分落地】

**已落地（2026-08-04）**：
- [x] 过滤侧 **IN / NOT IN**：`x IN (1,2,3)` / `x NOT IN (...)`，列 IN 常量集合，native Go kernel（`evalIn`，arrow_filter.go），支持 int64 / string 两类列（同 family 常量）。
- [x] 过滤侧 **IS NULL / IS NOT NULL**：KWDB 以 `ComparisonExpr EQ/NE DNull` 表达（无独立 `Is` 运算符）；`canArrowFilterExpr`/`buildArrowFilterNode` 识别该形态并生成 `is_null`/`is_not_null` 单操作数节点，`evalIsNull` 遍历列有效性位图（任意类型）。
- [x] 投影侧 **overlay(str,substr,start) / split_part(str,sep,n)**：native loop（`evalArrowOverlay`/`evalArrowSplitPart`），经 `arrowStringFuncName` + `canArrowRender` 接入。并把 `replace`/`trim`/`ltrim`/`rtrim`/`btrim` 补入 `eval` switch（此前缺失会错误回退 compute kernel）。
- [x] 过滤 `LIKE`/`ILIKE`（含非字符串左操作数 CAST 到 string）—— 既有
- [x] 过滤 `CAST`（int/float/bool/string）—— 既有
- [x] 投影字符串 trim/ltrim/rtrim/btrim/replace、数值标量 abs/sqrt/ln/sign/power —— P1 既有

**待做**：
- [ ] 投影 `CAST` 常用类型对（decimal↔string 等）Arrow 路径：`canArrowRender`/`addArrowRendering` 均缺 `CastExpr` 分支，目前投影 CAST 整体回退行式。arrow/compute v17 cast kernel 对 decimal128↔string 支持需确认（vendored 版本可能缺），故暂仅规划 int/float/bool/string 互转的安全子集。
- [ ] 逐步收敛 arrow_adapter.go 的 `default:` 回退分支

### 阶段 3 — 统一调度层
- [ ] planner 引入 `buildUnifiedStage`：以 UnifiedProcessor 契约统一编排，取代各算子散落 `useArrow`
- [ ] 统一 Arrow core 构造入口（proj/filter/agg/join/sort/distinct/window 共用 `canArrowX` 判定）
- [ ] colexec 经 `arrow_bridge` 纳入统一 DAG

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
