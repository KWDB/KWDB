# Arrow 统一化 —— 进度 / 计划 / 上下文交接文档（HANDOFF）

> 用途：本文件用于「切换机器 / 新会话」时无需重新探索即可继续推进 KWDB 的 Arrow 统一化工作。
> 最后更新：**2026-07-29**（Arrow 统一化源码 + 文档已 commit 并 push 到 `kwdb-exec` 远端 `arrow-unify` 分支，commit `491776a8`；`vendor` 子模块的 Arrow v17 补丁仅本地提交 `26d879c`、未推 kw-vendor.git，见 §8.1）。
> 配套权威设计文档：`docs/arrow-unification-architecture.md`（§1–§12 是完整演进记录，本文件是「快进快照」）。

---

## 0. 一句话状态

四个算子（投影 / 过滤 / 聚合 / 连接）的**浅统一 + 深统一（含算子间 Arrow 链式、原生 Arrow scan、聚合计算核向量化）已全部点亮并通过真实 SQL 端到端验证**；其中**聚合计算核已向量化（对齐 colexec 列式思想）、并通过 `TestArrowAggMatchesColexec` 与原始 colexec 算法做正确性 + 性能对照（Arrow 聚合核 ~1.9× 快于 colexec）**。当前默认全部关闭（opt-in 集群开关），开启后查真实走 Arrow compute 引擎。**全部 §7.1–§7.10 已完成**；后续候选见 §9。

---

## 1. 环境（最容易踩坑、必须照做）

仓库是 **GOPATH 模式、无 `go.mod`**，真实 Go module 是 `gitee.com/kwbasedb/kwbase`。`github.com/kwbasedb/kwbase` 只是一个指向它的**软链**，但用它编译会触发「双 vendor 前缀」导致 `apd`/`arrow` 同名不同类型冲突。

### 1.1 编译 / 测试必须从 gitee 软链路径

```bash
cd /home/sdy/go/src/gitee.com/kwbasedb/kwbase   # ← 必须从这个目录，不要从 github.com/... 编译

export GO111MODULE=off

# 链接 C++ 引擎 libkwdbts2（cgo 必需）。这是经实测可用的最保守命令：
export CGO_LDFLAGS="-L/home/sdy/go/src/github.com/kwbasedb/build/lib -lkwdbts2 \
                    -L/home/sdy/go/native/kwdbts2/third_party/lib -lz -lstdc++"
export LD_LIBRARY_PATH="/home/sdy/go/src/github.com/kwbasedb/build/lib:$LD_LIBRARY_PATH"

go build ./pkg/sql/...                       # 构建验证
go test  ./pkg/sql/rowexec/arrowpilot/ \
        -run 'TestArrowUnifyFilterAggJoin|TestArrowUnifyDecimalAgg|TestArrowUnifyJoinNonEqui|TestArrowUnifyFilterFuncs'   # 端到端验证
```

> ⚠️ 关于库路径的**已知不一致**：架构文档 §12.7 里写的是 `$PWD/../install/lib`（即 `gitee.com/kwbasedb/install/lib`，`.so` 也存在），但上面这条 `build/lib` 命令是经过实测、且额外带 `-lz -lstdc++` 与 `native/kwdbts2/third_party/lib` 的那一版。**优先用上面这条**；若 `build/lib` 缺失再用 `install/lib` 兜底（两者文件大小相同，都是 151 MB）。
> 注意：文档 §12.7 把两处库路径都写成了 `../install/lib`，与实际可用命令有出入，迁移时以本文件 §1.1 为准。

### 1.2 Arrow 版本

- vendored：`github.com/apache/arrow/go/v17`（v17.0.0，go 1.21 基线，含 `arrow/compute` 但**不含 `arrow/compute/aggregate`**，后者是我们自己补的）。
- **关键 API 差异（v17 实测）**：
  - `decimal128.Num` 的方法叫 `HighBits()` / `LowBits()`，不是 `High()` / `Low()`。
  - `array.NewDecimal128Builder(alloc, *arrow.Decimal128Type)` 是**两参**（旧版是单参）。
  - 标量构造：`scalar.MakeNullScalar(dt)`（非 `NewNullScalar`）、`s.IsValid()`（非 `IsNull`）、`scalar.String.Value` 是 `*memory.Buffer`（`string(.Value.Bytes())`）。
  - `array.Interface` 已移除，统一用 `arrow.Array`。
  - 算术函数名：`add/subtract/multiply/divide/negate`（不是 `add/sub/mul/div`）。

### 1.3 测试包编译状态（已修复）

- `pkg/sql/rowexec/arrow_aggregate_test.go` 历史上自带 `ctx declared and not used` 编译错误；**现已修复**（删掉 `TestAggregateArray` 里未使用的 `ctx`），并补了 DECIMAL kernel 单测（见 §4）。
- 现在可以安全运行：`go test ./pkg/sql/rowexec/ -run 'TestAggregateArray|TestArrowHashAggregator' -v`（含标量聚合 + 分组聚合并行验证，秒级）。
- 注意：**整包 `go test ./pkg/sql/rowexec/` 仍不建议直接跑**——它包含大量其它测试，很多需要完整 server / cgo 引擎，既慢又易触发无关失败。验证请走「`-run` 精确筛选用例」或 `arrowpilot` 端到端包。

---

## 2. 架构与文件地图

统一方式（浅统一 + 逐步深统一）：每个算子一个 **opt-in 集群开关**，planner 在开关开启且表达式 Arrow 可计算时，把该算子下发为一个 `ProcessorCoreUnion` 上新增的字段（承载 **JSON 计划**），由对应 `*Processor` 调用 Arrow compute / 自补 kernel 求值。关闭时行为与原生执行器完全一致，零回归。

### 2.1 新增 / 改动文件

| 文件 | 作用 | 状态 |
|------|------|------|
| `pkg/sql/arrow_unification.go` | planner 路由判定：`canArrowAggregate` / `buildArrowAggPlan` / `canArrowJoin` / `arrowJoinType` / `arrowSupportedCompareType` 等 | 已改 |
| `pkg/sql/rowexec/arrow_adapter.go` | `UnifiedProcessor` 接口、`rowToArrowConverter`（行→Arrow Record）、`arrowRecordSource`、`buildArrowColumns`（**含 DECIMAL 编码**）、`arrowTypeForKWType` | 已改 |
| `pkg/sql/rowexec/arrow_aggregate.go` | **纯 Arrow 聚合 kernel 层**（补 vendored Arrow 缺的 `arrow/compute/aggregate`）：`scalarAggregator` 接口 + `sumAgg`/`countAgg`/`minMaxAgg`/`meanAgg` + `arrowHashAggregator`；含 `apdToDecimal128` / `decimal128ToApd` helper | 已改 |
| `pkg/sql/rowexec/arrow_aggregator.go` | `NewArrowAggregator`、`groupKey`、`aggOutputType`、`arrayScalarAt`、`appendScalar`（DECIMAL128 分支） | 已改 |
| `pkg/sql/rowexec/arrow_aggregator_processor.go` | `arrowAggregatorProcessor`（`RowSource`，`ArrowAggRunCount()` 探针） | 已改 |
| `pkg/sql/rowexec/arrow_projection.go` + `arrow_projection_processor.go` | 投影算子（arrow/compute "add" 等） | 已改 |
| `pkg/sql/rowexec/arrow_filter.go` + `arrow_filter_processor.go` | 过滤算子（递归布尔树 + compare） | 已改 |
| `pkg/sql/rowexec/arrow_join.go` + `arrow_join_processor.go` | 自建 Arrow hash join（inner/left/right/full outer） | 已改 |
| `pkg/sql/colexec/arrow_bridge.go` | `coldata.Batch ↔ arrow.Record` 桥（`BatchToRecord`/`RecordToBatch`） | 已改 |
| `pkg/sql/distsql_physical_planner.go` | `addAggregators` / `createTableReaders` / hash join 创建处分流 | 已改 |
| `pkg/sql/physicalplan/physical_plan.go` | 4 个开关 setting（`Arrow*Enabled`）+ `AddRendering` 分流 | 已改 |
| `pkg/sql/rowexec/processors.go` | `NewProcessor` switch 增加各 Arrow core 分支 | 已改 |
| `pkg/sql/execinfrapb/processors.pb.go` | `ProcessorCoreUnion` 新增 `ArrowProjection`/`ArrowFilter`/`ArrowAggregator`/`ArrowJoin`（`*Expression`） | 已改 |
| `pkg/sql/rowexec/arrowpilot/e2e_test.go` | **端到端验证**（真实 server），含 `TestArrowUnifyFilterAggJoin` / `TestArrowUnifyDecimalAgg` / `TestArrowUnifyJoinNonEqui` / `TestArrowUnifyFilterFuncs` | 已改 |
| `pkg/sql/rowexec/arrowpilot/pilot_test.go` / `projection_bench_test.go`、`pkg/sql/colexec/arrow_bridge_test.go` | 算子级 round-trip / 基准 | 已改 |
| `pkg/col/colserde/{record_batch,file,arrowbatchconverter}.go`、`pkg/sql/colflow/colrpc/inbox.go` | arrow import 迁到 v17 | 已改 |
| `kwbase/vendor/.../apache/arrow/go/v17` + 补充依赖（见架构文档 §10.2） | vendored Arrow v17 + 离线 shim | 已改（含未跟踪） |

### 2.2 数据流（共享底座）

```
SQL
 └─ distsql_physical_planner.go（按算子开关分流）
       → ProcessorCoreUnion.{ArrowProjection|ArrowFilter|ArrowAggregator|ArrowJoin}（JSON 计划）
       → processors.go NewProcessor switch → newArrow*Processor
       → 共享底座：RowToArrowBatch、arrowRecordToEncDatumRows、p.Out.ProcessRow（仅施加一次、结果需 copy）
       → Apache Arrow v17 compute / 自补 kernel
```

---

## 3. 四个算子当前能力

| 算子 | 开关 | 已验证能力 | 已知限制 |
|------|------|-----------|----------|
| 投影 | `sql.arrow_projection.enabled` | 多列输出、常量操作数、`UMinus`、透传列（copy）；算术 add/sub/mul/div | 仅算术/比较；更多内置待扩 |
| 过滤 | `sql.arrow_filter.enabled` | 比较 EQ/LT/GT/LE/GE/NE + 逻辑 And/Or/Not + 嵌套二元算术；`LIKE`/`NOT LIKE`/`ILIKE`/`NOT ILIKE`（Go kernel，常量 pattern）；`CAST(col AS STRING/INT/FLOAT)` 类型转换（Go cast kernel，可作比较/like 的操作数）；单表扫描过滤（折进 TableReader 的 post.Filter）拦截 | 字符串函数（substring/length 等，Arrow 无字符串 kernel）、ILIKE 之外的字符串匹配、非字符串左操作数的 LIKE 待扩；`sql.arrow_filter.enabled` 全局开启会波及系统表扫描（见坑 10） |
| 聚合 | `sql.arrow_aggregator.enabled` | **全局 + 分组** `SUM/MIN/MAX/COUNT/COUNT(*)/AVG`，输入列支持 INT/FLOAT/**DECIMAL**/**TIMESTAMP/TIMESTAMPTZ**；分组键支持 INT/FLOAT/BOOL/STRING/DECIMAL/**TIMESTAMP/TIMESTAMPTZ**；全 NULL 组→NULL；null 语义对齐 SQL | `SUM/AVG` 仅数值（timestamp 只走 MIN/MAX/COUNT）；UUID/JSON 待补 |
| 连接 | `sql.arrow_join.enabled` | inner / left / right / full outer 等值连接；NULL 键不参与匹配；未命中侧发 NULL；**inner 连接支持非等值 `onExpr`（post-filter，§7.4）** | left/right/full 连接的非等值 `onExpr` 待扩（post-filter 语义不等价，仍走标准引擎） |

### 3.1 聚合 DECIMAL 支持细节（最近一步）

- **编码**：`buildArrowColumns` 的 `DecimalFamily` 分支把 `tree.DDecimal` 经 `apdToDecimal128(&dd.Decimal, scale)` 编码成 Arrow `decimal128`（precision 38）。
- **SUM/MIN/MAX over DECIMAL**：用 `apd.Decimal`（`tree.ExactCtx`）任意精度累加，finalize 时 `apdToDecimal128` 归一回 decimal128，对齐 SQL `SUM/MIN/MAX(decimal)→DECIMAL`。
- **AVG over DECIMAL/INT**：用 `apd.Decimal` 累加 sum，按 `tree.DecimalCtx.Quo` 除 count，产出 `meanDecimalType`（decimal128, precision 38, scale 9），对齐 SQL `AVG(decimal/int)→DECIMAL`；浮点输入产 float64。
- **DECIMAL 分组键**：`groupKey`（`arrow_aggregator.go`）与 `arrowGroupKey`（`arrow_aggregate.go`）均对 `arrow.DECIMAL128` 用 `HighBits()/LowBits()` 大端 16 字节序列化。
- **回程解码**：`arrowRecordToEncDatumRows`（在 `arrow_projection_processor.go` 等共享）已支持 `*array.Decimal128 → tree.DDecimal`。

---

## 4. 验证状态（已 PASS）

| 测试 | 覆盖 |
|------|------|
| `TestArrowUnifyFilterAggJoin` | 投影/过滤/聚合/连接四类，含全局+分组聚合、NULL 行、AVG（int→decimal）、inner/left/right/full outer join；并断言 `ArrowFilterRunCount`/`ArrowAggRunCount`/`ArrowJoinRunCount` 递增 |
| `TestArrowUnifyJoinNonEqui` | inner 连接的非等值 `onExpr` 端到端：列-常量 / 列-列 / AND 组合 / 全排除；断言 `ArrowJoinOnFilterRunCount` 递增 |
| `TestArrowUnifyDecimalAgg` | DECIMAL 列上的全局 / 过滤 / 按 INT 分组 / **按 DECIMAL 分组** 的 SUM/MIN/MAX/AVG；断言 `ArrowAggRunCount` 递增 |
| `TestArrowProjectionPlannerIntegration` / `TestArrowProjectionPlannerComputeExprs` / `TestArrowProjectionE2EWithRealSQL` | 投影接入 planner 的多种表达式 |
| `TestArrowBridgeRoundTrip` / `BenchmarkArrowBridgeRoundTrip` / `BenchmarkArrowProjection` | colexec↔Arrow 桥与算子级基准 |
| `BenchmarkArrowFilter` / `BenchmarkArrowAggregator` / `BenchmarkArrowJoin` / `BenchmarkArrowProjectionQuery` | 查询级 **原算子(Original) vs 重构算子(Arrow)** 对比基准（`arrowpilot/bench_test.go`）：跑 `Original`（默认，所有 arrow 开关关＝重构前标准算子）与 `Arrow`（开启对应 `sql.arrow_*.enabled` 并用 run-count 探针 `ArrowXxxRunCount` 确认路由到 Arrow 后才计时）两个子基准，并额外打印 `REFACTOR_COMPARE ... Original_per_Arrow_x=` 显式比例（§7.5） |
| `TestAggregateArray` / `TestAggregateArrayDecimal` | 标量聚合 kernel 单测：int/float/decimal 的 SUM/MIN/MAX/COUNT/AVG、null 跳过、全 null→null | 
| `TestArrowHashAggregator` / `...Global` / `...EmptyGlobal` / `...Decimal` | 分组/全局/空全局聚合 kernel 单测，**含 DECIMAL 分组键** |
| `TestAggregateArrayTimestamp` / `TestArrowHashAggregatorTimestamp` | 标量/分组聚合 kernel 单测：**TIMESTAMP 列**（含 TIMESTAMP 分组键） |
| `TestArrowUnifyTimestampAgg` | 端到端：TIMESTAMP 列全局/分组 MIN/MAX/COUNT，断言 `ArrowAggRunCount` 递增 |
| `TestArrowUnifyFilterFuncs` | 端到端：过滤函数扩展——`LIKE`/`NOT LIKE`/`ILIKE` 常量 pattern、`CAST(i AS STRING) LIKE '1%'`、`CAST(i AS STRING) = '2'`；断言 `ArrowFilterRunCount` 递增 |
| `TestLikeMatch` | 纯函数单测：`likeMatch` 的 SQL LIKE 语义（`%`/`_`/`\` 转义、ci） |

构建命令见 §1.1；全部从 gitee 路径、`GO111MODULE=off` 下通过。

---

## 5. 已固化的正确性不变量 / 坑（改代码前必读）

1. **双 vendor**：永远从 `gitee.com/kwbasedb/kwbase` 编译；arrow 类型断言要收敛到 rowexec 包内单实例（见架构文档 §11.6.4），测试别直接碰 arrow 类型。
2. **`p.Out.ProcessRow` 返回可复用 buffer**：保留前必须 `copy`，否则所有输出行指向同一末行。
3. **后处理只施加一次**（在 `compute()` 内），`Next()` 直接发射，勿重复 `ProcessRowHelper`。
4. **常量 leaf 必须 `Col:-1`**，否则会被当成列 `col0`。
5. **null 处理**：`arrowRecordToEncDatumRows` 必须对每个元素 `IsNull(i)` 置 `DNull`（left join 未命中、含 null 列）。
6. **聚合分组不可为空**：全局聚合要把所有行放进唯一组；规约结果取后勿重复 `Release`（`takeResultArray` 内部已 Release）。
7. **`mean` 输出类型**：INT/DECIMAL 输入→decimal128，FLOAT→float64；`aggOutputType` 与 `meanAgg.Finalize` 必须一致。
8. **Arrow 函数名**：`add/subtract/multiply/divide/negate`（v17）。
9. **聚合 post-process 不要融合比较/EXTRACT**：对聚合结果做 `(MIN(ts) = ...)` 或 `EXTRACT(EPOCH FROM MIN(ts))` 会被 planner 融进聚合的 PostProcessSpec，导致 flow setup 报「unsupported comparison operator: <string> = <timestamp>」/「unknown signature: extract(STRING, FLOAT8)」（聚合输出类型被误判）。对比断言改用下游 `CAST(agg AS <type>)`（如 `MIN(ts)::STRING`），与 decimal e2e 一致。
10. **`sql.arrow_filter.enabled` 全局开启会波及系统表扫描**：该开关全局生效时，`count-table-stream`（`stream_planner.go:208`）等内部扫描也会走 Arrow 统一底座，而系统表含 `JsonFamily` 列、`arrow_adapter.go:279` 的 `arrowTypeForKWType` 尚不支持 JSON，会报 `count-table-stream: unsupported type family JsonFamily for arrow unification`。现象：开启该开关后执行 `DROP TABLE` 的清理会失败（内部 row-count 扫描触雷）。规避：e2e 测试在 `DROP TABLE` 前先 `SET CLUSTER SETTING sql.arrow_filter.enabled = false`；或干脆不 DROP、靠停 server 清理（与 `TestArrowUnifyFilterAggJoin` 一致）。属广统一遗留限制，**非 §7.3 过滤函数改动引入**。
11. **join `onExpr` 的列索引落在「left++right 输入空间」**：`arrowFilterLeafFromExpr` 复用 §7.3 过滤逻辑时，原本用 `p.ResultTypes`（join 规划期的**输出**列）做类型/越界校验；但对 `onExpr`，其 `IndexedVar` 索引引用的是「left 输入列 ++ right 输入列」的输入空间，与 equi-join 合并后的 Arrow Record 列序（`col0..col{nL+nR-1}`）一致。若仍用 `p.ResultTypes`（输出列数更小）会越界失败、导致 `BuildArrowOnExprJSON` 返回空、退化走标准引擎。已改为直接用 `IndexedVar.ResolvedType()` 取列类型，并仅用 `indexVarMap`（即 `[0..nL+nR-1]`）做成员校验。若将来要支持 left/right/full 连接的非等值 `onExpr`，需注意 post-filter 语义与「匹配期求值 + 恒发 NULL 扩展行」不等价——不能简单套用 inner 的 post-filter，需在执行期区分「已匹配行」与「NULL 扩展行」后再过滤。

---

## 6. 当前未提交改动说明（git 现状）

- 已修改（tracked，未提交）：`arrowbatchconverter.go`/`file.go`/`record_batch.go`+其 test、`types_integration_test.go`、`colrpc/inbox.go`、`distsql_physical_planner.go`、`physicalplan/physical_plan.go`、`rowexec/processors.go`、`kwbase/vendor`（含未跟踪内容）、`kwdbts2/third_party`（未跟踪）。
- 未跟踪（新文件）：`docs/`、`pkg/sql/arrow_unification.go`、`arrowsmoke/`、`colexec/arrow_bridge*.go`、`rowexec/arrow_adapter.go`、`arrow_aggregate*.go`、`arrow_aggregator*.go`、`arrow_filter*.go`（`arrow_filter.go`/`arrow_filter_processor.go`/`arrow_filter_test.go`）、`arrow_join*.go`、`arrowpilot/`（部分）等。
- **尚未 commit**，也未 push。若换机器，需要把这些改动整体带走（git stash / 打包 / 分支 transfer）；注意 `vendor` 与 `kwdbts2/third_party` 含未跟踪大文件。

---

## 7. 下一步候选（按收益排序，挑一个继续）

1. ~~**补 `arrow_aggregate_test.go` 的编译错误 + 加 kernel 单测**（高优先、低风险）~~ ✅ **已完成**（2026-07-24）
   - 修掉 `ctx declared and not used`；新增 `TestAggregateArrayDecimal`（decimal 标量聚合）与 `TestArrowHashAggregatorDecimal`（DECIMAL 分组键聚合）等纯 kernel 单测；`buildRecord` 改为按实际列类型建 Record 以免 decimal 列触发 schema/record 类型不匹配 panic；顺手修掉 `arrow_aggregate.go:403` 的 `fmt.Errorf %s` 对 `aggOp`（int 类型）的 vet 告警。全部 PASS。
2. ~~**扩展聚合输入类型（TIMESTAMP）**~~ ✅ **已完成**（2026-07-24）
   - 加 `TIMESTAMP/TIMESTAMPTZ` 的 **MIN/MAX/COUNT**（含作为分组键）走 Arrow 引擎：编码（`arrow_adapter.go` 的 `arrowTypeForKWType`+`buildArrowColumns` 用 `arrow.FixedWidthTypes.Timestamp_us`）、kernel（`minMaxAgg` 复用 int64 累加；`arrowGroupKey`/`groupKey`/`arrayScalarAt`/`appendScalar`/`arrowRecordToEncDatumRows` 加 TIMESTAMP 分支）、planner（`canArrowAggregate` 允许 MIN/MAX/COUNT over timestamp、`arrowSupportedCompareType` 放行 timestamp 分组键）。
   - `SUM/AVG` 刻意仍仅数值（timestamp 做 sum/avg 无意义，planner 拒绝）。
   - 单测 `TestAggregateArrayTimestamp`/`TestArrowHashAggregatorTimestamp` + 端到端 `TestArrowUnifyTimestampAgg` 全 PASS。
   - ⚠️ 坑：把 `(MIN(ts) = ...)` 或 `EXTRACT(EPOCH FROM MIN(ts))` 这类**比较/函数**融合进聚合的 post-process 会导致 flow setup 失败（聚合输出类型被误判为 STRING）；e2e 改用下游 `CAST(ts AS STRING)` 比较（与 decimal e2e 一致）。UUID/JSON 仍未做，列为下一步。
3. ~~**扩展过滤函数**：`LIKE`、字符串函数、类型转换（`canArrowFilterExpr`），扩 `arrow_filter.go` 的递归树。~~ ✅ **已完成**（2026-07-24）
   - `LIKE`/`NOT LIKE`/`ILIKE`/`NOT ILIKE`：`tree.ComparisonExpr` 的 `Like/NotLike/ILike/NotILike` 运算符；Arrow compute v17 无 `match_like`，故在 `arrow_filter.go` 新增 `evalLike`（Go kernel，左操作数 string array、右操作数常量 string pattern，`likeMatch` 实现 `%`/`_`/`\` 转义 + ci）。planner：`canArrowFilterExpr` 要求左操作数 `StringFamily`、右操作数常量字符串；`buildArrowFilterNode` 映射为 `like/not_like/ilike/not_ilike`。
   - **类型转换 `CAST(col AS ...)`**：`tree.CastExpr` 接入 `arrowFilterLeafFromExpr`，产出 `arrowFilterCast`（tag STRING/INT/FLOAT）；`arrow_filter.go` 新增 `evalCast`/`castArrowArray`（numeric/boolean→string、string→numeric）；`ArrowArg.Cast`（`arrow_projection.go`）经 `leafToArrowArg` 下发。
   - **局限**：字符串函数（substring/length 等）Arrow 无对应 kernel、未做；pattern 仅支持常量（列 pattern 回退标准引擎）；非字符串左操作数的 LIKE 回退标准引擎（SQL 类型检查会先 `CAST` 成 string，故 `int LIKE '1%'` 实际走 `CAST(int AS STRING) LIKE '1%'`，已被覆盖）。
   - 单测 `TestLikeMatch`（纯 likeMatch 语义）+ 端到端 `TestArrowUnifyFilterFuncs`（LIKE/NOT LIKE/ILIKE/CAST+LIKE/CAST+EQ，并断言 `ArrowFilterRunCount` 递增）全 PASS。
4. ~~**连接非等值 `onExpr`**：`canArrowJoin` 拒绝 `onExpr != ""`，在 `arrowJoinProcessor` 增加 post-filter 阶段。~~ ✅ **已完成**（2026-07-24）
   - planner：inner 连接的 `onExpr` 经 `physicalplan.BuildArrowOnExprJSON` 复用 §7.3 的 `canArrowFilterExpr`/`buildArrowFilterNode` 构建成与 Arrow 过滤完全同构的 JSON 计划，经 `arrowJoinPlan.OnFilter`（`json:"on_filter"`）序列化下发给执行器；`canArrowJoin` 不再拒绝 `onExpr`。非 inner 连接（left/right/full）的 `onExpr` 仍走标准引擎（post-filter 语义与「匹配期求值 + 恒发 NULL 扩展行」不等价）。
   - 执行器：`arrow_join_processor.go` 在 equi-join 之后、回程解码之前，对合并后的 Arrow Record（列序 = left 列 ++ right 列，即 `col0..col{nL+nR-1}`）套用 `arrowFilterCore.eval` 做 post-filter；新增 `ArrowJoinOnFilterRunCount()` 探针。
   - 关键坑：join `onExpr` 的列索引落在「left++right 输入空间」，与合并 Record 列序一致；`arrowFilterLeafFromExpr` 原来用 `p.ResultTypes`（此时是 join **输出**列）做类型/越界校验，对 `onExpr` 会越界失败——已改为直接用 `IndexedVar.ResolvedType()`（列类型由表达式自身携带），不再依赖 `p.ResultTypes`。
   - 端到端 `TestArrowUnifyJoinNonEqui`：列-常量（`v>15`）、列-列（`v<w`）、AND 组合（`v>15 AND w<300`）、全排除（`v>100`）四类非等值 `onExpr`，结果对齐标准引擎并断言 `ArrowJoinOnFilterRunCount` 递增。全 PASS。
5. ~~**性能量化**：补完整 SQL 查询级 benchmark（已接 planner 深统一后，对比原生路径 QPS/延迟）。~~ ✅ **已完成**（2026-07-24）
   - 新增 `pkg/sql/rowexec/arrowpilot/bench_test.go`：每个算子一个 `BenchmarkArrowXxx`，内部跑 `Original`（默认，所有 arrow 开关关＝重构前的标准算子）与 `Arrow`（开启对应 `sql.arrow_*.enabled` 开关，并用 run-count 探针 `ArrowXxxRunCount` 轮询确认 planner 真的把查询路由到 Arrow 处理器后才计时）两个子基准，公平对比延迟/内存/分配；并额外 `fmt.Printf` 一行 `REFACTOR_COMPARE ... Original_per_Arrow_x=`（原算子/重构算子显式比例，>1 表示重构后 Arrow 更快）。
   - 数据（单节点 in-process 服务器，10k 行 `t(a INT,b INT)`/`jl`/`jr`，`-benchtime=20x`，`GOMAXPROCS=8`）。**结论：当前 Arrow 路径是「正确性优先的薄桥接」，比原生路径更慢、分配更多**——开销来自桥边界的逐行 `tree.Datum` 转换，而非 Arrow 计算本身（这正是 §7.6 深统一的动机）：

     | 算子 / 查询 | 原算子(Original) ns/op | 重构算子(Arrow) ns/op | 延迟比 | 内存比 | allocs 比 |
     |---|---|---|---|---|---|
     | Filter `SELECT a FROM t WHERE a*2 > b` | 7.04M | 10.02M | 1.42× | 6.3× | 3.3× |
     | Aggregator(全局) `SELECT SUM(a),COUNT(*),MIN(b),MAX(b) FROM t` | 4.95M | 5.69M | 1.15× | 6.0× | 6.9× |
     | Join(equi inner) `SELECT jl.v,jr.w FROM jl JOIN jr ON jl.k=jr.k` | 14.64M | 20.42M | 1.39× | 3.2× | 4.0× |
     | Projection `SELECT a+b FROM t` | 6.20M | 7.93M | 1.28× | 4.5× | 1.9× |

   - 注意：聚合目前仅**全局（无 GROUP BY）**形式稳定走 Arrow（`distsql_physical_planner.go:4737` 要求 `len(GroupCols)==0 || ResultRouters==1`）；GROUP BY 分布式聚合仍走 `setupMultiAggFinalState`（标准引擎），故聚合基准用全局聚合查询。
   - 运行：`go test ./pkg/sql/rowexec/arrowpilot/ -run '^$' -bench 'BenchmarkArrowFilter|BenchmarkArrowAggregator|BenchmarkArrowJoin|BenchmarkArrowProjectionQuery' -benchtime=20x -benchmem`（需 §1.1 的 `CGO_LDFLAGS`/`LD_LIBRARY_PATH`）。
   - 投影在 §7.6 经深统一后，与原生路径**几乎持平**（6.15M vs 6.10M ns/op，见 §7.6），上表 1.28× 为深统一前基线。
6. ~~**深统一下一步**：把某个算子的「逐行 `tree.Datum` 计算」改为真正批量 Arrow kernel（如投影已部分深统一），兑现性能收益。~~ ✅ **已完成（投影算子）**（2026-07-24）
   - 关键发现（用 §7.5 benchmark 定位）：投影/过滤/连接/聚合的执行器都是「行式包装器」——`compute()` 逐行 `p.input.Next()` + 每格 `EnsureDecoded`（堆分配 `tree.DInt`）把输入行物化成 Arrow，再跑 `compute.CallFunction`，再逐行物化回行。§7.5 测出的「Arrow 更慢、分配更多」**大头在算子的输入物化（逐行 Datum），不在 compute kernel**（kernel 已批量）。底部桥接 `buildArrowColumns` 因上游已预解码，`EnsureDecoded` 多为 no-op，故单纯优化桥接几乎无收益（已加 `GetInt` 快速路径作正向铺垫）。
   - 深统一落地（投影算子）：`arrowProjectionProcessor.compute` 拆分为 `computeInt`（全 INT 输入快速路径）+ `computeGeneric`（原路径，非 INT 退回）。`computeInt` 逐行用 `EncDatum.GetInt()` 把整数直接读入预分配 `[]int64`，再用 `array.NewInt64Builder.Reserve(n)` + `Append` 一次性构建输入 Arrow Record——**零逐行 Datum 分配**。顶部桥接 `arrowRecordToEncDatumRows` 仍须产出结果行（固有开销，与原生相当）。
   - 结果（10k 行，单节点，`-benchtime=100x`，有运行间噪声）：

     | 算子 / 查询 | 原算子(Original) ns/op | 重构算子(Arrow) ns/op | 延迟比 | allocs 比 |
     |---|---|---|---|---|
     | Projection `SELECT a+b FROM t` | 6.10M | 6.15M | **~1.0×（持平）** | 1.4× |

     投影从 §7.5 的 1.28× 更慢变为**与原生几乎持平**（早先 20x 跑批曾 8.5M vs 9.4M 反超原生）。延迟已兑现深统一收益；剩余 ~1.4× 分配来自顶部桥接逐行建 `EncDatumRow`+`tree.DInt`（结果行固有开销，非 Arrow 计算本身）。
   - 扩展方向（候选后续）：① 把同一「零 Datum 输入物化」模式套用到 filter/join/agg 的 `compute`（共用行式物化结构）；② 顶部桥接也改为批量构造 `EncDatumRow`；③ 更彻底——算子间直接链式传 `arrow.Record`（各算子作 `UnifiedProcessor`）+ scan 原生出 Arrow（架构文档阶段2），可消除全部桥接开销，是大改，需单独排期。
   - 验证：`arrowpilot` 全部 e2e 测试 PASS（`TestArrowProjectionPlannerIntegration` 等覆盖 `a+b`/`a*2`/`a-b`/`a*3+1`）；`BenchmarkArrowProjectionQuery` 复测确认持平。

7. ~~**扩展深统一到 filter/join/agg**：把投影的「零 Datum 输入物化」模式套用到另三个算子。~~ ✅ **已完成**（2026-07-24）
   - 落地方式（DRY）：抽出共享 helper `newArrowInputSource(alloc, input, da) (UnifiedProcessor, error)`（`arrow_filter_processor.go`，与 `drainInput` 同文件）。它对**全 INT 输入**走零 Datum 快速路径——`EncDatum.GetInt()` 直接读入预分配 `[]int64`，用 `array.NewInt64Builder.Reserve(n)`+`Append` 一次性构建输入 Arrow Record（`NewArrowRecordSource` 包装），**零逐行 Datum 分配**；非 INT 输入退回原 `drainInput`+`NewRowToArrowConverter`。三个算子的 `compute` 统一改用它（filter `arrow_filter_processor.go`、agg `arrow_aggregator_processor.go`、join `arrow_join_processor.go` 的左/右两路各一个），移除各自重复的 `drainInput`+`inPtrs`+`NewRowToArrowConverter` 样板。
   - 两个边界守卫（均修复了实测 panic）：① `n == 0`（输入有列但 0 行）时构建 `nIn` 个空 Int64 列，使 record 列数与 schema 一致；② `nIn == 0`（COUNT(*)/COUNT_ROWS 全局聚合，0 输入列）时直接返回 0 列空 record——核心对 `count_all` 不取列、直接数行，原 `vals[0]` 越界 panic 由此修复。
   - 结果（10k 行，单节点，`-benchtime=100x`，有运行间噪声，箭头为 §7.5 深统一前基线）：

     | 算子 / 查询 | 原算子(Original) ns/op | 重构算子(Arrow) ns/op | 延迟比 | 备注 |
     |---|---|---|---|---|
     | Projection `a+b` | ~6–7.6M | ~6–7.7M | **~1.0× 持平** | §7.6 已深统一 |
     | Filter `a*2>b` | 6.78M | 7.64M | 1.13×（原 1.42×） | 改善 |
     | Aggregator(全局INT) `SUM/COUNT/MIN/MAX(a,b)` | 4.30M | 3.87M | **0.90×（Arrow 更快）** | allocs 794 vs 1729（少 2.2×） |
     | Join(equi INT) `jl.k=jr.k` | 13.73M | 16.99M | 1.24×（原 1.39×） | 改善；allocs 仍高（顶部桥接） |

     四个算子全部向原生看齐：聚合器已反超原生，filter/join 显著拉近。剩余延迟/分配大头在**顶部桥接 `arrowRecordToEncDatumRows`**（逐行建 `EncDatumRow`+`tree.DInt`，结果行固有开销，与原生相当），非计算本身。
   - 验证：`arrowpilot` 全部 e2e 测试 PASS（含 `TestArrowUnifyDecimalAgg` 等 decimal/timestamp 路径，走非 INT 退回路径）；`BenchmarkArrowFilter/Aggregator/Join/ProjectionQuery` 复测确认收益。

7. ~~**顶部桥接批量化（§7.7）**：把 `arrowRecordToEncDatumRows` 的逐行/逐值分配改为批量。~~ ✅ **已完成**（2026-07-27）
   - 两个热点与修复（均在 `arrow_projection_processor.go` 的 `arrowRecordToEncDatumRows`）：
     - **A. 行切片合并**：原本 `rows[i] = make(EncDatumRow, len(typs))` 每行一次堆分配（n 次）；改为先 `make([]EncDatum, n*len(typs))` 一块连续 `flat` 缓冲区，再 `rows[i] = flat[i*len:(i+1)*len]` 切片切出（仅 1 次分配）。
     - **B. 值 Datum 批量预分配**：INT/FLOAT/BOOL 列原本每格 `v := tree.DXxx(...); &v` 各自逃逸到堆（n×cols 次分配）；改为每列 `make([]tree.DXxx, n)` **一个切片**，循环里 `vals[i] = ...; rows[i][ci] = EncDatum{Datum: &vals[i]}` 引用切片内元素（每列仅 1 次分配）。同时用 `arr.Int64Values()`/`arr.Float64Values()` 直读底层数组，省掉逐值 `Value(i)` 方法开销。String/Binary/Decimal/Timestamp 仍按值拷贝（其 Datum 含堆内 big.Int / 拷贝语义，批量收益有限，保持原样）。
   - 正确性：返回 `rows` 被下游逐行 `ProcessRow` 后拷贝进 `cp`，`flat` 与每列 `vals` 通过 `&vals[i]` 指针可达，不会提前 GC；`string(b)` 转换本就拷贝字节，record 释放安全（与改动前一致）。
   - 结果（10k 行，单节点，`-benchtime=100x`，同次运行内 原算子(Original) vs 重构算子(Arrow) 对比；对比 §7.6 基线）：

     | 算子 / 查询 | 原算子(Original) ns/op | 重构算子(Arrow) ns/op | 延迟比 | allocs (O / A) | 对比 §7.6 |
     |---|---|---|---|---|---|
     | Filter `a*2>b` | 6.99M | 7.53M | **1.08×** | 21938 / 20940 | 1.13× → 1.08× |
     | Aggregator(全局INT) | 4.49M | 4.05M | **0.90×** | 1734 / 813 | 持平（输出行数少，桥接占比小） |
     | Join(equi INT) | 14.15M | 16.65M | **1.18×** | 43686 / 100970 | 1.24× → 1.18×（allocs 从 151k 降） |
     | Projection `a+b` | 5.99M | 5.79M | **0.97×（Arrow 更快）** | 22207 / 11204 | 1.0× → 0.97× |

     顶部桥接批量后：filter/join 进一步向原生靠拢，projection/agg 反超原生，arrow 分配数普遍 ≤ 原生。四个算子均已达/超原生水平。
   - 验证：`arrowpilot` 全部 e2e 测试 PASS（`ok ... 21.796s`）；`BenchmarkArrowFilter/Aggregator/Join/ProjectionQuery` 复测确认收益。

8. ~~**算子间 Arrow buffer 链式传递（§7.8 数据结构替换）**：消除「每个算子入口各自把行输入桥接成 Arrow」的重复开销，让相邻 Arrow 算子之间直接以 `arrow.Record` 传递。~~ ✅ **已完成**（2026-07-27）
   - 接口层本已统一：`UnifiedProcessor.Next() (arrow.Record, ...)`，四个算子计算核都吃/吐 `arrow.Record`。但执行接线仍是每个顶层算子从遗留 `execinfra.RowSource`（行式 `EncDatumRows`）入口各自 `newArrowInputSource` 桥接，**算子与算子之间仍是行式传递**。本步把「桥接」上提到相邻 Arrow 算子之间，完成「初步数据结构替换」（先于内部算法优化）。
   - 落地：新增 `ArrowRecordEmitter` 接口（`ArrowOutput() arrow.Record`）+ `unifiedInputFrom(alloc, src, da)` helper（`arrow_filter_processor.go`）。每个 Arrow 算子在 `compute` 中把算好的 `rec` 存进 `p.outputRec`（**不再 `defer rec.Release()`**），并新增 `ArrowOutput()` 方法（惰性 `Retain` 后把所有权交给调用方）；`ConsumerClosed` 统一 `Release`。四个算子的 `compute` 输入由 `newArrowInputSource` 改为 `unifiedInputFrom`——当上游 `src` 实现 `ArrowRecordEmitter` 且产出非空 record 时，`unifiedInputFrom` 直接把上游的 `ArrowOutput()` 包成 `NewArrowRecordSource` 喂下游（**零行式 round-trip**）；否则退回 `newArrowInputSource`（scan 桥接 / 遗留 RowSource）。
   - 引用计数方案（已逐核核对安全）：上游 `compute` 置 `outputRec`（refcount 1，不 Release）→ `ArrowOutput()` 时 `Retain`（→2，交下游）；下游 core 消费该 record 后会 `rec.Release()`（filter `arrow_filter.go:87`、agg `arrow_aggregator.go:72`、join `readAll` 逐 rec Release + `Next` Release 合并结果、proj `arrow_projection.go:91` 均已 Release 输入 record）→ refcount 2→1；上游 `ConsumerClosed` 再 `Release` → 0。恰好 2 次 Release 对 refcount 2，无泄漏、无双释放。**附带修复**：`arrow_join_processor.go` 原 `defer rec.Release()` + `onFilter` 路径显式 `rec.Release()` 造成上游 record 双释放隐患，本步改为统一由 `outputRec`+`ConsumerClosed` 释放。
   - 行为保持：当前单算子计划（上游为 scan/遗留算子）一律走 `newArrowInputSource` 回退，与原行为完全一致；**当 planner 在单 flow 内连续启用多个 Arrow 算子时（如 `scan→filter→aggregate`），相邻算子自动以 `arrow.Record` 链式传递**，无需 planner 改动 —— 接口已就绪，接线自动激活。
   - 已知留口（后续）：投影 `computeInt` 全 INT 快速路径曾手动 `p.input.Next()` 逐行 drain 输入，导致 `filter→projection(全INT)` 这一跳走行式、未链式输入；现**已闭环**（2026-07-27）：`computeInt` 改走 `unifiedInputFrom` + `arr.Int64Values()` 直读输入列的连续 `int64` 缓冲（列按索引取，不依赖 field 名），`filter→projection(全INT)` 的输入侧也实现零行式 round-trip 链式传递，仅保留**输出**链式不变。其余算子（filter/join/agg）输入早已链式。至此 §7.8 的「算子间双向（入+出）Arrow 链式」已全部点亮。
   - 验证：`go vet ./pkg/sql/rowexec/` 干净；`arrowpilot` 全部 e2e 测试 PASS（`ok ... 21.186s`），相邻 Arrow 算子链式传递无 panic / 泄漏 / 结果偏差。

9. **原生 Arrow scan（§7.9 去除首节点桥接）**：**已完成**（2026-07-27）
   - 背景：首节点（scan/遗留 RowSource）喂给首个 Arrow 算子时，旧 `newArrowInputSource` 是「首节点桥接」——它把整张输入先物化成行缓冲（INT 走 `vals [][]int64` / `nulls [][]bool` 二维中间缓冲；其余类型走 `drainInput` 收集全部 `EncDatumRows`），再二次拷贝成 Arrow 列。属于「先全量行式物化、再列式转换」的冗余开销。
   - 落地：新增 `arrowScan`（`arrow_adapter.go`），实现 `UnifiedProcessor`。它读 `input.Next()` 时**逐行直接 append 进各列的 Arrow builder**（INT 走 `EncDatum.GetInt` 零 Datum 物化；其余类型 `EnsureDecoded` 后按类型 decode 一次），**不保留任何整行中间缓冲**，最后一次性 `NewRecord`。配套复用 `arrowTypeForKWType` + 新增 `newArrowBuilder` / `appendEncDatum`（decode 逻辑与 `buildArrowColumns` 严格一致，仅改为逐行 append）。
   - 接线：原 `unifiedInputFrom` 的回退分支 `newArrowInputSource` 改为 `newArrowScan`，并**删除** `newArrowInputSource` 与 `drainInput`（桥接函数整体下线）。首个 Arrow 算子现在拿到的是 scan 直接产出的原生 Arrow Record——首节点桥接消除，scan→Arrow 不再有「全量行缓冲 + 二次拷贝」两道开销。引用计数与原 `NewArrowRecordSource` 路径一致（record refcount 1，交给算子后由算子恰好 Release 一次）。
   - 行为保持：单算子 / 混合计划与原行为完全一致（下游仍走 `unifiedInputFrom`；链式 Arrow 算子仍走 `ArrowRecordEmitter` 短路）。空输入与 0 列（COUNT(*)）按 schema 产出对应类型空列 / 0 列 record，与旧路径语义对齐。
   - 验证：`go vet ./pkg/sql/rowexec/` 干净；`arrowpilot` 全部 e2e 测试 PASS（`ok ... 41.351s`，含 int / float / decimal scan→filter/agg/join/projection 全链路），无 panic / 泄漏 / 结果偏差。

10. **算子内部算法优化（§7.10 向量化核 + 融合 colexec）**：**已完成**（2026-07-27）
    - 背景：前序 (a)(b)(§7.8/§7.9) 已把「数据流动」全链路 Arrow 化、去掉所有桥接与行式 round-trip，但**聚合计算核仍是逐行 Go 循环**（`sumAgg.Consume` 逐行 `IsNull` 分支 + 每 (group,agg) 一次 `compute.take` 拷贝成独立数组再喂核）。这正是性能未提升的残根——数据流快了，但到计算核又退回行式。colexec 本身是**列式**引擎（把选择子 `sel []int32` 直接喂进连续列缓冲的累加器），从不逐行。本步对齐 colexec 思想。
    - 三项优化（`arrow_aggregate.go`）：
      1. **向量化核**：`sumAgg.Consume` / `minMaxAgg.Consume` / `meanAgg.Consume` 改为直接遍历 `arr.Int64Values()` / `Float64Values()` 的**连续底层缓冲**（INT/float 走 `[]int64`/`[]float64` 直读），单处 `arr.NullN()>0` 预判代替逐行 `IsNull` 分支；Decimal/Boolean/Timestamp 保持按值读（无连续缓冲 API）但同样经选择子喂入。
      2. **去 `take` 拷贝**：`arrowHashAggregator.feedGroup` 不再为每个 (group,agg) 做 `compute.take` 分配+拷贝，而是把**选择子切片 `[]int32` 直接喂给核**（如 `agg.Consume(valCol, idxs)`），与 colexec 选择子喂入完全同构；每 (group,agg) 的临时数组分配归零。
      3. **colexec 式 uint64 哈希分桶**：`groupBy` 由逐行构造 string `arrowGroupKey` 改为 colexec 式 `hash := arrowGroupHash(...)` 的 uint64 分桶（`arrowGroupHash` 用 `maphash` 折叠类型标签+值字节、**零分配**，每输入行一次而非每行一次 string 键）。桶内用 `arrowGroupRowEqual` **按值碰撞回退**（hash 相同才逐值比对），string 键只在每个**不同**组出现一次（喂 `states` map），不每行构造。
    - **融合 colexec 的对比验证**（`arrow_aggregate_colexec_bench_test.go`，外部包 `package rowexec_test` 以避开 import 环 `colexec→rowexec`）：
      - 正确性对照 `TestArrowAggMatchesColexec`：同一份随机 int 数据（cases `{1000,10,0}`/`{10000,7,0.1}`/`{8000,500,0}`）分别走 Arrow 聚合（`NewArrowRecordSource`+`NewArrowAggregator`）与 `colexec.NewHashAggregator`，比较**全局 SUM**（colexec 输出不含 group key，仅含聚合列，故比全局 SUM 干净隔离 SUM 算法）。三组全部相等 → PASS。
      - 性能基准 `BenchmarkArrowVsColexecSumGrouped`：每轮内**新建两条独立 record**（Arrow / colexec 各一条，避免共享引用被释放后读野指针）；colexec 侧 `RecordToBatch` 转换**预转换一次**仅计时 aggregator 算法本身，与 Arrow 侧公平对比。结果（10k 行，`-benchtime=10x`）：**Arrow ≈250µs vs colexec ≈486µs，Arrow 约 1.9× 快于 colexec**（多次运行区间 1.26×–1.94×，结论稳定：Arrow 聚合核不慢于 colexec，通常更快）。
    - **修复回归**：`arrowHashAggregator.Consume` 分组遍历 `map[uint64][]int32` 的桶顺序**不确定**，导致 `order`（首见组顺序）偶发错乱（`TestArrowHashAggregatorTimestamp` 时好时坏，依赖 `maphash.MakeSeed()` 进程级随机）。在 `Finalize` 中按各组首见行号 `groupRow[key]` 对 `order` 做 `sort.SliceStable` 排序，恢复「按行首见顺序」确定性语义（纯输出顺序修复，不影响聚合值——每组 state 按组键累加、可交换）。修复后 `go test -run 'Arrow|Aggregat'` **连续 20/20 PASS**（此前偶发 FAIL），`go vet` 干净。
    - 验证总览：`go vet ./pkg/sql/rowexec/` 干净；`TestArrowAggMatchesColexec` PASS；`go test -run 'Arrow|Aggregat' ./pkg/sql/rowexec/` 20 连跑全绿；`arrowpilot` e2e 全链路 PASS。

11. **JOIN 计算核向量化（§7.11 colexec 式哈希分桶）**：**已完成**（2026-07-29）
    - 背景：§7.10 完成聚合核向量化后，四个 opt-in 算子里仅 **JOIN 仍是逐行**——`arrow_join.go` 的 `recordKey` 每行用 `strings.Builder`+`fmt.Fprintf` 构造字符串组合键，`build`/`probe` 都逐行查 `map[string][]int32`。这正是 §7.10 聚合里替换掉的同一种「逐行字符串键」模式（colexec 本身非逐行）。
    - 优化（`arrow_join.go` `arrowJoinCore.eval` + 新 helper `joinKeyArrays`/`joinHasNull`/`joinRowHash`/`joinRowsEqual`/`arrValEqual`）：
      1. **colexec 式 uint64 哈希分桶**：`joinRowHash`（复用聚合 `arrowGroupHash` 的字节折叠思路，零分配、按类型标签+值字节）替代逐行 string 构造；build 阶段把右表键按哈希分桶，probe 阶段左表键算哈希后只在同桶内比对，碰撞由 `joinRowsEqual`（按值比较，NULL 不参与）回退——与 §7.10 聚合分组同构。
      2. **稳定性修复**：哈希种子在 build+probe **前设一次**（`seed.SetSeed(maphash.MakeSeed())`），`joinRowHash` 内用 `seed.Reset()` 保留种子（不能每次 `MakeSeed` 随机，否则左右同键值哈希不一致 → 零匹配）。
      3. **空侧守卫**：仅当该侧 `nL>0`/`nR>0` 才 `joinKeyArrays` 解析键列。原因：planner 把非等值谓词下推成 arrow filter 后，filter 输出 **0 行且 0 列**的退化 record；baseline 的 `recordHasNull`/`recordKey` 写在 `for r<nR` 循环体内（nR==0 不执行故不触碰），而 eager 解析会 `arrowOperandColumn` 越界 panic。空侧本就不可能产生匹配对，跳过键解析正确（outer join 的 unmatched 行由 Phase 2/3 用各自有列 record 正常发出）。
    - 正确性/性能：inner/left/right/full + 非等值 post-filter 全链路经 `arrowpilot` e2e 验证 PASS（41.3s，与 baseline 一致）；`go vet ./pkg/sql/rowexec/` 干净；`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` PASS。
    - **colexec 对照（本步追加）**：仿 §7.10 导出 `colexec.NewHashJoiner`（包装 `makeHashJoinerSpec`+`newHashJoiner`），新增 `arrow_join_colexec_bench_test.go`（外部包 `rowexec_test`，复用 §7.10 的 `oneShotBatchSource`）。`TestArrowJoinMatchesColexec` 用「输出按整行排序后做多重集相等」对照：覆盖 **inner/left/right/full 四种类型 × 多对多 fan-out / NULL 键**两个数据集，逐一验证 arrow join 与 colexec 原生向量化 hash joiner 输出完全一致；`BenchmarkArrowVsColexecJoin` 在 1024 行、64 键（~16× 扇出）数据集上对比——**Arrow 9.82 ms/op vs Colexec 12.5 ms/op，Arrow 约 1.27× 快**（arrow join 输出走 `gatherColumn` 零拷贝构造，且无 colexec hash-table 跨 batch build 开销）。
    - **顺带修复的真实缺陷**：对照测试暴露 outer join 对 **NULL 键行**的错误——`gatherColumn`/`appendValueAt` 只处理「对侧未命中填 NULL（idx=-1）」，未处理「源行自身为 NULL」，对 NULL 键行直接取值（arrow 返回零值）导致 key 列输出 `0` 而非 `NULL`。已在 `appendValueAt` 取值前加 `src.IsNull(idx)` 检查（并补全 `TIMESTAMP`/`DECIMAL128` 分支），修复后 left/right/full 的 NULL 键行与 colexec 完全一致。
    - 验证：`TestArrowJoinMatchesColexec` PASS；`go vet ./pkg/sql/rowexec/` 干净；`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` PASS（51s，含 arrowpilot e2e）。filter/projection 核已走 arrow compute（原生向量化），暂无需改。

---

## 8. 快速恢复 checklist（新机器 / 新会话）

> ⚠️ **入库状态（2026-07-29 更新）**：Arrow 统一化源码 + 文档**已提交并推送到 `kwdb-exec` 远端分支 `arrow-unify`**（commit `491776a8`，已 `git ls-remote` 核实）。但 `vendor` 是**子模块**（上游 `kw-vendor.git`），里面的 Arrow v17 升级 + aggregate 补丁只提交在**本地子模块**（`26d879c`），**尚未推到 `kw-vendor.git`**——换机后 `git submodule update` 会因找不到 `26d879c` 失败。详见 §8.1 的两种解决路径。**另外 `.codebuddy/`、`kwdbts2/third_party`（C++ 第三方库）、`mod-doc/` 被刻意排除在提交之外**，需单独 tar 携带。

### 8.1 把改动搬到新服务器（git 状态 + 搬运）

- **当前 git 状态**：
  - 主仓库 `arrow-unify` 分支已 push 到 `kwdb-exec`（`https://gitee.com/kaiwuDB/kwdb-exec`，SSH：`git@gitee.com:kaiwuDB/kwdb-exec.git`）。包含：所有 Arrow 统一化 Go 源码（`pkg/sql/...`、`pkg/col/colserde/...`）+ `docs/` 交接文档。**未含** `.codebuddy/`、`kwdbts2/third_party`、`mod-doc/`（本地数据 / C++ 第三方库，需另行 tar）。
  - `vendor` 子模块：`491776a8` 记录其指针为 `26d879c`（本地子模块提交，含 apache/arrow v17 升级 + `arrow/compute/aggregate` 补丁 + 两个 shim `JohnCGriffin/overflow`、`zeebo/xxh3`）。该 `26d879c` **仅在本地**，未 push 到 `kw-vendor.git`。
- **vendor 子模块如何解决（二选一，否则新机无法构建）**：
  1. **推送到 kw-vendor.git**：在 `vendor/` 子模块内 `git push <kw-vendor 远端> <当前分支>`（本机已提交 `26d879c`）。这样 `kwdb-exec` 的子模块指针 `26d879c` 在 `kw-vendor.git` 可达，`git submodule update` 即通。**注意**：这会把整个 Arrow v0→v17 升级推到共享 vendor 仓库，属对共享依赖的改动，需确认你对该仓库有推送权且团队认可（本会话未擅自推送 kw-vendor）。
  2. **tar 携带补丁（不碰 kw-vendor）**：在新机 clone `kwdb-exec` 后，把本机 `vendor/github.com/apache/arrow/go/v17` 整个目录 + `vendor/github.com/JohnCGriffin/overflow` + `vendor/github.com/zeebo/xxh3` 打包，解压覆盖到新机 `vendor/` 对应位置，再 `git -C vendor checkout 26d879c`（或直接在子模块内 `git commit` 同样内容）。这避免改动共享 vendor 仓库。
- **C++ 引擎与第三方库（必须单独带）**：`libkwdbts2.so`（151MB，构建产物，不在仓库）与 `kwdbts2/third_party`（C++ 第三方依赖，子模块，`?` 未跟踪）不在 git 内。换机需 `tar` 这两部分，或在新机从 `kwdbts2/` 源码重新构建（重度 C++ 步骤）。
- **新机拉取命令**：`git clone -b arrow-unify git@gitee.com:kaiwuDB/kwdb-exec.git && cd kwbase && git submodule update --init vendor`（路径按 §8.2 调整；若走路径②则覆盖 v17 后再 checkout `26d879c`）。

### 8.2 新服务器最低恢复项

- [ ] 仓库在 `gitee.com/kwbasedb/kwbase`（软链到 github.com 同款），`GO111MODULE=off`。**所有绝对路径 `/home/sdy/go/...` 都要改成新机的 GOPATH / 源码路径**（见 §1.1，路径是服务器相关的，不要照抄本机路径）。
- [ ] **C++ 引擎库 `libkwdbts2.so`（151 MB）必须预先构建就位**——这是 cgo 测试的硬前提，缺它 `go test` 直接链接失败。从 `kwdbts2/` 源码构建（这是单独的重度 C++ 构建步骤，不在 Go 流程内），产物放到新机对应 `build/lib/`。§1.1 的 `CGO_LDFLAGS`/`LD_LIBRARY_PATH` 路径随之调整。
- [ ] `vendor/.../apache/arrow/go/v17`（含我们的补丁）+ 两个 shim 就位（见 §8.1）。
- [ ] 先 `go build ./pkg/sql/...` 确认编译干净；再跑 `TestArrowUnifyFilterAggJoin|TestArrowUnifyDecimalAgg|TestArrowUnifyTimestampAgg|TestArrowUnifyFilterFuncs` 确认全绿（`go test ./pkg/sql/rowexec/ -run TestLikeMatch` 作纯函数快验）。
- [ ] 读 `docs/arrow-unification-architecture.md` §11–§12 拿完整上下文，再从 §7 / §9 挑任务继续。
- [ ] **换机后首跑若报 apd/opentracing 模块路径冲突**（`vendor` 里 arrow/go 与 kaiwu 前缀混用），执行 `go clean -cache` 清掉旧机器残留的构建缓存再编译（这是 GOPATH 模式跨机迁移的已知坑）。

---

## 9. 当前总待办 / 后续候选（NEXT）

> §7.1–§7.11 全部 ✅（含 JOIN colexec 对照）。下面按收益/风险排序，挑一个继续；**第一项（git 入库）建议优先于任何新开发**，否则换机容易丢改动。

1. **[最高优先] 把工作区改动入库**：按 §8.1 建 `arrow-unify` 分支 commit + push（或 tar 打包带走）。**这是切换服务器的前置动作**，当前所有成果都还在未提交工作区。
2. **[可选] 向量化扩展到更多核**：当前 §7.10 已向量化的聚合核是 sum/minMax/mean（INT/FLOAT 走连续缓冲直读；DECIMAL/BOOL/TIMESTAMP 仍按值读但经选择子喂入）。可继续把 filter/projection/join 的**计算核**也改为选择子驱动的列式循环（colexec 式），进一步消除逐行 `Value(i)` 调用；以及把 mean 的 sum/count 累加也走 `Int64Values()` 连续缓冲。**本步已收尾**：① filter.eval 改用原生 `compute.Filter` 核替代「逐行收集 indices + take」，彻底消除每行的 `IsNull/Value` 调用；② mean 的 DECIMAL 分支也从 `a.Value(i)` 改为 `a.Values()` 连续缓冲（与 sum/minMax 风格统一）；filter 的 `like`/CAST 因 arrow 无对应 kernel 仍保留行式 Go 实现。
3. **[可选] 把 colexec 对照扩展到分组态**：§7.10 的正确性/性能对照目前只比了**全局 SUM**（因 colexec `NewHashAggregator` 输出不含 group key，仅含聚合列）。若要对照分组聚合，需要让 colexec 也产出 group key，或在对比层做「按组 SUM 字典」对齐——属增强验证，不影响当前结论。**本步已完成（§9.4）**：新增 `TestArrowGroupedAggMatchesColexec`（分组 SUM 正确性，arrow 精确 (group→sum) map vs Go 真值字典；colexec 因不输出 group key 走「排序 multiset vs 真值」对照，二者对齐即证明 arrow==colexec 分组一致）+ `BenchmarkArrowVsColexecGrouped`。**关键结论**：分组态下 arrow（~1005µs/op）反而比 colexec（~553µs/op）慢约 0.55×，与全局 SUM 时 arrow 1.9× 快形成对比——arrow 的 uint64 哈希分桶 + 每 distinct 组字符串 key 物化 + map 查找 + Finalize 排序开销在分组场景压过了列存直读优势。这提示 §9.4（分布式分组聚合路由 Arrow）需先做分组路径的微优化（减少 string key 物化 / 用更紧凑的组容器）再上生产。
4. **[可选] 接 planner 让聚合真正走分组 Arrow 路径**：§7.5 提到分布式 GROUP BY 仍走 `setupMultiAggFinalState`（标准引擎），当前 Arrow 聚合在 `distsql_physical_planner.go:4737` 仅全局聚合稳定走 Arrow。可扩 planner 让分组聚合也路由 Arrow（需确认 refcount / 流式 finalize 语义）。
5. **[可选] 补更多类型/函数**：UUID/JSON 分组键、字符串函数（substring/length，Arrow 无 kernel 需自补）、非字符串左操作数的 LIKE；left/right/full 连接的非等值 `onExpr`（post-filter 语义不等价，需在执行期区分已匹配行与 NULL 扩展行）。

### 9.1 本次会话（2026-07-27）收尾要点回顾

- **完成**：§7.10 聚合计算核向量化 + 去 `take` 拷贝 + colexec 式 uint64 哈希分桶；新增 `arrow_aggregate_colexec_bench_test.go`（外部包 `rowexec_test` 避开 `colexec→rowexec` 导入环），含 `TestArrowAggMatchesColexec`（正确性对照 PASS）与 `BenchmarkArrowVsColexecSumGrouped`（公平基准：Arrow ≈250µs vs colexec ≈486µs，约 1.9× 快）。
- **修复回归**：`arrowHashAggregator.Consume` 遍历 `map[uint64][]int32` 桶顺序不确定 → `Finalize` 加 `sort.SliceStable(h.order)` 按首见行号排序，恢复确定性组顺序。`go test -run 'Arrow|Aggregat' ./pkg/sql/rowexec/` **连续 20/20 PASS**，`go vet` 干净。
- **验证**：`arrowpilot` e2e 全链路 PASS；`go vet ./pkg/sql/rowexec/` 干净。
- **未提交**：全部改动仍在 `master` 工作区（见 §8.1）。

### 9.2 本次会话（2026-07-29）收尾要点回顾

- **完成**：§7.11 JOIN 计算核 colexec 对照——导出 `colexec.NewHashJoiner`，新增 `arrow_join_colexec_bench_test.go`（`TestArrowJoinMatchesColexec` 正确性对照 + `BenchmarkArrowVsColexecJoin` 性能基准）。四种 join 类型 × 多对多/NULL 键数据集证实 arrow join 与 colexec 原生 hash joiner 输出**完全一致**；Arrow 9.82 ms/op vs Colexec 12.5 ms/op（约 1.27× 快）。
- **修复真实缺陷**：对照暴露 outer join 对 NULL 键行把 key 列输出成 `0` 而非 `NULL`——`appendValueAt` 补齐 `src.IsNull(idx)` 检查 + `TIMESTAMP`/`DECIMAL128` 分支。
- **验证**：`TestArrowJoinMatchesColexec` PASS；`go vet ./pkg/sql/rowexec/` 干净；`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` PASS（51s，含 arrowpilot e2e）。
- **状态**：改动含 `hashjoiner.go`(新增导出 `NewHashJoiner`)、`arrow_join.go`(NULL 修复)、`arrow_join_colexec_bench_test.go`(新增)。**待入库**（见 §8.1 / §9 第 1 项）。

### 9.3 本次会话（2026-07-29 第二波）收尾要点回顾

- **完成（§9.2 项收尾）**：把 filter/projection/join 计算核进一步列式化、把 mean 的 DECIMAL 累加也走连续缓冲。
  - `arrow_filter.go` 的 `eval`：原「逐行 `for i { mask.IsNull/Value }` 收集 indices → 每列 `compute.take`」改为直接用原生 `compute.Filter(col, mask)` 核一次性按布尔掩码选择所有列（默认 `NullSelectionBehavior=DROP`，与原 `mask.IsNull(i) || !mask.Value(i)` 语义完全一致），彻底消除逐行循环。
  - `arrow_aggregate.go` 的 `meanAgg.Consume` 的 `*array.Decimal128` 分支：从 `a.Value(i)` 改为 `a.Values()` 连续切片直读（与 sum/minMax 的 `Int64Values()/Float64Values()` 风格统一）。
- **验证**：`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` PASS（51s，含 arrowpilot e2e）；编译干净。
- **状态**：改动含 `arrow_filter.go`、`arrow_aggregate.go`。**待入库**（见 §8.1 / §9 第 1 项）。

### 9.4 本次会话（2026-07-29 第三波）收尾要点回顾 —— colexec 对照扩展到分组态

- **完成（§9 NEXT 第 3 项）**：把 colexec 对照从「全局 SUM」扩展到「分组 SUM」的正确性 + 性能对照。
  - 新增 `TestArrowGroupedAggMatchesColexec`：用 Go 从原始切片算**真值分组 SUM 字典** `map[int64]int64`，两端各自对照——arrow 走 `GroupCols:["col0"]` 精确 `(group_key→sum)` map 对比（同时验证 arrow 的 group key 输出正确）；colexec 因 `hashAggregator` 不输出 group key，走「排序 multiset（值序列 + NULL 组计数）vs 真值」对照。两端都对齐真值 ⇒ 证明 arrow==colexec 的分组聚合等价。用例含 4 组（含 500 组高基数、20% NULL）。
  - 新增 `BenchmarkArrowVsColexecGrouped`：分组 SUM(int64) 同数据同列存对照。
- **关键性能结论（初测）**：分组态下 **arrow ≈1005µs/op 反而比 colexec ≈553µs/op 慢约 0.55×**。原因初判为 arrow 的 colexec 式 uint64 哈希分桶 + 每 distinct 组物化字符串 key（`arrowGroupKey`）+ `map[string]` 查找。后经 §9.5 剖析，**真正瓶颈不是 `map[string]`**，而是「每分组每 1 行调用一次 scalarAgg.Consume（逐行 IsNull/Value 访问器）」——colexec 是把整段 selection vector 一次性喂入。详见 §9.5。
- **验证**：`TestArrowGroupedAggMatchesColexec`、`TestArrowAggMatchesColexec` PASS；`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` 全绿（51s 含 arrowpilot e2e）；`go vet` 干净。
- **状态**：改动仅含 `arrow_aggregate_colexec_bench_test.go`（测试/基准，不影响生产代码）。**待入库**。

### 9.5 本次会话（2026-07-29 第四波）收尾要点回顾 —— 分组核重写（Arrow 原生式 Grouper）

- **背景**：用户指出"Go 版 arrow 若无对应实现，可参考 C++ 版本在 kwbase 重写添加"。核查结论：vendor 的 Arrow **Go v17（乃至上游 `main`）都没有原生分组/Grouper 实现**——仅全局标量聚合 + `compute.Unique`，`FuncHashAgg` 只是占位枚举；handoff §8 提到的 `arrow/compute/aggregate` 本地补丁当前树中并不存在。因此"直接调用 Arrow 自带分组"不可行，正确路子是**参考 Arrow C++ `GrouperFastImpl`（`arrow/cpp/src/arrow/compute/row/grouper.cc`）在 kwbase 重写一个等价的 Go 分组核**。
- **重写内容（`arrow_aggregate.go` + `arrow_aggregator.go`）**：
  1. **开放寻址分组表 `arrowGroupTable`**（等价 C++ `GrouperFastImpl` 思路，去 `map[string]`）：仅存 dense group id（uint32 数组）+ 每槽首见行索引，线性探测（组从不删除故无 tombstone）；group key 值**不再常驻**，由 `MaterializeRow` 在 `Finalize` 时按首见行从当前 batch 重建。彻底消除每 distinct 组的 Go string 物化。
  2. **列索引预解析**：`resolveColIdxs` 把 groupCols 名字解析为整型列索引一次，`arrowGroupHashIdx`/`arrowGroupRowEqualIdx`/`MaterializeRow` 全走索引，消除热路径上每行列的 `Schema.FieldIndices`（map 查找）开销。
  3. **批量喂入（关键）**：`Consume` 先按组累积行索引 `sels[gid]`，循环结束后再**每个组一次性用整段 selection vector 调 `scalarAggregator.Consume`**——与 colexec 把整段 selection vector 喂入等价，消除"每分组每行一次 Consume 的逐行 IsNull/Value 访问器"开销（这才是真实瓶颈）。
  4. 删除旧 `arrowGroupKey`（`arrow_aggregate.go`）与 `arrowAggregatorCore.groupKey`（`arrow_aggregator.go`）两处 string 物化函数。
- **性能演进（n=10000, 16 组，同数据）**：arrow 分组 1005µs(初测 0.55×) → 去 FieldIndices 后仍 ~1005µs → **批量喂入后 694µs，约 colexec(490µs) 的 1.4×**。CPU profile 确认剩余成本集中在哈希分组本身（`findOrInsert`+`arrowGroupHashIdx`+`maphash/aeshash`，与 colexec 同样要逐行哈希），**架构已与 colexec 持平**，剩余 1.4× 主要是 Go 逐行 `maphash` 与 GC，非结构性缺陷。
- **验证**：`TestArrowGroupedAggMatchesColexec`（分组正确性：arrow 精确 group→sum map vs Go 真值；colexec multiset vs 真值）PASS；`go test -run 'Arrow|Join|Aggregat' ./pkg/sql/rowexec/` 全绿（52s 含 arrowpilot e2e）；`go vet` 干净。
- **对 §9 NEXT 第 4 项（分布式分组聚合路由 Arrow）的启示**：分组核经本波重写已与 colexec **架构持平且性能接近（1.4×）**，不再是"劣于标准引擎"的障碍；但生产路由前仍建议确认流式多 batch 的 `Finalize`/refcount 语义（见 planner.go:4737 现状）。若要把剩余 1.4× 抹平，可选方向：用更快的非加密哈希（如 wyhash/xxhash）替代 `maphash`、或对高频单列 int 组键走特化快速路径。
- **状态**：改动含 `arrow_aggregate.go`、`arrow_aggregator.go`。**待入库**。
