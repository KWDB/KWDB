# rowexec 算子代码走读

> 工程:`gitee.com/kwbasedb/kwbase`(KaiwuDB / KwBasedb)
> 包路径:`pkg/sql/rowexec`
> 说明:本文对行式物理执行层 `rowexec` 的核心算子做源码级走读,聚焦设计模式、状态机与数据流。

---

## 1. 包定位与整体架构

`rowexec`(row-level executor)是 KaiwuDB 分布式 SQL 引擎的**行式物理算子执行层**,对应 CockroachDB 的 `sql/rowexec`。它是 DistSQL 执行框架中真正"干活"的一层:每个 `Processor` 消费上游 `RowSource` 的行、完成一种关系代数运算,再把结果 push 给下游 `RowReceiver`。

整包约 70+ 个文件,每个文件对应一类物理算子。所有算子的入口是 `NewProcessor`(`processors.go`)——一个根据 `ProcessorCoreUnion` 类型做 `switch` 分发的工厂函数,把 planner 生成的执行计划(`execinfrapb`)翻译为具体结构体。

---

## 2. 执行框架与数据流模型

### 2.1 Pull/Push 混合模型

实际上是**拉模型(pull)**:上游提供 `Next() (sqlbase.EncDatumRow, *ProducerMetadata)`,算子在自己的 `Next()` 里从 input 拉行、处理、再返回。但"数据流方向"表现为算子把结果 **push** 给下游——统一通过 `emitHelper`(`processors.go`)完成:

- `emitHelper` 调用 `output.Push(row, meta)`,根据返回的 `consumerStatus` 决定:
  - `NeedMoreRows` → 继续生产;
  - `DrainRequested` → 进入 draining 只转发 trailing metadata;
  - `ConsumerClosed` → 直接关闭,不再生产。

### 2.2 状态机骨架

每个算子内嵌 `execinfra.ProcessorBase`(提供 `StateRunning/StateDraining/StateTrailingMeta/StateClosed`),并额外维护一个 `runningState` 细粒度状态机。模板是:

```go
func (p *xxx) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
    for p.State == execinfra.StateRunning {
        // 按 runningState 分发到各状态处理函数
        // 通过 emitHelper 发射 / MoveToDraining 收尾
    }
    return nil, p.DrainHelper()
}
```

`ProcessorBase` 还统一处理 **PostProcess**(`ProcessRowHelper` / `Out`):投影、过滤表达式、offset/limit、输出列裁剪,所有算子在发射前都要过这一层。

### 2.3 行格式

全程使用 `sqlbase.EncDatumRow`(即 `[]EncDatum`)——**延迟解码的编码行**:列值以 KV 编码形式存放,只在需要时(比较、表达式求值)才 decode。这避免了 scan 之后无谓的全列解码,是列式编码与行式处理之间的桥梁。

---

## 3. 算子全景

| 分类 | 文件 | 算子 | 关键设计 |
|------|------|------|----------|
| 扫描 | `tablereader.go` | tableReader | 计划起点,KV 读取+过滤 |
| 扫描 | `index_skip_table_reader.go` | indexSkipTableReader | 跳索引扫描 |
| 扫描 | `interleaved_reader_joiner.go` | interleavedReaderJoiner | 交织表读取即连接 |
| 连接 | `hashjoiner.go` | hashJoiner | build/probe,可落盘 |
| 连接 | `mergejoiner.go` | mergeJoiner | 有序归并 |
| 连接 | `joinerbase.go` | joinerBase | 所有 join 的公共基类 |
| 连接 | `joinreader.go` / `batchlookupjoiner.go` | joinReader / batchLookupJoiner | 索引回表 lookup join |
| 连接 | `indexjoiner.go` / `zigzagjoiner.go` | indexJoiner / zigzagJoiner | 索引连接 / Z 字形扫描 |
| 聚合 | `aggregator.go` / `streamaggregator.go` | aggregator / streamAgg | 分组聚合 / 流式聚合 |
| 排序 | `sorter.go` | sortAll / sortTopK | 全量/TopK,可落盘 |
| 去重 | `distinct.go` | distinct / sortedDistinct | hash / 有序优化 |
| 窗口 | `windower.go` | windower | 分区+窗口函数 |
| 投影/其他 | `project_set.go` / `values.go` / `ordinality.go` / `subquery.go` | — | 表达式投影、常量、序号列、子查询 |
| 采样/统计 | `sampler.go` / `stats.go` / `countrows.go` | — | 统计信息收集 |
| 回填 | `backfiller.go` / `columnbackfiller.go` / `indexbackfiller.go` | — | schema 变更回填 |
| 时序 | `tsProcessor.go` / `tsInserter.go` / `tsTableReader.go` / `tsDeleter.go` / `tsTagUpdater.go` 等 | — | **Kw 时序特色算子** |

---

## 4. 核心算子走读

### 4.1 tableReader —— 数据流起点(`tablereader.go`)

- 实现 `Processor`/`RowSource`/`Releasable`/`OpNode` 接口。通过 `trPool sync.Pool` 复用对象(`tablereader.go:82`)。
- `newTableReader` 中(`tablereader.go:89`)做两件事:① `tr.Init(...)` 初始化框架;② `initRowFetcher`(`:132`)按 `neededColumns`(`tr.Out.NeededColumns()`)做**列裁剪**,只 fetch 下游需要的列。
- 内部持有 `rowFetcher`(封装 `row.Fetcher`),把 `spec.Spans`(`roachpb.Spans`)翻译成 KV 范围读取,并支持 `maxResults`(LIMIT 提示)、`Reverse`(反向扫描)、`LockingStrength`(锁)。
- 因为是源头,`InputsToDrain: nil`——它没有真正的上游行源,只是 KV 适配器。

### 4.2 join 家族 —— 公共基类 `joinerBase`(`joinerbase.go`)

所有连接算子共享 `joinerBase`,提供:

- `joinType`、`leftEqCols`/`rightEqCols`(等值列)、`onExpr`(非等值 ON 过滤)、`combinedRow`(拼接左右行的缓冲)。
- `emptyLeft`/`emptyRight`(`EncDatumRow`):outer join 时用来给未匹配侧填充 NULL。
- 函数式处理:`calcTag`/`processJoinTree` 等把"拼接+ON 过滤+emit"封装成回调,各子类只负责"如何获取匹配行"。

**hashJoiner**(`hashjoiner.go`)——最复杂的状态机:

状态枚举 `hjBuilding → hjConsumingStoredSide → hjReadingProbeSide → hjProbingRow → hjEmittingUnmatched`:

1. **hjBuilding**:同时消费左右两路,哪边先用满 `initialBufferSize` 就选哪边做 **build 端(短流优化)**;空的一侧且是 inner/对应 outer 时直接短路结束。
2. **落盘**:build 端内存超限且 `useTempStorage` 开启时,落到 `HashDiskBackedRowContainer`,否则 `HashMemRowContainer`。
3. **hjReadingProbeSide / hjProbingRow**:probe 端每行用 `NewBucketIterator` 在 build 端的 hash 桶里找等值行,`combinedRow` 拼接并过 `onExpr`,逐条 emit。
4. **hjEmittingUnmatched**:对 left/right/full outer join,用 `NewUnmarkedIterator` 扫出未匹配的 build 端行,以 `emptyRight`/`emptyLeft` 补 NULL 后发出——这就是 mark 位机制(`ReserveMarkMemoryMaybe`)。

**mergeJoiner**(`mergejoiner.go`):两侧输入按等值列有序,用 `streamMerger`(多路归并器,`mergejoiner.go:114`)逐段对齐;通过 `matchedRight util.FastIntSet` 追踪已匹配的右行,finish 时发出右端未匹配行(RIGHT/FULL outer)。无 build 端内存压力,适合大表有序连接。

### 4.3 aggregator —— 分组聚合 + 时序扩展(`aggregator.go`)

`aggregatorBase`(`:70`)持有 `funcs []*aggregateFuncHolder`(每个聚合列一个函数持有者)、`groupCols`(分组键)、状态机 `runningState`。特点:

- **流式 vs 哈希聚合**:输入按 group key 有序时走 `streamaggregator`(边读边吐分组),无序时走哈希聚合(先物化再算)。
- **`isScalar`**:无分组列时(如 `SELECT MAX(n) FROM t`)即使无输入也产一行,处理空集语义。
- **Kw 时序扩展(重点)**:`groupWindow` 结构(`aggregator.go:134`)在聚合过程中叠加了**四种时序窗口**:
  - `StateWindow` / `EventWindow`(状态/事件窗)
  - `CountWindow`(计数窗,支持滑动 `slidingWindowSize`)
  - `TimeWindow`(时间窗,支持滑动 `SlidingTime`)
  - `SessionWindow`(会话窗,按间隔 gap 切分)
  这些通过 `CheckAndGetWindowDatum`(`:266`)在每行聚合时动态计算窗口编号 `groupWindowValue`,并写回窗口列 `groupWindowColID`。另有 `gapfill`(时间桶补洞)、`time_bucket_gapFill` 等时序专属能力——这是 KwBasedb 区别于原生 CockroachDB 的关键增量。

### 4.4 sorter —— 排序(`sorter.go`)

`sorterBase`(`:45`)持有 `rows rowcontainer.SortableRowContainer` 与迭代器 `i`。

- **内存/落盘**:`useTempStorage` 时构造 `DiskBackedRowContainer` 并配 `diskMonitor`,内存超限自动 spill 到临时存储;否则 `MemRowContainer`。
- **前缀有序优化**:`matchLen` 表示前 N 列已有序(上游已按这些列排好),可省去这部分比较。
- `sortAll`(全量排序)与 `sortTopK`(堆选 TopK)共用 `sorterBase.Next`(`:123`)——先把全部/部分行装入 container,排序后逐行迭代输出。

### 4.5 distinct —— 去重(`distinct.go`)

- `distinct` 用 `seen map[string]struct{}` 按"去重列的编码 key"判重;`orderedCols`/`distinctCols` 区分有序列与去重列;`nullsAreDistinct` 控制 NULL 是否视为互异;`errorOnDup` 支持 `DISTINCT ... ON` 报错语义。
- **sortedDistinct 优化**:当所有去重列都已有序,只需比较"上一行 `lastGroupKey`",无需 map,复杂度降为 O(n),是典型"有序即免哈希"优化。

### 4.6 windower —— 窗口函数(`windower.go`)

`windower`(`:72`)状态机 `windowerAccumulating → windowerEmittingRows`:

- 先把输入按 `partitionBy` 分区物化进 `allRowsPartitioned`(哈希分桶,`HashDiskBackedRowContainer`,可落盘)。
- 对每个分区用 `DiskBackedIndexedRowContainer` 排序,逐窗口函数(`windowFns`/`builtins`)计算后把值写回 `outputColIdx`。
- 最小内存约束 `memRequiredByWindower = 100KB`(`:66`),超阈值走磁盘容器。

---

## 5. 贯穿所有算子的公共模式

1. **内存与落盘**:几乎每个算子都配 `mon.BytesMonitor` / `mon.BoundAccount`,超 `memorylimit` 时借助 `rowcontainer` 的 `DiskBacked*` 系列 spill 到 `TempStorage`(`sorter`/`hashJoiner`/`windower`/聚合均如此)。这是"内存优先、磁盘兜底"的统一策略。
2. **取消与超时**:`cancelChecker *sqlbase.CancelChecker` 在执行循环里周期性检查,支持查询取消。
3. **统计与追踪**:`newInputStatCollector` 包裹 input 收集行数/耗时;`FinishTrace = outputStatsToTrace` 把算子级耗时写入 tracing span,对应 `DistSQLSpanStats`。
4. **对象复用**:`sync.Pool`(`trPool`)+ `Releasable` 接口,减少 GC 压力。
5. **向量化旁路**:`ProcessorBase` 保留了 `Vectorize` 标志,KwBasedb 在部分路径支持向量化执行(`execgen` 生成的 `exec/`),行式算子是其兼容/兜底实现。

---

## 6. KwBasedb 的特色增量

相比上游 CockroachDB,本包显著扩展了**时序(Time-Series)能力**:独立的 `tsProcessor`/`tsInserter`/`tsDeleter`/`tsTableReader`/`tsTagUpdater`/`tsCreateTable`/`ts-alter-table` 算子族,直接面向时序场景的写入/读取/标签更新;以及在 `aggregator` 中嵌入的 `groupWindow`(状态/计数/时间/会话/事件五类窗口)+ `gapfill` + `time_bucket_gapFill`。这是理解本工程行式执行层与社区版本差异的核心。

---

## 附:主要文件速查

| 文件 | 关键符号 |
|------|----------|
| `processors.go` | `NewProcessor`、`emitHelper` |
| `joinerbase.go` | `joinerBase`、`combinedRow`、`emptyLeft/Right` |
| `hashjoiner.go` | `hashJoinerState` 状态机、`HashMemRowContainer`/`HashDiskBackedRowContainer` |
| `mergejoiner.go` | `mergeJoiner`、`streamMerger`、`matchedRight` |
| `aggregator.go` | `aggregatorBase`、`groupWindow`、`CheckAndGetWindowDatum` |
| `sorter.go` | `sorterBase`、`SortableRowContainer`、`sortAll`/`sortTopK` |
| `tablereader.go` | `tableReader`、`trPool`、`initRowFetcher` |
| `distinct.go` | `distinct`、`sortedDistinct`、`seen` |
| `windower.go` | `windower`、`allRowsPartitioned` |
