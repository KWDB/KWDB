# RowExec / ColExec 统一到 Arrow 的架构演进建议

> 背景：rowexec 行式执行模型单行性能有限，colexec 向量化覆盖度不足，且两条路径因内存格式不同（EncDatumRow vs coldata）无法逐算子混合使用。本文给出以 Arrow 统一两条计算路径、分阶段提升执行性能的工程建议。
>
> 范围：不考虑时序（ts）系列算子，聚焦通用关系算子的行式 / 向量化执行。

---

## 1. 现状与问题根因

当前执行层有三套内存表示：

| 表示 | 位置 | 形态 | 适用 |
|------|------|------|------|
| `EncDatumRow` | `pkg/sql/rowexec` | 行式，`[]EncDatum`，延迟解码 KV 编码 | 逐行 pull 模型 |
| `coldata` | `pkg/sql/colexec` | 列式，带 Selection 向量，变长列 offset+data | 向量化批量 |
| Arrow 列式 | `vendor/.../apache/arrow/go/arrow` | 列式连续 buffer | 已 vendored，暂未用于执行层 |

观察到的三个现象，根因是两道独立的墙，必须分开治理：

| 现象 | 根因 |
|------|------|
| rowexec 单行性能低 | 逐行 `Next()` + 每次走 `tree.Datum` 表达式 + 大量小对象分配 |
| colexec 覆盖不足 | 向量化 kernel/builtin 是手写 + execgen 生成，许多表达式/算子没有向量化版本 |
| 两条路径不能混合 | 内存格式不同（EncDatumRow vs coldata），边界必须经 `materializer`（批量↔逐行）转换，开销大，planner 被迫整段选一路 |

**关键判断：格式不统一只是"不能混合"的墙；覆盖不足的根因在表达式引擎，而非内存格式。用 Arrow 统一格式能拆掉第一道墙，但第二道墙需另外解决——这正是 colexec 覆盖面不足的根本原因，换 Arrow 不会自动消失。**

---

## 2. 方向：用 Arrow 统一，但要定义"统一到哪一层"

Arrow 统一有两种深度，收益与代价差异巨大：

### 浅统一（格式统一，计算两套）
所有算子读写 `arrow.Record` / `ArrayData`。行式算子内部仍逐行取 `tree.Datum` 计算，向量化算子用 Arrow kernel。
- ✅ 立即解锁"任意混合"：边界仅剩 batch size 差异，**零格式转换**，planner 可逐算子选向量化/行式。
- ✅ 覆盖度立刻 = rowexec（全）+ colexec（部分），不再整段回退。
- ⚠️ 行式算子性能无本质提升（仍逐 datum 计算），但混合问题被解决。

### 深统一（格式 + 计算统一到 arrow）
表达式也写成 Arrow compute kernel，行式 fallback 逐步淘汰。
- ✅ 真正性能 + 覆盖兼得。
- ❌ 工程量 = 重写向量化表达式层 + 全部标量 kernel，是 colexec 当年未走完的路。

**建议：浅统一先行，深统一渐进，不要一上来就深统一。**

---

## 3. 最省力的杠杆：复用 arrow/compute

本地 workspace（`github.com/apache/arrow-go/v18/arrow/compute`）已有一整套成熟的向量化执行框架（前面已走读）：
- Kernel / KernelExecutor 框架，zero-copy 的 `ArraySpan` 视图；
- Scalar / Vector kernel 分类，`DispatchBest` 类型推断；
- `propagateNulls` 等 null 处理约定；
- Validity bitmap（LE bit packing）+ 定长值 / offset+data / 索引等标准列式布局。

最务实的路线不是"用 Arrow 从零重写 coldata kernel"，而是：

> **让 colexec 的热点算子直接调用 `arrow/compute` 的 kernel，而不是自己维护一套 coldata kernel。**

收益：
- 大量 builtin / 聚合 / 比较 / cast 直接复用 Arrow 现成 kernel，**覆盖度问题被 Arrow 生态兜底**；
- 避免重复维护 `*.tmpl` / `*.eg.go` 两套代码生成；
- 与"Arrow 统一格式"自然契合（coldata 直接映射成 Arrow，或让 colexec 直接产出 Arrow batch）。

---

## 4. 渐进式落地路线

```
阶段0  定义统一接口 Next() (arrow.Record, done) + 统一 memory.Allocator / BufferPool
       （对齐 Arrow 版本：vendor 旧路径 apache/arrow/go/arrow 与本 workspace v18 存在 API 代差，需先定版本）

阶段1  Adapter 包装
       - rowexec 算子包成"消费/产出 arrow.Record"（内部仍逐行，但接口统一）
       - colexec 算子输出 arrow.Record（coldata ↔ arrow 映射）
       此时混合已可行，无需 materializer 重转换

阶段2  热点算子深统一：tableReader 扫描、filter/projection、hashjoin、agg、sort
       的表达式切到 arrow/compute kernel（复用现有 kernel，最快见效）

阶段3  淘汰 coldata，全部收敛到 Arrow；行式 fallback 仅作不支持类型的兜底
```

---

## 5. 必须正面处理的设计点

### 5.1 保留 KV pass-through
`EncDatum` 的精髓是"排序键编码字节可直接 `bytes.Compare`，不解码"。Arrow 列式默认要定长解码值。建议：
- 对键列引入 **Arrow Dictionary 编码** 或 **extension / fixedsizebinary 保留原始 KV 编码字节**；
- 按需 decode 成 `tree.Datum`；
- 比较走 Arrow 的定长 / 字节 compare kernel。
否则扫描端会多一次全量 decode，抵消列式收益。

### 5.2 表达式与 tree.Datum 的桥
保留 `vec_elem_to_datum` 这类双向转换（类似 colexec 现有机制），让未向量化的 builtin 仍能回退到 `tree.Datum` 计算——这是"覆盖不足"时的安全网，保证不回退整段。

---

## 6. 不换格式的短期缓解（可与统一工程并行）

在统一完成前，rowexec 单行性能可做低成本优化：
- **Batch Next**：一次返回 N 行而非 1 行，摊薄接口 / 函数调用开销；
- **表达式闭包化 / 预编译**：避免每行表达式重绑定；
- **Arena 分配**：用 `stringarena`（rowexec 已在用）或 bump allocator 降低 `tree.Datum` 分配 / GC 压力。

---

## 7. 风险提示

- **版本分裂（精确）**：vendor 旧版模块路径为 `github.com/apache/arrow/go/arrow`（无版本号，约 2018–2019 的 dep 时代代码，仅含 `array`/`memory`，**无 `compute` 包**）；本 workspace 为 `github.com/apache/arrow-go/v18`（go 1.25 dev，发布版 v18.0.0 的 go.mod 为 `go 1.22.0`）。**底层列式 buffer 物理布局两者兼容**（ArrayData 的 validity bitmap/offset+data 约定稳定），但模块路径不互通、且旧版缺失 `compute` 执行层。**版本选型约束**：因计划 go 工具链上限为 1.21，须以 `arrow-go/v17`（`github.com/apache/arrow/go/v17`，go.mod `go 1.21`，含完整 `compute` 包）为基线；v18 要求 `go 1.22.0` 超出上限不可用。统一方案须基于 v17 做依赖路径迁移（go.mod + import 重写），列式 buffer 数据本身可直接搬运，无需格式转换层。
- **表达式覆盖是长期工程**：Arrow 统一解决"混合"，但 builtin / 表达式的向量化覆盖仍需逐个补齐，是持续投入，非一次性迁移。
- **时序（ts）路径**：已有列式向量化读取（`colexec/ts_reader.go`），统一时要与其衔接而非冲突。

---

## 8. 一句话总结

Arrow 统一是对的"总方向"，但其真正价值是**拆掉格式壁垒、让行式 / 向量化任意混合**，而非让 buffer 变快；要落地为"性能 + 覆盖"双赢，关键是**顺手复用 arrow/compute kernel 补 colexec 的覆盖缺口**，并保留 KV 编码的 pass-through（字典 / extension 类型）。先浅统一解锁混合，再对热点算子做深统一，分阶段推进、可随时回退。

---

## 附录：相关事实依据

- `vendor/github.com/apache/arrow/go/arrow/go.mod` 存在 → Arrow 已 vendored，工程可直接使用列式类型。
- `pkg/sql/colexec/` 含 `columnarizer.go`（行转列）、`materializer.go`（列转行）、`rowstovec`、`allocator.go` 及 `*.eg.go`（execgen 生成产物）→ 已有完整向量化层，使用自定义 `coldata` 格式，非 Arrow。
- `pkg/sql/sqlbase/encoded_datum.go`：`EncDatumRow = []EncDatum`，`EncDatum` 为 (encoding, encoded []byte, Datum) tagged union，支持延迟解码与 KV 字节直比。
- `pkg/sql/rowcontainer/row_container.go`：`MemRowContainer` 底层为 `[]EncDatumRow` 数组树；仅 `DiskBacked*` 在落盘时序列化为连续变长编码字节流。
- 本地 workspace `github.com/apache/arrow-go/v18/arrow/compute`：提供成熟的向量化 kernel 执行框架，可作为 colexec 热点算子的 kernel 来源。
- **版本差异实测**：vendor `github.com/apache/arrow/go/arrow/go.mod` 模块路径无版本号，`require` 仅 flatbuffers v1.10.0 + testify 等，残留 `Gopkg.lock`/`Gopkg.toml`，顶层仅 `array`/`memory`/**无 `compute`**；本 workspace `go.mod` 为 `github.com/apache/arrow-go/v18`、`go 1.25`(发布版 v18.0.0 实测 `go 1.22.0`)，含完整 `compute/exec`。**列式 buffer 物理格式两者一致**（ArrayData 内存布局稳定），差异在命名空间与执行能力，非 buffer 格式断裂。统一方案须基于 v17 而非 vendor 旧版。
- **arrow-go 版本 ↔ Go 要求对照**（实测各 tag 的 go.mod）：
  - v13 / v14 / v15：`github.com/apache/arrow/go/v1x`，`go 1.20`
  - **v16.0.0**：`github.com/apache/arrow/go/v16`，`go 1.21`
  - **v17.0.0**：`github.com/apache/arrow/go/v17`，`go 1.21` ← **支持 go 1.21 的最高版本，含 `compute` 包，统一方案基线**
  - v18.0.0：仓库迁移为 `github.com/apache/arrow-go/v18`，`go 1.22.0`（超出 go 1.21 上限，不可用）
  - 注：新仓库 `github.com/apache/arrow-go` 仅有 v18.x 系列 tag；v17 及更早位于旧仓库 `github.com/apache/arrow`。

---

## 9. arrow-go v17 代码获取与依赖接入（go 1.21 基线）

v17 的 import 路径、Git 仓库、tag 三者不在同一层，这是"路径无法访问 / 404"的常见原因：

- **Go module 路径（写 import / go.mod 用）**：`github.com/apache/arrow/go/v17`
  - 包路径示例：`github.com/apache/arrow/go/v17/arrow`、`/arrow/array`、`/arrow/compute`、`/arrow/memory`
- **实际 Git 仓库（旧 monorepo）**：`github.com/apache/arrow`（注意不是 `apache/arrow-go`）
  - v17 及更早版本都在旧仓库；新仓库 `apache/arrow-go` 仅有 v18.x
- **源码 tag**：`go/v17.0.0`，模块代码位于仓库的 `go/` 子目录
- **浏览 / clone**：
  - 在线浏览：`https://github.com/apache/arrow/tree/go/v17.0.0/go`
  - clone：`git clone https://github.com/apache/arrow && cd arrow && git checkout go/v17.0.0`（代码在 `go/` 目录）
- **Go 拉取**：`go get github.com/apache/arrow/go/v17@v17.0.0`（经 GOPROXY 解析到旧仓库的 `go/v17.0.0` tag）

### 在 kwbasedb 中接入

`go.mod` 增加：

```
require github.com/apache/arrow/go/v17 v17.0.0
```

代码中 import：

```go
import (
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/compute"
    "github.com/apache/arrow/go/v17/arrow/memory"
)
```

### 注意

- 直接访问 `https://github.com/apache/arrow/go/v17` 会 404，因为那不是仓库 URL（仓库是 `apache/arrow`，路径里的 `/go/v17` 是 module 后缀）；正确浏览入口是上面的 `tree/go/v17.0.0/go`。
- 若 `go get` 直连 GitHub 失败，配置代理：`GOPROXY=https://goproxy.cn,direct`（或 `proxy.golang.org`）。
- 本 workspace（`/home/sdy/go/src/github.com/arrow-go`）是 v18 新仓库代码，**切不到 v17**；读源码参考可用本 workspace，但编译依赖必须改用 v17 module。
- v17 与 v18 的 `arrow/array`、`arrow/compute`、`ArrayData` 列式布局一致，可互相参照。

---

## 10. 试点实现与验证（已落地）

> 本章记录基于前 9 节方案的**实际落地结果**，用于验证「以 rowexec 为基础、用 Arrow 统一执行层」的可行性。

### 10.1 工程构建模式（关键前提）

本仓库**无 `go.mod`**，采用 `GO111MODULE=off` + GOPATH + `vendor/` 构建（`make` 默认 `GO ?= go`）。因此「接入 v17」不是改 `go.mod` 的 `require`，而是：

1. 把 v17 源码（`/home/sdy/go/src/github.com/arrow/go` 的 `go/` 子目录，module path `github.com/apache/arrow/go/v17`）整树复制到
   `kwbase/vendor/github.com/apache/arrow/go/v17/`；
2. 将现有 7 处 `github.com/apache/arrow/go/arrow/{array,memory}` 的 import 改为 `.../go/v17/arrow/{array,memory}`
   （`col/colserde/{record_batch,file,arrowbatchconverter}.go`、`colflow/colrpc/inbox.go` 及 3 个测试）。
3. **必须从 `gitee.com/kwbasedb/kwbase` 的 GOPATH 路径编译**（该目录是指向 `github.com/kwbasedb/kwbase` 的软链）。若从 `github.com/...` 路径编译，vendor 前缀不一致会触发两套 opentracing/oid 副本冲突。

### 10.2 v17 依赖闭包与离线 shim

v17 的 `arrow/compute` 触发了比预期更大的传递依赖。经实测，需补入 vendor 的依赖（均在 `GOMODCACHE` 已解压可得）：

| 依赖 | 用途 | 来源 |
|------|------|------|
| `golang.org/x/exp` 的 `constraints/maps/slices` 子包 | `arrow/type_traits` 等 | GOMODCACHE（仅**追加**子包，不覆盖旧 `rand`）|
| `golang.org/x/sync` | `errgroup.SetLimit` | GOMODCACHE（覆盖旧版，API 向后兼容）|
| `github.com/goccy/go-json` | `arrow/ipc` | GOMODCACHE |
| `github.com/klauspost/compress` | `arrow/ipc` 压缩 | GOMODCACHE |
| `github.com/pierrec/lz4/v4` | `arrow/ipc` 压缩 | GOMODCACHE |
| `golang.org/x/xerrors` | compute | GOMODCACHE |
| `github.com/JohnCGriffin/overflow` | `compute` 算术 kernel | **本地无源码，写最小 shim**（`Mul64`）|
| `github.com/zeebo/xxh3` | `internal/hashing`（仅 hash 内核用）| **本地无源码，写最小 shim**（`Hash`）|

> `arrow/compute/exprs`（依赖 `substrait-go`）被隔离在子包内，顶层 `compute` 包**不** import 它；只要不引用 `arrow/compute/exprs`，就不会拉入 substrait 重型闭包。两个 shim 仅实现被引用的单个函数，且项目当前不触发 hash 内核，**不影响正确性**。

实测：v17 的 `arrow/compute` 在 `go1.20.12`（低于其 `go.mod` 声明的 1.21）下**可编译**——GOPATH 模式不读取 `go.mod` 的 `go` 指令，且源码未用到 1.21 专属语言特性。

### 10.3 新增代码（ pilot ）

| 文件 | 内容 |
|------|------|
| `pkg/sql/rowexec/arrow_adapter.go` | `UnifiedProcessor` 接口（`Next(ctx)->arrow.Record,done,err` + 统一 `memory.Allocator`）；`rowToArrowConverter`（内部仍按行取 `EncDatumRow`，对外攒批为 Arrow Record）；`arrowRecordSource`（把已有 Record 包装为 `UnifiedProcessor`）|
| `pkg/sql/rowexec/arrow_projection.go` | `arrowProjection` 算子：调用 `arrow/compute.CallFunction`（如 `"add"`）在 Arrow Record 上做向量化投影，零拷贝视图 `compute.NewDatum(arr)` |
| `pkg/sql/colexec/arrow_bridge.go` | `BatchToRecord` / `RecordToBatch`：`coldata.Batch ↔ arrow.Record` 桥接（Int64/Float64/Bool/Bytes/Null），打通 colexec 向量化算子与 Arrow 下游 |
| `pkg/sql/rowexec/arrowpilot/pilot_test.go` | 端到端验证测试 |

### 10.4 验证结果

`go test ./pkg/sql/rowexec/arrowpilot/`（链接 `libkwdbts2.so`）**通过**：

```
rowexec EncDatumRows  --(rowToArrowConverter)-->  Arrow Record (2列 × 4行)
Arrow Record         --(arrowProjection, arrow/compute "add")-->  投影 Record
断言结果 c = a + b == [11, 22, 33, 44]   ✅
```

`go build ./pkg/sql/colexec/`（含 `arrow_bridge.go`）**通过**。

结论：**可行性已证实**——Arrow v17（含 `compute`）可在本离线 GOPATH+vendor 工程下编译，且「rowexec 基础 + 对外 Arrow Record + colexec 桥」的统一路径可端到端跑通。

### 10.5 性能预期（与可行性结论一致）

- **会提速的部分**：热点算子（如 projection）走 `arrow/compute` 向量化 kernel（O(n) 批量、紧循环、零拷贝视图）；colexec↔Arrow 桥消除 `materializer` 整批重建。
- **不会自动提速的部分**：浅统一本身不加速 rowexec（仍逐行 `tree.Datum`）；`arrow/compute` 仅覆盖通用数值/字符串/布尔类型，`DECIMAL`/`TIMESTAMP`/`UUID`/`JSON` 等需回退，收益归零。
- **收益依赖逐算子深统一**：单一个 projection 试点仅在基础类型大批量下体现（预计 1.5–3x）；整体性能需把更多热点算子（filter/agg/join key）也接到 `arrow/compute` 后兑现。

### 10.6 后续步骤

1. 将现有 7 处 arrow import 迁移到 `/v17`（机械替换 + 少量 API 适配），移除旧 `vendor/.../apache/arrow/go/arrow`。
2. 在 planner 中接入 `UnifiedProcessor`，允许 rowexec/colexec 算子按 `UnifiedProcessor` 任意混排。
3. 逐个把 filter/agg 等热点算子深统一到 `arrow/compute`，并补充 `DECIMAL`/`TIMESTAMP` 等类型的自定义 kernel 或 decode 回退。
4. 补充 colexec 桥的 round-trip 测试与含 projection 的查询级 benchmark，量化转换开销下降。

## 11. 第一轮迁移完成情况（已落地）

### 11.1 旧 arrow import 迁移到 v17

将 7 处工程代码的 import 从 `github.com/apache/arrow/go/arrow`（不含 compute 的旧版）迁移到 `github.com/apache/arrow/go/v17/arrow`：

- `pkg/col/colserde/record_batch.go`
- `pkg/col/colserde/file.go`
- `pkg/col/colserde/arrowbatchconverter.go`
- `pkg/sql/colflow/colrpc/inbox.go`
- `pkg/col/colserde/record_batch_test.go`
- `pkg/col/colserde/arrowbatchconverter_test.go`
- `pkg/sql/colexec/types_integration_test.go`

包名（`arrow` / `array` / `memory`）保持不变，仅做路径替换。v17 的 `array.NewData(..., buffers []*memory.Buffer, ...)` 与旧版签名一致，故 `arrowbatchconverter.go` 中基于 `*memory.Buffer` 的构造逻辑无需改动。

**必要的 API 适配（仅 2 类）：**

1. `array.NewXxxArray().Data()` 在 v17 返回 `arrow.ArrayData` 接口，原代码期望 `*array.Data`，需断言 `.(*array.Data)`：
   - `arrowbatchconverter.go`：4 处（`boolBuilder.NewBooleanArray()` / `binaryBuilder.NewBinaryArray()`）。
   - `record_batch_test.go`：3 处（`builder.NewArray().Data()` / `b.NewArray().Data()`）。
2. `array.Interface` 在 v17 已移除，数组接口统一为 `arrow.Array`：`arrowbatchconverter.go` 中 `var arr array.Interface` 改为 `var arr arrow.Array`（仅用于 `arr.NullBitmapBytes()`，接口保留该方法）。

完成后移除旧 `vendor/github.com/apache/arrow/go/arrow` 目录，vendor 下仅保留 `v17`。已全工程搜索确认除旧目录自身外无任何外部引用（含 `pkg/col/colserde/arrowserde` 子包）。

### 11.2 编译验证

- `go build ./pkg/col/colserde/ ./pkg/sql/colflow/colrpc/`：通过。
- 测试二进制编译 + 链接（需 `CGO_LDFLAGS="-L<repo>/install/lib"` 链接 `libkwdbts2.so`）：
  - `pkg/col/colserde`（含 `record_batch_test.go` / `arrowbatchconverter_test.go`）：通过。
  - `pkg/sql/colexec`（含 `types_integration_test.go`）：通过。

所有从 `github.com/kwbasedb/kwbase` 的 gitee 软链路径编译（`GO111MODULE=off` + vendor），避免 vendor 前缀双副本冲突。

### 11.3 新增 colexec 桥测试与基准

- 新增 `pkg/sql/colexec/arrow_bridge_test.go`：
  - `TestArrowBridgeRoundTrip`：colexec `Batch` → Arrow `Record`（`BatchToRecord`）→ colexec `Batch`（`RecordToBatch`）往返，覆盖 `Int64` / `Float64` / `Bool` / `Bytes`，数据完全一致（PASS）。
  - `BenchmarkArrowBridgeRoundTrip`：1024 行 × 3 列往返 ≈ **59µs/op**。
- 新增 `pkg/sql/rowexec/arrowpilot/projection_bench_test.go`：
  - `BenchmarkArrowProjection`：1024 行投影（`c = a + b`）经 rowexec → Arrow → `arrow/compute "add"` 统一路径 ≈ **68µs/op**（PASS）。

以上基准量化了"row/col 混排时每次跨模型转换"的固有开销，为后续深统一（消除重复编码）提供基线。

### 11.4 端到端验证（真实 SQL 查询）

新增 `pkg/sql/rowexec/arrowpilot/e2e_test.go`（`TestArrowProjectionE2EWithRealSQL`）：

- 通过 `serverutils.StartServer` 启动真实 in-process KWDB server；
- 执行 `CREATE TABLE t (a INT, b INT)` + `INSERT INTO t VALUES (...)` 产生真实数据；
- 以真实查询 `SELECT a+b FROM t ORDER BY a` 作为**标准答案**；
- 以真实查询 `SELECT a, b FROM t ORDER BY a` 的返回行（真实列类型与编码）经 `RowToArrowConverter → ArrowProjection("add")`（arrow/compute）算出 `c=a+b`；
- 逐行对比统一路径结果与标准答案，**完全一致（PASS，0.16s）**。

该测试把统一路径接到真实 SQL 查询产生的真实数据上，验证了投影算子在真实数据类型与编码下的正确性。下一步是把 `arrowProjection` 接入 planner 的投影算子（`renderNode` / `projectNode` 或 distsql 的 projection processor），使查询**执行本身**走统一路径，而非仅在测试侧用真实数据驱动。

### 11.5 待办（下一轮）

- 见 10.6 第 3 项：把 filter/agg/join key 等热点算子深统一到 `arrow/compute`，并补充 `DECIMAL`/`TIMESTAMP` 类型的自定义 kernel 或 decode 回退。
- "含 projection 的查询级 benchmark"目前为算子级微基准；完整的 SQL 查询级 benchmark 需接入 planner 的 materializer-free 路径后再补。
- `pkg/sql/arrowsmoke/` 探针包仍保留（未删除），可在统一路径稳定后清理。

### 11.6 planner 接入（已落地）：真实 SQL 查询的执行走统一路径

> 本节把 11.4 的"测试侧用真实数据驱动统一路径"推进为"planner 自身把投影算子下发为 Arrow compute 阶段"，使 `SELECT a+b FROM t` 这类查询的**执行本身**经由 `arrowProjectionProcessor`（arrow/compute）完成。

#### 11.6.1 设计原则

- **opt-in**：新增集群设置 `sql.arrow_projection.enabled`（默认 `false`），默认行为完全不变；仅当显式开启时，planner 才会把投影下发给 Arrow compute。
- **复用现有 proto 通道**：用既有 `execinfrapb.Expression` 消息承载一个 JSON 计划（`Expr` 字段），避免重新生成 protobuf —— 在 `ProcessorCoreUnion`（flat struct，非 oneof）上新增一个 `ArrowProjection *Expression` 字段即可。
- **最小侵入**：仅在 `physicalplan.AddRendering` 顶部加一个分流分支，命中条件才走 Arrow；否则走原有 scalar 渲染路径。

#### 11.6.2 改动清单

| 文件 | 内容 |
|------|------|
| `pkg/sql/execinfrapb/processors.pb.go` | `ProcessorCoreUnion` 新增 `ArrowProjection *Expression`（field 51）；Marshal/Size/Unmarshal 增加对应分支（字节 tag `0x9a,0x03`，模式照搬既有 `LocalPlanNode` field 24） |
| `pkg/sql/physicalplan/physical_plan.go` | 注册 `arrowProjectionEnabledSetting = settings.RegisterBoolSetting("sql.arrow_projection.enabled", ..., false)`；`arrowProjectionEnabled(evalCtx)` 读 `evalCtx.Settings.SV`；`AddRendering` 顶部：`if enabled && canArrowRender && hasArrowComputeExpr { return addArrowRendering }`（`hasArrowComputeExpr` 避免纯透传 `SELECT a` 也走 Arrow 而增加无谓 stage）；`canArrowRender` 放行 `BinaryExpr`（`Plus/Minus/Mult/Div`，操作数可为列或同族常量）、`UnaryExpr`（`UMinus`）、以及纯列透传，类型须属 `Int/Float/Bool/String/Bytes` 族，且 `post.RenderExprs` 为空、`MergeOrdering` 为空；`addArrowRendering` 为每个 render 表达式生成 `compute`（`add/subtract/multiply/divide/negate`）或 `passthrough`（`copy`）列，常量以 `Col:-1` + `cint/cfloat` 编码，最后序列化 JSON 并 `AddNoGroupingStage(core, PostProcessSpec{}, outT, MergeOrdering)` |
| `pkg/sql/rowexec/processors.go` | `NewProcessor` switch 在 `Noop` 之后：若 `core.ArrowProjection != nil`，`checkNumInOut(inputs,outputs,1,1)` 后返回 `newArrowProjectionProcessor(...)` |
| `pkg/sql/rowexec/arrow_projection_processor.go` | 新增 `arrowProjectionProcessor`（`RowSource`）；`newArrowProjectionProcessor` 解析 JSON 计划 → `Init`；`Start` 调 `compute`；`Next` 遍历 `outputRows`；`ArrowProjectionRunCount()`（atomic）供测试确认路径命中；`ArrowProjectionSpec` 的 `Args []ArrowArg` 支持列（`ColName`）或标量常量（`Scalar compute.Datum`），`eval` 对 `copy` 走保留切分、其余调 `compute.CallFunction`；并提供 `ArrowProjectionResultInt64` / `ArrowProjectionResultsInt64`（多列）把 arrow 类型断言收敛在 rowexec 包内（见 11.6.4） |

#### 11.6.3 数据流与关键正确性点

`compute` 阶段：
1. 从 `input` 逐行 `Next()` 取出 `EncDatumRow`，对每列 `EnsureDecoded(&inTypes[i], p.da)` 解码出**不可变** `tree.Datum`，再复制进新 `EncDatum{Datum: d.Datum}` —— 必须复制，因为上游 `EncDatumRow` buffer 在两次 `Next()` 之间被复用，否则所有行都会读到最后一行的数据。
2. 经 `RowToArrowConverter` 攒成 Arrow Record（`inPtrs []*types.T`）。
3. 据 JSON 计划构造 `[]ArrowProjectionSpec`（compute 列 `out%d`、输入列 `col%d`，纯透传列直接映射），调 `NewArrowProjection` 在 Arrow Record 上算出投影列。
4. `arrowRecordToEncDatumRows` 把 Int64/Float64/Boolean/String/Binary 解码回 `EncDatum` 输出。

#### 11.6.4 arrow 双拷贝类型问题（构建正确性）

测试包若直接 `out.Column(0).(*array.Int64)` 断言，会因同一仓库从两个 import 路径（`github.com/apache/arrow/go/v17/...` 与 vendor 前缀 `github.com/kwbasedb/.../vendor/...`）编译出"同名不同包"的两个 arrow 类型而报 `*array.Int64 未实现 arrow.Array`。解法是把 arrow 类型断言**收敛到 rowexec 包内单个 arrow 实例**，由 `ArrowProjectionResultInt64` 在包内完成 `Column(0).(*array.Int64)` 断言并返回普通 `[]int64`；测试只调用该 helper，不再触碰 arrow 类型。

#### 11.6.5 验证结果

新增 `pkg/sql/rowexec/arrowpilot/e2e_test.go` 的两个 server 级测试：
- `TestArrowProjectionPlannerIntegration`（单 binary add）：启动真实 server → `CREATE TABLE t (a INT, b INT)` → `INSERT` 6 行 → `SET CLUSTER SETTING sql.arrow_projection.enabled = true` → 在 retry loop 中执行 `SELECT a+b FROM t` 直到 `rowexec.ArrowProjectionRunCount()` 递增 → 断言结果 `{11,22,33,44,55,66}`。
- `TestArrowProjectionPlannerComputeExprs`（表达式覆盖）：同一套建表/插入/开关，逐条验证以下查询**确由 `arrowProjectionProcessor` 执行（`ArrowProjectionRunCount` 递增）且结果正确**：

  | 查询 | 期望结果（按首列升序） |
  |------|------|
  | `SELECT a+b FROM t` | `{11,22,33,44,55,66}` |
  | `SELECT a+b, a*2 FROM t` | `{{11,2},{22,4},{33,6},{44,8},{55,10},{66,12}}` |
  | `SELECT -a FROM t` | `{-1,-2,-3,-4,-5,-6}` |
  | `SELECT a+1 FROM t` | `{2,3,4,5,6,7}` |
  | `SELECT a+b, a FROM t` | `{{11,1},{22,2},{33,3},{44,4},{55,5},{66,6}}`（含透传列 copy） |

**全部通过（PASS）**：
```
--- PASS: TestArrowProjectionPlannerIntegration (0.11s)
--- PASS: TestArrowProjectionPlannerComputeExprs (0.25s)
--- PASS: TestArrowProjectionE2EWithRealSQL       (真实数据驱动，PASS)
--- PASS: TestArrowProjectionPilot                (算子级 round-trip，PASS)
```

#### 11.6.6 表达式覆盖扩展（本轮新增）

在 11.6 基础上，统一路径的表达式覆盖从"单 binary add、仅列操作数"扩展到：

- **多输出列**：`AddRendering` 本就接收全部 render 表达式，`addArrowRendering` 为每个表达式生成独立的 `ArrowProjectionSpec`，Arrow compute 一次性产出多列（见 `SELECT a+b, a*2`）。
- **常量操作数**：`arrowOperandArg` 把 `*tree.DInt`/`*tree.DFloat` 编码为 `arrowArg{Col:-1, cint/cfloat}`；processor 端经 `arrowConstDatum` 用 `compute.NewDatum(int64/float64(...))` 包成标量，与数组同帧传入 `compute.CallFunction`（arrow v17 支持标量广播）。`canArrowRender` 要求常量与另一操作数**同类型族**（避免隐式提升），且至少有一个列引用。
- **一元负号**：`UnaryExpr(UMinus)` → `negate` 单目 kernel（也兼容优化器把 `-a` 重写成 `0 - a` 的 binary 形式，两条路径都放行）。
- **透传列**：纯列引用（混合在 compute 表达式中，如 `SELECT a+b, a`）走 `passthrough` → `copy`。注意 Arrow v17 没有名为 `copy` 的函数，故 `eval` 对 `Func=="copy"` 特判，用 `array.NewSlice` 返回**保留引用**的输入列切片（避免 `in` 在 `Next` 中 `Release` 后悬空）。`hasArrowComputeExpr` 仅在没有 compute 表达式时退回标量路径，避免 `SELECT a` 这类纯透传也白开一个 stage。

**踩坑**：常量 `arrowArg` 必须显式 `Col:-1`，否则 `Col` 零值为 `0`，processor 会把该参数当成列 `col0` 而忽略常量（`multiply(col0, col0)` 退化成 `col0`）—— 已修复。

结论：planner 已在 opt-in 开关下把真实 SQL 查询的投影（含多列、常量、一元、透传）下发为 Arrow compute 阶段，`arrowProjectionProcessor` 端到端执行正确；默认关闭时行为不变。

### 11.7 filter / agg / join 深统一（本轮新增）

在前序「投影接入 planner」的基础上，把三个热点算子 filter（过滤）、agg（聚合）、join（连接）也以同样的 **opt-in 集群设置 + `ProcessorCoreUnion` 承载 JSON 计划 + 真实 SQL 端到端验证** 模式深统一到 Arrow。三个新开关均默认 `false`，关闭时行为完全不变。

#### 11.7.1 新增集群设置

| 设置 | 作用 |
|------|------|
| `sql.arrow_filter.enabled` | 把可计算的布尔过滤表达式下发给 Arrow compute |
| `sql.arrow_aggregator.enabled` | 把可计算的聚合下发给 Arrow（Go 侧规约，见 11.7.5） |
| `sql.arrow_join.enabled` | 把 inner/left equi-join 下发给自建 Arrow hash join |

`ProcessorCoreUnion` 新增 `ArrowFilter` / `ArrowAggregator` / `ArrowJoin`（`*execinfrapb.Expression`，`Expr` 承载 JSON 计划）。`physicalplan` 包导出 `ArrowFilterEnabled` / `ArrowAggregatorEnabled` / `ArrowJoinEnabled(evalCtx)`。

#### 11.7.2 filter 深统一

- **planner 分流点（两条路径）**：
  1. `AddRelationalFilter`（即 `filterNode` 路径）：当当前 post 为 identity（filter 空、无 render、无 offset/limit）且过滤可计算时，加 `ArrowFilter` 阶段（`p.buildArrowFilterNode` → JSON）。
  2. 单表扫描的过滤会被优化器折入 `TableReader` 的 `PostProcessSpec.Filter`（`initTableReaderSpec`）。故在 `createTableReaders` 中用 `InterceptArrowFilterForScan` 拦截：若 `n.filter` 可计算且非 virtual 表，清掉 TableReader post 的 filter，新增 `ArrowFilter` 阶段（加入 `IsVirtualTable` 守卫，避免干扰 introspection 查询）。
- **表达式覆盖**：比较 `EQ/LT/GT/LE/GE/NE` + 逻辑 `And/Or/Not`，操作数除列 / 同族常量外，支持嵌套二元算术 `Plus/Minus/Mult/Div`（`add/subtract/multiply/divide`），常量 leaf 必须显式 `Col:-1`（见 11.6.6 的踩坑）。
- **processor**：`arrowFilterProcessor.compute` 把输入转为 Arrow Record → `NewArrowFilter`（递归布尔树 `and/or/not/比较`，比较经 `compute.CallFunction` 取 mask → 选行索引）→ `arrowRecordToEncDatumRows`。因 filter 是列的恒等变换，Record 携带全部输入列；先按输入类型解码，再经 `p.Out.ProcessRow` 施加本阶段的后处理（投影 / offset / limit）得到最终输出行。（注意 `p.Out.ProcessRow` 返回的是可复用 buffer，存行前须 `copy`。）

#### 11.7.3 agg 深统一

- **planner 分流点**：`distsql_physical_planner.go` 的 `addAggregators` 单组路径（无分组列、或单 router、或无需分布式）前置判断 `ArrowAggregatorEnabled && canArrowAggregate(spec, p.ResultTypes, engine)`；`canArrowAggregate` 仅放行 `SUM/MIN/MAX`（限 int/float 列）、`COUNT`/`COUNT_ROWS`、分组列须受支持类型、无 `DISTINCT`、无 `ON` 谓词。命中则下 `ArrowAggregator` 阶段。
- **processor**：`arrowAggregatorProcessor.compute` 把全量输入攒成 Arrow Record → `NewArrowAggregator`。`eval` 把整个 Record 交给 `arrow_aggregate.go` 里的纯 Arrow 聚合 kernel 层（**参照 C++ `arrow/cpp/src/arrow/compute/kernels/aggregate_basic.cc` 与 `hash_aggregate.cc` 实现**，补齐 vendored Arrow v17 缺失的 `arrow/compute/aggregate` 包）：
  - `ScalarAggregator`（接口 `Consume` / `MergeFrom` / `Finalize`）是标量聚合与分组聚合共用的构建块，直接作用在 `arrow.Array` 上（非 Go 行），对应 C++ `ScalarAggregator`；
  - 累加器类型遵循 C++ `FindAccumulatorType`：整数列升位到 `int64`、浮点列升位到 `float64`、布尔 `SUM` 计 true 个数；
  - null 语义遵循 C++ `ScalarAggregateOptions(skip_nulls=true, min_count=1)`：规约时跳过 null，但 `SUM/MIN/MAX/MEAN` 在"一个非 null 都没观察到"时 finalize 为 null 标量；`COUNT(col)` 只计非 null、`COUNT(*)` 计行数；
  - `hashAggregator` 对应 C++ `HashAggregateFunction`：按分组列 hash 出各组，每组维护一组 `ScalarAggregator`（每 `(group, agg)` 一个），把每组的取值片段 `take` 出来喂给 kernel，finalize 出每个分组一行；
  - `count_all`：`scalar.NewInt64Scalar(len(groupRows))`；
  - 分组列透传：`take` 每组首行；
  - 全局聚合（`GroupCols` 空）把**所有输入行**放进唯一组（初版漏掉导致空组 panic，已修）。
  - 同样按输入类型解码后 `p.Out.ProcessRow` 施加后处理。

#### 11.7.4 join 深统一

- **planner 分流点**：`distsql_physical_planner.go` 建哈希 join 核处，若 `!leftMergeOrd.Columns` 且 `ArrowJoinEnabled && canArrowJoin(engine, leftEq, rightEq, joinType, onExpr, leftTypes, rightTypes)`，下 `ArrowJoin`；否则回退 hash/merge join。`canArrowJoin` 仅放行 inner/left 等值连接、连接键须受支持类型、无 `ON` 谓词。
- **processor**：`arrowJoinProcessor.compute` 分别 `readAll` 两输入（各自攒成单 Record），`NewArrowJoin` 在右表等值键上建 hash map，探左键产出 (inner 命中 / left 未命中发 null)；输出为「左列 + 右列」拼接的 Record。因输出列数 = 左 + 右，先按「左 + 右全类型」解码，再经 `p.Out.ProcessRow` 投影到 `SELECT` 需要的列。`gatherColumn` 对 `idx==-1`（left outer 未命中）发 null。

#### 11.7.5 关键正确性点 / 踩坑

- **投影不匹配（filter/join 共用）**：Arrow 算子输出的是「全部列」的恒等变换，而 `SELECT` 投影被 planner 作为后处理挂在该阶段上。若直接 `arrowRecordToEncDatumRows(p.Out.OutputTypes, rec)` 解码，`p.Out.OutputTypes` 已被投影裁剪（列数 < rec 列数），会 `index out of range`。正确做法：先按**输入/全列类型**解码成全列行，再经 `p.Out.ProcessRow` 施加阶段后处理；`Next` 直接返回已投影行（不可再调 `ProcessRowHelper`，否则重复施加）。
- **`ProcessRow` 可复用 buffer**：`ProcOutputHelper.ProcessRow` 返回内部 `outputRow` 复用缓冲，循环内 `append(out, processed)` 会把所有行指向同一底层数据（末行覆盖），须 `copy` 后再保留。
- **常量 leaf 的 `Col:-1`**：同投影踩坑；否则常量被误当列。
- **Arrow 算术函数名**：v17 为 `add/subtract/multiply/divide`（非 `add/sub/mul/div`）。
- **vendored Arrow 未注册聚合 kernel**：`compute.CallFunction("sum"/"min"/"max"/"count")` 报 `function 'sum' not found`。改为在 Go 侧对 `take` 出的组内数组做 sum/min/max/count 规约（Int64/Int32/Float64/Float32）。
- **Arrow 标量 API**：`scalar.MakeNullScalar(dt)`（非 `NewNullScalar`）、`s.IsValid()`（非 `IsNull`）、`scalar.String.Value` 为 `*memory.Buffer`（`string(.Value.Bytes())`）。
- **null 解码**：`arrowRecordToEncDatumRows` 须对 `arr.IsNull(i)` 置 `tree.DNull`（left join 未命中右表列时原返回 0，已修为 -9999 哨兵对应）。
- **双释放**：`takeResultArray(res)` 内部已 `res.Release()`；原先紧接着再 `res.Release()` 造成 Arrow 引用计数双释放并触发 nil 指针 panic（已删）。

#### 11.7.6 验证结果

新增 `pkg/sql/rowexec/arrowpilot/e2e_test.go` 的 `TestArrowUnifyFilterAggJoin`：建表 `t(a,b)` / `j1(k,v)` / `j2(k,w)`，开启三个开关后，对真实查询同时校验结果正确性 + `ArrowFilterRunCount` / `ArrowAggRunCount` / `ArrowJoinRunCount` 递增。覆盖：

- filter：`a*2 > b`、`a+b > a*2`、`a*2 > b AND a < 5`（含计算表达式与 `And`）；
- agg：`SUM(a), COUNT(*), MIN(b), MAX(b)`、`b, SUM(a) GROUP BY b`、`b, COUNT(*) GROUP BY b`（含全局聚合与分组聚合）；
- join：`j1 JOIN j2 ON j1.k=j2.k` 与 `j1 LEFT JOIN j2 ON j1.k=j2.k`（inner 与 left outer，未命中发 null）。

**全部通过（PASS）**。默认关闭时回退原有行式/向量化路径，行为不变。

---

# 12. 整体实现方案（统一视图）

本章把上文阶段性工作收敛为一份**可直接阅读的整体方案**。方案已落地：默认全部关闭（行为与原生执行器完全一致），按算子开关开启后，投影 / 过滤 / 聚合 / 连接四类算子改由 Apache Arrow v17 compute 引擎求值。

## 12.1 目标与边界

- **目标**：在不改变 SQL 语义、不触碰执行框架主路径的前提下，把若干个热点算子"深统一"到 Arrow——即真正用 Arrow 的列式 RecordBatch 与 compute 函数求值，而不仅是数据搬运。
- **边界**：
  - 仅覆盖执行器（`rowexec`）内的算子求值；优化器、计划生成、流控、物化层不变。
  - 每个算子均为 **opt-in**：由集群设置控制，关闭时走原路径，零回归风险。
  - 算子表达式需"Arrow 可计算"，不可计算时 planner 自动回退到原生实现（见 12.5 的可计算判定）。

## 12.2 统一架构

```
                          SQL
                           │
                 distsql_physical_planner.go
                    （按算子开关分流）
      ┌───────────────┬───────────────┬───────────────┐
      ▼               ▼               ▼               ▼
  AddRendering   InterceptArrow-   buildArrowAgg-   buildArrowJoin-
  （投影）        FilterForScan      Plan（聚合）      Plan（连接）
                  （单表扫描过滤）
      │               │               │               │
      └───────┬───────┴───────┬───────┴───────┬───────┘
              ▼               ▼               ▼
        ProcessorCoreUnion（承载 JSON 计划）
   .ArrowProjection │ .ArrowFilter │ .ArrowAggregator │ .ArrowJoin
              │
              ▼
   processors.go  switch(core) ──► newArrow*Processor(...)
              │
              ▼
   ┌────────────── 共享底座（plumbing）──────────────┐
   │ • RowToArrowBatch：行 → Arrow RecordBatch         │
   │ • arrowRecordToEncDatumRows：Arrow → 行（含 null）│
   │ • p.Out.ProcessRow：施加本阶段后处理（投影/offset/limit）│
   │ • Arrow*RunCount：运行计数探针（供测试断言命中）   │
   └──────────────────────────────────────────────────┘
              │
              ▼
   Apache Arrow v17 compute（callFunction / 自建规约）
```

## 12.3 共享底座（四类算子共用）

1. **集群开关（4 个 bool setting）**：`sql.arrow_projection.enabled`（§11.4/11.6）、`sql.arrow_filter.enabled` / `sql.arrow_aggregator.enabled` / `sql.arrow_join.enabled`（§11.7.1）。均默认 `false`，由 `settings.RegisterBoolSetting` 注册；`physicalplan/physical_plan.go` 内 `arrow*Enabled(evalCtx)` + 导出 `Arrow*Enabled(evalCtx)` 供 `distsql_physical_planner.go` 读取。
2. **`execinfrapb.ProcessorCoreUnion` 新增 4 个字段**（projection / filter / aggregator / join），类型均为 `*execinfrapb.Expression`，承载**序列化后的 JSON 计划**（运行期结构见各自 `arrow_*.go`）。
3. **行 ⇄ Arrow 互转**（见 `arrow_projection_processor.go`）：
   - `RowToArrowBatch(outputTypes, rows)`：按输出类型把 `[]EncDatumRow` 编码为列式 `arrow.Record`。
   - `arrowRecordToEncDatumRows(types, rec)`：逐列解码回 `EncDatumRow`；对每个元素先做 `IsNull(i)` 判空，空值置 `tree.DNull`（这是 left join 未命中行、含 null 列的保真关键）。
4. **阶段后处理只施加一次**：Arrow 处理器在 `compute()` 内对解码后的整行调用 `p.Out.ProcessRow(ctx, row)`（投影 / offset / limit / 末尾过滤），用 `copy` 复制结果后存入 `outputRows`；`Next()` 直接发射，避免重复后处理（filter / join 均已对齐到该模式）。
5. **运行计数探针**：`ArrowProjectionRunCount` / `ArrowFilterRunCount` / `ArrowAggRunCount` / `ArrowJoinRunCount()`，每个处理器构造时 `atomic.AddInt64` 自增（分别位于各 `arrow_*_processor.go`），供端到端测试断言"确实走了 Arrow 路径"。

## 12.4 四个算子的接入点与数据流

| 算子 | planner 接入点 | 开关 | 生成阶段的方式 | 处理器（run-count 探针） |
|------|----------------|------|----------------|--------------------------|
| 投影 | `createPlanForNode` 的 `AddRendering` 分支（`physicalplan/physical_plan.go:1295`） | `sql.arrow_projection.enabled` | `canArrowRender` → `addArrowRendering` 生成 `ArrowProjection` 阶段 | `arrow_projection_processor.go`（`ArrowProjectionRunCount`） |
| 过滤 | `createTableReaders`（`distsql_physical_planner.go:1820`） | `sql.arrow_filter.enabled` | 单表扫描的 filter 被优化器折入 `TableReader.PostProcessSpec.Filter`；此处经 `InterceptArrowFilterForScan` 剥离 last-stage 的 `post.Filter`，改挂 `ArrowFilter` 阶段 | `arrow_filter_processor.go`（`ArrowFilterRunCount`） |
| 聚合 | 聚合阶段创建处（`distsql_physical_planner.go:4741`） | `sql.arrow_aggregator.enabled` | `canArrowAggregate` → `buildArrowAggPlan` → `json.Marshal` → `ArrowAggregator` core | `arrow_aggregator_processor.go`（`ArrowAggRunCount`） |
| 连接 | hash join 创建处（`distsql_physical_planner.go:5444`） | `sql.arrow_join.enabled` | `canArrowJoin`（inner / left / right / full outer equi-join）→ `buildArrowJoinPlan` → `ArrowJoin` core | `arrow_join_processor.go`（`ArrowJoinRunCount`） |

> **过滤的特殊性**：单表 `WHERE` 会被优化器折进 `TableReader` 的 `PostProcessSpec.Filter`，不会走 `AddFilter`。因此接入点是 `createTableReaders`：`InterceptArrowFilterForScan` 校验 `canArrowFilterExpr` → `buildArrowFilterNode` 建树 → `json.Marshal(arrowFilterPlan{Root})` → 清空 `post.Filter` 并 `SetLastStagePost` → 追加 `ArrowFilter` 阶段。`IsVirtualTable()` 守卫避免干扰 introspection 查询。

## 12.5 各算子的 Arrow 计算要点

- **投影（`arrow_projection.go`）**：对每组 render 表达式，按 `callFunction` 函数名映射（关键修正：`mul→multiply`、`sub→subtract`、`div→divide`、`add` 不变）调用 Arrow compute。常量 leaf 必须显式 `Col:-1`，否则会被当成列引用（`a*2` 误为 `a*a`）。
- **过滤（`arrow_filter.go`）**：递归布尔树，`Operands` 支持 `and` / `or` / `not` / 比较（`gt/lt/ge/le/eq/ne`）。每个 `leaf` 可为输入列、常量字面量，或嵌套二元算术（`arrowFilterBinary`：`add/subtract/multiply/divide`）。`compute()` 先 `RowToArrowBatch` 编码全列，再用 `callFunction` 逐条件求值，最后 `and`/`or` 归并得到布尔掩码，用 `arrow.Select` 选出命中的行。
- **聚合（`arrow_aggregator.go` + `arrow_aggregate.go`）**：vendored Arrow v17 **没有 `arrow/compute/aggregate` 包**，因此按 C++ 引擎设计在 `arrow_aggregate.go` 补齐了一层纯 Arrow 聚合 kernel：
  - `ScalarAggregator` 接口（`Consume` / `MergeFrom` / `Finalize`）直接作用在 `arrow.Array` 上，对应 C++ `ScalarAggregator`；
  - `sumAgg` / `countAgg` / `minMaxAgg` / `meanAgg` 四个具体 kernel，累加器类型遵循 C++ `FindAccumulatorType`（整数→`int64`、浮点→`float64`、布尔 `SUM` 计 true 数；**`DECIMAL`→`apd.Decimal` 任意精度累加，最终归一为 `decimal128` 输出**，与 SQL `SUM/MIN/MAX/AVG(decimal)`→`DECIMAL` 对齐）；
  - null 语义遵循 C++ `ScalarAggregateOptions(skip_nulls=true, min_count=1)`：`SUM/MIN/MAX/MEAN` 在无非 null 观察值时 finalize 为 null；`COUNT(col)` 只计非 null、`COUNT(*)` 计行数；
  - `hashAggregator` 对应 C++ `HashAggregateFunction`：按分组列 hash 出各组、每组维护一组 `ScalarAggregator`，把每组取值片段喂给 kernel；`eval` 仅负责把 Record 交给它并 finalize。
  - `mean`（`MEAN`）kernel：对**整数或 `DECIMAL` 输入**用任意精度 `apd.Decimal` 累加 `sum`、按 `tree.DecimalCtx` 求 `sum/count`，产出 **decimal128**（SQL `AVG(int)`/`AVG(decimal)` 均返回 `DECIMAL`）；对浮点输入产出 `float64`（SQL `AVG(float)` 返回 `FLOAT`）。已接入 planner 路由（`canArrowAggregate` 放行 `AVG`，`buildArrowAggPlan` 映射到 `mean`），并通过 e2e 验证。
- **连接（`arrow_join.go`）**：自建右表 hash map（`buildHashTableForJoin`），左表逐行探测；`gatherColumn` 按右表命中行号 `idx` 收集合并列，`idx=-1` 时发 `MakeNullScalar`（未命中侧保 null 而非 0）。结果列按 `left.OutputTypes()+right.OutputTypes()` 全类型解码并经 `p.Out.ProcessRow` 后处理。支持 **inner / left / right / full outer** 四类 equi-join：`eval` 分三阶段——匹配的 `(left,right)` 对、未命中左表行（right/full，右列 null）、未命中右表行（right/full，左列 null）；等值键含 NULL 的行不参与匹配（符合 SQL 语义）。

## 12.6 正确性不变量（实现中踩过的坑，已固化）

1. `p.Out.ProcessRow` 返回的是**可复用内部 buffer**，要保留结果必须先 `copy`，否则所有输出行指向同一末行。
2. 后处理只对 `outputRows` 施加一次（`compute` 内），`Next` 直接发射。
3. Arrow v17 标量接口：`scalar.MakeNullScalar(dt)`（非 `NewNullScalar`）、`s.IsValid()`（非 `IsNull`）、`scalar.String.Value` 为 `*memory.Buffer`（用 `string(.Value.Bytes())`）。
4. 过滤/投影常量 leaf 必须 `Col:-1`；比较/算术函数名须用 Arrow v17 命名（`add/subtract/multiply/divide`）。
5. `arrowRecordToEncDatumRows` 必须处理 null 元素（置 `DNull`），否则 left join 未命中行、含 null 列会错为 0。
6. 聚合分组不可为空（全局聚合须显式填满所有行），否则 `take` 空索引 panic；规约结果取后勿重复 `Release`。纯 Arrow kernel 的 null 语义须与 SQL 对齐：`SUM/MIN/MAX/MEAN` 全 null 组返回 null、`COUNT(col)` 跳过 null、`COUNT(*)` 计所有行。

## 12.7 如何启用与验证

```sql
-- 逐算子开关（默认 false）
SET CLUSTER SETTING sql.arrow_projection.enabled = true;
SET CLUSTER SETTING sql.arrow_filter.enabled    = true;
SET CLUSTER SETTING sql.arrow_aggregator.enabled = true;
SET CLUSTER SETTING sql.arrow_join.enabled       = true;
```

构建（注意：必须从 `gitee.com` 软链路径、关闭 module 模式构建，详见 §10.1）：

```bash
cd /home/sdy/go/src/gitee.com/kwbasedb/kwbase
GO111MODULE=off \
  CGO_LDFLAGS="-L$PWD/../install/lib" \
  LD_LIBRARY_PATH="$PWD/../install/lib" \
  go build ./pkg/sql/...
go test ./pkg/sql/rowexec/arrowpilot/ -run 'TestArrowUnifyFilterAggJoin|TestArrowUnifyDecimalAgg'
```

`TestArrowUnifyFilterAggJoin`（`rowexec/arrowpilot/e2e_test.go`）覆盖：投影（`a*2`、`a+b`）、过滤（`a*2>b`、`a+b>a*2`、`a*2>b AND a<5`）、聚合（全局 / 分组 `SUM/COUNT/MIN/MAX`，并新增 NULL 行验证"SUM/COUNT(col) 跳过 null、COUNT(*) 计所有行、全 null 组 SUM 为 null"）、连接（inner / left / right / full outer），并断言 `ArrowFilterRunCount` / `ArrowAggRunCount` / `ArrowJoinRunCount` 确实递增；全部 PASS。`TestArrowUnifyDecimalAgg` 覆盖：`DECIMAL` 列上的全局 / 过滤 / 按 `INT` 分组 / 按 `DECIMAL` 分组的 `SUM/MIN/MAX/AVG`，并断言 `ArrowAggRunCount` 递增。`arrow_aggregate_test.go` 另有纯 kernel 单测（`TestAggregateArray` / `TestArrowHashAggregator` / `TestArrowHashAggregatorGlobal` / `TestArrowHashAggregatorEmptyGlobal`），锁定 `sum/min/max/count/mean` 与分组、空全局聚合的 null 语义。

## 12.8 已知限制与后续

- **聚合**：vendored Arrow v17 无 `arrow/compute/aggregate`，已在 `arrow_aggregate.go` 按 C++ 引擎设计补齐纯 Arrow 聚合 kernel 层（`ScalarAggregator` + `hashAggregator`，覆盖 `sum/count/min/max` 与 `mean`），并已点亮 planner 路由在真实 `DECIMAL` 列上验证：`SUM/MIN/MAX` 与 `AVG` 均支持 `INT`/`FLOAT`/`DECIMAL` 输入，分组键也支持 `DECIMAL`（解码路径 `arrowRecordToEncDatumRows` 已支持 `*array.Decimal128`→`tree.DDecimal`，编码路径 `buildArrowColumns` 现已把 `DECIMAL` 列编码为 Arrow `decimal128`）。`AVG` 对整数/`DECIMAL` 输入产 decimal128，对浮点输入产 `float64`。
- **过滤**：`canArrowFilterExpr` 当前支持列/常量/嵌套二元算术/布尔组合；可扩展更多内置函数（如 `LIKE`、字符串函数、类型转换）。
- **连接**：已完成 inner / left / right / full outer 四类 equi-join 的 Arrow 深统一（§12.5）；后续可扩展非等值条件（需新增 `onExpr` 处理）与多键分区的极端场景。
- **投影**：已支持常见算术与比较 render；可随 Arrow compute 函数集扩充。
