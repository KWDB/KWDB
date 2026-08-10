# arrow/compute 执行框架代码走读

> 源码版本：github.com/apache/arrow-go/v18 (PkgVersion 18.7.0)
> 走读范围：`arrow/compute` 包及其子包 `arrow/compute/exec`

## 1. 框架总览与分层

`arrow/compute` 是一个与 SQL 执行模型类似的**列式计算引擎**，其调用链路自顶向下分为 5 层：

```
Function (functions.go)        // 面向用户/注册表的抽象，负责 kernel 分发
   │ DispatchBest(...)
   ▼
Kernel (exec/kernel.go)        // 类型签名 + ExecFn + Null/Mem 策略
   │
   ▼
KernelExecutor (executor.go)   // 调度器：分块迭代、预分配、null 传播
   │ Execute(ctx, *ExecBatch, chan<- Datum)
   ▼
ExecSpan (exec/span.go)        // 零开销、非拥有的输入/输出视图
   │
   ▼
kernel.ExecFn(ctx, *ExecSpan, *ExecResult)  // 实际计算逻辑（多含 SIMD 汇编）
```

关键设计理念：**对外用 `Datum`（拥有语义 + 引用计数），对内用 `ExecSpan/ArraySpan`（非拥有视图）**，从而把执行期的 retain/release 开销降到最低（`exec/span.go:71-75` 注释明确说明这一点）。

## 2. 顶层数据载体：Datum

`Datum` 是包裹各种 Arrow 数据结构（Scalar / Array / ChunkedArray / RecordBatch / Table）的变体接口（`datum.go:60`）：

```go
type Datum interface {
    Kind() DatumKind
    Len() int64
    Equals(Datum) bool
    Release()
    data() any
}
```

- `DatumKind`（`datum.go:32-41`）：`KindScalar / KindArray / KindChunked / KindRecord / KindTable`。
- 便捷接口：`ArrayLikeDatum`（`datum.go:74`，含 `Type()/NullN()/Chunks()`）、`TableLikeDatum`（`datum.go:83`，含 `Schema()`）。
- 注意：`Datum` 是**有所有权**的，内部通过 `array.Data` 的引用计数管理生命周期，调用方需 `Release()`。

## 3. Kernel 抽象层（exec/kernel.go）

### 3.1 类型匹配：`TypeMatcher / InputType / OutputType`

kernel 用签名来描述"接受什么类型、产出什么类型"：

- `TypeMatcher`（`kernel.go:98`）：`Matches(typ)` 谓词。内置一系列实现：`SameTypeID`、`Integer()`、`BinaryLike()`、`Primitive()`、`RunEndEncoded(...)`、`TimestampTypeUnit(...)` 等（`kernel.go:104-309`）。
- `InputType`（`kernel.go:326`）：三种 `InputKind`——`InputAny / InputExact / InputUseMatcher`（`kernel.go:314`），分别用 `NewExactInput` / `NewMatchedInput` 构造。
- `OutputType`（`kernel.go:425`）：两种 `ResolveKind`——`ResolveFixed`（固定类型，`NewOutputType`）或 `ResolveComputed`（由 `TypeResolver` 函数按输入类型推导，`NewComputedOutputType`）。

### 3.2 `KernelSignature` 与分发

`KernelSignature`（`kernel.go:523`）持有 `[]InputType` + `OutType` + `IsVarArgs`，提供 `MatchesInputs`（`kernel.go:583`）做输入合法性校验，并用 `maphash` 计算哈希以支持快速匹配。变参函数（`IsVarArgs=true`）允许前 N-1 个类型逐个校验、第 N 个匹配剩余所有参数（`kernel.go:586-595`）。

### 3.3 控制策略：`NullHandling` 与 `MemAlloc`

这是框架"通用化执行"的核心旋钮：

- `NullHandling`（`kernel.go:457`）：
  - `NullIntersection`：框架自动按位 AND 所有输入 validity bitmap（默认，kernel 一般无需处理 null）。
  - `NullComputedPrealloc` / `NullComputedNoPrealloc`：kernel 自己写 bitmap（预分配/自分配）。
  - `NullNoOutput`：输出永远非 null，不分配 bitmap。
- `MemAlloc`（`kernel.go:480`）：
  - `MemPrealloc`：框架为定宽类型预分配输出数据 buffer（默认）。
  - `MemNoPrealloc`：kernel 自分配（变长类型如 Binary/List 必须自分配）。

### 3.4 `ScalarKernel` 与 `VectorKernel`

`kernel` 基类（`kernel.go:619`）含 `Init / Signature / Data / Parallelizable`。两个具体实现：

- `ScalarKernel`（`kernel.go:632`）：逐元素运算，输出长度=输入长度（广播后）。默认值 `NullIntersection + MemPrealloc + CanWriteIntoSlices=true + Parallelizable=true`（`kernel.go:655-663`）。
  - 执行入口 `Exec`（`kernel.go:672`）直接转发 `ExecFn`。
- `VectorKernel`（`kernel.go:693`）：运算依赖整列上下文（如排序、hash、top-n），输出长度可与输入不同。默认 `NullComputedNoPrealloc + MemNoPrealloc + CanExecuteChunkWise=true + OutputChunked=true`（`kernel.go:717-727`）。
  - 额外可选字段：`ExecChunked`（ChunkedArray 专用路径）、`Finalize`（多批结果后处理）、`OutputChunked`。

`ArrayKernelExec`（`kernel.go:617`）是统一的执行函数签名：

```go
func(*KernelCtx, *ExecSpan, *ExecResult) error
```

## 4. 执行期零开销数据结构（exec/span.go）

这是框架性能的关键——**视图而非拷贝**：

- `BufferSpan`（`span.go:35`）：对一个 buffer 的轻量引用，含 `Buf []byte`、`Owner *memory.Buffer`、`SelfAlloc bool`。`SetBuffer` 标记外部拥有（不增引用计数），`WrapBuffer` 标记自分配（需负责释放）。
- `ArraySpan`（`span.go:76`）：精简版 `arrow.ArrayData`，**不持有内存所有权**（除非 `SelfAlloc`）。含 `Type / Len / Nulls / Offset / Buffers[3] / Scratch[2] / Children`。
  - `SetMembers`（`span.go:488`）：非拥有引用输入 `ArrayData`——执行期间输入不得释放。
  - `TakeOwnership`（`span.go:433`）：通过 `Retain` 真正接管 buffer。
  - `MakeData`（`span.go:132`）：转回 `arrow.ArrayData`，并正确处理 `SelfAlloc` 时的引用计数。
  - `FillFromScalar`（`span.go:254`）：把 scalar 展开成长度为 1 的 `ArraySpan`（Promote）。
- `ExecValue`（`span.go:544`）：输入单元，要么 `Array ArraySpan` 要么 `Scalar scalar.Scalar`。
- `ExecResult = ArraySpan`（`span.go:562`）：kernel 填充的输出。
- `ExecSpan`（`span.go:569`）：一批 `[]ExecValue` + `Len`，是喂给 kernel 的切片视图。

> 设计要点：计算全程在操作这些非拥有视图，`MakeData()` 仅在 span 需要"固化"成对外 `Datum` 时调用一次（`executor.go:704 emitResult`、`executor.go:1088`）。

## 5. 执行调度框架（executor.go）

### 5.1 执行上下文 `ExecCtx`

`ExecCtx`（`executor.go:46`）通过 `context.Context` 传递：

- `ChunkSize`：单次迭代处理的批大小（默认 `MaxInt64`）。
- `PreallocContiguous`：是否为一整次执行预分配**连续**输出 buffer（默认 true）。
- `NumParallel`：并行 goroutine 数（默认 `runtime.NumCPU()`）。
- `Registry`：函数注册表（默认 `GetFunctionRegistry()`）。

### 5.2 执行批次 `ExecBatch`

`ExecBatch`（`executor.go:131`）是 kernel 的工作单元，含 `[]Datum Values` 与 `Len`。语义类似 RecordBatch，但常量"列"用 **Scalar** 表示（无需复制成数组）——这是与 RecordBatch 的核心区别。

### 5.3 `KernelExecutor` 接口

```go
type KernelExecutor interface {
    Init(*exec.KernelCtx, exec.KernelInitArgs) error
    Execute(context.Context, *ExecBatch, chan<- Datum) error
    WrapResults(ctx context.Context, out <-chan Datum, chunkedArgs bool) Datum
    CheckResultType(out Datum) error
    Clear()
}
```

执行结果通过 channel 流式传出，最终由 `WrapResults` 聚合成最终 `Datum`（`KernelExecutor` 定义于 `executor.go:392`）。

### 5.4 `scalarExecutor` 流程

`scalarExecutor`（`executor.go:487`）嵌入 `nonAggExecImpl`（`executor.go:418`），后者持有 `ctx / kernel / outType / dataPrealloc`。

`Execute`（`executor.go:498`）流程：

1. `iterateExecSpans` 构建分块迭代器（见 5.7）。
2. `setupPrealloc`（`executor.go:658`）根据 Null/Mem 策略决定是否预分配 validity/data buffer，并判断能否 `preallocContiguous`。
3. `executeSpans`（`executor.go:582`）：
   - 若 `preallocContiguous`：一次性 `prepareOutput(iterLen)` 大 buffer，循环切片 `output.SetSlice(resultOffset, input.Len)` 喂给各 span（`executor.go:598-622`）——避免逐块分配。
   - 否则逐块 `prepareOutput(input.Len)` 并 `emitResult`。
4. `executeSingleSpan`（`executor.go:644`）：按 `NullHandling` 调 `propagateNulls`，再调 `kernel.Exec`。

`emitResult`（`executor.go:704`）：`allScalars` 时把输出 ArraySpan 拆回 `ScalarDatum`（用 `scalar.GetScalar(arr,0)`），否则 `MakeData()` 成 `ArrayDatum` 推入 channel。

### 5.5 `vectorExecutor` 流程

`vectorExecutor`（`executor.go:886`）差异：

- **支持 `ExecChunked`**：若输入含 ChunkedArray 且 kernel 定义了 `ExecChunked`，走 `execChunked`（`executor.go:1097`）——把输入包成 `[]*arrow.Chunked`，一次性调用 kernel 的 chunk 路径。
- **支持 `Finalize`**：普通 `exec`（`executor.go:1073`）把每个 span 结果 accumulate 到 `v.results`，全部跑完后再调 `Finalize`（`executor.go:961-974`）做后处理（如排序/top-n 收敛）。
- `OutputChunked` 为 false 时 `WrapResults` 只取单个结果（`executor.go:981`）。

### 5.6 `propagateNulls` —— null 传播

`propagateNulls`（`executor.go:237`）是 `NullIntersection` 策略的实现：

- 先用 `getNullGen`（`executor.go:190`）推断每个输入的 null 概况（`allNull / allValid / perhapsNull`）。
- **短路优化**：任一输入全 null → 输出全 null（可直接复用/新分配 bitmap 写 0）。
- 0 个有 null 的输入：输出无 null。
- 1 个：直接拷贝/零拷贝该 bitmap（按 offset 判断能否 `SliceBuffer`）。
- ≥2 个：`bitutil.BitmapAnd` 逐位 AND 求交集（`executor.go:337-346`）。

### 5.7 `iterateExecSpans` —— 分块迭代器

`iterateExecSpans`（`executor.go:757`）返回 `(haveAllScalars, iterator, err)`：

- 先做长度校验（所有 array 参数必须等长，`inferBatchLength`）。
- 对 `*ScalarDatum` 填入 `span.Values[i].Scalar`；对 Array/Chunked 用 `SetMembers` 建立视图。
- 若全为 scalar 且 `promoteIfAllScalar`，`PromoteExecSpanScalars`（`span.go:634`）把 scalar 提升为长度 1 的 `ArraySpan`（统一 kernel 处理路径）。
- 返回的迭代函数（闭包，`executor.go:841`）：每次按 `min(ChunkSize, 剩余长度, 最小 chunk 长度)` 切出下一个 span，并对各 Array 输入调 `SetSlice(pos, iterSz)` 移动视图窗口。ChunkedArray 通过 `nextChunkSpan` 跨 chunk 跟踪。

### 5.8 性能细节

- `sync.Pool` 复用 `scalarExecutor` / `vectorExecutor`（`executor.go:865-873`），避免高频创建。
- 并发执行共享 kernel 时，清理逻辑使用 `ctx.State` 而非 `kernel.Data`，避免竞态（`executor.go:583-590` 注释明确说明）。

## 6. Function 与 Registry（functions.go / registry.go）

### 6.1 `Function` 接口

```go
type Function interface {
    Name() string
    Kind() FuncKind
    Arity() Arity
    Doc() FunctionDoc
    NumKernels() int
    Execute(context.Context, FunctionOptions, ...Datum) (Datum, error)
    DispatchExact(...arrow.DataType) (exec.Kernel, error)
    DispatchBest(...arrow.DataType) (exec.Kernel, error)
    DefaultOptions() FunctionOptions
    Validate() error
}
```

（`functions.go:30`）

- `FuncKind`（`functions.go:83`）：`FuncScalar / FuncVector / FuncScalarAgg / FuncHashAgg / FuncMeta`。
- `Arity`（`functions.go:46`）：`NArgs + IsVarArgs`，`Unary()/Binary()/VarArgs(n)` 等便捷构造。
- `baseFunction`（`functions.go:134`）+ 泛型 `funcImpl[KT kernelType]`（`functions.go:198`，约束 `ScalarKernel | VectorKernel`）实现大部分接口，`Execute` 内完成 `DispatchBest → Init → executor.Execute → WrapResults` 的完整调度。

### 6.2 分发 `DispatchBest`

`DispatchBest`（`funcImpl` 方法）按输入类型在 kernel 列表中选出**最匹配**的一个 `exec.Kernel`，并可能做隐式类型提升（cast），最终返回供 executor 使用的 kernel。

### 6.3 `FunctionRegistry`

`FunctionRegistry`（`registry.go:30`）管理函数名→Function 映射，支持别名（AddAlias）。全局单例 `GetFunctionRegistry()`（`registry.go:47`）在 `sync.Once` 中初始化并注册全部内置函数：

```go
RegisterScalarCast / RegisterVectorSelection / RegisterVectorSort /
RegisterScalarBoolean / RegisterScalarArithmetic / RegisterScalarComparisons /
RegisterVectorHash / RegisterVectorRunEndFuncs / RegisterScalarSetLookup
```

支持 `NewChildRegistry`（`registry.go:68`）实现可覆盖的层级注册表。

## 7. 一次标量函数调用的完整时序

```
用户调用 fn.Execute(ctx, opts, d1, d2)
  └─ funcImpl.Execute
      1. checkArity / checkOptions                 (functions.go)
      2. DispatchBest(inTypes…) → 选定 ScalarKernel (functions.go)
      3. 构造 KernelCtx + KernelInitArgs → kernel.Init 生成 state
      4. NewScalarExecutor().Init(ctx, args)       (executor.go:435，解析 outType)
      5. executor.Execute(ctx, *ExecBatch, outCh)
           ├─ iterateExecSpans → spanIterator      (executor.go:757)
           ├─ setupPrealloc                         (executor.go:658)
           ├─ executeSpans → executeSingleSpan
           │     ├─ propagateNulls (NullIntersection) (executor.go:237)
           │     └─ kernel.Exec(ctx, &span, &out)   (kernel.go:672)
           └─ emitResult → outCh                    (executor.go:704)
      6. WrapResults(outCh, hasChunked) → 最终 Datum (executor.go:521)
      7. 返回 Datum（调用方负责 Release）
```

## 8. 设计要点总结

| 维度 | 实现策略 |
|------|----------|
| **零拷贝执行** | 内部全程 `ArraySpan`/`ExecSpan` 非拥有视图，仅在产出 `Datum` 时 `MakeData()` 一次 |
| **null 处理通用化** | `NullIntersection` 默认自动 bitmap AND；支持预分配/自分配/无输出三种策略 |
| **内存预分配** | 定宽类型可 `MemPrealloc`；`PreallocContiguous` 让整段执行只分配一次大 buffer |
| **分块与流式** | `iterateExecSpans` 按 ChunkSize 切窗口；结果经 channel 流出，支持 ChunkedArray 输出 |
| **Kernel 复用** | `sync.Pool` 复用 executor；并发时 kernel state 存于 `ctx.State` 避免竞态 |
| **类型分发** | `KernelSignature` + `maphash` 快速匹配；`DispatchBest` 支持隐式 cast |
| **可扩展性** | 新增函数=实现 `Function`+若干 `Kernel` 并 `AddFunction`；聚合/哈希另走 `aggregate` 子包 |
