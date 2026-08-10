# Parquet 模块代码走读报告

> 源码版本：github.com/apache/arrow-go/v18 (PkgVersion 18.7.0)
> 走读范围：`parquet` 包及其子包（`file` / `schema` / `metadata` / `pqarrow` / `internal/encoding` 等）

## 1. 模块定位与整体架构

`parquet` 是 Apache Parquet 列存格式的**纯 Go 原生实现**（`parquet/doc.go:17-24`），不依赖 cgo。它独立于 `arrow` 包（仅类型映射时引用），通过 `pqarrow` 子包桥接 Arrow。

分层（自顶向下）：

```
parquet/        顶层类型、常量、读写属性 (types.go, reader_writer_properties.go)
  ├─ schema/     Schema 树：Node / GroupNode / PrimitiveNode / LogicalType
  ├─ metadata/   Thrift 元数据：FileMetaData / ColumnChunkMetaData / 统计 / PageIndex
  ├─ file/       文件 IO：Reader / Writer / RowGroup / Column / Page 读写器
  ├─ pqarrow/    Arrow ⇄ Parquet 转换桥
  └─ internal/
       ├─ encoding/  编码：Plain / Dict / Delta* / ByteStreamSplit / Levels
       ├─ thrift/     Thrift 协议编解码
       ├─ gen-go/     parquet.thrift 生成的 Go 代码（//go:generate）
       ├─ utils/      位打包、RLE、压缩 reader/writer
       ├─ compression/ 压缩码
       └─ bmi/        位操作 SIMD
compress/        压缩 codec 注册
```

## 2. 顶层类型与常量（parquet/types.go）

为隔离 Thrift 依赖，所有枚举均是对 `format.*` 的类型别名/重定义：

- `Type`（`types.go:169-186`）：8 种物理类型 `Boolean/Int32/Int64/Int96/Float/Double/ByteArray/FixedLenByteArray`，通过 `Types` 常量组访问（`types.go:261`）。
- `Encoding`（`types.go:290`）：`Plain/PlainDict/RLE/RLEDict/DeltaByteArray/DeltaBinaryPacked/DeltaLengthByteArray/ByteStreamSplit`。
- `Repetition`（`types.go:325`）：`Required/Optional/Repeated`（嵌套 shredding 的核心）。
- `Version`（`types.go:220`）：`V1_0/V2_4/V2_6`，控制 uint32/nanos 等特性的写入。
- `DataPageVersion`（`types.go:247`）：`DataPageV1/V2`。

**three 关键 Go 类型**（零拷贝 `unsafe` 转换）：

- `ByteArray`/`FixedLenByteArray`（`types.go:108,136`）：仅是 `[]byte`，提供 `Traits.CastFromBytes` 直接 reinterpret（`types.go:130` 等）。
- `Int96`（`types.go:70`）：12 字节时间戳（64 位纳秒 + 32 位儒略日），`ToTime` 做日期换算（`types.go:84`）。

**Arrow⇄Parquet 类型映射表**（`doc.go:86-128`）是桥接层的核心依据，例如 `Int8→Int32(Int(8,signed))`、`String→ByteArray(String)`、`List/Struct/Map→Group`；不支持 `DURATION/UNION/VIEW/DECIMAL32/64` 等（返回 `arrow.ErrNotImplemented`）。

## 3. Schema 层（schema/）

Parquet 的 schema 是一棵**节点树**，用访问者模式处理：

- `Node` 接口（`node.go:41`）：`Primitive/Group` 两类（`node.go:33`），含 `Name/RepetitionType/ConvertedType/LogicalType/Parent/Path/Visit`。
- `Visitor`（`node.go:64`）：`VisitPre/VisitPost` 遍历树，可在 Group 子节点处剪枝。
- `Schema` 容器（`schema.go:55`）：持有 root，`leaves []*Column`、`nodeToLeaf/leafToBase` 映射，以及每叶子列的 **最大 repetition/definition level**——这是从 shredding 编码重建嵌套结构的依据（`schema.go:48-54`）。
- `FromParquet`（`schema.go:65`）：把 Thrift `SchemaElement` 序列递归还原成 Node 树。
- 辅助：`logical_types.go`/`converted_types.go`（逻辑类型注解）、`reflection.go`（用 struct tag 反射生成 schema）、`helpers.go`。

## 4. 文件格式层（file/）

### 4.1 物理布局

`PAR1` 魔数 + 若干 RowGroup（每列一个 ColumnChunk，含若干 DataPage）+ Footer(`FileMetaData`) + 4 字节 footer 长度 + `PAR1`。`footerSize=8`（`file_reader.go:36`，= 4 长度 + 4 魔数）。

### 4.2 读路径 `Reader`（`file/file_reader.go:46`）

- `OpenParquetFile`（`file_reader.go:80`）：可选 `memoryMap`（`mmapOpen`）加速随机读。
- `NewParquetReader`（`file_reader.go:103`）：`parseMetaData()` 读 Footer；维护 `bufferPool sync.Pool`（带 `SetFinalizer` 自动 Release，`file_reader.go:113-121`）；构造 `pageIndexReader`（`file_reader.go:129`）与 `bloomFilterReader`，支持 `fileDecryptor` 解密。
- 分模块：`row_group_reader.go`、`column_reader.go`（泛型生成 `column_reader_types.gen.go`）、`page_reader.go`、`record_reader.go`、`level_conversion.go`（def/rep level→Arrow 偏移）。

### 4.3 写路径 `Writer`（`file/file_writer.go:33`）

- `NewParquetWriterWithError`（`file_writer.go:101`）：优先返回 error（历史 `NewParquetWriter` 会 panic "failed to write magic number"，保留兼容，`file_writer.go:81-89`）。
- 持有 `metadata.FileMetaDataBuilder`（`file_writer.go:118`）、`rowGroupWriter`、`pageIndexBuilder`、`bloomFilters`、`fileEncryptor`。
- 分模块：`row_group_writer.go`、`column_writer.go`（泛型生成）、`page_writer.go`、`size_statistics.go`。

## 5. 编码层（internal/encoding/）

编码对每种物理类型实现 `Encoder`/`Decoder` 接口（`typed_encoder.go`、`encoder.go`、`decoder.go`）：

- **Plain**：`plain_encoding_types.go`，`TypedEncoder`（`typed_encoder.go`）按类型特化。
- **Dictionary**：基于 `memo_table.go` 的哈希记忆表去重；模板生成 `memo_table_types.gen.go`。
- **Delta 系列**：`delta_bit_packing.go`（整数）、`delta_byte_array.go`、`delta_length_byte_array.go`（ByteArray）。
- **ByteStreamSplit**：浮点专用，含 AVX2/NEON 汇编（`byte_stream_split_decode_avx2_amd64.s`、`..._neon_arm64.s`）与大小端回退（`byte_stream_split_big_endian.go` 等）。
- **Levels**：`levels.go` 用 RLE 编码 definition/repetition levels（嵌套结构的关键）。
- `streaming/` 子包：流式解码器。

## 6. 元数据与 Thrift（metadata/, internal/gen-go, internal/thrift）

- `parquet.thrift` 经 `//go:generate thrift -o internal -r --gen go ../parquet.thrift`（`types.go:132`）生成 `internal/gen-go/parquet`。顶层包的类型枚举是对 `format.*` 的别名，从而避免对外暴露 Thrift。
- `metadata/`：管理 `FileMetaData`、`ColumnChunkMetaData`、`ColumnIndex/OffsetIndex`（PageIndex）、`BloomFilter`、`statistics`。`file_reader.go` 的 `pageIndexReader`/`bloomFilterReader` 即基于此。
- `internal/thrift/`：Thrift 紧凑二进制协议编解码。

## 7. 压缩（compress/ + internal/utils）

`internal/utils` 提供压缩 reader/writer 封装，`compress/` 注册 codec（Snappy/Gzip/Zstd/LZ4/Brotli 等），在 `page_writer.go`/`page_reader.go` 中对 DataPage 透明压缩。

## 8. Arrow 桥接层（pqarrow/）

这是 Arrow 用户最常接触的入口：

- `FileReader`（`pqarrow/file_reader.go:82`）：包装 `*file.Reader`，构造 `SchemaManifest`（`NewSchemaManifest`，`file_reader.go:96`），把 Parquet schema 映射成 Arrow schema（`FromParquet`，`file_reader.go:111`）。
  - `ReadTable`（`file_reader.go:63`）一步读成 `arrow.Table`；底层 `column_readers.go` 按列并行解码（用 `errgroup`，`file_reader.go:37`）。
- `FileWriter`：`encode_arrow.go` 把 Arrow 数组写成 Parquet；`encode_dict_compute.go`/`encode_dict_nocompute.go` 两条字典化路径（compute 加速与否）。
- `schema.go`：Arrow⇄Parquet schema 双向映射（依据 `doc.go` 映射表）。
- `path_builder.go`、`properties.go`、`variant/`：Variant 类型支持。

## 9. 读路径端到端

```
OpenParquetFile → NewParquetReader
  ├─ parseMetaData (读 footer, 含 schema + 各列 Chunk 偏移/统计)
  ├─ 构造 pageIndexReader / bloomFilterReader
pqarrow.NewFileReader → SchemaManifest (schema 映射)
  └─ ReadTable / ReadRowGroup / ReadColumn
       └─ columnIterator → column_readers.go
            ├─ page_reader.go 按 ColumnChunkMetaData seek 到页
            ├─ internal/encoding 解码 (Plain/Dict/Delta/...)
            ├─ level_conversion.go 把 def/rep level 还原成 Arrow offsets
            └─ 组装 arrow.Array 并预分配 (binary_prealloc)
```

## 10. 写路径端到端

```
NewParquetWriter(schema) → metadata.FileMetaDataBuilder + 写 PAR1
  └─ AppendRowGroup → AppendColumn / WriteArrow
       └─ column_writer.go
            ├─ encode_arrow.go 转成物理列 + def/rep levels
            ├─ internal/encoding 编码 (阈值触发 dict fallback)
            ├─ compress 压缩 DataPage
            └─ page_writer.go 写页 + 累计 ColumnChunkMetaData
  └─ Close: 写 Footer (FileMetaData) + PageIndex + BloomFilter + 长度 + PAR1
```

## 11. 设计要点总结

| 维度 | 实现特征 |
|------|----------|
| **Thrift 隔离** | 顶层类型是对 `format.*` 的别名/重定义，避免对外暴露 Thrift |
| **零拷贝类型** | `ByteArray/Int96` 用 `unsafe` reinterpret，减少中间分配 |
| **Schema 树** | `Node`+`Visitor` 模式，`Schema` 预计算 max def/rep level 与列描述符 |
| **代码生成** | `column_reader/writer_types.gen.go`、`memo_table_types.gen.go` 由 `*.tmpl` 生成；Thrift 由 `parquet.thrift` 生成 |
| **SIMD 加速** | ByteStreamSplit 的 AVX2/NEON 汇编 + 大小端回退 |
| **嵌套支持** | def/rep levels（`levels.go` / `level_conversion.go`）实现 record shredding |
| **Arrow 桥接** | `SchemaManifest` + 双向 schema 映射；列并行解码（`errgroup`） |
| **可扩展** | `compress/` 注册 codec；编码按类型特化接口 |
