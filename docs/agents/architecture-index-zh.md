# 架构索引

本文为 [`architecture-index.md`](architecture-index.md) 的中文对照稿。

本文为 agent 可用的仓库架构地图：回答「应先去哪里看」，不复述完整设计文档。

## 顶层模块索引

### `common/`

- **用途：** 在各引擎侧组件间复用的 C/C++ 公共基础设施。
- **主要职责：** 日志；错误栈与编码映射；线程；追踪；闩锁；内存辅助；通用工具。
- **常见改动类型：** 日志行为；错误传递；共享工具；底层运行时修补。
- **高风险区：** 全进程日志表现；错误 JSON 外形；线程生命周期；被多个模块使用的底层助手。
- **关键目录：** `common/src/log/`、`common/src/error/`、`common/src/thread/`、`common/src/trace/`

### `kwbase/`

- **用途：** Go 侧数据库逻辑与用户可见的数据库能力面。
- **主要职责：** SQL、优化器/计划器、执行器、KV/服务、CLI、负载、配置、作业、测试工具及运维接口。
- **常见改动类型：** SQL 语义、DDL/DML 行为、服务端配置、CLI 行为、逻辑测试。
- **高风险区：** SQL 兼容性、存储与 KV 行为、集群版本语义、升级路径对外的错误外形。
- **关键目录：** `kwbase/pkg/sql/`、`kv/`、`server/`、`cli/`、`testutils/`、`workload/`

### `kwdbts2/`

- **用途：** C/C++ 时序存储与引擎实现。
- **主要职责：** 引擎、存储、执行、统计、mmap/brpc 等依赖、共享 TS 运行时。
- **常见改动类型：** 存储行为；执行流水线；引擎优化；TS 查询/运行时修补。
- **高风险区：** 持久化；所有权与内存；并发；TS 执行正确性；性能热点。
- **关键目录：** `engine/`、`storage/`、`exec/`、`statistic/`、`ts_engine/`、`common/`

### `qa/`

- **用途：** 测试脚手架与回归/性能工具。
- **主要职责：** 集成脚本、本地回归编排、TSBS、性能套件、初始化/ teardown 脚本。
- **常用入口：** `qa/run_test_local_v2.sh`、`qa/run_test_v2.sh`、`qa/run_tsbs_test.sh`

## 常见任务路由

- **SQL 语义变更：** 自 `kwbase/pkg/sql/` 起，再结合相关逻辑测试与 server/session 代码。
- **Server / CLI 行为：** 查 `kwbase/pkg/server/` 或 `cli/`。
- **时序执行或存储：** 查 `kwdbts2/exec/`、`storage/`、`engine/` 及对应测试。
- **日志或错误行为：** 先查 `common/src/log/` 与 `common/src/error/`。
- **回归或集成缺陷：** `qa/Integration/`、`qa/util/` 及对应 `make regression-test` 入口。
- **性能问题：** 相关运行时模块，外加 `qa/perf_test/`、`qa/tsbs_test/`。

## 评审边界

- 跨 `kwbase` 与 `kwdbts2` 的语义变更须有显式兼容性评审。
- 变更 `common/src/log/`、`common/src/error/` 往往影响面广，应按「共享表面」修改处理。
- 若改变 SQL 输出、协议行为、错误码或持久化行为，验证范围应大于局部单测。
- 性能改动应附工作负载证据，而非仅静态阅码。

## 相关引用

（路径与英文名与英文原版一致）：`AGENTS.md`、`testing-flow.md`、`logging.md`、`errors.md`、`pr-guide.md`。
