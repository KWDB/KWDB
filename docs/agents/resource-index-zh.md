# Agent 资源索引

本文为 [`resource-index.md`](resource-index.md) 的中文版 **`resource-index-zh.md`**。

本文件是本仓库中与 agent 相关资源的稳定索引。

## 核心文件

- `AGENTS.md`：仓库级运行规则与稳定验证期望
- `.agents/skills/README.md`：仓库内工作流技能的候选清单

## 外部任务工件

- 详细的缺陷、特性、性能规格说明应存放在本仓库之外。
- 请以 issue tracker、wiki 或其他团队认可的系统为权威来源。
- 在每个 issue、PR 或变更摘要中给出指向该权威工件的链接。

## 参考文档

- `docs/agents/README.md`：agent 文档与技能目录布局的总览
- `docs/agents/*-zh.md`：重要条目的中文对应版；开源默认仍以英文正文为准
- `docs/agents/architecture-index.md`：模块地图与任务路由提示
- `docs/agents/coding-style-go-cpp.md`：面向 KWDB 维护的 OSS 代码的 Go/C++ 约定
- `docs/agents/ai-collaboration-model.md`：人机协作的阶段、门禁与度量
- `docs/agents/ci-release-guide.md`：当前可用的验证面与推荐的 CI/发布门禁
- `docs/agents/testing-flow.md`：验证层级与常用命令
- `docs/agents/logging.md`：共享日志约束与评审清单
- `docs/agents/errors.md`：错误码及兼容性约束
- `docs/agents/pr-guide.md`：可供评审的 PR 摘要要求
- `docs/agents/components/README.md`：组件级注释组织方式

## 仓库内其他参考

- `README.md`：仓库介绍、构建前提与概要搭建说明
- `Makefile`：根目录构建、单元测试、逻辑测试与回归入口
- `kwbase/CONTRIBUTING.md`：上游 KaiwuDB 贡献流程入口
- `qa/perf_test/README.md`：当前性能脚手架说明
- `qa/run_test_local_v2.sh`：本地回归入口脚本
- `qa/run_tsbs_test.sh`：TSBS 测试入口脚本

## 技能目录

- `.agents/skills/README.md`：未来仓库级技能的候选说明

## 模块级参考路径

- `common/src/log/`：共享日志实现
- `common/src/error/`：共享错误实现
- `kwbase/pkg/`：Go 侧能力与 SQL 实现
- `kwdbts2/`：时序与引擎侧实现
- `qa/`：回归与性能脚手架
