# Agent 文档概览（中文版）

本文为 [`README.md`](README.md) 的中文镜像，对应文件名 **`README-zh.md`**。

- **`-zh`**：语言后缀（与仓库里 `*-suffix.md` 的短横线风格一致）。
- **`-cn`**：多表示国家/地域或交付圈层，不推荐用来标记「中文版文档」。
- 若以后要细到「简体中国区」，可考虑 `*.zh-CN.md`（需单独约定）。

本目录存放面向编码代理（agents）的稳定知识库正文；易变的需求规格与在日常协作系统里的任务文档仍驻留在仓库外。

采纳路径：先从仓库根的 `AGENTS.md` 与本目录所列文件起步。建议阅读顺序为 **`AGENTS.md` → `architecture-index.md` → `testing-flow.md` → 用 `resource-index.md` 按需进入** `logging.md`、`errors.md`、`pr-guide.md`、`coding-style-go-cpp.md` 等。若在 `.agents/skills/` 下封装可重复的流程技能，应保持可选，仅在流程被多次验证值得一包再说（参见 `.agents/skills/README.md`）。

带 `-zh` 后缀的中文版与同名英文正文对应。**不含 `-zh` 的英文文件仍是开源场景的默认权威条目**。

下列文件与英文 [`README.md`](README.md) 中 **Chinese-language companions** 对照表一致：`README-zh.md`、`resource-index-zh.md`、`architecture-index-zh.md`、`testing-flow-zh.md`、`ai-collaboration-model-zh.md`、`coding-style-go-cpp-zh.md`、`pr-guide-zh.md`、`logging-zh.md`、`errors-zh.md`、`ci-release-guide-zh.md`。

## 目录布局

结构与英文 [`README.md`](README.md) 中的 `Directory Layout` 代码块一致；维护时请同步更新中英文两份。

## 分层约定

### `AGENTS.md`

- 适用于绝大多数任务的仓库级规则。
- 保持短小、稳定、偏策略。
- 不要存放易变需求细节或单次任务计划。

### `docs/agents/*`

- 面向 agents 的陈述性知识。
- 适合：架构、不变量、评审边界、测试面、日志/错误兼容性、CI/发布期望、组件地图。
- 不适合：更应做成 skill 的逐任务清单。

### `.agents/skills/*`

- 可重复工作流的程序性知识。
- 团队在确认流程足够稳定并有明确边界前，可先仅维护候选清单。

### 外部需求文档

- 详细的缺陷、特性、性能规格应在仓库之外维护。
- 权威工件应位于团队认可的 issue tracker、wiki 等系统；issue、PR、变更摘要中须带可访问链接。

## 当前文档条目

与各文件英文名对照，请参阅英文 [`README.md`](README.md) 之 **Current Docs**（此处不重复罗列，以免双份漂移）。

## 设计立场与待补缺口

与英文 [`README.md`](README.md) 中的 **Design Posture**、**Remaining Gaps** 对应；维护时以英文条为准更新，再同步本中文版。
