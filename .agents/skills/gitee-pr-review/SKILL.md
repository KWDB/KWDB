---
name: gitee-pr-review
description: |
  Use when needing to review Pull Requests on Gitee kwdb/kwdb repository, including reviewing all open PRs,
  checking PR status in batch, or requesting AI-generated code review reports for specific PRs.
  This skill fetches PR lists via MCP, analyzes code changes, and posts formatted review reports to PR comments.
---

# Gitee PR Review Skill

本 skill 自动对 Gitee 上 kwdb/kwdb 仓库的 Pull Request 进行代码 review，生成格式化的 review 报告并发布到 PR 评论区。

## 前提条件

1. **MCP Server**: 需要配置并启用 `mcp-gitee` MCP server
2. **Git 仓库**: kwdb 代码仓库根目录为 `.agents/skills` 的父目录（即 `.agents/../`）
3. **关联 Skill**: **REQUIRED SUB-SKILL:** `code-review-expert` skill 用于专家级代码 review

## 核心工作流程

```
获取 PR 列表 → 展示列表并确认 → 用户选择要 review 的 PR → 逐个处理 → 发布评论
```

### Phase 1: 获取 PR 列表

使用 `mcp__gitee__list_repo_pulls` 获取 kwdb/kwdb 仓库的 open PRs：

```
mcp__gitee__list_repo_pulls(
  owner: "kwdb",
  repo: "kwdb",
  state: "open"
)
```

### Phase 2: 展示列表并确认

获取 PR 列表后，向用户展示所有 open PR 的摘要信息：

| # | 标题 | 作者 | 源分支 → 目标分支 | 创建时间 |
|---|------|------|-------------------|----------|
| 1593 | xxx | author1 | feature-xxx → master | 2026-05-01 |
| 1590 | yyy | author2 | fix-yyy → master | 2026-05-10 |
| ... | ... | ... | ... | ... |

然后询问用户：**"请选择要 review 的 PR 编号（如 1593），或输入 'all' review 所有 PR"**

### Phase 3: 处理用户选择的 PR

根据用户选择处理：
- **用户指定编号**: 只 review 指定的 PR
- **用户输入 'all'**: review 所有 open PRs

对于用户选择的每个 PR，按照以下步骤处理：

#### Step 3.1: 获取 PR 详情

使用 `mcp__gitee__get_pull_detail` 获取 PR 的详细信息：

```
mcp__gitee__get_pull_detail(
  owner: "kwdb",
  repo: "kwdb",
  number: {PR编号}
)
```

获取的信息包括：
- PR 标题和描述
- 源分支 (head) 和目标分支 (base)
- 作者信息
- 创建时间
- PR 状态

#### Step 3.2: 拉取分支到本地

在 kwdb 代码仓库中拉取源分支和目标分支：

```bash
cd {KWDB_REPO_ROOT}  # 即 .agents/skills 的父目录
git fetch origin {source_branch}:{source_branch}
git fetch origin {target_branch}:{target_branch}
```

#### Step 3.3: 获取 PR 修改的文件

使用 `mcp__gitee__get_diff_files` 获取修改的文件列表：

```
mcp__gitee__get_diff_files(
  owner: "kwdb",
  repo: "kwdb",
  number: {PR编号}
)
```

#### Step 3.4: 分析代码变更

在本地使用 `git diff` 对比源分支和目标分支：

```bash
cd {KWDB_REPO_ROOT}
git diff {target_branch}...{source_branch} > /tmp/pr_{PR编号}_diff.txt
git diff --stat {target_branch}...{source_branch}
```

分析内容：
- 修改了哪些文件
- 变更的类型（新增、修改、删除）
- 变更的目的是什么（修复 bug/新功能/性能优化等）
- 具体是如何实现的

#### Step 3.5: 调用 code-review-expert 进行 Review

使用 `Skill` 工具调用 `code-review-expert` skill：

```
Skill("code-review-expert")
```

在调用时提供：
- PR 的标题和描述（了解变更目的）
- 代码 diff 内容（通过 git diff 获取）
- 修改的文件列表

#### Step 3.6: 生成格式化 Review 报告

按照 `references/PR_REVIEW_REPORT_TEMPLATE.md` 模板生成报告：

报告结构：
```
# PR Review Report: #{PR编号} - {PR标题}

## PR 信息
- 编号: #{PR编号}
- 作者: {AUTHOR}
- 分支: `{SOURCE_BRANCH}` → `{TARGET_BRANCH}`
- Review 时间: {REVIEW_TIMESTAMP}

## Code Review 结果
变更统计: {FILES_CHANGED} 文件, +{LINES_ADDED}/-{LINES_DELETED}
整体评估: [APPROVE / REQUEST_CHANGES / COMMENT]

## Findings
### P0 - Critical
(none 或问题列表)

### P1 - High
...

### P2 - Medium
...

### P3 - Low
...

## Removal/Iteration Plan
(如适用)

## Additional Suggestions
(可选改进建议)

---
*自动生成 by AI | code-review-expert skill*
```

#### Step 3.7: 发布评论到 PR

使用 `mcp__gitee__create_comment` 将 review 报告发布到 PR 评论区：

```
mcp__gitee__create_comment(
  owner: "kwdb",
  repo: "kwdb",
  number: {PR编号},
  body: {review报告内容},
  resource_type: "pull"
)
```

## 错误处理

### Git 拉取失败
- 如果分支拉取失败，记录错误并继续处理下一个 PR
- 在报告中标注该 PR 分析不完整

### PR 内容获取失败
- 如果无法获取 PR 详情，跳过该 PR
- 记录失败原因

### Review 失败
- 如果 code-review-expert 调用失败，使用简化版 review
- 确保报告仍能生成并发布

## 输出确认

处理完所有 PR 后，向用户汇报：
1. 处理了多少个 PR
2. 每个 PR 的 review 状态（成功/失败）
3. 失败的原因（如有）

## 示例对话

**用户**: "帮我 review 一下 kwdb 仓库的 PRs"

**助手**:
1. 调用 `mcp__gitee__list_repo_pulls` 获取 open PR 列表
2. 展示 PR 列表供用户选择
3. 用户选择要 review 的 PR（或输入 'all'）
4. 按用户选择处理

**用户**: "review PR #1593"

**助手**:
1. 获取 PR #1593 的详情
2. 进行完整的 review 流程
3. 发布评论到 PR #1593

## 参考文件

| 文件 | 路径 | 说明 |
|------|------|------|
| Review 报告模板 | `references/PR_REVIEW_REPORT_TEMPLATE.md` | 代码 review 报告的格式模板 |
| code-review-expert | 已安装的 skill | 专家级代码 review |
