# CI 与发布指南（中文版）

本文为 [`ci-release-guide.md`](ci-release-guide.md) 的中文镜像。

仓库提供本地构建与测试入口；若在托管 CI 上使用 YAML/Jenkinsfile 等，一般由基础设施在源码树之外维护。

具体构建与验证命令见 [testing-flow-zh.md](testing-flow-zh.md)。

## 验证入口

与英文原版 **Verification Surfaces** 一致：`Makefile`、`qa/` 下相关脚本、`qa/perf_test/*`。

## 按改动类型的最低期望

与英文原版 **Minimum Expectations by Change Type** 一致。

## 发布前清单

与英文原版 **Release Checklist** 一致。

## 禁止事项

与英文原版 **What Not to Do** 一致。
