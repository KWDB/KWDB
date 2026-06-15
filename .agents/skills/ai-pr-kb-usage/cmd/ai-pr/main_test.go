// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewDefaultEnvironmentUsesRepoRootOverride(t *testing.T) {
	t.Setenv("AI_PR_REPO_ROOT", "/tmp/custom-repo-root")

	env := newDefaultEnvironment()

	if env.repoRoot != "/tmp/custom-repo-root" {
		t.Fatalf("repo root mismatch: %q", env.repoRoot)
	}
	if env.defaultPRGuide != filepath.Join("/tmp/custom-repo-root", "docs", "agents", "pr-guide.md") {
		t.Fatalf("default guide mismatch: %q", env.defaultPRGuide)
	}
}

func TestRunGitIncludesStderrOnFailure(t *testing.T) {
	env := newDefaultEnvironment()

	_, err := env.runGit("rev-parse", "--verify", "definitely-does-not-exist")
	if err == nil {
		t.Fatal("expected git error")
	}
	if !strings.Contains(err.Error(), "fatal:") {
		t.Fatalf("expected stderr in error, got: %v", err)
	}
}

func TestResolvePRGuidePathRejectsOutsideRepo(t *testing.T) {
	h := newHarness(t)
	h.setenv("AI_PR_CALLER_CWD", h.repoRoot)

	stderr := &bytes.Buffer{}
	h.env.stderr = stderr
	if code := h.env.run([]string{"pr-template", "--pr-guide", "../outside/pr-guide.md"}); code != 1 {
		t.Fatalf("unexpected exit code: %d\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "path escapes repository root") {
		t.Fatalf("expected path traversal error, got:\n%s", stderr.String())
	}
}

func TestResolvePRGuidePathRejectsEnvironmentOverrideOutsideRepo(t *testing.T) {
	h := newHarness(t)
	h.setenv("AI_PR_GUIDE_PATH", filepath.Join(h.tempDir, "outside-pr-guide.md"))

	stderr := &bytes.Buffer{}
	h.env.stderr = stderr
	if code := h.env.run([]string{"pr-template"}); code != 1 {
		t.Fatalf("unexpected exit code: %d\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "path escapes repository root") {
		t.Fatalf("expected path traversal error, got:\n%s", stderr.String())
	}
}

func TestStartAndSessionCountAccumulate(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "Claude-4-Sonnet", "--tool", "Codex")
	h.run("session-add")
	h.run("session-add", "--count", "2")

	rec := h.readRecord()
	if rec.Branch != h.branch {
		t.Fatalf("branch mismatch: %q", rec.Branch)
	}
	if !rec.AIAssist {
		t.Fatalf("expected ai assist true")
	}
	if got := strings.Join(rec.AIModels, ","); got != "Claude-4-Sonnet" {
		t.Fatalf("models mismatch: %q", got)
	}
	if got := strings.Join(rec.AIAgentTools, ","); got != "Codex" {
		t.Fatalf("tools mismatch: %q", got)
	}
	if rec.AISessionCount != 3 {
		t.Fatalf("session count mismatch: %d", rec.AISessionCount)
	}
}

func TestDocAddDedupeAndRender(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "GPT-5", "--tool", "Codex")
	h.run("session-add")
	h.run("doc-add", "DOC-102", "DOC-103")
	h.run("doc-add", "DOC-102")
	h.run("missed", "--note", "WAL replay flow missing")

	rec := h.readRecord()
	if got := strings.Join(rec.KBDocsUsed, ","); got != "DOC-102,DOC-103" {
		t.Fatalf("docs mismatch: %q", got)
	}
	rendered := h.run("render")
	for _, needle := range []string{
		"## AI / KB Usage",
		"- AI-Assist: yes",
		"- AI-Model: GPT-5",
		"- AI-Agent-Tool: Codex",
		"- AI-Session-Count: 1",
		"- KB-Docs-Used: DOC-102, DOC-103",
		"- KB-Missed: yes",
		"- KB-Missed-Note: WAL replay flow missing",
	} {
		if !strings.Contains(rendered, needle) {
			t.Fatalf("render missing %q\n%s", needle, rendered)
		}
	}
}

func TestMissedClearAndBodyValidation(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "Claude-4-Sonnet", "--tool", "Codex")
	h.run("session-add", "--count", "2")
	h.run("doc-add", "DOC-201")
	h.run("missed", "--note", "Need more docs")
	h.run("missed-clear")

	rendered := h.run("render")
	if !strings.Contains(rendered, "- KB-Missed: no") || !strings.Contains(rendered, "- KB-Missed-Note:") {
		t.Fatalf("unexpected render:\n%s", rendered)
	}

	bodyPath := filepath.Join(h.tempDir, "pr-body.md")
	mustWriteFile(t, bodyPath, "# Summary\n\nExisting body.\n")
	h.run("body-apply", "--file", bodyPath)
	body := mustReadFile(t, bodyPath)
	if !strings.Contains(body, "## AI / KB Usage") || !strings.Contains(body, "- AI-Session-Count: 2") {
		t.Fatalf("body apply failed:\n%s", body)
	}
	h.run("validate-body", "--file", bodyPath)

	badBodyPath := filepath.Join(h.tempDir, "bad-pr-body.md")
	mustWriteFile(t, badBodyPath, "## AI / KB Usage\n- AI-Assist: maybe\n")
	h.runExpect(1, "validate-body", "--file", badBodyPath)
}

func TestDirtySyncCheckAndPRReady(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "Claude-4-Sonnet", "--tool", "Codex")
	h.run("session-add")

	var dirtyPayload map[string]any
	if err := json.Unmarshal([]byte(h.run("status", "--json")), &dirtyPayload); err != nil {
		t.Fatal(err)
	}
	if dirty, _ := dirtyPayload["dirty"].(bool); !dirty {
		t.Fatalf("expected dirty true")
	}
	h.runExpect(1, "sync-check")

	bodyPath := filepath.Join(h.tempDir, "pr-ready.md")
	h.run("pr-ready", "--file", bodyPath)
	body := mustReadFile(t, bodyPath)
	for _, needle := range []string{"## Summary", "## Verification", "## AI / KB Usage", "- AI-Session-Count: 1"} {
		if !strings.Contains(body, needle) {
			t.Fatalf("pr-ready missing %q\n%s", needle, body)
		}
	}
	var cleanPayload map[string]any
	if err := json.Unmarshal([]byte(h.run("status", "--json")), &cleanPayload); err != nil {
		t.Fatal(err)
	}
	if dirty, _ := cleanPayload["dirty"].(bool); dirty {
		t.Fatalf("expected clean after pr-ready")
	}
	if cleanPayload["last_pr_guide_path"] != filepath.Join(h.repoRoot, "docs", "agents", "pr-guide.md") {
		t.Fatalf("unexpected guide path: %#v", cleanPayload["last_pr_guide_path"])
	}
	if cleanPayload["last_body_sync_at"] == "" || cleanPayload["last_body_sync_fingerprint"] == "" {
		t.Fatalf("missing sync metadata")
	}
	h.run("sync-check")
	h.run("doc-add", "DOC-900")
	var dirtyAgain map[string]any
	if err := json.Unmarshal([]byte(h.run("status", "--json")), &dirtyAgain); err != nil {
		t.Fatal(err)
	}
	if dirty, _ := dirtyAgain["dirty"].(bool); !dirty {
		t.Fatalf("expected dirty after doc-add")
	}
}

func TestPRReadyReplacesExistingSection(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "GPT-5", "--tool", "Codex")
	h.run("session-add", "--count", "2")

	bodyPath := filepath.Join(h.tempDir, "existing-pr.md")
	mustWriteFile(t, bodyPath, "# Existing Title\n\n## Summary\nExisting.\n\n## AI / KB Usage\n- AI-Assist: no\n- AI-Model: -\n")
	h.run("pr-ready", "--file", bodyPath)
	body := mustReadFile(t, bodyPath)
	if strings.Count(body, "## AI / KB Usage") != 1 {
		t.Fatalf("expected single AI / KB section\n%s", body)
	}
	if !strings.Contains(body, "- AI-Assist: yes") || !strings.Contains(body, "- AI-Session-Count: 2") {
		t.Fatalf("unexpected body\n%s", body)
	}
}

func TestStartPrefersExplicitModelAndToolOverEnvironment(t *testing.T) {
	h := newHarness(t)
	h.setenv("CODEX_MODEL", "Claude-4-Sonnet")
	h.setenv("ANTHROPIC_MODEL", "Claude-3.7-Sonnet")
	h.setenv("AI_PR_TOOL_NAME", "Codex")
	h.run("start", "--model", "GPT-5", "--tool", "Claude Code")

	rec := h.readRecord()
	if got := strings.Join(rec.AIModels, ","); got != "GPT-5" {
		t.Fatalf("models mismatch: %q", got)
	}
	if got := strings.Join(rec.AIAgentTools, ","); got != "Claude Code" {
		t.Fatalf("tools mismatch: %q", got)
	}
}

func TestStartSeedsFirstDetectedModelOnly(t *testing.T) {
	h := newHarness(t)
	h.setenv("CODEX_MODEL", "Claude-4-Sonnet")
	h.setenv("ANTHROPIC_MODEL", "Claude-3.7-Sonnet")
	h.run("start")

	rec := h.readRecord()
	if got := strings.Join(rec.AIModels, ","); got != "Claude-4-Sonnet" {
		t.Fatalf("models mismatch: %q", got)
	}
}

func TestStartPreservesExistingModelsAndTools(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "GPT-5", "--tool", "Codex")
	h.run("session-add", "--model", "Claude-4-Sonnet", "--tool", "Claude Code")
	h.run("start", "--model", "GPT-5", "--tool", "Codex")

	rec := h.readRecord()
	if got := strings.Join(rec.AIModels, ","); got != "GPT-5,Claude-4-Sonnet" {
		t.Fatalf("models mismatch: %q", got)
	}
	if got := strings.Join(rec.AIAgentTools, ","); got != "Codex,Claude Code" {
		t.Fatalf("tools mismatch: %q", got)
	}
}

func TestStartDoesNotAppendDetectedModelWhenRecordExists(t *testing.T) {
	h := newHarness(t)
	h.run("start", "--model", "GPT-5", "--tool", "Codex")
	h.setenv("CODEX_MODEL", "Claude-4-Sonnet")
	h.run("start")

	rec := h.readRecord()
	if got := strings.Join(rec.AIModels, ","); got != "GPT-5" {
		t.Fatalf("models mismatch: %q", got)
	}
}

func TestSessionAddSeedsModelAndToolFromEnvironment(t *testing.T) {
	h := newHarness(t)
	h.setenv("AI_PR_MODEL_NAME", "GPT-5")
	h.setenv("AI_PR_TOOL_NAME", "Codex")
	h.run("session-add")

	rec := h.readRecord()
	if !rec.AIAssist {
		t.Fatalf("expected ai assist true")
	}
	if got := strings.Join(rec.AIModels, ","); got != "GPT-5" {
		t.Fatalf("models mismatch: %q", got)
	}
	if got := strings.Join(rec.AIAgentTools, ","); got != "Codex" {
		t.Fatalf("tools mismatch: %q", got)
	}
}

func TestParsePRGuideHeadingsSupportsChineseLabels(t *testing.T) {
	h := newHarness(t)
	guidePath := filepath.Join(h.tempDir, "pr-guide-zh.md")
	mustWriteFile(t, guidePath, strings.Join([]string{
		"# PR 说明",
		"",
		"## PR 描述应包含",
		"- 背景。",
		"- 验证结果。",
		"- 风险与影响。",
		"",
		"## 其他",
	}, "\n"))

	headings := parsePRGuideHeadings(guidePath)
	if got := strings.Join(headings, ","); got != "## Summary,## Verification,## Risks" {
		t.Fatalf("headings mismatch: %q", got)
	}
}

func TestReplaceSectionHandlesLeadingBlankLines(t *testing.T) {
	updated := replaceSection("\n\n## AI / KB Usage\n- AI-Assist: no\n- AI-Model: -\n", renderSection(record{
		AIAssist:       true,
		AIModels:       []string{"GPT-5"},
		AIAgentTools:   []string{"Codex"},
		AISessionCount: 1,
	}))
	if strings.Count(updated, "## AI / KB Usage") != 1 {
		t.Fatalf("expected single section\n%s", updated)
	}
	if !strings.Contains(updated, "- AI-Assist: yes") || !strings.Contains(updated, "- AI-Model: GPT-5") {
		t.Fatalf("unexpected body\n%s", updated)
	}
	if !strings.HasPrefix(updated, "\n\n## AI / KB Usage\n") {
		t.Fatalf("expected leading blank lines preserved\n%q", updated)
	}
}

func TestMissedRequiresNoteWithExample(t *testing.T) {
	h := newHarness(t)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	h.env.stdout = stdout
	h.env.stderr = stderr
	if code := h.env.run([]string{"missed"}); code != 2 {
		t.Fatalf("unexpected exit code: %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--note is required when setting KB missed") {
		t.Fatalf("missing primary error:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), `Usage: ai-pr missed --note "WAL replay flow has no single summary doc"`) {
		t.Fatalf("missing usage example:\n%s", stderr.String())
	}
}

func TestPRTemplateSupportsChineseGuideLabels(t *testing.T) {
	h := newHarness(t)
	h.useTempRepoRoot()
	guidePath := filepath.Join(h.tempDir, "pr-guide-zh.md")
	mustWriteFile(t, guidePath, strings.Join([]string{
		"# PR 说明",
		"",
		"## PR 描述应包含",
		"- 背景。",
		"- 验证结果。",
		"- 风险与影响。",
		"",
		"## 其他",
	}, "\n"))

	body := h.run("pr-template", "--pr-guide", guidePath)
	for _, needle := range []string{"## Summary", "## Verification", "## Risks"} {
		if !strings.Contains(body, needle) {
			t.Fatalf("template missing %q\n%s", needle, body)
		}
	}
}

type harness struct {
	t            *testing.T
	tempDir      string
	metricsDir   string
	branch       string
	repoRoot     string
	env          commandEnvironment
	getenvValues map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	tempDir := t.TempDir()
	metricsDir := filepath.Join(tempDir, ".ai-metrics")
	repoRoot := findRepoRoot(t)
	baseTime := time.Date(2026, 5, 26, 3, 30, 0, 0, time.UTC)
	currentTime := baseTime
	getenvValues := map[string]string{
		"AI_PR_METRICS_DIR": metricsDir,
		"AI_PR_BRANCH_NAME": "feat/test-ai-pr-kb-usage",
	}
	h := &harness{
		t:            t,
		tempDir:      tempDir,
		metricsDir:   metricsDir,
		branch:       "feat/test-ai-pr-kb-usage",
		repoRoot:     repoRoot,
		getenvValues: getenvValues,
	}
	h.env = commandEnvironment{
		repoRoot:       repoRoot,
		defaultPRGuide: filepath.Join(repoRoot, "docs", "agents", "pr-guide.md"),
		stdout:         &bytes.Buffer{},
		stderr:         &bytes.Buffer{},
		stdin:          strings.NewReader(""),
		getenv: func(key string) string {
			return getenvValues[key]
		},
		now: func() time.Time {
			currentTime = currentTime.Add(time.Second)
			return currentTime
		},
		runGit: func(args ...string) (string, error) {
			return h.branch + "\n", nil
		},
	}
	return h
}

func (h *harness) useTempRepoRoot() {
	h.t.Helper()
	h.repoRoot = h.tempDir
	h.env.repoRoot = h.tempDir
	h.env.defaultPRGuide = filepath.Join(h.tempDir, "docs", "agents", "pr-guide.md")
}

func (h *harness) setenv(key, value string) {
	h.t.Helper()
	h.getenvValues[key] = value
}

func (h *harness) run(args ...string) string {
	h.t.Helper()
	return h.runExpect(0, args...)
}

func (h *harness) runExpect(expected int, args ...string) string {
	h.t.Helper()
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	h.env.stdout = stdout
	h.env.stderr = stderr
	code := h.env.run(args)
	if code != expected {
		h.t.Fatalf("unexpected exit code for %v: got=%d want=%d\nstdout:\n%s\nstderr:\n%s", args, code, expected, stdout.String(), stderr.String())
	}
	return stdout.String()
}

func (h *harness) readRecord() record {
	h.t.Helper()
	data := mustReadFile(h.t, filepath.Join(h.metricsDir, sanitizeBranchName(h.branch)+".json"))
	var rec record
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		h.t.Fatal(err)
	}
	return rec
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "..", "..", "..", "..")
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
