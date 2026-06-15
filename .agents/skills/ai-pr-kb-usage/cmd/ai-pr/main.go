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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	sectionHeading = "## AI / KB Usage"
)

var (
	headingPattern  = regexp.MustCompile(`(?m)^##\s+(.+?)\s*$`)
	defaultSections = []string{"## Summary", "## Verification", "## Risks"}
	yesNo           = map[string]bool{"yes": true, "no": false}
)

type commandEnvironment struct {
	repoRoot       string
	defaultPRGuide string
	stdout         io.Writer
	stderr         io.Writer
	getenv         func(string) string
	runGit         func(args ...string) (string, error)
	now            func() time.Time
	stdin          io.Reader
}

type record struct {
	Version                 int      `json:"version"`
	Branch                  string   `json:"branch"`
	AIAssist                bool     `json:"ai_assist"`
	AIModels                []string `json:"ai_models"`
	AIAgentTools            []string `json:"ai_agent_tools"`
	AISessionCount          int      `json:"ai_session_count"`
	KBDocsUsed              []string `json:"kb_docs_used"`
	KBMissed                bool     `json:"kb_missed"`
	KBMissedNote            string   `json:"kb_missed_note"`
	Dirty                   bool     `json:"dirty"`
	LastBodySyncAt          string   `json:"last_body_sync_at"`
	LastBodySyncFingerprint string   `json:"last_body_sync_fingerprint"`
	LastPRGuidePath         string   `json:"last_pr_guide_path"`
	CreatedAt               string   `json:"created_at"`
	UpdatedAt               string   `json:"updated_at"`
}

type context struct {
	metricsPath string
	record      record
}

func main() {
	os.Exit(newDefaultEnvironment().run(os.Args[1:]))
}

func newDefaultEnvironment() commandEnvironment {
	repoRoot := mustRepoRoot(os.Getenv)
	return commandEnvironment{
		repoRoot:       repoRoot,
		defaultPRGuide: filepath.Join(repoRoot, "docs", "agents", "pr-guide.md"),
		stdout:         os.Stdout,
		stderr:         os.Stderr,
		getenv:         os.Getenv,
		now:            time.Now,
		stdin:          os.Stdin,
		runGit: func(args ...string) (string, error) {
			cmd := exec.Command("git", args...)
			cmd.Dir = repoRoot
			out, err := cmd.CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
			}
			return string(out), nil
		},
	}
}

func mustRepoRoot(getenv func(string) string) string {
	if value := strings.TrimSpace(getenv("AI_PR_REPO_ROOT")); value != "" {
		return filepath.Clean(value)
	}
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.CombinedOutput()
	if err == nil {
		if root := strings.TrimSpace(string(out)); root != "" {
			return filepath.Clean(root)
		}
	}
	panic(fmt.Errorf("detect repository root with git rev-parse --show-toplevel: %w\n%s", err, strings.TrimSpace(string(out))))
}

func (env commandEnvironment) run(argv []string) int {
	if len(argv) == 0 {
		env.printMainUsage()
		return 2
	}

	switch argv[0] {
	case "start":
		return env.handleStart(argv[1:])
	case "session-add":
		return env.handleSessionAdd(argv[1:])
	case "doc-add":
		return env.handleDocAdd(argv[1:])
	case "doc-remove":
		return env.handleDocRemove(argv[1:])
	case "missed":
		return env.handleMissed(argv[1:])
	case "missed-clear":
		return env.handleMissedClear(argv[1:])
	case "render":
		return env.handleRender(argv[1:])
	case "status":
		return env.handleStatus(argv[1:])
	case "body-apply":
		return env.handleBodyApply(argv[1:])
	case "validate-body":
		return env.handleValidateBody(argv[1:])
	case "sync-check":
		return env.handleSyncCheck(argv[1:])
	case "pr-template":
		return env.handlePRTemplate(argv[1:])
	case "pr-ready":
		return env.handlePRReady(argv[1:])
	case "-h", "--help", "help":
		env.printMainUsage()
		return 0
	default:
		fmt.Fprintf(env.stderr, "unknown command: %s\n", argv[0])
		env.printMainUsage()
		return 2
	}
}

func (env commandEnvironment) printMainUsage() {
	fmt.Fprintln(env.stdout, "usage: ai-pr <command> [options]")
	fmt.Fprintln(env.stdout, "")
	fmt.Fprintln(env.stdout, "commands:")
	for _, line := range []string{
		"  start          Initialize or update the current PR metrics.",
		"  session-add    Increment the AI session count.",
		"  doc-add        Add one or more confirmed doc IDs.",
		"  doc-remove     Remove one or more doc IDs.",
		"  missed         Mark KB missed and capture a short note.",
		"  missed-clear   Clear the KB missed flag.",
		"  render         Render the AI / KB Usage markdown block.",
		"  status         Show current branch-local metrics.",
		"  body-apply     Insert or replace the AI / KB Usage section in a file.",
		"  validate-body  Validate the AI / KB Usage section in a PR body.",
		"  sync-check     Fail when local metrics have not been synced.",
		"  pr-template    Render a minimal PR body template from pr-guide.md.",
		"  pr-ready       Prepare a PR body file and inject the AI / KB section.",
	} {
		fmt.Fprintln(env.stdout, line)
	}
}

func (env commandEnvironment) newCommonFlagSet(name string) (*flag.FlagSet, *string, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.stderr)
	branch := fs.String("branch", "", "Override the current branch name.")
	metricsDir := fs.String("metrics-dir", "", "Override the metrics directory.")
	return fs, branch, metricsDir
}

func (env commandEnvironment) newGuideFlagSet(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.stderr)
	prGuide := fs.String("pr-guide", "", "Override the PR guide path.")
	return fs, prGuide
}

func (env commandEnvironment) handleStart(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("start")
	var models multiFlag
	var tools multiFlag
	aiAssist := fs.String("ai-assist", "yes", "Explicitly mark AI usage.")
	fs.Var(&models, "model", "AI model used for this PR.")
	fs.Var(&tools, "tool", "AI agent tool used for this PR.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if _, ok := yesNo[*aiAssist]; !ok {
		fmt.Fprintln(env.stderr, "--ai-assist must be yes or no")
		return 2
	}

	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	ctx.record.AIAssist = yesNo[*aiAssist]
	ctx.record.AIModels = appendUsageMetadata(ctx.record.AIModels, models, env.detectModelNames())
	ctx.record.AIAgentTools = appendUsageMetadata(ctx.record.AIAgentTools, tools, env.detectToolNames())
	ctx.record.Dirty = true

	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleSessionAdd(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("session-add")
	count := fs.Int("count", 1, "How many sessions to add.")
	var models multiFlag
	var tools multiFlag
	fs.Var(&models, "model", "Append a model name while adding a session.")
	fs.Var(&tools, "tool", "Append an agent tool while adding a session.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if *count < 1 {
		fmt.Fprintln(env.stderr, "--count must be >= 1")
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	ctx.record.AIAssist = true
	ctx.record.AISessionCount += *count
	ctx.record.AIModels = appendUsageMetadata(ctx.record.AIModels, models, env.detectModelNames())
	ctx.record.AIAgentTools = appendUsageMetadata(ctx.record.AIAgentTools, tools, env.detectToolNames())
	ctx.record.Dirty = true
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleDocAdd(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("doc-add")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(env.stderr, "doc-add requires at least one doc ID")
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	ctx.record.KBDocsUsed = dedupePreserve(append(ctx.record.KBDocsUsed, fs.Args()...))
	ctx.record.Dirty = true
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleDocRemove(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("doc-remove")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(env.stderr, "doc-remove requires at least one doc ID")
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	remove := make(map[string]struct{}, fs.NArg())
	for _, id := range fs.Args() {
		remove[id] = struct{}{}
	}
	filtered := make([]string, 0, len(ctx.record.KBDocsUsed))
	for _, id := range ctx.record.KBDocsUsed {
		if _, ok := remove[id]; !ok {
			filtered = append(filtered, id)
		}
	}
	ctx.record.KBDocsUsed = filtered
	ctx.record.Dirty = true
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleMissed(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("missed")
	note := fs.String("note", "", "Short explanation of the missing knowledge base coverage.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if strings.TrimSpace(*note) == "" {
		fmt.Fprintln(env.stderr, "--note is required when setting KB missed")
		fmt.Fprintln(env.stderr, `Usage: ai-pr missed --note "WAL replay flow has no single summary doc"`)
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	ctx.record.KBMissed = true
	ctx.record.KBMissedNote = strings.TrimSpace(*note)
	ctx.record.Dirty = true
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleMissedClear(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("missed-clear")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	ctx.record.KBMissed = false
	ctx.record.KBMissedNote = ""
	ctx.record.Dirty = true
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleRender(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("render")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	fmt.Fprint(env.stdout, renderSection(ctx.record))
	return 0
}

func (env commandEnvironment) handleStatus(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("status")
	jsonOutput := fs.Bool("json", false, "Print raw JSON plus metrics file path.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	if *jsonOutput {
		payload := map[string]any{
			"version":                    ctx.record.Version,
			"branch":                     ctx.record.Branch,
			"ai_assist":                  ctx.record.AIAssist,
			"ai_models":                  ctx.record.AIModels,
			"ai_agent_tools":             ctx.record.AIAgentTools,
			"ai_session_count":           ctx.record.AISessionCount,
			"kb_docs_used":               ctx.record.KBDocsUsed,
			"kb_missed":                  ctx.record.KBMissed,
			"kb_missed_note":             ctx.record.KBMissedNote,
			"dirty":                      ctx.record.Dirty,
			"last_body_sync_at":          ctx.record.LastBodySyncAt,
			"last_body_sync_fingerprint": ctx.record.LastBodySyncFingerprint,
			"last_pr_guide_path":         ctx.record.LastPRGuidePath,
			"created_at":                 ctx.record.CreatedAt,
			"updated_at":                 ctx.record.UpdatedAt,
			"metrics_file":               ctx.metricsPath,
		}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return env.writeError(err)
		}
		fmt.Fprintln(env.stdout, string(data))
		return 0
	}
	env.printStatus(ctx)
	return 0
}

func (env commandEnvironment) handleBodyApply(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("body-apply")
	file := fs.String("file", "", "Target markdown file.")
	prGuide := fs.String("pr-guide", "", "Override the PR guide path.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(env.stderr, "--file is required")
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	guidePath, err := env.resolvePRGuidePath(*prGuide)
	if err != nil {
		return env.writeError(err)
	}
	target := env.resolveUserPath(*file)
	original, _ := os.ReadFile(target)
	updated := replaceSection(string(original), renderSection(ctx.record))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return env.writeError(err)
	}
	if err := os.WriteFile(target, []byte(updated), 0o644); err != nil {
		return env.writeError(err)
	}
	env.markSynced(&ctx.record, updated, guidePath)
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	fmt.Fprintf(env.stdout, "updated %s\n", target)
	return 0
}

func (env commandEnvironment) handleValidateBody(argv []string) int {
	fs := flag.NewFlagSet("validate-body", flag.ContinueOnError)
	fs.SetOutput(env.stderr)
	file := fs.String("file", "", "Path to a file containing a PR body.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	var body string
	if *file != "" {
		data, err := os.ReadFile(env.resolveUserPath(*file))
		if err != nil {
			return env.writeError(err)
		}
		body = string(data)
	} else {
		data, err := io.ReadAll(env.stdin)
		if err != nil {
			return env.writeError(err)
		}
		body = string(data)
	}
	section := extractSection(body)
	if section == "" {
		fmt.Fprintln(env.stderr, "missing section: ## AI / KB Usage")
		return 1
	}
	errors := validateSection(section)
	if len(errors) > 0 {
		for _, item := range errors {
			fmt.Fprintln(env.stderr, item)
		}
		return 1
	}
	fmt.Fprintln(env.stdout, "AI / KB Usage section looks valid")
	return 0
}

func (env commandEnvironment) handleSyncCheck(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("sync-check")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	if ctx.record.Dirty {
		fmt.Fprintln(env.stderr, "AI / KB usage is dirty; run pr-ready or body-apply before submitting the PR.")
		return 1
	}
	fmt.Fprintln(env.stdout, "AI / KB usage is synced")
	return 0
}

func (env commandEnvironment) handlePRTemplate(argv []string) int {
	fs, prGuide := env.newGuideFlagSet("pr-template")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	guidePath, err := env.resolvePRGuidePath(*prGuide)
	if err != nil {
		return env.writeError(err)
	}
	fmt.Fprint(env.stdout, buildPRBodyTemplate(guidePath))
	return 0
}

func (env commandEnvironment) handlePRReady(argv []string) int {
	fs, branch, metricsDir := env.newCommonFlagSet("pr-ready")
	file := fs.String("file", "", "Target PR body markdown file.")
	prGuide := fs.String("pr-guide", "", "Override the PR guide path.")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(env.stderr, "--file is required")
		return 2
	}
	ctx, err := env.loadContext(*branch, *metricsDir)
	if err != nil {
		return env.writeError(err)
	}
	guidePath, err := env.resolvePRGuidePath(*prGuide)
	if err != nil {
		return env.writeError(err)
	}
	body := buildPRBodyTemplate(guidePath)
	targetFile := env.resolveUserPath(*file)
	if data, err := os.ReadFile(targetFile); err == nil {
		body = string(data)
	}
	body = ensureRequiredPRSections(body, guidePath)
	body = replaceSection(body, renderSection(ctx.record))
	if err := os.MkdirAll(filepath.Dir(targetFile), 0o755); err != nil {
		return env.writeError(err)
	}
	if err := os.WriteFile(targetFile, []byte(body), 0o644); err != nil {
		return env.writeError(err)
	}
	env.markSynced(&ctx.record, body, guidePath)
	if err := env.saveContext(ctx); err != nil {
		return env.writeError(err)
	}
	fmt.Fprintf(env.stdout, "prepared %s\n", targetFile)
	return 0
}

func (env commandEnvironment) loadContext(branchOverride, metricsOverride string) (context, error) {
	branch := strings.TrimSpace(branchOverride)
	if branch == "" {
		branch = env.detectBranchName()
	}
	metricsDir := env.resolveMetricsDir(metricsOverride)
	metricsPath := filepath.Join(metricsDir, sanitizeBranchName(branch)+".json")
	rec, err := env.ensureRecord(metricsPath, branch)
	if err != nil {
		return context{}, err
	}
	return context{metricsPath: metricsPath, record: rec}, nil
}

func (env commandEnvironment) ensureRecord(path, branch string) (record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return defaultRecord(branch, env.now), nil
	}
	if err != nil {
		return record{}, err
	}
	var rec record
	if err := json.Unmarshal(data, &rec); err != nil {
		return record{}, err
	}
	return normalizeRecord(rec, branch, env.now), nil
}

func (env commandEnvironment) saveContext(ctx context) error {
	ctx.record.UpdatedAt = formatTime(env.now())
	if err := os.MkdirAll(filepath.Dir(ctx.metricsPath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(ctx.record, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(ctx.metricsPath), filepath.Base(ctx.metricsPath)+".*.tmp")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(name, ctx.metricsPath)
}

func (env commandEnvironment) detectBranchName() string {
	if branch := strings.TrimSpace(env.getenv("AI_PR_BRANCH_NAME")); branch != "" {
		return branch
	}
	out, err := env.runGit("branch", "--show-current")
	if err == nil && strings.TrimSpace(out) != "" {
		return strings.TrimSpace(out)
	}
	return "unknown-branch"
}

func (env commandEnvironment) detectModelNames() []string {
	candidates := []string{"AI_PR_MODEL_NAME", "CODEX_MODEL", "OPENAI_MODEL", "ANTHROPIC_MODEL", "OLLAMA_MODEL", "MODEL_NAME"}
	for _, name := range candidates {
		if value := strings.TrimSpace(env.getenv(name)); value != "" {
			return []string{value}
		}
	}
	return nil
}

func (env commandEnvironment) detectToolNames() []string {
	if value := strings.TrimSpace(env.getenv("AI_PR_TOOL_NAME")); value != "" {
		return []string{value}
	}
	if env.getenv("CODEX_HOME") != "" || env.getenv("CODEX_SANDBOX") != "" {
		return []string{"Codex"}
	}
	return nil
}

func (env commandEnvironment) resolveMetricsDir(override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	if value := strings.TrimSpace(env.getenv("AI_PR_METRICS_DIR")); value != "" {
		return value
	}
	return filepath.Join(env.repoRoot, ".ai-metrics")
}

func (env commandEnvironment) resolvePRGuidePath(override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return env.resolveRepoPath(override)
	}
	if value := strings.TrimSpace(env.getenv("AI_PR_GUIDE_PATH")); value != "" {
		return env.resolveRepoPath(value)
	}
	return env.resolveRepoPath(env.defaultPRGuide)
}

func (env commandEnvironment) resolveUserPath(path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	if callerCWD := strings.TrimSpace(env.getenv("AI_PR_CALLER_CWD")); callerCWD != "" {
		return filepath.Join(callerCWD, path)
	}
	return path
}

func (env commandEnvironment) resolveRepoPath(path string) (string, error) {
	resolved := filepath.Clean(env.resolveUserPath(path))
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(env.repoRoot, resolved)
	}
	root := filepath.Clean(env.repoRoot)
	rel, err := filepath.Rel(root, resolved)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes repository root: %s", path)
	}
	return resolved, nil
}

func (env commandEnvironment) markSynced(rec *record, body, prGuidePath string) {
	rec.Dirty = false
	rec.LastBodySyncAt = formatTime(env.now())
	hash := sha256.Sum256([]byte(body))
	rec.LastBodySyncFingerprint = "sha256:" + hex.EncodeToString(hash[:])
	rec.LastPRGuidePath = prGuidePath
}

func (env commandEnvironment) printStatus(ctx context) {
	lines := []string{
		"Branch: " + ctx.record.Branch,
		"Metrics-File: " + ctx.metricsPath,
		"AI-Assist: " + yesNoString(ctx.record.AIAssist),
		"AI-Model: " + renderList(ctx.record.AIModels),
		"AI-Agent-Tool: " + renderList(ctx.record.AIAgentTools),
		fmt.Sprintf("AI-Session-Count: %d", ctx.record.AISessionCount),
		"KB-Docs-Used: " + renderList(ctx.record.KBDocsUsed),
		"KB-Missed: " + yesNoString(ctx.record.KBMissed),
		"KB-Missed-Note: " + ctx.record.KBMissedNote,
		"Dirty: " + yesNoString(ctx.record.Dirty),
		"Last-Body-Sync-At: " + ctx.record.LastBodySyncAt,
		"Last-Body-Sync-Fingerprint: " + ctx.record.LastBodySyncFingerprint,
		"Last-PR-Guide-Path: " + ctx.record.LastPRGuidePath,
	}
	fmt.Fprintln(env.stdout, strings.Join(lines, "\n"))
}

func (env commandEnvironment) writeError(err error) int {
	fmt.Fprintln(env.stderr, err)
	return 1
}

func defaultRecord(branch string, now func() time.Time) record {
	timestamp := formatTime(now())
	return record{
		Version:         1,
		Branch:          branch,
		AIModels:        []string{},
		AIAgentTools:    []string{},
		KBDocsUsed:      []string{},
		CreatedAt:       timestamp,
		UpdatedAt:       timestamp,
		LastPRGuidePath: "",
	}
}

func normalizeRecord(rec record, branch string, now func() time.Time) record {
	base := defaultRecord(branch, now)
	if rec.Version != 0 {
		base.Version = rec.Version
	}
	if strings.TrimSpace(rec.Branch) != "" {
		base.Branch = rec.Branch
	}
	base.AIAssist = rec.AIAssist
	base.AIModels = dedupePreserve(rec.AIModels)
	base.AIAgentTools = dedupePreserve(rec.AIAgentTools)
	base.AISessionCount = rec.AISessionCount
	base.KBDocsUsed = dedupePreserve(rec.KBDocsUsed)
	base.KBMissed = rec.KBMissed
	base.KBMissedNote = rec.KBMissedNote
	base.Dirty = rec.Dirty
	base.LastBodySyncAt = rec.LastBodySyncAt
	base.LastBodySyncFingerprint = rec.LastBodySyncFingerprint
	base.LastPRGuidePath = rec.LastPRGuidePath
	if strings.TrimSpace(rec.CreatedAt) != "" {
		base.CreatedAt = rec.CreatedAt
	}
	if strings.TrimSpace(rec.UpdatedAt) != "" {
		base.UpdatedAt = rec.UpdatedAt
	}
	return base
}

func formatTime(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

func sanitizeBranchName(branch string) string {
	var builder strings.Builder
	for _, ch := range strings.TrimSpace(branch) {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-' {
			builder.WriteRune(ch)
			continue
		}
		builder.WriteRune('_')
	}
	if builder.Len() == 0 {
		return "unknown-branch"
	}
	return builder.String()
}

func dedupePreserve(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func renderList(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}

func yesNoString(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func renderSection(rec record) string {
	lines := []string{
		sectionHeading,
		"- AI-Assist: " + yesNoString(rec.AIAssist),
		"- AI-Model: " + renderList(rec.AIModels),
		"- AI-Agent-Tool: " + renderList(rec.AIAgentTools),
		fmt.Sprintf("- AI-Session-Count: %d", rec.AISessionCount),
		"- KB-Docs-Used: " + renderList(rec.KBDocsUsed),
		"- KB-Missed: " + yesNoString(rec.KBMissed),
		"- KB-Missed-Note: " + rec.KBMissedNote,
	}
	return strings.Join(lines, "\n") + "\n"
}

func replaceSection(body, section string) string {
	normalized := strings.TrimSpace(section) + "\n"
	existing := strings.TrimRight(body, "\n")
	start, end, ok := findSectionBounds(existing, sectionHeading)
	if ok {
		replaced := existing[:start] + normalized + strings.TrimLeft(existing[end:], "\n")
		return strings.TrimRight(replaced, "\n") + "\n"
	}
	if existing == "" {
		return normalized
	}
	return existing + "\n\n" + normalized
}

func extractSection(body string) string {
	start, end, ok := findSectionBounds(body, sectionHeading)
	if !ok {
		return ""
	}
	return strings.TrimSpace(body[start:end]) + "\n"
}

func parseSection(section string) map[string]string {
	result := make(map[string]string)
	for _, raw := range strings.Split(section, "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "- ") {
			continue
		}
		content := strings.TrimPrefix(line, "- ")
		label, value, ok := strings.Cut(content, ":")
		if !ok {
			continue
		}
		result[strings.TrimSpace(label)] = strings.TrimSpace(value)
	}
	return result
}

func validateSection(section string) []string {
	parsed := parseSection(section)
	required := []string{
		"AI-Assist",
		"AI-Model",
		"AI-Agent-Tool",
		"AI-Session-Count",
		"KB-Docs-Used",
		"KB-Missed",
		"KB-Missed-Note",
	}
	var issues []string
	for _, field := range required {
		if _, ok := parsed[field]; !ok {
			issues = append(issues, "missing field: "+field)
		}
	}
	if value := parsed["AI-Assist"]; value != "" {
		if _, ok := yesNo[value]; !ok {
			issues = append(issues, "AI-Assist must be yes or no")
		}
	}
	if value := parsed["KB-Missed"]; value != "" {
		if _, ok := yesNo[value]; !ok {
			issues = append(issues, "KB-Missed must be yes or no")
		}
	}
	if value := parsed["AI-Session-Count"]; value != "" {
		for _, ch := range value {
			if ch < '0' || ch > '9' {
				issues = append(issues, "AI-Session-Count must be a non-negative integer")
				break
			}
		}
	}
	for _, field := range []string{"AI-Model", "AI-Agent-Tool", "KB-Docs-Used"} {
		if value, ok := parsed[field]; ok && value == "" {
			issues = append(issues, field+" must not be empty; use - when nothing is recorded")
		}
	}
	if parsed["KB-Missed"] == "yes" && parsed["KB-Missed-Note"] == "" {
		issues = append(issues, "KB-Missed-Note must be non-empty when KB-Missed is yes")
	}
	return issues
}

func parsePRGuideHeadings(prGuidePath string) []string {
	data, err := os.ReadFile(prGuidePath)
	if err != nil {
		return cloneStrings(defaultSections)
	}
	lines := strings.Split(string(data), "\n")
	var result []string
	collecting := false
	for _, raw := range lines {
		stripped := strings.TrimSpace(raw)
		if stripped == "## PR Description Should Include" || stripped == "## PR 描述应包含" {
			collecting = true
			continue
		}
		if collecting && strings.HasPrefix(stripped, "## ") {
			break
		}
		if collecting && strings.HasPrefix(stripped, "- ") {
			if heading := mapPRGuideHeading(stripped); heading != "" {
				result = append(result, heading)
			}
		}
	}
	result = dedupePreserve(result)
	if len(result) == 0 {
		return cloneStrings(defaultSections)
	}
	return result
}

func buildPRBodyTemplate(prGuidePath string) string {
	lines := []string{"# PR Title", ""}
	for _, heading := range parsePRGuideHeadings(prGuidePath) {
		lines = append(lines, heading, "")
	}
	return strings.TrimRight(strings.Join(lines, "\n"), "\n") + "\n"
}

func ensureRequiredPRSections(body, prGuidePath string) string {
	result := strings.TrimRight(body, "\n")
	headings := make(map[string]struct{})
	for _, match := range headingPattern.FindAllString(result, -1) {
		headings[strings.TrimSpace(match)] = struct{}{}
	}
	for _, heading := range parsePRGuideHeadings(prGuidePath) {
		if _, ok := headings[heading]; ok {
			continue
		}
		if result != "" {
			result += "\n\n"
		}
		result += heading + "\n"
	}
	return strings.TrimRight(result, "\n") + "\n"
}

func cloneStrings(values []string) []string {
	return append([]string(nil), values...)
}

func appendUsageMetadata(existing []string, explicit multiFlag, detected []string) []string {
	merged := cloneStrings(existing)
	if len(explicit) > 0 {
		return dedupePreserve(append(merged, explicit...))
	}
	if len(merged) == 0 {
		return dedupePreserve(append(merged, detected...))
	}
	return merged
}

func mapPRGuideHeading(raw string) string {
	label := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "- ")))
	label = strings.TrimRight(label, ".。:：")
	switch {
	case strings.HasPrefix(label, "background"), strings.HasPrefix(label, "背景"):
		return "## Summary"
	case strings.HasPrefix(label, "verification performed"), strings.HasPrefix(label, "verification"), strings.HasPrefix(label, "已执行验证"), strings.HasPrefix(label, "验证结果"), strings.HasPrefix(label, "验证"):
		return "## Verification"
	case strings.HasPrefix(label, "known risks"), strings.HasPrefix(label, "risks"), strings.HasPrefix(label, "已知风险"), strings.HasPrefix(label, "风险与影响"), strings.HasPrefix(label, "风险"):
		return "## Risks"
	default:
		return ""
	}
}

func findSectionBounds(body, heading string) (int, int, bool) {
	lines := strings.SplitAfter(body, "\n")
	offset := 0
	start := -1
	end := len(body)
	for _, line := range lines {
		trimmed := strings.TrimSpace(strings.TrimRight(line, "\n"))
		if start == -1 {
			if trimmed == heading {
				start = offset
			}
		} else if strings.HasPrefix(trimmed, "## ") && trimmed != heading {
			end = offset
			break
		}
		offset += len(line)
	}
	if start == -1 {
		return 0, 0, false
	}
	return start, end, true
}

type multiFlag []string

func (m *multiFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}
