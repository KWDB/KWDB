// Copyright 2019 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

// smithtest is a tool to execute sqlsmith tests on kwbase demo
// instances. Failures are tracked, de-duplicated. Issues are
// prefilled for GitHub.
package main

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/internal/sqlsmith"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/google/go-github/github"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/browser"
	"github.com/pkg/errors"
)

var (
	flags  = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	kwbase = flags.String("kwbase", "./kwbase", "path to kwbase binary")
	num    = flags.Int("num", 1, "number of parallel testers")
)

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}

	ctx := context.Background()
	setup := WorkerSetup{
		kwbase: *kwbase,
		github: github.NewClient(nil),
	}
	rand.Seed(timeutil.Now().UnixNano())

	setup.populateGitHubIssues(ctx)

	fmt.Println("running...")

	g := ctxgroup.WithContext(ctx)
	for i := 0; i < *num; i++ {
		g.GoCtx(setup.work)
	}
	if err := g.Wait(); err != nil {
		log.Fatalf("%+v", err)
	}
}

// WorkerSetup contains initialization and configuration for running smithers.
type WorkerSetup struct {
	kwbase string
	github *github.Client
}

// populateGitHubIssues populates seen with issues already in GitHub.
func (s WorkerSetup) populateGitHubIssues(ctx context.Context) {
	var opts github.SearchOptions
	for {
		results, _, err := s.github.Search.Issues(ctx, "repo:kwbasedb/kwbase type:issue state:open label:C-bug label:O-sqlsmith", &opts)
		if err != nil {
			log.Fatal(err)
		}
		for _, issue := range results.Issues {
			title := filterIssueTitle(issue.GetTitle())
			seenIssues[title] = true
			fmt.Println("pre populate", title)
		}
		if results.GetIncompleteResults() {
			opts.Page++
			continue
		}
		return
	}
}

func (s WorkerSetup) work(ctx context.Context) error {
	rnd := rand.New(rand.NewSource(rand.Int63()))
	for {
		if err := s.run(ctx, rnd); err != nil {
			return err
		}
	}
}

var (
	// lock protects seenIssues from concurrent access. RLock in run() blocks
	// new queries while failure() holds the write lock to file an issue.
	lock syncutil.RWMutex
	// seenIssues tracks the seen github issues.
	seenIssues = map[string]bool{}

	connRE         = regexp.MustCompile(`(?m)^sql:\s*(postgresql://.*)$`)
	panicRE        = regexp.MustCompile(`(?m)^(panic: .*?)( \[recovered\])?$`)
	stackRE        = regexp.MustCompile(`panic: .*\n\ngoroutine \d+ \[running\]:\n(?s:(.*))$`)
	fatalRE        = regexp.MustCompile(`(?m)^(fatal error: .*?)$`)
	runtimeStackRE = regexp.MustCompile(`goroutine \d+ \[running\]:\n(?s:(.*?))\n\n`)
)

// run is a single sqlsmith worker. It starts a new sqlsmither and in-memory
// single-node cluster. If an error is found it submits the
// issue. If an issue is successfully found, this function returns, causing
// the started kwbase instance to shut down. An error is only returned if
// something unexpected happened. That is, panics and internal errors will
// return nil, since they are expected. Something unexpected would be like the
// initialization SQL was unable to run.
func (s WorkerSetup) run(ctx context.Context, rnd *rand.Rand) error {
	// Stop running after a while to get new setup and settings.
	done := timeutil.Now().Add(time.Minute)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.kwbase,
		"start-single-node",
		"--port", "0",
		"--http-port", "0",
		"--insecure",
		"--store=type=mem,size=1GB",
		"--logtostderr",
	)

	// Look for the connection string.
	var pgdb *pgx.Conn
	var db *gosql.DB
	var output bytes.Buffer

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "start")
	}

	scanner := bufio.NewScanner(io.TeeReader(stderr, &output))
	for scanner.Scan() {
		line := scanner.Text()
		if match := connRE.FindStringSubmatch(line); match != nil {
			config, err := pgx.ParseURI(match[1])
			if err != nil {
				return errors.Wrap(err, "parse uri")
			}
			pgdb, err = pgx.Connect(config)
			if err != nil {
				return errors.Wrap(err, "connect")
			}

			connector, err := pq.NewConnector(match[1])
			if err != nil {
				return errors.Wrap(err, "connector error")
			}
			db = gosql.OpenDB(connector)
			if err != nil {
				return errors.Wrap(err, "connect")
			}
			fmt.Println("connected to", match[1])
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(output.String())
		return errors.Wrap(err, "scanner error")
	}
	if db == nil {
		fmt.Println(output.String())
		return errors.New("no DB address found")
	}
	fmt.Println("worker started")

	initSQL := sqlsmith.Setups[sqlsmith.RandSetup(rnd)](rnd)
	if _, err := pgdb.ExecEx(ctx, initSQL, nil); err != nil {
		return errors.Wrap(err, "init")
	}

	setting := sqlsmith.Settings[sqlsmith.RandSetting(rnd)](rnd)
	opts := append([]sqlsmith.SmitherOption{
		sqlsmith.DisableMutations(),
	}, setting.Options...)
	smither, err := sqlsmith.NewSmither(db, rnd, opts...)
	if err != nil {
		return errors.Wrap(err, "new smither")
	}
	for {
		if timeutil.Now().After(done) {
			return nil
		}

		// If lock is locked for writing (due to a found bug in another
		// goroutine), block here until issue filing completes.
		lock.RLock()
		stmt := smither.Generate()
		done := make(chan struct{}, 1)
		go func() {
			_, err = pgdb.ExecEx(ctx, stmt, nil)
			done <- struct{}{}
		}()
		// Timeout slow statements by returning, which will cancel the
		// command's context by the above defer.
		select {
		case <-time.After(10 * time.Second):
			fmt.Printf("TIMEOUT:\n%s\n", stmt)
			lock.RUnlock()
			return nil
		case <-done:
		}
		lock.RUnlock()
		if err != nil {
			if strings.Contains(err.Error(), "internal error") {
				// Return from this function on internal
				// errors. This causes the current kwbase
				// instance to shut down and we start a new
				// one. This is not strictly necessary, since
				// internal errors don't mess up the rest of
				// kwbase, but it's just easier to have a
				// single logic flow in case of a found error,
				// which is to shut down and start over (just
				// like the panic case below).
				return s.failure(ctx, initSQL, stmt, err)
			}

		}
		// If we can't ping, check if the statement caused a panic.
		if err := db.PingContext(ctx); err != nil {
			input := fmt.Sprintf("%s; %s;", initSQL, stmt)
			out, _ := exec.CommandContext(ctx, s.kwbase, "demo", "--empty", "-e", input).CombinedOutput()
			var pqerr pq.Error
			if match := stackRE.FindStringSubmatch(string(out)); match != nil {
				pqerr.Detail = strings.TrimSpace(match[1])
			}
			if match := panicRE.FindStringSubmatch(string(out)); match != nil {
				// We found a panic as expected.
				pqerr.Message = match[1]
				return s.failure(ctx, initSQL, stmt, &pqerr)
			}
			// Not a panic. Maybe a fatal?
			if match := runtimeStackRE.FindStringSubmatch(string(out)); match != nil {
				pqerr.Detail = strings.TrimSpace(match[1])
			}
			if match := fatalRE.FindStringSubmatch(string(out)); match != nil {
				// A real bad non-panic error.
				pqerr.Message = match[1]
				return s.failure(ctx, initSQL, stmt, &pqerr)
			}
			// A panic was not found. Shut everything down by returning an error so it can be investigated.
			fmt.Printf("output:\n%s\n", out)
			fmt.Printf("Ping stmt:\n%s;\n", stmt)
			return err
		}
	}
}

// failure de-duplicates and files errors. It generally returns nil
// indicating that this was successfully filed and we should continue looking
// for errors.
func (s WorkerSetup) failure(ctx context.Context, initSQL, stmt string, err error) error {
	var message, stack string
	if pqerr, ok := err.(pgx.PgError); ok {
		stack = pqerr.Detail
		message = pqerr.Message
	} else {
		message = err.Error()
	}
	filteredMessage := filterIssueTitle(regexp.QuoteMeta(message))
	message = fmt.Sprintf("sql: %s", message)

	lock.Lock()
	// Keep this locked for the remainder of the function so that smither
	// tests won't run while filing an issue.
	defer lock.Unlock()
	sqlFilteredMessage := fmt.Sprintf("sql: %s", filteredMessage)
	alreadySeen := seenIssues[sqlFilteredMessage]
	if !alreadySeen {
		seenIssues[sqlFilteredMessage] = true
	}
	if alreadySeen {
		fmt.Println("already found", message)
		return nil
	}
	fmt.Println("found", message)
	input := fmt.Sprintf("%s\n\n%s;", initSQL, stmt)
	fmt.Printf("SQL:\n%s\n\n", input)

	// Generate the pre-filled github issue.
	makeBody := func() string {
		return fmt.Sprintf("```\n%s\n```\n\n```\n%s\n```", strings.TrimSpace(input), strings.TrimSpace(stack))
	}
	query := url.Values{
		"title":  []string{message},
		"labels": []string{"C-bug,O-sqlsmith"},
		"body":   []string{makeBody()},
	}
	url := url.URL{
		Scheme:   "https",
		Host:     "github.com",
		Path:     "/kwbasedb/kwbase/issues/new",
		RawQuery: query.Encode(),
	}
	const max = 8000
	// Remove lines from the stack trace to shorten up the request so it's
	// under the github limit.
	for len(url.String()) > max {
		last := strings.LastIndex(stack, "\n")
		if last < 0 {
			break
		}
		stack = stack[:last]
		query["body"][0] = makeBody()
		url.RawQuery = query.Encode()
	}
	if len(url.String()) > max {
		fmt.Println(stmt)
		return errors.New("request could not be shortened to max length")
	}

	if err := browser.OpenURL(url.String()); err != nil {
		return err
	}

	return nil
}

// filterIssueTitle handles issue title where some words in the title can
// vary for identical issues. Usually things like number of bytes, IDs, or
// counts. These are converted into their regex equivalent so they can be
// correctly de-duplicated.
func filterIssueTitle(s string) string {
	for _, reS := range []string{
		`given: .*, expected .*`,
		`Datum is .*, not .*`,
		`expected .*, found .*`,
		`\d+`,
		`\*tree\.D\w+`,
	} {
		re := regexp.MustCompile(reS)
		s = re.ReplaceAllString(s, reS)
	}
	return s
}
