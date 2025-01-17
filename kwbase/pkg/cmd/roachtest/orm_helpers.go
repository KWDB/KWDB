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

package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// alterZoneConfigAndClusterSettings changes the zone configurations so that GC
// occurs more quickly and jobs are retained for less time. This is useful for
// most ORM tests because they create/drop/alter tables frequently, which can
// cause thousands of table descriptors and schema change jobs to accumulate
// rapidly, thereby decreasing performance.
func alterZoneConfigAndClusterSettings(
	ctx context.Context, version string, c *cluster, nodeIdx int,
) error {
	db, err := c.ConnE(ctx, nodeIdx)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.ExecContext(
		ctx, `ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `ALTER DATABASE system CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `ALTER TABLE system.public.jobs CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `ALTER RANGE meta CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `ALTER RANGE system CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `ALTER RANGE liveness CONFIGURE ZONE USING num_replicas = 1, gc.ttlseconds = 60;`,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx, `SET CLUSTER SETTING jobs.retention_time = '180s';`,
	); err != nil {
		return err
	}

	// Shorten the merge queue interval to clean up ranges due to dropped tables.
	if _, err := db.ExecContext(
		ctx, `SET CLUSTER SETTING kv.range_merge.queue_interval = '200ms'`,
	); err != nil {
		return err
	}

	// Disable syncs associated with the Raft log which are the primary causes of
	// fsyncs.
	if _, err := db.ExecContext(
		ctx, `SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = 'true'`,
	); err != nil {
		return err
	}

	// Enable temp tables for v20.1+
	if strings.HasPrefix(version, "v20.") || strings.HasPrefix(version, "v21.") {
		if _, err := db.ExecContext(
			ctx, `SET CLUSTER SETTING sql.defaults.experimental_temporary_tables.enabled = 'true';`,
		); err != nil {
			return err
		}
	}

	return nil
}

// ormTestsResults is a helper struct to be used in all roachtests for ORMs and
// drivers' compatibility.
type ormTestsResults struct {
	currentFailures, allTests                    []string
	failUnexpectedCount, failExpectedCount       int
	ignoredCount, skipCount, unexpectedSkipCount int
	passUnexpectedCount, passExpectedCount       int
	// Put all the results in a giant map of [testname]result.
	results map[string]string
	// Put all issue hints in a map of [testname]issue.
	allIssueHints map[string]string
	runTests      map[string]struct{}
}

func newORMTestsResults() *ormTestsResults {
	return &ormTestsResults{
		results:       make(map[string]string),
		allIssueHints: make(map[string]string),
		runTests:      make(map[string]struct{}),
	}
}

// summarizeAll summarizes the result of running an ORM or a driver test suite
// against a kwbase node. If an unexpected result is observed (for example,
// a test unexpectedly failed or passed), a new blocklist is populated.
func (r *ormTestsResults) summarizeAll(
	t *test, ormName, blocklistName string, expectedFailures blocklist, version, tag string,
) {
	// Collect all the tests that were not run.
	notRunCount := 0
	for test, issue := range expectedFailures {
		if _, ok := r.runTests[test]; ok {
			continue
		}
		r.allTests = append(r.allTests, test)
		r.results[test] = fmt.Sprintf("--- FAIL: %s - %s (not run)", test, maybeAddGithubLink(issue))
		notRunCount++
	}

	// Log all the test results. We re-order the tests alphabetically here.
	sort.Strings(r.allTests)
	for _, test := range r.allTests {
		result, ok := r.results[test]
		if !ok {
			t.Fatalf("can't find %s in test result list", test)
		}
		t.l.Printf("%s\n", result)
	}

	t.l.Printf("------------------------\n")

	r.summarizeFailed(
		t, ormName, blocklistName, expectedFailures, version, tag, notRunCount,
	)
}

// summarizeFailed prints out the results of running an ORM or a driver test
// suite against a kwbase node. It is similar to summarizeAll except that it
// doesn't pay attention to all the tests - only to the failed ones.
// If a test suite outputs only the failures, then this method should be used.
func (r *ormTestsResults) summarizeFailed(
	t *test,
	ormName, blocklistName string,
	expectedFailures blocklist,
	version, latestTag string,
	notRunCount int,
) {
	var bResults strings.Builder
	fmt.Fprintf(&bResults, "Tests run on Cockroach %s\n", version)
	fmt.Fprintf(&bResults, "Tests run against %s %s\n", ormName, latestTag)
	totalTestsRun := r.passExpectedCount + r.passUnexpectedCount + r.failExpectedCount + r.failUnexpectedCount
	fmt.Fprintf(&bResults, "%d Total Tests Run\n",
		totalTestsRun,
	)
	if totalTestsRun == 0 {
		t.Fatal("No tests ran! Fix the testing commands.")
	}

	p := func(msg string, count int) {
		testString := "tests"
		if count == 1 {
			testString = "test"
		}
		fmt.Fprintf(&bResults, "%d %s %s\n", count, testString, msg)
	}
	p("passed", r.passUnexpectedCount+r.passExpectedCount)
	p("failed", r.failUnexpectedCount+r.failExpectedCount)
	p("skipped", r.skipCount)
	p("ignored", r.ignoredCount)
	p("passed unexpectedly", r.passUnexpectedCount)
	p("failed unexpectedly", r.failUnexpectedCount)
	p("expected failed but skipped", r.unexpectedSkipCount)
	p("expected failed but not run", notRunCount)

	fmt.Fprintf(&bResults, "---\n")
	for _, result := range r.results {
		if strings.Contains(result, "unexpected") {
			fmt.Fprintf(&bResults, "%s\n", result)
		}
	}

	fmt.Fprintf(&bResults, "For a full summary look at the %s artifacts \n", ormName)
	t.l.Printf("%s\n", bResults.String())
	t.l.Printf("------------------------\n")

	if r.failUnexpectedCount > 0 || r.passUnexpectedCount > 0 ||
		notRunCount > 0 || r.unexpectedSkipCount > 0 {
		// Create a new blocklist so we can easily update this test.
		sort.Strings(r.currentFailures)
		var b strings.Builder
		fmt.Fprintf(&b, "Here is new %s blocklist that can be used to update the test:\n\n", ormName)
		fmt.Fprintf(&b, "var %s = blocklist{\n", blocklistName)
		for _, test := range r.currentFailures {
			issue := expectedFailures[test]
			if len(issue) == 0 || issue == "unknown" {
				issue = r.allIssueHints[test]
			}
			if len(issue) == 0 {
				issue = "unknown"
			}
			fmt.Fprintf(&b, "  \"%s\": \"%s\",\n", test, issue)
		}
		fmt.Fprintf(&b, "}\n\n")
		t.l.Printf("\n\n%s\n\n", b.String())
		t.l.Printf("------------------------\n")
		t.Fatalf("\n%s\nAn updated blocklist (%s) is available in the artifacts' %s log\n",
			bResults.String(),
			blocklistName,
			ormName,
		)
	}
}
