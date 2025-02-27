// Copyright 2020 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
)

var activerecordResultRegex = regexp.MustCompile(`^(?P<test>[^\s]+#[^\s]+) = (?P<timing>\d+\.\d+ s) = (?P<result>.)$`)
var railsReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)\.?(?P<subpoint>\d*)$`)
var supportedRailsVersion = "6.0.3.4"
var adapterVersion = "v6.0.0beta2"

// This test runs pgjdbc's full test suite against a single kwbase node.

func registerActiveRecord(r *testRegistry) {
	runActiveRecord := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up kwbase")
		c.Put(ctx, kwbase, "./kwbase", c.All())
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("creating database used by tests")
		db, err := c.ConnE(ctx, node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.ExecContext(
			ctx, `CREATE DATABASE activerecord_unittest;`,
		); err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(
			ctx, `CREATE DATABASE activerecord_unittest2;`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rails and installing prerequisites")
		// Report the latest tag, but do not use it. The newest versions produces output that breaks our xml parser,
		// and we want to pin to the working version for now.
		latestTag, err := repeatGetLatestTag(
			ctx, c, "rails", "rails", railsReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest rails release is %s.", latestTag)
		c.l.Printf("Supported rails release is %s.", supportedRailsVersion)
		c.l.Printf("Supported adapter version is %s.", adapterVersion)

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install ruby-full ruby-dev rubygems build-essential zlib1g-dev libpq-dev libsqlite3-dev`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install ruby 2.7",
			`mkdir -p ruby-install && \
        curl -fsSL https://github.com/postmodern/ruby-install/archive/v0.6.1.tar.gz | tar --strip-components=1 -C ruby-install -xz && \
        sudo make -C ruby-install install && \
        sudo ruby-install --system ruby 2.7.1 && \
        sudo gem update --system`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old activerecord adapter", `rm -rf /mnt/data1/activerecord-kwbasedb-adapter`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/kwbasedb/activerecord-kwbasedb-adapter.git",
			"/mnt/data1/activerecord-kwbasedb-adapter",
			adapterVersion,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("installing bundler")
		if err := repeatRunE(
			ctx,
			c,
			node,
			"installing bundler",
			`cd /mnt/data1/activerecord-kwbasedb-adapter/ && sudo gem install bundler:2.1.4`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("installing gems")
		if err := repeatRunE(
			ctx,
			c,
			node,
			"installing gems",
			fmt.Sprintf(
				`cd /mnt/data1/activerecord-kwbasedb-adapter/ && `+
					`RAILS_VERSION=%s sudo bundle install`, supportedRailsVersion),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignorelistName, ignorelist := activeRecordBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No activerecord blocklist defined for kwbase version %s", version)
		}
		status := fmt.Sprintf("Running kwbase version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf("Running kwbase version %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		c.l.Printf("%s", status)

		t.Status("running activerecord test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/activerecord-kwbasedb-adapter/ && `+
				`sudo RUBYOPT="-W0" TESTOPTS="-v" bundle exec rake test`,
		)

		c.l.Printf("Test Results:\n%s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			match := activerecordResultRegex.FindStringSubmatch(scanner.Text())
			if match == nil {
				continue
			}
			test, result := match[1], match[3]
			pass := result == "."
			skipped := result == "S"
			results.allTests = append(results.allTests, test)

			ignoredIssue, expectedIgnored := ignorelist[test]
			issue, expectedFailure := expectedFailures[test]
			switch {
			case expectedIgnored:
				results.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				results.ignoredCount++
			case skipped && expectedFailure:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (unexpected)", test)
				results.unexpectedSkipCount++
			case skipped:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (expected)", test)
				results.skipCount++
			case pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				results.passExpectedCount++
			case pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				results.passUnexpectedCount++
			case !pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				results.failExpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			case !pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
				results.failUnexpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			}
			results.runTests[test] = struct{}{}
		}

		results.summarizeAll(
			t, "activerecord" /* ormName */, blocklistName, expectedFailures, version, supportedRailsVersion,
		)
	}

	r.Add(testSpec{
		MinVersion: "v20.2.0",
		Name:       "activerecord",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runActiveRecord(ctx, t, c)
		},
	})
}
