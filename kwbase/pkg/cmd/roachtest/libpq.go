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
	"regexp"
	"strings"

	"github.com/stretchr/testify/require"
)

var libPQReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

func registerLibPQ(r *testRegistry) {
	runLibPQ := func(ctx context.Context, t *test, c *cluster) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up kwbase")
		c.Put(ctx, kwbase, "./kwbase", c.All())
		c.Start(ctx, t, c.All())
		version, err := fetchCockroachVersion(ctx, c, node[0])
		require.NoError(t, err)
		err = alterZoneConfigAndClusterSettings(ctx, version, c, node[0])
		require.NoError(t, err)

		t.Status("cloning lib/pq and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, c, "lib", "pq", libPQReleaseTagRegex)
		require.NoError(t, err)
		c.l.Printf("Latest lib/pq release is %s.", latestTag)

		installLatestGolang(ctx, t, c, node)

		const (
			libPQRepo   = "github.com/lib/pq"
			libPQPath   = goPath + "/src/" + libPQRepo
			resultsDir  = "~/logs/report/libpq-results"
			resultsPath = resultsDir + "/report.xml"
		)

		// Remove any old lib/pq installations
		err = repeatRunE(
			ctx, c, node, "remove old lib/pq", fmt.Sprintf("rm -rf %s", libPQPath),
		)
		require.NoError(t, err)

		// Install go-junit-report to convert test results to .xml format we know
		// how to work with.
		err = repeatRunE(ctx, c, node, "install go-junit-report",
			fmt.Sprintf("GOPATH=%s go get -u github.com/jstemmer/go-junit-report", goPath),
		)
		require.NoError(t, err)

		err = repeatGitCloneE(
			ctx,
			t.l,
			c,
			fmt.Sprintf("https://%s.git", libPQRepo),
			libPQPath,
			latestTag,
			node,
		)
		require.NoError(t, err)
		_ = c.RunE(ctx, node, fmt.Sprintf("mkdir -p %s", resultsDir))

		blocklistName, expectedFailures, ignorelistName, ignoreList := libPQBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No lib/pq blocklist defined for kwbase version %s", version)
		}
		c.l.Printf("Running kwbase version %s, using blocklist %s, using ignorelist %s", version, blocklistName, ignorelistName)

		t.Status("running lib/pq test suite and collecting results")

		// List all the tests that start with Test or Example.
		testListRegex := "^(Test|Example)"
		buf, err := c.RunWithBuffer(
			ctx,
			t.l,
			node,
			fmt.Sprintf(`cd %s && PGPORT=26257 PGUSER=root PGSSLMODE=disable PGDATABASE=postgres go test -list "%s"`, libPQPath, testListRegex),
		)
		require.NoError(t, err)

		// Convert the output of go test -list into an list.
		tests := strings.Fields(string(buf))
		var allowedTests []string

		compiledTestListRegex, err := regexp.Compile(testListRegex)
		require.NoError(t, err)
		for _, testName := range tests {
			// Ignore tests that do not match the test regex pattern.
			matched := compiledTestListRegex.MatchString(testName)
			if !matched {
				continue
			}
			// If the test is part of ignoreList, do not run the test.
			if _, ok := ignoreList[testName]; !ok {
				allowedTests = append(allowedTests, testName)
			}
		}

		allowedTestsRegExp := fmt.Sprintf(`"^(%s)$"`, strings.Join(allowedTests, "|"))

		// Ignore the error as there will be failing tests.
		_ = c.RunE(
			ctx,
			node,
			fmt.Sprintf("cd %s && PGPORT=26257 PGUSER=root PGSSLMODE=disable PGDATABASE=postgres go test -run %s -v 2>&1 | %s/bin/go-junit-report > %s",
				libPQPath, allowedTestsRegExp, goPath, resultsPath),
		)

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "lib/pq" /* ormName */, []byte(resultsPath),
			blocklistName, expectedFailures, ignoreList, version, latestTag,
		)
	}

	r.Add(testSpec{
		Name:       "lib/pq",
		Owner:      OwnerAppDev,
		MinVersion: "v20.2.0",
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `driver`},
		Run:        runLibPQ,
	})
}
