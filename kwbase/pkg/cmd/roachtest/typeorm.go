// Copyright 2018 The Cockroach Authors.
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
)

var typeORMReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedTypeORMRelease = "0.2.24"

// This test runs TypeORM's full test suite against a single kwbase node.
func registerTypeORM(r *testRegistry) {
	runTypeORM := func(
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

		t.Status("cloning TypeORM and installing prerequisites")
		latestTag, err := repeatGetLatestTag(ctx, c, "typeorm", "typeorm", typeORMReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest TypeORM release is %s.", latestTag)
		c.l.Printf("Supported TypeORM release is %s.", supportedTypeORMRelease)

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
			`sudo apt-get -qq install make python3 libpq-dev python-dev gcc g++ `+
				`python-software-properties build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install nodejs and npm", `sudo apt-get -qq install nodejs`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "update npm", `sudo npm i -g npm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old TypeORM", `sudo rm -rf /mnt/data1/typeorm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/typeorm/typeorm.git",
			"/mnt/data1/typeorm",
			supportedTypeORMRelease,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// TypeORM is super picky about this file format and if it cannot be parsed
		// it will return a file not found error.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for kwbase only",
			fmt.Sprintf("echo '%s' > /mnt/data1/typeorm/ormconfig.json", typeORMConfigJSON),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"patch TypeORM test script to run all tests even on failure",
			`sed -i 's/--bail //' /mnt/data1/typeorm/package.json`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"building TypeORM",
			`cd /mnt/data1/typeorm/ && sudo npm install --unsafe-perm=true --allow-root`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("running TypeORM test suite - approx 12 mins")
		rawResults, err := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/typeorm/ && sudo npm test --unsafe-perm=true --allow-root`,
		)
		rawResultsStr := string(rawResults)
		c.l.Printf("Test Results: %s", rawResultsStr)
		if err != nil {
			// Ignore the failure discussed in #38180 and in
			// https://github.com/typeorm/typeorm/pull/4298.
			// TODO(jordanlewis): remove this once the failure is resolved.
			if t.IsBuildVersion("v19.2.0") &&
				strings.Contains(rawResultsStr, "1 failing") &&
				strings.Contains(rawResultsStr, "AssertionError: expected 2147483647 to equal '2147483647'") {
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	r.Add(testSpec{
		Name:       "typeorm",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v19.1.0",
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTypeORM(ctx, t, c)
		},
	})
}

// This full config is required, but notice that all the non-kwbase databases
// are set to skip.  Some of the unit tests look for a specific config, like
// sqlite and will fail if it is not present.
const typeORMConfigJSON = `
[
  {
    "skip": true,
    "name": "mysql",
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mariadb",
    "type": "mariadb",
    "host": "localhost",
    "port": 3307,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "sqlite",
    "type": "sqlite",
    "database": "temp/sqlitedb.db",
    "logging": false
  },
  {
    "skip": true,
    "name": "postgres",
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "username": "test",
    "password": "test",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mssql",
    "type": "mssql",
    "host": "localhost",
    "username": "sa",
    "password": "Admin12345",
    "database": "tempdb",
    "logging": false
  },
  {
    "skip": true,
    "name": "oracle",
    "type": "oracle",
    "host": "localhost",
    "username": "system",
    "password": "oracle",
    "port": 1521,
    "sid": "xe.oracle.docker",
    "logging": false
  },
  {
    "skip": false,
    "name": "kwbasedb",
    "type": "kwbasedb",
    "host": "localhost",
    "port": 26257,
    "username": "root",
    "password": "",
    "database": "defaultdb"
  },
  {
    "skip": true,
    "disabledIfNotEnabledImplicitly": true,
    "name": "mongodb",
    "type": "mongodb",
    "database": "test",
    "logging": false,
    "useNewUrlParser": true
  }
]
`
