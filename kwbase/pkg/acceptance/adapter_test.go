// Copyright 2017 The Cockroach Authors.
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

package acceptance

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func TestDockerC(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	t.Run("Success", func(t *testing.T) {
		testDockerSuccess(ctx, t, "c", []string{
			"sh", "-c",
			`cd /mnt/data/c && make test && ./test 'SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12'`,
		})
	})
	t.Run("Fail", func(t *testing.T) {
		testDockerFail(ctx, t, "c", []string{
			"sh", "-c",
			`cd /mnt/data/c && make test && ./test 'SELECT 1'`,
		})
	})
}

func TestDockerCSharp(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "csharp", []string{"sh", "-c", "cd /mnt/data/csharp && dotnet run"})
	testDockerFail(ctx, t, "csharp", []string{"sh", "-c", "cd /mnt/data/csharp && dotnet notacommand"})
}

func TestDockerJava(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "java", []string{"sh", "-c", "cd /mnt/data/java && mvn -o test"})
	testDockerFail(ctx, t, "java", []string{"sh", "-c", "cd /mnt/data/java && mvn -o foobar"})
}

func TestDockerElixir(t *testing.T) {
	t.Skip("Elixir requires network to run, which can flake. When attempting to update this (#52341), the new Elixir version does not work with KWDB/TLS.")

	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "elixir", []string{"sh", "-c", "cd /mnt/data/elixir/test_kwdb && mix local.hex --force && mix deps.get && psql -c 'CREATE DATABASE IF NOT EXISTS testdb' && mix test"})
	testDockerFail(ctx, t, "elixir", []string{"sh", "-c", "cd /mnt/data/elixir/test_kwdb && mix local.hex --force && mix deps.get && mix thisshouldfail"})
}

func TestDockerNodeJS(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	const nodeJS = `
	set -e
	cd /mnt/data/node

	export SHOULD_FAIL=%v
	# Get access to globally installed node modules.
	export NODE_PATH=$NODE_PATH:/usr/lib/node
	/usr/lib/node/.bin/mocha .
	`

	ctx := context.Background()
	testDockerSuccess(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "", 1)})
	testDockerFail(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "fail", 1)})
}

func TestDockerPHP(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 3"})
	testDockerFail(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 1"})
}

func TestDockerPSQL(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "psql", []string{"/mnt/data/psql/test-psql.sh"})
}

func TestDockerPython(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "python", []string{"sh", "-c", "cd /mnt/data/python && python test.py 3"})
	testDockerFail(ctx, t, "python", []string{"sh", "-c", "cd /mnt/data/python && python test.py 2"})
}

func TestDockerRuby(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "ruby", []string{"sh", "-c", "cd /mnt/data/ruby && ruby test.rb 3"})
	testDockerFail(ctx, t, "ruby", []string{"sh", "-c", "cd /mnt/data/ruby && ruby test.rb 1"})
}
