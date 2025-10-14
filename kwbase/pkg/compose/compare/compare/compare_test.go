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

// "make test" would normally test this file, but it should only be tested
// within docker compose.

//go:build compose
// +build compose

package compare

import (
	"context"
	"flag"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/smithcmp/cmpconn"
	"gitee.com/kwbasedb/kwbase/pkg/internal/sqlsmith"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

var (
	flagEach      = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagArtifacts = flag.String("artifacts", "", "artifact directory")
)

func TestCompare(t *testing.T) {
	uris := map[string]struct {
		addr string
		init []string
	}{
		"postgres": {
			addr: "postgresql://postgres@postgres:5432/",
			init: []string{
				"drop schema if exists public cascade",
				"create schema public",
			},
		},
		"kwbase1": {
			addr: "postgresql://root@kwbase1:26257/?sslmode=disable",
			init: []string{
				"drop database if exists defaultdb",
				"create database defaultdb",
			},
		},
		"kwbase2": {
			addr: "postgresql://root@kwbase2:26257/?sslmode=disable",
			init: []string{
				"drop database if exists defaultdb",
				"create database defaultdb",
			},
		},
	}
	configs := map[string]testConfig{
		"postgres": {
			setup:         sqlsmith.Setups["rand-tables"],
			setupMutators: []sqlbase.Mutator{mutations.PostgresCreateTableMutator},
			opts:          []sqlsmith.SmitherOption{sqlsmith.PostgresMode()},
			conns: []testConn{
				{
					name:     "kwbase1",
					mutators: []sqlbase.Mutator{},
				},
				{
					name:     "postgres",
					mutators: []sqlbase.Mutator{mutations.PostgresMutator},
				},
			},
		},
		"mutators": {
			setup: sqlsmith.Setups["rand-tables"],
			opts:  []sqlsmith.SmitherOption{sqlsmith.CompareMode()},
			conns: []testConn{
				{
					name:     "kwbase1",
					mutators: []sqlbase.Mutator{},
				},
				{
					name: "kwbase2",
					mutators: []sqlbase.Mutator{
						mutations.StatisticsMutator,
						mutations.ForeignKeyMutator,
						mutations.ColumnFamilyMutator,
						mutations.StatisticsMutator,
						mutations.IndexStoringMutator,
					},
				},
			},
		},
	}

	ctx := context.Background()
	for confName, config := range configs {
		t.Run(confName, func(t *testing.T) {
			rng, _ := randutil.NewPseudoRand()
			setup := config.setup(rng)
			setup, _ = mutations.ApplyString(rng, setup, config.setupMutators...)

			conns := map[string]*cmpconn.Conn{}
			for _, testCn := range config.conns {
				uri, ok := uris[testCn.name]
				if !ok {
					t.Fatalf("bad connection name: %s", testCn.name)
				}
				conn, err := cmpconn.NewConn(uri.addr, rng, testCn.mutators)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				for _, init := range uri.init {
					if err := conn.Exec(ctx, init); err != nil {
						t.Fatalf("%s: %v", testCn.name, err)
					}
				}
				connSetup, _ := mutations.ApplyString(rng, setup, testCn.mutators...)
				if err := conn.Exec(ctx, connSetup); err != nil {
					t.Log(connSetup)
					t.Fatalf("%s: %v", testCn.name, err)
				}
				conns[testCn.name] = conn
			}
			smither, err := sqlsmith.NewSmither(conns[config.conns[0].name].DB, rng, config.opts...)
			if err != nil {
				t.Fatal(err)
			}

			until := time.After(*flagEach)
			for {
				select {
				case <-until:
					return
				default:
				}
				query := smither.Generate()
				query, _ = mutations.ApplyString(rng, query, mutations.PostgresMutator)
				if err := cmpconn.CompareConns(ctx, time.Second*30, conns, "" /* prep */, query); err != nil {
					path := filepath.Join(*flagArtifacts, confName+".log")
					if err := ioutil.WriteFile(path, []byte(err.Error()), 0666); err != nil {
						t.Log(err)
					}
					t.Fatal(err)
				}
				// Make sure we can still ping on a connection. If we can't we may have
				// crashed something.
				for name, conn := range conns {
					if err := conn.Ping(); err != nil {
						t.Log(query)
						t.Fatalf("%s: ping: %v", name, err)
					}
				}
			}
		})
	}
}

type testConfig struct {
	opts          []sqlsmith.SmitherOption
	conns         []testConn
	setup         sqlsmith.Setup
	setupMutators []sqlbase.Mutator
}

type testConn struct {
	name     string
	mutators []sqlbase.Mutator
}
