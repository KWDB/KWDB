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

package mutations

import (
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestPostgresMutator(t *testing.T) {
	q := `
		CREATE TABLE t (s STRING FAMILY fam1, b BYTES, FAMILY fam2 (b), PRIMARY KEY (s ASC, b DESC), INDEX (s) STORING (b))
		    PARTITION BY LIST (s)
		        (
		            PARTITION europe_west VALUES IN ('a', 'b')
		        );
		ALTER TABLE table1 INJECT STATISTICS 'blah';
		SET CLUSTER SETTING "sql.stats.automatic_collection.enabled" = false;
	`

	rng, _ := randutil.NewPseudoRand()
	{
		mutated, changed := ApplyString(rng, q, PostgresMutator)
		if !changed {
			t.Fatal("expected changed")
		}
		mutated = strings.TrimSpace(mutated)
		expect := `CREATE TABLE t (s TEXT, b BYTEA, PRIMARY KEY (s ASC, b DESC), INDEX (s) INCLUDE (b));`
		if mutated != expect {
			t.Fatalf("unexpected: %s", mutated)
		}
	}
	{
		mutated, changed := ApplyString(rng, q, PostgresCreateTableMutator, PostgresMutator)
		if !changed {
			t.Fatal("expected changed")
		}
		mutated = strings.TrimSpace(mutated)
		expect := "CREATE TABLE t (s TEXT, b BYTEA, PRIMARY KEY (s, b));\nCREATE INDEX ON t (s) INCLUDE (b);"
		if mutated != expect {
			t.Fatalf("unexpected: %s", mutated)
		}
	}
}

func TestApply(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	// Test with a simple CREATE TABLE statement
	q := `CREATE TABLE t (id INT PRIMARY KEY, name STRING);`
	parsed, err := parser.Parse(q)
	if err != nil {
		t.Fatal(err)
	}

	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}

	// Apply ColumnFamilyMutator
	mutated, changed := Apply(rng, stmts, ColumnFamilyMutator)
	if !changed {
		t.Fatal("expected changed")
	}
	if len(mutated) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(mutated))
	}
}

func TestApplyString(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	// Test with a simple CREATE TABLE statement
	q := `CREATE TABLE t (id INT PRIMARY KEY, name STRING);`
	mutated, changed := ApplyString(rng, q, ColumnFamilyMutator)
	if !changed {
		t.Fatal("expected changed")
	}
	if !strings.Contains(mutated, "CREATE TABLE") {
		t.Fatalf("expected CREATE TABLE in result, got %s", mutated)
	}
}

func TestRandNonNegInt(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	// Test that randNonNegInt returns a non-negative integer
	for i := 0; i < 100; i++ {
		v := randNonNegInt(rng)
		if v < 0 {
			t.Fatalf("expected non-negative integer, got %d", v)
		}
	}
}

func TestStatisticsMutator(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	// Test with a simple CREATE TABLE statement
	q := `CREATE TABLE t (id INT PRIMARY KEY, name STRING);`
	parsed, err := parser.Parse(q)
	if err != nil {
		t.Fatal(err)
	}

	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}

	// Apply StatisticsMutator
	mutated, changed := statisticsMutator(rng, stmts)
	if !changed {
		t.Fatal("expected changed")
	}
	if len(mutated) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(mutated))
	}
}

func TestForeignKeyMutator(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	// Test with two CREATE TABLE statements
	q := `
		CREATE TABLE t1 (id INT PRIMARY KEY, name STRING);
		CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT);
	`
	parsed, err := parser.Parse(q)
	if err != nil {
		t.Fatal(err)
	}

	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}

	// Apply ForeignKeyMutator
	mutated, _ := foreignKeyMutator(rng, stmts)
	// The mutator may or may not add foreign keys, so we just check that it doesn't panic
	if len(mutated) < 2 {
		t.Fatalf("expected at least 2 statements, got %d", len(mutated))
	}
}
