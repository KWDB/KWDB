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

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestCommentOnIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT, INDEX t_c_idx (c));
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON INDEX t_c_idx IS 'index_comment'`,
			`SELECT obj_description(oid) from pg_class WHERE relname='t_c_idx';`,
			gosql.NullString{String: `index_comment`, Valid: true},
		},
		{
			`TRUNCATE t`,
			`SELECT obj_description(oid) from pg_class WHERE relname='t_c_idx';`,
			gosql.NullString{String: `index_comment`, Valid: true},
		},
		{
			`COMMENT ON INDEX t_c_idx IS NULL`,
			`SELECT obj_description(oid) from pg_class WHERE relname='t_c_idx';`,
			gosql.NullString{Valid: false},
		},
	}

	for _, tc := range testCases {
		if _, err := db.Exec(tc.exec); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(tc.query)
		var comment gosql.NullString
		if err := row.Scan(&comment); err != nil {
			t.Fatal(err)
		}
		if tc.expect != comment {
			t.Fatalf("expected comment %v, got %v", tc.expect, comment)
		}
	}
}

func TestCommentOnIndexWhenDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT, INDEX t_c_idx (c));
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON INDEX t_c_idx IS 'index_comment'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP TABLE t`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT "comment" FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}

func TestCommentOnIndexWhenDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT, INDEX t_c_idx (c));
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON INDEX t_c_idx IS 'index_comment'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP INDEX t_c_idx`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT "comment" FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}
