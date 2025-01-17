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

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// Regression tests for #22304.
// Checks that a mutation with RETURNING checks low-level constraints
// before returning anything -- or that at least no constraint-violating
// values are visible to the client.
func TestConstraintValidationBeforeBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
CREATE DATABASE d;
CREATE TABLE d.a(a INT PRIMARY KEY);
INSERT INTO d.a(a) VALUES (1);
	`); err != nil {
		t.Fatal(err)
	}

	step1 := func() (*gosql.Rows, error) {
		return db.Query("INSERT INTO d.a(a) TABLE generate_series(1,3000) RETURNING a")
	}
	step2 := func() (*gosql.Rows, error) {
		if _, err := db.Exec(`INSERT INTO d.a(a) TABLE generate_series(2, 3000)`); err != nil {
			return nil, err
		}
		return db.Query("UPDATE d.a SET a = a - 1 WHERE a > 1 RETURNING a")
	}
	for i, step := range []func() (*gosql.Rows, error){step1, step2} {
		rows, err := step()
		if err != nil {
			if !testutils.IsError(err, `duplicate key value \(a\)=\(1\)`) {
				t.Errorf("%d: %v", i, err)
			}
		} else {
			defer rows.Close()

			hasNext := rows.Next()
			if !hasNext {
				t.Errorf("%d: returning claims to return no error, yet returns no rows either", i)
			} else {
				var val int
				err := rows.Scan(&val)

				if err != nil {
					if !testutils.IsError(err, `duplicate key value \(a\)=\(1\)`) {
						t.Errorf("%d: %v", i, err)
					}
				} else {
					// No error. Maybe it'll come later.
					if val == 1 {
						t.Errorf("%d: returning returns rows, including an invalid duplicate", i)
					}

					for rows.Next() {
						err := rows.Scan(&val)
						if err != nil {
							if !testutils.IsError(err, `duplicate key value \(a\)=\(1\)`) {
								t.Errorf("%d: %v", i, err)
							}
						}
						if val == 1 {
							t.Errorf("%d returning returns rows, including an invalid duplicate", i)
						}
					}
				}
			}
		}
	}
}
