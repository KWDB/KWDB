// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDescribe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	db := sqlutils.MakeSQLRunner(rawDB)

	// Workspace.
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `SET DATABASE = test`)

	// Prepare source and destination tables.
	db.Exec(t, `CREATE TABLE like_src (
		id INT PRIMARY KEY,
		a  INT NOT NULL DEFAULT 1,
		b  STRING,
		INDEX like_src_a_idx (a),
		UNIQUE (b)
	)`)
	db.Exec(t, `CREATE TABLE like_dst LIKE like_src`)

	// DESCRIBE behaves consistently with SHOW COLUMNS (at least on column name + type set).
	showRows := db.QueryStr(t, `SELECT column_name, data_type FROM [SHOW COLUMNS FROM like_dst]`)
	descRows := db.QueryStr(t, `DESCRIBE like_dst`)
	if len(descRows) != len(showRows) {
		t.Fatalf("DESCRIBE rows %d != SHOW rows %d", len(descRows), len(showRows))
	}
	// Build a map of expected (name,type) pairs.
	expected := map[string]struct{}{}
	for _, r := range showRows {
		if len(r) >= 2 {
			expected[r[0]+"/"+r[1]] = struct{}{}
		}
	}
	for _, r := range descRows {
		if len(r) < 2 {
			t.Fatalf("unexpected DESCRIBE row shape: %v", r)
		}
		key := r[0] + "/" + r[1]
		if _, ok := expected[key]; !ok {
			t.Fatalf("DESCRIBE mismatch on column/type: %v", r)
		}
	}
}
