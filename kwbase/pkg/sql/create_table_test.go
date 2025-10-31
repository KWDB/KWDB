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

package sql

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIsTypeSupportedInVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		v clusterversion.VersionKey
		t *types.T

		ok bool
	}{
		{clusterversion.Version19_2, types.Time, true},
		{clusterversion.Version19_2, types.Timestamp, true},
		{clusterversion.Version19_2, types.Interval, true},

		{clusterversion.Version19_2, types.TimeTZ, false},
		{clusterversion.VersionTimeTZType, types.TimeTZ, true},

		{clusterversion.Version19_2, types.MakeTime(0), false},
		{clusterversion.Version19_2, types.MakeTimeTZ(0), false},
		{clusterversion.VersionTimeTZType, types.MakeTimeTZ(0), false},
		{clusterversion.Version19_2, types.MakeTimestamp(0), false},
		{clusterversion.Version19_2, types.MakeTimestampTZ(0), false},
		{
			clusterversion.Version19_2,
			types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			false,
		},
		{
			clusterversion.Version19_2,
			types.MakeInterval(
				types.IntervalTypeMetadata{
					DurationField: types.IntervalDurationField{DurationType: types.IntervalDurationType_SECOND},
				},
			),
			false,
		},
		{clusterversion.VersionTimePrecision, types.MakeTime(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimeTZ(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimestamp(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimestampTZ(0), true},
		{
			clusterversion.VersionTimePrecision,
			types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			true,
		},
		{
			clusterversion.VersionTimePrecision,
			types.MakeInterval(
				types.IntervalTypeMetadata{
					DurationField: types.IntervalDurationField{DurationType: types.IntervalDurationType_SECOND},
				},
			),
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s:%s", tc.v, tc.t.SQLString()), func(t *testing.T) {
			ok, err := isTypeSupportedInVersion(
				clusterversion.ClusterVersion{Version: clusterversion.VersionByKey(tc.v)},
				tc.t,
			)
			require.NoError(t, err)
			require.Equal(t, tc.ok, ok)
		})
	}
}

func TestCreateTableLike(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	db := sqlutils.MakeSQLRunner(rawDB)

	// Workspace.
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `SET DATABASE = test`)

	// Source table with defaults and indexes.
	db.Exec(t, `CREATE TABLE like_src (
		id INT PRIMARY KEY,
		a  INT NOT NULL DEFAULT 1,
		b  STRING,
		INDEX like_src_a_idx (a),
		UNIQUE (b)
	)`)

	// Create destination from LIKE.
	db.Exec(t, `CREATE TABLE like_dst LIKE like_src`)

	// Columns and types copied.
	db.CheckQueryResults(t,
		`SELECT column_name, data_type FROM [SHOW COLUMNS FROM like_dst] ORDER BY column_name`,
		[][]string{
			{"a", "INT4"},
			{"b", "STRING"},
			{"id", "INT4"},
		},
	)

	// Default on column a copied.
	db.Exec(t, `INSERT INTO like_dst (id, b) VALUES (100, 'x')`)
	db.CheckQueryResults(t, `SELECT a FROM like_dst WHERE id = 100`, [][]string{{"1"}})

	// Indexes copied: non-unique on a, unique on b.
	db.CheckQueryResults(t,
		`SELECT count(*) FROM [SHOW INDEXES FROM like_dst] WHERE column_name = 'a' AND non_unique = true`,
		[][]string{{"1"}},
	)
	db.CheckQueryResults(t,
		`SELECT count(*) FROM [SHOW INDEXES FROM like_dst] WHERE column_name = 'b' AND non_unique = false`,
		[][]string{{"1"}},
	)
}
