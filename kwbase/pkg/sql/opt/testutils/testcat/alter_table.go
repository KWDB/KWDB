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

package testcat

import (
	gojson "encoding/json"
	"fmt"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// AlterTable is a partial implementation of the ALTER TABLE statement.
//
// Supported commands:
//   - INJECT STATISTICS: imports table statistics from a JSON object.
func (tc *Catalog) AlterTable(stmt *tree.AlterTable) {
	tn := stmt.Table.ToTableName()
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&tn)
	tab := tc.Table(&tn)

	for _, cmd := range stmt.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterTableInjectStats:
			injectTableStats(tab, t.Stats)

		default:
			panic(fmt.Sprintf("unsupported ALTER TABLE command %T", t))
		}
	}
}

// injectTableStats sets the table statistics as specified by a JSON object.
func injectTableStats(tt *Table, statsExpr tree.Expr) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	typedExpr, err := tree.TypeCheckAndRequire(
		statsExpr, &semaCtx, types.Jsonb, "INJECT STATISTICS",
	)
	if err != nil {
		panic(err)
	}
	val, err := typedExpr.Eval(&evalCtx)
	if err != nil {
		panic(err)
	}

	if val == tree.DNull {
		panic("statistics cannot be NULL")
	}
	jsonStr := val.(*tree.DJSON).JSON.String()
	var stats []stats.JSONStatistic
	if err := gojson.Unmarshal([]byte(jsonStr), &stats); err != nil {
		panic(err)
	}
	tt.Stats = make([]*TableStat, len(stats))
	for i := range stats {
		tt.Stats[i] = &TableStat{js: stats[i], tt: tt}
	}
	// Call ColumnOrdinal on all possible columns to assert that
	// the column names are valid.
	for _, ts := range tt.Stats {
		for i := 0; i < ts.ColumnCount(); i++ {
			ts.ColumnOrdinal(i)
		}
	}

	// Finally, sort the stats with most recent first.
	sort.Sort(tt.Stats)
}
