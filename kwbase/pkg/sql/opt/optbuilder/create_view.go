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

package optbuilder

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

func (b *Builder) buildCreateView(cv *tree.CreateView, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, _ := b.resolveSchemaForCreate(&cv.Name, tree.RelationalTable)
	schID := b.factory.Metadata().AddSchema(sch)
	if sch.GetDatabaseType() == tree.EngineTypeTimeseries {
		panic(sqlbase.TSUnsupportedError("create view"))
	}

	// We build the select statement to:
	//  - check the statement semantically,
	//  - get the fully resolved names into the AST, and
	//  - collect the view dependencies in b.viewDeps.
	// The result is not otherwise used.
	b.insideObjectDef.SetFlags(InsideViewDef)
	b.trackViewDeps = true
	b.qualifyDataSourceNamesInAST = true
	defer func() {
		b.insideObjectDef.ClearFlags(InsideViewDef)
		b.trackViewDeps = false
		b.viewDeps = nil
		b.qualifyDataSourceNamesInAST = false
	}()

	b.pushWithFrame()
	defScope := b.buildStmtAtRoot(cv.AsSource, nil /* desiredTypes */, inScope)

	// create view not applicable to timeseries table or in timeseries database.
	if b.PhysType == tree.TS || sch.GetDatabaseType() == tree.EngineTypeTimeseries {
		panic(pgerror.New(pgcode.FeatureNotSupported, "create view is not supported in timeseries databases"))
	}
	b.popWithFrame(defScope)

	p := defScope.makePhysicalProps().Presentation
	if len(cv.ColumnNames) != 0 {
		if len(p) != len(cv.ColumnNames) {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
				len(cv.ColumnNames), util.Pluralize(int64(len(cv.ColumnNames))),
				len(p), util.Pluralize(int64(len(p)))),
			))
		}
		// Override the columns.
		for i := range p {
			p[i].Alias = string(cv.ColumnNames[i])
		}
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateView(
		&memo.CreateViewPrivate{
			Schema:       schID,
			ViewName:     cv.Name.Table(),
			IfNotExists:  cv.IfNotExists,
			Temporary:    cv.Temporary,
			Materialized: cv.Materialized,
			ViewQuery:    tree.AsStringWithFlags(cv.AsSource, tree.FmtParsable),
			Columns:      p,
			Deps:         b.viewDeps,
		},
	)
	return outScope
}
