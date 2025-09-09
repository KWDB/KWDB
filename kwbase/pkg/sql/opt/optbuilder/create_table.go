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

package optbuilder

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	if ct.IsTS() && ct.Temporary {
		panic(sqlbase.TSUnsupportedError("temporary"))
	}
	isTemp := resolveTemporaryStatus(&ct.Table, ct.Temporary)
	if isTemp {
		// Postgres allows using `pg_temp` as an alias for the session specific temp
		// schema. In PG, the following are equivalent:
		// CREATE TEMP TABLE t <=> CREATE TABLE pg_temp.t <=> CREATE TEMP TABLE pg_temp.t
		//
		// The temporary schema is created the first time a session creates
		// a temporary object, so it is possible to use `pg_temp` in a fully
		// qualified name when the temporary schema does not exist. To allow this,
		// we explicitly set the SchemaName to `public` for temporary tables, as
		// the public schema is guaranteed to exist. This ensures the FQN can be
		// resolved correctly.
		// TODO(solon): Once it is possible to drop schemas, it will no longer be
		// safe to set the schema name to `public`, as it may have been dropped.
		ct.Table.TableNamePrefix.SchemaName = tree.PublicSchemaName
		ct.Temporary = true
	}
	sch, resName := b.resolveSchemaForCreate(&ct.Table, ct.TableType)
	ct.Table.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	if ct.AsFrom {
		_, srcRseName := b.resolveSchemaForCreate(&ct.SrcTable, tree.ColumnBasedTable)
		ct.SrcTable.TableNamePrefix = srcRseName
	}

	// HoistConstraints normalizes any column constraints in the CreateTable AST
	// node.
	ct.HoistConstraints()

	var input memo.RelExpr
	var inputCols physical.Presentation
	if ct.As() {
		if sch.GetDatabaseType() == tree.EngineTypeTimeseries {
			panic(sqlbase.TSUnsupportedError("create table as"))
		}
		// The execution code might need to stringify the query to run it
		// asynchronously. For that we need the data sources to be fully qualified.
		// TODO(radu): this interaction is pretty hacky, investigate moving the
		// generation of the string to the optimizer.
		b.qualifyDataSourceNamesInAST = true
		defer func() {
			b.qualifyDataSourceNamesInAST = false
		}()

		b.pushWithFrame()
		// Build the input query.
		outScope = b.buildStmt(ct.AsSource, nil /* desiredTypes */, inScope)
		b.popWithFrame(outScope)

		numColNames := 0
		for i := 0; i < len(ct.Defs); i++ {
			if _, ok := ct.Defs[i].(*tree.ColumnTableDef); ok {
				numColNames++
			}
		}
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns)))))
		}

		input = outScope.expr
		if !ct.AsHasUserSpecifiedPrimaryKey() {
			// Synthesize rowid column, and append to end of column list.
			props, overloads := builtins.GetBuiltinProperties("unique_rowid")
			private := &memo.FunctionPrivate{
				Name:       "unique_rowid",
				Typ:        types.Int,
				Properties: props,
				Overload:   &overloads[0],
			}
			fn := b.factory.ConstructFunction(memo.EmptyScalarListExpr, private)
			scopeCol := b.synthesizeColumn(outScope, "rowid", types.Int, nil /* expr */, fn)
			input = b.factory.CustomFuncs().ProjectExtraCol(outScope.expr, fn, scopeCol.id)
		}
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	outScope = b.allocScope()
	if len(ct.Instances) != 0 {
		b.synthesizeResultColumns(outScope, sqlbase.CreateChildTablesColumns)
	}
	outScope.expr = b.factory.ConstructCreateTable(
		input,
		&memo.CreateTablePrivate{
			Schema:    schID,
			InputCols: inputCols,
			Columns:   colsToColList(outScope.cols),
			Syntax:    ct,
		},
	)
	return outScope
}
