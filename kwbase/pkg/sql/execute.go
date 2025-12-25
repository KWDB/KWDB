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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// fillInPlaceholder helps with the EXECUTE foo(args) SQL statement: it takes in
// a prepared statement returning
// the referenced prepared statement and correctly updated placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func (ex *connExecutor) fillInPlaceholders(
	psInterface prepare.PreparedResult, name string, params tree.Exprs, oldSemaCtx *tree.SemaContext,
) (*tree.PlaceholderInfo, error) {
	ps := psInterface.(*PreparedStatement)
	loc := ex.planner.semaCtx.Location
	if len(ps.Types) != len(params) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(ps.Types), len(params))
	}

	qArgs := make(tree.QueryArguments, len(params))
	var semaCtx tree.SemaContext
	semaCtx.Location = loc
	semaCtx.UserDefinedVars = oldSemaCtx.UserDefinedVars
	semaCtx.ProcUserDefinedVars = oldSemaCtx.ProcUserDefinedVars
	for i, e := range params {
		idx := tree.PlaceholderIdx(i)

		typ, ok := ps.ValueType(idx)
		if !ok {
			return nil, errors.AssertionFailedf("no type for placeholder %s", idx)
		}
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(
			e, typ, "EXECUTE parameter", /* context */
			&semaCtx, true /* allowImpure */, ps.needTSTypeCheck, name)
		if err != nil {
			return nil, pgerror.New(pgcode.WrongObjectType, err.Error())
		}

		qArgs[idx] = typedExpr
	}
	return &tree.PlaceholderInfo{
		Values: qArgs,
		PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
			TypeHints: ps.TypeHints,
			Types:     ps.Types,
		},
	}, nil
}
