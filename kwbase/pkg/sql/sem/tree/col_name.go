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

package tree

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// GetRenderColName computes a name for a result column.
// A name specified with AS takes priority, otherwise a name
// is derived from the expression.
//
// This function is meant to be used on untransformed syntax trees.
//
// The algorithm is borrowed from FigureColName() in PostgreSQL 10, to be
// found in src/backend/parser/parse_target.c. We reuse this algorithm
// to provide names more compatible with PostgreSQL.
func GetRenderColName(searchPath sessiondata.SearchPath, target SelectExpr) (string, error) {
	if target.As != "" {
		return string(target.As), nil
	}

	_, s, err := ComputeColNameInternal(searchPath, target.Expr)
	if err != nil {
		return s, err
	}
	if len(s) == 0 {
		s = "?column?"
	}
	return s, nil
}

// ComputeColNameInternal is the workhorse for GetRenderColName.
// The return value indicates the strength of the confidence in the result:
// 0 - no information
// 1 - second-best name choice
// 2 - good name choice
//
// The algorithm is borrowed from FigureColnameInternal in PostgreSQL 10,
// to be found in src/backend/parser/parse_target.c.
func ComputeColNameInternal(sp sessiondata.SearchPath, target Expr) (int, string, error) {
	// The order of the type cases below mirrors that of PostgreSQL's
	// own code, so that code reviews can more easily compare the two
	// implementations.
	switch e := target.(type) {
	case *UnresolvedName:
		if e.Star {
			return 0, "", nil
		}
		return 2, e.Parts[0], nil

	case *ColumnItem:
		return 2, e.Column(), nil

	case *IndirectionExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *FuncExpr:
		fd, err := e.Func.Resolve(sp)
		if err != nil {
			return 0, "", err
		}
		// if fd.Name == "last" || fd.Name == "last_row" {
		// 	// get column name
		// 	colName := ""
		// 	if colItem, ok := e.Exprs[0].(*ColumnItem); ok {
		// 		colName = string(colItem.ColumnName)
		// 	}
		// 	return 2, fd.Name + "(" + colName + ")", nil
		// }
		return 2, fd.Name, nil

	case *NullIfExpr:
		return 2, "nullif", nil

	case *IfExpr:
		return 2, "if", nil

	case *ParenExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *CastExpr:
		strength, s, err := ComputeColNameInternal(sp, e.Expr)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			return 1, computeCastName(e.Type), nil
		}
		return strength, s, nil

	case *AnnotateTypeExpr:
		// Ditto CastExpr.
		strength, s, err := ComputeColNameInternal(sp, e.Expr)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			return 1, computeCastName(e.Type), nil
		}
		return strength, s, nil

	case *CollateExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *ArrayFlatten:
		return 2, "array", nil

	case *Subquery:
		if e.Exists {
			return 2, "exists", nil
		}
		return computeColNameInternalSubquery(sp, e.Select)

	case *CaseExpr:
		strength, s, err := 0, "", error(nil)
		if e.Else != nil {
			strength, s, err = ComputeColNameInternal(sp, e.Else)
		}
		if strength <= 1 {
			s = "case"
			strength = 1
		}
		return strength, s, err

	case *Array:
		return 2, "array", nil

	case *Tuple:
		if e.Row {
			return 2, "row", nil
		}
		if len(e.Exprs) == 1 {
			if len(e.Labels) > 0 {
				return 2, e.Labels[0], nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0])
		}

	case *CoalesceExpr:
		return 2, "coalesce", nil

		// CockroachDB-specific nodes follow.
	case *IfErrExpr:
		if e.Else == nil {
			return 2, "iserror", nil
		}
		return 2, "iferror", nil

	case *ColumnAccessExpr:
		return 2, e.ColName, nil

	case *UserDefinedVar:
		return 2, e.VarName, nil

	case *AssignmentExpr:
		return 2, e.String(), nil

	case *DBool:
		// PostgreSQL implements the "true" and "false" literals
		// by generating the expressions 't'::BOOL and 'f'::BOOL, so
		// the derived column name is just "bool". Do the same.
		return 1, "bool", nil
	case dNull:
		return 1, "null", nil
	}

	return 0, "", nil
}

// computeColNameInternalSubquery handles the cases of subqueries that
// cannot be handled by the function above due to the Go typing
// differences.
func computeColNameInternalSubquery(
	sp sessiondata.SearchPath, s SelectStatement,
) (int, string, error) {
	switch e := s.(type) {
	case *ParenSelect:
		return computeColNameInternalSubquery(sp, e.Select.Select)
	case *ValuesClause:
		if len(e.Rows) > 0 && len(e.Rows[0]) == 1 {
			return 2, "column1", nil
		}
	case *SelectClause:
		if len(e.Exprs) == 1 {
			if len(e.Exprs[0].As) > 0 {
				return 2, string(e.Exprs[0].As), nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0].Expr)
		}
	}
	return 0, "", nil
}

// computeCastName returns the name manufactured by Postgres for a computed (or
// annotated, in case of KWDB) column.
func computeCastName(typ *types.T) string {
	// Postgres uses the array element type name in case of array casts.
	if typ.Family() == types.ArrayFamily {
		typ = typ.ArrayContents()
	}
	return typ.PGName()

}
