// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// buildSelectInto builds a SELECT ... INTO ... statement.
func (b *Builder) buildSelectInto(sel *tree.SelectInto, inScope *scope) (outScope *scope) {
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(sel.Values, nil, emptyScope)
	if len(inputScope.cols) != len(sel.Names) {
		panic(pgerror.Newf(
			pgcode.Syntax, "The used SELECT statements have a different number of columns",
		))
	}

	var vars opt.VarNames
	for _, name := range sel.Names {
		varName := strings.ToLower(name.VarName)
		vars = append(vars, varName)
	}

	outScope = inScope.push()
	outScope.expr = b.factory.ConstructSelectInto(
		inputScope.expr.(memo.RelExpr),
		&memo.SelectIntoPrivate{
			Vars:  vars,
			Props: inputScope.makePhysicalProps(),
		},
	)
	return outScope
}
