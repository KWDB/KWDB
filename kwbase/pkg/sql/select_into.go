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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type selectIntoNode struct {
	rows planNode
	vars []string
	end  bool
}

func (n *selectIntoNode) startExec(params runParams) error {
	return nil
}

func (n *selectIntoNode) Next(params runParams) (bool, error) {
	if n.end {
		return false, nil
	}
	n.end = true
	if ok, err := n.rows.Next(params); err != nil {
		return ok, err
	} else if !ok {
		return ok, pgerror.Newf(pgcode.AmbiguousColumn, "result consisted of zero rows")
	}
	firstVal := n.rows.Values()
	if len(firstVal) != len(n.vars) {
		return false, pgerror.Newf(pgcode.Syntax, "the used SELECT statements have a different number of columns")
	}
	if ok, err := n.rows.Next(params); err != nil && !ok {
		return ok, err
	} else if ok {
		return ok, pgerror.Newf(pgcode.AmbiguousColumn, "result consisted of more than one row")
	}
	for i := range n.vars {
		if err := params.p.sessionDataMutator.SetUserDefinedVar(n.vars[i], firstVal[i]); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (n *selectIntoNode) Values() tree.Datums {
	return nil
}

func (n *selectIntoNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
