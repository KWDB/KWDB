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

package ddl

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// dropFunctionNode represents a drop function node.
var _ sql.PlanNode = &dropFunctionNode{}

type dropFunctionNode struct {
	n *tree.DropFunction
	p *GenericPlanner
}

// DropFunction gets a dropfunctionNode to get function should delete from name.
func DropFunction(
	ctx context.Context, p *GenericPlanner, n *tree.DropFunction,
) (sql.PlanNode, error) {
	if !p.ExtendedEvalContext().TxnImplicit {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "Drop Function statement is not supported in explicit transaction")
	}
	return &dropFunctionNode{
		n: n,
		p: p,
	}, nil
}

// StartExec is interface implementation, which execute the event of dropping function(s).
func (n *dropFunctionNode) StartExec(params RunParams) error {
	for _, v := range n.n.Names {
		funcName := v

		const getUdfQuery = `
	   SELECT 
     name
	   FROM system.user_defined_routine
	   WHERE name = $1 and routine_type in ($2, $3)
	 `
		rows, err := n.p.ExecCfg().InternalExecutor.QueryRowEx(
			params.Ctx,
			"Get-udf",
			params.GetTxn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			getUdfQuery,
			funcName,
			sqlbase.LUAFunction,
			sqlbase.SQLFunction)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return pgerror.Newf(pgcode.UndefinedFunction, "unknown function '%s' when dropping function", funcName)
		}

		const deleteUdfQuery = `
    DELETE
    FROM system.user_defined_routine
    WHERE name = $1
     AND routine_type IN ($2, $3)
    `
		if _, err := n.p.ExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			"drop-udf",
			params.GetTxn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			deleteUdfQuery,
			funcName,
			sqlbase.LUAFunction,
			sqlbase.SQLFunction); err != nil {
			return err
		}

		tree.ConcurrentFunDefs.DeleteFunc(funcName)

		if err := sql.GossipUdfDeleted(n.p.ExecCfg().Gossip, v); err != nil {
			return err
		}
	}

	return nil
}

// Next implements the dropTopicNode interface.
func (n *dropFunctionNode) Next(RunParams) (bool, error) { return false, nil }

// Close implements the dropTopicNode interface.
func (n *dropFunctionNode) Close(context.Context) {}

func (n *dropFunctionNode) Values() tree.Datums { return tree.Datums{} }
