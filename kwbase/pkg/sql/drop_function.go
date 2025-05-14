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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// dropFunctionNode represents a drop function node.
type dropFunctionNode struct {
	n *tree.DropFunction
	p *planner
}

// DropFunction gets a dropfunctionNode to get function should delete from name.
func (p *planner) DropFunction(ctx context.Context, n *tree.DropFunction) (planNode, error) {
	if !p.extendedEvalCtx.TxnImplicit {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "Drop Function statement is not supported in explicit transaction")
	}
	return &dropFunctionNode{
		n: n,
		p: p,
	}, nil
}

// startExec is interface implementation, which execute the event of dropping function(s).
func (n *dropFunctionNode) startExec(params runParams) error {
	for _, v := range n.n.Names {
		const getUdfQuery = `
	   SELECT 
     function_name
	   FROM system.user_defined_function
	   WHERE function_name = $1
	 `
		rows, err := n.p.ExecCfg().InternalExecutor.Query(params.ctx, "Get-udf", nil /* txn */, getUdfQuery, v)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return pgerror.Newf(pgcode.UndefinedFunction, "unknown function '%s' when dropping function", v)
		}
		if err := GossipUdfDeleted(n.p.execCfg.Gossip, v); err != nil {
			return err
		}
	}
	// Set timeouts and timers to wait for function deletion information to propagate
	timeoutDuration := time.Duration(len(n.n.Names)) * time.Second
	timeout := time.After(timeoutDuration)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	// Wait for all functions to be removed from tree.FunDefs or until timeout
	for _, v := range n.n.Names {
		deleted := false
		for !deleted {
			select {
			case <-timeout:
				return pgerror.Newf(pgcode.Warning, "remove %s function timeout, Please wait", v)
			case <-ticker.C:
				if _, ok := tree.ConcurrentFunDefs.LookupFunc(v); !ok {
					deleted = true
					break
				}
			}
		}
	}
	return nil
}

// Next implements the dropTopicNode interface.
func (n *dropFunctionNode) Next(runParams) (bool, error) { return false, nil }

// Close implements the dropTopicNode interface.
func (n *dropFunctionNode) Close(context.Context) {}

// Values implements the dropTopicNode interface.
func (n *dropFunctionNode) Values() tree.Datums { return tree.Datums{} }
