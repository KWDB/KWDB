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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type vacuumNode struct {
}

// Vacuum creates a vacuum plan node.
func (p *planner) Vacuum(ctx context.Context, n *tree.Vacuum) (planNode, error) {
	return &vacuumNode{}, nil
}

func (*vacuumNode) startExec(params runParams) error {
	return nil
}

func (*vacuumNode) Next(runParams) (bool, error) { return false, nil }
func (*vacuumNode) Values() tree.Datums          { return tree.Datums{} }
func (*vacuumNode) Close(context.Context)        {}
