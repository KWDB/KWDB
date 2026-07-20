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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type rebalanceTsDataNode struct {
	n *tree.RebalanceTsData
}

// RebalanceTsDataNode rebalance ts data.
func (p *GenericPlanner) RebalanceTsDataNode(
	ctx context.Context, n *tree.RebalanceTsData,
) (PlanNode, error) {
	advance := settings.AllowAdvanceDistributeSetting.Get(p.execCfg.SV())
	if !advance {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "rebalance ts data is not support when cluster setting server.advanced_distributed_operations.enabled is false.")
	}
	if p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf(
			"rebalance replica")
	}
	return &rebalanceTsDataNode{n: n}, nil
}

func (n *rebalanceTsDataNode) StartExec(params RunParams) error {
	return pgerror.New(pgcode.FeatureNotSupported, "rebalance ts data is not supported")
}

func (*rebalanceTsDataNode) Next(RunParams) (bool, error) { return false, nil }
func (*rebalanceTsDataNode) Values() tree.Datums          { return tree.Datums{} }
func (*rebalanceTsDataNode) Close(context.Context)        {}
