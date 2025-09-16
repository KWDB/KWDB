// Copyright 2020 The Cockroach Authors.
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

type refreshMaterializedViewNode struct {
	n    *tree.RefreshMaterializedView
	desc *sqlbase.MutableTableDescriptor
}

func (p *planner) RefreshMaterializedView(
	ctx context.Context, n *tree.RefreshMaterializedView,
) (planNode, error) {
	desc, err := p.ResolveMutableTableDescriptorEx(ctx, n.Name, true /* required */, ResolveRequireViewDesc)
	if err != nil {
		return nil, err
	}
	if !desc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.Name)
	}
	// TODO (rohany): Not sure if this is a real restriction, but let's start with
	//  it to be safe.
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if mut.GetMaterializedViewRefresh() != nil {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "view is already being refreshed")
		}
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.UPDATE); err != nil {
		return nil, err
	}
	return &refreshMaterializedViewNode{n: n, desc: desc}, nil
}

func (n *refreshMaterializedViewNode) startExec(params runParams) error {
	if !params.p.EvalContext().TxnImplicit {
		return pgerror.Newf(pgcode.InvalidTransactionState, "cannot refresh view in an explicit transaction")
	}
	// We refresh a materialized view by creating a new set of indexes to write
	// the result of the view query into. The existing set of indexes will remain
	// present and readable so that reads of the view during the refresh operation
	// will return consistent data. The schema change process will backfill the
	// results of the view query into the new set of indexes, and then change the
	// set of indexes over to the new set of indexes atomically.

	// Prepare the new set of indexes by cloning all existing indexes on the view.
	newPrimaryIndex := protoutil.Clone(&n.desc.PrimaryIndex).(*sqlbase.IndexDescriptor)
	newIndexes := make([]sqlbase.IndexDescriptor, len(n.desc.Indexes))
	for i := range n.desc.Indexes {
		newIndexes[i] = *protoutil.Clone(&n.desc.Indexes[i]).(*sqlbase.IndexDescriptor)
	}

	// Reset and allocate new IDs for the new indexes.
	getID := func() sqlbase.IndexID {
		res := n.desc.NextIndexID
		n.desc.NextIndexID++
		return res
	}
	newPrimaryIndex.ID = getID()
	for i := range newIndexes {
		newIndexes[i].ID = getID()
	}

	// Queue the refresh mutation.
	n.desc.AddMaterializedViewRefreshMutation(&sqlbase.MaterializedViewRefresh{
		NewPrimaryIndex: *newPrimaryIndex,
		NewIndexes:      newIndexes,
		AsOf:            params.p.Txn().ReadTimestamp(),
	})

	return params.p.writeSchemaChange(
		params.ctx,
		n.desc,
		n.desc.ClusterVersion.NextMutationID,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *refreshMaterializedViewNode) Next(params runParams) (bool, error) { return false, nil }
func (n *refreshMaterializedViewNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *refreshMaterializedViewNode) Close(ctx context.Context)           {}
