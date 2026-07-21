// Copyright 2015 The Cockroach Authors.
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

package ddl

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

var _ sql.PlanNode = &alterSequenceNode{}
var _ sql.PlanNodeReadingOwnWrites = &alterSequenceNode{}

type alterSequenceNode struct {
	n       *tree.AlterSequence
	seqDesc *MutableTableDescriptor
}

// NewAlterSequenceNode creates a new alterSequenceNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterSequenceNode(
	n *tree.AlterSequence, seqDesc *MutableTableDescriptor,
) *alterSequenceNode {
	return &alterSequenceNode{
		n:       n,
		seqDesc: seqDesc,
	}
}

// AlterSequence transforms a tree.AlterSequence into a plan node.
func AlterSequence(
	ctx context.Context, p *GenericPlanner, n *tree.AlterSequence,
) (sql.PlanNode, error) {
	seqDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Name, !n.IfExists, sql.ResolveRequireSequenceDesc,
	)
	if err != nil {
		return nil, err
	}
	if seqDesc == nil {
		return sql.NewZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, seqDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterSequenceNode{n: n, seqDesc: seqDesc}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because ALTER SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterSequenceNode) ReadingOwnWrites() {}

func (n *alterSequenceNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("sequence"))
	desc := n.seqDesc

	err := sql.AssignSequenceOptions(desc.SequenceOpts, n.n.Options, false /* setDefaults */, &params, desc.GetID())
	if err != nil {
		return err
	}

	if err := params.GetPlanner().WriteSchemaChange(
		params.Ctx, n.seqDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Record this sequence alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	params.GetPlanner().SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	return nil
}

func (n *alterSequenceNode) Next(RunParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterSequenceNode) Close(context.Context)        {}
