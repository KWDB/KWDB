// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

var _ sql.PlanNode = &dropStreamNode{}

type dropStreamNode struct {
	streamID   uint64
	streamName tree.Name
	jobID      int64
	tableID    uint64
}

// DropStream creates a drop stream node for exec.
func DropStream(ctx context.Context, p *GenericPlanner, n *tree.DropStream) (sql.PlanNode, error) {
	stream1, err := sql.LoadStreamByName(ctx, p, n.StreamName)
	if err != nil {
		return nil, err
	}

	if stream1 == nil {
		if n.IfExists {
			return &dropStreamNode{streamName: "", jobID: 0}, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "stream %q does not exist", n.StreamName)
	}

	// check if the current user is the stream creator or has the system admin role
	if err = sql.CheckStreamPrivilege(
		ctx, p, nil, privilege.DROP, privilege.ALL,
		stream1.CreateBy, n.StreamName.String(),
	); err != nil {
		return nil, err
	}

	return &dropStreamNode{
		streamID:   stream1.ID,
		streamName: n.StreamName,
		jobID:      stream1.JobID,
		tableID:    stream1.SourceTableID,
	}, nil
}

func canRemoveAllTableOwnedStreams(
	ctx context.Context, p *GenericPlanner, desc *MutableTableDescriptor, behavior tree.DropBehavior,
) error {
	return sql.CheckTableUsedByStream(
		ctx, p, uint64(desc.ID), desc.Name, nil, behavior == tree.DropCascade)
}

func (n *dropStreamNode) StartExec(params RunParams) error {
	if n.streamName == "" {
		return nil
	}
	err := sql.RemoveStream(params.Ctx, params.GetPlanner(), n.jobID, n.streamID, n.tableID)
	if err != nil {
		return err
	}

	params.GetPlanner().SetAuditTarget(uint32(n.streamID), n.streamName.String(), nil)
	return nil
}

func (*dropStreamNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropStreamNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropStreamNode) Close(context.Context)        {}
