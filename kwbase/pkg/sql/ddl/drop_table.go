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
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

var _ sql.PlanNode = &dropTableNode{}

type dropTableNode struct {
	n *tree.DropTable
	// td is a map from table descriptor to sql.TableToDelete struct, indicating which
	// tables this operation should delete.
	td map[sqlbase.ID]sql.TableToDelete
}

// DropTable drops a table.
// Privileges: DROP on table.
//
//	Notes: postgres allows only the table owner to DROP a table.
//	       mysql requires the DROP privilege on the table.
func DropTable(ctx context.Context, p *GenericPlanner, n *tree.DropTable) (sql.PlanNode, error) {
	td := make(map[sqlbase.ID]sql.TableToDelete, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.PrepareDropTable(ctx, tn, !n.IfExists, sql.ResolveRequireTableDesc, n.DropBehavior == tree.DropCascade)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			continue
		}
		// drop multiple time-series tables at once is not supported
		if droppedDesc.IsTSTable() && len(n.Names) > 1 {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "drop multiple time-series tables at once is not supported")
		}

		// dropping timeseries table within explicit txn not supported
		if !p.ExtendedEvalContext().TxnImplicit && droppedDesc.IsTSTable() {
			return nil, sqlbase.UnsupportedTSExplicitTxnError()
		}

		// check drop behavior
		if tree.TableType(droppedDesc.TableType) == tree.TemplateTable {
			if n.DropBehavior != tree.DropCascade {
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
					"can not drop template table %s without cascade", droppedDesc.Name)
			}
		}

		td[droppedDesc.ID] = sql.TableToDelete{tn, droppedDesc}
	}

	for _, toDel := range td {
		droppedDesc := toDel.Desc

		if err := canRemoveAllTableOwnedStreams(ctx, p, droppedDesc, n.DropBehavior); err != nil {
			return nil, err
		}

		if droppedDesc.IsTSTable() {
			if err := sql.CanDropInsTable(ctx, p, toDel.Desc.ID); err != nil {
				return nil, err
			}
			// checkTableRelatedPubsAndSubs checks whether the specified table has been published or subscribed.
			if err := checkTableRelatedPubsAndSubs(ctx, p, uint64(droppedDesc.ID), droppedDesc.Name, n.StatOp(), nil); err != nil {
				return nil, err
			}
			continue
		}

		for i := range droppedDesc.InboundFKs {
			ref := &droppedDesc.InboundFKs[i]
			if _, ok := td[ref.OriginTableID]; !ok {
				if err := sql.CanRemoveFKBackreference(ctx, p, droppedDesc.Name, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		for _, idx := range droppedDesc.AllNonDropIndexes() {
			for _, ref := range idx.InterleavedBy {
				if _, ok := td[ref.Table]; !ok {
					if err := sql.CanRemoveInterleave(ctx, p, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, ref := range droppedDesc.DependedOnBy {
			if _, ok := td[ref.ID]; !ok {
				if err := p.CanRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		if err := canRemoveAllTableOwnedSequences(ctx, p, droppedDesc, n.DropBehavior); err != nil {
			return nil, err
		}
	}

	if len(td) == 0 {
		return sql.NewZeroNode(nil /* columns */), nil
	}
	return &dropTableNode{
		n:  n,
		td: td,
	}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because DROP TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropTableNode) ReadingOwnWrites() {}

func (n *dropTableNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("table"))

	ctx := params.Ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.Desc
		if droppedDesc == nil {
			continue
		}
		// drop time-series table
		//switch toDel.Desc.TableType {
		//case tree.InstanceTable:
		//	return params.GetPlanner().dropInstanceTable(ctx, toDel.Tn.Catalog(), toDel.Tn.Table(), toDel.Desc)
		//case tree.TemplateTable, tree.TimeseriesTable:
		//	return params.GetPlanner().dropTsTable(ctx, toDel.Tn.Catalog(), toDel.Desc)
		//}
		droppedViews, err := sql.DropTableImpl(ctx, params.GetPlanner(),
			droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()), n.n.DropBehavior)
		if err != nil {
			params.GetPlanner().SetAuditTarget(0, droppedDesc.GetName(), droppedViews)
			return err
		}
		params.GetPlanner().SetAuditTarget(uint32(droppedDesc.ID), droppedDesc.GetName(), droppedViews)
	}

	stmt := tree.AsStringWithFQNames(n.n, params.Ann())
	if err := sql.SendDropTableStmtToPipe(params, n.td, sqlconst.KafkaMsgKindDropTable, stmt); err != nil {
		return err
	}
	return nil
}

func (*dropTableNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropTableNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropTableNode) Close(context.Context)        {}

//func (p *GenericPlanner) dropTsTable(
//	ctx context.Context, dbName string, desc *MutableTableDescriptor,
//) error {
//	if _, err := api.GetAvailableNodeIDs(ctx); err != nil {
//		return err
//	}
//	log.Infof(ctx, "drop ts table %s 1st txn start, id: %d", desc.Name, desc.ID)
//	var err error
//	desc.TableDesc().State = sqlbase.TableDescriptor_DROP
//	if err = p.writeTableDesc(ctx, desc); err != nil {
//		return err
//	}
//	// Create a Job to perform the second stage of ts DDL.
//	syncDetail := jobspb.SyncMetaCacheDetails{
//		Type:       dropKwdbTsTable,
//		SNTable:    desc.TableDescriptor,
//		DropMEInfo: []sqlbase.DeleteMeMsg{{DatabaseName: dbName, TableName: desc.Name, TableID: uint32(desc.ID), TsVersion: uint32(desc.TsTable.GetTsVersion())}},
//	}
//	jobID, err := p.CreateTSSchemaChangeJob(ctx, syncDetail, p.stmt.SQL)
//	if err != nil {
//		return err
//	}
//
//	if err := eventlog.MakeEventLogger(p.ExecCfg()).InsertEventRecord(
//		ctx,
//		p.Txn(),
//		EventLogDropTable,
//		int32(desc.ID),
//		int32(p.ExecCfg().NodeID.Get()),
//		struct {
//			TableName string
//			TableID   uint32
//			Statement string
//			User      string
//		}{
//			desc.Name,
//			uint32(desc.ID),
//			p.stmt.SQL,
//			p.User()},
//	); err != nil {
//		return err
//	}
//	// Actively commit a transaction, and read/write system table operations
//	// need to be performed before this.
//	if err = p.Txn().Commit(ctx); err != nil {
//		return err
//	}
//
//	// After the transaction commits successfully, execute the Job and wait for it to complete.
//	if err = p.ExecCfg().JobRegistry.Run(
//		ctx,
//		p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
//		[]int64{jobID},
//	); err != nil {
//		return err
//	}
//	log.Infof(ctx, "drop ts table %s 1st txn finished, id: %d", desc.Name, desc.ID)
//	return nil
//}

// dropInstanceTable deletes instance table record from related system table.
//func (p *GenericPlanner) dropInstanceTable(
//	ctx context.Context, dbName string, tableName string, temTable *MutableTableDescriptor,
//) error {
//	insTable, found, err := sqlbase.ResolveInstanceName(ctx, p.Txn(), dbName, tableName)
//	if err != nil {
//		return err
//	} else if !found {
//		return sqlbase.NewUndefinedTableError(tableName)
//	}
//
//	insTable.State = sqlbase.ChildDesc_DROP
//	if err := WriteInstTableMeta(ctx, p.Txn(), []sqlbase.InstNameSpace{insTable}, true); err != nil {
//		return err
//	}
//
//	// Create a Job to perform the second stage of ts DDL.
//	syncDetail := jobspb.SyncMetaCacheDetails{
//		Type: dropKwdbInsTable,
//		DropMEInfo: []sqlbase.DeleteMeMsg{{
//			DatabaseName: insTable.DBName,
//			TableName:    insTable.InstName,
//			TableID:      uint32(insTable.InstTableID),
//			TemplateID:   uint32(insTable.TmplTableID),
//		}},
//	}
//	jobID, err := p.CreateTSSchemaChangeJob(ctx, syncDetail, p.stmt.SQL)
//	if err != nil {
//		return err
//	}
//
//	if err := eventlog.MakeEventLogger(p.ExecCfg()).InsertEventRecord(
//		ctx,
//		p.Txn(),
//		EventLogDropTable,
//		int32(temTable.ID),
//		int32(p.ExecCfg().NodeID.Get()),
//		struct {
//			TableName  string
//			TableID    uint32
//			TemplateID uint32
//			Statement  string
//			User       string
//		}{
//			insTable.InstName,
//			uint32(insTable.InstTableID),
//			uint32(insTable.TmplTableID),
//			p.stmt.SQL,
//			p.User()},
//	); err != nil {
//		return err
//	}
//
//	// Actively commit a transaction, and read/write system table operations
//	// need to be performed before this.
//	if err := p.Txn().Commit(ctx); err != nil {
//		return err
//	}
//
//	// After the transaction commits successfully, execute the Job and wait for it to complete.
//	if err = p.ExecCfg().JobRegistry.Run(
//		ctx,
//		p.ExecCfg().InternalExecutor,
//		[]int64{jobID},
//	); err != nil {
//		return err
//	}
//
//	return nil
//}
