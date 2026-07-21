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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

var _ sql.PlanNode = &alterTSDatabaseNode{}

type alterTSDatabaseNode struct {
	n                 *tree.AlterTSDatabase
	lifeTime          uint64
	partitionInterval uint64
	databaseDesc      *DatabaseDescriptor
}

// NewAlterTSDatabaseNode creates a new alterTSDatabaseNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterTSDatabaseNode(
	n *tree.AlterTSDatabase,
	lifeTime uint64,
	partitionInterval uint64,
	databaseDesc *DatabaseDescriptor,
) *alterTSDatabaseNode {
	return &alterTSDatabaseNode{
		n:                 n,
		lifeTime:          lifeTime,
		partitionInterval: partitionInterval,
		databaseDesc:      databaseDesc,
	}
}

// AlterTSDatabase validates the new definition of database, returns AlterTSDatabase PlanNode.
// Parameters:
// - n: AlterTSDatabase AST
// Returns:
// - PlanNode: AlterTSDatabase PlanNode
func AlterTSDatabase(
	ctx context.Context, p *GenericPlanner, n *tree.AlterTSDatabase,
) (PlanNode, error) {
	if err := p.RequireAdminRole(ctx, "ALTER DATABASE"); err != nil {
		return nil, err
	}

	if n.Database == "" {
		return nil, sqlerror.ErrEmptyDatabaseName
	}
	// Check that the database exists.
	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Database), true)
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return sql.NewZeroNode(nil /* columns */), nil
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	// validates new retention or partition interval
	lifeTime := dbDesc.TsDb.Lifetime
	partitionInterval := dbDesc.TsDb.PartitionInterval
	if n.LifeTime != nil {
		lifeTime = uint64(sqlutil.GetTimeFromTimeInput(*n.LifeTime))
		if !(lifeTime <= sqlconst.MaxLifeTime && lifeTime >= 0) {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "lifetime is out of range: %d", lifeTime)
		}
	}

	if n.PartitionInterval != nil {
		switch n.PartitionInterval.Unit {
		case "s", "second", "m", "minute", "h", "hour":
			// we support day, week, month, year
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported partition interval unit: %s",
				n.PartitionInterval.Unit)
		}
		partitionInterval = uint64(sqlutil.GetTimeFromTimeInput(*n.PartitionInterval))
		if !(partitionInterval <= sqlconst.MaxLifeTime && partitionInterval > 0) {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "partition interval is out of range %d, the time range is [1day, 1000year]", partitionInterval)
		}
	}
	return &alterTSDatabaseNode{
		n:                 n,
		lifeTime:          lifeTime,
		partitionInterval: partitionInterval,
		databaseDesc:      dbDesc,
	}, nil
}

func (n *alterTSDatabaseNode) StartExec(params RunParams) error {
	// explicit txn is not allowed in time-series mode.
	log.Infof(params.Ctx, "alter ts database %s start, id: %d", n.databaseDesc.Name, n.databaseDesc.ID)
	p := params.GetPlanner()
	ctx := params.Ctx

	// set new retention/partition interval
	databaseDesc := n.databaseDesc
	databaseDesc.TsDb.Lifetime = n.lifeTime
	databaseDesc.TsDb.PartitionInterval = n.partitionInterval

	// validate database descriptor
	if err := databaseDesc.Validate(); err != nil {
		return err
	}
	descID := databaseDesc.GetID()
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(databaseDesc)

	// write new database descriptor into system table
	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)
	if err1 := p.Txn().Run(ctx, b); err1 != nil {
		return err1
	}

	p.SetAuditTarget(uint32(descID), databaseDesc.GetName(), nil)
	log.Infof(params.Ctx, "alter ts database %s finished, id: %d", n.databaseDesc.Name, n.databaseDesc.ID)
	return nil
}

func (n *alterTSDatabaseNode) Next(params RunParams) (bool, error) { return false, nil }

func (n *alterTSDatabaseNode) Values() tree.Datums { return tree.Datums{} }

func (n *alterTSDatabaseNode) Close(ctx context.Context) {}
