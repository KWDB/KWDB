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

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &dropSchemaNode{}
var _ sql.PlanNodeReadingOwnWrites = &dropSchemaNode{}

type dropSchemaNode struct {
	n  *tree.DropSchema
	db *DatabaseDescriptor
	d  *dropCascadeState
}

// Use to satisfy the linter.
var _ sql.PlanNode = &dropSchemaNode{n: nil}

// DropSchema drops a user define schema.
// Privileges: DROP on schema and DROP on all tables in the schema.
//
// Parameters:
// - n: DropSchema AST
// Returns:
// - dropSchemaNode PlanNode
func DropSchema(ctx context.Context, p *GenericPlanner, n *tree.DropSchema) (sql.PlanNode, error) {
	// TODO (rohany, lucy-zhang): Use a mutable database access method here.
	db, err := p.ResolveUncachedDatabaseByName(ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return nil, err
	}
	if db.EngineType == tree.EngineTypeTimeseries {
		return nil, pgerror.New(pgcode.WrongObjectType, "cannot drop schema in timeseries database")
	}
	d := newDropCascadeState()

	// Collect all schemas to be deleted.
	for _, scName := range n.Names {
		found, sc, err := p.LogicalSchemaAccessor().GetSchema(ctx, p.Txn(), db.ID, scName)
		if err != nil {
			return nil, err
		}
		if !found {
			if n.IfExists {
				continue
			}
			return nil, sqlbase.NewUndefinedSchemaError(scName)
		}
		switch sc.Kind {
		case sqlbase.SchemaPublic, sqlbase.SchemaVirtual, sqlbase.SchemaTemporary:
			return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", scName)
		case sqlbase.SchemaUserDefined:
			if err := p.CheckPrivilege(ctx, sc.Desc, privilege.DROP); err != nil {
				return nil, err
			}
			namesBefore := len(d.objectNamesToDelete)
			if err := d.collectObjectsInSchema(ctx, p, db, &sc); err != nil {
				return nil, err
			}
			// We added some new objects to delete. Ensure that we have the correct
			// drop behavior to be doing this.
			if namesBefore != len(d.objectNamesToDelete) && n.DropBehavior != tree.DropCascade {
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
					"schema %q is not empty and CASCADE was not specified", scName)
			}
		default:
			return nil, errors.AssertionFailedf("unknown schema kind %d", sc.Kind)
		}

		if err := d.resolveCollectedObjects(ctx, p, db); err != nil {
			return nil, err
		}
	}
	return &dropSchemaNode{n: n, d: d, db: db}, nil
}

func (n *dropSchemaNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("schema"))

	ctx := params.Ctx
	p := params.GetPlanner()

	// Drop all collected objects.
	if err := n.d.dropAllCollectedObjects(ctx, p); err != nil {
		return err
	}

	// Queue the job to actually drop the schema.
	schemaIDs := make([]sqlbase.ID, len(n.d.schemasToDelete))
	b := &kv.Batch{}
	for i := range n.d.schemasToDelete {
		rsSchema := n.d.schemasToDelete[i]
		schemaIDs[i] = rsSchema.ID
		if err := sql.DropSchemaImpl(ctx, p, b, n.db.ID, rsSchema); err != nil {
			return err
		}
		p.SetAuditTarget(uint32(rsSchema.ID), rsSchema.Name, nil)
	}
	if err := p.Txn().Run(ctx, b); err != nil {
		return err
	}

	// Create the job to drop the schema.
	if err := createDropSchemaJob(p,
		schemaIDs,
		n.d.getDroppedTableDetails(),
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return nil
}

// createDropSchemaJob creates a schema change job for DROP SCHEMA.
func createDropSchemaJob(
	p *GenericPlanner,
	schemas []sqlbase.ID,
	tableDropDetails []jobspb.DroppedTableDetails,
	jobDesc string,
) error {

	_, err := p.ExtendedEvalContext().QueueJob(jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: schemas,
		Details: jobspb.SchemaChangeDetails{
			DroppedSchemas:    schemas,
			DroppedTables:     tableDropDetails,
			DroppedDatabaseID: sqlbase.InvalidID,
			FormatVersion:     jobspb.JobResumerFormatVersion,
		},
		Progress: jobspb.SchemaChangeProgress{},
	})
	return err
}

func (n *dropSchemaNode) Next(params RunParams) (bool, error) { return false, nil }
func (n *dropSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropSchemaNode) Close(ctx context.Context)           {}
func (n *dropSchemaNode) ReadingOwnWrites()                   {}
