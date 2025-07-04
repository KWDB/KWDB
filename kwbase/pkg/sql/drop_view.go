// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type dropViewNode struct {
	n  *tree.DropView
	td []toDelete
}

// DropView drops a view.
// Privileges: DROP on view.
//
//	Notes: postgres allows only the view owner to DROP a view.
//	       mysql requires the DROP privilege on the view.
func (p *planner) DropView(ctx context.Context, n *tree.DropView) (planNode, error) {
	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, ResolveRequireViewDesc, false)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and the view did not exist.
			continue
		}

		if err := checkViewMatchesMaterialized(*droppedDesc, true /* requireView */, n.IsMaterialized); err != nil {
			return nil, err
		}

		td = append(td, toDelete{tn, droppedDesc})
	}

	// Ensure this view isn't depended on by any other views, or that if it is
	// then `cascade` was specified or it was also explicitly specified in the
	// DROP VIEW command.
	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, ref := range droppedDesc.DependedOnBy {
			// Don't verify that we can remove a dependent view if that dependent
			// view was explicitly specified in the DROP VIEW command.
			if descInSlice(ref.ID, td) {
				continue
			}
			if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
				return nil, err
			}
		}
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropViewNode{n: n, td: td}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP VIEW performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropViewNode) ReadingOwnWrites() {}

func (n *dropViewNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("view"))

	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}

		cascadeDroppedViews, err := params.p.dropViewImpl(
			ctx, droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()), n.n.DropBehavior,
		)
		if err != nil {
			return err
		}
		// Log a Drop View event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		params.p.SetAuditTarget(uint32(droppedDesc.GetID()), droppedDesc.GetName(), cascadeDroppedViews)
	}
	return nil
}

func (*dropViewNode) Next(runParams) (bool, error) { return false, nil }
func (*dropViewNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropViewNode) Close(context.Context)        {}

func descInSlice(descID sqlbase.ID, td []toDelete) bool {
	for _, toDel := range td {
		if descID == toDel.desc.ID {
			return true
		}
	}
	return false
}

func (p *planner) canRemoveDependentView(
	ctx context.Context,
	from *sqlbase.MutableTableDescriptor,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	return p.canRemoveDependentViewGeneric(ctx, from.TypeName(), from.Name, from.ParentID, ref, behavior)
}

func (p *planner) canRemoveDependentViewGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	parentID sqlbase.ID,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	found, _, err := p.GetProcedureNameByID(ctx, ref.ID)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	viewDesc, skipped, err := p.getViewDescForCascade(ctx, typeName, objName, parentID, ref.ID, behavior)
	if err != nil {
		return err
	}
	if skipped {
		return nil
	}
	if err := p.CheckPrivilege(ctx, viewDesc, privilege.DROP); err != nil {
		return err
	}
	// If this view is depended on by other views, we have to check them as well.
	for _, ref := range viewDesc.DependedOnBy {
		if err := p.canRemoveDependentView(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
}

// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func (p *planner) removeDependentView(
	ctx context.Context, tableDesc, viewDesc *sqlbase.MutableTableDescriptor, jobDesc string,
) ([]string, error) {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tableDesc.DependedOnBy = removeMatchingReferences(tableDesc.DependedOnBy, viewDesc.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.dropViewImpl(ctx, viewDesc, true /* queueJob */, jobDesc, tree.DropCascade)
}

// dropViewImpl does the work of dropping a view (and views that depend on it
// if `cascade is specified`). Returns the names of any additional views that
// were also dropped due to `cascade` behavior.
func (p *planner) dropViewImpl(
	ctx context.Context,
	viewDesc *sqlbase.MutableTableDescriptor,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := p.Tables().getMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return cascadeDroppedViews,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		// TODO (lucy): have more consistent/informative names for dependent jobs.
		if err := p.writeSchemaChange(
			ctx, dependencyDesc, sqlbase.InvalidMutationID, "removing references for view",
		); err != nil {
			return cascadeDroppedViews, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == tree.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, skipped, err := p.getViewDescForCascade(
				ctx, viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}
			if skipped {
				continue
			}
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			cascadedViews, err := p.dropViewImpl(ctx, dependentDesc, queueJob, "dropping dependent view", behavior)
			if err != nil {
				return cascadeDroppedViews, err
			}
			cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
			cascadeDroppedViews = append(cascadeDroppedViews, dependentDesc.Name)
		}
	}

	if err := p.initiateDropTable(ctx, viewDesc, queueJob, jobDesc, true /* drainName */); err != nil {
		return cascadeDroppedViews, err
	}

	return cascadeDroppedViews, nil
}

func (p *planner) getViewDescForCascade(
	ctx context.Context,
	typeName string,
	objName string,
	parentID, viewID sqlbase.ID,
	behavior tree.DropBehavior,
) (*sqlbase.MutableTableDescriptor, bool, error) {
	found, procName, err := p.GetProcedureNameByID(ctx, viewID)
	if err != nil {
		return nil, false, err
	}
	if found {
		if behavior != tree.DropCascade {
			msg := fmt.Sprintf("cannot drop %s %q because procedure %q depends on it",
				typeName, objName, tree.ErrString(&procName))
			hint := fmt.Sprintf("you can drop %s instead.", tree.ErrString(&procName))
			return nil, false, sqlbase.NewDependentObjectErrorWithHint(msg, hint)
		}
		if err := p.TryPurgeProcedureCache(ctx, procName, uint32(viewID)); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	viewDesc, err := p.Tables().getMutableTableVersionByID(ctx, viewID, p.txn)
	if err != nil {
		if err == sqlbase.ErrDescriptorNotFound {
			// TODO: Perhaps it is necessary to add state to the procedure metadata
			// to avoid the error of "descriptor not found" when depend on multiple tables
			// and then dropping database.
			return nil, true, nil
		}
		return nil, false, err
	}
	if behavior != tree.DropCascade {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc.TableDesc())
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				msg := fmt.Sprintf("cannot drop %s %q because a view depends on it", typeName, objName)
				return nil, false, sqlbase.NewDependentObjectError(msg)
			}
		}
		msg := fmt.Sprintf("cannot drop %s %q because view %q depends on it",
			typeName, objName, viewName)
		hint := fmt.Sprintf("you can drop %s instead.", viewName)
		return nil, false, sqlbase.NewDependentObjectErrorWithHint(msg, hint)
	}
	return viewDesc, false, nil
}
