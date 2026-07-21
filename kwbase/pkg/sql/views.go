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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// makeViewTableDesc returns the table descriptor for a new view.
//
// It creates the descriptor directly in the PUBLIC state rather than
// the ADDING state because back-references are added to the view's
// dependencies in the same transaction that the view is created and it
// doesn't matter if reads/writes use a cached descriptor that doesn't
// include the back-references.
func makeViewTableDesc(
	viewName string,
	viewQuery string,
	parentID sqlbase.ID,
	schemaID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	temporary bool,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(
		id,
		parentID,
		schemaID,
		viewName,
		creationTime,
		privileges,
		temporary,
		tree.RelationalTable,
		"",
	)
	desc.ViewQuery = viewQuery
	for _, colRes := range resultColumns {
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
		// Nullability constraints do not need to exist on the view, since they are
		// already enforced on the source data.
		columnTableDef.Nullable.Nullability = tree.SilentNull
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, semaCtx, true)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(col)
	}
	if err := desc.AllocateIDs(); err != nil {
		return sqlbase.MutableTableDescriptor{}, err
	}
	return desc, nil
}

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc *sqlbase.ImmutableTableDescriptor
	// deps is the list of ways in which the current plan depends on
	// that table. There can be more than one entries when the same
	// table is used in different places. The entries can also be
	// different because some may reference an index and others may
	// reference only a subset of the table's columns.
	// Note: the "ID" field of TableDescriptor_Reference is not
	// (and cannot be) filled during plan construction / dependency
	// analysis because the descriptor that is using this dependency
	// has not been constructed yet.
	deps []sqlbase.TableDescriptor_Reference
}

func (p *planDependencyInfo) GetDesc() *sqlbase.ImmutableTableDescriptor {
	return p.desc
}

func (p *planDependencyInfo) GetDeps() []sqlbase.TableDescriptor_Reference {
	return p.deps
}

// PlanDependencies maps the ID of a table depended upon to a list of
// detailed dependencies on that table.
type PlanDependencies map[sqlbase.ID]planDependencyInfo

// String implements the fmt.Stringer interface.
func (d PlanDependencies) String() string {
	var buf bytes.Buffer
	for id, deps := range d {
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameStringP(&deps.desc.Name))
		for _, dep := range deps.deps {
			buf.WriteString(" [")
			if dep.IndexID != 0 {
				fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
			}
			fmt.Fprintf(&buf, "cols: %v]", dep.ColumnIDs)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// GetViewDescForCascade retrieves view descriptors for cascade operations
func GetViewDescForCascade(
	ctx context.Context,
	p *GenericPlanner,
	typeName string,
	objName string,
	parentID, viewID sqlbase.ID,
	behavior tree.DropBehavior,
) (*sqlbase.MutableTableDescriptor, bool, error) {
	found, procName, err := GetProcedureNameByID(ctx, p.ExecCfg().InternalExecutor, p.Txn(), viewID)
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
	viewDesc, err := p.Tables().GetMutableTableVersionByID(ctx, viewID, p.txn)
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
			viewName, err = GetQualifiedTableName(ctx, p.Txn(), viewDesc.TableDesc())
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

// CanRemoveDependentView checks if dependent views can be safely removed
func (p *GenericPlanner) CanRemoveDependentView(
	ctx context.Context,
	from *sqlbase.MutableTableDescriptor,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	return CanRemoveDependentViewGeneric(ctx, p, from.TypeName(), from.Name, from.ParentID, ref, behavior)
}

// CanRemoveDependentViewGeneric checks if dependent views can be safely removed (generic version)
func CanRemoveDependentViewGeneric(
	ctx context.Context,
	p *GenericPlanner,
	typeName string,
	objName string,
	parentID sqlbase.ID,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	found, _, err := GetProcedureNameByID(ctx, p.ExecCfg().InternalExecutor, p.Txn(), ref.ID)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	viewDesc, skipped, err := GetViewDescForCascade(ctx, p, typeName, objName, parentID, ref.ID, behavior)
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
		if err := p.CanRemoveDependentView(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
}

// RemoveDependentView removes a dependent view during cascade drop operations
// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func RemoveDependentView(
	ctx context.Context,
	p *GenericPlanner,
	tableDesc, viewDesc *sqlbase.MutableTableDescriptor,
	jobDesc string,
) ([]string, error) {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tableDesc.DependedOnBy = RemoveMatchingReferences(tableDesc.DependedOnBy, viewDesc.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.DropViewImpl(ctx, viewDesc, true /* queueJob */, jobDesc, tree.DropCascade)
}

// DropViewImpl does the work of dropping a view (and views that depend on it
// if `cascade is specified`). Returns the names of any additional views that
// were also dropped due to `cascade` behavior.
func (p *GenericPlanner) DropViewImpl(
	ctx context.Context,
	viewDesc *sqlbase.MutableTableDescriptor,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := p.Tables().GetMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return cascadeDroppedViews,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = RemoveMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		// TODO (lucy): have more consistent/informative names for dependent jobs.
		if err := p.WriteSchemaChange(
			ctx, dependencyDesc, sqlbase.InvalidMutationID, "removing references for view",
		); err != nil {
			return cascadeDroppedViews, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == tree.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, skipped, err := GetViewDescForCascade(ctx, p,
				viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}
			if skipped {
				continue
			}
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			cascadedViews, err := p.DropViewImpl(ctx, dependentDesc, queueJob, "dropping dependent view", behavior)
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

// CheckViewMatchesMaterialized ensures that if a view is required, then the view
// is materialized or not as desired.
func CheckViewMatchesMaterialized(
	desc sqlbase.MutableTableDescriptor, requireView, wantMaterialized bool,
) error {
	if !requireView {
		return nil
	}
	if !desc.IsView() {
		return nil
	}
	isMaterialized := desc.MaterializedView()
	if isMaterialized && !wantMaterialized {
		err := pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", desc.GetName())
		return errors.WithHint(err, "use the corresponding MATERIALIZED VIEW command")
	}
	if !isMaterialized && wantMaterialized {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.GetName())
	}
	return nil
}
