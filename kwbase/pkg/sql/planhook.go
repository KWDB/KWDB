// Copyright 2016 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// planHookFn is a function that can intercept a statement being planned and
// provide an alternate implementation. It's primarily intended to allow
// implementation of certain sql statements to live outside of the sql package.
//
// To intercept a statement the function should return a non-nil function for
// `fn` as well as the appropriate sqlbase.ResultColumns describing the results
// it will return (if any). If the hook plan requires sub-plans to be planned
// and started by the usual machinery (e.g. to run a subquery), it must return
// then as well. `fn` will be called in a goroutine during the `Start` phase of
// plan execution.
type planHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (fn PlanHookRowFn, header sqlbase.ResultColumns, subplans []PlanNode, avoidBuffering bool, err error)

// PlanHookRowFn describes the row-production for hook-created plans. The
// channel argument is used to return results to the plan's runner. It's
// a blocking channel, so implementors should be careful to only use blocking
// sends on it when necessary. Any subplans returned by the hook when initially
// called are passed back, planned and started, for the the RowFn's use.
//
// TODO(dt): should this take RunParams like a normal PlanNode.Next?
type PlanHookRowFn func(context.Context, []PlanNode, chan<- tree.Datums) error

var planHooks []planHookFn

// wrappedPlanHookFn is similar to planHookFn but returns an existing plan type.
// Additionally, it takes a context.
type wrappedPlanHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (PlanNode, error)

var wrappedPlanHooks []wrappedPlanHookFn

// RunParams creates run parameters for executing a plan hook
func (p *GenericPlanner) RunParams(ctx context.Context) RunParams {
	return RunParams{ctx, p.ExtendedEvalContext(), p}
}

// PlanHookState exposes the subset of GenericPlanner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
//
// The PlanHookState is used by modules that are under the CCL. Since the OSS
// modules cannot depend on the CCL modules, the CCL modules need to inform the
// GenericPlanner when they should be invoked (via plan hooks). The only way for the
// CCL statements to get access to a "GenericPlanner" is through this PlanHookState
// that gets passed back due to this inversion of roles.
type PlanHookState interface {
	SchemaResolver
	RunParams(ctx context.Context) RunParams
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *LeaseManager
	TypeAsString(e tree.Expr, op string) (func() (string, error), error)
	TypeAsStringArray(e tree.Exprs, op string) (func() ([]string, error), error)
	TypeAsStringOpts(
		opts tree.KVOptions, optsValidate map[string]sqlconst.KVStringOptValidate,
	) (func() (map[string]string, error), error)
	User() string
	AuthorizationAccessor
	// The role create/drop call into OSS code to reuse plan nodes.
	// TODO(mberhault): it would be easier to just pass a GenericPlanner to plan hooks.
	GetAllRoles(ctx context.Context) (map[string]bool, error)
	BumpRoleMembershipTableVersion(ctx context.Context) error
	EvalAsOfTimestamp(asOf tree.AsOfClause) (hlc.Timestamp, error)
	ResolveUncachedDatabaseByName(
		ctx context.Context, dbName string, required bool) (*UncachedDatabaseDescriptor, error)
	ResolveMutableTableDescriptor(
		ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
	) (table *MutableTableDescriptor, err error)
	// ResolveImmutableTableDescriptor looks up an existing Immutable object.
	ResolveImmutableTableDescriptor(
		ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
	) (table *ImmutableTableDescriptor, err error)
	// ShowCreate(
	// 	ctx context.Context, dbPrefix string, allDescs []sqlbase.Descriptor, desc *sqlbase.TableDescriptor, displayOptions ddlopts.ShowCreateDisplayOptions,
	// ) (string, error)
	CreateSchemaNamespaceEntry(ctx context.Context, schemaNameKey roachpb.Key,
		schemaID sqlbase.ID) error
}

// AddPlanHook adds a hook used to short-circuit creating a PlanNode from a
// tree.Statement. If the func returned by the hook is non-nil, it is used to
// construct a PlanNode that runs that func in a goroutine during Start.
//
// See PlanHookState comments for information about why plan hooks are needed.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// ClearPlanHooks is used by tests to clear out any mocked out plan hooks that
// were registered.
func ClearPlanHooks() {
	planHooks = nil
}

// hookFnNode is a PlanNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
var _ PlanNode = &hookFnNode{}

type hookFnNode struct {
	OptColumnsSlot

	f        PlanHookRowFn
	header   sqlbase.ResultColumns
	subplans []PlanNode

	run hookFnRun
}

// hookFnRun contains the run-time state of hookFnNode during local execution.
type hookFnRun struct {
	resultsCh chan tree.Datums
	errCh     chan error

	row tree.Datums
}

func (f *hookFnNode) StartExec(params RunParams) error {
	// TODO(dan): Make sure the resultCollector is set to flush after every row.
	f.run.resultsCh = make(chan tree.Datums)
	f.run.errCh = make(chan error)
	go func() {
		err := f.f(params.Ctx, f.subplans, f.run.resultsCh)
		select {
		case <-params.Ctx.Done():
		case f.run.errCh <- err:
		}
		close(f.run.errCh)
		close(f.run.resultsCh)
	}()
	return nil
}

func (f *hookFnNode) Next(params RunParams) (bool, error) {
	select {
	case <-params.Ctx.Done():
		return false, params.Ctx.Err()
	case err := <-f.run.errCh:
		return false, err
	case f.run.row = <-f.run.resultsCh:
		return true, nil
	}
}

func (f *hookFnNode) Values() tree.Datums { return f.run.row }

func (f *hookFnNode) Close(ctx context.Context) {
	for _, sub := range f.subplans {
		sub.Close(ctx)
	}
}

// ResolveTableDescriptor resolves a table name to its mutable descriptor
func (p *GenericPlanner) ResolveTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool,
) (table *MutableTableDescriptor, err error) {
	return p.ResolveMutableTableDescriptor(ctx, tn, required, ResolveRequireTableDesc)
}

// GetStmtSQL is get sql from PlanHookState
func GetStmtSQL(p PlanHookState) string {
	return p.(*GenericPlanner).stmt.SQL
}
