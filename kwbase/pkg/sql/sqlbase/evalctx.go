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

package sqlbase

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// DummySequenceOperators implements the tree.SequenceOperators interface by
// returning errors.
type DummySequenceOperators struct{}

var _ tree.EvalDatabase = &DummySequenceOperators{}

var errSequenceOperators = unimplemented.NewWithIssue(42508,
	"cannot evaluate scalar expressions containing sequence operations in this context")

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// LookupSchema is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errors.WithStack(errSequenceOperators)
}

// IncrementSequence is part of the tree.SequenceOperators interface.
func (so *DummySequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators
// interface.
func (so *DummySequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// SetSequenceValue implements the tree.SequenceOperators interface.
func (so *DummySequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errors.WithStack(errSequenceOperators)
}

// DummyEvalPlanner implements the tree.EvalPlanner interface by returning
// errors.
type DummyEvalPlanner struct{}

var _ tree.EvalPlanner = &DummyEvalPlanner{}

var errEvalPlanner = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions using table lookups in this context")

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return parser.ParseQualifiedTableName(sql)
}

// LookupSchema is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errors.WithStack(errEvalPlanner)
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errEvalPlanner)
}

// ParseType is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) ParseType(sql string) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// MakeNewPlanAndRunForTsInsert create an internal planner as the planner and run
func (ep *DummyEvalPlanner) MakeNewPlanAndRunForTsInsert(
	ctx context.Context, evalCtx *tree.EvalContext, param tree.TSInsertSelectParam,
) (int, error) {
	return 0, nil
}

// GetStmt return stmt
func (ep *DummyEvalPlanner) GetStmt() string {
	return ""
}

// EvalSubquery is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ExecutorConfig is part of the Planner interface.
func (*DummyEvalPlanner) ExecutorConfig() interface{} {
	return nil
}

// DummyPrivilegedAccessor implements the tree.PrivilegedAccessor interface by returning errors.
type DummyPrivilegedAccessor struct{}

var _ tree.PrivilegedAccessor = &DummyPrivilegedAccessor{}

var errEvalPrivileged = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate privileged expressions in this context")

// LookupNamespaceID is part of the tree.PrivilegedAccessor interface.
func (ep *DummyPrivilegedAccessor) LookupNamespaceID(
	ctx context.Context, parentID int64, name string,
) (tree.DInt, bool, error) {
	return 0, false, errors.WithStack(errEvalPrivileged)
}

// LookupZoneConfigByNamespaceID is part of the tree.PrivilegedAccessor interface.
func (ep *DummyPrivilegedAccessor) LookupZoneConfigByNamespaceID(
	ctx context.Context, id int64,
) (tree.DBytes, bool, error) {
	return "", false, errors.WithStack(errEvalPrivileged)
}

// DummySessionAccessor implements the tree.EvalSessionAccessor interface by returning errors.
type DummySessionAccessor struct{}

var _ tree.EvalSessionAccessor = &DummySessionAccessor{}

var errEvalSessionVar = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions that access session variables in this context")

// GetSessionVar is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) GetSessionVar(
	_ context.Context, _ string, _ bool,
) (bool, string, error) {
	return false, "", errors.WithStack(errEvalSessionVar)
}

// GetUserDefinedVar is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) GetUserDefinedVar(
	_ context.Context, _ string, _ bool,
) (bool, interface{}, error) {
	return false, nil, errors.WithStack(errEvalSessionVar)
}

// SetSessionVar is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) SetSessionVar(_ context.Context, _, _ string) error {
	return errors.WithStack(errEvalSessionVar)
}

// HasAdminRole is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) HasAdminRole(_ context.Context) (bool, error) {
	return false, errors.WithStack(errEvalSessionVar)
}

// DummyClientNoticeSender implements the tree.ClientNoticeSender interface.
type DummyClientNoticeSender struct{}

var _ tree.ClientNoticeSender = &DummyClientNoticeSender{}

// SendClientNotice is part of the tree.ClientNoticeSender interface.
func (c *DummyClientNoticeSender) SendClientNotice(_ context.Context, _ error) {}
