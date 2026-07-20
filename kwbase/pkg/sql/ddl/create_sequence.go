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

var _ sql.PlanNode = &createSequenceNode{}
var _ sql.PlanNodeReadingOwnWrites = &createSequenceNode{}

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc *DatabaseDescriptor
}

// NewCreateSequenceNode creates a new createSequenceNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateSequenceNode(n *tree.CreateSequence, dbDesc *DatabaseDescriptor) *createSequenceNode {
	return &createSequenceNode{
		n:      n,
		dbDesc: dbDesc,
	}
}

// CreateSequence handles CREATE SEQUENCE statements
func CreateSequence(
	ctx context.Context, p *GenericPlanner, n *tree.CreateSequence,
) (sql.PlanNode, error) {
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Name)
	if err != nil {
		return nil, err
	}

	if err1 := TSDatabaseUnsupportedErr(dbDesc.EngineType, "create sequence"); err1 != nil {
		return nil, err1
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createSequenceNode{
		n:      n,
		dbDesc: dbDesc,
	}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because CREATE SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createSequenceNode) ReadingOwnWrites() {}

func (n *createSequenceNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("sequence"))
	isTemporary := n.n.Temporary

	_, schemaID, err := sql.GetTableCreateParams(params, n.dbDesc.ID, isTemporary, n.n.Name)
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	err = sql.DoCreateSequence(
		params, n.n.String(), n.dbDesc, schemaID, &n.n.Name, isTemporary, n.n.Options,
		tree.AsStringWithFQNames(n.n, params.Ann()), false,
	)
	if err != nil {
		return err
	}

	return nil
}

func (*createSequenceNode) Next(RunParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}
