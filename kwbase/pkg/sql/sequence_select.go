// Copyright 2018 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type sequenceSelectNode struct {
	OptColumnsSlot

	desc *sqlbase.ImmutableTableDescriptor

	val  int64
	done bool
}

var _ PlanNode = &sequenceSelectNode{}

// SequenceSelectNode creates a plan node for selecting the next sequence value
func (p *GenericPlanner) SequenceSelectNode(
	desc *sqlbase.ImmutableTableDescriptor,
) (PlanNode, error) {
	if desc.SequenceOpts == nil {
		return nil, errors.New(fmt.Sprintf("descriptor %s is not a sequence", desc.Name))
	}
	return &sequenceSelectNode{
		desc: desc,
	}, nil
}

func (ss *sequenceSelectNode) StartExec(RunParams) error {
	return nil
}

func (ss *sequenceSelectNode) Next(params RunParams) (bool, error) {
	if ss.done {
		return false, nil
	}
	val, err := params.p.GetSequenceValue(params.Ctx, ss.desc)
	if err != nil {
		return false, err
	}
	ss.val = val
	ss.done = true
	return true, nil
}

func (ss *sequenceSelectNode) Values() tree.Datums {
	valDatum := tree.DInt(ss.val)
	cntDatum := tree.DInt(0)
	calledDatum := tree.DBoolTrue
	return []tree.Datum{
		&valDatum,
		&cntDatum,
		calledDatum,
	}
}

func (ss *sequenceSelectNode) Close(ctx context.Context) {}
