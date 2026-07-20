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

package ordering

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

func mutationCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// The mutation operator can always pass through ordering to its input.
	return true
}

func mutationBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}

	// Remap each of the required columns to corresponding input columns.
	private := parent.Private().(*memo.MutationPrivate)

	optional := private.MapToInputCols(required.Optional())
	columns := make([]physical.OrderingColumnChoice, len(required.Columns))
	for i := range required.Columns {
		colChoice := &required.Columns[i]
		columns[i] = physical.OrderingColumnChoice{
			Group:      private.MapToInputCols(colChoice.Group),
			Descending: colChoice.Descending,
		}
	}
	ret := physical.OrderingChoice{Columns: columns}
	ret.OptionalInit(optional)
	return ret
}

func mutationBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	private := expr.Private().(*memo.MutationPrivate)
	input := expr.Child(0).(memo.RelExpr)
	provided := input.ProvidedPhysical().Ordering

	// Construct FD set that includes mapping to/from input columns. This will
	// be used by remapProvided.
	var fdset props.FuncDepSet
	fdset.CopyFrom(&input.Relational().FuncDeps)
	private.AddEquivTableCols(expr.Memo().Metadata(), &fdset)

	// Ensure that provided ordering only uses projected columns.
	return remapProvided(provided, &fdset, expr.Relational().OutputCols)
}
