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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// distinctNode de-duplicates rows returned by a wrapped PlanNode.
var _ PlanNode = &distinctNode{}

type distinctNode struct {
	plan PlanNode

	// distinctOnColIdxs are the column indices of the child PlanNode and
	// is what defines the distinct key.
	// For a normal DISTINCT (without the ON clause), distinctOnColIdxs
	// contains all the column indices of the child PlanNode.
	// Otherwise, distinctOnColIdxs is a strict subset of the child
	// PlanNode's column indices indicating which columns are specified in
	// the DISTINCT ON (<exprs>) clause.
	distinctOnColIdxs util.FastIntSet

	// Subset of distinctOnColIdxs on which the input guarantees an ordering.
	// All rows that are equal on these columns appear contiguously in the input.
	columnsInOrder util.FastIntSet

	reqOrdering ReqOrdering

	// nullsAreDistinct, if true, causes the distinct operation to treat NULL
	// values as not equal to one another. Each NULL value will cause a new row
	// group to be created. For example:
	//
	//   c
	//   ----
	//   NULL
	//   NULL
	//
	// A distinct operation on column "c" will result in one output row if
	// nullsAreDistinct is false, or two output rows if true. This is set to true
	// for UPSERT and INSERT..ON CONFLICT statements, since they must treat NULL
	// values as distinct.
	nullsAreDistinct bool

	// errorOnDup, if non-empty, is the text of the error that will be raised if
	// the distinct operation finds two rows with duplicate grouping column
	// values. This is used to implement the UPSERT and INSERT..ON CONFLICT
	// statements, both of which prohibit the same row from being changed twice.
	errorOnDup string

	// engine is a bit set that indicates which engine to exec.
	engine tree.EngineType
}

func (n *distinctNode) StartExec(params RunParams) error {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Next(params RunParams) (bool, error) {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Values() tree.Datums {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
