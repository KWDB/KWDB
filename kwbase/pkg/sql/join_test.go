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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
)

func newTestScanNode(kvDB *kv.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.GetImmutableTableDescriptor(kvDB, sqlutils.TestDB, tableName)

	p := GenericPlanner{}
	scan := p.Scan()
	scan.desc = desc
	err := scan.initDescDefaults(p.curPlan.deps, publicColumnsCfg)
	if err != nil {
		return nil, err
	}
	// Initialize the required ordering.
	columnIDs, dirs := scan.index.FullColumnIDs()
	ordering := make(sqlbase.ColumnOrdering, len(columnIDs))
	for i, colID := range columnIDs {
		idx, ok := scan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		ordering[i].ColIdx = idx
		ordering[i].Direction, err = dirs[i].ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	scan.reqOrdering = ordering
	sb := span.MakeBuilder(desc.TableDesc(), &desc.PrimaryIndex)
	scan.spans, err = sb.SpansFromConstraint(nil /* constraint */, exec.ColumnOrdinalSet{}, false /* forDelete */)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

func newTestJoinNode(kvDB *kv.DB, leftName, rightName string) (*joinNode, error) {
	left, err := newTestScanNode(kvDB, leftName)
	if err != nil {
		return nil, err
	}
	right, err := newTestScanNode(kvDB, rightName)
	if err != nil {
		return nil, err
	}
	return &joinNode{
		left:  planDataSource{plan: left},
		right: planDataSource{plan: right},
	}, nil
}
