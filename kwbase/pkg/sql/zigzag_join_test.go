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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestZigzagJoinNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a simple zigzagJoinNode for testing
	scan1 := &scanNode{}
	scan2 := &scanNode{}

	// Create zigzagJoinSides
	side1 := zigzagJoinSide{
		scan:      scan1,
		eqCols:    []int{0, 1},
		fixedVals: &valuesNode{},
	}
	side2 := zigzagJoinSide{
		scan:      scan2,
		eqCols:    []int{0, 1},
		fixedVals: &valuesNode{},
	}

	// Create columns
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.Int},
	}

	// Create zigzagJoinNode
	zj := &zigzagJoinNode{
		sides:       []zigzagJoinSide{side1, side2},
		columns:     columns,
		onCond:      nil,
		reqOrdering: ReqOrdering{},
	}

	// Test startExec
	defer func() {
		if r := recover(); r == nil {
			t.Error("startExec should have panicked")
		}
	}()
	zj.startExec(runParams{})

	// Test Next
	defer func() {
		if r := recover(); r == nil {
			t.Error("Next should have panicked")
		}
	}()
	zj.Next(runParams{})

	// Test Values
	defer func() {
		if r := recover(); r == nil {
			t.Error("Values should have panicked")
		}
	}()
	zj.Values()

	// Test Close
	// Close should not panic
	zj.Close(context.Background())
}
