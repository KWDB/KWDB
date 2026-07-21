// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCheckPlanNodeType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	writePlanNodeList := []PlanNode{
		&insertNode{},
		&tsInsertNode{},

		&updateNode{},

		&upsertNode{},

		&deleteNode{},
		&deleteRangeNode{},

		&truncateNode{},
	}
	readPlanNodeList := []PlanNode{
		&scanBufferNode{},
		&scanNode{},
		&tsScanNode{},
		&showTraceNode{},
		&explainDistSQLNode{},
		&explainPlanNode{},
	}
	for _, n := range writePlanNodeList {

		err := checkPlanNodeType(n)
		require.Equal(t, "cannot execute CREATE,INSERT,UPDATE,UPSERT,DELETE,DROP,ALTER,TRUNCATE,RENAME in a read-only transaction", err.Error())
	}
	for _, n := range readPlanNodeList {
		err := checkPlanNodeType(n)
		require.Equal(t, nil, err)
	}
}
