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
)

func TestCheckTsScanNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p planNode
	tsScan := &tsScanNode{}
	CheckTsScanNode(tsScan)

	p = &applyJoinNode{input: planDataSource{plan: tsScan}}
	CheckTsScanNode(p)

	p = &max1RowNode{plan: tsScan}
	CheckTsScanNode(p)

	p = &splitNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &unsplitNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &relocateNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &upsertNode{source: tsScan}
	CheckTsScanNode(p)

	p = &rowCountNode{source: &updateNode{source: tsScan}}
	CheckTsScanNode(p)

	p = &createTableNode{sourcePlan: tsScan}
	CheckTsScanNode(p)

	p = &explainVecNode{plan: tsScan}
	CheckTsScanNode(p)

	p = &ordinalityNode{source: tsScan}
	CheckTsScanNode(p)

	p = &spoolNode{source: tsScan}
	CheckTsScanNode(p)

	p = &saveTableNode{source: tsScan}
	CheckTsScanNode(p)

	p = &showTraceReplicaNode{plan: tsScan}
	CheckTsScanNode(p)

	p = &cancelQueriesNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &cancelSessionsNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &controlJobsNode{rows: tsScan}
	CheckTsScanNode(p)

	p = &errorIfRowsNode{plan: tsScan}
	CheckTsScanNode(p)

	p = &bufferNode{plan: tsScan}
	CheckTsScanNode(p)

	p = &recursiveCTENode{initial: tsScan}
	CheckTsScanNode(p)

	p = &exportNode{source: tsScan}
	CheckTsScanNode(p)

	p = &synchronizerNode{plan: tsScan}
	CheckTsScanNode(p)
}
