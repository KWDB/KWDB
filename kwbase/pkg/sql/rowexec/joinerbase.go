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

package rowexec

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/pkg/errors"
)

// joinerBase is the common core of all joiners.
type joinerBase struct {
	execinfra.ProcessorBase

	joinType    sqlbase.JoinType
	onCond      execinfra.ExprHelper
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow

	// EqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column EqCols[0][i] on the left side must match the
	// column EqCols[1][i] on the right side.
	eqCols [2][]uint32

	// numMergedEqualityColumns specifies how many of the equality
	// columns must be merged at the beginning of each result row. This
	// is the desired behavior for USING and NATURAL JOIN.
	numMergedEqualityColumns int
}

// Init initializes the joinerBase.
//
// opts is passed along to the underlying ProcessorBase. The zero value is used
// if the processor using the joinerBase is not implementing RowSource.
func (jb *joinerBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	leftTypes []types.T,
	rightTypes []types.T,
	jType sqlbase.JoinType,
	onExpr execinfrapb.Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	numMergedColumns uint32,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	opts execinfra.ProcStateOpts,
) error {
	jb.joinType = jType

	if jb.joinType.IsSetOpJoin() {
		if !onExpr.Empty() {
			return errors.Errorf("expected empty onExpr, got %v", onExpr.Expr)
		}
	}

	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = sqlbase.DatumToEncDatum(&leftTypes[i], tree.DNull)
	}
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = sqlbase.DatumToEncDatum(&rightTypes[i], tree.DNull)
	}

	jb.eqCols[leftSide] = leftEqColumns
	jb.eqCols[rightSide] = rightEqColumns
	jb.numMergedEqualityColumns = int(numMergedColumns)

	size := len(leftTypes) + jb.numMergedEqualityColumns + len(rightTypes)
	jb.combinedRow = make(sqlbase.EncDatumRow, size)

	condTypes := make([]types.T, 0, size)
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		ltype := leftTypes[jb.eqCols[leftSide][idx]]
		rtype := rightTypes[jb.eqCols[rightSide][idx]]
		var ctype types.T
		if ltype.Family() != types.UnknownFamily {
			ctype = ltype
		} else {
			ctype = rtype
		}
		condTypes = append(condTypes, ctype)
	}
	condTypes = append(condTypes, leftTypes...)
	condTypes = append(condTypes, rightTypes...)

	outputSize := len(leftTypes) + jb.numMergedEqualityColumns
	if shouldIncludeRightColsInOutput(jb.joinType) {
		outputSize += len(rightTypes)
	}
	outputTypes := condTypes[:outputSize]

	if err := jb.ProcessorBase.Init(
		self, post, outputTypes, flowCtx, processorID, output, nil /* memMonitor */, opts,
	); err != nil {
		return err
	}
	return jb.onCond.Init(onExpr, condTypes, jb.EvalCtx)
}

// BLLJ init is only used to init batchlookup joiner for multiple model processing
// when the switch is on and the server starts with single node mode.
// BLJInit initializes the joinerBase.
//
// opts is passed along to the underlying ProcessorBase. The zero value is used
// if the processor using the joinerBase is not implementing RowSource.
func (jb *joinerBase) BLJInit(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	leftTypes []types.T,
	rightTypes []types.T,
	jType sqlbase.JoinType,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	opts execinfra.ProcStateOpts,
) error {
	jb.joinType = jType

	// only output right side results
	outputTypes := rightTypes

	if err := jb.ProcessorBase.Init(
		self, post, outputTypes, flowCtx, processorID, output, nil /* memMonitor */, opts,
	); err != nil {
		return err
	}
	return nil
}

// joinSide is the utility type to distinguish between two sides of the join.
type joinSide uint8

const (
	// leftSide indicates the left side of the join.
	leftSide joinSide = 0
	// rightSide indicates the right side of the join.
	rightSide joinSide = 1
)

// otherSide returns the opposite to s side.
func otherSide(s joinSide) joinSide {
	return joinSide(1 - uint8(s))
}

func (j joinSide) String() string {
	if j == leftSide {
		return "left"
	}
	return "right"
}

// renderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer joins.
func (jb *joinerBase) renderUnmatchedRow(
	row sqlbase.EncDatumRow, side joinSide,
) sqlbase.EncDatumRow {
	lrow, rrow := jb.emptyLeft, jb.emptyRight
	if side == leftSide {
		lrow = row
	} else {
		rrow = row
	}

	// If there are merged columns, they take first positions in a row
	// Values are taken from non-empty row
	jb.combinedRow = jb.combinedRow[:0]
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		jb.combinedRow = append(jb.combinedRow, row[jb.eqCols[side][idx]])
	}
	jb.combinedRow = append(jb.combinedRow, lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	return jb.combinedRow
}

func shouldIncludeRightColsInOutput(joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin, sqlbase.LeftAntiJoin, sqlbase.IntersectAllJoin, sqlbase.ExceptAllJoin:
		return false
	default:
		return true
	}
}

// shouldEmitUnmatchedRow determines if we should emit am ummatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins and ANTI joins (depending on which stream is
// stored).
func shouldEmitUnmatchedRow(side joinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin, sqlbase.InnerJoin, sqlbase.IntersectAllJoin:
		return false
	case sqlbase.RightOuterJoin:
		return side == rightSide
	case sqlbase.LeftOuterJoin:
		return side == leftSide
	case sqlbase.LeftAntiJoin:
		return side == leftSide
	case sqlbase.ExceptAllJoin:
		return side == leftSide
	case sqlbase.FullOuterJoin:
		return true
	default:
		return true
	}
}

// render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
// Note the left and right merged equality columns (i.e. from a USING clause
// or after simplifying ON left.x = right.x) are NOT checked for equality.
// See CompareEncDatumRowForMerge.
func (jb *joinerBase) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	n := jb.numMergedEqualityColumns
	jb.combinedRow = jb.combinedRow[:n+len(lrow)+len(rrow)]
	for i := 0; i < n; i++ {
		// This function is called only when lrow and rrow match on the equality
		// columns which can never happen if there are any NULLs in these
		// columns. So we know for sure the lrow value is not null
		jb.combinedRow[i] = lrow[jb.eqCols[leftSide][i]]
	}
	copy(jb.combinedRow[n:], lrow)
	copy(jb.combinedRow[n+len(lrow):], rrow)

	if jb.onCond.Expr != nil {
		res, err := jb.onCond.EvalFilter(jb.combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}
	return jb.combinedRow, nil
}
