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

package memo

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestMemoCheckAESupportType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.Equal(t, checkAESupportType(oid.T_timestamp), true)
}

func TestMemoGetExprType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	GetExprType(&SumExpr{Input: &VariableExpr{Col: 1, Typ: types.Int}, Typ: types.Decimal})
	GetExprType(&FunctionExpr{IsConstForLogicPlan: true})
}

func TestMemoAddConstValueToPTagValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	src1 := VariableExpr{Col: 0, Typ: types.Int}
	src2 := FalseExpr{}
	src3 := TrueExpr{}
	primaryTagCol := make(TagColMap)
	val := make(PTagValues, 0)
	addConstValueToPTagValues(&src1, &src2, primaryTagCol, &val)
	addConstValueToPTagValues(&src1, &src3, primaryTagCol, &val)
	primaryTagCol[0] = struct{}{}
	addConstValueToPTagValues(&src1, &src2, primaryTagCol, &val)
	addConstValueToPTagValues(&src1, &src3, primaryTagCol, &val)
}

func TestMemoCheckAggCanParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src1 := &MaxExpr{}
	src2 := &AggDistinctExpr{Input: src1}
	src3 := &BitOrAggExpr{}
	src4 := &AggDistinctExpr{Input: src3}
	ret1, ret2 := CheckAggCanParallel(src1)
	require.Equal(t, ret1, true)
	require.Equal(t, ret2, false)

	ret1, ret2 = CheckAggCanParallel(src2)
	require.Equal(t, ret1, true)
	require.Equal(t, ret2, true)

	ret1, ret2 = CheckAggCanParallel(src3)
	require.Equal(t, ret1, false)
	require.Equal(t, ret2, false)

	ret1, ret2 = CheckAggCanParallel(src4)
	require.Equal(t, ret1, false)
	require.Equal(t, ret2, true)
}
