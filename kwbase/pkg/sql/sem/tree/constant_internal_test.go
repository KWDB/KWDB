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

package tree

import (
	"go/constant"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConstantGoCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("NumVal Methods", func(t *testing.T) {
		val := constant.MakeInt64(42)
		num := NewNumVal(val, "42", false)

		require.Equal(t, "42", num.ExactString())
		require.Equal(t, "42", num.OrigString())
		require.Equal(t, "42", num.FullOrigString())

		num.SetNegative()
		require.True(t, num.GetNegate())
		require.Equal(t, "-42", num.FullOrigString())

		num.SetNewVal(constant.MakeInt64(100), "100", false)
		require.False(t, num.GetNegate())
		require.Equal(t, "100", num.OrigString())

		require.True(t, num.ShouldBeInt64())

		f, err := num.AsFloat()
		require.NoError(t, err)
		require.Equal(t, float64(100), f)

		i32, err := num.AsInt32()
		require.NoError(t, err)
		require.Equal(t, int32(100), i32)

		i16, err := num.AsInt16()
		require.NoError(t, err)
		require.Equal(t, int16(100), i16)

		desirable := num.DesirableTypes()
		require.Equal(t, NumValAvailInteger, desirable)

		numFloat := NewNumVal(constant.MakeFloat64(10.5), "10.5", false)
		require.False(t, numFloat.ShouldBeInt64())
		require.Equal(t, NumValAvailDecimalWithFraction, numFloat.DesirableTypes())

		// Test out of bounds
		numLarge := NewNumVal(constant.MakeInt64(9999999999), "9999999999", false)
		_, err = numLarge.AsInt32()
		require.Error(t, err)
		_, err = numLarge.AsInt16()
		require.Error(t, err)

		// ResolveAsType
		ctx := &SemaContext{}
		datum, err := num.ResolveAsType(ctx, types.Int)
		require.NoError(t, err)
		require.Equal(t, NewDInt(100), datum)

		datum, err = num.ResolveAsType(ctx, types.Oid)
		require.NoError(t, err)
		oid := NewDOid(100)
		oid.semanticType = types.Oid
		require.Equal(t, oid, datum)

		numRat := NewNumVal(constant.MakeFloat64(0.5), "1/2", false)
		datum, err = numRat.ResolveAsType(ctx, types.Decimal)
		require.NoError(t, err)
		dec, _ := ParseDDecimal("0.5")
		require.Equal(t, dec, datum)

		numRatDiv0 := NewNumVal(constant.MakeFloat64(0.0), "1/0", false)
		_, err = numRatDiv0.ResolveAsType(ctx, types.Decimal)
		require.Error(t, err)

		_, err = numRat.ResolveAsType(ctx, types.String)
		require.Error(t, err)
	})

	t.Run("StrVal Methods", func(t *testing.T) {
		str := NewStrVal("hello")
		str.AssignString("world")
		require.Equal(t, "world", str.RawString())

		desirable := str.DesirableTypes()
		require.Equal(t, StrValAvailAllParsable, desirable)
	})

	t.Run("Unexported functions", func(t *testing.T) {
		num := NewNumVal(constant.MakeInt64(1), "1", false)
		require.Equal(t, types.Int, naturalConstantType(num))
		require.True(t, canConstantBecome(num, types.Float))
		require.False(t, canConstantBecome(num, types.String))

		xs := []*types.T{types.Int, types.Float, types.Decimal}
		ys := []*types.T{types.Float, types.String, types.Int}
		intersect := intersectTypeSlices(xs, ys)
		require.ElementsMatch(t, []*types.T{types.Int, types.Float}, intersect)

		str := NewStrVal("test")
		exprs := []Expr{num, str}

		// Common constant type with retTyp matched
		typ, ok := commonConstantType(exprs, []int{0}, types.Int)
		require.True(t, ok)
		require.Equal(t, types.Int, typ)

		// Common constant type intersect
		typ, ok = commonConstantType(exprs, []int{0, 1}, nil)
		require.True(t, ok)
		require.Equal(t, types.Int, typ) // Int is common in both NumVal and StrVal types

		// StrVal Format needSource
		ctx := NewFmtCtx(FmtSimple)
		ctx.needSource = true
		str.Format(ctx)
		require.Equal(t, "'test'", ctx.CloseAndGetString())
	})
}
