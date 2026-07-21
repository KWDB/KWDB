// Copyright (c) 2026-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.

package main

import (
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFloatDivisionOverloadChecksForZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	assign := floatCustomizer{width: 64}.getBinOpAssignFunc()
	generated := assign(overload{BinOp: tree.Div}, "target", "left", "right")
	require.True(t, strings.Contains(generated, "right == 0.0"))
	require.True(t, strings.Contains(generated, "execerror.NonVectorizedPanic(tree.ErrDivByZero)"))
}
