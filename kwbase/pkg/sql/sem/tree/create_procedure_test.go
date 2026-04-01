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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateProcedureFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateProcedure
		expected string
	}{
		{
			name: "basic procedure",
			node: &CreateProcedure{
				Name: MakeTableName("test_db", "test_proc"),
				Parameters: []*ProcedureParameter{
					{
						Name: "p1",
						Type: types.Int,
					},
					{
						Name: "p2",
						Type: types.String,
					},
				},
				Block: Block{
					Body: []Statement{
						&ProcSet{
							Name:  "p1",
							Value: NewNumVal(nil, "1", false),
						},
					},
				},
			},
			expected: "CREATE PROCEDURE test_db.public.test_proc(p1 INT8, p2 STRING)\nBEGIN\n\tSET p1 = 1;\nEND",
		},
		{
			name: "procedure with empty params",
			node: &CreateProcedure{
				Name:       MakeTableName("test_db", "test_proc"),
				Parameters: []*ProcedureParameter{},
				Block: Block{
					Body: []Statement{
						&ProcSet{
							Name:  "a",
							Value: NewNumVal(nil, "2", false),
						},
					},
				},
			},
			expected: "CREATE PROCEDURE test_db.public.test_proc()\nBEGIN\n\tSET a = 2;\nEND",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCreateProcedurePGFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateProcedurePG
		expected string
	}{
		{
			name: "basic pg procedure",
			node: &CreateProcedurePG{
				Name: MakeTableName("test_db", "test_proc"),
				Parameters: []*ProcedureParameter{
					{
						Name: "p1",
						Type: types.Int,
					},
				},
				BodyStr: "\nBEGIN\n\tSET p1 = 1;\nEND\n",
			},
			expected: "CREATE PROCEDURE test_db.public.test_proc(p1 INT8) $$\nBEGIN\n\tSET p1 = 1;\nEND\n$$",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestBlockFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *Block
		expected string
	}{
		{
			name: "block without label",
			node: &Block{
				Body: []Statement{
					&ProcSet{
						Name:  "a",
						Value: NewNumVal(nil, "1", false),
					},
				},
			},
			expected: "BEGIN\nSET a = 1END;\n",
		},
		{
			name: "block with label",
			node: &Block{
				Label: "my_label",
				Body: []Statement{
					&ProcSet{
						Name:  "a",
						Value: NewNumVal(nil, "1", false),
					},
				},
			},
			expected: "<<my_label>>\nBEGIN\nSET a = 1END my_label;\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestProcedureReturnFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcedureReturn{
		ReturnVal: NewNumVal(nil, "1", false),
	}
	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, "RETURN 1", ctx.CloseAndGetString())
}

func TestControlCursorFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *ControlCursor
		expected string
	}{
		{
			name: "open cursor",
			node: &ControlCursor{
				CurName: "my_cursor",
				Command: OpenCursor,
			},
			expected: "OPEN my_cursor",
		},
		{
			name: "close cursor",
			node: &ControlCursor{
				CurName: "my_cursor",
				Command: CloseCursor,
			},
			expected: "CLOSE my_cursor",
		},
		{
			name: "fetch cursor into",
			node: &ControlCursor{
				CurName:   "my_cursor",
				Command:   FetchCursor,
				FetchInto: []string{"a", "b"},
			},
			expected: "FETCH my_cursor INTO a, b",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestProcSetFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *ProcSet
		expected string
	}{
		{
			name: "basic set",
			node: &ProcSet{
				Name:  "a",
				Value: NewNumVal(nil, "1", false),
			},
			expected: "SET a = 1",
		},
		{
			name: "set new trigger col",
			node: &ProcSet{
				Name:  "new.col",
				Value: NewNumVal(nil, "1", false),
			},
			expected: "SET new.col = 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCallProcedureFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CallProcedure{
		Name: MakeTableName("db", "proc"),
		Exprs: Exprs{
			NewNumVal(nil, "1", false),
			NewNumVal(nil, "2", false),
		},
	}
	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, "CALL db.public.proc(1, 2)", ctx.CloseAndGetString())
}

func TestProcIfFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcIf{
		Condition: NewNumVal(nil, "1", false),
		ThenBody: []Statement{
			&ProcSet{Name: "a", Value: NewNumVal(nil, "1", false)},
		},
		ElseIfList: []ElseIf{
			{
				Condition: NewNumVal(nil, "2", false),
				Stmts: []Statement{
					&ProcSet{Name: "b", Value: NewNumVal(nil, "2", false)},
				},
			},
		},
		ElseBody: []Statement{
			&ProcSet{Name: "c", Value: NewNumVal(nil, "3", false)},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	expected := "IF 1 THEN\n\t\tSET a = 1;\n\tELSIF 2 THEN\n\t\tSET b = 2;\n\tELSE\n\t\tSET c = 3;\n\tENDIF"
	require.Equal(t, expected, ctx.CloseAndGetString())
}

func TestProcWhileFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcWhile{
		Label:     "l1",
		Condition: NewNumVal(nil, "1", false),
		Body: []Statement{
			&ProcSet{Name: "a", Value: NewNumVal(nil, "1", false)},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	expected := "LABEL l1:\n\tWHILE 1 DO\n\t\tSET a = 1; \n\tENDWHILE l1"
	require.Equal(t, expected, ctx.CloseAndGetString())
}

func TestProcLoopFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcLoop{
		Label: "l2",
		Body: []Statement{
			&ProcSet{Name: "a", Value: NewNumVal(nil, "1", false)},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	expected := "<<l2>>\nLOOP\nSET a = 1END LOOP l2;\n"
	require.Equal(t, expected, ctx.CloseAndGetString())
}

func TestProcCaseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcCase{
		TestExpr: NewNumVal(nil, "1", false),
		CaseWhenList: []*CaseWhen{
			{
				Expr: NewNumVal(nil, "2", false),
				Stmts: []Statement{
					&ProcSet{Name: "a", Value: NewNumVal(nil, "1", false)},
				},
			},
		},
		HaveElse: true,
		ElseStmts: []Statement{
			&ProcSet{Name: "b", Value: NewNumVal(nil, "2", false)},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	expected := "CASE1\nWHEN 2 THEN\n  SET a = 1ELSE\n  SET b = 2END CASE\n"
	require.Equal(t, expected, ctx.CloseAndGetString())
}

func TestProcLeaveFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &ProcLeave{Label: "l1"}
	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, "LEAVE l1", ctx.CloseAndGetString())
}

func TestProcedureValueProperty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p1 := NewLocalColProperty(true, DeclareValue, 1)
	require.True(t, p1.IsParam())
	require.True(t, p1.IsDeclared())
	require.False(t, p1.IsUdv())
	require.Equal(t, 1, p1.RealIdx())

	p2 := NewUDFColProperty(false, "my_udf")
	require.False(t, p2.IsParam())
	require.False(t, p2.IsDeclared())
	require.True(t, p2.IsUdv())
	require.Equal(t, "my_udf", p2.UDFName())

	p3 := p1.Copy()
	require.Equal(t, p1.IsParam(), p3.IsParam())
	require.Equal(t, p1.IsDeclared(), p3.IsDeclared())
	require.Equal(t, p1.RealIdx(), p3.RealIdx())
}

func TestIntoHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()

	h1 := ProdureUDFValue("test")
	require.Equal(t, IntoUDFValue, h1.Type)
	require.Equal(t, "test", h1.UDFValueName)

	h2 := ProdureDeclareValue(5)
	require.Equal(t, IntoDeclareValue, h2.Type)
	require.Equal(t, 5, h2.DeclareValueIdx)
}
