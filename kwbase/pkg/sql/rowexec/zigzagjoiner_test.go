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

package rowexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

type zigzagJoinerTestCase struct {
	desc          string
	spec          execinfrapb.ZigzagJoinerSpec
	outCols       []uint32
	fixedValues   []sqlbase.EncDatumRow
	expectedTypes []types.T
	expected      string
}

func intCols(numCols int) []types.T {
	cols := make([]types.T, numCols)
	for i := range cols {
		cols[i] = *types.Int
	}
	return cols
}

func encInt(i int) sqlbase.EncDatum {
	return sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
}

func TestZigzagJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	null := tree.DNull

	identity := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row))
	}
	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 5))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 5))
	}
	cFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row%3 + 3))
	}
	dFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt((row+1)%4 + 4))
	}
	eFn := func(row int) tree.Datum {
		if row%5 == 0 {
			return null
		}
		return tree.NewDInt(tree.DInt((row+1)%4 + 4))
	}

	offsetFn := func(oldFunc func(int) tree.Datum, offset int) func(int) tree.Datum {
		offsetFunc := func(row int) tree.Datum {
			return oldFunc(row + offset)
		}
		return offsetFunc
	}

	sqlutils.CreateTableDebug(t, sqlDB, "empty",
		"a INT, b INT, x INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		0,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	// Drop a column to test https://gitee.com/kwbasedb/kwbase/issues/37196
	_, err := sqlDB.Exec("ALTER TABLE test.empty DROP COLUMN x")
	require.NoError(t, err)

	// Drop and add an index to test
	// https://gitee.com/kwbasedb/kwbase/issues/42164.
	_, err = sqlDB.Exec("DROP INDEX test.empty@d")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE INDEX d ON test.empty(d)")
	require.NoError(t, err)

	sqlutils.CreateTableDebug(t, sqlDB, "single",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		1,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "small",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		10,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "med",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "overlapping",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX ac (a, c), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "comp",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX cab (c, a, b), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "rev",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX cba (c, b, a), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
		true, /* shouldPrint */
	)

	offset := 44
	sqlutils.CreateTableDebug(t, sqlDB, "offset",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		20,
		sqlutils.ToRowFn(offsetFn(aFn, offset), offsetFn(bFn, offset), offsetFn(cFn, offset), offsetFn(dFn, offset)),
		true, /* shouldPrint */
	)

	unqOffset := 20
	sqlutils.CreateTableDebug(t, sqlDB, "unq",
		"a INT, b INT, c INT UNIQUE, d INT, PRIMARY KEY (a, b), INDEX cb (c, b), INDEX d (d)",
		20,
		sqlutils.ToRowFn(
			offsetFn(aFn, unqOffset),
			offsetFn(bFn, unqOffset),
			offsetFn(identity, unqOffset),
			offsetFn(dFn, unqOffset),
		),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "t2",
		"b INT, a INT, PRIMARY KEY (b, a)",
		10,
		sqlutils.ToRowFn(bFn, aFn),
		true, /* shouldPrint */
	)

	sqlutils.CreateTableDebug(t, sqlDB, "nullable",
		"a INT, b INT, e INT, d INT, PRIMARY KEY (a, b), INDEX e (e), INDEX d (d)",
		10,
		sqlutils.ToRowFn(aFn, bFn, eFn, dFn),
		true, /* shouldPrint */
	)

	empty := sqlbase.GetTableDescriptor(kvDB, "test", "empty")
	single := sqlbase.GetTableDescriptor(kvDB, "test", "single")
	smallDesc := sqlbase.GetTableDescriptor(kvDB, "test", "small")
	medDesc := sqlbase.GetTableDescriptor(kvDB, "test", "med")
	highRangeDesc := sqlbase.GetTableDescriptor(kvDB, "test", "offset")
	overlappingDesc := sqlbase.GetTableDescriptor(kvDB, "test", "overlapping")
	compDesc := sqlbase.GetTableDescriptor(kvDB, "test", "comp")
	revCompDesc := sqlbase.GetTableDescriptor(kvDB, "test", "rev")
	compUnqDesc := sqlbase.GetTableDescriptor(kvDB, "test", "unq")
	t2Desc := sqlbase.GetTableDescriptor(kvDB, "test", "t2")
	nullableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "nullable")

	testCases := []zigzagJoinerTestCase{
		{
			desc: "join on an empty table with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*empty, *empty},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join an empty table on the left with a populated table on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*empty, *highRangeDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a populated table on the left with an empty table on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*highRangeDesc, *empty},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a table with a single row with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*single, *single},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a table with a few rows with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*smallDesc, *smallDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7]]",
		},
		{
			desc: "join a populated table that has a match in the last row with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*medDesc, *medDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [3 3 3 7]]",
		},
		{
			desc: "(a) is free, and outputs cartesian product",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*medDesc, *medDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{0}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 4, 5, 7},
			expectedTypes: intCols(6),
			expected: "[[0 3 3 0 2 7] [1 4 3 1 1 7] [1 1 3 1 1 7] [2 2 3 2 4 7] [2 2 3 2 0 7] [3 3 3 3 3 7] " +
				"[3 0 3 3 3 7] [4 1 3 4 2 7]]",
		},
		{
			desc: "set the fixed columns to be a part of the primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*medDesc, *medDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{1}}, {Columns: []uint32{1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7), encInt(1)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7]]",
		},
		{
			desc: "join should work when there is a block of matches",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*highRangeDesc, *highRangeDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{0}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 4, 5, 7},
			expectedTypes: intCols(6),
			expected: "[[9 3 3 9 1 7] [9 0 3 9 1 7] [10 4 3 10 4 7] [10 4 3 10 0 7] [10 1 3 10 4 7] " +
				"[10 1 3 10 0 7] [11 2 3 11 3 7] [12 3 3 12 2 7] [12 0 3 12 2 7]]",
		},
		{
			desc: "join two different tables where first one is larger",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*medDesc, *smallDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 4, 5, 7},
			expectedTypes: intCols(6),
			expected:      "[[1 1 3 1 1 7]]",
		},
		{
			desc: "join two different tables where second is larger",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*smallDesc, *medDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 4, 5, 7},
			expectedTypes: intCols(6),
			expected:      "[[1 1 3 1 1 7]]",
		},
		{
			desc: "join on an index containing primary key columns explicitly",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*overlappingDesc, *overlappingDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{1}}, {Columns: []uint32{1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (a, c) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3) /*a*/, encInt(3) /*c*/}, {encInt(7) /*d*/, encInt(3) /*a*/}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[3 3 3 7]]",
		},
		{
			desc: "join two tables with different schemas",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*smallDesc, *t2Desc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0 /* (a, b) */, 0 /* (a, b) */},
			},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [1 0] [1 1] [2 0]]",
		},
		{
			desc: "join two tables with different schemas flipped",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*t2Desc, *smallDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0 /* (a, b) */, 0 /* (a, b) */},
			},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [1 0] [1 1] [2 0]]",
		},
		{
			desc: "join on a populated table with no fixed columns",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*smallDesc, *smallDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0 /* (a, b) */, 0 /* (a, b) */},
			},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [0 3] [0 4] [1 0] [1 1] [1 2] [1 3] [1 4] [2 0]]",
		},
		{
			desc: "join tables with different schemas with no locked columns",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*smallDesc, *t2Desc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{1}}, {Columns: []uint32{1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0 /* (a, b) */, 0 /* (a, b) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(1)}, {encInt(1)}},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[1 0] [1 1]]",
		},
		{
			desc: "join a composite index with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*compDesc, *compDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c, a, b) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [3 3 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*revCompDesc, *revCompDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{0}}}, // join on a
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c, b, a) */, 2 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [4 1 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself with onExpr on value on one side",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*revCompDesc, *revCompDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{0}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c, b, a) */, 2 /* (d) */},
				OnExpr:        execinfrapb.Expression{Expr: "@1 > 1"},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[4 1 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself and with onExpr comparing both sides",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*revCompDesc, *revCompDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{0}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (c, b, a) */, 2 /* (d) */},
				OnExpr:        execinfrapb.Expression{Expr: "@8 < 2 * @1"},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[4 1 3 7]]",
		},
		{
			desc: "join a composite index that doesn't contain the full primary key with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*compUnqDesc, *compUnqDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{1}}, {Columns: []uint32{1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{2 /* (c, b) */, 3 /* (d) */},
			},
			fixedValues:   []sqlbase.EncDatumRow{{encInt(21) /* c */}, {encInt(6), encInt(4) /* d, a */}},
			outCols:       []uint32{0, 1, 2, 7},
			expectedTypes: intCols(4),
			expected:      "[[4 1 21 6]]",
		},
		{
			desc: "test when equality columns may be null",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*nullableDesc, *nullableDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{2}}, {Columns: []uint32{3}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{1 /* (e) */, 2 /* (d) */},
			},
			outCols:       []uint32{0, 1, 2, 4, 5, 7},
			expectedTypes: intCols(6),
			expected: "[[1 2 4 1 2 4] [1 2 4 0 3 4] [0 3 4 1 2 4] [0 3 4 0 3 4] [1 3 5 1 3 5] " +
				"[1 3 5 0 4 5] [0 4 5 1 3 5] [0 4 5 0 4 5] [1 4 6 1 4 6] [1 4 6 1 0 6] [1 4 6 0 1 6] " +
				"[0 1 6 1 4 6] [0 1 6 1 0 6] [0 1 6 0 1 6] [1 1 7 2 0 7] [1 1 7 1 1 7] [1 1 7 0 2 7] " +
				"[0 2 7 2 0 7] [0 2 7 1 1 7] [0 2 7 0 2 7]]",
		},
		{
			desc: "test joining with primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*medDesc, *medDesc},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0}}, {Columns: []uint32{3}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0 /* primary (a, b) */, 2 /* (d) */},
			},
			outCols:       []uint32{0, 1, 2, 3, 4, 5, 7},
			expectedTypes: intCols(7),
			expected: "[[4 2 4 7 3 4 4] [4 1 3 6 3 4 4] [4 0 5 5 3 4 4] [4 2 4 7 3 0 4] [4 1 3 6 3 0 4] " +
				"[4 0 5 5 3 0 4] [4 2 4 7 2 1 4] [4 1 3 6 2 1 4] [4 0 5 5 2 1 4] [4 2 4 7 1 2 4] " +
				"[4 1 3 6 1 2 4] [4 0 5 5 1 2 4] [4 2 4 7 0 3 4] [4 1 3 6 0 3 4] [4 0 5 5 0 3 4]]",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: st},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
			}

			out := &distsqlutils.RowBuffer{}
			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			z, err := newZigzagJoiner(&flowCtx, 0 /* processorID */, &c.spec, c.fixedValues, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			z.Run(ctx)

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}

			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(c.expectedTypes); result != c.expected {
				t.Errorf("invalid results for test '%s': %s, expected %s'", c.desc, result, c.expected)
			}
		})
	}
}

// TestJoinReaderDrain tests various scenarios in which a zigzagJoiner's consumer
// is closed.
func TestZigzagJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	v := [10]tree.Datum{}
	for i := range v {
		v[i] = tree.NewDInt(tree.DInt(i))
	}
	encThree := sqlbase.DatumToEncDatum(types.Int, v[3])
	encSeven := sqlbase.DatumToEncDatum(types.Int, v[7])

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn, sqlutils.RowIdxFn, sqlutils.RowIdxFn),
	)
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	// Run the flow in a snowball trace so that we can test for tracing info.
	tracer := tracing.NewTracer()
	ctx, sp := tracing.StartSnowballTrace(context.Background(), tracer, "test flow ctx")
	defer sp.Finish()
	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), &leafInputState)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
		Txn:     leafTxn,
	}

	testReaderProcessorDrain(ctx, t, func(out execinfra.RowReceiver) (execinfra.Processor, error) {
		return newZigzagJoiner(
			&flowCtx,
			0, /* processorID */
			&execinfrapb.ZigzagJoinerSpec{
				Tables:        []sqlbase.TableDescriptor{*td, *td},
				EqColumns:     []execinfrapb.Columns{{Columns: []uint32{0, 1}}, {Columns: []uint32{0, 1}}},
				Type:          sqlbase.InnerJoin,
				IndexOrdinals: []uint32{0, 1},
			},
			[]sqlbase.EncDatumRow{{encThree}, {encSeven}},
			&execinfrapb.PostProcessSpec{Projection: true, OutputColumns: []uint32{0, 1}},
			out,
		)
	})
}
