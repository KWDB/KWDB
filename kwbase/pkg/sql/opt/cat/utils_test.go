// Copyright 2019 The Cockroach Authors.
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

package cat_test

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/treeprinter"
	"github.com/gogo/protobuf/proto"
)

func TestExpandDataSourceGlob(t *testing.T) {
	testcat := testcat.New()
	ctx := context.Background()

	exec := func(sql string) {
		if _, err := testcat.ExecuteDDL(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE a (x INT)")
	exec("CREATE TABLE b (x INT)")
	exec("CREATE TABLE c (x INT)")

	testCases := []struct {
		pattern  tree.TablePattern
		expected string
	}{
		{
			pattern:  tree.NewTableName("t", "a"),
			expected: `[t.public.a]`,
		},
		{
			pattern:  tree.NewTableName("t", "z"),
			expected: `error: no data source matches prefix: "t.public.z"`,
		},
		{
			pattern:  &tree.AllTablesSelector{TableNamePrefix: tree.TableNamePrefix{}},
			expected: `[t.public.a t.public.b t.public.c]`,
		},
		{
			pattern: &tree.AllTablesSelector{TableNamePrefix: tree.TableNamePrefix{
				SchemaName: "t", ExplicitSchema: true,
			}},
			expected: `[t.public.a t.public.b t.public.c]`,
		},
		{
			pattern: &tree.AllTablesSelector{TableNamePrefix: tree.TableNamePrefix{
				SchemaName: "z", ExplicitSchema: true,
			}},
			expected: `error: target database or schema does not exist`,
		},
	}

	for _, tc := range testCases {
		var res string
		names, err := cat.ExpandDataSourceGlob(ctx, testcat, cat.Flags{}, tc.pattern)
		if err != nil {
			res = fmt.Sprintf("error: %v", err)
		} else {
			var r []string
			for _, n := range names {
				r = append(r, n.FQString())
			}
			res = fmt.Sprintf("%v", r)
		}
		if res != tc.expected {
			t.Errorf("pattern: %v  expected: %s  got: %s", tc.pattern, tc.expected, res)
		}
	}
}

func TestResolveTableIndex(t *testing.T) {
	testcat := testcat.New()
	ctx := context.Background()

	exec := func(sql string) {
		if _, err := testcat.ExecuteDDL(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE a (x INT, INDEX idx1(x))")
	exec("CREATE TABLE b (x INT, INDEX idx2(x))")
	exec("CREATE TABLE c (x INT, INDEX idx2(x))")

	testCases := []struct {
		name     tree.TableIndexName
		expected string
	}{
		// Both table name and index are set.
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
				Index: "idx2",
			},
			expected: `error: index "idx2" does not exist`,
		},

		// Only table name is set.
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
			},
			expected: `t.public.a@primary`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("z", "a"),
			},
			expected: `error: no data source matches prefix: "z.public.a"`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "z"),
			},
			expected: `error: no data source matches prefix: "t.public.z"`,
		},

		// Only index name is set.
		{
			name: tree.TableIndexName{
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", ""),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: func() tree.TableName {
					var t tree.TableName
					t.SchemaName = "public"
					t.ExplicitSchema = true
					return t
				}(),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("z", ""),
				Index: "idx1",
			},
			expected: `error: target database or schema does not exist`,
		},
		{
			name: tree.TableIndexName{
				Index: "idx2",
			},
			expected: `error: index name "idx2" is ambiguous (found in t.public.c and t.public.b)`,
		},
	}

	for _, tc := range testCases {
		var res string
		idx, tn, err := cat.ResolveTableIndex(ctx, testcat, cat.Flags{}, &tc.name)
		if err != nil {
			res = fmt.Sprintf("error: %v", err)
		} else {
			res = fmt.Sprintf("%s@%s", tn.FQString(), idx.Name())
		}
		if res != tc.expected {
			t.Errorf("pattern: %v  expected: %s  got: %s", tc.name.String(), tc.expected, res)
		}
	}
}

func TestConvertColumnIDsToOrdinals(t *testing.T) {
	col1 := &testcat.Column{Ordinal: 0, Name: ("col1")}
	col2 := &testcat.Column{Ordinal: 1, Name: ("col2")}
	col3 := &testcat.Column{Ordinal: 2, Name: ("col3")}

	table := &testcat.Table{
		TabName: tree.MakeTableName("", "test_table"),
		TabID:   123,
		Columns: []*testcat.Column{col1, col2, col3},
	}

	columnIDs := []tree.ColumnID{1, 2, 3}
	ordinals := cat.ConvertColumnIDsToOrdinals(table, columnIDs)

	expected := []int{0, 1, 2}
	for i, exp := range expected {
		if ordinals[i] != exp {
			t.Errorf("Expected ordinal %d to be %d, got %d", i, exp, ordinals[i])
		}
	}
}

func TestFindTableColumnByName(t *testing.T) {
	col1 := &testcat.Column{Ordinal: 1, Name: ("col1")}
	col2 := &testcat.Column{Ordinal: 2, Name: ("col2")}
	col3 := &testcat.Column{Ordinal: 3, Name: ("col3")}

	table := &testcat.Table{
		TabName: tree.MakeTableName("", "test_table"),
		TabID:   123,
		Columns: []*testcat.Column{col1, col2, col3},
	}

	// Test finding existing column
	ordinal := cat.FindTableColumnByName(table, ("col2"))
	if ordinal != 1 {
		t.Errorf("Expected ordinal to be 1, got %d", ordinal)
	}

	// Test finding non-existing column
	ordinal = cat.FindTableColumnByName(table, ("nonexistent"))
	if ordinal != -1 {
		t.Errorf("Expected ordinal to be -1 for non-existent column, got %d", ordinal)
	}
}

func TestFormatTable(t *testing.T) {
	col1 := &testcat.Column{
		Ordinal:  1,
		Name:     "id",
		Type:     types.Int,
		Nullable: false,
	}
	col2 := &testcat.Column{
		Ordinal:  2,
		Name:     ("name"),
		Type:     types.String,
		Nullable: true,
	}

	indexCol1 := cat.IndexColumn{Column: col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: col2, Descending: true}

	index := &testcat.Index{
		IdxName:     "primaryIdx",
		IdxOrdinal:  101,
		Columns:     []cat.IndexColumn{indexCol1, indexCol2},
		KeyCount:    1,
		LaxKeyCount: 1,
		Inverted:    false,
		IdxZone: &zonepb.ZoneConfig{
			NumReplicas:   proto.Int32(3),
			Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
			RangeMinBytes: proto.Int64(0),
			RangeMaxBytes: proto.Int64(64000),
			LeasePreferences: []zonepb.LeasePreference{
				{
					Constraints: []zonepb.Constraint{{Value: "a", Type: zonepb.Constraint_REQUIRED}},
				},
				{
					Constraints: []zonepb.Constraint{{Value: "b", Type: zonepb.Constraint_PROHIBITED}},
				},
			},
		},
	}

	family := &testcat.Family{
		FamName: ("primary"),
		Ordinal: 201,
		Columns: []cat.FamilyColumn{{Column: col1, Ordinal: 1}, {Column: col2, Ordinal: 2}},
	}

	check := &cat.CheckConstraint{
		Constraint: "id > 0",
	}

	table := &testcat.Table{
		TabName:     tree.MakeTableName("", "users"),
		TabID:       123,
		Columns:     []*testcat.Column{col1, col2},
		Indexes:     []*testcat.Index{index},
		Families:    []*testcat.Family{family},
		Checks:      []cat.CheckConstraint{*check},
		OutboundFKs: []testcat.ForeignKeyConstraint{},
	}

	tp := treeprinter.New()
	cat.FormatTable(nil, table, tp)
	output := tp.String()

	// Check that the output contains expected elements
	if len(output) == 0 {
		t.Error("Expected FormatTable to produce output")
	}
}

// func TestFormatCols(t *testing.T) {
// 	col1 := &testcat.Column{Ordinal: 1, Name: ("col1")}
// 	col2 := &testcat.Column{Ordinal: 2, Name: ("col2")}

// 	table := &testcat.TableForUtils{
// 		Name:    ("test_table"),
// 		Ordinal: 123,
// 		columns: []cat.Column{col1, col2},
// 	}

// 	// Function to return column ordinals
// 	colOrdinalFunc := func(tab cat.Table, i int) int {
// 		return i // Simple mapping: i-th column ordinal is i
// 	}

// 	result := cat.FormatCols(table, 2, colOrdinalFunc)

// 	// Check that the result contains both column names
// 	if result != "(col1, col2)" {
// 		t.Errorf("Expected '(col1, col2)', got '%s'", result)
// 	}
// }

// func TestFormatColumn(t *testing.T) {
// 	col := &testcat.Column{
// 		Ordinal:        1,
// 		Name:           ("test_col"),
// 		dataType:       types.Int,
// 		nullable:       false,
// 		hasDefault:     true,
// 		defaultExprStr: "10",
// 		hidden:         true,
// 		isComputed:     false,
// 	}

// 	var buf bytes.Buffer
// 	cat.FormatColumn(col, false, &buf)
// 	result := buf.String()

// 	// Check that the result contains expected elements
// 	if result == "" {
// 		t.Error("Expected FormatColumn to produce output")
// 	}
// }

// func TestFormatFamily(t *testing.T) {
// 	col1 := &testcat.Column{Ordinal: 1, Name: ("col1")}
// 	col2 := &testcat.Column{Ordinal: 2, Name: ("col2")}

// 	family := &testcat.Family{
// 		Name:    ("test_family"),
// 		Ordinal: 201,
// 		columns: []cat.Column{col1, col2},
// 	}

// 	var buf bytes.Buffer
// 	cat.FormatFamily(family, &buf)
// 	result := buf.String()

// 	// Check that the result contains expected elements
// 	if result == "" {
// 		t.Error("Expected FormatFamily to produce output")
// 	}
// }
