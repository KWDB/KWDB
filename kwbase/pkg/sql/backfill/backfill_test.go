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

package backfill

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestColumnMutationFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		mutation sqlbase.DescriptorMutation
		expected bool
	}{
		{
			name: "add column",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Column{
					Column: &sqlbase.ColumnDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_ADD,
			},
			expected: true,
		},
		{
			name: "drop column",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Column{
					Column: &sqlbase.ColumnDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_DROP,
			},
			expected: true,
		},
		{
			name: "add index",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Index{
					Index: &sqlbase.IndexDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_ADD,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := ColumnMutationFilter(tc.mutation)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestIndexMutationFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		mutation sqlbase.DescriptorMutation
		expected bool
	}{
		{
			name: "add index",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Index{
					Index: &sqlbase.IndexDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_ADD,
			},
			expected: true,
		},
		{
			name: "drop index",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Index{
					Index: &sqlbase.IndexDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_DROP,
			},
			expected: false,
		},
		{
			name: "add column",
			mutation: sqlbase.DescriptorMutation{
				Descriptor_: &sqlbase.DescriptorMutation_Column{
					Column: &sqlbase.ColumnDescriptor{},
				},
				Direction: sqlbase.DescriptorMutation_ADD,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := IndexMutationFilter(tc.mutation)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestContainsInvertedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		added    []sqlbase.IndexDescriptor
		expected bool
	}{
		{
			name:     "empty indexes",
			added:    []sqlbase.IndexDescriptor{},
			expected: false,
		},
		{
			name: "forward index only",
			added: []sqlbase.IndexDescriptor{
				{Type: sqlbase.IndexDescriptor_FORWARD},
			},
			expected: false,
		},
		{
			name: "inverted index",
			added: []sqlbase.IndexDescriptor{
				{Type: sqlbase.IndexDescriptor_INVERTED},
			},
			expected: true,
		},
		{
			name: "mixed indexes with inverted",
			added: []sqlbase.IndexDescriptor{
				{Type: sqlbase.IndexDescriptor_FORWARD},
				{Type: sqlbase.IndexDescriptor_INVERTED},
			},
			expected: true,
		},
		{
			name: "mixed indexes without inverted",
			added: []sqlbase.IndexDescriptor{
				{Type: sqlbase.IndexDescriptor_FORWARD},
				{Type: sqlbase.IndexDescriptor_FORWARD},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ib := &IndexBackfiller{added: tc.added}
			actual := ib.ContainsInvertedIndex()
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestIndexBackfillerInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int2},
				{ID: 2, Name: "col2", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:   2,
							Name: "idx1",
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 1)
	require.Equal(t, sqlbase.IndexID(2), ib.added[0].ID)
}

func TestIndexBackfillerInitWithColumnMutationOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 0)
}

func TestIndexBackfillerInitWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 0)
}

func TestColumnBackfillerInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.Int, Nullable: true},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1, 2}, ColumnNames: []string{"col1", "col2"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					MutationID: 1,
				},
			},
		},
	}

	evalCtx := &tree.EvalContext{}
	cb := &ColumnBackfiller{}
	err := cb.Init(evalCtx, desc)
	require.NoError(t, err)
	require.Len(t, cb.added, 1)
}

func TestColumnBackfillerInitNoMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1}, ColumnNames: []string{"col1"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
		},
	}

	evalCtx := &tree.EvalContext{}
	cb := &ColumnBackfiller{}
	err := cb.Init(evalCtx, desc)
	require.NoError(t, err)
	require.Len(t, cb.added, 0)
}

func TestColumnBackfillerInitWithDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.Int},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1, 2}, ColumnNames: []string{"col1", "col2"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_DROP,
					MutationID: 1,
				},
			},
		},
	}

	evalCtx := &tree.EvalContext{}
	cb := &ColumnBackfiller{}
	err := cb.Init(evalCtx, desc)
	require.NoError(t, err)
	require.Len(t, cb.added, 0)
	require.Len(t, cb.updateExprs, 1)
}

func makeTestTableDescriptorWithIndex() *sqlbase.ImmutableTableDescriptor {
	return &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.Int},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1, 2}, ColumnNames: []string{"col1", "col2"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:          1,
				Name:        "primary",
				ColumnIDs:   []sqlbase.ColumnID{1},
				ColumnNames: []string{"col1"},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "idx1",
							ColumnIDs:   []sqlbase.ColumnID{2},
							ColumnNames: []string{"col2"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}
}

func TestBuildIndexEntriesChunkRequiresInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	desc := makeTestTableDescriptorWithIndex()
	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.NotNil(t, ib.types)
	require.Len(t, ib.added, 1)

	// Since we provide a nil txn, StartScan will panic.
	require.Panics(t, func() {
		_, _, _ = ib.BuildIndexEntriesChunk(ctx, nil, desc, roachpb.Span{}, 10, false)
	})
}

func TestRunColumnBackfillChunkPanicsOnNilTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tableDesc := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "col1", Type: *types.Int},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:        1,
			ColumnIDs: []sqlbase.ColumnID{1},
		},
		Mutations: []sqlbase.DescriptorMutation{
			{
				Descriptor_: &sqlbase.DescriptorMutation_Column{
					Column: &sqlbase.ColumnDescriptor{
						ID:   2,
						Name: "col2",
						Type: *types.Int,
					},
				},
				Direction:  sqlbase.DescriptorMutation_ADD,
				State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
				MutationID: 1,
			},
		},
	})

	otherTables := []*sqlbase.ImmutableTableDescriptor{}

	cb := &ColumnBackfiller{
		updateCols: []sqlbase.ColumnDescriptor{
			{ID: 2, Name: "col2", Type: *types.Int},
		},
	}
	err := cb.Init(&tree.EvalContext{}, tableDesc)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = cb.RunColumnBackfillChunk(
			ctx,
			nil, // txn = nil will cause panic in StartScan
			tableDesc,
			otherTables,
			roachpb.Span{},
			100,
			false,
			false,
		)
	})
}

func TestRunIndexBackfillChunkPanicsWithNilTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	desc := makeTestTableDescriptorWithIndex()
	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = ib.RunIndexBackfillChunk(ctx, nil, desc, roachpb.Span{}, 10, false, false)
	})
}

func TestIndexBackfillerInitWithInvertedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "idx1",
							ColumnIDs:   []sqlbase.ColumnID{1},
							ColumnNames: []string{"col1"},
							Type:        sqlbase.IndexDescriptor_INVERTED,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 1)
	require.True(t, ib.added[0].Type == sqlbase.IndexDescriptor_INVERTED)
}

func TestColumnBackfillerInitWithComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.Int},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1, 2}, ColumnNames: []string{"col1", "col2"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					MutationID: 1,
				},
			},
		},
	}

	evalCtx := &tree.EvalContext{}
	cb := &ColumnBackfiller{}
	err := cb.Init(evalCtx, desc)
	require.NoError(t, err)
	require.Len(t, cb.added, 1)
	require.NotNil(t, cb.updateExprs[0])
}

func TestIndexBackfillerWithPrimaryIndexEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:          1,
				Name:        "primary",
				ColumnIDs:   []sqlbase.ColumnID{1},
				ColumnNames: []string{"col1"},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:   2,
							Name: "idx1",
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 1)
}

func TestIndexBackfillerTypesInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.String},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "idx1",
							ColumnIDs:   []sqlbase.ColumnID{2},
							ColumnNames: []string{"col2"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.types, 2)
	firstType := ib.types[0]
	secondType := ib.types[1]
	require.Equal(t, "int", firstType.Name())
	require.Equal(t, "string", secondType.Name())
}

func TestColumnBackfillerWithMultipleMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1}, ColumnNames: []string{"col1"}},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   2,
							Name: "col2",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					MutationID: 1,
				},
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   3,
							Name: "col3",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_DROP,
					MutationID: 1,
				},
			},
		},
	}

	evalCtx := &tree.EvalContext{}
	cb := &ColumnBackfiller{}
	err := cb.Init(evalCtx, desc)
	require.NoError(t, err)
	require.Len(t, cb.added, 1)
	require.Len(t, cb.updateExprs, 2)
}

func TestContainsInvertedIndexTrue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "inverted_idx",
							ColumnIDs:   []sqlbase.ColumnID{1},
							ColumnNames: []string{"col1"},
							Type:        sqlbase.IndexDescriptor_INVERTED,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.True(t, ib.ContainsInvertedIndex())
}

func TestContainsInvertedIndexFalse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "regular_idx",
							ColumnIDs:   []sqlbase.ColumnID{1},
							ColumnNames: []string{"col1"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.False(t, ib.ContainsInvertedIndex())
}

func TestIndexBackfillerInitWithPrimaryIndexColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
				{ID: 2, Name: "col2", Type: *types.String},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Column{
						Column: &sqlbase.ColumnDescriptor{
							ID:   3,
							Name: "col3",
							Type: *types.Int,
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							Name:        "idx1",
							ColumnIDs:   []sqlbase.ColumnID{1, 2},
							ColumnNames: []string{"col1", "col2"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 1)
	require.Len(t, ib.types, 3)
}

func TestIndexBackfillerInitWithMultipleMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          2,
							ColumnIDs:   []sqlbase.ColumnID{1},
							ColumnNames: []string{"col1"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 1,
				},
				{
					Descriptor_: &sqlbase.DescriptorMutation_Index{
						Index: &sqlbase.IndexDescriptor{
							ID:          3,
							ColumnIDs:   []sqlbase.ColumnID{1},
							ColumnNames: []string{"col1"},
						},
					},
					Direction:  sqlbase.DescriptorMutation_ADD,
					State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
					MutationID: 2,
				},
			},
		},
	}

	ib := &IndexBackfiller{}
	err := ib.Init(desc)
	require.NoError(t, err)
	require.Len(t, ib.added, 1)
}

func TestIndexBackfillerInitEmptyMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "col1", Type: *types.Int},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				ColumnIDs: []sqlbase.ColumnID{1},
			},
			Mutations: []sqlbase.DescriptorMutation{},
		},
	}

	ib := &IndexBackfiller{}
	require.Panics(t, func() {
		_ = ib.Init(desc)
	})
}

func TestRunColumnBackfillChunkFkError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a table with an outbound foreign key.
	tableDesc := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
		OutboundFKs: []sqlbase.ForeignKeyConstraint{
			{
				Name:                "fk1",
				ReferencedTableID:   2,
				OriginColumnIDs:     []sqlbase.ColumnID{1},
				ReferencedColumnIDs: []sqlbase.ColumnID{1},
			},
		},
	})

	// Missing referenced table in otherTables.
	otherTables := []*sqlbase.ImmutableTableDescriptor{}

	cb := &ColumnBackfiller{}

	_, err := cb.RunColumnBackfillChunk(
		ctx,
		nil, // txn not needed for this early error
		tableDesc,
		otherTables,
		roachpb.Span{},
		100,
		false,
		false,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "not sent by coordinator")
}

func TestRunColumnBackfillChunkMakeUpdaterError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a valid table descriptor.
	tableDesc := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "col1", Type: *types.Int},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:        1,
			ColumnIDs: []sqlbase.ColumnID{1},
		},
	})

	otherTables := []*sqlbase.ImmutableTableDescriptor{}

	// Create ColumnBackfiller with an invalid updateCol.
	cb := &ColumnBackfiller{
		updateCols: []sqlbase.ColumnDescriptor{
			{ID: 2, Name: "invalid_col", Type: *types.Int}, // ID 2 does not exist in tableDesc
		},
	}

	_, err := cb.RunColumnBackfillChunk(
		ctx,
		nil, // txn
		tableDesc,
		otherTables,
		roachpb.Span{},
		100,
		false,
		false,
	)

	require.Error(t, err)
}

func TestRunColumnBackfillChunkPanicsOnNonColumnOnlyUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tableDesc := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "col1", Type: *types.Int},
			{ID: 2, Name: "col2", Type: *types.Int},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:        1,
			ColumnIDs: []sqlbase.ColumnID{1},
		},
		Indexes: []sqlbase.IndexDescriptor{
			{
				ID:        2,
				ColumnIDs: []sqlbase.ColumnID{2}, // index on col2
			},
		},
	})

	otherTables := []*sqlbase.ImmutableTableDescriptor{}

	// If we update col2, which is part of a secondary index,
	// MakeUpdater will configure it to update indexes as well,
	// so IsColumnOnlyUpdate() will return false and it should panic.
	cb := &ColumnBackfiller{
		updateCols: []sqlbase.ColumnDescriptor{
			{ID: 2, Name: "col2", Type: *types.Int},
		},
	}
	err := cb.Init(&tree.EvalContext{}, tableDesc)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = cb.RunColumnBackfillChunk(
			ctx,
			nil, // txn
			tableDesc,
			otherTables,
			roachpb.Span{},
			100,
			false,
			false,
		)
	})
}

func TestConvertBackfillErrorPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// 1. Without mutations: MakeFirstMutationPublic panics (index out of range)
	tableDescNoMut := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
	})
	require.Panics(t, func() {
		_ = ConvertBackfillError(ctx, tableDescNoMut, nil)
	})

	// 2. With mutations: row.ConvertBatchError panics on nil batch
	tableDescWithMut := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		ID: 1,
		Mutations: []sqlbase.DescriptorMutation{
			{
				Descriptor_: &sqlbase.DescriptorMutation_Column{
					Column: &sqlbase.ColumnDescriptor{
						ID:   2,
						Name: "col2",
						Type: *types.Int,
					},
				},
				Direction:  sqlbase.DescriptorMutation_ADD,
				State:      sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
				MutationID: 1,
			},
		},
	})
	require.Panics(t, func() {
		_ = ConvertBackfillError(ctx, tableDescWithMut, nil)
	})
}
