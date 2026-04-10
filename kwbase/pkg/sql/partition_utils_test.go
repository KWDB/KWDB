// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestIndexCoveringsForPartitioning tests the indexCoveringsForPartitioning function
func TestIndexCoveringsForPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a simple table descriptor
	tableDesc := &sqlbase.TableDescriptor{
		ID: 1,
		Columns: []sqlbase.ColumnDescriptor{
			{
				ID:   1,
				Name: "id",
				Type: *types.Int,
			},
			{
				ID:   2,
				Name: "name",
				Type: *types.String,
			},
		},
	}

	// Create an index descriptor
	idxDesc := &sqlbase.IndexDescriptor{
		ID:          1,
		Name:        "primary",
		ColumnNames: []string{"id", "name"},
		ColumnIDs: []sqlbase.ColumnID{
			sqlbase.ColumnID(1),
			sqlbase.ColumnID(2),
		},
	}

	a := &sqlbase.DatumAlloc{}

	// Test case 1: Empty partitioning descriptor
	t.Run("EmptyPartitioning", func(t *testing.T) {
		partDesc := &sqlbase.PartitioningDescriptor{
			NumColumns: 0,
		}
		relevantPartitions := map[string]int32{}
		var prefixDatums []tree.Datum

		coverings, err := indexCoveringsForPartitioning(a, tableDesc, idxDesc, partDesc, relevantPartitions, prefixDatums)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(coverings) != 0 {
			t.Fatalf("expected empty coverings, got: %d", len(coverings))
		}
	})

	// Test case 2: List partitioning
	t.Run("ListPartitioning", func(t *testing.T) {
		// Create a list partitioning descriptor
		// Encode values using EncodeTableValue
		var value1, value2, value3 []byte
		var scratch []byte
		value1, err := sqlbase.EncodeTableValue(value1, sqlbase.ColumnID(1), tree.NewDInt(1), scratch)
		if err != nil {
			t.Fatalf("failed to encode value 1: %v", err)
		}
		value2, err = sqlbase.EncodeTableValue(value2, sqlbase.ColumnID(1), tree.NewDInt(2), scratch)
		if err != nil {
			t.Fatalf("failed to encode value 2: %v", err)
		}
		value3, err = sqlbase.EncodeTableValue(value3, sqlbase.ColumnID(1), tree.NewDInt(3), scratch)
		if err != nil {
			t.Fatalf("failed to encode value 3: %v", err)
		}

		partDesc := &sqlbase.PartitioningDescriptor{
			NumColumns: 1,
			List: []sqlbase.PartitioningDescriptor_List{
				{
					Name:   "p1",
					Values: [][]byte{value1, value2},
				},
				{
					Name:   "p2",
					Values: [][]byte{value3},
				},
			},
		}

		relevantPartitions := map[string]int32{
			"p1": 0,
			"p2": 1,
		}
		var prefixDatums []tree.Datum

		_, err = indexCoveringsForPartitioning(a, tableDesc, idxDesc, partDesc, relevantPartitions, prefixDatums)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	// Test case 3: Range partitioning
	t.Run("RangePartitioning", func(t *testing.T) {
		// Create a range partitioning descriptor
		// Encode values using EncodeTableValue
		var from1, to1, from2, to2 []byte
		var scratch []byte
		from1, err := sqlbase.EncodeTableValue(from1, sqlbase.ColumnID(1), tree.NewDInt(1), scratch)
		if err != nil {
			t.Fatalf("failed to encode from1: %v", err)
		}
		to1, err = sqlbase.EncodeTableValue(to1, sqlbase.ColumnID(1), tree.NewDInt(10), scratch)
		if err != nil {
			t.Fatalf("failed to encode to1: %v", err)
		}
		from2, err = sqlbase.EncodeTableValue(from2, sqlbase.ColumnID(1), tree.NewDInt(10), scratch)
		if err != nil {
			t.Fatalf("failed to encode from2: %v", err)
		}
		to2, err = sqlbase.EncodeTableValue(to2, sqlbase.ColumnID(1), tree.NewDInt(20), scratch)
		if err != nil {
			t.Fatalf("failed to encode to2: %v", err)
		}

		partDesc := &sqlbase.PartitioningDescriptor{
			NumColumns: 1,
			Range: []sqlbase.PartitioningDescriptor_Range{
				{
					Name:          "p1",
					FromInclusive: from1,
					ToExclusive:   to1,
				},
				{
					Name:          "p2",
					FromInclusive: from2,
					ToExclusive:   to2,
				},
			},
		}

		relevantPartitions := map[string]int32{
			"p1": 0,
			"p2": 1,
		}
		var prefixDatums []tree.Datum

		coverings, err := indexCoveringsForPartitioning(a, tableDesc, idxDesc, partDesc, relevantPartitions, prefixDatums)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(coverings) != 2 {
			t.Fatalf("expected 2 coverings, got: %d", len(coverings))
		}
		// Verify that each covering has one range
		for i, covering := range coverings {
			if len(covering) != 1 {
				t.Fatalf("expected 1 range in covering %d, got: %d", i, len(covering))
			}
		}
	})

	// Test case 4: Partitioning with subpartitions
	t.Run("PartitioningWithSubpartitions", func(t *testing.T) {
		// Create a partitioning descriptor with subpartitions
		// Encode values using EncodeTableValue
		var value1, valueA []byte
		var scratch []byte
		value1, err := sqlbase.EncodeTableValue(value1, sqlbase.ColumnID(1), tree.NewDInt(1), scratch)
		if err != nil {
			t.Fatalf("failed to encode value1: %v", err)
		}
		valueA, err = sqlbase.EncodeTableValue(valueA, sqlbase.ColumnID(2), tree.NewDString("a"), scratch)
		if err != nil {
			t.Fatalf("failed to encode valueA: %v", err)
		}

		partDesc := &sqlbase.PartitioningDescriptor{
			NumColumns: 1,
			List: []sqlbase.PartitioningDescriptor_List{
				{
					Name:   "p1",
					Values: [][]byte{value1},
					Subpartitioning: sqlbase.PartitioningDescriptor{
						NumColumns: 1,
						List: []sqlbase.PartitioningDescriptor_List{
							{
								Name:   "p1_1",
								Values: [][]byte{valueA},
							},
						},
					},
				},
			},
		}

		relevantPartitions := map[string]int32{
			"p1":   0,
			"p1_1": 1,
		}
		var prefixDatums []tree.Datum

		coverings, err := indexCoveringsForPartitioning(a, tableDesc, idxDesc, partDesc, relevantPartitions, prefixDatums)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(coverings) != 2 {
			t.Fatalf("expected 2 coverings, got: %d", len(coverings))
		}
		// Subpartitions should come first (higher precedence)
		if len(coverings[0]) != 1 {
			t.Fatalf("expected 1 range in subpartition covering, got: %d", len(coverings[0]))
		}
		if len(coverings[1]) != 1 {
			t.Fatalf("expected 1 range in parent partition covering, got: %d", len(coverings[1]))
		}
	})

	// Test case 5: Non-relevant partitions
	t.Run("NonRelevantPartitions", func(t *testing.T) {
		// Create a list partitioning descriptor
		// Encode values using EncodeTableValue
		var value1, value2 []byte
		var scratch []byte
		value1, err := sqlbase.EncodeTableValue(value1, sqlbase.ColumnID(1), tree.NewDInt(1), scratch)
		if err != nil {
			t.Fatalf("failed to encode value1: %v", err)
		}
		value2, err = sqlbase.EncodeTableValue(value2, sqlbase.ColumnID(1), tree.NewDInt(2), scratch)
		if err != nil {
			t.Fatalf("failed to encode value2: %v", err)
		}

		partDesc := &sqlbase.PartitioningDescriptor{
			NumColumns: 1,
			List: []sqlbase.PartitioningDescriptor_List{
				{
					Name:   "p1",
					Values: [][]byte{value1},
				},
				{
					Name:   "p2", // Not in relevantPartitions
					Values: [][]byte{value2},
				},
			},
		}

		relevantPartitions := map[string]int32{
			"p1": 0,
		}
		var prefixDatums []tree.Datum

		coverings, err := indexCoveringsForPartitioning(a, tableDesc, idxDesc, partDesc, relevantPartitions, prefixDatums)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(coverings) != 1 {
			t.Fatalf("expected 1 covering, got: %d", len(coverings))
		}
		if len(coverings[0]) != 1 {
			t.Fatalf("expected 1 range in covering, got: %d", len(coverings[0]))
		}
	})
}
