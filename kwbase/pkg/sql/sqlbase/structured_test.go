// Copyright 2015 The Cockroach Authors.
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

package sqlbase

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// Makes an IndexDescriptor with all columns being ascending.
func makeIndexDescriptor(name string, columnNames []string) IndexDescriptor {
	dirs := make([]IndexDescriptor_Direction, 0, len(columnNames))
	for range columnNames {
		dirs = append(dirs, IndexDescriptor_ASC)
	}
	idx := IndexDescriptor{
		ID:               IndexID(0),
		Name:             name,
		ColumnNames:      columnNames,
		ColumnDirections: dirs,
	}
	return idx
}

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "foo",
		Columns: []ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
		PrimaryIndex: makeIndexDescriptor("c", []string{"a", "b"}),
		Indexes: []IndexDescriptor{
			makeIndexDescriptor("d", []string{"b", "a"}),
			makeIndexDescriptor("e", []string{"b"}),
			func() IndexDescriptor {
				idx := makeIndexDescriptor("f", []string{"c"})
				idx.EncodingType = PrimaryIndexEncoding
				return idx
			}(),
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	})
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	expected := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Version:  1,
		Name:     "foo",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "a"},
			{ID: 2, Name: "b"},
			{ID: 3, Name: "c"},
		},
		Families: []ColumnFamilyDescriptor{
			{
				ID: 0, Name: "primary",
				ColumnNames:     []string{"a", "b", "c"},
				ColumnIDs:       []ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		PrimaryIndex: IndexDescriptor{
			ID: 1, Name: "c", ColumnIDs: []ColumnID{1, 2},
			ColumnNames: []string{"a", "b"},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC,
				IndexDescriptor_ASC}},
		Indexes: []IndexDescriptor{
			{ID: 2, Name: "d", ColumnIDs: []ColumnID{2, 1}, ColumnNames: []string{"b", "a"},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC,
					IndexDescriptor_ASC}},
			{ID: 3, Name: "e", ColumnIDs: []ColumnID{2}, ColumnNames: []string{"b"},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				ExtraColumnIDs:   []ColumnID{1}},
			{ID: 4, Name: "f", ColumnIDs: []ColumnID{3}, ColumnNames: []string{"c"},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				EncodingType:     PrimaryIndexEncoding},
		},
		Privileges:     NewDefaultPrivilegeDescriptor(),
		NextColumnID:   4,
		NextFamilyID:   1,
		NextIndexID:    5,
		NextMutationID: 1,
		NextTriggerID:  1,
		FormatVersion:  FamilyFormatVersion,
	})
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}

	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}
}

func TestValidateTableDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		err  string
		desc TableDescriptor
	}{
		{`empty table name`,
			TableDescriptor{}},
		{`invalid table ID 0`,
			TableDescriptor{ID: 0, Name: "foo"}},
		{`invalid parent ID 0`,
			TableDescriptor{ID: 2, Name: "foo"}},
		{`table "foo" is encoded using using version 0, but this client only supports version 2 and 3`,
			TableDescriptor{ID: 2, ParentID: 1, Name: "foo"}},
		{`table must contain at least 1 column`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
			}},
		{`empty column name`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`invalid column ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`duplicate column name: "bar"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`at least 1 column family must be specified`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`the 0th family must have ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{`duplicate family name: "baz"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`family "qux" duplicate ID of family "baz": 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 0, Name: "qux"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`duplicate family name: "baz"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 3, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`mismatched column ID size (1) and name size (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" contains unknown column "2"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" column 1 should have name "bar", but found name "qux"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is not in any column family`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is in both family 0 and 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "qux", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID:               0,
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`invalid index ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 0, Name: "bar",
					ColumnIDs:        []ColumnID{0},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`index "bar" must contain at least 1 column`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar", "blah"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "bar", ColumnIDs: []ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{2},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames:      []string{"blah"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and directions (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "foo_kwdb_internal_bar_shard_5_bar_idx" refers to non-existent shard column "does not exist"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "kwdb_internal_bar_shard_5"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []ColumnID{1, 2},
						ColumnNames: []string{"bar", "kwdb_internal_bar_shard_5"},
					},
				},
				PrimaryIndex: IndexDescriptor{
					ID: 1, Name: "primary",
					Unique:           true,
					ColumnIDs:        []ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					StoreColumnNames: []string{"kwdb_internal_bar_shard_5"},
					StoreColumnIDs:   []ColumnID{2},
				},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "foo_kwdb_internal_bar_shard_5_bar_idx",
						ColumnIDs:        []ColumnID{2, 1},
						ColumnNames:      []string{"kwdb_internal_bar_shard_5", "bar"},
						ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
						Sharded: ShardedDescriptor{
							IsSharded:    true,
							Name:         "does not exist",
							ShardBuckets: 5,
						},
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		if err := d.desc.ValidateTable(); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, d.err, d.desc)
		} else if d.err != err.Error() && "internal error: "+d.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, d.err, err)
		}
	}
}

func TestValidateCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		err        string
		desc       TableDescriptor
		otherDescs []TableDescriptor
	}{
		// Foreign keys
		{
			err: `invalid foreign key: missing table=73: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				OutboundFKs: []ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   73,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
			otherDescs: nil,
		},
		{
			err: `missing fk back reference "fk" to "foo" from "baz"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				OutboundFKs: []ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `invalid foreign key backreference: missing table=52: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				InboundFKs: []ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
		},
		{
			err: `missing fk forward reference "fk" to "foo" from "baz"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:   1,
					Name: "bar",
				},
				InboundFKs: []ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},

		// Interleaves
		{
			err: `invalid interleave: missing table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID: 1,
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: nil,
		},
		{
			err: `invalid interleave: missing table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID: 1,
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `missing interleave back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:   1,
					Name: "bar",
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid interleave backreference table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `broken interleave backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					Name:          "bar",
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
	}

	{
		var v roachpb.Value
		desc := &Descriptor{Union: &Descriptor_Database{}}
		if err := v.SetProto(desc); err != nil {
			t.Fatal(err)
		}
		if err := kvDB.Put(ctx, MakeDescMetadataKey(0), &v); err != nil {
			t.Fatal(err)
		}
	}

	for i, test := range tests {
		for _, otherDesc := range test.otherDescs {
			otherDesc.Privileges = NewDefaultPrivilegeDescriptor()
			var v roachpb.Value
			desc := &Descriptor{Union: &Descriptor_Table{Table: &otherDesc}}
			if err := v.SetProto(desc); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.Put(ctx, MakeDescMetadataKey(otherDesc.ID), &v); err != nil {
				t.Fatal(err)
			}
		}
		txn := kv.NewTxn(ctx, kvDB, s.NodeID())
		if err := test.desc.validateCrossReferences(ctx, txn); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, test.err, test.desc)
		} else if test.err != err.Error() && "internal error: "+test.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, test.err, err.Error())
		}
		for _, otherDesc := range test.otherDescs {
			if err := kvDB.Del(ctx, MakeDescMetadataKey(otherDesc.ID)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestValidatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc TableDescriptor
	}{
		{"at least one of LIST or RANGE partitioning must be used",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{}},
						Range:      []PartitioningDescriptor_Range{{}},
					},
				},
			},
		},
		{"PARTITION name must be non-empty",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{}},
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: empty array",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{{
							Name: "p1", Values: [][]byte{{}},
						}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: int64 varint decoding failed: 0",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03}}},
						},
					},
				},
			},
		},
		{"PARTITION p1: superfluous data in encoded value",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02, 0x00}}},
						},
					},
				},
			},
		},
		{"partitions p1 and p2 overlap",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1, 1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						Range: []PartitioningDescriptor_Range{
							{Name: "p1", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
							{Name: "p2", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
						},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{Name: "p1", Values: [][]byte{{0x03, 0x04}}},
						},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: PartitioningDescriptor{
								NumColumns: 1,
								List:       []PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
							},
						}},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: *types.Int}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1, 1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: PartitioningDescriptor{
									NumColumns: 1,
									List: []PartitioningDescriptor_List{
										{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for i, test := range tests {
		err := test.desc.validatePartitioning()
		if !testutils.IsError(err, test.err) {
			t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
		}
	}
}

func TestColumnTypeSQLString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		colType     *types.T
		expectedSQL string
	}{
		{types.MakeBit(2), "BIT(2)"},
		{types.MakeVarBit(2), "VARBIT(2)"},
		{types.Int, "INT8"},
		{types.Float, "FLOAT8"},
		{types.Float4, "FLOAT4"},
		{types.Decimal, "DECIMAL"},
		{types.MakeDecimal(6, 0), "DECIMAL(6)"},
		{types.MakeDecimal(8, 7), "DECIMAL(8,7)"},
		{types.Date, "DATE"},
		{types.Timestamp, "TIMESTAMP"},
		{types.Interval, "INTERVAL"},
		{types.String, "STRING"},
		{types.MakeString(10), "STRING(10)"},
		{types.Bytes, "BYTES"},
	}
	for i, d := range testData {
		t.Run(d.colType.DebugString(), func(t *testing.T) {
			sql := d.colType.SQLString()
			if d.expectedSQL != sql {
				t.Errorf("%d: expected %s, but got %s", i, d.expectedSQL, sql)
			}
		})
	}
}

func TestFitColumnToFamily(t *testing.T) {
	intEncodedSize := 10 // 1 byte tag + 9 bytes max varint encoded size

	makeTestTableDescriptor := func(familyTypes [][]types.T) *MutableTableDescriptor {
		nextColumnID := ColumnID(8)
		var desc TableDescriptor
		for _, fTypes := range familyTypes {
			var family ColumnFamilyDescriptor
			for _, t := range fTypes {
				desc.Columns = append(desc.Columns, ColumnDescriptor{
					ID:   nextColumnID,
					Type: t,
				})
				family.ColumnIDs = append(family.ColumnIDs, nextColumnID)
				nextColumnID++
			}
			desc.Families = append(desc.Families, family)
		}
		return NewMutableCreatedTableDescriptor(desc)
	}

	emptyFamily := []types.T{}
	partiallyFullFamily := []types.T{
		*types.Int,
		*types.Bytes,
	}
	fullFamily := []types.T{
		*types.Bytes,
	}
	maxIntsInOneFamily := make([]types.T, FamilyHeuristicTargetBytes/intEncodedSize)
	for i := range maxIntsInOneFamily {
		maxIntsInOneFamily[i] = *types.Int
	}

	tests := []struct {
		newCol           types.T
		existingFamilies [][]types.T
		colFits          bool
		idx              int // not applicable if colFits is false
	}{
		// Bounded size column.
		{colFits: true, idx: 0, newCol: *types.Bool,
			existingFamilies: nil,
		},
		{colFits: true, idx: 0, newCol: *types.Bool,
			existingFamilies: [][]types.T{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: *types.Bool,
			existingFamilies: [][]types.T{partiallyFullFamily},
		},
		{colFits: true, idx: 0, newCol: *types.Bool,
			existingFamilies: [][]types.T{fullFamily},
		},
		{colFits: true, idx: 0, newCol: *types.Bool,
			existingFamilies: [][]types.T{fullFamily, emptyFamily},
		},

		// Unbounded size column.
		{colFits: true, idx: 0, newCol: *types.Decimal,
			existingFamilies: [][]types.T{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: *types.Decimal,
			existingFamilies: [][]types.T{partiallyFullFamily},
		},
	}
	for i, test := range tests {
		desc := makeTestTableDescriptor(test.existingFamilies)
		idx, colFits := fitColumnToFamily(desc, ColumnDescriptor{Type: test.newCol})
		if colFits != test.colFits {
			if colFits {
				t.Errorf("%d: expected no fit for the column but got one", i)
			} else {
				t.Errorf("%d: expected fit for the column but didn't get one", i)
			}
			continue
		}
		if colFits && idx != test.idx {
			t.Errorf("%d: got a fit in family offset %d but expected offset %d", i, idx, test.idx)
		}
	}
}

func TestMaybeUpgradeFormatVersion(t *testing.T) {
	tests := []struct {
		desc       TableDescriptor
		expUpgrade bool
		verify     func(int, TableDescriptor) // nil means no extra verification.
	}{
		{
			desc: TableDescriptor{
				FormatVersion: BaseFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
			},
			expUpgrade: true,
			verify: func(i int, desc TableDescriptor) {
				if len(desc.Families) == 0 {
					t.Errorf("%d: expected families to be set, but it was empty", i)
				}
			},
		},
		// Test that a version from the future is left alone.
		{
			desc: TableDescriptor{
				FormatVersion: InterleavedFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
			},
			expUpgrade: false,
			verify:     nil,
		},
	}
	for i, test := range tests {
		desc := test.desc
		upgraded := desc.maybeUpgradeFormatVersion()
		if upgraded != test.expUpgrade {
			t.Fatalf("%d: expected upgraded=%t, but got upgraded=%t", i, test.expUpgrade, upgraded)
		}
		if test.verify != nil {
			test.verify(i, desc)
		}
	}
}

func TestUnvalidateConstraints(t *testing.T) {
	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		Name:          "test",
		ParentID:      ID(1),
		Columns:       []ColumnDescriptor{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		FormatVersion: FamilyFormatVersion,
		Indexes:       []IndexDescriptor{makeIndexDescriptor("d", []string{"b", "a"})},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		OutboundFKs: []ForeignKeyConstraint{
			{
				Name:              "fk",
				ReferencedTableID: ID(1),
				Validity:          ConstraintValidity_Validated,
			},
		},
	})
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	lookup := func(_ ID) (*TableDescriptor, error) {
		return desc.TableDesc(), nil
	}

	before, err := desc.GetConstraintInfoWithLookup(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := before["fk"]; !ok || c.Unvalidated {
		t.Fatalf("expected to find a validated constraint fk before, found %v", c)
	}
	desc.InvalidateFKConstraints()

	after, err := desc.GetConstraintInfoWithLookup(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := after["fk"]; !ok || !c.Unvalidated {
		t.Fatalf("expected to find an unvalidated constraint fk before, found %v", c)
	}
}

func TestKeysPerRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(dan): This server is only used to turn a CREATE TABLE statement into
	// a TableDescriptor. It should be possible to move MakeTableDesc into
	// sqlbase. If/when that happens, use it here instead of this server.
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	if _, err := conn.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatalf("%+v", err)
	}

	tests := []struct {
		createTable string
		indexID     IndexID
		expected    int
	}{
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 1, 1},                                     // Primary index
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 2, 1},                                     // 'b' index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 1, 2},             // Primary index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 2, 1},             // 'b' index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (a) STORING (b))", 2, 2}, // 'a' index
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s - %d", test.createTable, test.indexID), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(conn)
			tableName := fmt.Sprintf("t%d", i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE d.%s %s`, tableName, test.createTable))

			var descBytes []byte
			// Grab the most recently created descriptor.
			row := sqlDB.QueryRow(t,
				`SELECT descriptor FROM system.descriptor ORDER BY id DESC LIMIT 1`)
			row.Scan(&descBytes)
			var desc Descriptor
			descBytes, err := hex.DecodeString(string(descBytes[2:]))
			if err != nil {
				t.Fatal(err)
			}
			if err := protoutil.Unmarshal(descBytes, &desc); err != nil {
				t.Fatalf("%+v", err)
			}

			// nolint:descriptormarshal
			keys, err := desc.GetTable().KeysPerRow(test.indexID)
			if err != nil {
				t.Fatal(err)
			}
			if test.expected != keys {
				t.Errorf("expected %d keys got %d", test.expected, keys)
			}
		})
	}
}

func TestColumnNeedsBackfill(t *testing.T) {
	// Define variable strings here such that we can pass their address below.
	null := "NULL"
	four := "4:::INT8"
	// Create Column Descriptors that reflect the definition of a column with a
	// default value of NULL that was set implicitly, one that was set explicitly,
	// and one that has an INT default value, respectively.
	implicitNull := &ColumnDescriptor{Name: "im", ID: 2, DefaultExpr: nil, Nullable: true, ComputeExpr: nil}
	explicitNull := &ColumnDescriptor{Name: "ex", ID: 3, DefaultExpr: &null, Nullable: true, ComputeExpr: nil}
	defaultNotNull := &ColumnDescriptor{Name: "four", ID: 4, DefaultExpr: &four, Nullable: true, ComputeExpr: nil}
	// Verify that a backfill doesn't occur according to the ColumnNeedsBackfill
	// function for the default NULL values, and that it does occur for an INT
	// default value.
	if ColumnNeedsBackfill(implicitNull) != false {
		t.Fatal("Expected implicit SET DEFAULT NULL to not require a backfill," +
			" ColumnNeedsBackfill states that it does.")
	}
	if ColumnNeedsBackfill(explicitNull) != false {
		t.Fatal("Expected explicit SET DEFAULT NULL to not require a backfill," +
			" ColumnNeedsBackfill states that it does.")
	}
	if ColumnNeedsBackfill(defaultNotNull) != true {
		t.Fatal("Expected explicit SET DEFAULT NULL to require a backfill," +
			" ColumnNeedsBackfill states that it does not.")
	}
}

func TestDefaultExprNil(t *testing.T) {
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	if _, err := conn.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatalf("%+v", err)
	}
	t.Run(fmt.Sprintf("%s - %d", "(a INT PRIMARY KEY)", 1), func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(conn)
		// Execute SQL commands with both implicit and explicit setting of the
		// default expression.
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO t (a) VALUES (1), (2)`)
		sqlDB.Exec(t, `ALTER TABLE t ADD COLUMN b INT NULL`)
		sqlDB.Exec(t, `INSERT INTO t (a) VALUES (3)`)
		sqlDB.Exec(t, `ALTER TABLE t ADD COLUMN c INT DEFAULT NULL`)

		var descBytes []byte
		// Grab the most recently created descriptor.
		row := sqlDB.QueryRow(t,
			`SELECT descriptor FROM system.descriptor ORDER BY id DESC LIMIT 1`)
		row.Scan(&descBytes)
		var desc Descriptor
		b, err := hex.DecodeString(string(descBytes[2:]))
		if err != nil {
			t.Fatal(err)
		}
		if err := protoutil.Unmarshal(b, &desc); err != nil {
			t.Fatalf("%+v", err)
		}
		// Test and verify that the default expressions of the column descriptors
		// are all nil.
		// nolint:descriptormarshal
		for _, col := range desc.GetTable().Columns {
			if col.DefaultExpr != nil {
				t.Errorf("expected Column Default Expression to be 'nil', got %s instead.", *col.DefaultExpr)
			}
		}
	})
}

func TestSQLString(t *testing.T) {
	colNames := []string{"derp", "foo"}
	indexName := "idx"
	tableName := tree.MakeTableName("DB", "t1")
	tableName.ExplicitCatalog = false
	tableName.ExplicitSchema = false
	index := IndexDescriptor{Name: indexName,
		ID:               0x0,
		Unique:           false,
		ColumnNames:      colNames,
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
	}
	expected := fmt.Sprintf("INDEX %s ON t1 (%s ASC, %s ASC)", indexName, colNames[0], colNames[1])
	if got := index.SQLString(&tableName); got != expected {
		t.Errorf("Expected '%s', but got '%s'", expected, got)
	}
	expected = fmt.Sprintf("INDEX %s (%s ASC, %s ASC)", indexName, colNames[0], colNames[1])
	if got := index.SQLString(&AnonymousTable); got != expected {
		t.Errorf("Expected '%s', but got '%s'", expected, got)
	}
}

func TestForeignKeyReferenceResetStringSize(t *testing.T) {
	m := &ForeignKeyReference{
		Table:           1,
		Index:           2,
		Name:            "test_ref",
		SharedPrefixLen: 3,
		OnDelete:        ForeignKeyReference_NO_ACTION,
		OnUpdate:        ForeignKeyReference_NO_ACTION,
		Match:           ForeignKeyReference_SIMPLE,
	}
	m.Reset()
	if m.Table != 0 {
		t.Errorf("Reset failed: expected Table 0, got %d", m.Table)
	}

	str := m.String()
	if len(str) == 0 {
		t.Error("String returned empty string")
	}

	size := m.Size()
	if size < 0 {
		t.Errorf("Size returned negative value: %d", size)
	}

	m2 := &ForeignKeyReference{
		Table:           1,
		Index:           2,
		Name:            "test_ref",
		SharedPrefixLen: 3,
		OnDelete:        ForeignKeyReference_NO_ACTION,
		OnUpdate:        ForeignKeyReference_NO_ACTION,
		Match:           ForeignKeyReference_SIMPLE,
	}
	// target := make([]byte, 0, 1000)
	// m2.XXX_Unmarshal(nil)
	// m2.XXX_Marshal(target, false)
	m2.XXX_Size()
	m2.XXX_DiscardUnknown()
}

func TestForeignKeyConstraintResetStringSize(t *testing.T) {
	m := &ForeignKeyConstraint{
		OriginTableID:       1,
		OriginColumnIDs:     []ColumnID{1, 2},
		ReferencedColumnIDs: []ColumnID{3, 4},
		ReferencedTableID:   3,
		Name:                "fk_test",
		OnDelete:            ForeignKeyReference_NO_ACTION,
		OnUpdate:            ForeignKeyReference_NO_ACTION,
		Match:               ForeignKeyReference_SIMPLE,
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed: expected Name empty, got %s", m.Name)
	}

	str := m.String()
	if len(str) == 0 {
		t.Error("String returned empty string")
	}

	size := m.Size()
	if size < 0 {
		t.Errorf("Size returned negative value: %d", size)
	}

	// m.XXX_DiscardUnknown()
}

func TestColumnDescriptorResetStringSize(t *testing.T) {
	m := &ColumnDescriptor{
		Name:     "test_col",
		ID:       1,
		Nullable: true,
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed: expected Name empty")
	}

	str := m.String()
	if len(str) == 0 {
		t.Error("String returned empty string")
	}

	size := m.Size()
	if size < 0 {
		t.Errorf("Size returned negative value: %d", size)
	}

	// // m.XXX_DiscardUnknown()
}

func TestIndexDescriptorResetStringSize(t *testing.T) {
	m := &IndexDescriptor{
		Name:             "test_idx",
		ID:               1,
		Unique:           false,
		ColumnNames:      []string{"col1"},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed")
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// // m.XXX_DiscardUnknown()
}

func TestConstraintToUpdateResetStringSize(t *testing.T) {
	m := &ConstraintToUpdate{
		ConstraintType: ConstraintToUpdate_NOT_NULL,
		Name:           "not_null_con",
		NotNullColumn:  1,
	}
	m.Reset()
	if m.ConstraintType == ConstraintToUpdate_NOT_NULL {
		t.Errorf("Reset failed")
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// // m.XXX_DiscardUnknown()
}

func TestDescriptorResetStringSize(t *testing.T) {
	m := &Descriptor{
		Union: &Descriptor_Table{Table: &TableDescriptor{ID: 1, Name: "test"}},
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	m.Reset()
	if m.GetTable() != nil {
		t.Errorf("Reset failed")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// // m.XXX_DiscardUnknown()
}

func TestTableDescriptorResetStringSize(t *testing.T) {
	m := &TableDescriptor{
		Name: "test_table",
		ID:   1,
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed")
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// // m.XXX_DiscardUnknown()
}

func TestDatabaseDescriptorResetStringSize(t *testing.T) {
	m := &DatabaseDescriptor{
		Name: "test_db",
		ID:   1,
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed")
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// m.XXX_DiscardUnknown()
}

func TestSchemaDescriptorResetStringSize(t *testing.T) {
	m := &SchemaDescriptor{
		Name: "test_schema",
		ID:   1,
	}
	m.Reset()
	if m.Name != "" {
		t.Errorf("Reset failed")
	}
	str := m.String()
	if len(str) == 0 {
		t.Error("String empty")
	}
	size := m.Size()
	if size < 0 {
		t.Errorf("Size negative")
	}
	// m.XXX_DiscardUnknown()
}

func TestConstraintValidityString(t *testing.T) {
	if ConstraintValidity_Validated.String() != "Validated" {
		t.Errorf("Expected Validated")
	}
}

func TestForeignKeyReferenceMatchString(t *testing.T) {
	if ForeignKeyReference_SIMPLE.String() != "MATCH SIMPLE" {
		t.Errorf("Expected MATCH SIMPLE")
	}
}

func TestIndexDescriptorDirectionString(t *testing.T) {
	if IndexDescriptor_ASC.String() != "ASC" {
		t.Errorf("Expected ASC")
	}
}

func TestPrimaryKeySwapResetStringSize(t *testing.T) {
	m := &PrimaryKeySwap{OldPrimaryIndexId: 1, NewPrimaryIndexId: 2}
	m.Reset()
	if m.OldPrimaryIndexId != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestInterleaveDescriptorResetStringSize(t *testing.T) {
	m := &InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{{TableID: 1, IndexID: 2}}}
	m.Reset()
	if len(m.Ancestors) != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestPartitioningDescriptorResetStringSize(t *testing.T) {
	m := &PartitioningDescriptor{NumColumns: 1}
	m.Reset()
	if m.NumColumns != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestShardedDescriptorResetStringSize(t *testing.T) {
	m := &ShardedDescriptor{IsSharded: true, ColumnNames: []string{"col1"}}
	m.Reset()
	if m.IsSharded {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestTriggerDescriptorResetStringSize(t *testing.T) {
	m := &TriggerDescriptor{Name: "trig"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestImportTableResetStringSize(t *testing.T) {
	m := &ImportTable{TableName: "tbl"}
	m.Reset()
	if m.TableName != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestDescriptorMutationResetStringSize(t *testing.T) {
	m := &DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: &ColumnDescriptor{Name: "c1"}}}
	m.Reset()
	if m.Descriptor_ != nil {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestColumnFamilyDescriptorResetStringSize(t *testing.T) {
	m := &ColumnFamilyDescriptor{Name: "fam", ID: 1}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestMaterializedViewRefreshResetStringSize(t *testing.T) {
	m := &MaterializedViewRefresh{AsOf: hlc.Timestamp{1111111111, 1111111}}
	m.Reset()
	if m.AsOf.WallTime != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestConstraintToUpdateConstraintTypeString(t *testing.T) {
	if ConstraintToUpdate_CHECK.String() != "CHECK" {
		t.Error("Expected CHECK")
	}
}

func TestConstraintToUpdateConstraintTypeFOREIGNKEYString(t *testing.T) {
	if ConstraintToUpdate_FOREIGN_KEY.String() != "FOREIGN_KEY" {
		t.Error("Expected FOREIGN_KEY")
	}
}

func TestFunctionDescriptorResetStringSize(t *testing.T) {
	m := &FunctionDescriptor{Name: "fn", ArgumentTypes: []uint32{1}, ReturnType: []uint32{2}, FunctionBody: "body"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestProcedureDescriptorResetStringSize(t *testing.T) {
	m := &ProcedureDescriptor{Name: "proc", Parameters: []ProcParam{{Name: "p1"}}, ProcBody: "body"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestProcParamResetStringSize(t *testing.T) {
	m := &ProcParam{Name: "param", Direction: ProcParam_InDirection}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestTSDBResetStringSize(t *testing.T) {
	m := &TSDB{Creator: "creator"}
	m.Reset()
	if m.Creator != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestTSTableResetStringSize(t *testing.T) {
	m := &TSTable{Lifetime: 20}
	m.Reset()
	if m.Lifetime != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestKWDBTSColumnResetStringSize(t *testing.T) {
	m := &KWDBTSColumn{ColumnId: 2}
	m.Reset()
	if m.ColumnId != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestCDCDescriptorResetStringSize(t *testing.T) {
	m := &CDCDescriptor{ID: 1}
	m.Reset()
	if m.ID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestKWDBScheOptionResetStringSize(t *testing.T) {
	m := &KWDBScheOption{Wait: "opt"}
	m.Reset()
	if m.Wait != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestKWDBTagValueResetStringSize(t *testing.T) {
	m := &KWDBTagValue{CTableName: "tag"}
	m.Reset()
	if m.CTableName != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestChildDescResetStringSize(t *testing.T) {
	m := &ChildDesc{STableName: "Stable"}
	m.Reset()
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestSortedHistogramInfoResetStringSize(t *testing.T) {
	m := &SortedHistogramInfo{FromTimeStamp: 5}
	m.Reset()
	if m.FromTimeStamp != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestTableDescriptorCheckConstraintResetStringSize(t *testing.T) {
	m := &TableDescriptor_CheckConstraint{Name: "check"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorNameInfoResetStringSize(t *testing.T) {
	m := &TableDescriptor_NameInfo{ParentID: 1, Name: "tbl"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorReferenceResetStringSize(t *testing.T) {
	m := &TableDescriptor_Reference{ID: 1}
	m.Reset()
	if m.ID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorSequenceOptsResetStringSize(t *testing.T) {
	m := &TableDescriptor_SequenceOpts{Start: 10}
	m.Reset()
	if m.Start != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorSchemaChangeLeaseResetStringSize(t *testing.T) {
	m := &TableDescriptor_SchemaChangeLease{NodeID: 12345}
	m.Reset()
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorReplacementResetStringSize(t *testing.T) {
	m := &TableDescriptor_Replacement{ID: 1}
	m.Reset()
	if m.ID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestPartitioningDescriptorListResetStringSize(t *testing.T) {
	m := &PartitioningDescriptor_List{}
	m.Reset()
	_ = m.String()
	_ = m.Size()
}

func TestPartitioningDescriptorRangeResetStringSize(t *testing.T) {
	m := &PartitioningDescriptor_Range{}
	m.Reset()
	_ = m.String()
	_ = m.Size()
}

func TestPartitioningDescriptorHashPointResetStringSize(t *testing.T) {
	m := &PartitioningDescriptor_HashPoint{}
	m.Reset()
	_ = m.String()
	_ = m.Size()
}

func TestDescriptorMutationColumnResetStringSize(t *testing.T) {
	m := &DescriptorMutation_Column{}
	_ = m.Size()
}

func TestDescriptorMutationIndexResetStringSize(t *testing.T) {
	m := &DescriptorMutation_Index{}
	_ = m.Size()
}

func TestDescriptorMutationConstraintResetStringSize(t *testing.T) {
	m := &DescriptorMutation_Constraint{}
	_ = m.Size()
}

func TestDescriptorMutationPrimaryKeySwapResetStringSize(t *testing.T) {
	m := &DescriptorMutation_PrimaryKeySwap{}
	_ = m.Size()
}

func TestDescriptorMutationMaterializedViewRefreshResetStringSize(t *testing.T) {
	m := &DescriptorMutation_MaterializedViewRefresh{}
	_ = m.Size()
}

func TestSchemaDescriptorNameInfoResetStringSize(t *testing.T) {
	m := &SchemaDescriptor_NameInfo{Name: "sch"}
	m.Reset()
	if m.Name != "" {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTSColResetStringSize(t *testing.T) {
	m := &TSCol{ColOffset: 1}
	m.Reset()
	if m.ColOffset != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
	// m.XXX_DiscardUnknown()
}

func TestTableDescriptorMutationJobResetStringSize(t *testing.T) {
	m := &TableDescriptor_MutationJob{MutationID: 1}
	m.Reset()
	if m.MutationID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorGCDescriptorMutationResetStringSize(t *testing.T) {
	m := &TableDescriptor_GCDescriptorMutation{JobID: 1}
	m.Reset()
	if m.JobID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestTableDescriptorSequenceOptsSequenceOwnerResetStringSize(t *testing.T) {
	m := &TableDescriptor_SequenceOpts_SequenceOwner{OwnerTableID: 1, OwnerColumnID: 1}
	m.Reset()
	if m.OwnerTableID != 0 {
		t.Error("Reset failed")
	}
	_ = m.String()
	_ = m.Size()
}

func TestColumnDescriptor_IsComputed(t *testing.T) {
	col := ColumnDescriptor{Name: "a"}
	if col.IsComputed() {
		t.Error("Expected IsComputed to be false when ComputeExpr is nil")
	}
	expr := "a + 1"
	col.ComputeExpr = &expr
	if !col.IsComputed() {
		t.Error("Expected IsComputed to be true when ComputeExpr is set")
	}
}

func TestColumnDescriptor_HasDefault(t *testing.T) {
	col := ColumnDescriptor{Name: "a"}
	if col.HasDefault() {
		t.Error("Expected HasDefault to be false when DefaultExpr is nil")
	}
	expr := "1"
	col.DefaultExpr = &expr
	if !col.HasDefault() {
		t.Error("Expected HasDefault to be true when DefaultExpr is set")
	}
}

func TestColumnDescriptor_IsNullable(t *testing.T) {
	col := ColumnDescriptor{Name: "a"}
	col.Nullable = false
	if col.IsNullable() {
		t.Error("Expected IsNullable to be false")
	}
	col.Nullable = true
	if !col.IsNullable() {
		t.Error("Expected IsNullable to be true")
	}
}

func TestColumnDescriptor_SQLString(t *testing.T) {
	col := ColumnDescriptor{Name: "a", Type: *types.Int}
	sql := col.SQLString()
	if sql == "" {
		t.Error("Expected SQLString to return non-empty string")
	}
}

func TestIndexDescriptor_IsSharded(t *testing.T) {
	idx := IndexDescriptor{}
	if idx.IsSharded() {
		t.Error("Expected IsSharded to be false by default")
	}
	idx.Sharded.IsSharded = true
	if !idx.IsSharded() {
		t.Error("Expected IsSharded to be true when Sharded.IsSharded is true")
	}
}

func TestIndexDescriptor_ContainsColumnID(t *testing.T) {
	idx := IndexDescriptor{
		ColumnIDs: []ColumnID{1, 2, 3},
	}
	if !idx.ContainsColumnID(2) {
		t.Error("Expected ContainsColumnID to return true for 2")
	}
	if idx.ContainsColumnID(5) {
		t.Error("Expected ContainsColumnID to return false for 5")
	}
}

func TestTableDescriptor_FindColumnByName(t *testing.T) {
	td := TableDescriptor{
		Columns: []ColumnDescriptor{
			{Name: "a", ID: 1},
			{Name: "b", ID: 2},
		},
	}
	col, _, err := td.FindColumnByName("b")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if col == nil || col.Name != "b" {
		t.Error("Expected to find column 'b'")
	}
	_, _, err = td.FindColumnByName("x")
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

func TestTableDescriptor_FindIndexByName(t *testing.T) {
	td := TableDescriptor{
		PrimaryIndex: IndexDescriptor{Name: "pk"},
		Indexes: []IndexDescriptor{
			{Name: "idx1"},
			{Name: "idx2"},
		},
	}
	idx, _, err := td.FindIndexByName("idx2")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if idx == nil || idx.Name != "idx2" {
		t.Error("Expected to find index 'idx2'")
	}
	_, _, err = td.FindIndexByName("notfound")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestTableDescriptor_GetColumnNames(t *testing.T) {
	td := TableDescriptor{
		Columns: []ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
		},
	}
	names := td.GetColumns()
	if len(names) != 2 || names[0].Name != "a" || names[1].Name != "b" {
		t.Errorf("Expected column names [a b], got %v", names)
	}
}

func TestIsShardColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mutable table descriptor
	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "test_table",
		Columns: []ColumnDescriptor{
			{Name: "id", Type: *types.Int},
			{Name: "name", Type: *types.String},
			{Name: "kwdb_internal_id_shard_5", Type: *types.Int},
		},
		PrimaryIndex: IndexDescriptor{
			ID:               1,
			Name:             "primary",
			ColumnNames:      []string{"id"},
			ColumnIDs:        []ColumnID{1},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
			Sharded: ShardedDescriptor{
				IsSharded:    true,
				Name:         "kwdb_internal_id_shard_5",
				ShardBuckets: 5,
			},
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	})

	// Test shard column
	shardCol := &ColumnDescriptor{Name: "kwdb_internal_id_shard_5", Type: *types.Int}
	if !desc.IsShardColumn(shardCol) {
		t.Error("expected kwdb_internal_id_shard_5 to be a shard column")
	}

	// Test non-shard column
	nonShardCol := &ColumnDescriptor{Name: "name", Type: *types.String}
	if desc.IsShardColumn(nonShardCol) {
		t.Error("expected name to not be a shard column")
	}
}

func TestAddIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mutable table descriptor
	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "test_table",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int},
			{ID: 2, Name: "name", Type: *types.String},
		},
		PrimaryIndex: IndexDescriptor{
			ID:               1,
			Name:             "primary",
			ColumnNames:      []string{"id"},
			ColumnIDs:        []ColumnID{1},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	})

	// Create an index
	idx := &IndexDescriptor{
		ID:               2,
		Name:             "idx_name",
		ColumnNames:      []string{"name"},
		ColumnIDs:        []ColumnID{2},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
	}

	// Add index mutation
	err := desc.AddIndexMutation(idx, DescriptorMutation_ADD)
	if err != nil {
		t.Fatalf("failed to add index mutation: %v", err)
	}

	// Verify mutation was added
	if len(desc.Mutations) != 1 {
		t.Errorf("expected 1 mutation, got %d", len(desc.Mutations))
	}

	if desc.Mutations[0].Direction != DescriptorMutation_ADD {
		t.Errorf("expected mutation direction ADD, got %v", desc.Mutations[0].Direction)
	}
}

func TestMakeMutationComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mutable table descriptor
	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "test_table",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int, Nullable: true},
		},
		PrimaryIndex: IndexDescriptor{
			ID:               1,
			Name:             "primary",
			ColumnNames:      []string{"id"},
			ColumnIDs:        []ColumnID{1},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	})

	// Create a NOT NULL constraint mutation
	ck := MakeNotNullCheckConstraint("id", 1, nil, ConstraintValidity_Validating)
	mutation := DescriptorMutation{
		Direction: DescriptorMutation_ADD,
		Descriptor_: &DescriptorMutation_Constraint{
			Constraint: &ConstraintToUpdate{
				ConstraintType: ConstraintToUpdate_NOT_NULL,
				Name:           ck.Name,
				NotNullColumn:  1,
				Check:          *ck,
			},
		},
	}

	// Add the check constraint to the descriptor
	desc.Checks = append(desc.Checks, ck)

	// Complete the mutation
	err := desc.MakeMutationComplete(mutation)
	if err != nil {
		t.Fatalf("failed to make mutation complete: %v", err)
	}

	// Verify the column is now NOT NULL
	col, err := desc.FindColumnByID(1)
	if err != nil {
		t.Fatalf("failed to find column: %v", err)
	}
	if col.Nullable {
		t.Error("expected column to be NOT NULL")
	}

	// Verify the check constraint was removed
	if len(desc.Checks) != 0 {
		t.Errorf("expected 0 check constraints, got %d", len(desc.Checks))
	}
}

func TestRenameConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mutable table descriptor
	desc := NewMutableCreatedTableDescriptor(TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "test_table",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int},
			{ID: 2, Name: "name", Type: *types.String},
		},
		PrimaryIndex: IndexDescriptor{
			ID:               1,
			Name:             "primary",
			ColumnNames:      []string{"id"},
			ColumnIDs:        []ColumnID{1},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		},
		Checks: []*TableDescriptor_CheckConstraint{
			{
				Name:      "old_check",
				Expr:      "id > 0",
				Validity:  ConstraintValidity_Validated,
				ColumnIDs: []ColumnID{1},
			},
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	})

	// Create constraint detail
	detail := ConstraintDetail{
		Kind: ConstraintTypeCheck,
		CheckConstraint: &TableDescriptor_CheckConstraint{
			Name:      "old_check",
			Expr:      "id > 0",
			Validity:  ConstraintValidity_Validated,
			ColumnIDs: []ColumnID{1},
		},
	}

	// Rename the constraint
	err := desc.RenameConstraint(detail, "old_check", "new_check", nil, nil)
	if err != nil {
		t.Fatalf("failed to rename constraint: %v", err)
	}

	// Verify the constraint was renamed
	if len(desc.Checks) != 1 {
		t.Errorf("expected 1 check constraint, got %d", len(desc.Checks))
	}
	if detail.CheckConstraint.Name != "new_check" {
		t.Errorf("expected constraint name 'new_check', got '%s'", detail.CheckConstraint.Name)
	}
}
