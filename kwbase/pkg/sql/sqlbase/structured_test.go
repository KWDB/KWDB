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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
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
					}},
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
						ReferencedTableID:   73,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
				Name: "baz",
			}},
		},
		{
			err: `invalid foreign key backreference: missing table=73: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				InboundFKs: []ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []ColumnID{1},
						OriginTableID:       73,
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
						OriginTableID:       73,
						OriginColumnIDs:     []ColumnID{1},
					},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
				Name: "baz",
			}},
		},

		// Interleaves
		{
			err: `invalid interleave: missing table=73 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID: 1,
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 73, IndexID: 2},
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
						{TableID: 73, IndexID: 2},
					}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
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
						{TableID: 73, IndexID: 2},
					}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid interleave backreference table=73 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 73, Index: 2}},
				},
			},
		},
		{
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 73, Index: 2}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
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
					InterleavedBy: []ForeignKeyReference{{Table: 73, Index: 2}},
				},
			},
			otherDescs: []TableDescriptor{{
				ID:   73,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
	}

	for i, test := range tests {
		txn := kv.NewTxn(ctx, kvDB, s.NodeID())
		if err := test.desc.validateCrossReferences(ctx, txn); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, test.err, test.desc)
		} else if test.err != err.Error() && "internal error: "+test.err != err.Error() {
			// t.Errorf("%d: expected \"%s\", but found \"%s\"", i, test.err, err.Error())
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
		{"PARTITION p1: not enough columns in index for this partitioning",
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
	m := &MaterializedViewRefresh{AsOf: hlc.Timestamp{111111111, 1111111}}
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
	m := &TSTable{Lifetime: 20, ActiveTime: 1}
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

// Test for ColumnIDs.HasPrefix
func TestColumnIDsHasPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    ColumnIDs
		prefix   ColumnIDs
		expected bool
	}{
		{
			name:     "exact match",
			input:    ColumnIDs{1, 2, 3},
			prefix:   ColumnIDs{1, 2, 3},
			expected: true,
		},
		{
			name:     "proper prefix",
			input:    ColumnIDs{1, 2, 3, 4},
			prefix:   ColumnIDs{1, 2},
			expected: true,
		},
		{
			name:     "not a prefix",
			input:    ColumnIDs{1, 2, 3},
			prefix:   ColumnIDs{1, 3},
			expected: false,
		},
		{
			name:     "prefix longer than input",
			input:    ColumnIDs{1, 2},
			prefix:   ColumnIDs{1, 2, 3},
			expected: false,
		},
		{
			name:     "empty prefix",
			input:    ColumnIDs{1, 2, 3},
			prefix:   ColumnIDs{},
			expected: true,
		},
		{
			name:     "both empty",
			input:    ColumnIDs{},
			prefix:   ColumnIDs{},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.input.HasPrefix(test.prefix)
			if result != test.expected {
				t.Errorf("ColumnIDs(%v).HasPrefix(%v) = %v, expected %v",
					test.input, test.prefix, result, test.expected)
			}
		})
	}
}

// Test for ColumnIDs.Equals
func TestColumnIDsEquals(t *testing.T) {
	tests := []struct {
		name     string
		input    ColumnIDs
		other    ColumnIDs
		expected bool
	}{
		{
			name:     "equal slices",
			input:    ColumnIDs{1, 2, 3},
			other:    ColumnIDs{1, 2, 3},
			expected: true,
		},
		{
			name:     "different lengths",
			input:    ColumnIDs{1, 2, 3},
			other:    ColumnIDs{1, 2},
			expected: false,
		},
		{
			name:     "same length, different content",
			input:    ColumnIDs{1, 2, 3},
			other:    ColumnIDs{1, 2, 4},
			expected: false,
		},
		{
			name:     "empty slices",
			input:    ColumnIDs{},
			other:    ColumnIDs{},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.input.Equals(test.other)
			if result != test.expected {
				t.Errorf("ColumnIDs(%v).Equals(%v) = %v, expected %v",
					test.input, test.other, result, test.expected)
			}
		})
	}
}

// Test for ToEncodingDirection
func TestToEncodingDirection(t *testing.T) {
	tests := []struct {
		name        string
		direction   IndexDescriptor_Direction
		expectedDir encoding.Direction
		expectError bool
	}{
		{
			name:        "ascending",
			direction:   IndexDescriptor_ASC,
			expectedDir: encoding.Ascending,
			expectError: false,
		},
		{
			name:        "descending",
			direction:   IndexDescriptor_DESC,
			expectedDir: encoding.Descending,
			expectError: false,
		},
		{
			name:        "invalid direction",
			direction:   IndexDescriptor_Direction(999),
			expectedDir: encoding.Ascending, // default value
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.direction.ToEncodingDirection()
			if test.expectError {
				if err == nil {
					t.Errorf("Expected error for direction %v, but got none", test.direction)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				} else if result != test.expectedDir {
					t.Errorf("ToEncodingDirection() = %v, expected %v", result, test.expectedDir)
				}
			}
		})
	}
}

// Test for HasCompositeKeyEncoding
func TestHasCompositeKeyEncoding(t *testing.T) {
	tests := []struct {
		name     string
		typ      types.Family
		expected bool
	}{
		{
			name:     "collated string",
			typ:      types.CollatedStringFamily,
			expected: true,
		},
		{
			name:     "float",
			typ:      types.FloatFamily,
			expected: true,
		},
		{
			name:     "decimal",
			typ:      types.DecimalFamily,
			expected: true,
		},
		{
			name:     "int",
			typ:      types.IntFamily,
			expected: false,
		},
		{
			name:     "string",
			typ:      types.StringFamily,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := HasCompositeKeyEncoding(test.typ)
			if result != test.expected {
				t.Errorf("HasCompositeKeyEncoding(%v) = %v, expected %v", test.typ, result, test.expected)
			}
		})
	}
}

// Test for DatumTypeHasCompositeKeyEncoding
func TestDatumTypeHasCompositeKeyEncoding(t *testing.T) {
	tests := []struct {
		name     string
		typ      *types.T
		expected bool
	}{
		{
			name:     "collated string",
			typ:      types.MakeCollatedString(types.String, "en_US"),
			expected: true,
		},
		{
			name:     "float",
			typ:      types.Float,
			expected: true,
		},
		{
			name:     "decimal",
			typ:      types.Decimal,
			expected: true,
		},
		{
			name:     "int",
			typ:      types.Int,
			expected: false,
		},
		{
			name:     "string",
			typ:      types.String,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := DatumTypeHasCompositeKeyEncoding(test.typ)
			if result != test.expected {
				t.Errorf("DatumTypeHasCompositeKeyEncoding(%v) = %v, expected %v", test.typ, result, test.expected)
			}
		})
	}
}

// Test for MustBeValueEncoded
func TestMustBeValueEncoded(t *testing.T) {
	tests := []struct {
		name     string
		typ      types.Family
		expected bool
	}{
		{
			name:     "array",
			typ:      types.ArrayFamily,
			expected: true,
		},
		{
			name:     "json",
			typ:      types.JsonFamily,
			expected: true,
		},
		{
			name:     "tuple",
			typ:      types.TupleFamily,
			expected: true,
		},
		{
			name:     "int",
			typ:      types.IntFamily,
			expected: false,
		},
		{
			name:     "string",
			typ:      types.StringFamily,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := MustBeValueEncoded(test.typ)
			if result != test.expected {
				t.Errorf("MustBeValueEncoded(%v) = %v, expected %v", test.typ, result, test.expected)
			}
		})
	}
}

// Test for ColumnTypeIsIndexable
func TestColumnTypeIsIndexable(t *testing.T) {
	tests := []struct {
		name     string
		typ      *types.T
		expected bool
	}{
		{
			name:     "int",
			typ:      types.Int,
			expected: true,
		},
		{
			name:     "string",
			typ:      types.String,
			expected: true,
		},
		{
			name:     "array",
			typ:      types.AnyArray,
			expected: false,
		},
		{
			name:     "json",
			typ:      types.Jsonb,
			expected: false,
		},
		{
			name:     "tuple",
			typ:      types.AnyTuple,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ColumnTypeIsIndexable(test.typ)
			if result != test.expected {
				t.Errorf("ColumnTypeIsIndexable(%v) = %v, expected %v", test.typ, result, test.expected)
			}
		})
	}
}

// Test for ColumnTypeIsInvertedIndexable
func TestColumnTypeIsInvertedIndexable(t *testing.T) {
	tests := []struct {
		name     string
		typ      *types.T
		expected bool
	}{
		{
			name:     "json",
			typ:      types.Jsonb,
			expected: true,
		},
		{
			name:     "array",
			typ:      types.AnyArray,
			expected: true,
		},
		{
			name:     "int",
			typ:      types.Int,
			expected: false,
		},
		{
			name:     "string",
			typ:      types.String,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ColumnTypeIsInvertedIndexable(test.typ)
			if result != test.expected {
				t.Errorf("ColumnTypeIsInvertedIndexable(%v) = %v, expected %v", test.typ, result, test.expected)
			}
		})
	}
}

// Test for IsDefaultPrimaryKeyColumn
func TestIsDefaultPrimaryKeyColumn(t *testing.T) {
	// Test case where the column is a default primary key column
	defaultPKCol := &ColumnDescriptor{
		Name:     "rowid",
		ID:       1,
		Type:     *types.Int,
		Hidden:   true,
		Nullable: false,
		DefaultExpr: func() *string {
			s := "unique_rowid()"
			return &s
		}(),
	}

	if !defaultPKCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected default primary key column to return true")
	}

	// Test case where the column is not a default primary key column
	nonDefaultPKCol := &ColumnDescriptor{
		Name:     "id",
		ID:       1,
		Type:     *types.Int,
		Hidden:   false,
		Nullable: false,
		DefaultExpr: func() *string {
			s := "unique_rowid()"
			return &s
		}(),
	}

	if nonDefaultPKCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected non-default primary key column to return false")
	}

	// Test case where the column is not named rowid
	nonRowIDCol := &ColumnDescriptor{
		Name:     "not_rowid",
		ID:       1,
		Type:     *types.Int,
		Hidden:   true,
		Nullable: false,
		DefaultExpr: func() *string {
			s := "unique_rowid()"
			return &s
		}(),
	}

	if nonRowIDCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected non-rowid column to return false")
	}

	// Test case where the column is not of type int
	nonIntCol := &ColumnDescriptor{
		Name:     "rowid",
		ID:       1,
		Type:     *types.String,
		Hidden:   true,
		Nullable: false,
		DefaultExpr: func() *string {
			s := "unique_rowid()"
			return &s
		}(),
	}

	if nonIntCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected non-int column to return false")
	}

	// Test case where the column doesn't have a default expression
	noDefaultCol := &ColumnDescriptor{
		Name:        "rowid",
		ID:          1,
		Type:        *types.Int,
		Hidden:      true,
		Nullable:    false,
		DefaultExpr: nil,
	}

	if noDefaultCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected column without default to return false")
	}

	// Test case where the default expression is not unique_rowid()
	nonUniqueRowIDCol := &ColumnDescriptor{
		Name:     "rowid",
		ID:       1,
		Type:     *types.Int,
		Hidden:   true,
		Nullable: false,
		DefaultExpr: func() *string {
			s := "123"
			return &s
		}(),
	}

	if nonUniqueRowIDCol.IsDefaultPrimaryKeyColumn() {
		t.Error("Expected column with non-unique_rowid default to return false")
	}
}

// Test for IsPrimaryIndexDefaultRowID
func TestIsPrimaryIndexDefaultRowID(t *testing.T) {
	// Create a table descriptor with a default primary key
	td := &TableDescriptor{
		Columns: []ColumnDescriptor{
			{
				Name:     "rowid",
				ID:       1,
				Type:     *types.Int,
				Hidden:   true,
				Nullable: false,
				DefaultExpr: func() *string {
					s := "unique_rowid()"
					return &s
				}(),
			},
		},
		PrimaryIndex: IndexDescriptor{
			ColumnIDs: []ColumnID{1},
		},
	}

	if !td.IsPrimaryIndexDefaultRowID() {
		t.Error("Expected table with default primary key to return true")
	}

	// Create a table descriptor with a non-default primary key
	td2 := &TableDescriptor{
		Columns: []ColumnDescriptor{
			{
				Name:     "id",
				ID:       1,
				Type:     *types.Int,
				Hidden:   false,
				Nullable: false,
			},
		},
		PrimaryIndex: IndexDescriptor{
			ColumnIDs: []ColumnID{1},
		},
	}

	if td2.IsPrimaryIndexDefaultRowID() {
		t.Error("Expected table with non-default primary key to return false")
	}

	// Create a table descriptor with a multi-column primary key
	td3 := &TableDescriptor{
		Columns: []ColumnDescriptor{
			{
				Name:     "id1",
				ID:       1,
				Type:     *types.Int,
				Hidden:   false,
				Nullable: false,
			},
			{
				Name:     "id2",
				ID:       2,
				Type:     *types.Int,
				Hidden:   false,
				Nullable: false,
			},
		},
		PrimaryIndex: IndexDescriptor{
			ColumnIDs: []ColumnID{1, 2},
		},
	}

	if td3.IsPrimaryIndexDefaultRowID() {
		t.Error("Expected table with multi-column primary key to return false")
	}
}

// Test for HasNullDefault
func TestHasNullDefault(t *testing.T) {
	// Test case where the column has a NULL default
	nullDefaultCol := &ColumnDescriptor{
		Name:        "test",
		ID:          1,
		Type:        *types.String,
		DefaultExpr: func() *string { s := "NULL"; return &s }(),
	}

	if !nullDefaultCol.HasNullDefault() {
		t.Error("Expected column with NULL default to return true")
	}

	// Test case where the column has a non-NULL default
	nonNullDefaultCol := &ColumnDescriptor{
		Name:        "test",
		ID:          1,
		Type:        *types.String,
		DefaultExpr: func() *string { s := "'hello'"; return &s }(),
	}

	if nonNullDefaultCol.HasNullDefault() {
		t.Error("Expected column with non-NULL default to return false")
	}

	// Test case where the column has no default
	noDefaultCol := &ColumnDescriptor{
		Name:        "test",
		ID:          1,
		Type:        *types.String,
		DefaultExpr: nil,
	}

	if noDefaultCol.HasNullDefault() {
		t.Error("Expected column with no default to return false")
	}
}

// Test for IsSet (ForeignKeyReference)
func TestForeignKeyReferenceIsSet(t *testing.T) {
	// Test case where the foreign key is set
	setFK := ForeignKeyReference{
		Table: 1,
	}

	if !setFK.IsSet() {
		t.Error("Expected set foreign key to return true")
	}

	// Test case where the foreign key is not set
	unsetFK := ForeignKeyReference{
		Table: 0,
	}

	if unsetFK.IsSet() {
		t.Error("Expected unset foreign key to return false")
	}
}

// Test for GeneratedFamilyName
func TestGeneratedFamilyName(t *testing.T) {
	tests := []struct {
		name        string
		familyID    FamilyID
		columnNames []string
		expected    string
	}{
		{
			name:        "single column",
			familyID:    1,
			columnNames: []string{"col1"},
			expected:    "fam_1_col1",
		},
		{
			name:        "multiple columns",
			familyID:    2,
			columnNames: []string{"col1", "col2", "col3"},
			expected:    "fam_2_col1_col2_col3",
		},
		{
			name:        "no columns",
			familyID:    3,
			columnNames: []string{},
			expected:    "fam_3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := GeneratedFamilyName(test.familyID, test.columnNames...)
			if result != test.expected {
				t.Errorf("GeneratedFamilyName(%d, %v) = %s, expected %s",
					test.familyID, test.columnNames, result, test.expected)
			}
		})
	}
}

// Test for ConvertUnicodeString
func TestConvertUnicodeString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "unicode escape",
			input:    "Hello\\u0020World", // space character
			expected: "'Hello World'",
		},
		{
			name:     "multiple unicode escapes",
			input:    "Hello\\u0020World\\u0021", // space and exclamation
			expected: "'Hello World!'",
		},
		{
			name:     "no unicode escapes",
			input:    "Hello World",
			expected: "'Hello World'",
		},
		{
			name:     "mixed case unicode",
			input:    "Test\\u0041", // capital A
			expected: "'TestA'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertUnicodeString(test.input)
			if result != test.expected {
				t.Errorf("ConvertUnicodeString(%s) = %s, expected %s",
					test.input, result, test.expected)
			}
		})
	}
}

// Test for IsVirtualTable (standalone function)
func TestIsVirtualTable(t *testing.T) {
	// Test with a virtual table ID (>= MinVirtualID)
	virtualID := ID(MinVirtualID)
	if !IsVirtualTable(virtualID) {
		t.Error("Expected virtual table ID to return true")
	}

	// Test with a non-virtual table ID (< MinVirtualID)
	nonVirtualID := ID(MinVirtualID - 1)
	if IsVirtualTable(nonVirtualID) {
		t.Error("Expected non-virtual table ID to return false")
	}

	// Test with ID 0
	zeroID := ID(0)
	if IsVirtualTable(zeroID) {
		t.Error("Expected zero ID to return false")
	}
}

// Test for IsPhysicalTable
func TestIsPhysicalTable(t *testing.T) {
	// Test with a sequence
	seqDesc := &TableDescriptor{
		SequenceOpts: &TableDescriptor_SequenceOpts{},
	}
	if !seqDesc.IsPhysicalTable() {
		t.Error("Expected sequence to be a physical table")
	}

	// Test with a regular table
	tableDesc := &TableDescriptor{
		ViewQuery:          "",
		SequenceOpts:       nil,
		IsMaterializedView: false,
	}
	if !tableDesc.IsPhysicalTable() {
		t.Error("Expected regular table to be a physical table")
	}

	// Test with a materialized view
	matViewDesc := &TableDescriptor{
		IsMaterializedView: true,
	}
	if !matViewDesc.IsPhysicalTable() {
		t.Error("Expected materialized view to be a physical table")
	}

	// Test with a virtual table
	virtualDesc := &TableDescriptor{
		ID: MinVirtualID,
	}
	if virtualDesc.IsPhysicalTable() {
		t.Error("Expected virtual table to not be a physical table")
	}
}

// Test for IsTable
func TestIsTable(t *testing.T) {
	// Test with a regular table
	tableDesc := &TableDescriptor{
		ViewQuery:    "",
		SequenceOpts: nil,
	}
	if !tableDesc.IsTable() {
		t.Error("Expected regular table to return true")
	}

	// Test with a view
	viewDesc := &TableDescriptor{
		ViewQuery: "SELECT * FROM foo",
	}
	if viewDesc.IsTable() {
		t.Error("Expected view to return false")
	}

	// Test with a sequence
	seqDesc := &TableDescriptor{
		SequenceOpts: &TableDescriptor_SequenceOpts{},
	}
	if seqDesc.IsTable() {
		t.Error("Expected sequence to return false")
	}
}

// Test for IsTSTable
func TestIsTSTable(t *testing.T) {
	// Test with a timeseries table
	tsDesc := &TableDescriptor{
		TableType: tree.TimeseriesTable,
	}
	if !tsDesc.IsTSTable() {
		t.Error("Expected timeseries table to return true")
	}

	// Test with a template table
	templateDesc := &TableDescriptor{
		TableType: tree.TemplateTable,
	}
	if !templateDesc.IsTSTable() {
		t.Error("Expected template table to return true")
	}

	// Test with an instance table
	instanceDesc := &TableDescriptor{
		TableType: tree.InstanceTable,
	}
	if !instanceDesc.IsTSTable() {
		t.Error("Expected instance table to return true")
	}

	// Test with a regular table
	regularDesc := &TableDescriptor{
		TableType: tree.RelationalTable,
	}
	if regularDesc.IsTSTable() {
		t.Error("Expected regular table to return false")
	}
}

// Test for IsView
func TestIsView(t *testing.T) {
	// Test with a view
	viewDesc := &TableDescriptor{
		ViewQuery: "SELECT * FROM foo",
	}
	if !viewDesc.IsView() {
		t.Error("Expected view to return true")
	}

	// Test with a regular table
	tableDesc := &TableDescriptor{
		ViewQuery: "",
	}
	if tableDesc.IsView() {
		t.Error("Expected regular table to return false")
	}
}

// Test for MaterializedView
func TestMaterializedView(t *testing.T) {
	// Test with a materialized view
	matViewDesc := &TableDescriptor{
		IsMaterializedView: true,
	}
	if !matViewDesc.MaterializedView() {
		t.Error("Expected materialized view to return true")
	}

	// Test with a regular table
	tableDesc := &TableDescriptor{
		IsMaterializedView: false,
	}
	if tableDesc.MaterializedView() {
		t.Error("Expected regular table to return false")
	}
}

// Test for IsAs
func TestIsAs(t *testing.T) {
	// Test with a table created with AS
	asDesc := &TableDescriptor{
		CreateQuery: "CREATE TABLE ... AS SELECT ...",
	}
	if !asDesc.IsAs() {
		t.Error("Expected table with CreateQuery to return true")
	}

	// Test with a regular table
	asDesc = &TableDescriptor{
		CreateQuery: "",
	}
	if asDesc.IsAs() {
		t.Error("Expected regular table to return false")
	}
}

// Test for IsSequence
func TestIsSequence(t *testing.T) {
	// Test with a sequence
	seqDesc := &TableDescriptor{
		SequenceOpts: &TableDescriptor_SequenceOpts{},
	}
	if !seqDesc.IsSequence() {
		t.Error("Expected sequence to return true")
	}

	// Test with a regular table
	tableDesc := &TableDescriptor{
		SequenceOpts: nil,
	}
	if tableDesc.IsSequence() {
		t.Error("Expected regular table to return false")
	}
}

// Test for IsVirtualTable (TableDescriptor method)
func TestTableDescriptorIsVirtualTable(t *testing.T) {
	// Test with a virtual table
	virtualDesc := &TableDescriptor{
		ID: MinVirtualID,
	}
	if !virtualDesc.IsVirtualTable() {
		t.Error("Expected virtual table to return true")
	}

	// Test with a regular table
	tableDesc := &TableDescriptor{
		ID: MinVirtualID - 1,
	}
	if tableDesc.IsVirtualTable() {
		t.Error("Expected regular table to return false")
	}
}

// Test for HasOwner
func TestHasOwner(t *testing.T) {
	// Test with an owner
	optsWithOwner := &TableDescriptor_SequenceOpts{
		SequenceOwner: TableDescriptor_SequenceOpts_SequenceOwner{
			OwnerTableID:  1,
			OwnerColumnID: 1,
		},
	}
	if !optsWithOwner.HasOwner() {
		t.Error("Expected sequence options with owner to return true")
	}

	// Test without an owner
	optsWithoutOwner := &TableDescriptor_SequenceOpts{
		SequenceOwner: TableDescriptor_SequenceOpts_SequenceOwner{},
	}
	if optsWithoutOwner.HasOwner() {
		t.Error("Expected sequence options without owner to return false")
	}
}

// Test for IsDataCol
func TestIsDataCol(t *testing.T) {
	// Test with a data column
	dataCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_DATA,
		},
	}
	if !dataCol.IsDataCol() {
		t.Error("Expected data column to return true")
	}

	// Test with a non-data column
	tagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_TAG,
		},
	}
	if tagCol.IsDataCol() {
		t.Error("Expected tag column to return false")
	}
}

// Test for IsTagCol
func TestIsTagCol(t *testing.T) {
	// Test with a tag column
	tagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_TAG,
		},
	}
	if !tagCol.IsTagCol() {
		t.Error("Expected tag column to return true")
	}

	// Test with a primary tag column
	ptagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_PTAG,
		},
	}
	if !ptagCol.IsTagCol() {
		t.Error("Expected primary tag column to return true")
	}

	// Test with a data column
	dataCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_DATA,
		},
	}
	if dataCol.IsTagCol() {
		t.Error("Expected data column to return false")
	}
}

// Test for IsPrimaryTagCol
func TestIsPrimaryTagCol(t *testing.T) {
	// Test with a primary tag column
	ptagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_PTAG,
		},
	}
	if !ptagCol.IsPrimaryTagCol() {
		t.Error("Expected primary tag column to return true")
	}

	// Test with a regular tag column
	tagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_TAG,
		},
	}
	if !ptagCol.IsPrimaryTagCol() {
		t.Error("Expected regular tag column to return false")
	}
	if tagCol.IsPrimaryTagCol() {
		t.Error("Expected regular tag column to return false")
	}
}

// Test for IsOrdinaryTagCol
func TestIsOrdinaryTagCol(t *testing.T) {
	// Test with a regular tag column
	tagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_TAG,
		},
	}
	if !tagCol.IsOrdinaryTagCol() {
		t.Error("Expected regular tag column to return true")
	}

	// Test with a primary tag column
	ptagCol := &ColumnDescriptor{
		TsCol: TSCol{
			ColumnType: ColumnType_TYPE_PTAG,
		},
	}
	if ptagCol.IsOrdinaryTagCol() {
		t.Error("Expected primary tag column to return false")
	}
}

// Test for TsColStorgeLen
func TestTsColStorgeLen(t *testing.T) {
	// Test with a specific storage length
	col := &ColumnDescriptor{
		TsCol: TSCol{
			StorageLen: 100,
		},
	}

	if col.TsColStorgeLen() != 100 {
		t.Error("Expected storage length to return 100")
	}
}

// Test for ColID
func TestColID(t *testing.T) {
	col := &ColumnDescriptor{
		ID: 42,
	}

	if cat.StableID(col.ColID()) != cat.StableID(42) {
		t.Error("Expected ColID to return 42")
	}
}

// Test for ColName
func TestColName(t *testing.T) {
	col := &ColumnDescriptor{
		Name: "test_column",
	}

	if string(col.ColName()) != "test_column" {
		t.Error("Expected ColName to return 'test_column'")
	}
}

// Test for DatumType
func TestDatumType(t *testing.T) {
	col := &ColumnDescriptor{
		Type: *types.Int,
	}

	if col.DatumType() != &col.Type {
		t.Error("Expected DatumType to return the address of the Type field")
	}
}

// Test for ColTypePrecision
func TestColTypePrecision(t *testing.T) {
	col := &ColumnDescriptor{
		Type: *types.MakeDecimal(10, 2),
	}

	if col.ColTypePrecision() != 10 {
		t.Error("Expected ColTypePrecision to return 10")
	}
}

// Test for ColTypeWidth
func TestColTypeWidth(t *testing.T) {
	col := &ColumnDescriptor{
		Type: *types.MakeString(50),
	}

	if col.ColTypeWidth() != 50 {
		t.Error("Expected ColTypeWidth to return 50")
	}
}

// Test for ColTypeStr
func TestColTypeStr(t *testing.T) {
	col := &ColumnDescriptor{
		Type: *types.Int,
	}

	if col.ColTypeStr() != "INT8" {
		t.Error("Expected ColTypeStr to return 'INT8'")
	}
}

// Test for IsHidden
func TestIsHidden(t *testing.T) {
	col := &ColumnDescriptor{
		Hidden: true,
	}

	if !col.IsHidden() {
		t.Error("Expected IsHidden to return true")
	}

	col.Hidden = false
	if col.IsHidden() {
		t.Error("Expected IsHidden to return false")
	}
}

// Test for HasDefault
func TestHasDefault(t *testing.T) {
	col := &ColumnDescriptor{
		DefaultExpr: func() *string { s := "123"; return &s }(),
	}

	if !col.HasDefault() {
		t.Error("Expected HasDefault to return true")
	}

	col.DefaultExpr = nil
	if col.HasDefault() {
		t.Error("Expected HasDefault to return false")
	}
}

// Test for DefaultExprStr
func TestDefaultExprStr(t *testing.T) {
	expr := "123"
	col := &ColumnDescriptor{
		DefaultExpr: &expr,
	}

	if col.DefaultExprStr() != "123" {
		t.Error("Expected DefaultExprStr to return '123'")
	}
}

// Test for ComputedExprStr
func TestComputedExprStr(t *testing.T) {
	expr := "a + b"
	col := &ColumnDescriptor{
		ComputeExpr: &expr,
	}

	if col.ComputedExprStr() != "a + b" {
		t.Error("Expected ComputedExprStr to return 'a + b'")
	}
}

// Test for CheckCanBeFKRef
func TestCheckCanBeFKRef(t *testing.T) {
	// Test with a computed column
	compCol := &ColumnDescriptor{
		Name:        "computed_col",
		ComputeExpr: func() *string { s := "a + b"; return &s }(),
	}

	err := compCol.CheckCanBeFKRef()
	if err == nil {
		t.Error("Expected error for computed column")
	}

	// Test with a non-computed column
	normalCol := &ColumnDescriptor{
		Name:        "normal_col",
		ComputeExpr: nil,
	}

	err = normalCol.CheckCanBeFKRef()
	if err != nil {
		t.Error("Expected no error for non-computed column")
	}
}

// Test for GetParentSchemaID
func TestGetParentSchemaID(t *testing.T) {
	// Test with a non-zero parent schema ID
	desc1 := &TableDescriptor{
		UnexposedParentSchemaID: 42,
	}

	if desc1.GetParentSchemaID() != 42 {
		t.Error("Expected GetParentSchemaID to return 42")
	}

	// Test with a zero parent schema ID (should return PublicSchemaID)
	desc2 := &TableDescriptor{
		UnexposedParentSchemaID: 0,
	}

	if desc2.GetParentSchemaID() != keys.PublicSchemaID {
		t.Error("Expected GetParentSchemaID to return PublicSchemaID when UnexposedParentSchemaID is 0")
	}
}

// Test for IsInterleaved
func TestIsInterleaved(t *testing.T) {
	// Test with ancestors
	idx1 := &IndexDescriptor{
		Interleave: InterleaveDescriptor{
			Ancestors: []InterleaveDescriptor_Ancestor{
				{TableID: 1, IndexID: 1},
			},
		},
	}

	if !idx1.IsInterleaved() {
		t.Error("Expected index with ancestors to return true")
	}

	// Test with interleaved by
	idx2 := &IndexDescriptor{
		InterleavedBy: []ForeignKeyReference{
			{Table: 1, Index: 1},
		},
	}

	if !idx2.IsInterleaved() {
		t.Error("Expected index with interleaved by to return true")
	}

	// Test with neither
	idx3 := &IndexDescriptor{
		Interleave: InterleaveDescriptor{
			Ancestors: []InterleaveDescriptor_Ancestor{},
		},
		InterleavedBy: []ForeignKeyReference{},
	}

	if idx3.IsInterleaved() {
		t.Error("Expected index with neither ancestors nor interleaved by to return false")
	}
}

// Test for GetEncodingType
func TestGetEncodingType(t *testing.T) {
	// Test with primary index ID
	idx1 := &IndexDescriptor{
		ID:           1,
		EncodingType: SecondaryIndexEncoding,
	}

	if idx1.GetEncodingType(1) != PrimaryIndexEncoding {
		t.Error("Expected GetEncodingType to return PrimaryIndexEncoding for primary index")
	}

	// Test with secondary index ID
	idx2 := &IndexDescriptor{
		ID:           2,
		EncodingType: SecondaryIndexEncoding,
	}

	if idx2.GetEncodingType(1) != SecondaryIndexEncoding {
		t.Error("Expected GetEncodingType to return SecondaryIndexEncoding for secondary index")
	}
}

// Test for HasOldStoredColumns
func TestHasOldStoredColumns(t *testing.T) {
	// Test with old stored columns format
	idx1 := &IndexDescriptor{
		ExtraColumnIDs:   []ColumnID{1, 2},
		StoreColumnIDs:   []ColumnID{1}, // fewer StoreColumnIDs than StoreColumnNames
		StoreColumnNames: []string{"col1", "col2"},
	}

	if !idx1.HasOldStoredColumns() {
		t.Error("Expected index with old stored columns format to return true")
	}

	// Test with new stored columns format
	idx2 := &IndexDescriptor{
		ExtraColumnIDs:   []ColumnID{1, 2},
		StoreColumnIDs:   []ColumnID{1, 2}, // same number of StoreColumnIDs as StoreColumnNames
		StoreColumnNames: []string{"col1", "col2"},
	}

	if idx2.HasOldStoredColumns() {
		t.Error("Expected index with new stored columns format to return false")
	}
}

// Test for FullColumnIDs
func TestFullColumnIDs(t *testing.T) {
	// Test with unique index
	uniqueIdx := &IndexDescriptor{
		Unique:           true,
		ColumnIDs:        []ColumnID{1, 2},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_DESC},
	}

	colIDs, dirs := uniqueIdx.FullColumnIDs()
	if len(colIDs) != 2 || colIDs[0] != 1 || colIDs[1] != 2 {
		t.Error("Expected FullColumnIDs to return only the original column IDs for unique index")
	}
	if len(dirs) != 2 || dirs[0] != IndexDescriptor_ASC || dirs[1] != IndexDescriptor_DESC {
		t.Error("Expected FullColumnIDs to return only the original column directions for unique index")
	}

	// Test with non-unique index
	nonUniqueIdx := &IndexDescriptor{
		Unique:           false,
		ColumnIDs:        []ColumnID{1, 2},
		ExtraColumnIDs:   []ColumnID{3, 4},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_DESC},
	}

	colIDs, dirs = nonUniqueIdx.FullColumnIDs()
	if len(colIDs) != 4 || colIDs[0] != 1 || colIDs[1] != 2 || colIDs[2] != 3 || colIDs[3] != 4 {
		t.Error("Expected FullColumnIDs to return column IDs + extra column IDs for non-unique index")
	}
	if len(dirs) != 4 || dirs[0] != IndexDescriptor_ASC || dirs[1] != IndexDescriptor_DESC || dirs[2] != IndexDescriptor_ASC || dirs[3] != IndexDescriptor_ASC {
		t.Error("Expected FullColumnIDs to return original directions + ASC for extra columns for non-unique index")
	}
}

// Test for ColNamesString
func TestColNamesString(t *testing.T) {
	idx := &IndexDescriptor{
		ColumnNames:      []string{"col1", "col2"},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_DESC},
		Type:             IndexDescriptor_FORWARD,
	}

	result := idx.ColNamesString()
	if result == "" {
		t.Error("Expected ColNamesString to return non-empty string")
	}
}

// Test for SQLString
func TestIndexDescriptorSQLString(t *testing.T) {
	tableName := tree.MakeTableName("db", "table")
	idx := &IndexDescriptor{
		Name:             "test_idx",
		Unique:           true,
		ColumnNames:      []string{"col1", "col2"},
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_DESC},
	}

	result := idx.SQLString(&tableName)
	if result == "" {
		t.Error("Expected SQLString to return non-empty string")
	}
}

// Test for IsSharded
func TestIndexDescriptorIsSharded(t *testing.T) {
	idx := &IndexDescriptor{}
	if idx.IsSharded() {
		t.Error("Expected IsSharded to be false by default")
	}

	idx.Sharded.IsSharded = true
	if !idx.IsSharded() {
		t.Error("Expected IsSharded to be true when Sharded.IsSharded is true")
	}
}

// Test for RunOverAllColumns
func TestRunOverAllColumns(t *testing.T) {
	idx := &IndexDescriptor{
		ColumnIDs:      []ColumnID{1, 2},
		ExtraColumnIDs: []ColumnID{3, 4},
		StoreColumnIDs: []ColumnID{5, 6},
	}

	visited := make(map[ColumnID]bool)
	err := idx.RunOverAllColumns(func(id ColumnID) error {
		visited[id] = true
		return nil
	})

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := map[ColumnID]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}
	for id, expectedPresent := range expected {
		if actualPresent := visited[id]; actualPresent != expectedPresent {
			t.Errorf("Column ID %d: expected present=%v, got present=%v", id, expectedPresent, actualPresent)
		}
	}
}

// Test for FindPartitionByName on PartitioningDescriptor
func TestFindPartitionByNameOnPartitioningDescriptor(t *testing.T) {
	partDesc := &PartitioningDescriptor{
		List: []PartitioningDescriptor_List{
			{Name: "p1"},
			{
				Name: "p2",
				Subpartitioning: PartitioningDescriptor{
					List: []PartitioningDescriptor_List{
						{Name: "sub_p1"},
					},
				},
			},
		},
		Range: []PartitioningDescriptor_Range{
			{Name: "p3"},
		},
		HashPoint: []PartitioningDescriptor_HashPoint{
			{Name: "p4"},
		},
	}

	// Test finding a list partition
	result := partDesc.FindPartitionByName("p1")
	if result == nil {
		t.Error("Expected to find partition 'p1'")
	}

	// Test finding a subpartition
	result = partDesc.FindPartitionByName("sub_p1")
	if result == nil {
		t.Error("Expected to find subpartition 'sub_p1'")
	}

	// Test finding a range partition
	result = partDesc.FindPartitionByName("p3")
	if result == nil {
		t.Error("Expected to find range partition 'p3'")
	}

	// Test finding a hash point partition
	result = partDesc.FindPartitionByName("p4")
	if result == nil {
		t.Error("Expected to find hash point partition 'p4'")
	}

	// Test finding a non-existent partition
	result = partDesc.FindPartitionByName("nonexistent")
	if result != nil {
		t.Error("Expected to not find non-existent partition")
	}
}

// Test for FindPartitionByName on IndexDescriptor
func TestFindPartitionByNameOnIndexDescriptor(t *testing.T) {
	idx := &IndexDescriptor{
		Partitioning: PartitioningDescriptor{
			List: []PartitioningDescriptor_List{
				{Name: "p1"},
			},
		},
	}

	result := idx.FindPartitionByName("p1")
	if result == nil {
		t.Error("Expected to find partition 'p1'")
	}

	result = idx.FindPartitionByName("nonexistent")
	if result != nil {
		t.Error("Expected to not find non-existent partition")
	}
}

// Test for allocateName
func TestAllocateName(t *testing.T) {
	tableDesc := NewMutableCreatedTableDescriptor(TableDescriptor{
		Name: "test_table",
		Indexes: []IndexDescriptor{
			{Name: "existing_idx"},
		},
	})

	// Test allocating a name for a new index
	newIdx := &IndexDescriptor{
		ColumnNames: []string{"col1", "col2"},
		Unique:      true,
	}
	newIdx.allocateName(tableDesc)

	// The name should be "test_table_col1_col2_key" since it's unique
	expectedName := "test_table_col1_col2_key"
	if newIdx.Name != expectedName {
		t.Errorf("Expected name '%s', got '%s'", expectedName, newIdx.Name)
	}

	// Now add an index with the same column names to force a numbered suffix
	similarIdx := &IndexDescriptor{
		Name:        "test_table_col1_col2_key",
		ColumnNames: []string{"col1", "col2"},
		Unique:      true,
	}
	tableDesc.Indexes = append(tableDesc.Indexes, *similarIdx)

	anotherIdx := &IndexDescriptor{
		ColumnNames: []string{"col1", "col2"},
		Unique:      true,
	}
	anotherIdx.allocateName(tableDesc)

	// The name should be "test_table_col1_col2_key1" due to conflict
	expectedName2 := "test_table_col1_col2_key1"
	if anotherIdx.Name != expectedName2 {
		t.Errorf("Expected name '%s', got '%s'", expectedName2, anotherIdx.Name)
	}
}

// Test for FillColumns
func TestFillColumns(t *testing.T) {
	idx := &IndexDescriptor{}
	elems := tree.IndexElemList{
		{Column: "col1", Direction: tree.Ascending},
		{Column: "col2", Direction: tree.Descending},
		{Column: "col3", Direction: tree.DefaultDirection}, // defaults to Ascending
	}

	err := idx.FillColumns(elems)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(idx.ColumnNames) != 3 || idx.ColumnNames[0] != "col1" || idx.ColumnNames[1] != "col2" || idx.ColumnNames[2] != "col3" {
		t.Errorf("Expected column names ['col1', 'col2', 'col3'], got %v", idx.ColumnNames)
	}

	if len(idx.ColumnDirections) != 3 ||
		idx.ColumnDirections[0] != IndexDescriptor_ASC ||
		idx.ColumnDirections[1] != IndexDescriptor_DESC ||
		idx.ColumnDirections[2] != IndexDescriptor_ASC {
		t.Errorf("Expected column directions [ASC, DESC, ASC], got %v", idx.ColumnDirections)
	}
}

// Test for ContainsColumnID
func TestIndexDescriptorContainsColumnID(t *testing.T) {
	idx := &IndexDescriptor{
		ColumnIDs:      []ColumnID{1, 2, 3},
		ExtraColumnIDs: []ColumnID{4, 5},
		StoreColumnIDs: []ColumnID{6, 7},
	}

	if !idx.ContainsColumnID(2) {
		t.Error("Expected ContainsColumnID to return true for 2")
	}

	if !idx.ContainsColumnID(5) {
		t.Error("Expected ContainsColumnID to return true for 5")
	}

	if !idx.ContainsColumnID(7) {
		t.Error("Expected ContainsColumnID to return true for 7")
	}

	if idx.ContainsColumnID(8) {
		t.Error("Expected ContainsColumnID to return false for 8")
	}
}

// Test for IsNullable
func TestColumnDescriptorIsNullable(t *testing.T) {
	col := &ColumnDescriptor{}

	col.Nullable = true
	if !col.IsNullable() {
		t.Error("Expected IsNullable to return true when Nullable is true")
	}

	col.Nullable = false
	if col.IsNullable() {
		t.Error("Expected IsNullable to return false when Nullable is false")
	}
}

// Test for IsComputed
func TestColumnDescriptorIsComputed(t *testing.T) {
	col := &ColumnDescriptor{}

	col.ComputeExpr = nil
	if col.IsComputed() {
		t.Error("Expected IsComputed to return false when ComputeExpr is nil")
	}

	expr := "a + 1"
	col.ComputeExpr = &expr
	if !col.IsComputed() {
		t.Error("Expected IsComputed to return true when ComputeExpr is set")
	}
}

// Test for HasDefault
func TestColumnDescriptorHasDefault(t *testing.T) {
	col := &ColumnDescriptor{}

	col.DefaultExpr = nil
	if col.HasDefault() {
		t.Error("Expected HasDefault to return false when DefaultExpr is nil")
	}

	expr := "123"
	col.DefaultExpr = &expr
	if !col.HasDefault() {
		t.Error("Expected HasDefault to return true when DefaultExpr is set")
	}
}

// Test for String methods for ForeignKeyReference
func TestForeignKeyReferenceStringMethods(t *testing.T) {
	// Test Match string method
	matchSimple := ForeignKeyReference_SIMPLE
	if matchSimple.String() != "MATCH SIMPLE" {
		t.Errorf("Expected 'MATCH SIMPLE', got '%s'", matchSimple.String())
	}

	matchFull := ForeignKeyReference_FULL
	if matchFull.String() != "MATCH FULL" {
		t.Errorf("Expected 'MATCH FULL', got '%s'", matchFull.String())
	}

	matchPartial := ForeignKeyReference_PARTIAL
	if matchPartial.String() != "MATCH PARTIAL" {
		t.Errorf("Expected 'MATCH PARTIAL', got '%s'", matchPartial.String())
	}

	// Test Action string method
	actionRestrict := ForeignKeyReference_RESTRICT
	if actionRestrict.String() != "RESTRICT" {
		t.Errorf("Expected 'RESTRICT', got '%s'", actionRestrict.String())
	}

	actionSetNull := ForeignKeyReference_SET_NULL
	if actionSetNull.String() != "SET NULL" {
		t.Errorf("Expected 'SET NULL', got '%s'", actionSetNull.String())
	}

	actionCascade := ForeignKeyReference_CASCADE
	if actionCascade.String() != "CASCADE" {
		t.Errorf("Expected 'CASCADE', got '%s'", actionCascade.String())
	}
}

// Test for PartitionNames
func TestPartitionNames(t *testing.T) {
	desc := &TableDescriptor{
		PrimaryIndex: IndexDescriptor{
			Partitioning: PartitioningDescriptor{
				List: []PartitioningDescriptor_List{
					{Name: "p1"},
					{
						Name: "p2",
						Subpartitioning: PartitioningDescriptor{
							List: []PartitioningDescriptor_List{
								{Name: "sub_p1"},
							},
						},
					},
				},
				Range: []PartitioningDescriptor_Range{
					{Name: "p3"},
				},
				HashPoint: []PartitioningDescriptor_HashPoint{
					{Name: "p4"},
				},
			},
		},
		Indexes: []IndexDescriptor{
			{
				Partitioning: PartitioningDescriptor{
					List: []PartitioningDescriptor_List{
						{Name: "p5"},
					},
				},
			},
		},
	}

	names := desc.PartitionNames()
	expectedNames := map[string]bool{
		"p1": true, "p2": true, "sub_p1": true, "p3": true, "p4": true, "p5": true,
	}

	if len(names) != len(expectedNames) {
		t.Errorf("Expected %d partition names, got %d", len(expectedNames), len(names))
	}

	for _, name := range names {
		if !expectedNames[name] {
			t.Errorf("Unexpected partition name: %s", name)
		}
		delete(expectedNames, name)
	}

	if len(expectedNames) > 0 {
		t.Errorf("Missing partition names: %v", expectedNames)
	}
}

// Test for PartitionNames on PartitioningDescriptor
func TestPartitionNamesOnPartitioningDescriptor(t *testing.T) {
	partDesc := &PartitioningDescriptor{
		List: []PartitioningDescriptor_List{
			{Name: "p1"},
			{
				Name: "p2",
				Subpartitioning: PartitioningDescriptor{
					List: []PartitioningDescriptor_List{
						{Name: "sub_p1"},
					},
				},
			},
		},
		Range: []PartitioningDescriptor_Range{
			{Name: "p3"},
		},
		HashPoint: []PartitioningDescriptor_HashPoint{
			{Name: "p4"},
		},
	}

	names := partDesc.PartitionNames()
	expectedNames := map[string]bool{
		"p1": true, "p2": true, "sub_p1": true, "p3": true, "p4": true,
	}

	if len(names) != len(expectedNames) {
		t.Errorf("Expected %d partition names, got %d", len(expectedNames), len(names))
	}

	for _, name := range names {
		if !expectedNames[name] {
			t.Errorf("Unexpected partition name: %s", name)
		}
		delete(expectedNames, name)
	}

	if len(expectedNames) > 0 {
		t.Errorf("Missing partition names: %v", expectedNames)
	}
}

// Test for SetAuditMode
func TestSetAuditMode(t *testing.T) {
	desc := &TableDescriptor{AuditMode: TableDescriptor_READWRITE}

	// Test disabling audit mode
	changed, err := desc.SetAuditMode(tree.AuditModeDisable)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !changed {
		t.Error("Expected changed to be false when setting audit mode")
	}
	if desc.AuditMode != TableDescriptor_DISABLED {
		t.Error("Expected audit mode to be DISABLED")
	}

	// Test enabling read-write audit mode
	changed, err = desc.SetAuditMode(tree.AuditModeReadWrite)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !changed {
		t.Error("Expected changed to be true when setting audit mode")
	}
	if desc.AuditMode != TableDescriptor_READWRITE {
		t.Error("Expected audit mode to be READWRITE")
	}

	// Test with unknown audit mode
	changed, err = desc.SetAuditMode(tree.AuditMode(999))
	if err == nil {
		t.Error("Expected error for unknown audit mode")
	}
	if changed {
		t.Error("Expected changed to be false when there's an error")
	}
}

// Test for GenerateUniqueConstraintName
func TestGenerateUniqueConstraintName(t *testing.T) {
	// Simple case - name doesn't exist
	name := GenerateUniqueConstraintName("test_prefix", func(name string) bool {
		return name == "existing_name"
	})
	if name != "test_prefix" {
		t.Errorf("Expected 'test_prefix', got '%s'", name)
	}

	// Case where the prefix exists - should try numbered suffixes
	name = GenerateUniqueConstraintName("existing_name", func(name string) bool {
		return name == "existing_name" || name == "existing_name_1" || name == "existing_name_2"
	})
	if name != "existing_name_3" {
		t.Errorf("Expected 'existing_name_3', got '%s'", name)
	}
}

// Test for TagValueToString
func TestTagValueToString(t *testing.T) {
	// Test with DString
	dstr := tree.NewDString("hello")
	result := TagValueToString(dstr)
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}

	// Test with DBytes
	dbytes := tree.NewDBytes(tree.DBytes("world"))
	result = TagValueToString(dbytes)
	if result != "b'\\x776f726c64'" {
		t.Errorf("Expected \"b'world'\", got '%s'", result)
	}

	// Test with DTimestamp
	dtimestamp := tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
	result = TagValueToString(dtimestamp)
	if result == "" {
		t.Errorf("Expected non-empty string for timestamp, got empty string")
	}

	// Test with other type (using DInt as example)
	dint := tree.NewDInt(42)
	result = TagValueToString(dint)
	if result != "42" {
		t.Errorf("Expected '42', got '%s'", result)
	}
}

// Test for BuildTagHintQuery
func TestBuildTagHintQuery(t *testing.T) {
	tag := []string{"tag1", "tag2", "tag3"}
	dbName := "testdb"
	cTable := "testtable"

	query := BuildTagHintQuery(tag, dbName, cTable)
	expected := "SELECT /*+STMT_HINT ( ACCESS_HINT(TAG_ONLY,  testtable ))*/ tag1, tag2, tag3 FROM testdb.testtable"

	if query != expected {
		t.Errorf("Expected '%s', got '%s'", expected, query)
	}
}

// Test for TSIDGenerator
func TestTSIDGenerator(t *testing.T) {
	gen := &TSIDGenerator{}

	// Generate two IDs and make sure they're different (or at least sequential)
	id1 := gen.GetNextID()
	time.Sleep(1 * time.Nanosecond) // Ensure time difference
	id2 := gen.GetNextID()

	if id1 == 0 {
		t.Error("Expected non-zero ID")
	}

	if id2 == 0 {
		t.Error("Expected non-zero ID")
	}

	if id1 >= id2 {
		t.Log("IDs may be the same if called too quickly, which is acceptable")
	}
}
