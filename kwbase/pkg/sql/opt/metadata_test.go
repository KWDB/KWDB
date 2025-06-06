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

package opt_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestMetadata(t *testing.T) {
	var md opt.Metadata
	schID := md.AddSchema(&testcat.Schema{})
	colID := md.AddColumn("col", types.Int)
	tabID := md.AddTable(&testcat.Table{}, &tree.TableName{})
	seqID := md.AddSequence(&testcat.Sequence{})
	md.AddView(&testcat.View{})

	// Call Init and add objects from catalog, verifying that IDs have been reset.
	testCat := testcat.New()
	tab := &testcat.Table{Revoked: true}
	testCat.AddTable(tab)

	md.Init()
	if md.AddSchema(testCat.Schema()) != schID {
		t.Fatalf("unexpected schema id")
	}
	if md.AddColumn("col2", types.Int) != colID {
		t.Fatalf("unexpected column id")
	}
	if md.AddTable(tab, &tree.TableName{}) != tabID {
		t.Fatalf("unexpected table id")
	}
	if md.AddSequence(&testcat.Sequence{SeqID: 100}) != seqID {
		t.Fatalf("unexpected sequence id")
	}

	md.AddView(&testcat.View{ViewID: 101})
	if len(md.AllViews()) != 1 {
		t.Fatalf("unexpected views")
	}

	md.AddDependency(opt.DepByName(&tab.TabName), tab, privilege.CREATE)
	depsUpToDate, err := md.CheckDependencies(context.TODO(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked")
	}

	// Call CopyFrom and verify that same objects are present in new metadata.
	expr := &memo.ProjectExpr{}
	md.AddWithBinding(1, expr)
	var mdNew opt.Metadata
	mdNew.CopyFrom(&md)
	if mdNew.Schema(schID) != testCat.Schema() {
		t.Fatalf("unexpected schema")
	}
	if mdNew.ColumnMeta(colID).Alias != "col2" {
		t.Fatalf("unexpected column")
	}

	if mdNew.TableMeta(tabID).Table != tab {
		t.Fatalf("unexpected table")
	}

	if mdNew.Sequence(seqID).(*testcat.Sequence).SeqID != 100 {
		t.Fatalf("unexpected sequence")
	}

	if v := mdNew.AllViews(); len(v) != 1 && v[0].(*testcat.View).ViewID != 101 {
		t.Fatalf("unexpected view")
	}

	depsUpToDate, err = md.CheckDependencies(context.TODO(), testCat)
	if err == nil || depsUpToDate {
		t.Fatalf("expected table privilege to be revoked in metadata copy")
	}

	paniced := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				paniced = true
			}
		}()
		mdNew.WithBinding(1)
	}()
	if !paniced {
		t.Fatalf("with bindings should not be copied!")
	}
}

func TestMetadataSchemas(t *testing.T) {
	var md opt.Metadata

	sch := &testcat.Schema{
		SchemaID:   1,
		SchemaName: cat.SchemaName{CatalogName: "db", SchemaName: "schema"},
	}

	schID := md.AddSchema(sch)
	lookup := md.Schema(schID)
	if lookup.ID() != 1 {
		t.Fatalf("unexpected schema id: %d", lookup.ID())
	}
	if lookup.Name().String() != sch.SchemaName.String() {
		t.Fatalf("unexpected schema name: %s", lookup.Name())
	}
}

func TestMetadataColumns(t *testing.T) {
	var md opt.Metadata

	// Add standalone column.
	colID := md.AddColumn("alias", types.Int)
	if colID != 1 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	colMeta := md.ColumnMeta(colID)
	if colMeta.Alias != "alias" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	if colMeta.Type.Family() != types.IntFamily {
		t.Fatalf("unexpected column type: %s", colMeta.Type)
	}

	if n := md.NumColumns(); n != 1 {
		t.Fatalf("unexpected num columns: %d", n)
	}

	// Add another column.
	colID = md.AddColumn("alias2", types.String)
	if colID != 2 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	colMeta = md.ColumnMeta(colID)
	if colMeta.Alias != "alias2" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	if colMeta.Type.Family() != types.StringFamily {
		t.Fatalf("unexpected column type: %s", colMeta.Type)
	}

	if n := md.NumColumns(); n != 2 {
		t.Fatalf("unexpected num columns: %d", n)
	}
}

func TestMetadataTables(t *testing.T) {
	var md opt.Metadata

	// Add a table reference to the metadata.
	a := &testcat.Table{TabID: 1}
	a.TabName = tree.MakeUnqualifiedTableName(tree.Name("a"))
	x := &testcat.Column{Name: "x"}
	y := &testcat.Column{Name: "y"}
	a.Columns = append(a.Columns, x, y)

	tabID := md.AddTable(a, &tree.TableName{})
	if tabID == 0 {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	tab := md.Table(tabID)
	if tab != a {
		t.Fatal("table didn't match table added to metadata")
	}

	colID := tabID.ColumnID(0)
	if colID == 0 {
		t.Fatalf("unexpected column id: %d", colID)
	}

	colMeta := md.ColumnMeta(colID)
	if colMeta.Alias != "x" {
		t.Fatalf("unexpected column alias: %s", colMeta.Alias)
	}

	// Add another table reference to the metadata.
	b := &testcat.Table{TabID: 1}
	b.TabName = tree.MakeUnqualifiedTableName(tree.Name("b"))
	b.Columns = append(b.Columns, &testcat.Column{Name: "x"})

	otherTabID := md.AddTable(b, &tree.TableName{})
	if otherTabID == tabID {
		t.Fatalf("unexpected table id: %d", tabID)
	}

	alias := md.ColumnMeta(otherTabID.ColumnID(0)).Alias
	if alias != "x" {
		t.Fatalf("unexpected column alias: %s", alias)
	}
}

// TestIndexColumns tests that we can extract a set of columns from an index ordinal.
func TestIndexColumns(t *testing.T) {
	cat := testcat.New()
	_, err := cat.ExecuteDDL(
		"CREATE TABLE a (" +
			"k INT PRIMARY KEY, " +
			"i INT, " +
			"s STRING, " +
			"f FLOAT, " +
			"INDEX (i, k), " +
			"INDEX (s DESC) STORING(f))")
	if err != nil {
		t.Fatal(err)
	}

	var md opt.Metadata
	tn := tree.NewUnqualifiedTableName("a")
	a := md.AddTable(cat.Table(tn), tn)

	k := a.ColumnID(0)
	i := a.ColumnID(1)
	s := a.ColumnID(2)
	f := a.ColumnID(3)

	testCases := []struct {
		index        int
		expectedCols opt.ColSet
	}{
		{1, opt.MakeColSet(k, i)},
		{2, opt.MakeColSet(s, f, k)},
	}

	for _, tc := range testCases {
		actual := md.TableMeta(a).IndexColumns(tc.index)
		if !tc.expectedCols.Equals(actual) {
			t.Errorf("expected %v, got %v", tc.expectedCols, actual)
		}
	}
}
