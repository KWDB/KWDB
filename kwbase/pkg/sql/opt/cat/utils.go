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

package cat

import (
	"bytes"
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/treeprinter"
	"github.com/pkg/errors"
)

// ExpandDataSourceGlob is a utility function that expands a tree.TablePattern
// into a list of object names.
func ExpandDataSourceGlob(
	ctx context.Context, catalog Catalog, flags Flags, pattern tree.TablePattern,
) ([]DataSourceName, error) {

	switch p := pattern.(type) {
	case *tree.TableName:
		_, name, err := catalog.ResolveDataSource(ctx, flags, p)
		if err != nil {
			return nil, err
		}
		return []DataSourceName{name}, nil

	case *tree.AllTablesSelector:
		schema, _, err := catalog.ResolveSchema(ctx, flags, &p.TableNamePrefix)
		if err != nil {
			return nil, err
		}

		return schema.GetDataSourceNames(ctx)

	default:
		return nil, errors.Errorf("invalid TablePattern type %T", p)
	}
}

// ResolveTableIndex resolves a TableIndexName.
func ResolveTableIndex(
	ctx context.Context, catalog Catalog, flags Flags, name *tree.TableIndexName,
) (Index, DataSourceName, error) {
	if name.Table.TableName != "" {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			return nil, DataSourceName{}, pgerror.Newf(
				pgcode.WrongObjectType, "%q is not a table", name.Table.TableName,
			)
		}
		if table.IsVirtualTable() {
			return nil, DataSourceName{}, pgerror.Newf(
				pgcode.WrongObjectType, "%q is a virtual table", name.Table.String(),
			)
		}
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), tn, nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, tn, nil
			}
		}
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	schema, _, err := catalog.ResolveSchema(ctx, flags, &name.Table.TableNamePrefix)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	dsNames, err := schema.GetDataSourceNames(ctx)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	var found Index
	var foundTabName DataSourceName
	for i := range dsNames {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			// Not a table, ignore.
			continue
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				if found != nil {
					return nil, DataSourceName{}, pgerror.Newf(pgcode.AmbiguousParameter,
						"index name %q is ambiguous (found in %s and %s)",
						name.Index, tn.String(), foundTabName.String())
				}
				found = idx
				foundTabName = tn
				break
			}
		}
	}
	if found == nil {
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}
	return found, foundTabName, nil
}

// ConvertColumnIDsToOrdinals converts a list of ColumnIDs (such as from a
// tree.TableRef), to a list of ordinal positions of columns within the given
// table. See tree.Table for more information on column ordinals.
func ConvertColumnIDsToOrdinals(tab Table, columns []tree.ColumnID) (ordinals []int) {
	ordinals = make([]int, len(columns))
	for i, c := range columns {
		ord := 0
		cnt := tab.ColumnCount()
		for ord < cnt {
			if tab.Column(ord).ColID() == StableID(c) {
				break
			}
			ord++
		}
		if ord >= cnt {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"column [%d] does not exist", c))
		}
		ordinals[i] = ord
	}
	return ordinals
}

// FindTableColumnByName returns the ordinal of the non-mutation column having
// the given name, if one exists in the given table. Otherwise, it returns -1.
func FindTableColumnByName(tab Table, name tree.Name) int {
	for ord, n := 0, tab.ColumnCount(); ord < n; ord++ {
		if tab.Column(ord).ColName() == name {
			return ord
		}
	}
	return -1
}

// FormatTable nicely formats a catalog table using a treeprinter for debugging
// and testing.
func FormatTable(cat Catalog, tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.Name())
	if tab.IsVirtualTable() {
		child.Child("virtual table")
	}

	var buf bytes.Buffer
	for i := 0; i < tab.DeletableColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), IsMutationColumn(tab, i), &buf)
		child.Child(buf.String())
	}

	// If we only have one primary family (the default), don't print it.
	if tab.FamilyCount() > 1 || tab.Family(0).Name() != "primary" {
		for i := 0; i < tab.FamilyCount(); i++ {
			buf.Reset()
			formatFamily(tab.Family(i), &buf)
			child.Child(buf.String())
		}
	}

	for i := 0; i < tab.CheckCount(); i++ {
		child.Childf("CHECK (%s)", tab.Check(i).Constraint)
	}

	for i := 0; i < tab.DeletableIndexCount(); i++ {
		formatCatalogIndex(tab, i, child)
	}

	for i := 0; i < tab.OutboundForeignKeyCount(); i++ {
		formatCatalogFKRef(cat, false /* inbound */, tab.OutboundForeignKey(i), child)
	}

	for i := 0; i < tab.InboundForeignKeyCount(); i++ {
		formatCatalogFKRef(cat, true /* inbound */, tab.InboundForeignKey(i), child)
	}

	// TODO(radu): show stats.
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(tab Table, ord int, tp treeprinter.Node) {
	idx := tab.Index(ord)
	inverted := ""
	if idx.IsInverted() {
		inverted = "INVERTED "
	}
	mutation := ""
	if IsMutationIndex(tab, ord) {
		mutation = " (mutation)"
	}
	child := tp.Childf("%sINDEX %s%s", inverted, idx.Name(), mutation)

	var buf bytes.Buffer
	colCount := idx.ColumnCount()
	if ord == PrimaryIndex {
		// Omit the "stored" columns from the primary index.
		colCount = idx.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, false /* isMutationCol */, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		child.Child(buf.String())
	}

	FormatZone(idx.Zone(), child)

	partPrefixes := idx.PartitionByListPrefixes()
	if len(partPrefixes) != 0 {
		c := child.Child("partition by list prefixes")
		for i := range partPrefixes {
			c.Child(partPrefixes[i].String())
		}
	}
}

// formatColPrefix returns a string representation of a list of columns. The
// columns are provided through a function.
func formatCols(tab Table, numCols int, colOrdinal func(tab Table, i int) int) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i := 0; i < numCols; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		colName := tab.Column(colOrdinal(tab, i)).ColName()
		buf.WriteString(colName.String())
	}
	buf.WriteByte(')')

	return buf.String()
}

// formatCatalogFKRef nicely formats a catalog foreign key reference using a
// treeprinter for debugging and testing.
func formatCatalogFKRef(
	catalog Catalog, inbound bool, fkRef ForeignKeyConstraint, tp treeprinter.Node,
) {
	originDS, _, err := catalog.ResolveDataSourceByID(context.TODO(), Flags{}, fkRef.OriginTableID())
	if err != nil {
		panic(err)
	}
	refDS, _, err := catalog.ResolveDataSourceByID(context.TODO(), Flags{}, fkRef.ReferencedTableID())
	if err != nil {
		panic(err)
	}
	title := "CONSTRAINT"
	if inbound {
		title = "REFERENCED BY " + title
	}
	var extra bytes.Buffer
	if fkRef.MatchMethod() != tree.MatchSimple {
		fmt.Fprintf(&extra, " %s", fkRef.MatchMethod())
	}

	if action := fkRef.DeleteReferenceAction(); action != tree.NoAction {
		fmt.Fprintf(&extra, " ON DELETE %s", action.String())
	}

	tp.Childf(
		"%s %s FOREIGN KEY %v %s REFERENCES %v %s%s",
		title,
		fkRef.Name(),
		originDS.Name(),
		formatCols(originDS.(Table), fkRef.ColumnCount(), fkRef.OriginColumnOrdinal),
		refDS.Name(),
		formatCols(refDS.(Table), fkRef.ColumnCount(), fkRef.ReferencedColumnOrdinal),
		extra.String(),
	)
}

func formatColumn(col Column, isMutationCol bool, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsComputed() {
		fmt.Fprintf(buf, " as (%s) stored", col.ComputedExprStr())
	}
	if col.HasDefault() {
		fmt.Fprintf(buf, " default (%s)", col.DefaultExprStr())
	}
	if col.IsHidden() {
		fmt.Fprintf(buf, " [hidden]")
	}
	if isMutationCol {
		fmt.Fprintf(buf, " [mutation]")
	}
}

func formatFamily(family Family, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "FAMILY %s (", family.Name())
	for i, n := 0, family.ColumnCount(); i < n; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		col := family.Column(i)
		buf.WriteString(string(col.ColName()))
	}
	buf.WriteString(")")
}
