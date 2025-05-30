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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// tableComments stores the comment data for a table.
type tableComments struct {
	comment *string
	columns []comment
	indexes []comment
}

type comment struct {
	subID   int
	comment string
}

// selectComment retrieves all the comments pertaining to a table (comments on the table
// itself but also column and index comments.)
func selectComment(ctx context.Context, p PlanHookState, tableID sqlbase.ID) (tc *tableComments) {
	query := fmt.Sprintf(`SELECT type, object_id, sub_id, "comment" FROM system.comments WHERE object_id = %d`, tableID)

	commentRows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "show-tables-with-comment", p.Txn(), query)
	if err != nil {
		log.VEventf(ctx, 1, "%q", err)
	} else {
		for _, row := range commentRows {
			commentType := int(tree.MustBeDInt(row[0]))
			switch commentType {
			case keys.TableCommentType, keys.ColumnCommentType, keys.IndexCommentType:
				subID := int(tree.MustBeDInt(row[2]))
				cmt := string(tree.MustBeDString(row[3]))

				if tc == nil {
					tc = &tableComments{}
				}

				switch commentType {
				case keys.TableCommentType:
					tc.comment = &cmt
				case keys.ColumnCommentType:
					tc.columns = append(tc.columns, comment{subID, cmt})
				case keys.IndexCommentType:
					tc.indexes = append(tc.indexes, comment{subID, cmt})
				}
			}
		}
	}

	return tc
}

// ShowCreateView returns a valid SQL representation of the CREATE VIEW
// statement used to create the given view. It is used in the implementation of
// the kwdb_internal.create_statements virtual table.
func ShowCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	if desc.IsMaterializedView {
		f.WriteString("MATERIALIZED ")
	}
	f.WriteString("VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	for i := range desc.Columns {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&desc.Columns[i].Name)
	}
	f.WriteString(") AS ")
	f.WriteString(desc.ViewQuery)
	return f.CloseAndGetString(), nil
}

// showComments prints out the COMMENT statements sufficient to populate a
// table's comments, including its index and column comments.
func showComments(table *sqlbase.TableDescriptor, tc *tableComments, buf *bytes.Buffer) error {
	if tc == nil {
		return nil
	}
	f := tree.NewFmtCtx(tree.FmtSimple)
	tn := tree.MakeUnqualifiedTableName(tree.Name(table.Name))
	un := tn.ToUnresolvedObjectName()
	if tc.comment != nil {
		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnTable{
			Table:   un,
			Comment: tc.comment,
		})
	}

	for _, columnComment := range tc.columns {
		col, err := table.FindColumnByID(sqlbase.ColumnID(columnComment.subID))
		if err != nil {
			return err
		}

		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnColumn{
			ColumnItem: &tree.ColumnItem{
				TableName:  tn.ToUnresolvedObjectName(),
				ColumnName: tree.Name(col.Name),
			},
			Comment: &columnComment.comment,
		})
	}

	for _, indexComment := range tc.indexes {
		idx, err := table.FindIndexByID(sqlbase.IndexID(indexComment.subID))
		if err != nil {
			return err
		}

		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnIndex{
			Index: tree.TableIndexName{
				Table: tn,
				Index: tree.UnrestrictedName(idx.Name),
			},
			Comment: &indexComment.comment,
		})
	}

	buf.WriteString(f.CloseAndGetString())
	return nil
}

// showForeignKeyConstraint returns a valid SQL representation of a FOREIGN KEY
// clause for a given index.
func showForeignKeyConstraint(
	buf *bytes.Buffer,
	dbPrefix string,
	originTable *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	lCtx *internalLookupCtx,
) error {
	var refNames []string
	var originNames []string
	var fkTableName tree.TableName
	if lCtx != nil {
		fkTable, err := lCtx.getTableByID(fk.ReferencedTableID)
		if err != nil {
			return err
		}
		fkDb, err := lCtx.getDatabaseByID(fkTable.ParentID)
		if err != nil {
			return err
		}
		refNames, err = fkTable.NamesForColumnIDs(fk.ReferencedColumnIDs)
		if err != nil {
			return err
		}
		fkTableName = tree.MakeTableName(tree.Name(fkDb.Name), tree.Name(fkTable.Name))
		fkTableName.ExplicitSchema = fkDb.Name != dbPrefix
		originNames, err = originTable.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return err
		}
	} else {
		refNames = []string{"???"}
		originNames = []string{"???"}
		fkTableName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as ref]", fk.ReferencedTableID)))
		fkTableName.ExplicitSchema = false
	}
	buf.WriteString("FOREIGN KEY (")
	formatQuoteNames(buf, originNames...)
	buf.WriteString(") REFERENCES ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&fkTableName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString("(")
	formatQuoteNames(buf, refNames...)
	buf.WriteByte(')')
	// We omit MATCH SIMPLE because it is the default.
	if fk.Match != sqlbase.ForeignKeyReference_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(fk.Match.String())
	}
	if fk.OnDelete != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(fk.OnUpdate.String())
	}
	return nil
}

// ShowCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func ShowCreateSequence(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.SequenceOpts
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START %d", opts.Start)
	if opts.Virtual {
		f.Printf(" VIRTUAL")
	}
	return f.CloseAndGetString(), nil
}

// showFamilyClause creates the FAMILY clauses for a CREATE statement, writing them
// to tree.FmtCtx f
func showFamilyClause(desc *sqlbase.TableDescriptor, f *tree.FmtCtx) {
	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if col, err := desc.FindActiveColumnByID(colID); err == nil {
				if !desc.IsPrimaryIndexDefaultRowID() && col.IsDefaultPrimaryKeyColumn() {
					continue
				}
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		f.WriteString(",\n\tFAMILY ")
		formatQuoteNames(&f.Buffer, fam.Name)
		f.WriteString(" (")
		formatQuoteNames(&f.Buffer, activeColumnNames...)
		f.WriteString(")")
	}
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func showCreateInterleave(
	idx *sqlbase.IndexDescriptor, buf *bytes.Buffer, dbPrefix string, lCtx *internalLookupCtx,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID
	var err error
	var parentName tree.TableName
	if lCtx != nil {
		parentName, err = lCtx.getParentAsTableName(parentTableID, dbPrefix)
		if err != nil {
			return err
		}
	} else {
		parentName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
		parentName.ExplicitCatalog = false
		parentName.ExplicitSchema = false
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	buf.WriteString(" INTERLEAVE IN PARENT ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&parentName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString(" (")
	formatQuoteNames(buf, idx.ColumnNames[:sharedPrefixLen]...)
	buf.WriteString(")")
	return nil
}

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION BY `)
	if len(partDesc.List) > 0 {
		if partDesc.IsHash {
			buf.WriteString(`HASH`)
		} else {
			buf.WriteString(`LIST`)
		}
	} else if len(partDesc.Range) > 0 {
		buf.WriteString(`RANGE`)
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, partDesc)
	}
	buf.WriteString(` (`)
	for i := 0; i < int(partDesc.NumColumns); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(idxDesc.ColumnNames[colOffset+i])
	}
	buf.WriteString(`) (`)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i := range partDesc.List {
		part := &partDesc.List[i]
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&part.Name)
		_, _ = fmtCtx.Buffer.WriteTo(buf)
		buf.WriteString(` VALUES IN (`)
		for j, values := range part.Values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}
		buf.WriteString(`)`)
		if err := ShowCreatePartitioning(
			a, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
			colOffset+int(partDesc.NumColumns),
		); err != nil {
			return err
		}
	}
	for i, part := range partDesc.Range {
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		buf.WriteString(part.Name)
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
	}
	buf.WriteString("\n")
	buf.WriteString(indentStr)
	buf.WriteString(")")
	return nil
}

// showConstraintClause creates the CONSTRAINT clauses for a CREATE statement,
// writing them to tree.FmtCtx f
func showConstraintClause(desc *sqlbase.TableDescriptor, f *tree.FmtCtx) {
	for _, e := range desc.AllActiveAndInactiveChecks() {
		if e.Hidden {
			continue
		}
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		f.WriteString(e.Expr)
		f.WriteString(")")
	}
	f.WriteString("\n)")
}
