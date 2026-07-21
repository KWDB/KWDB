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

package ddl

import (
	"bytes"
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TableComments stores the comment data for a table.
type TableComments struct {
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
func selectComment(ctx context.Context, p *GenericPlanner, tableID sqlbase.ID) (tc *TableComments) {
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
					tc = &TableComments{}
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

// showComments prints out the COMMENT statements sufficient to populate a
// table's comments, including its index and column comments.
func showComments(table *TableDescriptor, tc *TableComments, buf *bytes.Buffer) error {
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

// showFamilyClause creates the FAMILY clauses for a CREATE statement, writing them
// to tree.FmtCtx f
func showFamilyClause(desc *TableDescriptor, f *tree.FmtCtx) {
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
		sqlutil.FormatQuoteNames(&f.Buffer, fam.Name)
		f.WriteString(" (")
		sqlutil.FormatQuoteNames(&f.Buffer, activeColumnNames...)
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
	idx *IndexDescriptor, buf *bytes.Buffer, dbPrefix string, lCtx *sql.InternalLookupCtx,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID
	var err error
	var parentName tree.TableName
	if lCtx != nil {
		parentName, err = lCtx.GetParentAsTableName(parentTableID, dbPrefix)
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
	sqlutil.FormatQuoteNames(buf, idx.ColumnNames[:sharedPrefixLen]...)
	buf.WriteString(")")
	return nil
}

// ShowConstraintClause creates the CONSTRAINT clauses for a CREATE statement,
// writing them to tree.FmtCtx f
func ShowConstraintClause(desc *TableDescriptor, f *tree.FmtCtx) {
	for _, e := range desc.AllActiveAndInactiveChecks() {
		if e.Hidden {
			continue
		}
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			sqlutil.FormatQuoteNames(&f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		f.WriteString(e.Expr)
		f.WriteString(")")
	}
	f.WriteString("\n)")
}
