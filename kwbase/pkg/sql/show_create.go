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
	"bytes"
	"context"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

type shouldOmitFKClausesFromCreate int

const (
	_ shouldOmitFKClausesFromCreate = iota
	// OmitFKClausesFromCreate will not include any foreign key information in the
	// create statement.
	OmitFKClausesFromCreate
	// IncludeFkClausesInCreate will include foreign key information in the create
	// statement, and error if a FK cannot be resolved.
	IncludeFkClausesInCreate
	// OmitMissingFKClausesFromCreate will include foreign key information only if they
	// can be resolved. If not, it will ignore those constraints.
	// This is used in the case when showing the create statement for
	// tables stored in backups. Not all relevant tables may have been
	// included in the back up, so some foreign key information may be
	// impossible to retrieve.
	OmitMissingFKClausesFromCreate
)

// ShowCreateDisplayOptions is a container struct holding the options that
// ShowCreate uses to determine how much information should be included in the
// CREATE statement.
type ShowCreateDisplayOptions struct {
	FKDisplayMode shouldOmitFKClausesFromCreate
	// Comment resolution requires looking up table data from system.comments
	// table. This is sometimes not possible. For example, in the context of a
	// SHOW BACKUP which may resolve the create statement, there is no mechanism
	// to read any table data from the backup (nor is there a guarantee that the
	// system.comments table is included in the backup at all).
	IgnoreComments bool
}

// ShowCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func ShowCreateTable(
	ctx context.Context,
	p PlanHookState,
	tn *tree.Name,
	dbPrefix string,
	desc *sqlbase.TableDescriptor,
	lCtx *internalLookupCtx,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	a := &sqlbase.DatumAlloc{}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	primaryKeyIsOnVisibleColumn := false
	visibleCols := desc.VisibleColumns()
	for i := range visibleCols {
		col := &visibleCols[i]
		if i != 0 {
			f.WriteString(",")
		}
		f.WriteString("\n\t")
		f.WriteString(col.SQLString())
		if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primaryKeyIsOnVisibleColumn to true if the primary key
			// is on a visible column (not rowid).
			primaryKeyIsOnVisibleColumn = true
		}
	}
	if !desc.IsTSTable() {
		if primaryKeyIsOnVisibleColumn ||
			(desc.IsPhysicalTable() && desc.PrimaryIndex.IsSharded()) {
			f.WriteString(",\n\tCONSTRAINT ")
			formatQuoteNames(&f.Buffer, desc.PrimaryIndex.Name)
			f.WriteString(" ")
			f.WriteString(desc.PrimaryKeyString())
		}
		// TODO (lucy): Possibly include FKs in the mutations list here, or else
		// exclude check mutations below, for consistency.
		if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
			for i := range desc.OutboundFKs {
				fkCtx := tree.NewFmtCtx(tree.FmtSimple)
				fk := &desc.OutboundFKs[i]
				fkCtx.WriteString(",\n\tCONSTRAINT ")
				fkCtx.FormatNameP(&fk.Name)
				fkCtx.WriteString(" ")
				if err := showForeignKeyConstraint(&fkCtx.Buffer, dbPrefix, desc, fk, lCtx); err != nil {
					if displayOptions.FKDisplayMode == OmitMissingFKClausesFromCreate {
						continue
					} else { // When FKDisplayMode == IncludeFkClausesInCreate.
						return "", err
					}
				}
				f.WriteString(fkCtx.String())
			}
		}
		allIdx := append(desc.Indexes, desc.PrimaryIndex)
		for i := range allIdx {
			idx := &allIdx[i]
			// Only add indexes to the create_statement column, and not to the
			// create_nofks column if they are not associated with an INTERLEAVE
			// statement.
			// Initialize to false if Interleave has no ancestors, indicating that the
			// index is not interleaved at all.
			includeInterleaveClause := len(idx.Interleave.Ancestors) == 0
			if displayOptions.FKDisplayMode != OmitFKClausesFromCreate {
				// The caller is instructing us to not omit FK clauses from inside the CREATE.
				// (i.e. the caller does not want them as separate DDL.)
				// Since we're including FK clauses, we need to also include the PARTITION and INTERLEAVE
				// clauses as well.
				includeInterleaveClause = true
			}
			if idx.ID != desc.PrimaryIndex.ID && includeInterleaveClause {
				// Showing the primary index is handled above.
				f.WriteString(",\n\t")
				f.WriteString(idx.SQLString(&sqlbase.AnonymousTable))
				// Showing the INTERLEAVE and PARTITION BY for the primary index are
				// handled last.

				// Add interleave or Foreign Key indexes only to the create_table columns,
				// and not the create_nofks column.
				if includeInterleaveClause {
					if err := showCreateInterleave(idx, &f.Buffer, dbPrefix, lCtx); err != nil {
						return "", err
					}
				}
				if err := ShowCreatePartitioning(
					a, desc, idx, &idx.Partitioning, &f.Buffer, 1 /* indent */, 0, /* colOffset */
				); err != nil {
					return "", err
				}
			}
		}

		// Create the FAMILY and CONSTRAINTs of the CREATE statement
		showFamilyClause(desc, f)
		showConstraintClause(desc, f)
	}

	if desc.IsTSTable() {
		f.WriteString("\n)")
		f.WriteString(" TAGS")
		f.WriteString(" (")
		var primaryTags []string
		var tagCount int
		for i := range desc.Columns {
			if desc.Columns[i].IsTagCol() {
				if desc.TableType == tree.TemplateTable && desc.Columns[i].IsPrimaryTagCol() {
					continue
				}
				if tagCount != 0 {
					f.WriteString(",")
				}
				tagCount++
				if desc.Columns[i].IsPrimaryTagCol() {
					primaryTags = append(primaryTags, desc.Columns[i].Name)
				}
				f.WriteString("\n\t")
				f.FormatNameP(&desc.Columns[i].Name)
				f.WriteString(" ")
				typeStr := desc.Columns[i].Type.SQLString()
				f.WriteString(typeStr)
				typ := desc.Columns[i].Type
				// display types with length
				if IsTypeWithLength(typ.Oid()) && !strings.Contains(typeStr, "(") {
					f.WriteString("(")
					f.WriteString(strconv.Itoa(int(typ.InternalType.Width)))
					f.WriteString(")")
				}
				if !desc.Columns[i].Nullable {
					f.WriteString(" NOT NULL")
				}
			}
		}
		f.WriteString(" )")
		if desc.TableType == tree.TimeseriesTable {
			f.WriteString(" ")
			f.WriteString("PRIMARY TAGS")
			f.WriteString("(")
			for i, pt := range primaryTags {
				if i != 0 {
					f.WriteString(", ")
				}
				f.FormatNameP(&pt)
			}
			f.WriteString(")")
		}

		f.WriteString("\n\t")
		f.WriteString("retentions ")
		if len(desc.TsTable.Downsampling) > 0 {
			f.WriteString(desc.TsTable.Downsampling[0])
		} else {
			f.WriteString(strconv.Itoa(int(desc.TsTable.Lifetime)))
			f.WriteString("s")
		}

		f.WriteString("\n\t")
		f.WriteString("activetime ")
		if desc.TsTable.ActiveTimeInput != nil {
			f.WriteString(*desc.TsTable.ActiveTimeInput)
		} else {
			f.WriteString("1d")
		}
		if desc.TsTable.Sde {
			f.WriteString(" DICT ENCODING")
		}
		f.WriteString("\n\t")
		f.WriteString("partition interval ")
		if desc.TsTable.PartitionIntervalInput != nil {
			f.WriteString(*desc.TsTable.PartitionIntervalInput)
		} else {
			f.WriteString(strconv.Itoa(int(desc.TsTable.PartitionInterval / 86400)))
			f.WriteString("d")
		}
	}

	if err := showCreateInterleave(&desc.PrimaryIndex, &f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, &f.Buffer, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	if !displayOptions.IgnoreComments {
		if err := showComments(desc, selectComment(ctx, p, desc.ID), &f.Buffer); err != nil {
			return "", err
		}
	}

	return f.CloseAndGetString(), nil
}

// ShowCreateInstanceTable returns a valid SQL representation of the CREATE
// INSTANCE TABLE statement used to create the given table.
func ShowCreateInstanceTable(
	sTable tree.Name, cTable string, tagName []string, tagValue []string, sde bool, typ []types.T,
) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	f.WriteString("TABLE ")
	f.WriteString(cTable)
	f.WriteString(" USING ")
	f.FormatNode(&sTable)
	f.WriteString(" (")
	for i := range tagName {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")
		f.WriteString(tagName[i])
	}
	f.WriteString(" )")
	f.WriteString(" TAGS")
	f.WriteString(" (")
	for i := range tagValue {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")

		if tagValue[i] != sqlbase.EmptyTagValue && typ[i].InternalType.Family == types.BytesFamily && sqlbase.NeedConvert(tagValue[i]) {
			f.WriteString(tree.NewDBytes(tree.DBytes(strings.Trim(tagValue[i], "'"))).String())
		} else {
			f.WriteString(tagValue[i])
		}
	}
	f.WriteString(" )")
	if sde {
		f.WriteString(" DICT ENCODING")
	}
	return f.CloseAndGetString()
}

// formatQuoteNames quotes and adds commas between names.
func formatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
	buf.WriteString(f.CloseAndGetString())
}

// ShowCreate returns a valid SQL representation of the CREATE
// statement used to create the descriptor passed in. The
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func (p *planner) ShowCreate(
	ctx context.Context,
	dbPrefix string,
	allDescs []sqlbase.Descriptor,
	desc *sqlbase.TableDescriptor,
	displayOptions ShowCreateDisplayOptions,
) (string, error) {
	var stmt string
	var err error
	tn := (*tree.Name)(&desc.Name)
	if desc.IsView() {
		stmt, err = ShowCreateView(ctx, tn, desc)
	} else if desc.IsSequence() {
		stmt, err = ShowCreateSequence(ctx, tn, desc)
	} else {
		lCtx := newInternalLookupCtxFromDescriptors(allDescs, nil /* want all tables */)
		stmt, err = ShowCreateTable(ctx, p, tn, dbPrefix, desc, lCtx, displayOptions)
	}

	return stmt, err
}
