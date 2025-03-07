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

package memo

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// ScalarFmtInterceptor is a callback that can be set to a custom formatting
// function. If the function returns a non-empty string, the normal formatting
// code is bypassed.
var ScalarFmtInterceptor func(f *ExprFmtCtx, expr opt.ScalarExpr) string

// ExprFmtFlags controls which properties of the expression are shown in
// formatted output.
type ExprFmtFlags int

const (
	// ExprFmtShowAll shows all properties of the expression.
	ExprFmtShowAll ExprFmtFlags = 0

	// ExprFmtHideMiscProps does not show outer columns, row cardinality, provided
	// orderings, side effects, or error text in the output.
	ExprFmtHideMiscProps ExprFmtFlags = 1 << (iota - 1)

	// ExprFmtHideConstraints does not show inferred constraints in the output.
	ExprFmtHideConstraints

	// ExprFmtHideFuncDeps does not show functional dependencies in the output.
	ExprFmtHideFuncDeps

	// ExprFmtHideRuleProps does not show rule-specific properties in the output.
	ExprFmtHideRuleProps

	// ExprFmtHideStats does not show statistics in the output.
	ExprFmtHideStats

	// ExprFmtHideCost does not show expression cost in the output.
	ExprFmtHideCost

	// ExprFmtHideQualifications removes the qualification from column labels
	// (except when a shortened name would be ambiguous).
	ExprFmtHideQualifications

	// ExprFmtHideScalars removes subtrees that contain only scalars and replaces
	// them with the SQL expression (if possible).
	ExprFmtHideScalars

	// ExprFmtHidePhysProps hides all required physical properties, except for
	// Presentation (see ExprFmtHideColumns).
	ExprFmtHidePhysProps

	// ExprFmtHideTypes hides type information from columns and scalar
	// expressions.
	ExprFmtHideTypes

	// ExprFmtHideNotNull hides the !null specifier from columns.
	ExprFmtHideNotNull

	// ExprFmtHideColumns removes column information.
	ExprFmtHideColumns

	// ExprFmtHideAll shows only the basic structure of the expression.
	// Note: this flag should be used judiciously, as its meaning changes whenever
	// we add more flags.
	ExprFmtHideAll ExprFmtFlags = (1 << iota) - 1
)

// HasFlags tests whether the given flags are all set.
func (f ExprFmtFlags) HasFlags(subset ExprFmtFlags) bool {
	return f&subset == subset
}

// FormatExpr returns a string representation of the given expression, formatted
// according to the specified flags.
func FormatExpr(e opt.Expr, flags ExprFmtFlags, mem *Memo, catalog cat.Catalog) string {
	if catalog == nil {
		// Automatically hide qualifications if we have no catalog.
		flags |= ExprFmtHideQualifications
	}
	f := MakeExprFmtCtx(flags, mem, catalog)
	f.FormatExpr(e)
	return f.Buffer.String()
}

// ExprFmtCtx is passed as context to expression formatting functions, which
// need to know the formatting flags and memo in order to format. In addition,
// a reusable bytes buffer avoids unnecessary allocations.
type ExprFmtCtx struct {
	Buffer *bytes.Buffer

	// Flags controls how the expression is formatted.
	Flags ExprFmtFlags

	// Memo must contain any expression that is formatted.
	Memo *Memo

	// Catalog must be set unless the ExprFmtHideQualifications flag is set.
	Catalog cat.Catalog

	// nameGen is used to generate a unique name for each relational
	// subexpression when Memo.saveTablesPrefix is non-empty. These names
	// correspond to the tables that would be saved if the query were run
	// with the session variable `save_tables_prefix` set to the same value.
	nameGen *ExprNameGenerator
}

// MakeExprFmtCtx creates an expression formatting context from a new buffer.
func MakeExprFmtCtx(flags ExprFmtFlags, mem *Memo, catalog cat.Catalog) ExprFmtCtx {
	return MakeExprFmtCtxBuffer(&bytes.Buffer{}, flags, mem, catalog)
}

// MakeExprFmtCtxBuffer creates an expression formatting context from an
// existing buffer.
func MakeExprFmtCtxBuffer(
	buf *bytes.Buffer, flags ExprFmtFlags, mem *Memo, catalog cat.Catalog,
) ExprFmtCtx {
	var nameGen *ExprNameGenerator
	if mem != nil && mem.saveTablesPrefix != "" {
		nameGen = NewExprNameGenerator(mem.saveTablesPrefix)
	}
	return ExprFmtCtx{Buffer: buf, Flags: flags, Memo: mem, Catalog: catalog, nameGen: nameGen}
}

// HasFlags tests whether the given flags are all set.
func (f *ExprFmtCtx) HasFlags(subset ExprFmtFlags) bool {
	return f.Flags.HasFlags(subset)
}

// FormatExpr constructs a treeprinter view of the given expression for testing
// and debugging, according to the flags in this context.
func (f *ExprFmtCtx) FormatExpr(e opt.Expr) {
	tp := treeprinter.New()
	f.formatExpr(e, tp)
	f.Buffer.Reset()
	f.Buffer.WriteString(tp.String())
}

func (f *ExprFmtCtx) space() {
	f.Buffer.WriteByte(' ')
}

func (f *ExprFmtCtx) formatExpr(e opt.Expr, tp treeprinter.Node) {
	scalar, ok := e.(opt.ScalarExpr)
	if ok {
		f.formatScalar(scalar, tp)
	} else {
		f.formatRelational(e.(RelExpr), tp)
	}
}

func (f *ExprFmtCtx) formatRelational(e RelExpr, tp treeprinter.Node) {
	md := f.Memo.Metadata()
	relational := e.Relational()
	required := e.RequiredPhysical()
	if required == nil {
		// required can be nil before optimization has taken place.
		required = physical.MinRequired
	}

	// Special cases for merge-join and lookup-join: we want the type of the join
	// to show up first.
	f.Buffer.Reset()
	switch t := e.(type) {
	case *MergeJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (merge)", t.JoinType)

	case *LookupJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (lookup", t.JoinType)
		FormatPrivate(f, e.Private(), required)
		f.Buffer.WriteByte(')')

	case *ZigzagJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (zigzag", opt.InnerJoinOp)
		FormatPrivate(f, e.Private(), required)
		f.Buffer.WriteByte(')')

	case *ScanExpr, *VirtualScanExpr, *IndexJoinExpr, *ShowTraceForSessionExpr,
		*InsertExpr, *UpdateExpr, *UpsertExpr, *DeleteExpr, *SequenceSelectExpr,
		*WindowExpr, *OpaqueRelExpr, *OpaqueMutationExpr, *OpaqueDDLExpr,
		*AlterTableSplitExpr, *AlterTableUnsplitExpr, *AlterTableUnsplitAllExpr,
		*AlterTableRelocateExpr, *ControlJobsExpr, *CancelQueriesExpr,
		*CancelSessionsExpr, *CreateViewExpr, *ExportExpr:
		fmt.Fprintf(f.Buffer, "%v", e.Op())
		FormatPrivate(f, e.Private(), required)

	case *SortExpr:
		if t.InputOrdering.Any() {
			fmt.Fprintf(f.Buffer, "%v", e.Op())
		} else {
			fmt.Fprintf(f.Buffer, "%v (segmented)", e.Op())
		}

	case *WithExpr:
		fmt.Fprintf(f.Buffer, "%v &%d", e.Op(), t.ID)
		if t.Name != "" {
			fmt.Fprintf(f.Buffer, " (%s)", t.Name)
		}

	case *WithScanExpr:
		fmt.Fprintf(f.Buffer, "%v &%d", e.Op(), t.With)
		if t.Name != "" {
			fmt.Fprintf(f.Buffer, " (%s)", t.Name)
		}

	default:
		fmt.Fprintf(f.Buffer, "%v", e.Op())
		if opt.IsJoinNonApplyOp(t) {
			// All join ops that weren't handled above execute as a hash join.
			if leftEqCols, _ := ExtractJoinEqualityColumns(
				e.Child(0).(RelExpr).Relational().OutputCols,
				e.Child(1).(RelExpr).Relational().OutputCols,
				*e.Child(2).(*FiltersExpr),
			); len(leftEqCols) == 0 {
				// The case where there are no equality columns is executed as a
				// degenerate case of hash join; let's be explicit about that.
				f.Buffer.WriteString(" (cross)")
			} else {
				f.Buffer.WriteString(" (hash)")
			}
		}
	}

	tp = tp.Child(f.Buffer.String())

	if f.nameGen != nil {
		name := f.nameGen.GenerateName(e.Op())
		tp.Childf("save-table-name: %s", name)
	}

	var colList opt.ColList
	// Special handling to improve the columns display for certain ops.
	switch t := e.(type) {
	case *ProjectExpr:
		// We want the synthesized column IDs to map 1-to-1 to the projections,
		// and the pass-through columns at the end.

		// Get the list of columns from the ProjectionsOp, which has the natural
		// order.
		for i := range t.Projections {
			colList = append(colList, t.Projections[i].Col)
		}

		// Add pass-through columns.
		t.Passthrough.ForEach(func(i opt.ColumnID) {
			colList = append(colList, i)
		})

	case *ValuesExpr:
		colList = t.Cols

	case *UnionExpr, *IntersectExpr, *ExceptExpr,
		*UnionAllExpr, *IntersectAllExpr, *ExceptAllExpr:
		colList = e.Private().(*SetPrivate).OutCols

	default:
		// Fall back to writing output columns in column id order.
		colList = opt.ColSetToList(e.Relational().OutputCols)
	}

	f.formatColumns(e, tp, colList, required.Presentation)

	switch t := e.(type) {
	// Special-case handling for GroupBy private; print grouping columns
	// and internal ordering in addition to full set of columns.
	case *GroupByExpr, *ScalarGroupByExpr, *DistinctOnExpr, *UpsertDistinctOnExpr:
		private := e.Private().(*GroupingPrivate)
		if !f.HasFlags(ExprFmtHideColumns) && !private.GroupingCols.Empty() {
			f.formatColList(e, tp, "grouping columns:", opt.ColSetToList(private.GroupingCols))
		}
		if !f.HasFlags(ExprFmtHidePhysProps) && !private.Ordering.Any() {
			tp.Childf("internal-ordering: %s", private.Ordering)
		}
		if !f.HasFlags(ExprFmtHideMiscProps) && private.ErrorOnDup {
			tp.Childf("error-on-dup")
		}

	case *LimitExpr:
		if !f.HasFlags(ExprFmtHidePhysProps) && !t.Ordering.Any() {
			tp.Childf("internal-ordering: %s", t.Ordering)
		}

	case *OffsetExpr:
		if !f.HasFlags(ExprFmtHidePhysProps) && !t.Ordering.Any() {
			tp.Childf("internal-ordering: %s", t.Ordering)
		}

	case *Max1RowExpr:
		if !f.HasFlags(ExprFmtHideMiscProps) {
			tp.Childf("error: \"%s\"", t.ErrorText)
		}

	// Special-case handling for set operators to show the left and right
	// input columns that correspond to the output columns.
	case *UnionExpr, *IntersectExpr, *ExceptExpr,
		*UnionAllExpr, *IntersectAllExpr, *ExceptAllExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			private := e.Private().(*SetPrivate)
			f.formatColList(e, tp, "left columns:", private.LeftCols)
			f.formatColList(e, tp, "right columns:", private.RightCols)
		}

	case *ScanExpr:
		if t.IsCanonical() {
			// For the canonical scan, show the expressions attached to the TableMeta.
			tab := md.TableMeta(t.Table)
			if len(tab.Constraints) > 0 {
				c := tp.Childf("check constraint expressions")
				for i := 0; i < len(tab.Constraints); i++ {
					f.formatExpr(tab.Constraints[i], c)
				}
			}
			if len(tab.ComputedCols) > 0 {
				c := tp.Childf("computed column expressions")
				cols := make(opt.ColList, 0, len(tab.ComputedCols))
				for col := range tab.ComputedCols {
					cols = append(cols, col)
				}
				sort.Slice(cols, func(i, j int) bool {
					return cols[i] < cols[j]
				})
				for _, col := range cols {
					f.Buffer.Reset()
					f.formatExpr(tab.ComputedCols[col], c.Child(f.ColumnString(col)))
				}
			}
		}
		if c := t.Constraint; c != nil {
			if c.IsContradiction() {
				tp.Childf("constraint: contradiction")
			} else if c.Spans.Count() == 1 {
				tp.Childf("constraint: %s: %s", c.Columns.String(), c.Spans.Get(0).String())
			} else {
				n := tp.Childf("constraint: %s", c.Columns.String())
				for i := 0; i < c.Spans.Count(); i++ {
					n.Child(c.Spans.Get(i).String())
				}
			}
		}
		if t.HardLimit.IsSet() {
			tp.Childf("limit: %s", t.HardLimit)
		}
		if !t.Flags.Empty() {
			if t.Flags.NoIndexJoin {
				tp.Childf("flags: no-index-join")
			} else if t.Flags.ForceIndex {
				idx := md.Table(t.Table).Index(t.Flags.Index)
				dir := ""
				switch t.Flags.Direction {
				case tree.DefaultDirection:
				case tree.Ascending:
					dir = ",fwd"
				case tree.Descending:
					dir = ",rev"
				}
				tp.Childf("flags: force-index=%s%s", idx.Name(), dir)
			}
		}
		if t.Locking != nil {
			strength := ""
			switch t.Locking.Strength {
			case tree.ForNone:
			case tree.ForKeyShare:
				strength = "for-key-share"
			case tree.ForShare:
				strength = "for-share"
			case tree.ForNoKeyUpdate:
				strength = "for-no-key-update"
			case tree.ForUpdate:
				strength = "for-update"
			default:
				panic(errors.AssertionFailedf("unexpected strength"))
			}
			wait := ""
			switch t.Locking.WaitPolicy {
			case tree.LockWaitBlock:
			case tree.LockWaitSkip:
				wait = ",skip-locked"
			case tree.LockWaitError:
				wait = ",nowait"
			default:
				panic(errors.AssertionFailedf("unexpected wait policy"))
			}
			tp.Childf("locking: %s%s", strength, wait)
		}

	case *LookupJoinExpr:
		if !t.Flags.Empty() {
			tp.Childf("flags: %s", t.Flags.String())
		}
		idxCols := make(opt.ColList, len(t.KeyCols))
		idx := md.Table(t.Table).Index(t.Index)
		for i := range idxCols {
			idxCols[i] = t.Table.ColumnID(idx.Column(i).Ordinal)
		}
		if !f.HasFlags(ExprFmtHideColumns) {
			tp.Childf("key columns: %v = %v", t.KeyCols, idxCols)
		}
		if t.LookupColsAreTableKey {
			tp.Childf("lookup columns are key")
		}

	case *ZigzagJoinExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			tp.Childf("eq columns: %v = %v", t.LeftEqCols, t.RightEqCols)
			leftVals := make([]tree.Datum, len(t.LeftFixedCols))
			rightVals := make([]tree.Datum, len(t.RightFixedCols))
			// FixedVals is always going to be a ScalarListExpr, containing tuples,
			// containing one ScalarListExpr, containing ConstExprs.
			for i := range t.LeftFixedCols {
				leftVals[i] = ExtractConstDatum(t.FixedVals[0].Child(0).Child(i))
			}
			for i := range t.RightFixedCols {
				rightVals[i] = ExtractConstDatum(t.FixedVals[1].Child(0).Child(i))
			}
			tp.Childf("left fixed columns: %v = %v", t.LeftFixedCols, leftVals)
			tp.Childf("right fixed columns: %v = %v", t.RightFixedCols, rightVals)
		}

	case *MergeJoinExpr:
		if !t.Flags.Empty() {
			tp.Childf("flags: %s", t.Flags.String())
		}
		if !f.HasFlags(ExprFmtHidePhysProps) {
			tp.Childf("left ordering: %s", t.LeftEq)
			tp.Childf("right ordering: %s", t.RightEq)
		}

	case *InsertExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			if len(colList) == 0 {
				tp.Child("columns: <none>")
			}
			f.formatMutationCols(e, tp, "insert-mapping:", t.InsertCols, t.Table)
			f.formatColList(e, tp, "check columns:", t.CheckCols)
			f.formatMutationCommon(tp, &t.MutationPrivate)
		}

	case *UpdateExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			if len(colList) == 0 {
				tp.Child("columns: <none>")
			}
			f.formatColList(e, tp, "fetch columns:", t.FetchCols)
			f.formatMutationCols(e, tp, "update-mapping:", t.UpdateCols, t.Table)
			f.formatColList(e, tp, "check columns:", t.CheckCols)
			f.formatMutationCommon(tp, &t.MutationPrivate)
		}

	case *UpsertExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			if len(colList) == 0 {
				tp.Child("columns: <none>")
			}
			if t.CanaryCol != 0 {
				tp.Childf("canary column: %d", t.CanaryCol)
				f.formatColList(e, tp, "fetch columns:", t.FetchCols)
				f.formatMutationCols(e, tp, "insert-mapping:", t.InsertCols, t.Table)
				f.formatMutationCols(e, tp, "update-mapping:", t.UpdateCols, t.Table)
				f.formatMutationCols(e, tp, "return-mapping:", t.ReturnCols, t.Table)
			} else {
				f.formatMutationCols(e, tp, "upsert-mapping:", t.InsertCols, t.Table)
			}
			f.formatColList(e, tp, "check columns:", t.CheckCols)
			f.formatMutationCommon(tp, &t.MutationPrivate)
		}

	case *DeleteExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			if len(colList) == 0 {
				tp.Child("columns: <none>")
			}
			f.formatColList(e, tp, "fetch columns:", t.FetchCols)
			f.formatMutationCommon(tp, &t.MutationPrivate)
		}

	case *WithScanExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			child := tp.Child("mapping:")
			for i := range t.InCols {
				f.Buffer.Reset()
				f.space()
				f.formatCol("" /* label */, t.InCols[i], opt.ColSet{} /* notNullCols */)
				f.Buffer.WriteString(" => ")
				f.formatCol("" /* label */, t.OutCols[i], opt.ColSet{} /* notNullCols */)
				child.Child(f.Buffer.String())
			}
		}

	case *CreateTableExpr:
		tp.Child(t.Syntax.String())

	case *CreateViewExpr:
		tp.Child(t.ViewQuery)

		f.Buffer.Reset()
		f.Buffer.WriteString("columns:")
		for _, col := range t.Columns {
			f.space()
			f.formatCol(col.Alias, col.ID, opt.ColSet{} /* notNullCols */)
		}
		tp.Child(f.Buffer.String())

		n := tp.Child("dependencies")
		for _, dep := range t.Deps {
			f.Buffer.Reset()
			name := dep.DataSource.Name()
			f.Buffer.WriteString(name.String())
			if dep.SpecificIndex {
				fmt.Fprintf(f.Buffer, "@%s", dep.DataSource.(cat.Table).Index(dep.Index).Name())
			}
			if !dep.ColumnOrdinals.Empty() {
				fmt.Fprintf(f.Buffer, " [columns: %s]", dep.ColumnOrdinals)
			}
			n.Child(f.Buffer.String())
		}

	case *ExportExpr:
		tp.Childf("format: %s", t.FileFormat)

	case *ExplainExpr:
		// ExplainPlan is the default, don't show it.
		m := ""
		if t.Options.Mode != tree.ExplainPlan {
			m = strings.ToLower(t.Options.Mode.String())
		}
		if t.Options.Flags[tree.ExplainFlagVerbose] {
			if m != "" {
				m += ", "
			}
			m += "verbose"
		}
		if m != "" {
			tp.Childf("mode: %s", m)
		}

	case *RecursiveCTEExpr:
		if !f.HasFlags(ExprFmtHideColumns) {
			tp.Childf("working table binding: &%d", t.WithID)
			f.formatColList(e, tp, "initial columns:", t.InitialCols)
			f.formatColList(e, tp, "recursive columns:", t.RecursiveCols)
		}

	default:
		if opt.IsJoinOp(t) {
			p := t.Private().(*JoinPrivate)
			if !p.Flags.Empty() {
				tp.Childf("flags: %s", p.Flags.String())
			}
		}
	}

	if !f.HasFlags(ExprFmtHideMiscProps) {
		if !relational.OuterCols.Empty() {
			tp.Childf("outer: %s", relational.OuterCols.String())
		}
		if relational.Cardinality != props.AnyCardinality {
			// Suppress cardinality for Scan ops if it's redundant with Limit field.
			if scan, ok := e.(*ScanExpr); !ok || !scan.HardLimit.IsSet() {
				tp.Childf("cardinality: %s", relational.Cardinality)
			}
		}

		f.Buffer.Reset()
		writeFlag := func(name string) {
			if f.Buffer.Len() != 0 {
				f.Buffer.WriteString(", ")
			}
			f.Buffer.WriteString(name)
		}

		if relational.CanHaveSideEffects {
			writeFlag("side-effects")
		}
		if relational.CanMutate {
			writeFlag("mutations")
		}
		if relational.HasPlaceholder {
			writeFlag("has-placeholder")
		}

		if f.Buffer.Len() != 0 {
			tp.Child(f.Buffer.String())
		}
	}

	if !f.HasFlags(ExprFmtHideStats) {
		tp.Childf("stats: %s", &relational.Stats)
	}

	if !f.HasFlags(ExprFmtHideCost) {
		cost := e.Cost()
		if cost != 0 {
			tp.Childf("cost: %.9g", cost)
		}
	}

	// Format functional dependencies.
	if !f.HasFlags(ExprFmtHideFuncDeps) {
		// Show the key separately from the rest of the FDs.
		if key, ok := relational.FuncDeps.StrictKey(); ok {
			tp.Childf("key: %s", key)
		} else if key, ok := relational.FuncDeps.LaxKey(); ok {
			tp.Childf("lax-key: %s", key)
		}
		if fdStr := relational.FuncDeps.StringOnlyFDs(); fdStr != "" {
			tp.Childf("fd: %s", fdStr)
		}
	}

	if !f.HasFlags(ExprFmtHidePhysProps) {
		if !required.Ordering.Any() {
			if f.HasFlags(ExprFmtHideMiscProps) {
				tp.Childf("ordering: %s", required.Ordering.String())
			} else {
				// Show the provided ordering as well, unless it's exactly the same.
				provided := e.ProvidedPhysical().Ordering
				reqStr := required.Ordering.String()
				provStr := provided.String()
				if provStr == reqStr {
					tp.Childf("ordering: %s", required.Ordering.String())
				} else {
					tp.Childf("ordering: %s [actual: %s]", required.Ordering.String(), provided.String())
				}
			}
		}
		if required.LimitHint != 0 {
			tp.Childf("limit hint: %.2f", required.LimitHint)
		}
	}

	if !f.HasFlags(ExprFmtHideRuleProps) {
		r := &relational.Rule
		if !r.PruneCols.Empty() {
			tp.Childf("prune: %s", r.PruneCols.String())
		}
		if !r.RejectNullCols.Empty() {
			tp.Childf("reject-nulls: %s", r.RejectNullCols.String())
		}
		if len(r.InterestingOrderings) > 0 {
			tp.Childf("interesting orderings: %s", r.InterestingOrderings.String())
		}
		if r.JoinSize > 1 {
			tp.Childf("join-size: %d", r.JoinSize)
		}
		if withUses := relational.Shared.Rule.WithUses; len(withUses) > 0 {
			n := tp.Childf("cte-uses")
			ids := make([]opt.WithID, 0, len(withUses))
			for id := range withUses {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool {
				return ids[i] < ids[j]
			})
			for _, id := range ids {
				info := withUses[id]
				n.Childf("&%d: count=%d used-columns=%s", id, info.Count, info.UsedCols)
			}
		}
	}

	switch t := e.(type) {
	case *CreateTableExpr:
		// Do not print dummy input expression if there was no AS clause.
		if !t.Syntax.As() {
			return
		}
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		f.formatExpr(e.Child(i), tp)
	}
}

func (f *ExprFmtCtx) formatScalar(scalar opt.ScalarExpr, tp treeprinter.Node) {
	switch scalar.Op() {
	case opt.ProjectionsOp, opt.AggregationsOp, opt.FKChecksOp, opt.KVOptionsOp:
		// Omit empty lists (except filters).
		if scalar.ChildCount() == 0 {
			return
		}

	case opt.FiltersOp:
		// Show empty Filters expression as "filters (true)".
		if scalar.ChildCount() == 0 {
			tp.Child("filters (true)")
			return
		}

	case opt.IfErrOp:
		f.Buffer.Reset()
		fmt.Fprintf(f.Buffer, "%v", scalar.Op())
		f.FormatScalarProps(scalar)

		tp = tp.Child(f.Buffer.String())

		f.formatExpr(scalar.Child(0), tp)
		if scalar.Child(1).ChildCount() > 0 {
			f.formatExpr(scalar.Child(1), tp.Child("else"))
		}
		if scalar.Child(2).ChildCount() > 0 {
			f.formatExpr(scalar.Child(2), tp.Child("err-code"))
		}

		return

	case opt.AggFilterOp:
		f.Buffer.Reset()
		fmt.Fprintf(f.Buffer, "%v", scalar.Op())
		f.FormatScalarProps(scalar)
		tp = tp.Child(f.Buffer.String())

		f.formatExpr(scalar.Child(0), tp)
		f.formatExpr(scalar.Child(1), tp.Child("filter"))

		return

	case opt.ScalarListOp:
		// Don't show scalar-list as a separate node, as it's redundant with its
		// parent.
		for i, n := 0, scalar.ChildCount(); i < n; i++ {
			f.formatExpr(scalar.Child(i), tp)
		}
		return
	}

	// Omit various list items from the output, but show some of their properties
	// along with the properties of their child.
	var scalarProps []string
	switch scalar.Op() {
	case opt.FiltersItemOp, opt.ProjectionsItemOp, opt.AggregationsItemOp,
		opt.ZipItemOp, opt.WindowsItemOp:

		emitProp := func(format string, args ...interface{}) {
			scalarProps = append(scalarProps, fmt.Sprintf(format, args...))
		}
		switch item := scalar.(type) {
		case *ProjectionsItem:
			if !f.HasFlags(ExprFmtHideColumns) {
				emitProp("as=%s", f.ColumnString(item.Col))
			}

		case *AggregationsItem:
			if !f.HasFlags(ExprFmtHideColumns) {
				emitProp("as=%s", f.ColumnString(item.Col))
			}

		case *ZipItem:
			// TODO(radu): show the item.Cols

		case *WindowsItem:
			if !f.HasFlags(ExprFmtHideColumns) {
				emitProp("as=%s", f.ColumnString(item.Col))
			}
			// Only show the frame if it differs from the default.
			def := WindowFrame{
				Mode:           tree.RANGE,
				StartBoundType: tree.UnboundedPreceding,
				EndBoundType:   tree.CurrentRow,
				FrameExclusion: tree.NoExclusion,
			}
			if item.Frame != def {
				emitProp("frame=%q", item.Frame.String())
			}
		}

		scalarProps = append(scalarProps, f.scalarPropsStrings(scalar)...)
		scalar = scalar.Child(0).(opt.ScalarExpr)

	default:
		scalarProps = f.scalarPropsStrings(scalar)
	}

	var intercepted bool
	f.Buffer.Reset()
	if f.HasFlags(ExprFmtHideScalars) && ScalarFmtInterceptor != nil {
		if str := ScalarFmtInterceptor(f, scalar); str != "" {
			f.Buffer.WriteString(str)
			intercepted = true
		}
	}
	if !intercepted {
		fmt.Fprintf(f.Buffer, "%v", scalar.Op())
		f.formatScalarPrivate(scalar)
	}
	if len(scalarProps) != 0 {
		f.Buffer.WriteString(" [")
		f.Buffer.WriteString(strings.Join(scalarProps, ", "))
		f.Buffer.WriteByte(']')
	}
	tp = tp.Child(f.Buffer.String())

	if !intercepted {
		for i, n := 0, scalar.ChildCount(); i < n; i++ {
			f.formatExpr(scalar.Child(i), tp)
		}
	}
}

// scalarPropsStrings returns a slice of strings, each describing a property;
// for example:
//   {"type=bool", "outer=(1)", "constraints=(/1: [/1 - /1]; tight)"}
func (f *ExprFmtCtx) scalarPropsStrings(scalar opt.ScalarExpr) []string {
	typ := scalar.DataType()
	if typ == nil {
		if scalar.Op() == opt.FKChecksItemOp || scalar.Op() == opt.KVOptionsItemOp {
			// These are not true scalars and have no properties.
			return nil
		}
		// Don't panic if scalar properties don't yet exist when printing
		// expression.
		return []string{"type=undefined"}
	}

	var res []string
	emitProp := func(format string, args ...interface{}) {
		res = append(res, fmt.Sprintf(format, args...))
	}
	if !f.HasFlags(ExprFmtHideTypes) && typ.Family() != types.AnyFamily {
		emitProp("type=%s", typ)
	}
	if propsExpr, ok := scalar.(ScalarPropsExpr); ok {
		scalarProps := propsExpr.ScalarProps()
		if !f.HasFlags(ExprFmtHideMiscProps) {
			if !scalarProps.OuterCols.Empty() {
				emitProp("outer=%s", scalarProps.OuterCols)
			}
			if scalarProps.CanHaveSideEffects {
				emitProp("side-effects")
			}
			if scalarProps.HasCorrelatedSubquery {
				emitProp("correlated-subquery")
			} else if scalarProps.HasSubquery {
				emitProp("subquery")
			}
		}

		if !f.HasFlags(ExprFmtHideConstraints) {
			if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
				var tight string
				if scalarProps.TightConstraints {
					tight = "; tight"
				}
				emitProp("constraints=(%s%s)", scalarProps.Constraints, tight)
			}
		}

		if !f.HasFlags(ExprFmtHideFuncDeps) && !scalarProps.FuncDeps.Empty() {
			emitProp("fd=%s", scalarProps.FuncDeps)
		}
	}
	return res
}

// FormatScalarProps writes out a string representation of the scalar
// properties (with a preceding space); for example:
//  " [type=bool, outer=(1), constraints=(/1: [/1 - /1]; tight)]"
func (f *ExprFmtCtx) FormatScalarProps(scalar opt.ScalarExpr) {
	props := f.scalarPropsStrings(scalar)
	if len(props) != 0 {
		f.Buffer.WriteString(" [")
		f.Buffer.WriteString(strings.Join(props, ", "))
		f.Buffer.WriteByte(']')
	}
}

func (f *ExprFmtCtx) formatScalarPrivate(scalar opt.ScalarExpr) {
	var private interface{}
	switch t := scalar.(type) {
	case *NullExpr, *TupleExpr, *CollateExpr:
		// Private is redundant with logical type property.
		private = nil

	case *AnyExpr:
		// We don't want to show the OriginalExpr; just show Cmp.
		private = t.Cmp

	case *ArrayFlattenExpr:
		if t.Input.Relational().OutputCols.Len() != 1 {
			fmt.Fprintf(f.Buffer, " col=%v", t.RequestedCol)
		}

	case *SubqueryExpr, *ExistsExpr:
		// We don't want to show the OriginalExpr.
		private = nil

	case *CastExpr:
		private = t.Typ.SQLString()

	case *KVOptionsItem:
		fmt.Fprintf(f.Buffer, " %s", t.Key)

	case *FKChecksItem:
		origin := f.Memo.metadata.TableMeta(t.OriginTable)
		referenced := f.Memo.metadata.TableMeta(t.ReferencedTable)
		var fk cat.ForeignKeyConstraint
		if t.FKOutbound {
			fk = origin.Table.OutboundForeignKey(t.FKOrdinal)
		} else {
			fk = referenced.Table.InboundForeignKey(t.FKOrdinal)
		}
		// Print the FK as:
		//   child(a,b) -> parent(a,b)
		//
		// TODO(radu): maybe flip these if we are deleting from the parent (i.e.
		// FKOutbound=false)?
		fmt.Fprintf(f.Buffer, ": %s(", origin.Alias.TableName)
		for i := 0; i < fk.ColumnCount(); i++ {
			if i > 0 {
				f.Buffer.WriteByte(',')
			}
			col := origin.Table.Column(fk.OriginColumnOrdinal(origin.Table, i))
			f.Buffer.WriteString(string(col.ColName()))
		}
		fmt.Fprintf(f.Buffer, ") -> %s(", referenced.Alias.TableName)
		for i := 0; i < fk.ColumnCount(); i++ {
			if i > 0 {
				f.Buffer.WriteByte(',')
			}
			col := referenced.Table.Column(fk.ReferencedColumnOrdinal(referenced.Table, i))
			f.Buffer.WriteString(string(col.ColName()))
		}
		f.Buffer.WriteByte(')')

	default:
		private = scalar.Private()
	}

	if private != nil {
		f.Buffer.WriteRune(':')
		FormatPrivate(f, private, &physical.Required{})
	}
}

func (f *ExprFmtCtx) formatColumns(
	nd RelExpr, tp treeprinter.Node, cols opt.ColList, presentation physical.Presentation,
) {
	if f.HasFlags(ExprFmtHideColumns) {
		return
	}
	if presentation.Any() {
		f.formatColList(nd, tp, "columns:", cols)
		return
	}

	// When a particular column presentation is required of the expression, then
	// print columns using that information. Include information about columns
	// that are hidden by the presentation separately.
	hidden := cols.ToSet()
	notNullCols := nd.Relational().NotNullCols
	f.Buffer.Reset()
	f.Buffer.WriteString("columns:")
	for _, col := range presentation {
		hidden.Remove(col.ID)
		f.space()
		f.formatCol(col.Alias, col.ID, notNullCols)
	}
	if !hidden.Empty() {
		f.Buffer.WriteString("  [hidden:")
		for _, col := range cols {
			if hidden.Contains(col) {
				f.space()
				f.formatCol("" /* label */, col, notNullCols)
			}
		}
		f.Buffer.WriteString("]")
	}
	tp.Child(f.Buffer.String())
}

// formatColList constructs a new treeprinter child containing the specified
// list of columns formatted using the formatCol method.
func (f *ExprFmtCtx) formatColList(
	nd RelExpr, tp treeprinter.Node, heading string, colList opt.ColList,
) {
	if len(colList) > 0 {
		notNullCols := nd.Relational().NotNullCols
		f.Buffer.Reset()
		f.Buffer.WriteString(heading)
		for _, col := range colList {
			if col != 0 {
				f.space()
				f.formatCol("" /* label */, col, notNullCols)
			}
		}
		tp.Child(f.Buffer.String())
	}
}

// formatMutationCols adds a new treeprinter child for each non-zero column in the
// given list. Each child shows how the column will be mutated, with the id of
// the "before" and "after" columns, similar to this:
//
//   a:1 => x:4
//
func (f *ExprFmtCtx) formatMutationCols(
	nd RelExpr, tp treeprinter.Node, heading string, colList opt.ColList, tabID opt.TableID,
) {
	if len(colList) == 0 {
		return
	}

	tpChild := tp.Child(heading)
	for i, col := range colList {
		if col != 0 {
			tpChild.Child(fmt.Sprintf("%s => %s", f.ColumnString(col), f.ColumnString(tabID.ColumnID(i))))
		}
	}
}

// formatMutationCommon shows the MutationPrivate fields that format the same
// for all types of mutations.
func (f *ExprFmtCtx) formatMutationCommon(tp treeprinter.Node, p *MutationPrivate) {
	if p.WithID != 0 {
		tp.Childf("input binding: &%d", p.WithID)
	}
	if p.FKFallback {
		tp.Childf("fk-fallback")
	}
}

// ColumnString returns the column in the same format as formatColSimple.
func (f *ExprFmtCtx) ColumnString(id opt.ColumnID) string {
	var buf bytes.Buffer
	f.formatColSimpleToBuffer(&buf, "" /* label */, id)
	return buf.String()
}

// formatColSimple outputs the specified column into the context's buffer using the
// following format:
//   label:id
//
// The :id part is omitted if the formatting flags include ExprFmtHideColumns.
//
// If a label is given, then it is used. Otherwise, a "best effort" label is
// used from query metadata.
func (f *ExprFmtCtx) formatColSimple(label string, id opt.ColumnID) {
	f.formatColSimpleToBuffer(f.Buffer, label, id)
}

func (f *ExprFmtCtx) formatColSimpleToBuffer(buf *bytes.Buffer, label string, id opt.ColumnID) {
	if label == "" {
		if f.Memo != nil {
			md := f.Memo.metadata
			fullyQualify := !f.HasFlags(ExprFmtHideQualifications)
			label = md.QualifiedAlias(id, fullyQualify, f.Catalog)
		} else {
			label = fmt.Sprintf("unknown%d", id)
		}
	}

	if !isSimpleColumnName(label) {
		// Add quotations around the column name if it is not composed of simple
		// ASCII characters.
		label = "\"" + label + "\""
	}

	buf.WriteString(label)
	if !f.HasFlags(ExprFmtHideColumns) {
		buf.WriteByte(':')
		fmt.Fprintf(buf, "%d", id)
	}
}

// formatCol outputs the specified column into the context's buffer using the
// following format:
//   label:id(type)
//
// If the column is not nullable, then this is the format:
//   label:id(type!null)
//
// Some of the components can be omitted depending on formatting flags.
//
// If a label is given, then it is used. Otherwise, a "best effort" label is
// used from query metadata.
func (f *ExprFmtCtx) formatCol(label string, id opt.ColumnID, notNullCols opt.ColSet) {
	f.formatColSimple(label, id)
	parenOpen := false
	if !f.HasFlags(ExprFmtHideTypes) && f.Memo != nil {
		f.Buffer.WriteByte('(')
		parenOpen = true
		f.Buffer.WriteString(f.Memo.metadata.ColumnMeta(id).Type.String())
	}
	if !f.HasFlags(ExprFmtHideNotNull) && notNullCols.Contains(id) {
		f.Buffer.WriteString("!null")
	}
	if parenOpen {
		f.Buffer.WriteByte(')')
	}
}

// ScanIsReverseFn is a callback that is used to figure out if a scan needs to
// happen in reverse (the code lives in the ordering package, and depending on
// that directly would be a dependency loop).
var ScanIsReverseFn func(md *opt.Metadata, s *ScanPrivate, required *physical.OrderingChoice) bool

// FormatPrivate outputs a description of the private to f.Buffer.
func FormatPrivate(f *ExprFmtCtx, private interface{}, physProps *physical.Required) {
	if private == nil {
		return
	}
	switch t := private.(type) {
	case *opt.ColumnID:
		f.space()
		f.formatColSimple("" /* label */, *t)

	case *opt.ColList:
		for _, col := range *t {
			f.space()
			f.formatColSimple("" /* label */, col)
		}

	case *TupleOrdinal:
		fmt.Fprintf(f.Buffer, " %d", *t)

	case *ScanPrivate:
		// Don't output name of index if it's the primary index.
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == cat.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tableAlias(f, t.Table))
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tableAlias(f, t.Table), tab.Index(t.Index).Name())
		}
		if ScanIsReverseFn(f.Memo.Metadata(), t, &physProps.Ordering) {
			f.Buffer.WriteString(",rev")
		}

	case *VirtualScanPrivate:
		fmt.Fprintf(f.Buffer, " %s", tableAlias(f, t.Table))

	case *SequenceSelectPrivate:
		seq := f.Memo.metadata.Sequence(t.Sequence)
		fmt.Fprintf(f.Buffer, " %s", seq.Name())

	case *MutationPrivate:
		fmt.Fprintf(f.Buffer, " %s", tableAlias(f, t.Table))

	case *OrdinalityPrivate:
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t.Ordering)
		}

	case *GroupingPrivate:
		fmt.Fprintf(f.Buffer, " cols=%s", t.GroupingCols.String())
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, ",ordering=%s", t.Ordering)
		}

	case *IndexJoinPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		fmt.Fprintf(f.Buffer, " %s", tab.Name())

	case *LookupJoinPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == cat.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tab.Name())
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tab.Name(), tab.Index(t.Index).Name())
		}

	case *ValuesPrivate:
		fmt.Fprintf(f.Buffer, " id=v%d", t.ID)

	case *ZigzagJoinPrivate:
		leftTab := f.Memo.metadata.Table(t.LeftTable)
		rightTab := f.Memo.metadata.Table(t.RightTable)
		fmt.Fprintf(f.Buffer, " %s", leftTab.Name())
		if t.LeftIndex != cat.PrimaryIndex {
			fmt.Fprintf(f.Buffer, "@%s", leftTab.Index(t.LeftIndex).Name())
		}
		fmt.Fprintf(f.Buffer, " %s", rightTab.Name())
		if t.RightIndex != cat.PrimaryIndex {
			fmt.Fprintf(f.Buffer, "@%s", rightTab.Index(t.RightIndex).Name())
		}

	case *MergeJoinPrivate:
		fmt.Fprintf(f.Buffer, " %s,%s,%s", t.JoinType, t.LeftEq, t.RightEq)

	case *FunctionPrivate:
		fmt.Fprintf(f.Buffer, " %s", t.Name)

	case *WindowsItemPrivate:
		fmt.Fprintf(f.Buffer, " frame=%q", &t.Frame)

	case *WindowPrivate:
		fmt.Fprintf(f.Buffer, " partition=%s", t.Partition)
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t.Ordering)
		}

	case *physical.OrderingChoice:
		if !t.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t)
		}

	case *OpaqueRelPrivate:
		f.space()
		f.Buffer.WriteString(t.Metadata.String())

	case *AlterTableSplitPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == cat.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tableAlias(f, t.Table))
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tableAlias(f, t.Table), tab.Index(t.Index).Name())
		}

	case *AlterTableRelocatePrivate:
		FormatPrivate(f, &t.AlterTableSplitPrivate, nil)
		if t.RelocateLease {
			f.Buffer.WriteString(" [lease]")
		}

	case *ControlJobsPrivate:
		fmt.Fprintf(f.Buffer, " (%s)", tree.JobCommandToStatement[t.Command])

	case *CancelPrivate:
		if t.IfExists {
			f.Buffer.WriteString(" [if-exists]")
		}

	case *CreateViewPrivate:
		schema := f.Memo.Metadata().Schema(t.Schema)
		fmt.Fprintf(f.Buffer, " %s.%s", schema.Name(), t.ViewName)

	case *JoinPrivate:
		// Nothing to show; flags are shown separately.

	case *ExplainPrivate, *opt.ColSet, *SetPrivate, *types.T, *ExportPrivate:
		// Don't show anything, because it's mostly redundant.

	default:
		fmt.Fprintf(f.Buffer, " %v", private)
	}
}

// tableAlias returns the alias for a table to be used for pretty-printing.
func tableAlias(f *ExprFmtCtx, tabID opt.TableID) string {
	tabMeta := f.Memo.metadata.TableMeta(tabID)
	if f.HasFlags(ExprFmtHideQualifications) {
		return tabMeta.Alias.String()
	}
	tn, err := f.Catalog.FullyQualifiedName(context.TODO(), tabMeta.Table)
	if err != nil {
		panic(err)
	}
	return tn.FQString()
}

// isSimpleColumnName returns true if the given label consists of only ASCII
// letters, numbers, underscores, quotation marks, and periods ("."). It is
// used to determine whether to enclose a column name in quotation marks for
// nicer display.
func isSimpleColumnName(label string) bool {
	for i, r := range label {
		if r > unicode.MaxASCII {
			return false
		}

		if i == 0 {
			if r != '"' && !unicode.IsLetter(r) {
				// The first character must be a letter or quotation mark.
				return false
			}
		} else if r != '.' && r != '_' && r != '"' && !unicode.IsNumber(r) && !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
