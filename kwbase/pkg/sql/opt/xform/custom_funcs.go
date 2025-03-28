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

package xform

import (
	"fmt"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/idxconstraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/ordering"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by
// the exploration rules. The unnamed xfunc.CustomFuncs allows
// CustomFuncs to provide a clean interface for calling functions from both the
// xform and xfunc packages using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e *explorer
}

const (
	// coefficient for the selectivity comparison adjustment
	ffCompFactor = 10
	// const threshold value of the small result set
	smallRSThreshold = 1000.0
)

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	c.CustomFuncs.Init(e.f)
	c.e = e
}

// GetSettingValues returns values in order to get cluster setting.
func (c *CustomFuncs) GetSettingValues() *settings.Values {
	return &c.e.evalCtx.Settings.SV
}

// ----------------------------------------------------------------------
//
// Scan Rules
//   Custom match and replace functions used with scan.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalScan returns true if the given ScanPrivate is an original
// unaltered primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(scan *memo.ScanPrivate) bool {
	return scan.IsCanonical()
}

// ExploreOrderedTSScan returns true if the given ScanPrivate need explore ordered tsscan
func (c *CustomFuncs) ExploreOrderedTSScan(scan *memo.TSScanPrivate) bool {
	return scan.ExploreOrderedScan
}

// IsLocking returns true if the ScanPrivate is configured to use a row-level
// locking mode. This can be the case either because the Scan is in the scope of
// a SELECT .. FOR [KEY] UPDATE/SHARE clause or because the Scan was configured
// as part of the row retrieval of a DELETE or UPDATE statement.
func (c *CustomFuncs) IsLocking(scan *memo.ScanPrivate) bool {
	return scan.IsLocking()
}

// GenerateIndexScans enumerates all secondary indexes on the given Scan
// operator's table and generates an alternate Scan operator for each index that
// includes the set of needed columns specified in the ScanOpDef.
//
// NOTE: This does not generate index joins for non-covering indexes (except in
//
//	case of ForceIndex). Index joins are usually only introduced "one level
//	up", when the Scan operator is wrapped by an operator that constrains
//	or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//	Index joins are only lower cost when their input does not include all
//	rows from the table. See ConstrainScans and LimitScans for cases where
//	index joins are introduced into the memo.
func (c *CustomFuncs) GenerateIndexScans(grp memo.RelExpr, scanPrivate *memo.ScanPrivate) {
	// Iterate over all secondary indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// Skip primary index.
		if iter.indexOrdinal == cat.PrimaryIndex {
			continue
		}

		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			scan := memo.ScanExpr{ScanPrivate: *scanPrivate}
			scan.Index = iter.indexOrdinal
			c.e.mem.AddScanToGroup(&scan, grp)
			continue
		}

		// Otherwise, if the index must be forced, then construct an IndexJoin
		// operator that provides the columns missing from the index. Note that
		// if ForceIndex=true, scanIndexIter only returns the one index that is
		// being forced, so no need to check that here.
		if !scanPrivate.Flags.ForceIndex &&
			!(scanPrivate.Flags.HintType == keys.ForceIndexScan) &&
			!(scanPrivate.Flags.HintType == keys.UseIndexScan) &&
			!(scanPrivate.Flags.HintType == keys.ForceIndexOnly) &&
			!(scanPrivate.Flags.HintType == keys.UseIndexOnly) {
			continue
		}

		var sb indexScanBuilder
		sb.init(c, scanPrivate.Table)

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		sb.addIndexJoin(scanPrivate.Cols)
		sb.build(grp)
	}
}

// GenerateOrderedTSScans create sort two type for ordered table
func (c *CustomFuncs) GenerateOrderedTSScans(grp memo.RelExpr, scanPrivate *memo.TSScanPrivate) {
	if scanPrivate.ExploreOrderedScan {
		var sb orderedTSScanBuilder
		sb.init(c, *scanPrivate)
		sb.build(grp)
	}
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// GenerateConstrainedScans enumerates all secondary indexes on the Scan
// operator's table and tries to push the given Select filter into new
// constrained Scan operators using those indexes. Since this only needs to be
// done once per table, GenerateConstrainedScans should only be called on the
// original unaltered primary index Scan operator (i.e. not constrained or
// limited).
//
// For each secondary index that "covers" the columns needed by the scan, there
// are three cases:
//
//   - a filter that can be completely converted to a constraint over that index
//     generates a single constrained Scan operator (to be added to the same
//     group as the original Select operator):
//
//     (Scan $scanDef)
//
//   - a filter that can be partially converted to a constraint over that index
//     generates a constrained Scan operator in a new memo group, wrapped in a
//     Select operator having the remaining filter (to be added to the same group
//     as the original Select operator):
//
//     (Select (Scan $scanDef) $filter)
//
//   - a filter that cannot be converted to a constraint generates nothing
//
// And for a secondary index that does not cover the needed columns:
//
//   - a filter that can be completely converted to a constraint over that index
//     generates a single constrained Scan operator in a new memo group, wrapped
//     in an IndexJoin operator that looks up the remaining needed columns (and
//     is added to the same group as the original Select operator)
//
//     (IndexJoin (Scan $scanDef) $indexJoinDef)
//
//   - a filter that can be partially converted to a constraint over that index
//     generates a constrained Scan operator in a new memo group, wrapped in an
//     IndexJoin operator that looks up the remaining needed columns; the
//     remaining filter is distributed above and/or below the IndexJoin,
//     depending on which columns it references:
//
//     (IndexJoin
//     (Select (Scan $scanDef) $filter)
//     $indexJoinDef
//     )
//
//     (Select
//     (IndexJoin (Scan $scanDef) $indexJoinDef)
//     $filter
//     )
//
//     (Select
//     (IndexJoin
//     (Select (Scan $scanDef) $innerFilter)
//     $indexJoinDef
//     )
//     $outerFilter
//     )
//
// GenerateConstrainedScans will further constrain the enumerated index scans
// by trying to use the check constraints and computed columns that apply to the
// table being scanned, as well as the partitioning defined for the index. See
// comments above checkColumnFilters, computedColFilters, and
// partitionValuesFilters for more detail.
func (c *CustomFuncs) GenerateConstrainedScans(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, explicitFilters memo.FiltersExpr,
) {
	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Generate implicit filters from constraints and computed columns and add
	// them to the list of explicit filters provided in the query.
	optionalFilters := c.checkConstraintFilters(scanPrivate.Table)
	computedColFilters := c.computedColFilters(scanPrivate.Table, explicitFilters, optionalFilters)
	optionalFilters = append(optionalFilters, computedColFilters...)

	filterColumns := c.FilterOuterCols(explicitFilters)
	filterColumns.UnionWith(c.FilterOuterCols(optionalFilters))

	// Iterate over all indexes.
	var iter scanIndexIter
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(scanPrivate.Table)
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// We only consider the partition values when a particular index can otherwise
		// not be constrained. For indexes that are constrained, the partitioned values
		// add no benefit as they don't really constrain anything.
		// Furthermore, if the filters don't take advantage of the index (use any of the
		// index columns), using the partition values add no benefit.
		//
		// If the index is partitioned (by list), we generate two constraints and
		// union them: the "main" constraint and the "in-between" constraint.The
		// "main" constraint restricts the index to the known partition ranges. The
		// "in-between" constraint restricts the index to the rest of the ranges
		// (i.e. everything that falls in-between the main ranges); the in-between
		// constraint is necessary for correctness (there can be rows outside of the
		// partitioned ranges).
		//
		// For both constraints, the partition-related filters are passed as
		// "optional" which guarantees that they return no remaining filters. This
		// allows us to merge the remaining filters from both constraints.
		//
		// Consider the following index and its partition:
		//
		// CREATE INDEX orders_by_seq_num
		//     ON orders (region, seq_num, id)
		//     STORING (total)
		//     PARTITION BY LIST (region)
		//         (
		//             PARTITION us_east1 VALUES IN ('us-east1'),
		//             PARTITION us_west1 VALUES IN ('us-west1'),
		//             PARTITION europe_west2 VALUES IN ('europe-west2')
		//         )
		//
		// The constraint generated for the query:
		//   SELECT sum(total) FROM orders WHERE seq_num >= 100 AND seq_num < 200
		// is:
		//   [/'europe-west2'/100 - /'europe-west2'/199]
		//   [/'us-east1'/100 - /'us-east1'/199]
		//   [/'us-west1'/100 - /'us-west1'/199]
		//
		// The spans before europe-west2, after us-west1 and in between the defined
		// partitions are missing. We must add these spans now, appropriately
		// constrained using the filters.
		//
		// It is important that we add these spans after the partition spans are generated
		// because otherwise these spans would merge with the partition spans and would
		// disallow the partition spans (and the in between ones) to be constrained further.
		// Using the partitioning example and the query above, if we added the in between
		// spans at the same time as the partitioned ones, we would end up with a span that
		// looked like:
		//   [ - /'europe-west2'/99]
		//
		// Allowing the partition spans to be constrained further and then adding
		// the spans give us a more constrained index scan as shown below:
		//   [ - /'europe-west2')
		//   [/'europe-west2'/100 - /'europe-west2'/199]
		//   [/e'europe-west2\x00'/100 - /'us-east1')
		//   [/'us-east1'/100 - /'us-east1'/199]
		//   [/e'us-east1\x00'/100 - /'us-west1')
		//   [/'us-west1'/100 - /'us-west1'/199]
		//   [/e'us-west1\x00'/100 - ]
		//
		// Notice how we 'skip' all the europe-west2 rows with seq_num < 100.
		//
		var partitionFilters, inBetweenFilters memo.FiltersExpr

		indexColumns := tabMeta.IndexKeyColumns(iter.indexOrdinal)
		firstIndexCol := scanPrivate.Table.ColumnID(iter.index.Column(0).Ordinal)
		if !filterColumns.Contains(firstIndexCol) && indexColumns.Intersects(filterColumns) {
			// Calculate any partition filters if appropriate (see below).
			partitionFilters, inBetweenFilters = c.partitionValuesFilters(scanPrivate.Table, iter.index)
		}

		// Check whether the filter (along with any partitioning filters) can constrain the index.
		constriction, remainingFilters, ok := c.tryConstrainIndex(
			explicitFilters,
			append(optionalFilters, partitionFilters...),
			scanPrivate.Table,
			iter.indexOrdinal,
			false, /* isInverted */
		)
		if !ok {
			continue
		}

		if len(partitionFilters) > 0 {
			inBetweenConstraint, inBetweenRemainingFilters, ok := c.tryConstrainIndex(
				explicitFilters,
				append(optionalFilters, inBetweenFilters...),
				scanPrivate.Table,
				iter.indexOrdinal,
				false, /* isInverted */
			)
			if !ok {
				panic(errors.AssertionFailedf("in-between filters didn't yield a constraint"))
			}

			constriction.UnionWith(c.e.evalCtx, inBetweenConstraint)

			// Even though the partitioned constraints and the inBetween constraints
			// were consolidated, we must make sure their Union is as well.
			constriction.ConsolidateSpans(c.e.evalCtx)

			// Add all remaining filters that need to be present in the
			// inBetween spans. Some of the remaining filters are common
			// between them, so we must deduplicate them.
			remainingFilters = c.ConcatFilters(remainingFilters, inBetweenRemainingFilters)
			remainingFilters.Sort()
			remainingFilters.Deduplicate()
		}

		convertedfilters := make(memo.FiltersExpr, 0)

		find := true
		for _, filter := range explicitFilters {
			for _, remain := range remainingFilters {
				if filter.Condition != remain.Condition {
					continue
				} else {
					find = false
					break
				}
			}
			if find == true {
				convertedfilters = append(convertedfilters, filter)
			} else {
				find = true
			}
		}

		// Construct new constrained ScanPrivate.
		newScanPrivate := *scanPrivate
		//collect
		if constriction.Filters == nil {
			constriction.Filters = make(constraint.FiltersExpr, len(convertedfilters))
		}
		for i, convertedfilter := range convertedfilters {
			constriction.Filters[i] = constraint.FiltersItem{Condition: convertedfilter.Condition}
		}

		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Constraint = constriction
		// Record whether we were able to use partitions to constrain the scan.
		newScanPrivate.PartitionConstrainedScan = (len(partitionFilters) > 0)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(&newScanPrivate)

			// If there are remaining filters, then the constrained Scan operator
			// will be created in a new group, and a Select operator will be added
			// to the same group as the original operator.
			sb.addSelect(remainingFilters)

			sb.build(grp)
			continue
		}

		// Otherwise, construct an IndexJoin operator that provides the columns
		// missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remainingFilters = sb.addSelectAfterSplit(remainingFilters, newScanPrivate.Cols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remainingFilters)

		sb.build(grp)
	}
}

// checkConstraintFilters generates all filters that we can derive from the
// check constraints. These are constraints that have been validated and are
// non-nullable. We only use non-nullable check constraints because they
// behave differently from filters on NULL. Check constraints are satisfied
// when their expression evaluates to NULL, while filters are not.
//
// For example, the check constraint a > 1 is satisfied if a is NULL but the
// equivalent filter a > 1 is not.
//
// These filters do not really filter any rows, they are rather facts or
// guarantees about the data but treating them as filters may allow some
// indexes to be constrained and used. Consider the following example:
//
// CREATE TABLE abc (
//
//	a INT PRIMARY KEY,
//	b INT NOT NULL,
//	c STRING NOT NULL,
//	CHECK (a < 10 AND a > 1),
//	CHECK (b < 10 AND b > 1),
//	CHECK (c in ('first', 'second')),
//	INDEX secondary (b, a),
//	INDEX tertiary (c, b, a))
//
// Now consider the query: SELECT a, b WHERE a > 5
//
// Notice that the filter provided previously wouldn't let the optimizer use
// the secondary or tertiary indexes. However, given that we can use the
// constraints on a, b and c, we can actually use the secondary and tertiary
// indexes. In fact, for the above query we can do the following:
//
// select
//
//	├── columns: a:1(int!null) b:2(int!null)
//	├── scan abc@tertiary
//	│		├── columns: a:1(int!null) b:2(int!null)
//	│		└── constraint: /3/2/1: [/'first'/2/6 - /'first'/9/9] [/'second'/2/6 - /'second'/9/9]
//	└── filters
//	      └── gt [type=bool]
//	          ├── variable: a [type=int]
//	          └── const: 5 [type=int]
//
// Similarly, the secondary index could also be used. All such index scans
// will be added to the memo group.
func (c *CustomFuncs) checkConstraintFilters(tabID opt.TableID) memo.FiltersExpr {
	md := c.e.mem.Metadata()
	tab := md.Table(tabID)
	tabMeta := md.TableMeta(tabID)

	// Maintain a ColSet of non-nullable columns.
	var notNullCols opt.ColSet
	for i := 0; i < tab.ColumnCount(); i++ {
		if !tab.Column(i).IsNullable() {
			notNullCols.Add(tabID.ColumnID(i))
		}
	}

	checkFilters := make(memo.FiltersExpr, 0, len(tabMeta.Constraints))
	for _, checkConstraint := range tabMeta.Constraints {
		// Check constraints that are guaranteed to not evaluate to NULL
		// are the only ones converted into filters.
		if memo.ExprIsNeverNull(checkConstraint, notNullCols) {
			checkFilters = append(checkFilters, c.e.f.ConstructFiltersItem(checkConstraint))
		}
	}

	return checkFilters
}

// computedColFilters generates all filters that can be derived from the list of
// computed column expressions from the given table. A computed column can be
// used as a filter when it has a constant value. That is true when:
//
//  1. All other columns it references are constant, because other filters in
//     the query constrain them to be so.
//  2. All functions in the computed column expression can be folded into
//     constants (i.e. they do not have problematic side effects).
//
// Note that computed columns can depend on other computed columns; in general
// the dependencies form an acyclic directed graph. computedColFilters will
// return filters for all constant computed columns, regardless of the order of
// their dependencies.
//
// As with checkConstraintFilters, computedColFilters do not really filter any
// rows, they are rather facts or guarantees about the data. Treating them as
// filters may allow some indexes to be constrained and used. Consider the
// following example:
//
//	CREATE TABLE t (
//	  k INT NOT NULL,
//	  hash INT AS (k % 4) STORED,
//	  PRIMARY KEY (hash, k)
//	)
//
//	SELECT * FROM t WHERE k = 5
//
// Notice that the filter provided explicitly wouldn't allow the optimizer to
// seek using the primary index (it would have to fall back to a table scan).
// However, column "hash" can be proven to have the constant value of 1, since
// it's dependent on column "k", which has the constant value of 5. This enables
// usage of the primary index:
//
//	scan t
//	 ├── columns: k:1(int!null) hash:2(int!null)
//	 ├── constraint: /2/1: [/1/5 - /1/5]
//	 ├── key: (2)
//	 └── fd: ()-->(1)
//
// The values of both columns in that index are known, enabling a single value
// constraint to be generated.
func (c *CustomFuncs) computedColFilters(
	tabID opt.TableID, requiredFilters, optionalFilters memo.FiltersExpr,
) memo.FiltersExpr {
	tabMeta := c.e.mem.Metadata().TableMeta(tabID)
	if len(tabMeta.ComputedCols) == 0 {
		return nil
	}

	// Start with set of constant columns, as derived from the list of filter
	// conditions.
	constCols := make(map[opt.ColumnID]opt.ScalarExpr)
	c.findConstantFilterCols(constCols, tabID, requiredFilters)
	c.findConstantFilterCols(constCols, tabID, optionalFilters)
	if len(constCols) == 0 {
		// No constant values could be derived from filters, so assume that there
		// are also no constant computed columns.
		return nil
	}

	// Construct a new filter condition for each computed column that is
	// constant (i.e. all of its variables are in the constCols set).
	var computedColFilters memo.FiltersExpr
	for colID := range tabMeta.ComputedCols {
		if c.tryFoldComputedCol(tabMeta, colID, constCols) {
			constVal := constCols[colID]
			// Note: Eq is not correct here because of NULLs.
			eqOp := c.e.f.ConstructIs(c.e.f.ConstructVariable(colID), constVal)
			computedColFilters = append(computedColFilters, c.e.f.ConstructFiltersItem(eqOp))
		}
	}
	return computedColFilters
}

// findConstantFilterCols adds to constFilterCols mappings from table column ID
// to the constant value of that column. It does this by iterating over the
// given lists of filters and finding expressions that constrain columns to a
// single constant value. For example:
//
//	x = 5 AND y = 'foo'
//
// This would add a mapping from x => 5 and y => 'foo', which constants can
// then be used to prove that dependent computed columns are also constant.
func (c *CustomFuncs) findConstantFilterCols(
	constFilterCols map[opt.ColumnID]opt.ScalarExpr, tabID opt.TableID, filters memo.FiltersExpr,
) {
	tab := c.e.mem.Metadata().Table(tabID)
	for i := range filters {
		// If filter constraints are not tight, then no way to derive constant
		// values.
		props := filters[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}

		// Iterate over constraint conjuncts with a single column and single
		// span having a single key.
		for i, n := 0, props.Constraints.Length(); i < n; i++ {
			cons := props.Constraints.Constraint(i)
			if cons.Columns.Count() != 1 || cons.Spans.Count() != 1 {
				continue
			}

			// Skip columns with a data type that uses a composite key encoding.
			// Each of these data types can have multiple distinct values that
			// compare equal. For example, 0 == -0 for the FLOAT data type. It's
			// not safe to treat these as constant inputs to computed columns,
			// since the computed expression may differentiate between the
			// different forms of the same value.
			colID := cons.Columns.Get(0).ID()
			colTyp := tab.Column(tabID.ColumnOrdinal(colID)).DatumType()
			if sqlbase.DatumTypeHasCompositeKeyEncoding(colTyp) {
				continue
			}

			span := cons.Spans.Get(0)
			if !span.HasSingleKey(c.e.evalCtx) {
				continue
			}

			datum := span.StartKey().Value(0)
			if datum != tree.DNull {
				constFilterCols[colID] = c.e.f.ConstructConstVal(datum, colTyp)
			}
		}
	}
}

// tryFoldComputedCol tries to reduce the computed column with the given column
// ID into a constant value, by evaluating it with respect to a set of other
// columns that are constant. If the computed column is constant, enter it into
// the constCols map and return false. Otherwise, return false.
func (c *CustomFuncs) tryFoldComputedCol(
	tabMeta *opt.TableMeta, computedColID opt.ColumnID, constCols map[opt.ColumnID]opt.ScalarExpr,
) bool {
	// Check whether computed column has already been folded.
	if _, ok := constCols[computedColID]; ok {
		return true
	}

	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		if variable, ok := e.(*memo.VariableExpr); ok {
			// Can variable be folded?
			if constVal, ok := constCols[variable.Col]; ok {
				// Yes, so replace it with its constant value.
				return constVal
			}

			// No, but that may be because the variable refers to a dependent
			// computed column. In that case, try to recursively fold that
			// computed column. There are no infinite loops possible because the
			// dependency graph is guaranteed to be acyclic.
			if _, ok := tabMeta.ComputedCols[variable.Col]; ok {
				if c.tryFoldComputedCol(tabMeta, variable.Col, constCols) {
					return constCols[variable.Col]
				}
			}

			return e
		}
		return c.e.f.Replace(e, replace)
	}

	computedCol := tabMeta.ComputedCols[computedColID]
	replaced := replace(computedCol).(opt.ScalarExpr)

	// If the computed column is constant, enter it into the constCols map.
	if opt.IsConstValueOp(replaced) {
		constCols[computedColID] = replaced
		return true
	}
	return false
}

// inBetweenFilters returns a set of filters that are required to cover all the
// in-between spans given a set of partition values. This is required for
// correctness reasons; although values are unlikely to exist between defined
// partitions, they may exist and so the constraints of the scan must incorporate
// these spans.
//
// For example, if we have:
//
//	PARTITION BY LIST (a, b) (
//	  PARTITION a VALUES IN ((1, 10)),
//	  PARTITION b VALUES IN ((2, 20)),
//	)
//
// The in-between filters are:
//
//	(a, b) < (1, 10) OR
//	((a, b) > (1, 10) AND (a, b) < (2, 20)) OR
//	(a, b) > (2, 20)
//
// When passed as optional filters to index constrains, these filters generate
// the desired spans:
//
//	[ - /1/10), (/1/10 - /2/20), (2/20 - ].
//
// TODO(radu,mgartner): technically these filters are not correct with respect
// to NULL values - we would want the tuple comparisons to treat NULLs as the
// smallest value. We compensate for this by adding an (a IS NULL) disjunct if
// column `a` is nullable; in addition, we know that these filters will only be
// used for span generation, and the span generation currently doesn't exclude
// NULLs on any columns other than the first in the tuple. This is fragile and
// would break if we improve the span generation for tuple inequalities.
func (c *CustomFuncs) inBetweenFilters(
	tabID opt.TableID, index cat.Index, partitionValues []tree.Datums,
) memo.FiltersExpr {
	var inBetween memo.ScalarListExpr

	if len(partitionValues) == 0 {
		return memo.EmptyFiltersExpr
	}

	// Sort the partitionValues lexicographically.
	sort.Slice(partitionValues, func(i, j int) bool {
		return partitionValues[i].Compare(c.e.evalCtx, partitionValues[j]) < 0
	})

	// The beginExpr created below will not include NULL values for the first
	// column. For example, with an index on (a, b) and partition values of
	// {'foo', 'bar'}, beginExpr would be (a, b) < ('foo', 'bar') which
	// evaluates to NULL if a is NULL. The index constraint span generated for
	// beginExpr expression would be (/NULL - /'foo'/'bar'), which excludes NULL
	// values for a. Any rows where a is NULL would not be scanned, producing
	// incorrect query results.
	//
	// Therefore, if the first column in the index is nullable, we add an IS
	// NULL expression so that NULL values are included in the index constraint
	// span generated later on. In the same example above, the constraint span
	// above becomes [/NULL - /'foo'/'bar'), which includes NULL values for a.
	//
	// This is only required for the first column in the index because the index
	// constraint spans built for tuple inequalities only exclude NULL values
	// for the first element in the tuple.
	firstCol := index.Column(0)
	if firstCol.IsNullable() {
		nullExpr := c.e.f.ConstructIs(
			c.e.f.ConstructVariable(tabID.ColumnID(firstCol.Ordinal)),
			memo.NullSingleton,
		)
		inBetween = append(inBetween, nullExpr)
	}

	// Add the beginning span.
	beginExpr := c.columnComparison(tabID, index, partitionValues[0], -1)
	inBetween = append(inBetween, beginExpr)

	// Add the end span.
	endExpr := c.columnComparison(tabID, index, partitionValues[len(partitionValues)-1], 1)
	inBetween = append(inBetween, endExpr)

	// Add the in-between spans.
	for i := 1; i < len(partitionValues); i++ {
		lowerPartition := partitionValues[i-1]
		higherPartition := partitionValues[i]

		// The between spans will be greater than the lower partition but smaller
		// than the higher partition.
		var largerThanLower opt.ScalarExpr
		if c.isPrefixOf(lowerPartition, higherPartition) {

			// Since the lower partition is a prefix of the higher partition, the span
			// must begin with the values defined in the lower partition. Consider the
			// partitions ('us') and ('us', 'cali'). In this case the in-between span
			// should be [/'us - /'us'/'cali').
			largerThanLower = c.columnComparison(tabID, index, lowerPartition, 0)
		} else {
			largerThanLower = c.columnComparison(tabID, index, lowerPartition, 1)
		}

		smallerThanHigher := c.columnComparison(tabID, index, higherPartition, -1)

		// Add the in-between span to the list of inBetween spans.
		betweenExpr := c.e.f.ConstructAnd(largerThanLower, smallerThanHigher)
		inBetween = append(inBetween, betweenExpr)
	}

	// Return an Or expression between all the expressions.
	return memo.FiltersExpr{c.e.f.ConstructFiltersItem(c.constructOr(inBetween))}
}

// columnComparison returns a filter that compares the index columns to the
// given values. The comp parameter can be -1, 0 or 1 to indicate whether the
// comparison type of the filter should be a Lt, Eq or Gt.
func (c *CustomFuncs) columnComparison(
	tabID opt.TableID, index cat.Index, values tree.Datums, comp int,
) opt.ScalarExpr {
	colTypes := make([]types.T, len(values))
	for i := range values {
		colTypes[i] = *values[i].ResolvedType()
	}

	columnVariables := make(memo.ScalarListExpr, len(values))
	scalarValues := make(memo.ScalarListExpr, len(values))

	for i, val := range values {
		colID := tabID.ColumnID(index.Column(i).Ordinal)
		columnVariables[i] = c.e.f.ConstructVariable(colID)
		scalarValues[i] = c.e.f.ConstructConstVal(val, val.ResolvedType())
	}

	colsTuple := c.e.f.ConstructTuple(columnVariables, types.MakeTuple(colTypes))
	valsTuple := c.e.f.ConstructTuple(scalarValues, types.MakeTuple(colTypes))
	if comp == 0 {
		return c.e.f.ConstructEq(colsTuple, valsTuple)
	} else if comp > 0 {
		return c.e.f.ConstructGt(colsTuple, valsTuple)
	}

	return c.e.f.ConstructLt(colsTuple, valsTuple)
}

// inPartitionFilters returns a FiltersExpr that is required to cover
// all the partition spans. For each partition defined, inPartitionFilters
// will contain a FilterItem that restricts the index columns by
// the partition values. Use inBetweenFilters to generate filters that
// cover all the spans that the partitions don't cover.
func (c *CustomFuncs) inPartitionFilters(
	tabID opt.TableID, index cat.Index, partitionValues []tree.Datums,
) memo.FiltersExpr {
	var partitions memo.ScalarListExpr

	// Sort the partition values so the most selective ones are first.
	sort.Slice(partitionValues, func(i, j int) bool {
		return len(partitionValues[i]) >= len(partitionValues[j])
	})

	// Construct all the partition filters.
	for i, partition := range partitionValues {

		// Only add this partition if a more selective partition hasn't
		// been defined on the same partition.
		partitionSeen := false
		for j, moreSelectivePartition := range partitionValues {
			if j >= i {
				break
			}

			// At this point we know whether the current partition was seen before.
			partitionSeen = c.isPrefixOf(partition, moreSelectivePartition)
			if partitionSeen {
				break
			}
		}

		// This partition is a prefix of a more selective partition and so,
		// will be taken care of by the in-between partitions.
		if partitionSeen {
			continue
		}

		// Get an expression that restricts the values of the index to the
		// partition values.
		inPartition := c.columnComparison(tabID, index, partition, 0)
		partitions = append(partitions, inPartition)
	}

	// Return an Or expression between all the expressions.
	return memo.FiltersExpr{c.e.f.ConstructFiltersItem(c.constructOr(partitions))}
}

// isPrefixOf returns whether pre is a prefix of other.
func (c *CustomFuncs) isPrefixOf(pre []tree.Datum, other []tree.Datum) bool {
	if len(pre) > len(other) {
		// Pre can't be a prefix of other as it is larger.
		return false
	}
	for i := range pre {
		if pre[i].Compare(c.e.evalCtx, other[i]) != 0 {
			return false
		}
	}

	return true
}

// constructOr constructs an expression that is an OR between all the
// provided conditions
func (c *CustomFuncs) constructOr(conditions memo.ScalarListExpr) opt.ScalarExpr {
	if len(conditions) == 0 {
		return c.e.f.ConstructFalse()
	}

	orExpr := conditions[0]
	for i := 1; i < len(conditions); i++ {
		orExpr = c.e.f.ConstructOr(conditions[i], orExpr)
	}

	return orExpr
}

// partitionValuesFilters constructs filters with the purpose of
// constraining an index scan using the partition values similar to
// the filters added from the check constraints (see
// checkConstraintFilters). It returns two sets of filters, one to
// create the partition spans, and one to create the spans for all
// the in between ranges that are not part of any partitions.
//
// For example consider the following table and partitioned index:
//
// CREATE TABLE orders (
//
//	region STRING NOT NULL, id INT8 NOT NULL, total DECIMAL NOT NULL, seq_num INT NOT NULL,
//	PRIMARY KEY (region, id)
//
// )
//
// CREATE INDEX orders_by_seq_num
//
//	ON orders (region, seq_num, id)
//	STORING (total)
//	PARTITION BY LIST (region)
//	    (
//	        PARTITION us_east1 VALUES IN ('us-east1'),
//	        PARTITION us_west1 VALUES IN ('us-west1'),
//	        PARTITION europe_west2 VALUES IN ('europe-west2')
//	    )
//
// Now consider the following query:
// SELECT sum(total) FROM orders WHERE seq_num >= 100 AND seq_num < 200
//
// Normally, the index would not be utilized but because we know what the
// partition values are for the prefix of the index, we can generate
// filters that allow us to use the index (adding the appropriate in-between
// filters to catch all the values that are not part of the partitions).
// By doing so, we get the following plan:
// scalar-group-by
//
//	├── select
//	│    ├── scan orders@orders_by_seq_num
//	│    │    └── constraint: /1/4/2: [ - /'europe-west2')
//	│    │                            [/'europe-west2'/100 - /'europe-west2'/199]
//	│    │                            [/e'europe-west2\x00'/100 - /'us-east1')
//	│    │                            [/'us-east1'/100 - /'us-east1'/199]
//	│    │                            [/e'us-east1\x00'/100 - /'us-west1')
//	│    │                            [/'us-west1'/100 - /'us-west1'/199]
//	│    │                            [/e'us-west1\x00'/100 - ]
//	│    └── filters
//	│         └── (seq_num >= 100) AND (seq_num < 200)
//	└── aggregations
//	     └── sum
//	          └── variable: total
func (c *CustomFuncs) partitionValuesFilters(
	tabID opt.TableID, index cat.Index,
) (partitionFilter, inBetweenFilter memo.FiltersExpr) {

	// Find all the partition values
	partitionValues := index.PartitionByListPrefixes()
	if len(partitionValues) == 0 {
		return partitionFilter, inBetweenFilter
	}

	// Get the in partition expressions.
	inPartition := c.inPartitionFilters(tabID, index, partitionValues)

	// Get the in between expressions.
	inBetween := c.inBetweenFilters(tabID, index, partitionValues)

	return inPartition, inBetween
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(scanPrivate *memo.ScanPrivate) bool {
	// Don't bother matching unless there's an inverted index.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	return iter.nextInverted()
}

// GenerateInvertedIndexScans enumerates all inverted indexes on the Scan
// operator's table and generates an alternate Scan operator for each inverted
// index that can service the query.
//
// The resulting Scan operator is pre-constrained and requires an IndexJoin to
// project columns other than the primary key columns. The reason it's pre-
// constrained is that we cannot treat an inverted index in the same way as a
// regular index, since it does not actually contain the indexed column.
func (c *CustomFuncs) GenerateInvertedIndexScans(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.nextInverted() {
		// Check whether the filter can constrain the index.
		constraint, remaining, ok := c.tryConstrainIndex(
			filters, nil /* optioanlFilters */, scanPrivate.Table, iter.indexOrdinal, true /* isInverted */)
		if !ok {
			continue
		}

		// Construct new ScanOpDef with the new index and constraint.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal
		newScanPrivate.Constraint = constraint

		// Though the index is marked as containing the JSONB column being
		// indexed, it doesn't actually, and it's only valid to extract the
		// primary key columns from it.
		newScanPrivate.Cols = sb.primaryKeyCols()

		// The Scan operator always goes in a new group, since it's always nested
		// underneath the IndexJoin. The IndexJoin may also go into its own group,
		// if there's a remaining filter above it.
		// TODO(justin): We might not need to do an index join in order to get the
		// correct columns, but it's difficult to tell at this point.
		sb.setScan(&newScanPrivate)

		// If remaining filter exists, split it into one part that can be pushed
		// below the IndexJoin, and one part that needs to stay above.
		remaining = sb.addSelectAfterSplit(remaining, newScanPrivate.Cols)
		sb.addIndexJoin(scanPrivate.Cols)
		sb.addSelect(remaining)

		sb.build(grp)
	}
}

func (c *CustomFuncs) initIdxConstraintForIndex(
	requiredFilters, optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	indexOrd int,
	isInverted bool,
) (ic *idxconstraint.Instance) {
	ic = &idxconstraint.Instance{}

	// Fill out data structures needed to initialize the idxconstraint library.
	// Use LaxKeyColumnCount, since all columns <= LaxKeyColumnCount are
	// guaranteed to be part of each row's key (i.e. not stored in row's value,
	// which does not take part in an index scan). Note that the OrderingColumn
	// slice cannot be reused, as Instance.Init can use it in the constraint.
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)
	columns := make([]opt.OrderingColumn, index.LaxKeyColumnCount())
	var notNullCols opt.ColSet
	for i := range columns {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal)
		columns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}

	// Generate index constraints.
	ic.Init(requiredFilters, optionalFilters, columns, notNullCols, isInverted, c.e.evalCtx, c.e.f)
	return ic
}

// tryConstrainIndex tries to derive a constraint for the given index from the
// specified filter. If a constraint is derived, it is returned along with any
// filter remaining after extracting the constraint. If no constraint can be
// derived, then tryConstrainIndex returns ok = false.
func (c *CustomFuncs) tryConstrainIndex(
	requiredFilters, optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	indexOrd int,
	isInverted bool,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	// Start with fast check to rule out indexes that cannot be constrained.
	if !isInverted &&
		!c.canMaybeConstrainIndex(requiredFilters, tabID, indexOrd) &&
		!c.canMaybeConstrainIndex(optionalFilters, tabID, indexOrd) {
		return nil, nil, false
	}

	ic := c.initIdxConstraintForIndex(requiredFilters, optionalFilters, tabID, indexOrd, isInverted)
	constraint = ic.Constraint()
	if constraint.IsUnconstrained() {
		return nil, nil, false
	}

	// Return 0 if no remaining filter.
	remaining := ic.RemainingFilters()

	// Make copy of constraint so that idxconstraint instance is not referenced.
	copy := *constraint
	return &copy, remaining, true
}

// allInvIndexConstraints tries to derive all constraints for the specified inverted
// index that can be derived. If no constraint is derived, then it returns ok = false,
// similar to tryConstrainIndex.
func (c *CustomFuncs) allInvIndexConstraints(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) (constraints []*constraint.Constraint, ok bool) {
	ic := c.initIdxConstraintForIndex(filters, nil /* optionalFilters */, tabID, indexOrd, true /* isInverted */)
	constraints, err := ic.AllInvertedIndexConstraints()
	if err != nil {
		return nil, false
	}
	// As long as there was no error, AllInvertedIndexConstraints is guaranteed
	// to add at least one constraint to the slice. It will be set to
	// unconstrained if no constraints could be derived for this index.
	constraint := constraints[0]
	if constraint.IsUnconstrained() {
		return constraints, false
	}

	return constraints, true
}

// canMaybeConstrainIndex performs two checks that can quickly rule out the
// possibility that the given index can be constrained by the specified filter:
//
//  1. If the filter does not reference the first index column, then no
//     constraint can be generated.
//  2. If none of the filter's constraints start with the first index column,
//     then no constraint can be generated.
func (c *CustomFuncs) canMaybeConstrainIndex(
	filters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) bool {
	md := c.e.mem.Metadata()
	index := md.Table(tabID).Index(indexOrd)

	for i := range filters {
		filterProps := filters[i].ScalarProps()

		// If the filter involves the first index column, then the index can
		// possibly be constrained.
		firstIndexCol := tabID.ColumnID(index.Column(0).Ordinal)
		if filterProps.OuterCols.Contains(firstIndexCol) {
			return true
		}

		// If the constraints are not tight, then the index can possibly be
		// constrained, because index constraint generation supports more
		// expressions than filter constraint generation.
		if !filterProps.TightConstraints {
			return true
		}

		// If any constraint involves the first index column, then the index can
		// possibly be constrained.
		cset := filterProps.Constraints
		for i := 0; i < cset.Length(); i++ {
			firstCol := cset.Constraint(i).Columns.Get(0).ID()
			if firstCol == firstIndexCol {
				return true
			}
		}
	}

	return false
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// LimitScanPrivate constructs a new ScanPrivate value that is based on the
// given ScanPrivate. The new private's HardLimit is set to the given limit,
// which must be a constant int datum value. The other fields are inherited from
// the existing private.
func (c *CustomFuncs) LimitScanPrivate(
	scanPrivate *memo.ScanPrivate, limit tree.Datum, required physical.OrderingChoice,
) *memo.ScanPrivate {
	// Determine the scan direction necessary to provide the required ordering.
	_, reverse := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)

	newScanPrivate := *scanPrivate
	newScanPrivate.HardLimit = memo.MakeScanLimit(int64(*limit.(*tree.DInt)), reverse)
	return &newScanPrivate
}

// CanLimitConstrainedScan returns true if the given scan has already been
// constrained and can have a row count limit installed as well. This is only
// possible when the required ordering of the rows to be limited can be
// satisfied by the Scan operator.
//
// NOTE: Limiting unconstrained scans is done by the PushLimitIntoScan rule,
//
//	since that can require IndexJoin operators to be generated.
func (c *CustomFuncs) CanLimitConstrainedScan(
	scanPrivate *memo.ScanPrivate, required physical.OrderingChoice,
) bool {
	if scanPrivate.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This would
		// usually only happen when normalizations haven't run, as otherwise
		// redundant Limit operators would be discarded.
		return false
	}

	if scanPrivate.Constraint == nil {
		// This is not a constrained scan, so skip it. The PushLimitIntoScan rule
		// is responsible for limited unconstrained scans.
		return false
	}

	ok, _ := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)
	return ok
}

// GenerateLimitedScans enumerates all secondary indexes on the Scan operator's
// table and tries to create new limited Scan operators from them. Since this
// only needs to be done once per table, GenerateLimitedScans should only be
// called on the original unaltered primary index Scan operator (i.e. not
// constrained or limited).
//
// For a secondary index that "covers" the columns needed by the scan, a single
// limited Scan operator is created. For a non-covering index, an IndexJoin is
// constructed to add missing columns to the limited Scan.
func (c *CustomFuncs) GenerateLimitedScans(
	grp memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	limit tree.Datum,
	required physical.OrderingChoice,
) {
	limitVal := int64(*limit.(*tree.DInt))

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all indexes, looking for those that can be limited.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = iter.indexOrdinal

		// If the alternate index does not conform to the ordering, then skip it.
		// If reverse=true, then the scan needs to be in reverse order to match
		// the required ordering.
		ok, reverse := ordering.ScanPrivateCanProvide(
			c.e.mem.Metadata(), &newScanPrivate, &required,
		)
		if !ok {
			continue
		}
		newScanPrivate.HardLimit = memo.MakeScanLimit(limitVal, reverse)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if iter.isCovering() {
			sb.setScan(&newScanPrivate)
			sb.build(grp)
			continue
		}

		// Otherwise, try to construct an IndexJoin operator that provides the
		// columns missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = iter.indexCols().Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.addIndexJoin(scanPrivate.Cols)

		sb.build(grp)
	}
}

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// NoJoinHints returns true if no hints were specified for this join.
func (c *CustomFuncs) NoJoinHints(p *memo.JoinPrivate) bool {
	return p.Flags.Empty()
}

// NoCommuteJoinHints determine if there is a hint
func (c *CustomFuncs) NoCommuteJoinHints(p *memo.JoinPrivate) bool {
	return !p.HintInfo.FromHintTree || p.HintInfo.FromHintTree && !p.HintInfo.RealOrder && !p.HintInfo.LeadingTable
}

// NoJoinHintsAndLeadingTable determine if there is a hint
func (c *CustomFuncs) NoJoinHintsAndLeadingTable(p *memo.JoinPrivate) bool {
	return (p.Flags.Empty() && !p.HintInfo.FromHintTree) ||
		(p.HintInfo.FromHintTree && !p.HintInfo.RealOrder && !p.HintInfo.LeadingTable)
}

// CommuteJoinFlags returns a join private for the commuted join (where the left
// and right sides are swapped). It adjusts any join flags that are specific to
// one side.
func (c *CustomFuncs) CommuteJoinFlags(p *memo.JoinPrivate) *memo.JoinPrivate {
	if p.Flags.Empty() {
		return p
	}

	// swap is a helper function which swaps the values of two (single-bit) flags.
	swap := func(f, a, b memo.JoinFlags) memo.JoinFlags {
		// If the bits are different, flip them both.
		if f.Has(a) != f.Has(b) {
			f ^= (a | b)
		}
		return f
	}
	f := p.Flags
	f = swap(f, memo.AllowLookupJoinIntoLeft, memo.AllowLookupJoinIntoRight)
	f = swap(f, memo.AllowHashJoinStoreLeft, memo.AllowHashJoinStoreRight)
	if p.Flags == f {
		return p
	}
	res := *p
	res.Flags = f
	res.HintInfo = p.HintInfo
	return &res
}

// GenerateMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) GenerateMergeJoins(
	grp memo.RelExpr,
	originalOp opt.Operator,
	left, right memo.RelExpr,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if !joinPrivate.Flags.Has(memo.AllowMergeJoin) {
		return
	}

	// DisallowMergeJoin
	if joinPrivate.HintInfo.DisallowMergeJoin {
		return
	}

	leftProps := left.Relational()
	rightProps := right.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftProps.OutputCols, rightProps.OutputCols, on,
	)
	n := len(leftEq)
	if n == 0 {
		if joinPrivate.HintInfo.DisallowHashJoin && joinPrivate.HintInfo.DisallowLookupJoin && joinPrivate.HintInfo.FromHintTree {
			panic(pgerror.New(pgcode.ExternalApplyMergeJoin, "stmt hint err: error use hint of merge_join: the length of lefteq is zero."))
		}
		return
	}

	// We generate MergeJoin expressions based on interesting orderings from the
	// left side. The CommuteJoin rule will ensure that we actually try both
	// sides.
	orders := DeriveInterestingOrderings(left).Copy()
	orders.RestrictToCols(leftEq.ToSet())

	if (!joinPrivate.HintInfo.FromHintTree &&
		!joinPrivate.Flags.Has(memo.AllowHashJoinStoreLeft) &&
		!joinPrivate.Flags.Has(memo.AllowHashJoinStoreRight)) ||
		(joinPrivate.HintInfo.FromHintTree && joinPrivate.HintInfo.DisallowHashJoin) {
		// If we don't allow hash join, we must do our best to generate a merge
		// join, even if it means sorting both sides. We append an arbitrary
		// ordering, in case the interesting orderings don't result in any merge
		// joins.
		o := make(opt.Ordering, len(leftEq))
		for i := range o {
			o[i] = opt.MakeOrderingColumn(leftEq[i], false /* descending */)
		}
		orders.Add(o)
	}

	if len(orders) == 0 {
		return
	}

	var colToEq util.FastIntMap
	for i := range leftEq {
		colToEq.Set(int(leftEq[i]), i)
		colToEq.Set(int(rightEq[i]), i)
	}

	var remainingFilters memo.FiltersExpr

	for _, o := range orders {
		if len(o) < n {
			// TODO(radu): we have a partial ordering on the equality columns. We
			// should augment it with the other columns (in arbitrary order) in the
			// hope that we can get the full ordering cheaply using a "streaming"
			// sort. This would not useful now since we don't support streaming sorts.
			continue
		}

		if remainingFilters == nil {
			remainingFilters = memo.ExtractRemainingJoinFilters(on, leftEq, rightEq)
		}

		merge := memo.MergeJoinExpr{Left: left, Right: right, On: remainingFilters}
		merge.JoinPrivate = *joinPrivate
		merge.JoinType = originalOp
		merge.LeftEq = make(opt.Ordering, n)
		merge.RightEq = make(opt.Ordering, n)
		merge.LeftOrdering.Columns = make([]physical.OrderingColumnChoice, 0, n)
		merge.RightOrdering.Columns = make([]physical.OrderingColumnChoice, 0, n)
		for i := 0; i < n; i++ {
			eqIdx, _ := colToEq.Get(int(o[i].ID()))
			l, r, descending := leftEq[eqIdx], rightEq[eqIdx], o[i].Descending()
			merge.LeftEq[i] = opt.MakeOrderingColumn(l, descending)
			merge.RightEq[i] = opt.MakeOrderingColumn(r, descending)
			merge.LeftOrdering.AppendCol(l, descending)
			merge.RightOrdering.AppendCol(r, descending)
		}

		// Simplify the orderings with the corresponding FD sets.
		merge.LeftOrdering.Simplify(&leftProps.FuncDeps)
		merge.RightOrdering.Simplify(&rightProps.FuncDeps)

		c.e.mem.AddMergeJoinToGroup(&merge, grp)
	}
}

// GenerateLookupJoins looks at the possible indexes and creates lookup join
// expressions in the current group. A lookup join can be created when the ON
// condition has equality constraints on a prefix of the index columns.
//
// There are two cases:
//
//  1. The index has all the columns we need; this is the simple case, where we
//     generate a LookupJoin expression in the current group:
//
//     Join                       LookupJoin(t@idx))
//     /   \                           |
//     /     \            ->            |
//     Input  Scan(t)                   Input
//
//  2. The index is not covering. We have to generate an index join above the
//     lookup join. Note that this index join is also implemented as a
//     LookupJoin, because an IndexJoin can only output columns from one table,
//     whereas we also need to output columns from Input.
//
//     Join                       LookupJoin(t@primary)
//     /   \                           |
//     /     \            ->            |
//     Input  Scan(t)                LookupJoin(t@idx)
//     |
//     |
//     Input
//
//     For example:
//     CREATE TABLE abc (a PRIMARY KEY, b INT, c INT)
//     CREATE TABLE xyz (x PRIMARY KEY, y INT, z INT, INDEX (y))
//     SELECT * FROM abc JOIN xyz ON a=y
//
//     We want to first join abc with the index on y (which provides columns y, x)
//     and then use a lookup join to retrieve column z. The "index join" (top
//     LookupJoin) will produce columns a,b,c,x,y; the lookup columns are just z
//     (the original index join produced x,y,z).
//
//     Note that the top LookupJoin "sees" column IDs from the table on both
//     "sides" (in this example x,y on the left and z on the right) but there is
//     no overlap.
func (c *CustomFuncs) GenerateLookupJoins(
	grp memo.RelExpr,
	joinType opt.Operator,
	input memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	on memo.FiltersExpr,
	joinPrivate *memo.JoinPrivate,
) {
	if !joinPrivate.Flags.Has(memo.AllowLookupJoinIntoRight) {
		return
	}
	// DisallowLookupJoin
	if joinPrivate.HintInfo.DisallowLookupJoin {
		return
	}

	md := c.e.mem.Metadata()
	inputProps := input.Relational()

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(inputProps.OutputCols, scanPrivate.Cols, on)
	n := len(leftEq)
	if n == 0 {
		if joinPrivate.HintInfo.DisallowHashJoin && joinPrivate.HintInfo.DisallowMergeJoin && joinPrivate.HintInfo.FromHintTree {
			panic(pgerror.New(pgcode.ExternalApplyLookupJoin, "stmt hint err: error use hint of lookup_join: the length of lefteq is zero."))
		}
		return
	}

	var pkCols opt.ColList

	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		// LookupJoin with given index
		if joinPrivate.HintInfo.FromHintTree && joinPrivate.HintInfo.DisallowHashJoin && joinPrivate.HintInfo.DisallowMergeJoin &&
			joinPrivate.HintInfo.HintIndex >= 0 && iter.indexOrdinal != joinPrivate.HintInfo.HintIndex {
			continue
		}

		idxCols := iter.indexCols()

		// Find the longest prefix of index key columns that are constrained by
		// an equality with another column or a constant.
		numIndexKeyCols := iter.index.LaxKeyColumnCount()
		constValMap := memo.ExtractValuesFromFilter(on, idxCols)
		constFilterMap := memo.ExtractConstantFilter(on, idxCols)

		var projections memo.ProjectionsExpr
		var constFilters memo.FiltersExpr
		if len(constValMap) > 0 {
			projections = make(memo.ProjectionsExpr, 0, numIndexKeyCols)
			constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols)
		}

		// Check if the first column in the index has an equality constraint, or if
		// it is constrained to a constant value. This check doesn't guarantee that
		// we will find lookup join key columns, but it avoids the unnecessary work
		// in most cases.
		firstIdxCol := scanPrivate.Table.ColumnID(iter.index.Column(0).Ordinal)
		if _, ok := rightEq.Find(firstIdxCol); !ok {
			if _, ok := constValMap[firstIdxCol]; !ok {
				continue
			}
		}

		lookupJoin := memo.LookupJoinExpr{Input: input}
		lookupJoin.JoinPrivate = *joinPrivate
		lookupJoin.JoinType = joinType
		lookupJoin.Table = scanPrivate.Table
		lookupJoin.Index = iter.indexOrdinal

		lookupJoin.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
		needProjection := false

		// All the lookup conditions must apply to the prefix of the index and so
		// the projected columns created must be created in order.
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanPrivate.Table.ColumnID(iter.index.Column(j).Ordinal)
			if eqIdx, ok := rightEq.Find(idxCol); ok {
				lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
				rightSideCols = append(rightSideCols, idxCol)
				continue
			}

			// Project a new column with a constant value if that allows the
			// index column to be constrained and used by the lookup joiner.
			filter, ok := constFilterMap[idxCol]
			if !ok {
				break
			}
			condition, ok := filter.Condition.(*memo.EqExpr)
			if !ok {
				break
			}
			idxColType := c.e.f.Metadata().ColumnMeta(idxCol).Type
			constColID := c.e.f.Metadata().AddColumn(
				fmt.Sprintf("project_const_col_@%d", idxCol),
				idxColType,
			)
			projections = append(projections, c.e.f.ConstructProjectionsItem(
				c.e.f.ConstructConst(constValMap[idxCol], condition.Right.DataType()),
				constColID,
			))

			needProjection = true
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, constColID)
			rightSideCols = append(rightSideCols, idxCol)
			constFilters = append(constFilters, filter)
		}

		if len(lookupJoin.KeyCols) == 0 {
			// We couldn't find equality columns which we can lookup.
			continue
		}

		tableFDs := memo.MakeTableFuncDep(md, scanPrivate.Table)
		// A lookup join will drop any input row which contains NULLs, so a lax key
		// is sufficient.
		lookupJoin.LookupColsAreTableKey = tableFDs.ColsAreLaxKey(rightSideCols.ToSet())

		// Construct the projections for the constant columns.
		if needProjection {
			lookupJoin.Input = c.e.f.ConstructProject(input, projections, input.Relational().OutputCols)
		}

		// Remove the redundant filters and update the lookup condition.
		lookupJoin.On = memo.ExtractRemainingJoinFilters(on, lookupJoin.KeyCols, rightSideCols)
		lookupJoin.On.RemoveCommonFilters(constFilters)
		lookupJoin.ConstFilters = constFilters

		if iter.isCovering() {
			// Case 1 (see function comment).
			lookupJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
			c.e.mem.AddLookupJoinToGroup(&lookupJoin, grp)
			continue
		}

		// All code that follows is for case 2 (see function comment).

		if scanPrivate.Flags.NoIndexJoin {
			continue
		}
		if joinType == opt.SemiJoinOp || joinType == opt.AntiJoinOp {
			// We cannot use a non-covering index for semi and anti join. Note that
			// since the semi/anti join doesn't pass through any columns, "non
			// covering" here means that not all columns in the ON condition are
			// available.
			//
			// TODO(radu): We could create a semi/anti join on top of an inner join if
			// the lookup columns form a key (to guarantee that input rows are not
			// duplicated by the inner join).
			continue
		}

		if pkCols == nil {
			pkIndex := iter.tab.Index(cat.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			}
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		indexCols := iter.indexCols()
		lookupJoin.Cols = scanPrivate.Cols.Intersection(indexCols)
		for i := range pkCols {
			lookupJoin.Cols.Add(pkCols[i])
		}
		lookupJoin.Cols.UnionWith(inputProps.OutputCols)

		var indexJoin memo.LookupJoinExpr

		// onCols are the columns that the ON condition in the (lower) lookup join
		// can refer to: input columns, or columns available in the index.
		onCols := indexCols.Union(inputProps.OutputCols)
		if c.FiltersBoundBy(lookupJoin.On, onCols) {
			// The ON condition refers only to the columns available in the index.
			//
			// For LeftJoin, both LookupJoins perform a LeftJoin. A null-extended row
			// from the lower LookupJoin will not have any matches in the top
			// LookupJoin (it has NULLs on key columns) and will get null-extended
			// there as well.
			indexJoin.On = memo.TrueFilter
		} else {
			// ON has some conditions that are bound by the columns in the index (at
			// the very least, the equality conditions we used for KeyCols), and some
			// conditions that refer to other columns. We can put the former in the
			// lower LookupJoin and the latter in the index join.
			//
			// This works for InnerJoin but not for LeftJoin because of a
			// technicality: if an input (left) row has matches in the lower
			// LookupJoin but has no matches in the index join, only the columns
			// looked up by the top index join get NULL-extended.
			if joinType == opt.LeftJoinOp {
				// TODO(radu): support LeftJoin, perhaps by looking up all columns and
				// discarding columns that are already available from the lower
				// LookupJoin. This requires a projection to avoid having the same
				// ColumnIDs on both sides of the index join.
				continue
			}
			conditions := lookupJoin.On
			lookupJoin.On = c.ExtractBoundConditions(conditions, onCols)
			indexJoin.On = c.ExtractUnboundConditions(conditions, onCols)
		}

		indexJoin.Input = c.e.f.ConstructLookupJoin(
			lookupJoin.Input,
			lookupJoin.On,
			&lookupJoin.LookupJoinPrivate,
		)
		indexJoin.JoinType = joinType
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = pkCols
		indexJoin.Cols = scanPrivate.Cols.Union(inputProps.OutputCols)
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// eqColsForZigzag is a helper function to generate eqCol lists for the zigzag
// joiner. The zigzag joiner requires that the equality columns immediately
// follow the fixed columns in the index. Fixed here refers to columns that
// have been constrained to a constant value.
//
// There are two kinds of equality columns that this function takes care of:
// columns that have the same ColumnID on both sides (i.e. the same column),
// as well as columns that have been equated in some ON filter (i.e. they are
// contained in leftEqCols and rightEqCols at the same index).
//
// This function iterates through all columns of the indexes in order,
// skips past the fixed columns, and then generates however many eqCols
// there are that meet the above criteria.
//
// Returns a list of column ordinals for each index.
//
// See the comment in pkg/sql/rowexec/zigzag_joiner.go for more details
// on the role eqCols and fixed cols play in zigzag joins.
func eqColsForZigzag(
	tab cat.Table,
	tabID opt.TableID,
	leftIndex cat.Index,
	rightIndex cat.Index,
	fixedCols opt.ColSet,
	leftEqCols opt.ColList,
	rightEqCols opt.ColList,
) (leftEqPrefix, rightEqPrefix opt.ColList) {
	leftEqPrefix = make(opt.ColList, 0, len(leftEqCols))
	rightEqPrefix = make(opt.ColList, 0, len(rightEqCols))
	// We can only zigzag on columns present in the key component of the index,
	// so use the LaxKeyColumnCount here because that's the longest prefix of the
	// columns in the index which is guaranteed to exist in the key component.
	// Using KeyColumnCount is invalid, because if we have a unique index with
	// nullable columns, the "key columns" include the primary key of the table,
	// which is only present in the key component if one of the other columns is
	// NULL.
	i, leftCnt := 0, leftIndex.LaxKeyColumnCount()
	j, rightCnt := 0, rightIndex.LaxKeyColumnCount()
	for ; i < leftCnt; i++ {
		colID := tabID.ColumnID(leftIndex.Column(i).Ordinal)
		if !fixedCols.Contains(colID) {
			break
		}
	}
	for ; j < rightCnt; j++ {
		colID := tabID.ColumnID(rightIndex.Column(j).Ordinal)
		if !fixedCols.Contains(colID) {
			break
		}
	}

	for i < leftCnt && j < rightCnt {
		leftColID := tabID.ColumnID(leftIndex.Column(i).Ordinal)
		rightColID := tabID.ColumnID(rightIndex.Column(j).Ordinal)
		i++
		j++

		if leftColID == rightColID {
			leftEqPrefix = append(leftEqPrefix, leftColID)
			rightEqPrefix = append(rightEqPrefix, rightColID)
			continue
		}
		leftIdx, leftOk := leftEqCols.Find(leftColID)
		rightIdx, rightOk := rightEqCols.Find(rightColID)
		// If both columns are at the same index in their respective
		// EqCols lists, they were equated in the filters.
		if leftOk && rightOk && leftIdx == rightIdx {
			leftEqPrefix = append(leftEqPrefix, leftColID)
			rightEqPrefix = append(rightEqPrefix, rightColID)
			continue
		} else {
			// We've reached the first non-equal column; the zigzag
			// joiner does not support non-contiguous/non-prefix equal
			// columns.
			break
		}

	}

	return leftEqPrefix, rightEqPrefix
}

// fixedColsForZigzag is a helper function to generate FixedCols lists for the
// zigzag join expression. It takes in a fixedValMap mapping column IDs to
// constant values they are constrained to. This function iterates through
// the columns of the specified index in order until it comes across the first
// column ID not in fixedValMap.
func (c *CustomFuncs) fixedColsForZigzag(
	index cat.Index, tabID opt.TableID, fixedValMap map[opt.ColumnID]tree.Datum,
) (opt.ColList, memo.ScalarListExpr, []types.T) {
	vals := make(memo.ScalarListExpr, 0, len(fixedValMap))
	typs := make([]types.T, 0, len(fixedValMap))
	fixedCols := make(opt.ColList, 0, len(fixedValMap))

	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		colID := tabID.ColumnID(index.Column(i).Ordinal)
		val, ok := fixedValMap[colID]
		if !ok {
			break
		}
		dt := index.Column(i).DatumType()
		vals = append(vals, c.e.f.ConstructConstVal(val, dt))
		typs = append(typs, *dt)
		fixedCols = append(fixedCols, colID)
	}
	return fixedCols, vals, typs
}

// GenerateZigzagJoins generates zigzag joins for all pairs of indexes of the
// Scan table which contain one of the constant columns in the FiltersExpr as
// its prefix.
//
// Similar to the lookup join, if the selected index pair does not contain
// all the columns in the output of the scan, we wrap the zigzag join
// in another index join (implemented as a lookup join) on the primary index.
// The index join is implemented with a lookup join since the index join does
// not support arbitrary input sources that are not plain index scans.
func (c *CustomFuncs) GenerateZigzagJoins(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {

	// Short circuit unless zigzag joins are explicitly enabled.
	if !c.e.evalCtx.SessionData.ZigzagJoinEnabled {
		return
	}

	fixedCols := memo.ExtractConstColumns(filters, c.e.mem, c.e.evalCtx)

	if fixedCols.Len() == 0 {
		// Zigzagging isn't helpful in the absence of fixed columns.
		return
	}

	// Iterate through indexes, looking for those prefixed with fixedEq cols.
	// Efficiently finding a set of indexes that make the most efficient zigzag
	// join, with no limit on the number of indexes selected, is an instance of
	// this NP-hard problem:
	// https://en.wikipedia.org/wiki/Maximum_coverage_problem
	//
	// A formal definition would be: Suppose we have a set of fixed columns F
	// (defined as fixedCols in the code above), and a set of indexes I. The
	// "fixed prefix" of every index, in this context, refers to the longest
	// prefix of each index's columns that is in F. In other words, we stop
	// adding to the prefix when we come across the first non-fixed column
	// in an index.
	//
	// We want to find at most k = 2 indexes from I (in the future k could be
	// >= 2 when the zigzag joiner supports 2+ index zigzag joins) that cover
	// the maximum number of columns in F. An index is defined to have covered
	// a column if that column is in the index's fixed prefix.
	//
	// Since only 2-way zigzag joins are currently supported, the naive
	// approach is bounded at n^2. For now, just do that - a quadratic
	// iteration through all indexes.
	//
	// TODO(itsbilal): Implement the greedy or weighted version of the
	// algorithm laid out here:
	// https://en.wikipedia.org/wiki/Maximum_coverage_problem
	var iter, iter2 scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.next() {
		if iter.indexOrdinal == cat.PrimaryIndex {
			continue
		}

		leftFixed := c.indexConstrainedCols(iter.index, scanPrivate.Table, fixedCols)
		// Short-circuit quickly if the first column in the index is not a fixed
		// column.
		if leftFixed.Len() == 0 {
			continue
		}
		iter2.init(c.e.mem, scanPrivate)
		// Only look at indexes after this one.
		iter2.indexOrdinal = iter.indexOrdinal

		for iter2.next() {
			rightFixed := c.indexConstrainedCols(iter2.index, scanPrivate.Table, fixedCols)
			// If neither side contributes a fixed column not contributed by the
			// other, then there's no reason to zigzag on this pair of indexes.
			if leftFixed.SubsetOf(rightFixed) || rightFixed.SubsetOf(leftFixed) {
				continue
			}
			// Columns that are in both indexes are, by definition, equal.
			leftCols := iter.indexCols()
			rightCols := iter2.indexCols()
			eqCols := leftCols.Intersection(rightCols)
			eqCols.DifferenceWith(fixedCols)
			if eqCols.Len() == 0 {
				// A simple index join is more efficient in such cases.
				continue
			}

			// If there are any equalities across the columns of the two indexes,
			// push them into the zigzag join spec.
			leftEq, rightEq := memo.ExtractJoinEqualityColumns(
				leftCols, rightCols, filters,
			)
			leftEqCols, rightEqCols := eqColsForZigzag(
				iter.tab,
				scanPrivate.Table,
				iter.index,
				iter2.index,
				fixedCols,
				leftEq,
				rightEq,
			)

			if len(leftEqCols) == 0 || len(rightEqCols) == 0 {
				// One of the indexes is not sorted by any of the equality
				// columns, because the equality columns do not immediately
				// succeed the fixed columns. A zigzag join cannot be planned.
				continue
			}

			// Confirm the primary key columns are in both leftEqCols and
			// rightEqCols. The conversion of a select with filters to a
			// zigzag join requires the primary key columns to be in the output
			// for output correctness; otherwise, we could be outputting more
			// results than there should be (due to an equality on a non-unique
			// non-required value).
			pkIndex := iter.tab.Index(cat.PrimaryIndex)
			pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
			pkColsFound := true
			for i := range pkCols {
				pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)

				if _, ok := leftEqCols.Find(pkCols[i]); !ok {
					pkColsFound = false
					break
				}
				if _, ok := rightEqCols.Find(pkCols[i]); !ok {
					pkColsFound = false
					break
				}
			}
			if !pkColsFound {
				continue
			}

			zigzagJoin := memo.ZigzagJoinExpr{
				On: filters,
				ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
					LeftTable:   scanPrivate.Table,
					LeftIndex:   iter.indexOrdinal,
					RightTable:  scanPrivate.Table,
					RightIndex:  iter2.indexOrdinal,
					LeftEqCols:  leftEqCols,
					RightEqCols: rightEqCols,
				},
			}

			// Fixed values are represented as tuples consisting of the
			// fixed segment of that side's index.
			fixedValMap := memo.ExtractValuesFromFilter(filters, fixedCols)

			if len(fixedValMap) != fixedCols.Len() {
				if util.RaceEnabled {
					panic(errors.AssertionFailedf(
						"we inferred constant columns whose value we couldn't extract",
					))
				}

				// This is a bug, but we don't want to block queries from running because of it.
				// TODO(justin): remove this when we fix extractConstEquality.
				continue
			}

			leftFixedCols, leftVals, leftTypes := c.fixedColsForZigzag(
				iter.index, scanPrivate.Table, fixedValMap,
			)
			rightFixedCols, rightVals, rightTypes := c.fixedColsForZigzag(
				iter2.index, scanPrivate.Table, fixedValMap,
			)

			zigzagJoin.LeftFixedCols = leftFixedCols
			zigzagJoin.RightFixedCols = rightFixedCols

			leftTupleTyp := types.MakeTuple(leftTypes)
			rightTupleTyp := types.MakeTuple(rightTypes)
			zigzagJoin.FixedVals = memo.ScalarListExpr{
				c.e.f.ConstructTuple(leftVals, leftTupleTyp),
				c.e.f.ConstructTuple(rightVals, rightTupleTyp),
			}

			zigzagJoin.On = memo.ExtractRemainingJoinFilters(
				filters,
				zigzagJoin.LeftEqCols,
				zigzagJoin.RightEqCols,
			)
			zigzagCols := leftCols.Copy()
			zigzagCols.UnionWith(rightCols)

			if scanPrivate.Cols.SubsetOf(zigzagCols) {
				// Case 1 (zigzagged indexes contain all requested columns).
				zigzagJoin.Cols = scanPrivate.Cols
				c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
				continue
			}

			if scanPrivate.Flags.NoIndexJoin {
				continue
			}

			// Case 2 (wrap zigzag join in an index join).
			var indexJoin memo.LookupJoinExpr
			// Ensure the zigzag join returns pk columns.
			zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
			for i := range pkCols {
				zigzagJoin.Cols.Add(pkCols[i])
			}

			if c.FiltersBoundBy(zigzagJoin.On, zigzagCols) {
				// The ON condition refers only to the columns available in the zigzag
				// indices.
				indexJoin.On = memo.TrueFilter
			} else {
				// ON has some conditions that are bound by the columns in the index (at
				// the very least, the equality conditions we used for EqCols and FixedCols),
				// and some conditions that refer to other table columns. We can put
				// the former in the lower ZigzagJoin and the latter in the index join.
				conditions := zigzagJoin.On
				zigzagJoin.On = c.ExtractBoundConditions(conditions, zigzagCols)
				indexJoin.On = c.ExtractUnboundConditions(conditions, zigzagCols)
			}

			indexJoin.Input = c.e.f.ConstructZigzagJoin(
				zigzagJoin.On,
				&zigzagJoin.ZigzagJoinPrivate,
			)
			indexJoin.JoinType = opt.InnerJoinOp
			indexJoin.Table = scanPrivate.Table
			indexJoin.Index = cat.PrimaryIndex
			indexJoin.KeyCols = pkCols
			indexJoin.Cols = scanPrivate.Cols
			indexJoin.LookupColsAreTableKey = true

			// Create the LookupJoin for the index join in the same group as the
			// original select.
			c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
		}
	}
}

// indexConstrainedCols computes the set of columns in allFixedCols which form
// a prefix of the key columns in idx.
func (c *CustomFuncs) indexConstrainedCols(
	idx cat.Index, tab opt.TableID, allFixedCols opt.ColSet,
) opt.ColSet {
	var constrained opt.ColSet
	for i, n := 0, idx.ColumnCount(); i < n; i++ {
		col := tab.ColumnID(idx.Column(i).Ordinal)
		if allFixedCols.Contains(col) {
			constrained.Add(col)
		} else {
			break
		}
	}
	return constrained
}

// GenerateInvertedIndexZigzagJoins generates zigzag joins for constraints on
// inverted index. It looks for cases where one inverted index can satisfy
// two constraints, and it produces zigzag joins with the same index on both
// sides of the zigzag join for those cases, fixed on different constant values.
func (c *CustomFuncs) GenerateInvertedIndexZigzagJoins(
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	// Short circuit unless zigzag joins are explicitly enabled.
	if !c.e.evalCtx.SessionData.ZigzagJoinEnabled {
		return
	}

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all inverted indexes.
	var iter scanIndexIter
	iter.init(c.e.mem, scanPrivate)
	for iter.nextInverted() {
		// See if there are two or more constraints that can be satisfied
		// by this inverted index. This is possible with inverted indexes as
		// opposed to secondary indexes, because one row in the primary index
		// can often correspond to multiple rows in an inverted index. This
		// function generates all constraints it can derive for this index;
		// not all of which might get used in this function.
		constraints, ok := c.allInvIndexConstraints(
			filters, scanPrivate.Table, iter.indexOrdinal,
		)
		if !ok || len(constraints) < 2 {
			continue
		}
		// In theory, we could explore zigzag joins on all constraint pairs.
		// However, in the absence of stats on inverted indexes, we will not
		// be able to distinguish more selective constraints from less
		// selective ones anyway, so just pick the first two constraints.
		//
		// TODO(itsbilal): Use the remaining constraints to build a remaining
		// filters expression, instead of just reusing filters from the scan.
		constraint := constraints[0]
		constraint2 := constraints[1]

		minPrefix := constraint.ExactPrefix(c.e.evalCtx)
		if otherPrefix := constraint2.ExactPrefix(c.e.evalCtx); otherPrefix < minPrefix {
			minPrefix = otherPrefix
		}

		if minPrefix == 0 {
			continue
		}

		zigzagJoin := memo.ZigzagJoinExpr{
			On: filters,
			ZigzagJoinPrivate: memo.ZigzagJoinPrivate{
				LeftTable:  scanPrivate.Table,
				LeftIndex:  iter.indexOrdinal,
				RightTable: scanPrivate.Table,
				RightIndex: iter.indexOrdinal,
			},
		}

		// Get constant values from each constraint. Add them to FixedVals as
		// tuples, with associated Column IDs in both {Left,Right}FixedCols.
		leftVals := make(memo.ScalarListExpr, minPrefix)
		leftTypes := make([]types.T, minPrefix)
		rightVals := make(memo.ScalarListExpr, minPrefix)
		rightTypes := make([]types.T, minPrefix)

		zigzagJoin.LeftFixedCols = make(opt.ColList, minPrefix)
		zigzagJoin.RightFixedCols = make(opt.ColList, minPrefix)
		for i := 0; i < minPrefix; i++ {
			leftVal := constraint.Spans.Get(0).StartKey().Value(i)
			rightVal := constraint2.Spans.Get(0).StartKey().Value(i)

			leftVals[i] = c.e.f.ConstructConstVal(leftVal, leftVal.ResolvedType())
			leftTypes[i] = *leftVal.ResolvedType()
			rightVals[i] = c.e.f.ConstructConstVal(rightVal, rightVal.ResolvedType())
			rightTypes[i] = *rightVal.ResolvedType()
			zigzagJoin.LeftFixedCols[i] = constraint.Columns.Get(i).ID()
			zigzagJoin.RightFixedCols[i] = constraint.Columns.Get(i).ID()
		}

		leftTupleTyp := types.MakeTuple(leftTypes)
		rightTupleTyp := types.MakeTuple(rightTypes)
		zigzagJoin.FixedVals = memo.ScalarListExpr{
			c.e.f.ConstructTuple(leftVals, leftTupleTyp),
			c.e.f.ConstructTuple(rightVals, rightTupleTyp),
		}

		// Set equality columns - all remaining columns after the fixed prefix
		// need to be equal.
		eqColLen := iter.index.ColumnCount() - minPrefix
		zigzagJoin.LeftEqCols = make(opt.ColList, eqColLen)
		zigzagJoin.RightEqCols = make(opt.ColList, eqColLen)
		for i := minPrefix; i < iter.index.ColumnCount(); i++ {
			colID := scanPrivate.Table.ColumnID(iter.index.Column(i).Ordinal)
			zigzagJoin.LeftEqCols[i-minPrefix] = colID
			zigzagJoin.RightEqCols[i-minPrefix] = colID
		}
		zigzagJoin.On = filters

		// Don't output the first column (i.e. the inverted index's JSON key
		// col) from the zigzag join. It could contain partial values, so
		// presenting it in the output or checking ON conditions against
		// it makes little sense.
		zigzagCols := iter.indexCols()
		for i, cnt := 0, iter.index.KeyColumnCount(); i < cnt; i++ {
			colID := scanPrivate.Table.ColumnID(iter.index.Column(i).Ordinal)
			zigzagCols.Remove(colID)
		}

		pkIndex := iter.tab.Index(cat.PrimaryIndex)
		pkCols := make(opt.ColList, pkIndex.KeyColumnCount())
		for i := range pkCols {
			pkCols[i] = scanPrivate.Table.ColumnID(pkIndex.Column(i).Ordinal)
			// Ensure primary key columns are always retrieved from the zigzag
			// join.
			zigzagCols.Add(pkCols[i])
		}

		// Case 1 (zigzagged indexes contain all requested columns).
		if scanPrivate.Cols.SubsetOf(zigzagCols) {
			zigzagJoin.Cols = scanPrivate.Cols
			c.e.mem.AddZigzagJoinToGroup(&zigzagJoin, grp)
			continue
		}

		if scanPrivate.Flags.NoIndexJoin {
			continue
		}

		// Case 2 (wrap zigzag join in an index join).

		var indexJoin memo.LookupJoinExpr
		// Ensure the zigzag join returns pk columns.
		zigzagJoin.Cols = scanPrivate.Cols.Intersection(zigzagCols)
		for i := range pkCols {
			zigzagJoin.Cols.Add(pkCols[i])
		}

		if c.FiltersBoundBy(zigzagJoin.On, zigzagCols) {
			// The ON condition refers only to the columns available in the zigzag
			// indices.
			indexJoin.On = memo.TrueFilter
		} else {
			// ON has some conditions that are bound by the columns in the index (at
			// the very least, the equality conditions we used for EqCols and FixedCols),
			// and some conditions that refer to other table columns. We can put
			// the former in the lower ZigzagJoin and the latter in the index join.
			conditions := zigzagJoin.On
			zigzagJoin.On = c.ExtractBoundConditions(conditions, zigzagCols)
			indexJoin.On = c.ExtractUnboundConditions(conditions, zigzagCols)
		}

		indexJoin.Input = c.e.f.ConstructZigzagJoin(
			zigzagJoin.On,
			&zigzagJoin.ZigzagJoinPrivate,
		)
		indexJoin.JoinType = opt.InnerJoinOp
		indexJoin.Table = scanPrivate.Table
		indexJoin.Index = cat.PrimaryIndex
		indexJoin.KeyCols = pkCols
		indexJoin.Cols = scanPrivate.Cols
		indexJoin.LookupColsAreTableKey = true

		// Create the LookupJoin for the index join in the same group as the
		// original select.
		c.e.mem.AddLookupJoinToGroup(&indexJoin, grp)
	}
}

// deriveJoinSize returns the number of base relations (i.e., not joins)
// being joined underneath the given relational expression.
func (c *CustomFuncs) deriveJoinSize(e memo.RelExpr) int {
	relProps := e.Relational()
	if relProps.IsAvailable(props.JoinSize) {
		return relProps.Rule.JoinSize
	}
	relProps.SetAvailable(props.JoinSize)

	switch j := e.(type) {
	case *memo.InnerJoinExpr:
		relProps.Rule.JoinSize = c.deriveJoinSize(j.Left) + c.deriveJoinSize(j.Right)
	default:
		relProps.Rule.JoinSize = 1
	}

	return relProps.Rule.JoinSize
}

// ShouldReorderJoins returns whether the optimizer should attempt to find
// a better ordering of inner joins.
func (c *CustomFuncs) ShouldReorderJoins(left, right memo.RelExpr) bool {
	// TODO(justin): referencing left and right here is a hack: ideally
	// we'd want to be able to reference the logical properties of the
	// expression being explored in this CustomFunc.
	size := c.deriveJoinSize(left) + c.deriveJoinSize(right)
	if c.e.evalCtx.SessionData.MultiModelEnabled &&
		(left.Memo().QueryType == memo.MultiModel ||
			right.Memo().QueryType == memo.MultiModel) {
		return size <= c.e.evalCtx.SessionData.MultiModelReorderJoinsLimit
	}

	return size <= c.e.evalCtx.SessionData.ReorderJoinsLimit
}

// SetPlanMode sets the plan mode for the ts table and its related relational table group.
// In multiple model query processing, when the join happens between a
// relational table group and a timeseries table, set the plan mode based on the analysis and the
// selectivity comparison.
func (c *CustomFuncs) SetPlanMode(
	left memo.RelExpr, right memo.RelExpr, child *memo.TSScanExpr, parent *memo.SelectExpr,
) bool {
	i := left.Memo().MultimodelHelper.GetTableIndexFromGroup(child.TSScanPrivate.Table)
	// check if the table to be checked is not found in the group, then skip the plan mode setting
	if i == -1 {
		return false
	}

	// if its plan mode is already set, don't overwrite the plan mode
	if left.Memo().MultimodelHelper.PlanMode[i] == memo.InsideOut {
		if len(left.Memo().MultimodelHelper.AggNotPushDown) > i &&
			!left.Memo().MultimodelHelper.AggNotPushDown[i] &&
			len(left.Memo().MultimodelHelper.PreGroupInfos) > i &&
			len(left.Memo().MultimodelHelper.PreGroupInfos[i].AggFuncs) > 0 {
			return true
		}
		// if the plan mode is already set to inside-out in analyze phase but aggregation can't be pushed down,
		// set the query type to unset and leave the plan mode in case it is overridden because of plan enumeration
		left.Memo().QueryType = memo.Unset
		return false
	} else if left.Memo().MultimodelHelper.PlanMode[i] == memo.OutsideIn {
		return true
	}

	// add a check if the table's plan mode is already set or not, can't overwrite if the plan mode is already set to inside-out
	ff := 1.0
	rc := 10000.0
	if parent != nil {
		ff = parent.Relational().Stats.Selectivity
		rc = parent.Relational().Stats.RowCount
	} else {
		ff = child.Relational().Stats.Selectivity
		rc = child.Relational().Stats.RowCount
	}

	// if the selectivity of the ts table is less than the selectivity of the relational table
	// and aggregation can be pushed down to ts engine, set the plan mode to inside-out and allow join commutativity
	// otherwise set the plan mode to outside-in
	if !left.Memo().MultimodelHelper.AggNotPushDown[i] &&
		len(left.Memo().MultimodelHelper.PreGroupInfos) > 0 &&
		len(left.Memo().MultimodelHelper.PreGroupInfos[i].AggFuncs) > 0 &&
		ff*ffCompFactor < right.FirstExpr().Relational().Stats.Selectivity {
		left.Memo().MultimodelHelper.PlanMode[i] = memo.InsideOut
		return true
	}

	// if the aggregation can't be pushed down and the total qualified row count of ts table is less than 1000,
	// still don't choose outside-in
	if left.Memo().MultimodelHelper.AggNotPushDown[i] &&
		rc <= smallRSThreshold {
		return false
	}

	left.Memo().MultimodelHelper.PlanMode[i] = memo.OutsideIn
	return true
}

// NewShouldReorderJoins returns whether the optimizer should attempt to find
// a better ordering for a join tree. This is the case if the given expression
// is the first expression of its group, and the join tree rooted at the
// expression has not previously been reordered. This is to avoid duplicate
// work. In addition, a join cannot be reordered if it has join hints.
func (c *CustomFuncs) NewShouldReorderJoins(root memo.RelExpr) bool {
	if root != root.FirstExpr() {
		// Only match the first expression of a group to avoid duplicate work.
		return false
	}
	if c.e.evalCtx.SessionData.ReorderJoinsLimit == 0 {
		// Join reordering has been disabled.
		return false
	}

	private, ok := root.Private().(*memo.JoinPrivate)
	if !ok {
		panic(errors.AssertionFailedf("operator does not have a join private: %v", root.Op()))
	}

	// Ensure that this join expression was not added to the memo by a previous
	// reordering, as well as that the join does not have hints.
	return !private.SkipReorderJoins && c.NoJoinHints(private)
}

// ReorderJoins adds alternate orderings of the given join tree to the memo. The
// first expression of the memo group is used for construction of the join
// graph. For more information, see the comment in join_order_builder.go.
func (c *CustomFuncs) ReorderJoins(grp memo.RelExpr) memo.RelExpr {
	c.e.o.JoinOrderBuilder().Init(c)
	c.e.o.JoinOrderBuilder().Reorder(grp.FirstExpr())
	return grp
}

// NoJoinManipulation returns whether the join has alternatives.
// In multiple model query processing, when the join happens between a
// relational table group and a timeseries table, set the plan mode based on the analysis result
// and do not proceed with further join manipulations.
func (c *CustomFuncs) NoJoinManipulation(left, right memo.RelExpr) bool {
	// Check if the left or right expression is a relational table, when multiple model
	// query processing is enabled and the current query is a multi-model query.
	if c.e.evalCtx.SessionData.MultiModelEnabled &&
		(left.Memo().QueryType == memo.MultiModel ||
			right.Memo().QueryType == memo.MultiModel) {
		switch lchild := left.FirstExpr().(type) {
		case *memo.TSScanExpr:
			if ret := c.SetPlanMode(left, right, lchild, nil); !ret {
				break
			}
			return false
		case *memo.SelectExpr:
			switch lgchild := lchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, right, lgchild, lchild); !ret {
					break
				}
				return false
			}
		case *memo.ProjectExpr:
			switch lgchild := lchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, right, lgchild, nil); !ret {
					break
				}
				return false
			case *memo.SelectExpr:
				switch lggchild := lgchild.Child(0).(type) {
				case *memo.TSScanExpr:
					if ret := c.SetPlanMode(left, right, lggchild, lgchild); !ret {
						break
					}
					return false
				}
			}
		}
		switch rchild := right.FirstExpr().(type) {
		case *memo.TSScanExpr:
			if ret := c.SetPlanMode(left, left, rchild, nil); !ret {
				break
			}
			return false
		case *memo.SelectExpr:
			switch rgchild := rchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, left, rgchild, rchild); !ret {
					break
				}
				return false
			}
		case *memo.ProjectExpr:
			switch rgchild := rchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, left, rgchild, nil); !ret {
					break
				}
				return false
			case *memo.SelectExpr:
				switch rggchild := rgchild.Child(0).(type) {
				case *memo.TSScanExpr:
					if ret := c.SetPlanMode(left, left, rggchild, rgchild); !ret {
						break
					}
					return false
				}
			}
		}
	}
	return true
}

// NoLookupJoinManipulation returns whether the join method has alternatives.
// In multiple model query processing, when the join happens between a
// relational table and a timeseries table, force the join sequence and
// method to favor outside-in and BLJ at the moment. Later on, will update to support
// inside-out and hybrid scenario.
func (c *CustomFuncs) NoLookupJoinManipulation(left memo.RelExpr) bool {
	// Check if the left expression is a TS table, when multiple model
	// query processing is enabled and the current query is a multi-model query.
	if c.e.evalCtx.SessionData.MultiModelEnabled &&
		(left.Memo().QueryType == memo.MultiModel) {
		switch lchild := left.FirstExpr().(type) {
		case *memo.TSScanExpr:
			i := left.Memo().MultimodelHelper.GetTableIndexFromGroup(lchild.TSScanPrivate.Table)
			if i == -1 {
				break
			}
			if left.Memo().MultimodelHelper.PlanMode[i] == memo.OutsideIn {
				return false
			}
		case *memo.SelectExpr:
			switch lgchild := lchild.Child(0).(type) {
			case *memo.TSScanExpr:
				i := left.Memo().MultimodelHelper.GetTableIndexFromGroup(lgchild.TSScanPrivate.Table)
				if i == -1 {
					break
				}
				if left.Memo().MultimodelHelper.PlanMode[i] == memo.OutsideIn {
					return false
				}
			}
		}
	}
	if opt.CheckOptMode(opt.TSQueryOptMode.Get(&c.e.evalCtx.Settings.SV), opt.OutsideInUseCBO) {
		if checkChildCanApplyOutsideIn(left) {
			return false
		}

	}
	return true
}

// NoMergeManipulation returns whether the join has alternatives.
// In multiple model query processing, when the join happens between a
// relational table group and a timeseries table, set the plan mode based on the analysis result
// and do not proceed with further join manipulations.
func (c *CustomFuncs) NoMergeManipulation(left, right memo.RelExpr) bool {
	// Check if the left or right expression is a relational table, when multiple model
	// query processing is enabled and the current query is a multi-model query.
	if c.e.evalCtx.SessionData.MultiModelEnabled &&
		(left.Memo().QueryType == memo.MultiModel ||
			right.Memo().QueryType == memo.MultiModel) {
		switch lchild := left.FirstExpr().(type) {
		case *memo.TSScanExpr:
			if ret := c.SetPlanMode(left, right, lchild, nil); !ret {
				break
			}
			return false
		case *memo.SelectExpr:
			switch lgchild := lchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, right, lgchild, lchild); !ret {
					break
				}
				return false
			}
		case *memo.ProjectExpr:
			switch lgchild := lchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, right, lgchild, nil); !ret {
					break
				}
				return false
			case *memo.SelectExpr:
				switch lggchild := lgchild.Child(0).(type) {
				case *memo.TSScanExpr:
					if ret := c.SetPlanMode(left, right, lggchild, lgchild); !ret {
						break
					}
					return false
				}
			}
		}
		switch rchild := right.FirstExpr().(type) {
		case *memo.TSScanExpr:
			if ret := c.SetPlanMode(left, left, rchild, nil); !ret {
				break
			}
			return false
		case *memo.SelectExpr:
			switch rgchild := rchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, left, rgchild, rchild); !ret {
					break
				}
				return false
			}
		case *memo.ProjectExpr:
			switch rgchild := rchild.Child(0).(type) {
			case *memo.TSScanExpr:
				if ret := c.SetPlanMode(left, left, rgchild, nil); !ret {
					break
				}
				return false
			case *memo.SelectExpr:
				switch rggchild := rgchild.Child(0).(type) {
				case *memo.TSScanExpr:
					if ret := c.SetPlanMode(left, left, rggchild, rgchild); !ret {
						break
					}
					return false
				}
			}
		}
	}

	if opt.CheckOptMode(opt.TSQueryOptMode.Get(&c.e.evalCtx.Settings.SV), opt.OutsideInUseCBO) {
		if checkChildCanApplyOutsideIn(left) || checkChildCanApplyOutsideIn(right) {
			return false
		}

	}
	return true
}

// IsSimpleEquality returns true if all of the filter conditions are equalities
// between simple data types (constants, variables, tuples and NULL).
func (c *CustomFuncs) IsSimpleEquality(filters memo.FiltersExpr) bool {
	for i := range filters {
		eqFilter, ok := filters[i].Condition.(*memo.EqExpr)
		if !ok {
			return false
		}

		left, right := eqFilter.Left, eqFilter.Right
		switch left.Op() {
		case opt.VariableOp, opt.ConstOp, opt.NullOp, opt.TupleOp:
		default:
			return false
		}

		switch right.Op() {
		case opt.VariableOp, opt.ConstOp, opt.NullOp, opt.TupleOp:
		default:
			return false
		}
	}

	return true
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalGroupBy returns true if the private is for the canonical version
// of the grouping operator. This is the operator that is built initially (and
// has all grouping columns as optional in the ordering), as opposed to variants
// generated by the GenerateStreamingGroupBy exploration rule.
func (c *CustomFuncs) IsCanonicalGroupBy(private *memo.GroupingPrivate) bool {
	return private.Ordering.Any() || private.GroupingCols.SubsetOf(private.Ordering.Optional)
}

// MakeProjectFromPassthroughAggs constructs a top-level Project operator that
// contains one output column per function in the given aggregrate list. The
// input expression is expected to return zero or one rows, and the aggregate
// functions are expected to always pass through their values in that case.
func (c *CustomFuncs) MakeProjectFromPassthroughAggs(
	grp memo.RelExpr, input memo.RelExpr, aggs memo.AggregationsExpr,
) {
	if !input.Relational().Cardinality.IsZeroOrOne() {
		panic(errors.AssertionFailedf("input expression cannot have more than one row: %v", input))
	}

	var passthrough opt.ColSet
	projections := make(memo.ProjectionsExpr, 0, len(aggs))
	for i := range aggs {
		// If aggregate remaps the column ID, need to synthesize projection item;
		// otherwise, can just pass through.
		variable := aggs[i].Agg.Child(0).(*memo.VariableExpr)
		if variable.Col == aggs[i].Col {
			passthrough.Add(variable.Col)
		} else {
			projections = append(projections, c.e.f.ConstructProjectionsItem(variable, aggs[i].Col))
		}
	}
	c.e.mem.AddProjectToGroup(&memo.ProjectExpr{
		Input:       input,
		Projections: projections,
		Passthrough: passthrough,
	}, grp)
}

// TryPushAgg determines whether the memo expression meets the optimization criteria.
// If it does, push down the group by expr and construct a new group by expr to return.
// input params:
//
//	input: child of GroupByExpr or ScalarGroupByExpr(only InnerJoinExpr can be optimized)
//	aggs : Aggregations of GroupByExpr or ScalarGroupByExpr, record info of agg functions
//	private : GroupingPrivate of GroupByExpr or ScalarGroupByExpr
//
// output params: the new GroupByExpr that had been optimized.
func (c *CustomFuncs) TryPushAgg(
	input memo.RelExpr, aggs memo.AggregationsExpr, private *memo.GroupingPrivate,
) memo.RelExpr {
	var optHelper = &memo.InsideOutOptHelper{}
	optHelper.ColsOfProjectAndAgg = make(map[opt.ColumnID]struct{})
	canOpt, join := c.precheckInsideOutOptApplicable(&input, aggs, private, optHelper)
	if !canOpt {
		return nil
	}

	finishOpt, newInnerJoin, newAggItems, projectItems, passCols := PushAggIntoJoinTSEngineNode(c.e.f, join, aggs, private, private.GroupingCols, optHelper)
	if finishOpt {
		c.e.mem.SetFlag(opt.FinishOptInsideOut)
		var newGroup memo.RelExpr
		for _, agg := range newAggItems {
			if private.GroupingCols.Empty() && agg.Agg.Op() == opt.SumIntOp {
				private.OptFlags |= opt.ScalarGroupByWithSumInt
			}
		}
		if len(projectItems) == 0 {
			if private.GroupingCols.Empty() {
				//newGroup = c.e.f.ConstructScalarGroupBy(newInnerJoin, newAggItems, private)
				newGroup = &memo.ScalarGroupByExpr{
					Input:           newInnerJoin,
					Aggregations:    newAggItems,
					GroupingPrivate: *private,
				}
			} else {
				//newGroup = c.e.f.ConstructGroupBy(newInnerJoin, newAggItems, private)
				newGroup = &memo.GroupByExpr{
					Input:           newInnerJoin,
					Aggregations:    newAggItems,
					GroupingPrivate: *private,
				}
			}
		} else {
			if private.GroupingCols.Empty() {
				newGroup = c.e.f.ConstructScalarGroupBy(newInnerJoin, newAggItems, private)
			} else {
				newGroup = c.e.f.ConstructGroupBy(newInnerJoin, newAggItems, private)
			}
			newGroup = &memo.ProjectExpr{
				Input:       newGroup,
				Projections: projectItems,
				Passthrough: passCols,
			}
		}

		return newGroup
	}

	return nil
}

// IsAvailable returns true when input is not nil.
func (c *CustomFuncs) IsAvailable(input memo.RelExpr) bool {
	return input != nil
}

// AddNewExprToGroup adds input to exprGroup of GroupByExpr.
func (c *CustomFuncs) AddNewExprToGroup(grp *memo.GroupByExpr, input memo.RelExpr) {
	switch t := input.(type) {
	case *memo.GroupByExpr:
		c.e.mem.AddGroupByToGroup(t, grp)
	case *memo.ScalarGroupByExpr:
		c.e.mem.AddScalarGroupByToGroup(t, grp)
	case *memo.ProjectExpr:
		c.e.mem.AddProjectToGroup(t, grp)
	}
}

// AddNewExprToScalarGroup adds input to exprGroup of ScalarGroupByExpr.
func (c *CustomFuncs) AddNewExprToScalarGroup(grp *memo.ScalarGroupByExpr, input memo.RelExpr) {
	switch t := input.(type) {
	case *memo.GroupByExpr:
		c.e.mem.AddGroupByToGroup(t, grp)
	case *memo.ScalarGroupByExpr:
		c.e.mem.AddScalarGroupByToGroup(t, grp)
	case *memo.ProjectExpr:
		c.e.mem.AddProjectToGroup(t, grp)
	}
}

// GenerateStreamingGroupBy generates variants of a GroupBy or DistinctOn
// expression with more specific orderings on the grouping columns, using the
// interesting orderings property. See the GenerateStreamingGroupBy rule.
func (c *CustomFuncs) GenerateStreamingGroupBy(
	grp memo.RelExpr,
	op opt.Operator,
	input memo.RelExpr,
	aggs memo.AggregationsExpr,
	private *memo.GroupingPrivate,
) {
	orders := DeriveInterestingOrderings(input)
	intraOrd := private.Ordering
	for _, o := range orders {
		// We are looking for a prefix of o that satisfies the intra-group ordering
		// if we ignore grouping columns.
		oIdx, intraIdx := 0, 0
		for ; oIdx < len(o); oIdx++ {
			oCol := o[oIdx].ID()
			if private.GroupingCols.Contains(oCol) || intraOrd.Optional.Contains(oCol) {
				// Grouping or optional column.
				continue
			}

			if intraIdx < len(intraOrd.Columns) &&
				intraOrd.Columns[intraIdx].Group.Contains(oCol) &&
				intraOrd.Columns[intraIdx].Descending == o[oIdx].Descending() {
				// Column matches the one in the ordering.
				intraIdx++
				continue
			}
			break
		}
		if oIdx == 0 || intraIdx < len(intraOrd.Columns) {
			// No match.
			continue
		}
		o = o[:oIdx]

		var newOrd physical.OrderingChoice
		newOrd.FromOrderingWithOptCols(o, opt.ColSet{})

		// Simplify the ordering according to the input's FDs. Note that this is not
		// necessary for correctness because buildChildPhysicalProps would do it
		// anyway, but doing it here once can make things more efficient (and we may
		// generate fewer expressions if some of these orderings turn out to be
		// equivalent).
		newOrd.Simplify(&input.Relational().FuncDeps)

		newPrivate := *private
		newPrivate.Ordering = newOrd

		switch op {
		case opt.GroupByOp:
			newExpr := memo.GroupByExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddGroupByToGroup(&newExpr, grp)

		case opt.DistinctOnOp:
			newExpr := memo.DistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddDistinctOnToGroup(&newExpr, grp)

		case opt.UpsertDistinctOnOp:
			newExpr := memo.UpsertDistinctOnExpr{
				Input:           input,
				Aggregations:    aggs,
				GroupingPrivate: newPrivate,
			}
			c.e.mem.AddUpsertDistinctOnToGroup(&newExpr, grp)
		}
	}
}

// OtherAggsAreConst returns true if all items in the given aggregate list
// contain ConstAgg functions, except for the "except" item. The ConstAgg
// functions will always return the same value, as long as there is at least
// one input row.
func (c *CustomFuncs) OtherAggsAreConst(
	aggs memo.AggregationsExpr, except *memo.AggregationsItem,
) bool {
	for i := range aggs {
		agg := &aggs[i]
		if agg == except {
			continue
		}

		switch agg.Agg.Op() {
		case opt.ConstAggOp:
			// Ensure that argument is a VariableOp.
			if agg.Agg.Child(0).Op() != opt.VariableOp {
				return false
			}

		default:
			return false
		}
	}
	return true
}

// MakeOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the order defined by
// (MIN/MAX) operator. This function was originally created to be used
// with the Replace(Min|Max)WithLimit exploration rules.
//
// WARNING: The MinOp case can return a NULL value if the column allows it. This
// is because NULL values sort first in KWDB.
func (c *CustomFuncs) MakeOrderingChoiceFromColumn(
	op opt.Operator, col opt.ColumnID,
) physical.OrderingChoice {
	oc := physical.OrderingChoice{}
	switch op {
	case opt.MinOp:
		oc.AppendCol(col, false /* descending */)
	case opt.MaxOp:
		oc.AppendCol(col, true /* descending */)
	}
	return oc
}

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//	var iter scanIndexIter
//	iter.init(mem, scanOpDef)
//	for iter.next() {
//	  doSomething(iter.indexOrdinal)
//	}
type scanIndexIter struct {
	mem          *memo.Memo
	scanPrivate  *memo.ScanPrivate
	tab          cat.Table
	indexOrdinal cat.IndexOrdinal
	index        cat.Index
	cols         opt.ColSet
}

func (it *scanIndexIter) init(mem *memo.Memo, scanPrivate *memo.ScanPrivate) {
	it.mem = mem
	it.scanPrivate = scanPrivate
	it.tab = mem.Metadata().Table(scanPrivate.Table)
	it.indexOrdinal = -1
	it.index = nil
}

// next advances iteration to the next index of the Scan operator's table. This
// is the primary index if it's the first time next is called, or a secondary
// index thereafter. Inverted index are skipped. If the ForceIndex flag is set,
// then all indexes except the forced index are skipped. When there are no more
// indexes to enumerate, next returns false. The current index is accessible via
// the iterator's "index" field.
func (it *scanIndexIter) next() bool {
	for {
		it.indexOrdinal++
		if it.indexOrdinal >= it.tab.IndexCount() {
			it.index = nil
			return false
		}
		it.index = it.tab.Index(it.indexOrdinal)
		if it.index.IsInverted() {
			continue
		}
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrdinal {
			// If we are forcing a specific index, ignore the others.
			continue
		}

		// new access method hint
		if (it.scanPrivate.Flags.HintType == keys.ForceTableScan || it.scanPrivate.Flags.HintType == keys.UseTableScan) &&
			it.indexOrdinal != cat.PrimaryIndex {
			return false
		}
		if (it.scanPrivate.Flags.HintType == keys.ForceIndexScan ||
			it.scanPrivate.Flags.HintType == keys.ForceIndexOnly) &&
			it.scanPrivate.Flags.HintIndexes[0] != it.indexOrdinal {
			continue
		}
		if it.scanPrivate.Flags.HintType == keys.IgnoreTableScan && it.indexOrdinal == cat.PrimaryIndex {
			continue
		}
		if it.scanPrivate.Flags.HintType == keys.IgnoreIndexScan ||
			it.scanPrivate.Flags.HintType == keys.IgnoreIndexOnly {
			continueSign := false
			for _, index := range it.scanPrivate.Flags.HintIndexes {
				if index == it.indexOrdinal {
					continueSign = true
					break
				}
			}
			if continueSign {
				continue
			}
		}
		if it.scanPrivate.Flags.HintType == keys.UseIndexScan || it.scanPrivate.Flags.HintType == keys.UseIndexOnly {
			find := false
			for _, index := range it.scanPrivate.Flags.HintIndexes {
				if index == it.indexOrdinal {
					find = true
					break
				}
			}
			if !find {
				continue
			}
		}

		it.cols = opt.ColSet{}
		return true
	}
}

// nextInverted advances iteration to the next inverted index of the Scan
// operator's table. It returns false when there are no more inverted indexes to
// enumerate (or if there were none to begin with). The current index is
// accessible via the iterator's "index" field.
func (it *scanIndexIter) nextInverted() bool {
	for {
		it.indexOrdinal++
		if it.indexOrdinal >= it.tab.IndexCount() {
			it.index = nil
			return false
		}

		it.index = it.tab.Index(it.indexOrdinal)
		if !it.index.IsInverted() {
			continue
		}
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrdinal {
			// If we are forcing a specific index, ignore the others.
			continue
		}

		if (it.scanPrivate.Flags.HintType == keys.ForceIndexScan ||
			it.scanPrivate.Flags.HintType == keys.ForceIndexOnly) &&
			it.scanPrivate.Flags.HintIndexes[0] != it.indexOrdinal {
			continue
		}

		if it.scanPrivate.Flags.HintType == keys.UseIndexScan || it.scanPrivate.Flags.HintType == keys.UseIndexOnly {
			find := false
			for _, index := range it.scanPrivate.Flags.HintIndexes {
				if index == it.indexOrdinal {
					find = true
					break
				}
			}
			if !find {
				continue
			}
		}

		it.cols = opt.ColSet{}
		return true
	}
}

// indexCols returns the set of columns contained in the current index.
func (it *scanIndexIter) indexCols() opt.ColSet {
	if it.cols.Empty() {
		it.cols = it.mem.Metadata().TableMeta(it.scanPrivate.Table).IndexColumns(it.indexOrdinal)
	}
	return it.cols
}

// isCovering returns true if the current index contains all columns projected
// by the Scan operator.
func (it *scanIndexIter) isCovering() bool {
	return it.scanPrivate.Cols.SubsetOf(it.indexCols())
}

// GenerateTagTSScans produce tag scan access mode tsscan
// params:
// - grp: memo expr struct
// - TSScanPrivate: tsscan private struct
// - explicitFilters: filters that select expr
func (c *CustomFuncs) GenerateTagTSScans(
	grp memo.RelExpr, TSScanPrivate *memo.TSScanPrivate, explicitFilters memo.FiltersExpr,
) {
	// only ts scan can execute in ts engine
	if !c.e.mem.TSSupportAllProcessor() {
		return
	}
	private := *TSScanPrivate
	// get primary tag value from filters
	// eg: select a, b, c from ts1 where ptag = 10 and tag1 < 11 and a > 12;
	// get ptag = 10, split 10 for ptag value
	// leaveFilter is  a > 12
	// tagFilters is tag1 < 11
	// primaryTagFilters is ptag = 10
	leaveFilter, tagFilters, primaryTagFilters := memo.GetPrimaryTagFilterValue(&private, &explicitFilters, c.e.mem)
	if tagFilters == nil && primaryTagFilters == nil {
		// can not optimize when have not tag filter
		return
	}

	// Get tag index key and tag index filters
	if primaryTagFilters == nil {
		private.TagIndexFilter = memo.GetTagIndexKeyAndFilter(&private, &tagFilters, c.e.mem, len(explicitFilters))
	}

	private.TagFilter = tagFilters
	private.PrimaryTagFilter = primaryTagFilters

	// get ts table access mode through tag filter and primary tag value
	// only primary tag value --- tag index
	// primary tag value and tag filter ----- tag index table
	// only tag filter --- table table table
	// not exist primary tag value and tag filter  ----- meta table
	private.AccessMode = memo.GetAccessMode(primaryTagFilters != nil, tagFilters != nil, len(private.TagIndexFilter) > 0, TSScanPrivate, c.e.mem)

	// filter can all push to table reader, so need remover select expr
	if leaveFilter == nil {
		newTSScan := &memo.TSScanExpr{TSScanPrivate: private}
		grp.Relational().Stats.SortDistribution = grp.Child(0).(*memo.TSScanExpr).Relational().Stats.SortDistribution
		c.e.mem.AddTSScanToGroup(newTSScan, grp)
	} else {
		newFilters := *(leaveFilter.(*memo.FiltersExpr))
		// filter can all push to table reader, so need remover select expr
		if newFilters == nil {
			newTSScan := &memo.TSScanExpr{TSScanPrivate: private}
			newTSScan.Relational().Stats = grp.Child(0).(*memo.TSScanExpr).Relational().Stats
			c.e.mem.AddTSScanToGroup(newTSScan, grp)
		} else {
			// move leave filter to select filter iterm
			newTSScan := c.e.f.ConstructTSScan(&private)
			newTSScan.Relational().Stats = grp.Child(0).(*memo.TSScanExpr).Relational().Stats
			sel := &memo.SelectExpr{Input: newTSScan, Filters: newFilters}
			c.e.mem.AddSelectToGroup(sel, grp)
		}
	}
}

// CanApplyOutsideIn checks if join expr can apply the GenerateBatchLookUpJoin exploration rule.
func (c *CustomFuncs) CanApplyOutsideIn(left, right memo.RelExpr, on memo.FiltersExpr) bool {
	if c.e.evalCtx.StartDistributeMode {
		return false
	}
	if !opt.CheckOptMode(opt.TSQueryOptMode.Get(&c.e.evalCtx.Settings.SV), opt.OutsideInUseCBO) {
		return false
	}
	lok := checkChildCanApplyOutsideIn(left)
	rok := checkChildCanApplyOutsideIn(right)
	if (lok && rok) || (!lok && !rok) {
		return false
	}

	if lok {
		if left.FirstExpr().Relational().Stats.Selectivity*ffCompFactor < right.FirstExpr().Relational().Stats.Selectivity {
			c.e.mem.ClearFlag(opt.CanApplyOutsideIn)
			return false
		}
	} else {
		if right.FirstExpr().Relational().Stats.Selectivity*ffCompFactor < left.FirstExpr().Relational().Stats.Selectivity {
			c.e.mem.ClearFlag(opt.CanApplyOutsideIn)
			return false
		}
	}

	// on filter can not be orExpr, and operator can only be =,<,<=,>,>=,
	// and child of operator must be relational column and tag column
	for _, jp := range on {
		if _, ok := jp.Condition.(*memo.OrExpr); ok {
			c.e.mem.ClearFlag(opt.CanApplyOutsideIn)
			return false
		} else if c.e.mem.IsTsColsJoinPredicate(jp) {
			c.e.mem.ClearFlag(opt.CanApplyOutsideIn)
			return false
		}
	}

	// limit of data length of columns in on filter.
	// can not use outside-in when the length is not equ.
	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		left.Relational().OutputCols,
		right.Relational().OutputCols,
		on,
	)
	for i := range leftEq {
		md := c.e.mem.Metadata()
		if md.ColumnMeta(leftEq[i]).Type.Family() == types.IntFamily ||
			md.ColumnMeta(leftEq[i]).Type.Family() == types.FloatFamily {
			leftLength := md.ColumnMeta(leftEq[i]).Type.InternalType.Width
			rightLength := md.ColumnMeta(rightEq[i]).Type.InternalType.Width
			if leftLength != rightLength {
				c.e.mem.ClearFlag(opt.CanApplyOutsideIn)
				return false
			}
		}
	}

	c.e.mem.SetFlag(opt.CanApplyOutsideIn)
	return true
}

// checkChildCanApplyOutsideIn checks if child of join can apply outside-in optimization.
// child of join can only be Project,Select,TSScan
func checkChildCanApplyOutsideIn(expr memo.RelExpr) bool {
	switch expr.Op() {
	case opt.TSScanOp:
		return true
	case opt.SelectOp:
		return checkChildCanApplyOutsideIn(expr.Child(0).(memo.RelExpr))
	case opt.ProjectOp:
		return checkChildCanApplyOutsideIn(expr.Child(0).(memo.RelExpr))
	}
	return false
}

// GenerateBatchLookUpJoin constructs BatchLookUpJoinExpr and adds it
// to exprGroup of InnerjoinExpr.
func (c *CustomFuncs) GenerateBatchLookUpJoin(
	grp memo.RelExpr,
	left memo.RelExpr,
	right memo.RelExpr,
	on memo.FiltersExpr,
	flags *memo.JoinPrivate,
) {
	_expr := &memo.BatchLookUpJoinExpr{
		Left:        right,
		Right:       left,
		On:          on,
		JoinPrivate: *flags,
	}
	c.e.mem.AddBatchLookUpJoinToGroup(_expr, grp)
}

// precheckInsideOutOptApplicable pre-checks whether group by can be optimized.
// Optimization needs to meet the following four points
// 1. The cluster parameter "sql.inside_out.enabled" needs to be set to true
// 2. GroupByExpr has not been optimized
// 3. The child of GroupByExp must be InnerJoinExpr or ProjectExpr. If it is a ProjectExpr,
// it must satisfy that the child of the ProjectExpr is InnerJoinExpr and the ProjectItems cannot cross modules
// 4. the col of grouping must be single col and the ts col must be tag col
// input params:
// input: child of GroupByExpr or ScalarGroupByExpr
// aggs: Aggregations of GroupByExpr or ScalarGroupByExpr, record infos of agg functions
// private: GroupingPrivate of GroupByExpr or ScalarGroupByExpr
// optHelper: record aggItems, grouping, projectionItems to help opt inside-out
//
// output params: is true when GroupByExpr can be optimized
func (c *CustomFuncs) precheckInsideOutOptApplicable(
	input *memo.RelExpr,
	aggs memo.AggregationsExpr,
	private *memo.GroupingPrivate,
	optHelper *memo.InsideOutOptHelper,
) (bool, *memo.InnerJoinExpr) {
	// could not opt inside-out when sql.inside_out.enabled is false
	if !opt.CheckOptMode(opt.TSQueryOptMode.Get(c.GetSettingValues()), opt.JoinPushAgg) {
		return false, nil
	}
	if c.e.mem.CheckFlag(opt.CanApplyOutsideIn) {
		return false, nil
	}

	optTimeBucket := opt.CheckOptMode(opt.TSQueryOptMode.Get(c.GetSettingValues()), opt.JoinPushTimeBucket)

	// do not need to opt when there has no ts table, or it has already opted.
	if !c.e.mem.CheckFlag(opt.IncludeTSTable) || c.e.mem.CheckFlag(opt.FinishOptInsideOut) {
		return false, nil
	}

	// can only opt when the agg is (Sum, Count, CountRows, Avg, Min, Max)
	for i := range aggs {
		if !c.checkAggOptApplicable(aggs[i].Agg, optHelper) {
			return false, nil
		}
	}

	// could opt inside-out when the child of GroupByExpr isn't InnerJoinExpr
	join, isJoin := (*input).(*memo.InnerJoinExpr)
	if !isJoin {
		j := c.getInnerJoin(*input, optHelper)
		if j != nil {
			join = j
		} else {
			return false, nil
		}
	}

	c.e.mem.CheckHelper.OptHelper = optHelper
	c.e.f.CheckWhiteListAndSetEngine(input)

	// check grouping is single col and tag col
	if !private.GroupingCols.Empty() {
		groupCols := private.GroupingCols
		isApplicable := true
		groupCols.ForEach(func(colID opt.ColumnID) {
			colMeta := c.e.mem.Metadata().ColumnMeta(colID)
			if _, ok := optHelper.ColsOfProjectAndAgg[colID]; ok && colMeta.TSType == opt.ColNormal {
				isApplicable = false
			}
			if colMeta.TSType != opt.ColNormal && !colMeta.IsTag() {
				isApplicable = false
			}
			if optTimeBucket {
				isTimeBucket := false
				if tb, ok := c.e.mem.CheckHelper.PushHelper.Find(colID); ok {
					isTimeBucket = tb.IsTimeBucket
				}
				isApplicable = isApplicable || isTimeBucket
			}
		})
		if !isApplicable {
			return false, nil
		}
	}

	return true, join
}

// getInnerJoin gets InnerJoinExpr.
func (c *CustomFuncs) getInnerJoin(
	e memo.RelExpr, optHelper *memo.InsideOutOptHelper,
) *memo.InnerJoinExpr {
	switch t := e.(type) {
	case *memo.InnerJoinExpr:
		return t
	case *memo.ProjectExpr:
		// check projectionItems(could not cross mode) and the project must can exec in ts engine
		if !c.checkProjectionApplicable(t, optHelper) {
			return nil
		}
		return c.getInnerJoin(t.Input, optHelper)
	default:
		return nil
	}
}

// checkProjectionApplicable checks if the projection is applicable for optimization.
// It must satisfy that the ProjectItems cannot cross modules.
func (c *CustomFuncs) checkProjectionApplicable(
	project *memo.ProjectExpr, optHelper *memo.InsideOutOptHelper,
) bool {
	for _, proj := range project.Projections {
		// check if elements of ProjectionExpr do not cross modules
		m := modeHelper{modes: 0}
		c.getEngineMode(proj.Element, &m)
		if m.isHybridMode() {
			return false
		}
		optHelper.Projections = append(optHelper.Projections, proj)
		if m.checkMode(tsMode) {
			// check if elements of ProjectionExpr can be pushed down
			push, _ := memo.CheckExprCanExecInTSEngine(proj.Element.(opt.Expr), memo.ExprPosProjList,
				c.e.f.TSWhiteListMap.CheckWhiteListParam, false, c.e.mem.CheckOnlyOnePTagValue())
			if !push {
				return false
			}
			optHelper.ProEngine = append(optHelper.ProEngine, tree.EngineTypeTimeseries)
		} else {
			optHelper.ProEngine = append(optHelper.ProEngine, tree.EngineTypeRelational)
		}
		// check whether count/sum/avg parameter is in relational mode
		for _, aggArg := range optHelper.AggArgs {
			if aggArg.ArgColID == -1 {
				return false
			}
			if proj.Col == aggArg.ArgColID {
				if aggArg.AggOp == opt.SumOp || aggArg.AggOp == opt.CountOp || aggArg.AggOp == opt.AvgOp {
					if m.checkMode(relMode) {
						return false
					}
				}
			}
		}
		// check if elements of ProjectionExpr is time_bucket
		if proj.Element.Op() == opt.FunctionOp {
			f := proj.Element.(*memo.FunctionExpr)
			if f.Name == tree.FuncTimeBucket && m.checkMode(tsMode) {
				c.e.mem.AddColumn(proj.Col, "", memo.GetExprType(proj.Element),
					memo.ExprPosProjList, 0, true)
			}
		}
	}
	return true
}

const (
	relMode = 1 << 0
	tsMode  = 1 << 1
	anyMode = 1 << 2
)

// modeHelper is used to get the mode in which the projection was executed.
type modeHelper struct {
	modes int
}

// setMode sets mode is true
func (m *modeHelper) setMode(mode int) {
	m.modes |= mode
}

// checkMode checks if the mode is set.
func (m *modeHelper) checkMode(mode int) bool {
	return m.modes&mode > 0
}

// isHybridMode checks to see if there is both timeseries mode and relational mode
func (m *modeHelper) isHybridMode() bool {
	return m.checkMode(relMode) && m.checkMode(tsMode)
}

// getEngineMode gets the execution mode of the input expression and places it in the modeHelper.
func (c *CustomFuncs) getEngineMode(src opt.ScalarExpr, m *modeHelper) {
	switch t := src.(type) {
	case *memo.VariableExpr:
		colType := c.e.mem.Metadata().ColumnMeta(t.Col).TSType
		if colType == opt.ColNormal {
			m.setMode(relMode)
		} else {
			m.setMode(tsMode)
		}

	case *memo.FunctionExpr:
		for _, param := range t.Args {
			c.getEngineMode(param, m)
		}

	case *memo.ScalarListExpr:
		for i := range *t {
			c.getEngineMode((*t)[i], m)
		}

	case *memo.TrueExpr, *memo.FalseExpr, *memo.ConstExpr, *memo.IsExpr, *memo.IsNotExpr, *memo.NullExpr:
		m.setMode(anyMode)

	case *memo.TupleExpr:
		for i := range t.Elems {
			c.getEngineMode(t.Elems[i], m)
		}

	case *memo.ArrayExpr:
		for i := range t.Elems {
			c.getEngineMode(t.Elems[i], m)
		}

	case *memo.CaseExpr:
		for i := range t.Whens {
			c.getEngineMode(t.Whens[i], m)
		}
		c.getEngineMode(t.Input, m)
		c.getEngineMode(t.OrElse, m)

	case *memo.CastExpr, *memo.NotExpr, *memo.RangeExpr:
		c.getEngineMode(t.Child(0).(opt.ScalarExpr), m)

	case *memo.AndExpr, *memo.OrExpr, *memo.GeExpr, *memo.GtExpr, *memo.NeExpr, *memo.EqExpr, *memo.LeExpr, *memo.LtExpr, *memo.LikeExpr,
		*memo.NotLikeExpr, *memo.ILikeExpr, *memo.NotILikeExpr, *memo.SimilarToExpr, *memo.NotSimilarToExpr, *memo.RegMatchExpr,
		*memo.NotRegMatchExpr, *memo.RegIMatchExpr, *memo.NotRegIMatchExpr, *memo.ContainsExpr, *memo.JsonExistsExpr,
		*memo.JsonAllExistsExpr, *memo.JsonSomeExistsExpr, *memo.AnyScalarExpr, *memo.BitandExpr, *memo.BitorExpr, *memo.BitxorExpr,
		*memo.PlusExpr, *memo.MinusExpr, *memo.MultExpr, *memo.DivExpr, *memo.FloorDivExpr, *memo.ModExpr, *memo.PowExpr, *memo.ConcatExpr,
		*memo.LShiftExpr, *memo.RShiftExpr, *memo.WhenExpr, *memo.InExpr, *memo.NotInExpr:
		c.getEngineMode(t.Child(0).(opt.ScalarExpr), m)
		c.getEngineMode(t.Child(1).(opt.ScalarExpr), m)

	default:
		m.setMode(relMode)
		m.setMode(tsMode)
	}
}

// PushAggIntoJoinTSEngineNode constructs GroupByExpr that can be push down, and then construct
// new InnerJoinExpr, return twice AggregationsItem
// ex:
// GroupByExpr       change to ====>    GroupByExpr
//
//	|                                     |
//
// InnerJoinExpr                        InnerJoinExpr
//
//	/        \                           /        \
//
// scanExpr  tsscanExpr                scanExpr  GroupByExpr
//
//	   																							 \
//																								tsscanExpr
//
// input params:
// f: Factory constructs a normalized expression tree within the memo.
// source: the memo tree，always is InnerJoinExpr.
// aggs: Aggregations of GroupByExpr or ScalarGroupByExpr, record infos of agg functions
// private: GroupingPrivate of GroupByExpr or ScalarGroupByExpr.
// colSet: set of column IDs, including all columns in the grouping and on filters
// optHelper: record aggItems, grouping, projectionItems to help opt inside-out
//
// output params:
// finishOp: is true when successful optimization.
// newInnerJoin: the new InnerJoinExpr constructed based on the pushed GroupByExpr.
// newAggItems: the new AggregationsItem, always are secondary aggregation functions
// projectItems: use to construct ProjectExpr,when AVG performs secondary aggregation, sum/count is required
// passCols: use to construct ProjectExpr
func PushAggIntoJoinTSEngineNode(
	f *norm.Factory,
	source *memo.InnerJoinExpr,
	aggs memo.AggregationsExpr,
	private *memo.GroupingPrivate,
	colSet opt.ColSet,
	optHelper *memo.InsideOutOptHelper,
) (
	finishOpt bool,
	newInnerJoin memo.RelExpr,
	newAggItems []memo.AggregationsItem,
	projectItems []memo.ProjectionsItem,
	passCols opt.ColSet,
) {
	newColSet := colSet.Copy()
	canOpt, tsEngineInLeft, tsEngineTable := checkInsideOutOptApplicable(f, source)
	if !canOpt {
		return false, newInnerJoin, newAggItems, projectItems, passCols
	}

	for _, filter := range source.On {
		getAllCols(filter.Condition, &newColSet)
	}

	// case: handle nested inner-join
	if !tsEngineTable.IsTSEngine() {
		return dealWithTsTable(f, tsEngineTable, aggs, &newColSet, private, source, tsEngineInLeft, optHelper)
	}

	// case: child of GroupByExpr is ProjectExpr, and the ProjectExpr can push down, construct new ProjectExpr.
	tsProjections := make([]memo.ProjectionsItem, 0)
	relProjections := make([]memo.ProjectionsItem, 0)
	for i, pro := range optHelper.Projections {
		if optHelper.ProEngine[i] == tree.EngineTypeTimeseries {
			tsProjections = append(tsProjections, pro)
		} else {
			relProjections = append(relProjections, pro)
		}
	}

	if len(tsProjections) != 0 {
		tsEngineTable = f.ConstructProject(tsEngineTable, tsProjections, tsEngineTable.Relational().OutputCols)
	}
	// case: ts node under InnerJoinExpr all can exec in ts engine, then construct GroupByExpr in ts node
	passCols.UnionWith(private.GroupingCols)
	constructNewAgg(f, tsEngineTable, aggs, &newAggItems, &projectItems, optHelper, &passCols)

	tsEngineTable.Relational().OutputCols.ForEach(func(col opt.ColumnID) {
		if newColSet.Contains(col) {
			optHelper.Grouping.Add(col)
		}
	})

	newExpr := f.ConstructGroupBy(tsEngineTable, optHelper.Aggs, &memo.GroupingPrivate{GroupingCols: optHelper.Grouping, IsInsideOut: true})
	if p, ok := newExpr.Child(0).(*memo.ProjectExpr); ok {
		p.IsInsideOut = true
	}
	if tsEngineInLeft {
		newInnerJoin = f.ConstructInnerJoin(newExpr, source.Right, source.On, &source.JoinPrivate)
	} else {
		newInnerJoin = f.ConstructInnerJoin(source.Left, newExpr, source.On, &source.JoinPrivate)
	}

	if len(relProjections) != 0 {
		newInnerJoin = f.ConstructProject(newInnerJoin, relProjections, newInnerJoin.Relational().OutputCols)
		// if p, ok := newInnerJoin.(*memo.ProjectExpr); ok {
		// 	p.IsInsideOut = true
		// }
	}
	return true, newInnerJoin, newAggItems, projectItems, passCols
}

// constructNewAgg constructs the pushed AggregationsItems and record them in InsideOutOptHelper,
// and construct the twice AggregationsItems
// input params:
// f: Factory constructs a normalized expression tree within the memo.
// tsEngineTable: the ts table.
// aggs: Aggregations of GroupByExpr or ScalarGroupByExpr, record infos of agg functions
// NewAggregations: the twice aggItems
// projectItems:use to construct ProjectExpr,when AVG performs secondary aggregation, sum/count is required
// optHelper: record aggItems, grouping, projectionItems to help opt inside-out
// passthroughCols: use to construct ProjectExpr
func constructNewAgg(
	f *norm.Factory,
	tsEngineTable memo.RelExpr,
	aggs memo.AggregationsExpr,
	NewAggregations *[]memo.AggregationsItem,
	projectItems *[]memo.ProjectionsItem,
	aggHelper *memo.InsideOutOptHelper,
	passthroughCols *opt.ColSet,
) {
	aggMap := make(map[opt.ColumnID]map[string]opt.ColumnID)
	for i, agg := range aggs {
		var colID opt.ColumnID
		if !(agg.Agg.ChildCount() < 1) {
			colID = getColIDOfParamOfAgg(agg.Agg.Child(0))
		}

		// construct new agg and twice agg
		if tsEngineTable.Relational().OutputCols.Contains(colID) {
			switch aggs[i].Agg.(type) {
			case *memo.AvgExpr:
				// construct the agg that push down
				sum := f.ConstructSum(agg.Agg.Child(0).(opt.ScalarExpr))
				count := f.ConstructCount(agg.Agg.Child(0).(opt.ScalarExpr))

				newSumID := addAggColumnAndGetNewID(f, sum, "sum"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, sum.DataType(), &aggMap, aggHelper)
				newCountID := addAggColumnAndGetNewID(f, count, "count"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, types.Int, &aggMap, aggHelper)

				// construct twice agg
				sumTwo := f.ConstructSum(f.ConstructVariable(newSumID))
				sumIntTwo := f.ConstructSumInt(f.ConstructVariable(newCountID))
				newSumTwoID := f.Metadata().AddColumn("sum("+f.Metadata().ColumnMeta(newSumID).Alias+")", sumTwo.DataType())
				newSumIntTwoID := f.Metadata().AddColumn("sum_int("+f.Metadata().ColumnMeta(newCountID).Alias+")", types.Int)
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(sumTwo, newSumTwoID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(sumIntTwo, newSumIntTwoID))

				div := f.ConstructDiv(f.ConstructVariable(newSumTwoID), f.ConstructVariable(newSumIntTwoID))
				if d, ok := div.(*memo.DivExpr); ok {
					// the type of Div needs to be consistent with the result type of avg.
					d.Typ = agg.Typ
				}
				*projectItems = append(*projectItems, f.ConstructProjectionsItem(div, agg.Col))
			case *memo.CountExpr:
				count := f.ConstructCount(agg.Agg.Child(0).(opt.ScalarExpr))
				newCountID := addAggColumnAndGetNewID(f, count, "count"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, types.Int, &aggMap, aggHelper)

				sumIntTwo := f.ConstructSumInt(f.ConstructVariable(newCountID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(sumIntTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			case *memo.SumExpr:
				sum := f.ConstructSum(agg.Agg.Child(0).(opt.ScalarExpr))
				newSumID := addAggColumnAndGetNewID(f, sum, "sum"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, agg.Typ, &aggMap, aggHelper)

				sumTwo := f.ConstructSum(f.ConstructVariable(newSumID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(sumTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			case *memo.MaxExpr:
				max := f.ConstructMax(agg.Agg.Child(0).(opt.ScalarExpr))
				newMaxID := addAggColumnAndGetNewID(f, max, "max"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, agg.Typ, &aggMap, aggHelper)

				maxTwo := f.ConstructMax(f.ConstructVariable(newMaxID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(maxTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			case *memo.MinExpr:
				min := f.ConstructMin(agg.Agg.Child(0).(opt.ScalarExpr))
				newMinID := addAggColumnAndGetNewID(f, min, "min"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, agg.Typ, &aggMap, aggHelper)

				minTwo := f.ConstructMin(f.ConstructVariable(newMinID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(minTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			case *memo.ConstAggExpr:
				min := f.ConstructConstAgg(agg.Agg.Child(0).(opt.ScalarExpr))
				newConstID := addAggColumnAndGetNewID(f, min, "const"+"("+f.Metadata().ColumnMeta(colID).Alias+")", colID, agg.Typ, &aggMap, aggHelper)

				constTwo := f.ConstructConstAgg(f.ConstructVariable(newConstID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(constTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			default:
				panic(pgerror.Newf(pgcode.Warning, "could not optimize aggregation function: %s", aggs[i].Agg.Op()))
			}
		} else {
			// case: count(*), count(1), without col,
			// construct the pushed agg: count(*),
			// the twice agg: sum_int(count(*))
			switch aggs[i].Agg.(type) {
			case *memo.CountRowsExpr:
				count := f.ConstructCountRows()
				newAggID := f.Metadata().AddColumn("count"+"("+"*"+")", types.Int)
				aggHelper.Aggs = append(aggHelper.Aggs, f.ConstructAggregationsItem(count, newAggID))

				sumIntTwo := f.ConstructSumInt(f.ConstructVariable(newAggID))
				*NewAggregations = append(*NewAggregations, f.ConstructAggregationsItem(sumIntTwo, agg.Col))
				passthroughCols.Add(agg.Col)
			case *memo.MaxExpr, *memo.MinExpr, *memo.ConstAggExpr, *memo.AggDistinctExpr:
				*NewAggregations = append(*NewAggregations, agg)
				passthroughCols.Add(agg.Col)
			default:
				panic(pgerror.Newf(pgcode.Warning, "could not optimize aggregation function: %s", aggs[i].Agg.Op()))
			}
		}
	}
}

// addAggColumnAndGetNewID adds the new agg col to metadata.
// input params:
// f: Factory constructs a normalized expression tree within the memo.
// pushAgg: the new agg that should be pushed
// newAggName: the name of new agg col
// colID: the ID of the param of new agg col
// typ: the type of new agg col
// aggMap: record the new aggs that already been constructed.
// optHelper: records agg functions and projections which can push down ts engine side.
func addAggColumnAndGetNewID(
	f *norm.Factory,
	pushAgg opt.ScalarExpr,
	newAggName string,
	colID opt.ColumnID,
	typ *types.T,
	aggMap *map[opt.ColumnID]map[string]opt.ColumnID,
	optHelper *memo.InsideOutOptHelper,
) opt.ColumnID {
	if v, ok := (*aggMap)[colID]; ok {
		if id, ok := v[newAggName]; ok {
			return id
		}
		newAggID := f.Metadata().AddColumn(newAggName, typ)
		optHelper.Aggs = append(optHelper.Aggs, f.ConstructAggregationsItem(pushAgg, newAggID))
		v[newAggName] = newAggID
		return newAggID
	}
	newAggID := f.Metadata().AddColumn(newAggName, typ)
	aggInfo := make(map[string]opt.ColumnID)
	optHelper.Aggs = append(optHelper.Aggs, f.ConstructAggregationsItem(pushAgg, newAggID))
	aggInfo[newAggName] = newAggID
	(*aggMap)[colID] = aggInfo
	return newAggID
}

// dealWithTsTable recursively processes InnerJoinExpr, such as
// GroupByExpr
//
//	|
//
// InnerJoinExpr
//
//	/          \
//
// scanExpr  InnerJoinExpr (handle this case)
//
//	     /        \
//	scanExpr  tsscanExpr
//
// input params:
// f: Factory constructs a normalized expression tree within the memo.
// tsEngineTable: the ts table.
// aggs: Aggregations of GroupByExpr or ScalarGroupByExpr, record infos of agg functions
// colSet:  set of column IDs, including all columns in the grouping and on filters
// private: the GroupingPrivate of GroupByExpr
// source:  is previous layer InnerJoinExpr.
// tsEngineInLeft: is true when the ts table is on the left of InnerJoinExpr
// optHelper: records agg functions and projections which can push down ts engine side.
// output params:
// p1: is true when successful optimization.
// p2: the new InnerJoinExpr constructed based on the pushed GroupByExpr.
// p3: the new AggregationsItem, always are secondary aggregation functions
// p4: use to construct ProjectExpr,when AVG performs secondary aggregation, sum/count is required
// p5: use to construct ProjectExpr
func dealWithTsTable(
	f *norm.Factory,
	tsEngineTable memo.RelExpr,
	aggs memo.AggregationsExpr,
	colSet *opt.ColSet,
	private *memo.GroupingPrivate,
	source *memo.InnerJoinExpr,
	tsEngineInLeft bool,
	optHelper *memo.InsideOutOptHelper,
) (bool, memo.RelExpr, []memo.AggregationsItem, []memo.ProjectionsItem, opt.ColSet) {
	var newInnerJoin memo.RelExpr
	if Join, ok := tsEngineTable.(*memo.InnerJoinExpr); ok {
		colSet.UnionWith(Join.Left.Relational().OuterCols)
		colSet.UnionWith(Join.Right.Relational().OuterCols)
		finishOpt, newInnerJoin, newAggItems, projectItems, passCols := PushAggIntoJoinTSEngineNode(f, Join, aggs, private, *colSet, optHelper)
		if finishOpt {
			if tsEngineInLeft {
				newInnerJoin = f.ConstructInnerJoin(newInnerJoin, source.Right, source.On, &source.JoinPrivate)
			} else {
				newInnerJoin = f.ConstructInnerJoin(source.Left, newInnerJoin, source.On, &source.JoinPrivate)
			}
		}

		return finishOpt, newInnerJoin, newAggItems, projectItems, passCols
	}
	return false, newInnerJoin, nil, nil, opt.ColSet{}
}

func (c *CustomFuncs) getAggTableID(agg opt.Expr) opt.TableID {
	switch agg.ChildCount() {
	case 0:
		if v, ok := agg.(*memo.VariableExpr); ok {
			return c.e.mem.Metadata().ColumnMeta(v.Col).Table
		}
	default:
		for i := 0; i < agg.ChildCount(); i++ {
			tableID := c.getAggTableID(agg.Child(i))
			if tableID > 0 {
				return tableID
			}
		}
	}
	return 0
}

// checkAggOptApplicable checks if the agg can be optimized
func (c *CustomFuncs) checkAggOptApplicable(
	agg opt.ScalarExpr, optHelper *memo.InsideOutOptHelper,
) bool {
	switch t := agg.(type) {
	case *memo.SumExpr:
		if !c.checkArgsOptApplicable(t.Input, optHelper, opt.SumOp) {
			return false
		}
	case *memo.AvgExpr:
		if !c.checkArgsOptApplicable(t.Input, optHelper, opt.AvgOp) {
			return false
		}
	case *memo.CountExpr:
		if !c.checkArgsOptApplicable(t.Input, optHelper, opt.CountOp) {
			return false
		}
	case *memo.CountRowsExpr:
		c.checkArgsOptApplicable(nil, optHelper, opt.CountRowsOp)
	case *memo.MinExpr:
		c.checkArgsOptApplicable(t.Input, optHelper, opt.MinOp)
	case *memo.MaxExpr:
		c.checkArgsOptApplicable(t.Input, optHelper, opt.MaxOp)
	case *memo.ConstAggExpr:
		c.checkArgsOptApplicable(t.Input, optHelper, opt.MaxOp)
	case *memo.AggDistinctExpr:
		c.checkAggOptApplicable(t.Input, optHelper)
		tableID := c.getAggTableID(t.Input)
		if tableID <= 0 || c.e.mem.Metadata().Table(tableID).GetTableType() == tree.TimeseriesTable {
			return false
		}
	default:
		return false
	}
	return true
}

// checkArgsOptApplicable checks if the arguments of agg can be optimized
func (c *CustomFuncs) checkArgsOptApplicable(
	arg opt.ScalarExpr, optHelper *memo.InsideOutOptHelper, aggOp opt.Operator,
) bool {
	if v, ok := arg.(*memo.VariableExpr); ok {
		optHelper.AggArgs = append(optHelper.AggArgs, memo.AggArgHelper{AggOp: aggOp, ArgColID: v.Col})
	} else {
		optHelper.AggArgs = append(optHelper.AggArgs, memo.AggArgHelper{AggOp: aggOp, ArgColID: -1})
	}
	m := modeHelper{modes: 0}
	c.getEngineMode(arg, &m)

	if !m.checkMode(tsMode) {
		return false
	}
	return true
}

// checkInsideOutOptApplicable checks if the InnerJoinExpr can be optimized
// Optimization needs to meet the following points
// 1. The left and right of InnerJoinExpr must be cross modules
// 2. The left and right of InnerJoinExpr cannot both are cross modular InnerJoinExpr
// 3. The column in the connection condition must be a tag column
// 4. The ts engine side of InnerJoinExpr can not be GroupByExpr
// input params:
// f: Factory constructs a normalized expression tree within the memo.
// join: is the InnerJoinExpr
//
// output params:
// canOpt: is true when InnerJoinExpr can be optimized
// tsEngineInLeft: is true when the ts table is on the left side of InnerJoinExpr
// tsEngineTable: is the ts table.
func checkInsideOutOptApplicable(
	f *norm.Factory, join *memo.InnerJoinExpr,
) (bool, bool, memo.RelExpr) {
	typL := getAllEngine(join.Left)
	typR := getAllEngine(join.Right)

	// typL|typR=3 when the source is the join that cross mode.
	// typL == 3 && typR == 3 when the left and right are all cross mode join.
	if (typL|typR) != 3 || (typL == 3 && typR == 3) {
		return false, false, nil
	}
	// check on filter, must be single column or single tag column
	for i, n := 0, join.On.ChildCount(); i < n; i++ {
		if FiltersItem, ok := join.On.Child(i).(*memo.FiltersItem); ok {
			if condition, ok1 := FiltersItem.Condition.(*memo.EqExpr); ok1 {
				if !checkCondition(condition, f) {
					return false, false, nil
				}
			} else {
				return false, false, nil
			}
		} else {
			return false, false, nil
		}
	}

	// check if the ts col on condition is a tag col
	if !checkTagColInFilter(f.Metadata(), join.On) {
		return false, false, nil
	}

	var tsEngineTable memo.RelExpr
	tsEngineInLeft := true

	if typR == 1 {
		tsEngineTable = join.Left
	} else {
		tsEngineTable = join.Right
		tsEngineInLeft = false
	}

	// ts node itself has group by, do not handle
	switch tsEngineTable.(type) {
	case *memo.GroupByExpr, *memo.ScalarGroupByExpr, *memo.DistinctOnExpr:
		return false, false, nil
	}

	return true, tsEngineInLeft, tsEngineTable
}

// checkCondition checks the left and right columns of the condition
func checkCondition(condition *memo.EqExpr, f *norm.Factory) bool {
	return checkConditionColumn(condition.Left, f) && checkConditionColumn(condition.Right, f)
}

// checkConditionColumn checks whether the column is single column or single tag column of the source table
func checkConditionColumn(column opt.ScalarExpr, f *norm.Factory) bool {
	if c, ok := column.(*memo.VariableExpr); ok {
		colMeta := f.Metadata().ColumnMeta(c.Col)
		if colMeta.TSType == opt.ColNormal {
			return true
		}
		if colMeta.TSType != opt.ColNormal && colMeta.IsTag() {
			return true
		}
	}
	return false
}

// getAllEngine returns EngineType base on input,
// Relational is 1 ,Timeseries is 2
func getAllEngine(input memo.RelExpr) int {
	ret := int(tree.EngineTypeRelational) + 1
	switch input.(type) {
	case *memo.TSScanExpr:
		return int(tree.EngineTypeTimeseries) + 1
	}

	for i := 0; i < input.ChildCount(); i++ {
		if v, ok := input.Child(i).(memo.RelExpr); ok {
			ret |= getAllEngine(v)
		}
	}

	return ret
}

// checkTagColInFilter checks if the ts col on condition is a tag col.
// not allow normal ts col in the on filter, because it is inefficient.
// allow no on filter.
// allow no ts col in the on filter.
func checkTagColInFilter(meta *opt.Metadata, on memo.FiltersExpr) bool {
	for _, v := range on {
		eq, ok1 := v.Condition.(*memo.EqExpr)
		if !ok1 {
			continue
		}

		lCol, ok2 := eq.Left.(*memo.VariableExpr)
		if !ok2 {
			continue
		}

		rCol, ok3 := eq.Right.(*memo.VariableExpr)
		if !ok3 {
			continue
		}

		if meta.IsSingleRelCol(lCol.Col) {
			if meta.ColumnMeta(rCol.Col).TSType == opt.TSColNormal {
				return false
			}
		} else if meta.IsSingleRelCol(rCol.Col) {
			if meta.ColumnMeta(lCol.Col).TSType == opt.TSColNormal {
				return false
			}
		}
	}

	return true
}

// getColIDOfParamOfAgg returns the col ID base on input.
func getColIDOfParamOfAgg(input opt.Expr) opt.ColumnID {
	switch src := input.(type) {
	case *memo.VariableExpr:
		return src.Col
	case *memo.AggDistinctExpr:
		return getColIDOfParamOfAgg(src.Input)
	}

	return -1
}

// getAllCols records all col ID into allCols from input.
func getAllCols(input opt.Expr, allCols *opt.ColSet) {
	if opt.IsAggregateOp(input) {
		for i := 0; i < input.ChildCount(); i++ {
			getAllCols(input.Child(i), allCols)
		}
	}

	switch src := input.(type) {
	case *memo.VariableExpr:
		allCols.Add(src.Col)
	case *memo.AggDistinctExpr:
		getAllCols(src.Input, allCols)
	default:
		for i := 0; i < input.ChildCount(); i++ {
			getAllCols(input.Child(i), allCols)
		}
	}
}
