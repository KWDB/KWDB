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
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RelExpr is implemented by all operators tagged as Relational. Relational
// expressions have a set of logical properties that describe the content and
// characteristics of their behavior and results. They are stored as part of a
// memo group that contains other logically equivalent expressions. Expressions
// in the same memo group are linked together in a list that can be traversed
// via calls to FirstExpr and NextExpr:
//
//	  +--------------------------------------+
//	  |  +---------------+                   |
//	  |  |               |FirstExpr          |FirstExpr
//	  v  v               |                   |
//	member #1 -------> member #2 --------> member #3 -------> nil
//	          NextExpr           NextExpr            NextExpr
//
// A relational expression's physical properties and cost are defined once it
// has been optimized.
type RelExpr interface {
	opt.Expr

	// Memo is the memo which contains this relational expression.
	Memo() *Memo

	// Relational is the set of logical properties that describe the content and
	// characteristics of this expression's behavior and results.
	Relational() *props.Relational

	// RequiredPhysical is the set of required physical properties with respect to
	// which this expression was optimized. Enforcers may be added to the
	// expression tree to ensure the physical properties are provided.
	//
	// Set when optimization is complete, only for the expressions in the final
	// tree.
	RequiredPhysical() *physical.Required

	// ClearRequiredPhysical clears the physical.Required, only be used in tsInsertSelect with
	// order by clause.
	ClearRequiredPhysical()

	// ProvidedPhysical is the set of provided physical properties (which must be
	// compatible with the set of required physical properties).
	//
	// Set when optimization is complete, only for the expressions in the final
	// tree.
	ProvidedPhysical() *physical.Provided

	// Cost is an estimate of the cost of executing this expression tree. Set
	// when optimization is complete, only for the expressions in the final tree.
	Cost() Cost

	// FirstExpr returns the first member expression in the memo group (could be
	// this expression if it happens to be first in the group). Subsequent members
	// can be enumerated by then calling NextExpr. Note that enforcer operators
	// are not part of this list (but do maintain a link to it).
	FirstExpr() RelExpr

	// NextExpr returns the next member expression in the memo group, or nil if
	// there are no further members in the group.
	NextExpr() RelExpr

	// group returns the memo group that contains this expression and any other
	// logically equivalent expressions. There is one group struct for each memo
	// group that stores the properties for the group, as well as the pointer to
	// the first member of the group.
	group() exprGroup

	// bestProps returns the instance of bestProps associated with this
	// expression.
	bestProps() *bestProps

	// setNext sets this expression's next pointer to point to the given
	// expression. setNext will panic if the next pointer has already been set.
	setNext(e RelExpr)

	// GetAddSynchronizer get is added synchronizer to ts engine
	GetAddSynchronizer() bool

	// SetAddSynchronizer set need add synchronizer to ts engine
	SetAddSynchronizer()

	// ResetAddSynchronizer reset need add synchronizer to ts engine
	ResetAddSynchronizer()
}

// ScalarPropsExpr is implemented by scalar expressions which cache scalar
// properties, like FiltersExpr and ProjectionsExpr. These expressions are also
// tagged with the ScalarProps tag.
type ScalarPropsExpr interface {
	opt.ScalarExpr

	// ScalarProps returns the scalar properties associated with the expression.
	ScalarProps() *props.Scalar
}

// TrueSingleton is a global instance of TrueExpr, to avoid allocations.
var TrueSingleton = &TrueExpr{
	engine: tree.EngineTypeTimeseries,
}

// FalseSingleton is a global instance of FalseExpr, to avoid allocations.
var FalseSingleton = &FalseExpr{
	engine: tree.EngineTypeTimeseries,
}

// NullSingleton is a global instance of NullExpr having the Unknown type (most
// common case), to avoid allocations.
var NullSingleton = &NullExpr{
	Typ:    types.Unknown,
	engine: tree.EngineTypeTimeseries,
}

// TODO(justin): perhaps these should be auto-generated.

// RankSingleton is the global instance of RankExpr.
var RankSingleton = &RankExpr{}

// RowNumberSingleton is the global instance of RowNumber.
var RowNumberSingleton = &RowNumberExpr{}

// DenseRankSingleton is the global instance of DenseRankExpr.
var DenseRankSingleton = &DenseRankExpr{}

// PercentRankSingleton is the global instance of PercentRankExpr.
var PercentRankSingleton = &PercentRankExpr{}

// CumeDistSingleton is the global instance of CumeDistExpr.
var CumeDistSingleton = &CumeDistExpr{}

// CountRowsSingleton maintains a global instance of CountRowsExpr, to avoid
// allocations.
var CountRowsSingleton = &CountRowsExpr{}

// TrueFilter is a global instance of the empty FiltersExpr, used in situations
// where the filter should always evaluate to true:
//
//	SELECT * FROM a INNER JOIN b ON True
var TrueFilter = FiltersExpr{}

// EmptyTuple is a global instance of a TupleExpr that contains no elements.
// While this cannot be created in SQL, it can be the created by normalizations.
var EmptyTuple = &TupleExpr{Typ: types.EmptyTuple}

// ScalarListWithEmptyTuple is a global instance of a ScalarListExpr containing
// a TupleExpr that contains no elements. It's used when constructing an empty
// ValuesExpr:
//
//	SELECT 1
var ScalarListWithEmptyTuple = ScalarListExpr{EmptyTuple}

// EmptyGroupingPrivate is a global instance of a GroupingPrivate that has no
// grouping columns and no ordering.
var EmptyGroupingPrivate = &GroupingPrivate{}

// EmptyJoinPrivate is a global instance of a JoinPrivate that has no fields
// set.
var EmptyJoinPrivate = &JoinPrivate{HintInfo: JoinHintInfo{HintIndex: -1}}

// LastGroupMember returns the last member in the same memo group of the given
// relational expression.
func LastGroupMember(e RelExpr) RelExpr {
	for {
		next := e.NextExpr()
		if next == nil {
			return e
		}
		e = next
	}
}

// IsTrue is true if the FiltersExpr always evaluates to true. This is the case
// when it has zero conditions.
func (n FiltersExpr) IsTrue() bool {
	return len(n) == 0
}

// IsFalse is true if the FiltersExpr always evaluates to false. The only case
// that's checked is the fully normalized case, when the list contains a single
// False condition.
func (n FiltersExpr) IsFalse() bool {
	return len(n) == 1 && n[0].Condition.Op() == opt.FalseOp
}

// OuterCols returns the set of outer columns needed by any of the filter
// condition expressions.
func (n FiltersExpr) OuterCols(mem *Memo) opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps().OuterCols)
	}
	return colSet
}

// Sort sorts the FilterItems in n by the IDs of the expression.
func (n *FiltersExpr) Sort() {
	sort.Slice(*n, func(i, j int) bool {
		return (*n)[i].Condition.(opt.ScalarExpr).ID() < (*n)[j].Condition.(opt.ScalarExpr).ID()
	})
}

// Deduplicate removes all the duplicate filters from n.
func (n *FiltersExpr) Deduplicate() {
	dedup := (*n)[:0]

	// Only add it if it hasn't already been added.
	for i, filter := range *n {
		found := false
		for j := i - 1; j >= 0; j-- {
			previouslySeenFilter := (*n)[j]
			if previouslySeenFilter.Condition == filter.Condition {
				found = true
				break
			}
		}
		if !found {
			dedup = append(dedup, filter)
		}
	}

	*n = dedup
}

// RemoveCommonFilters removes the filters found in other from n.
func (n *FiltersExpr) RemoveCommonFilters(other FiltersExpr) {
	// TODO(ridwanmsharif): Faster intersection using a map
	common := (*n)[:0]
	for _, filter := range *n {
		found := false
		for _, otherFilter := range other {
			if filter.Condition == otherFilter.Condition {
				found = true
				break
			}
		}
		if !found {
			common = append(common, filter)
		}
	}
	*n = common
}

// OutputCols returns the set of columns constructed by the Aggregations
// expression.
func (n AggregationsExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.Add(n[i].Col)
	}
	return colSet
}

// OuterCols returns the set of outer columns needed by any of the zip
// expressions.
func (n ZipExpr) OuterCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps().OuterCols)
	}
	return colSet
}

// OutputCols returns the set of columns constructed by the Zip expression.
func (n ZipExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		for _, col := range n[i].Cols {
			colSet.Add(col)
		}
	}
	return colSet
}

// TupleOrdinal is an ordinal index into an expression of type Tuple. It is
// used by the ColumnAccess scalar expression.
type TupleOrdinal uint32

// ScanLimit is used for a limited table or index scan and stores the limit as
// well as the desired scan direction. A value of 0 means that there is no
// limit.
type ScanLimit int64

// MakeScanLimit initializes a ScanLimit with a number of rows and a direction.
func MakeScanLimit(rowCount int64, reverse bool) ScanLimit {
	if reverse {
		return ScanLimit(-rowCount)
	}
	return ScanLimit(rowCount)
}

// IsSet returns true if there is a limit.
func (sl ScanLimit) IsSet() bool {
	return sl != 0
}

// RowCount returns the number of rows in the limit.
func (sl ScanLimit) RowCount() int64 {
	if sl.Reverse() {
		return int64(-sl)
	}
	return int64(sl)
}

// Reverse returns true if the limit requires a reverse scan.
func (sl ScanLimit) Reverse() bool {
	return sl < 0
}

func (sl ScanLimit) String() string {
	if sl.Reverse() {
		return fmt.Sprintf("%d(rev)", -sl)
	}
	return fmt.Sprintf("%d", sl)
}

// ScanFlags stores any flags for the scan specified in the query (see
// tree.IndexFlags). These flags may be consulted by transformation rules or the
// coster.
type ScanFlags struct {
	// NoIndexJoin disallows use of non-covering indexes (index-join) for scanning
	// this table.
	NoIndexJoin bool

	// ForceIndex forces the use of a specific index (specified in Index).
	// ForceIndex and NoIndexJoin cannot both be set at the same time.
	ForceIndex bool
	Direction  tree.Direction
	Index      int

	TableName            tree.Name
	HintType             keys.ScanMethodHintType
	HintIndexes          []int
	IndexName            []tree.Name
	TotalCardinality     float64
	EstimatedCardinality float64
	LeadingTable         bool
}

// Empty returns true if there are no flags set.
func (sf *ScanFlags) Empty() bool {
	return !sf.NoIndexJoin && !sf.ForceIndex
}

// JoinHintInfo stores hint information for join.
type JoinHintInfo struct {
	FromHintTree         bool
	IsUse                bool
	HintIndex            int
	RealOrder            bool
	TotalCardinality     float64
	EstimatedCardinality float64
	LeadingTable         bool
	DisallowLookupJoin   bool
	DisallowMergeJoin    bool
	DisallowHashJoin     bool
}

// JoinFlags stores restrictions on the join execution method, derived from
// hints for a join specified in the query (see tree.JoinTableExpr).
// It is a bitfield where a bit is 1 if a certain type of join is allowed. The
// value 0 is special and indicates that any join is allowed.
type JoinFlags uint8

// Each flag indicates if a certain type of join is allowed. The JoinFlags are
// an OR of these flags, with the special case that the value 0 means anything
// is allowed.
const (
	// AllowHashJoinStoreLeft corresponds to a hash join where the left side is
	// stored into the hashtable. Note that execution can override the stored side
	// if it finds that the other side is smaller (up to a certain size).
	AllowHashJoinStoreLeft JoinFlags = (1 << iota)

	// AllowHashJoinStoreRight corresponds to a hash join where the right side is
	// stored into the hashtable. Note that execution can override the stored side
	// if it finds that the other side is smaller (up to a certain size).
	AllowHashJoinStoreRight

	// AllowMergeJoin corresponds to a merge join.
	AllowMergeJoin

	// AllowLookupJoinIntoLeft corresponds to a lookup join where the lookup
	// table is on the left side.
	AllowLookupJoinIntoLeft

	// AllowLookupJoinIntoRight corresponds to a lookup join where the lookup
	// table is on the right side.
	AllowLookupJoinIntoRight
)

var joinFlagStr = map[JoinFlags]string{
	AllowHashJoinStoreLeft:   "hash join (store left side)",
	AllowHashJoinStoreRight:  "hash join (store right side)",
	AllowMergeJoin:           "merge join",
	AllowLookupJoinIntoLeft:  "lookup join (into left side)",
	AllowLookupJoinIntoRight: "lookup join (into right side)",
}

// Empty returns true if this is the default value (where all join types are
// allowed).
func (jf JoinFlags) Empty() bool {
	return jf == 0
}

// Has returns true if the given flag is set.
func (jf JoinFlags) Has(flag JoinFlags) bool {
	return jf.Empty() || jf&flag != 0
}

func (jf JoinFlags) String() string {
	if jf.Empty() {
		return "no flags"
	}

	// Special cases for prettier results.
	switch jf {
	case AllowHashJoinStoreLeft | AllowHashJoinStoreRight:
		return "force hash join"
	case AllowLookupJoinIntoLeft | AllowLookupJoinIntoRight:
		return "force lookup join"
	}

	var b strings.Builder
	b.WriteString("force ")
	first := true
	for jf != 0 {
		flag := JoinFlags(1 << uint8(bits.TrailingZeros8(uint8(jf))))
		if !first {
			b.WriteString(" or ")
		}
		first = false
		b.WriteString(joinFlagStr[flag])
		jf ^= flag
	}
	return b.String()
}

func (lj *LookupJoinExpr) initUnexportedFields(mem *Memo) {
	// lookupProps are initialized as necessary by the logical props builder.
}

func (zj *ZigzagJoinExpr) initUnexportedFields(mem *Memo) {
	// leftProps and rightProps are initialized as necessary by the logical props
	// builder.
}

// WindowFrame denotes the definition of a window frame for an individual
// window function, excluding the OFFSET expressions, if present.
type WindowFrame struct {
	Mode           tree.WindowFrameMode
	StartBoundType tree.WindowFrameBoundType
	EndBoundType   tree.WindowFrameBoundType
	FrameExclusion tree.WindowFrameExclusion
}

// HasOffset returns true if the WindowFrame contains a specific offset.
func (f *WindowFrame) HasOffset() bool {
	return f.StartBoundType.IsOffset() || f.EndBoundType.IsOffset()
}

func (f *WindowFrame) String() string {
	var bld strings.Builder
	switch f.Mode {
	case tree.GROUPS:
		fmt.Fprintf(&bld, "groups")
	case tree.ROWS:
		fmt.Fprintf(&bld, "rows")
	case tree.RANGE:
		fmt.Fprintf(&bld, "range")
	}

	frameBoundName := func(b tree.WindowFrameBoundType) string {
		switch b {
		case tree.UnboundedFollowing, tree.UnboundedPreceding:
			return "unbounded"
		case tree.CurrentRow:
			return "current-row"
		case tree.OffsetFollowing, tree.OffsetPreceding:
			return "offset"
		}
		panic(errors.AssertionFailedf("unexpected bound"))
	}
	fmt.Fprintf(&bld, " from %s to %s",
		frameBoundName(f.StartBoundType),
		frameBoundName(f.EndBoundType),
	)
	switch f.FrameExclusion {
	case tree.ExcludeCurrentRow:
		bld.WriteString(" exclude current row")
	case tree.ExcludeGroup:
		bld.WriteString(" exclude group")
	case tree.ExcludeTies:
		bld.WriteString(" exclude ties")
	}
	return bld.String()
}

// IsCanonical returns true if the ScanPrivate indicates an original unaltered
// primary index Scan operator (i.e. unconstrained and not limited).
func (s *ScanPrivate) IsCanonical() bool {
	return s.Index == cat.PrimaryIndex &&
		s.Constraint == nil &&
		s.HardLimit == 0
}

// IsLocking returns true if the ScanPrivate is configured to use a row-level
// locking mode. This can be the case either because the Scan is in the scope of
// a SELECT .. FOR [KEY] UPDATE/SHARE clause or because the Scan was configured
// as part of the row retrieval of a DELETE or UPDATE statement.
func (s *ScanPrivate) IsLocking() bool {
	return s.Locking != nil
}

// NeedResults returns true if the mutation operator can return the rows that
// were mutated.
func (m *MutationPrivate) NeedResults() bool {
	return m.ReturnCols != nil
}

// IsColumnOutput returns true if the i-th ordinal column should be part of the
// mutation's output columns.
func (m *MutationPrivate) IsColumnOutput(i int) bool {
	return i < len(m.ReturnCols) && m.ReturnCols[i] != 0
}

// MapToInputID maps from the ID of a returned column to the ID of the
// corresponding input column that provides the value for it. If there is no
// matching input column ID, MapToInputID returns 0.
//
// NOTE: This can only be called if the mutation operator returns rows.
func (m *MutationPrivate) MapToInputID(tabColID opt.ColumnID) opt.ColumnID {
	if m.ReturnCols == nil {
		panic(errors.AssertionFailedf("MapToInputID cannot be called if ReturnCols is not defined"))
	}
	ord := m.Table.ColumnOrdinal(tabColID)
	return m.ReturnCols[ord]
}

// MapToInputCols maps the given set of columns to a corresponding set of
// input columns using the PassthroughCols list and MapToInputID function.
func (m *MutationPrivate) MapToInputCols(cols opt.ColSet) opt.ColSet {
	var inCols opt.ColSet

	// First see if any of the columns come from the passthrough columns.
	for _, c := range m.PassthroughCols {
		if cols.Contains(c) {
			inCols.Add(c)
		}
	}

	// The remaining columns must come from the table.
	tabCols := cols.Difference(inCols)
	tabCols.ForEach(func(t opt.ColumnID) {
		id := m.MapToInputID(t)
		if id == 0 {
			panic(errors.AssertionFailedf("could not find input column for %d", log.Safe(t)))
		}
		inCols.Add(id)
	})

	return inCols
}

// AddEquivTableCols adds an FD to the given set that declares an equivalence
// between each table column and its corresponding input column.
func (m *MutationPrivate) AddEquivTableCols(md *opt.Metadata, fdset *props.FuncDepSet) {
	for i, n := 0, md.Table(m.Table).DeletableColumnCount(); i < n; i++ {
		t := m.Table.ColumnID(i)
		id := m.MapToInputID(t)
		if id != 0 {
			fdset.AddEquivalency(t, id)
		}
	}
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (m *MutationPrivate) WithBindingID() opt.WithID {
	return m.WithID
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (w *WithExpr) WithBindingID() opt.WithID {
	return w.ID
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (r *RecursiveCTEExpr) WithBindingID() opt.WithID {
	return r.WithID
}

// initUnexportedFields is called when a project expression is created.
func (prj *ProjectExpr) initUnexportedFields(mem *Memo) {
	inputProps := prj.Input.Relational()
	// Determine the not-null columns.
	prj.notNullCols = inputProps.NotNullCols.Copy()
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if ExprIsNeverNull(item.Element, inputProps.NotNullCols) {
			prj.notNullCols.Add(item.Col)
		}
	}

	// Determine the "internal" functional dependencies (for the union of input
	// columns and synthesized columns).
	prj.internalFuncDeps.CopyFrom(&inputProps.FuncDeps)
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if v, ok := item.Element.(*VariableExpr); ok && inputProps.OutputCols.Contains(v.Col) {
			// Handle any column that is a direct reference to an input column. The
			// optimizer sometimes constructs these in order to generate different
			// column IDs; they can also show up after constant-folding e.g. an ORDER
			// BY expression.
			prj.internalFuncDeps.AddEquivalency(v.Col, item.Col)
			continue
		}

		if !item.scalar.CanHaveSideEffects {
			from := item.scalar.OuterCols.Intersection(inputProps.OutputCols)

			// We want to set up the FD: from --> colID.
			// This does not necessarily hold for "composite" types like decimals or
			// collated strings. For example if d is a decimal, d::TEXT can have
			// different values for equal values of d, like 1 and 1.0.
			//
			// We only add the FD if composite types are not involved.
			//
			// TODO(radu): add a whitelist of expressions/operators that are ok, like
			// arithmetic.
			composite := false
			for i, ok := from.Next(0); ok; i, ok = from.Next(i + 1) {
				typ := mem.Metadata().ColumnMeta(i).Type
				if sqlbase.DatumTypeHasCompositeKeyEncoding(typ) {
					composite = true
					break
				}
			}
			if !composite {
				prj.internalFuncDeps.AddSynthesizedCol(from, item.Col)
			}
		}
	}
	prj.internalFuncDeps.MakeNotNull(prj.notNullCols)
}

// InternalFDs returns the functional dependencies for the set of all input
// columns plus the synthesized columns.
func (prj *ProjectExpr) InternalFDs() *props.FuncDepSet {
	return &prj.internalFuncDeps
}

// ExprIsNeverNull makes a best-effort attempt to prove that the provided
// scalar is always non-NULL, given the set of outer columns that are known
// to be not null. This is particularly useful with check constraints.
// Check constraints are satisfied when the condition evaluates to NULL,
// whereas filters are not. For example consider the following check constraint:
//
// CHECK (col IN (1, 2, NULL))
//
// Any row evaluating this check constraint with any value for the column will
// satisfy this check constraint, as they would evaluate to true (in the case
// of 1 or 2) or NULL (in the case of everything else).
func ExprIsNeverNull(e opt.ScalarExpr, notNullCols opt.ColSet) bool {
	switch t := e.(type) {
	case *VariableExpr:
		return notNullCols.Contains(t.Col)

	case *TrueExpr, *FalseExpr, *ConstExpr, *IsExpr, *IsNotExpr:
		return true

	case *NullExpr:
		return false

	case *TupleExpr:
		// TODO(ridwanmsharif): Make this less conservative and instead update how
		// IN and NOT IN behave w.r.t tuples and how IndirectionExpr works with arrays.
		// Currently, the semantics of this function on Tuples are different
		// as it returns whether a NULL evaluation is possible given the composition of
		// the tuple. Changing this will require some additional logic in the IN cases.
		for i := range t.Elems {
			if !ExprIsNeverNull(t.Elems[i], notNullCols) {
				return false
			}
		}
		return true

	case *InExpr, *NotInExpr:
		// TODO(ridwanmsharif): If a tuple is found in either side, determine if the
		// expression is nullable based on the composition of the tuples.
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols) &&
			ExprIsNeverNull(t.Child(1).(opt.ScalarExpr), notNullCols)

	case *ArrayExpr:
		for i := range t.Elems {
			if !ExprIsNeverNull(t.Elems[i], notNullCols) {
				return false
			}
		}
		return true

	case *CaseExpr:
		for i := range t.Whens {
			if !ExprIsNeverNull(t.Whens[i], notNullCols) {
				return false
			}
		}
		return ExprIsNeverNull(t.Input, notNullCols) && ExprIsNeverNull(t.OrElse, notNullCols)

	case *CastExpr, *NotExpr, *RangeExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols)

	case *AndExpr, *OrExpr, *GeExpr, *GtExpr, *NeExpr, *EqExpr, *LeExpr, *LtExpr, *LikeExpr,
		*NotLikeExpr, *ILikeExpr, *NotILikeExpr, *SimilarToExpr, *NotSimilarToExpr, *RegMatchExpr,
		*NotRegMatchExpr, *RegIMatchExpr, *NotRegIMatchExpr, *ContainsExpr, *JsonExistsExpr,
		*JsonAllExistsExpr, *JsonSomeExistsExpr, *AnyScalarExpr, *BitandExpr, *BitorExpr, *BitxorExpr,
		*PlusExpr, *MinusExpr, *MultExpr, *DivExpr, *FloorDivExpr, *ModExpr, *PowExpr, *ConcatExpr,
		*LShiftExpr, *RShiftExpr, *WhenExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols) &&
			ExprIsNeverNull(t.Child(1).(opt.ScalarExpr), notNullCols)

	default:
		return false
	}
}

const (
	// Intersection represents intersection of multiple tag indexes
	Intersection = 0
	// Combine represents union of multiple tag indexes
	Combine = 1
)

// PTagValues primary tag values
type PTagValues map[uint32][]string

// TagIndexInfo tag index values
type TagIndexInfo struct {
	UnionType int
	IndexID   []uint32
	// TagIndexValues tag index key values
	TagIndexValues []map[uint32][]string
}
