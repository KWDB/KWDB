// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
	"github.com/cockroachdb/apd"
)

// ArrowFilterSpec is the unified (compute) description of a boolean filter.
// It is a recursive boolean expression tree evaluated entirely by arrow/compute.
//
// Grammar:
//   comparison:  Func in {equal, not_equal, less, less_equal, greater,
//                greater_equal}, Operands are leaves (columns / constants).
//   membership:  Func in {in, not_in}, Operands[0] is a column leaf,
//                Operands[1] is a ConstSet leaf (constant set). Evaluated by a
//                Go kernel (arrow compute v17 has no is_in kernel exposed).
//   null-test:    Func in {is_null, is_not_null}, Operands[0] is a column leaf
//                 (no right operand). Evaluated by a Go kernel over the column's
//                 validity bitmap.
//   logical:      Func in {and, or},   Operands are nested boolean exprs.
//                 Func == "not",       Operands[0] is a nested boolean expr.
type ArrowFilterSpec struct {
	Func string
	// Operands are the arguments. For comparison funcs they are leaves
	// (columns/constants). For logical funcs (and/or/not) they are nested
	// boolean expressions.
	Operands []ArrowFilterOperand
}

// ArrowFilterOperand is one argument of a filter expression. Exactly one of
// Leaf or Expr is set depending on the parent function (see ArrowFilterSpec).
type ArrowFilterOperand struct {
	// Leaf is a column reference or scalar constant (comparison operands).
	Leaf *ArrowArg
	// Expr is a nested boolean expression (operands of and/or/not).
	Expr *ArrowFilterSpec
}

type arrowFilterCore struct {
	spec    ArrowFilterSpec
	alloc   memory.Allocator
	evalCtx *tree.EvalContext
}

func newArrowFilterCore(spec ArrowFilterSpec, alloc memory.Allocator, evalCtx *tree.EvalContext) arrowFilterCore {
	return arrowFilterCore{spec: spec, alloc: alloc, evalCtx: evalCtx}
}

// arrowFilter is a UnifiedProcessor that selects the matching rows of its
// input record (all columns preserved) using the Arrow compute engine.
type arrowFilter struct {
	arrowFilterCore
	input UnifiedProcessor
}

// NewArrowFilter builds a filter operator over input.
func NewArrowFilter(alloc memory.Allocator, input UnifiedProcessor, spec ArrowFilterSpec, evalCtx *tree.EvalContext) UnifiedProcessor {
	return &arrowFilter{arrowFilterCore: newArrowFilterCore(spec, alloc, evalCtx), input: input}
}

// Allocator implements UnifiedProcessor.
func (f *arrowFilterCore) Allocator() memory.Allocator { return f.alloc }

// Init implements UnifiedProcessor.
func (f *arrowFilter) Init(ctx context.Context) { f.input.Init(ctx) }

// Next implements UnifiedProcessor. It emits exactly one filtered Record then
// reports done, matching the single-batch behavior of the input converter.
func (f *arrowFilter) Next(ctx context.Context) (arrow.Record, bool, error) {
	rec, done, err := f.input.Next(ctx)
	if err != nil {
		return nil, false, err
	}
	if done {
		return nil, true, nil
	}
	out, err := f.eval(ctx, rec)
	if err != nil {
		rec.Release()
		return nil, false, err
	}
	rec.Release()
	return out, false, nil
}

// eval evaluates the boolean filter expression and returns a new Record that
// contains only the matching rows of rec (all columns preserved).
func (f *arrowFilterCore) eval(ctx context.Context, rec arrow.Record) (arrow.Record, error) {
	mask, err := f.evalBool(ctx, rec, f.spec)
	if err != nil {
		return nil, err
	}
	defer mask.Release()

	// Select the matching rows from every column. We use the native arrow
	// compute Filter kernel for all types except Binary: the vendored
	// arrow/compute FilterBinary kernel mishandles the canonical Binary
	// (variable-offset) span layout and panics on GetSpanOffsets. For Binary
	// columns we gather the surviving rows explicitly with the Go path, which
	// already handles arrow.BINARY (see gatherColumn / appendValueAt). A null
	// predicate bit is dropped, matching "mask.IsNull(i) || !mask.Value(i)".
	results := make([]compute.Datum, rec.NumCols())
	cols := make([]arrow.Array, rec.NumCols())
	var filterDatum compute.Datum
	useComputeFilter := false
	for ci := 0; ci < int(rec.NumCols()); ci++ {
		if arrow.IsBinaryLike(rec.Column(ci).DataType().ID()) {
			continue
		}
		useComputeFilter = true
		break
	}
	if useComputeFilter {
		filterDatum = compute.NewDatum(mask)
		defer filterDatum.Release()
	}
	for ci := 0; ci < int(rec.NumCols()); ci++ {
		col := rec.Column(ci)
		if arrow.IsBinaryLike(col.DataType().ID()) {
			idxs := make([]int32, 0, col.Len())
			for i := 0; i < col.Len(); i++ {
				if !mask.IsNull(i) && mask.Value(i) {
					idxs = append(idxs, int32(i))
				}
			}
			gathered := gatherColumn(f.alloc, col, idxs)
			cols[ci] = gathered
			results[ci] = nil
			continue
		}
		res, err := compute.Filter(ctx, compute.NewDatum(col), filterDatum, compute.FilterOptions{})
		if err != nil {
			for j := 0; j < ci; j++ {
				cols[j].Release()
				results[j].Release()
			}
			return nil, err
		}
		results[ci] = res
		cols[ci] = res.(*compute.ArrayDatum).MakeArray()
	}
	if len(cols) == 0 {
		// Degenerate zero-column record: return an empty record with the
		// original schema. (Not expected from the planner, but keeps eval
		// robust against an out-of-range cols[0] access below.)
		return array.NewRecord(rec.Schema(), nil, 0), nil
	}
	out := array.NewRecord(rec.Schema(), cols, int64(cols[0].Len()))
	for _, r := range results {
		if r != nil {
			r.Release()
		}
	}
	return out, nil
}

// evalBool evaluates a filter expression tree to a boolean Arrow array.
func (f *arrowFilterCore) evalBool(ctx context.Context, rec arrow.Record, spec ArrowFilterSpec) (*array.Boolean, error) {
	switch spec.Func {
	case "and", "or":
		l, err := f.evalOperandBool(ctx, rec, spec.Operands[0])
		if err != nil {
			return nil, err
		}
		r, err := f.evalOperandBool(ctx, rec, spec.Operands[1])
		if err != nil {
			l.Release()
			return nil, err
		}
		defer l.Release()
		defer r.Release()
		res, err := compute.CallFunction(ctx, spec.Func, nil, compute.NewDatum(l), compute.NewDatum(r))
		if err != nil {
			return nil, err
		}
		return asBoolean(res)

	case "not":
		c, err := f.evalOperandBool(ctx, rec, spec.Operands[0])
		if err != nil {
			return nil, err
		}
		defer c.Release()
		res, err := compute.CallFunction(ctx, "not", nil, compute.NewDatum(c))
		if err != nil {
			return nil, err
		}
		return asBoolean(res)

	default:
		switch spec.Func {
		case "like", "not_like", "ilike", "not_ilike":
			return f.evalLike(ctx, rec, spec)
		case "in", "not_in":
			return f.evalIn(ctx, rec, spec)
		case "is_null", "is_not_null":
			return f.evalIsNull(ctx, rec, spec)
		}
		args := make([]compute.Datum, len(spec.Operands))
		for i, op := range spec.Operands {
			d, err := f.evalLeafDatum(ctx, rec, op)
			if err != nil {
				return nil, err
			}
			args[i] = d
		}
		res, err := compute.CallFunction(ctx, spec.Func, nil, args...)
		if err != nil {
			return nil, err
		}
		return asBoolean(res)
	}
}

func (f *arrowFilterCore) evalOperandBool(ctx context.Context, rec arrow.Record, op ArrowFilterOperand) (*array.Boolean, error) {
	if op.Expr == nil {
		return nil, fmt.Errorf("arrow filter: expected boolean sub-expression for %q", f.spec.Func)
	}
	return f.evalBool(ctx, rec, *op.Expr)
}

func (f *arrowFilterCore) evalLeafDatum(ctx context.Context, rec arrow.Record, op ArrowFilterOperand) (compute.Datum, error) {
	if op.Leaf == nil {
		return nil, fmt.Errorf("arrow filter: expected column/constant operand for %q", f.spec.Func)
	}
	if op.Leaf.Cast != nil {
		inner, err := f.evalLeafDatum(ctx, rec, ArrowFilterOperand{Leaf: &op.Leaf.Cast.Arg})
		if err != nil {
			return nil, err
		}
		return f.evalCast(inner, op.Leaf.Cast.Type)
	}
	if op.Leaf.Binary != nil {
		return f.evalBinary(ctx, rec, op.Leaf.Binary)
	}
	if op.Leaf.Computed != nil {
		return f.evalComputed(ctx, rec, op.Leaf.Computed)
	}
	if op.Leaf.Case != nil {
		// CASE/COALESCE value leaf: reuse the projection CASE evaluator, which
		// supports all arrow value types and nested branches. The surrounding
		// filter predicate then consumes the resulting array.
		proj := &arrowProjection{alloc: f.alloc, evalCtx: f.evalCtx}
		arr, err := proj.eval(ctx, rec, *op.Leaf.Case)
		if err != nil {
			return nil, err
		}
		return compute.NewDatum(arr), nil
	}
	if op.Leaf.Scalar != nil {
		return op.Leaf.Scalar, nil
	}
	col := arrowOperandColumn(rec, op.Leaf.ColName)
	col.Retain()
	return compute.NewDatum(col), nil
}

// evalBinary evaluates a nested arithmetic expression (add/sub/mul/div) by
// recursively evaluating its arguments and calling the Arrow compute kernel.
func (f *arrowFilterCore) evalBinary(ctx context.Context, rec arrow.Record, b *ArrowArgBinary) (compute.Datum, error) {
	args := make([]compute.Datum, len(b.Args))
	for i := range b.Args {
		d, err := f.evalLeafDatum(ctx, rec, ArrowFilterOperand{Leaf: &b.Args[i]})
		if err != nil {
			for j := 0; j < i; j++ {
				args[j].Release()
			}
			return nil, err
		}
		args[i] = d
	}
	res, err := compute.CallFunction(ctx, b.Func, nil, args...)
	if err != nil {
		for j := 0; j < len(args); j++ {
			args[j].Release()
		}
		return nil, err
	}
	return res, nil
}

// evalComputed materializes a nested string-function leaf (e.g.
// substring(col,1,3)) into an Arrow array via the existing arrowProjection
// string kernels, then hands the array back as a column operand so the
// surrounding comparison / LIKE predicate can consume it. This keeps
// "substring(col,1,3) = 'abc'" fully in the Arrow engine instead of falling
// back to a row-by-row tree.Datum evaluation. Errors raised by the projection
// kernels (e.g. negative substring length) propagate up as query errors,
// matching the classic path.
func (f *arrowFilterCore) evalComputed(ctx context.Context, rec arrow.Record, c *ArrowProjectionSpec) (compute.Datum, error) {
	tmp := &arrowProjection{alloc: f.alloc}
	arr, err := tmp.evalArrowStringFunc(ctx, rec, *c)
	if err != nil {
		return nil, err
	}
	return compute.NewDatum(arr), nil
}

func asBoolean(res compute.Datum) (*array.Boolean, error) {
	ad, ok := res.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("arrow filter: expected array result, got %T", res)
	}
	arr := ad.MakeArray()
	arr.Retain()
	res.Release()
	return arr.(*array.Boolean), nil
}

// evalLike implements SQL LIKE / ILIKE (and their negated forms) as a Go kernel
// over an Arrow string column. Arrow compute v17 has no match_like kernel, so
// this is evaluated directly. The left operand is a string array (column or a
// CAST to string); the right operand must be a constant string pattern.
func (f *arrowFilterCore) evalLike(ctx context.Context, rec arrow.Record, spec ArrowFilterSpec) (*array.Boolean, error) {
	if len(spec.Operands) != 2 {
		return nil, fmt.Errorf("arrow like: expected 2 operands, got %d", len(spec.Operands))
	}
	left, err := f.evalLeafDatum(ctx, rec, spec.Operands[0])
	if err != nil {
		return nil, err
	}
	right, err := f.evalLeafDatum(ctx, rec, spec.Operands[1])
	if err != nil {
		left.Release()
		return nil, err
	}
	switch left.(type) {
	case *compute.ArrayDatum:
	default:
		left.Release()
		right.Release()
		return nil, fmt.Errorf("arrow like: left operand must be a column, got %T", left)
	}
	switch right.(type) {
	case *compute.ScalarDatum:
	default:
		left.Release()
		right.Release()
		return nil, fmt.Errorf("arrow like: pattern must be a constant, got %T", right)
	}
	lad := left.(*compute.ArrayDatum)
	arr := lad.MakeArray()
	left.Release()
	defer arr.Release()
	sa, ok := arr.(*array.String)
	if !ok {
		return nil, fmt.Errorf("arrow like: left operand must be string, got %T", arr)
	}
	pat, err := likePattern(right.(*compute.ScalarDatum))
	if err != nil {
		return nil, err
	}
	right.Release()

	ci := spec.Func == "ilike" || spec.Func == "not_ilike"
	neg := spec.Func == "not_like" || spec.Func == "not_ilike"

	b := array.NewBooleanBuilder(f.alloc)
	defer b.Release()
	for i := 0; i < sa.Len(); i++ {
		if sa.IsNull(i) {
			b.Append(false)
			continue
		}
		m := likeMatch(sa.Value(i), pat, ci)
		if neg {
			m = !m
		}
		b.Append(m)
	}
	return b.NewArray().(*array.Boolean), nil
}

// likePattern extracts the string value from a constant scalar operand.
func likePattern(d *compute.ScalarDatum) (string, error) {
	if s, ok := d.Value.(*scalar.String); ok {
		return string(s.Value.Bytes()), nil
	}
	if s, ok := d.Value.(*scalar.LargeString); ok {
		return string(s.Value.Bytes()), nil
	}
	return "", fmt.Errorf("arrow like: pattern must be a string scalar, got %T", d.Value)
}

// likeMatch reports whether s matches a SQL LIKE pattern. `%` matches any
// sequence (including empty), `_` matches exactly one character, and the
// default escape character `\` quotes the next character.
func likeMatch(s, pattern string, caseInsensitive bool) bool {
	if caseInsensitive {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	rs := []rune(s)
	rp := []rune(pattern)
	var match func(i, j int) bool
	match = func(i, j int) bool {
		for j < len(rp) {
			switch c := rp[j]; {
			case c == '\\' && j+1 < len(rp):
				if i >= len(rs) || rs[i] != rp[j+1] {
					return false
				}
				i++
				j += 2
			case c == '%':
				for k := i; k <= len(rs); k++ {
					if match(k, j+1) {
						return true
					}
				}
				return false
			case c == '_':
				if i >= len(rs) {
					return false
				}
				i++
				j++
			default:
				if i >= len(rs) || rs[i] != c {
					return false
				}
				i++
				j++
			}
		}
		return i == len(rs)
	}
	return match(0, 0)
}

// evalIn implements SQL IN / NOT IN as a Go kernel over an Arrow column. The
// left operand is a column array (int64 or string); the right operand is a
// ConstSet leaf holding the constant member set. Arrow compute v17 does not
// expose a usable is_in kernel for both types, so this is evaluated directly,
// mirroring the evalLike approach. A null column value never matches (so it is
// excluded by IN and retained by NOT IN), matching SQL three-valued logic.
func (f *arrowFilterCore) evalIn(ctx context.Context, rec arrow.Record, spec ArrowFilterSpec) (*array.Boolean, error) {
	if len(spec.Operands) != 2 {
		return nil, fmt.Errorf("arrow in: expected 2 operands, got %d", len(spec.Operands))
	}
	left, err := f.evalLeafDatum(ctx, rec, spec.Operands[0])
	if err != nil {
		return nil, err
	}
	if _, ok := left.(*compute.ArrayDatum); !ok {
		left.Release()
		return nil, fmt.Errorf("arrow in: left operand must be a column, got %T", left)
	}
	// The member set is carried on the operand's Leaf.ConstSet (built by
	// leafToArrowArg for IN/NOT IN). Float sets produced from a casted decimal
	// column live on ConstSet as Float64 scalars.
	setLeaf := spec.Operands[1].Leaf
	if setLeaf == nil || len(setLeaf.ConstSet) == 0 {
		left.Release()
		return nil, fmt.Errorf("arrow in: right operand must be a non-empty ConstSet")
	}
	lad := left.(*compute.ArrayDatum)
	arr := lad.MakeArray()
	left.Release()
	defer arr.Release()

	neg := spec.Func == "not_in"
	b := array.NewBooleanBuilder(f.alloc)
	defer b.Release()

	// Dispatch on the column's concrete array type. Only int64 and string sets
	// are supported (the planner gates the predicate to a single family).
	switch col := arr.(type) {
	case *array.Int64:
		set := make(map[int64]struct{}, len(setLeaf.ConstSet))
		for _, d := range setLeaf.ConstSet {
			sd, ok := d.(*compute.ScalarDatum)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be scalar, got %T", d)
			}
			v, ok := sd.Value.(*scalar.Int64)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be int64, got %T", sd.Value)
			}
			set[v.Value] = struct{}{}
		}
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				b.Append(neg)
				continue
			}
			_, hit := set[col.Value(i)]
			if neg {
				b.Append(!hit)
			} else {
				b.Append(hit)
			}
		}
	case *array.String:
		set := make(map[string]struct{}, len(setLeaf.ConstSet))
		for _, d := range setLeaf.ConstSet {
			sd, ok := d.(*compute.ScalarDatum)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be scalar, got %T", d)
			}
			v, ok := sd.Value.(*scalar.String)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be string, got %T", sd.Value)
			}
			set[string(v.Value.Bytes())] = struct{}{}
		}
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				b.Append(neg)
				continue
			}
			_, hit := set[col.Value(i)]
			if neg {
				b.Append(!hit)
			} else {
				b.Append(hit)
			}
		}
	case *array.Float64:
		// Decimal IN / NOT IN: the planner casts the decimal column to FLOAT
		// and emits the set as float64 (Arrow compute has no DECIMAL is_in kernel).
		set := make(map[float64]struct{}, len(setLeaf.ConstSet))
		for _, d := range setLeaf.ConstSet {
			sd, ok := d.(*compute.ScalarDatum)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be scalar, got %T", d)
			}
			v, ok := sd.Value.(*scalar.Float64)
			if !ok {
				return nil, fmt.Errorf("arrow in: set value must be float64, got %T", sd.Value)
			}
			set[v.Value] = struct{}{}
		}
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				b.Append(neg)
				continue
			}
			_, hit := set[col.Value(i)]
			if neg {
				b.Append(!hit)
			} else {
				b.Append(hit)
			}
		}
	default:
		return nil, fmt.Errorf("arrow in: unsupported column type %T (only int64/string/float64 supported)", arr)
	}
	return b.NewArray().(*array.Boolean), nil
}

// evalIsNull implements SQL IS NULL / IS NOT NULL over an Arrow column. It
// operates purely on the array validity bitmap, so it works for any column type
// (unlike evalIn which needs a concrete element type). IS NULL matches rows
// whose value is null; IS NOT NULL matches the complement.
func (f *arrowFilterCore) evalIsNull(ctx context.Context, rec arrow.Record, spec ArrowFilterSpec) (*array.Boolean, error) {
	if len(spec.Operands) != 1 {
		return nil, fmt.Errorf("arrow is_null: expected 1 operand, got %d", len(spec.Operands))
	}
	left, err := f.evalLeafDatum(ctx, rec, spec.Operands[0])
	if err != nil {
		return nil, err
	}
	lad, ok := left.(*compute.ArrayDatum)
	if !ok {
		left.Release()
		return nil, fmt.Errorf("arrow is_null: operand must be a column, got %T", left)
	}
	arr := lad.MakeArray()
	left.Release()
	defer arr.Release()

	neg := spec.Func == "is_not_null"
	b := array.NewBooleanBuilder(f.alloc)
	defer b.Release()
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			b.Append(!neg)
		} else {
			b.Append(neg)
		}
	}
	return b.NewArray().(*array.Boolean), nil
}

// evalCast converts the values of a column operand (given as a compute.Datum)
// to the target Arrow type, supporting the conversion pairs the filter engine
// needs: numeric/boolean -> string, string -> numeric. Constant-only casts are
// rejected by the planner, so the input is always an array.
func (f *arrowFilterCore) evalCast(d compute.Datum, to arrow.DataType) (compute.Datum, error) {
	ad, ok := d.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("arrow cast: expected array operand, got %T", d)
	}
	arr := ad.MakeArray()
	defer arr.Release()
	out, err := castArrowArray(f.alloc, arr, to)
	if err != nil {
		return nil, err
	}
	return compute.NewDatum(out), nil
}

// castArrowArray converts an arrow array to the target type, returning a new
// array owned by the caller.
func castArrowArray(alloc memory.Allocator, arr arrow.Array, to arrow.DataType) (arrow.Array, error) {
	switch to.ID() {
	case arrow.STRING:
		return castToString(alloc, arr)
	case arrow.INT64:
		return castToInt64(alloc, arr)
	case arrow.FLOAT64:
		return castToFloat64(alloc, arr)
	case arrow.DECIMAL128:
		return castToDecimal(alloc, arr, to.(*arrow.Decimal128Type))
	case arrow.BOOL:
		return castToBool(alloc, arr)
	case arrow.INT32:
		// DATE is materialized as Unix epoch days (Int32).
		return castToDate(alloc, arr)
	case arrow.TIMESTAMP:
		return castToTimestamp(alloc, arr)
	}
	return nil, fmt.Errorf("arrow cast: unsupported target type %s", to)
}

func castToString(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	switch a := arr.(type) {
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(strconv.FormatInt(a.Value(i), 10))
		}
	case *array.Int32:
		// Date columns are carried in Arrow as int32 days since the Unix epoch.
		// Render with the same layout KWDB uses for CAST(date AS string)
		// (DDate.Format -> pgdate "2006-01-02"), so Arrow and row-based output match.
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			t := time.Unix(int64(a.Value(i))*86400, 0).UTC()
			b.Append(t.Format("2006-01-02"))
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(strconv.FormatFloat(a.Value(i), 'g', -1, 64))
		}
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(strconv.FormatBool(a.Value(i)))
		}
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	case *array.Decimal128:
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			d := decimal128ToApd(a.Value(i), scale)
			b.Append(d.String())
		}
	case *array.Timestamp:
		// Arrow stores the timestamp as epoch units in UTC. Render it with the
		// same layout KWDB uses for CAST(timestamp AS string)
		// (DTimestamp.Format -> TimestampOutputFormat, UTC), so the Arrow path
		// and the row-based path produce identical output.
		unit := a.DataType().(*arrow.TimestampType).Unit
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			t := a.Value(i).ToTime(unit)
			b.Append(t.Format(tree.TimestampOutputFormat))
		}
	default:
		return nil, fmt.Errorf("arrow cast to STRING: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

func castToInt64(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewInt64Builder(alloc)
	defer b.Release()
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			v, err := strconv.ParseInt(strings.TrimSpace(a.Value(i)), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("arrow cast STRING->INT: %v", err)
			}
			b.Append(v)
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(int64(a.Value(i)))
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			if a.Value(i) {
				b.Append(1)
			} else {
				b.Append(0)
			}
		}
	case *array.Decimal128:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i).BigInt().Int64())
		}
	default:
		return nil, fmt.Errorf("arrow cast to INT: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

func castToFloat64(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewFloat64Builder(alloc)
	defer b.Release()
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(a.Value(i)), 64)
			if err != nil {
				return nil, fmt.Errorf("arrow cast STRING->FLOAT: %v", err)
			}
			b.Append(v)
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(float64(a.Value(i)))
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	case *array.Decimal128:
		// The decimal128 value is scaled by 10^scale; recover the true float.
		var scale int32
		if dt, ok := a.DataType().(*arrow.Decimal128Type); ok {
			scale = dt.Scale
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i).ToFloat64(scale))
		}
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			if a.Value(i) {
				b.Append(1)
			} else {
				b.Append(0)
			}
		}
	default:
		return nil, fmt.Errorf("arrow cast to FLOAT: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// castToDecimal converts a numeric/text column into a fixed-scale decimal128
// column. The target scale is taken from the Decimal128Type; source values are
// rounded half-away-from-zero to that scale via apdToDecimal128 (shared with the
// aggregator path). Supported sources: Int64, Float64, String.
func castToDecimal(alloc memory.Allocator, arr arrow.Array, dt *arrow.Decimal128Type) (arrow.Array, error) {
	b := array.NewDecimal128Builder(alloc, dt)
	defer b.Release()
	switch a := arr.(type) {
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			d := apd.New(a.Value(i), 0)
			num, err := apdToDecimal128(d, dt.Scale)
			if err != nil {
				return nil, fmt.Errorf("arrow cast INT->DECIMAL: %v", err)
			}
			b.Append(num)
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			d, _, err := apd.NewFromString(strconv.FormatFloat(a.Value(i), 'g', -1, 64))
			if err != nil {
				return nil, fmt.Errorf("arrow cast FLOAT->DECIMAL: %v", err)
			}
			num, err := apdToDecimal128(d, dt.Scale)
			if err != nil {
				return nil, fmt.Errorf("arrow cast FLOAT->DECIMAL: %v", err)
			}
			b.Append(num)
		}
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			d, _, err := apd.NewFromString(strings.TrimSpace(a.Value(i)))
			if err != nil {
				return nil, fmt.Errorf("arrow cast STRING->DECIMAL: %v", err)
			}
		num, err := apdToDecimal128(d, dt.Scale)
		if err != nil {
			return nil, fmt.Errorf("arrow cast STRING->DECIMAL: %v", err)
		}
		b.Append(num)
	}
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			var d *apd.Decimal
			if a.Value(i) {
				d = apd.New(1, 0)
			} else {
				d = apd.New(0, 0)
			}
			num, err := apdToDecimal128(d, dt.Scale)
			if err != nil {
				return nil, fmt.Errorf("arrow cast BOOL->DECIMAL: %v", err)
			}
			b.Append(num)
		}
	default:
		return nil, fmt.Errorf("arrow cast to DECIMAL: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// castToBool converts a column into a boolean column. Supported sources:
// String (true/t/1 -> true, false/f/0 -> false, case-insensitive), Int64,
// Float64, Decimal128 (non-zero -> true), Boolean.
func castToBool(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewBooleanBuilder(alloc)
	defer b.Release()
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			switch strings.ToLower(strings.TrimSpace(a.Value(i))) {
			case "true", "t", "1":
				b.Append(true)
			case "false", "f", "0":
				b.Append(false)
			default:
				return nil, fmt.Errorf("arrow cast STRING->BOOL: invalid boolean %q", a.Value(i))
			}
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i) != 0)
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i) != 0)
		}
	case *array.Decimal128:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i).BigInt().Sign() != 0)
		}
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	default:
		return nil, fmt.Errorf("arrow cast to BOOL: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// castToDate converts a column into a DATE column, materialized as Unix epoch
// days (Int32) to match arrowDataTypeForKWType. Supported sources: String
// ("2006-01-02"), Int64/Float64 (days), Decimal128 (days), Timestamp (truncated
// to day), Int32 (days, identity).
func castToDate(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewInt32Builder(alloc)
	defer b.Release()
	const microsPerDay = int64(24 * 60 * 60 * 1e6)
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			t, err := time.Parse("2006-01-02", strings.TrimSpace(a.Value(i)))
			if err != nil {
				return nil, fmt.Errorf("arrow cast STRING->DATE: %v", err)
			}
			b.Append(int32(t.Unix() / (24 * 60 * 60)))
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(int32(a.Value(i)))
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(int32(a.Value(i)))
		}
	case *array.Decimal128:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(int32(a.Value(i).BigInt().Int64()))
		}
	case *array.Int32:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	case *array.Timestamp:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(int32(int64(a.Value(i)) / microsPerDay))
		}
	default:
		return nil, fmt.Errorf("arrow cast to DATE: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// castToTimestamp converts a column into a TIMESTAMP column, materialized as
// microseconds since the Unix epoch (Timestamp_us) to match
// arrowDataTypeForKWType. Supported sources: String (RFC3339 / "2006-01-02
// 15:04:05" / "2006-01-02"), Int64/Float64 (Unix seconds), Decimal128 (Unix
// seconds), Date (Unix days), Timestamp (identity).
func castToTimestamp(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	b := array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
	defer b.Release()
	const microsPerSec = int64(1e6)
	const microsPerDay = int64(24 * 60 * 60 * 1e6)
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			s := strings.TrimSpace(a.Value(i))
			var t time.Time
			var err error
			switch {
			case strings.Contains(s, "T") || strings.Contains(s, "Z") || strings.Contains(s, "+"):
				t, err = time.Parse(time.RFC3339Nano, s)
			case strings.Contains(s, ":"):
				t, err = time.Parse("2006-01-02 15:04:05.999999", s)
			default:
				t, err = time.Parse("2006-01-02", s)
			}
			if err != nil {
				return nil, fmt.Errorf("arrow cast STRING->TIMESTAMP: %v", err)
			}
			b.Append(arrow.Timestamp(t.UnixMicro()))
		}
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Timestamp(a.Value(i) * microsPerSec))
		}
	case *array.Float64:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Timestamp(int64(a.Value(i) * float64(microsPerSec))))
		}
	case *array.Decimal128:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Timestamp(a.Value(i).ToFloat64(0) * float64(microsPerSec)))
		}
	case *array.Int32:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Timestamp(int64(a.Value(i)) * microsPerDay))
		}
	case *array.Timestamp:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(a.Value(i))
		}
	default:
		return nil, fmt.Errorf("arrow cast to TIMESTAMP: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// arrowOperandColumn returns the input record column with the given field name.
// The planner ships aggregate/filter operand columns as positional indices
// encoded as "colN" (see buildArrowAggSpec). Input record schema columns are
// named by their KWDB type name, so a name lookup usually fails; in that case
// we fall back to parsing the trailing integer of "colN" as the positional index.
func arrowOperandColumn(rec arrow.Record, name string) arrow.Array {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) > 0 {
		return rec.Column(idx[0])
	}
	var n int
	if _, err := fmt.Sscanf(name, "col%d", &n); err == nil && n >= 0 && n < int(rec.NumCols()) {
		return rec.Column(n)
	}
	panic(fmt.Sprintf("arrow operand column %q not found", name))
}
