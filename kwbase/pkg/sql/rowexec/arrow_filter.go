// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
)

// ArrowFilterSpec is the unified (compute) description of a boolean filter.
// It is a recursive boolean expression tree evaluated entirely by arrow/compute.
//
// Grammar:
//   comparison:  Func in {equal, not_equal, less, less_equal, greater,
//                greater_equal}, Operands are leaves (columns / constants).
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
	spec  ArrowFilterSpec
	alloc memory.Allocator
}

func newArrowFilterCore(spec ArrowFilterSpec, alloc memory.Allocator) arrowFilterCore {
	return arrowFilterCore{spec: spec, alloc: alloc}
}

// arrowFilter is a UnifiedProcessor that selects the matching rows of its
// input record (all columns preserved) using the Arrow compute engine.
type arrowFilter struct {
	arrowFilterCore
	input UnifiedProcessor
}

// NewArrowFilter builds a filter operator over input.
func NewArrowFilter(alloc memory.Allocator, input UnifiedProcessor, spec ArrowFilterSpec) UnifiedProcessor {
	return &arrowFilter{arrowFilterCore: newArrowFilterCore(spec, alloc), input: input}
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

	// Select the matching rows from every column via the native arrow compute
	// Filter kernel. A null predicate bit is dropped (DefaultFilterOptions),
	// which matches the prior "mask.IsNull(i) || !mask.Value(i)" semantics,
	// eliminating the per-row index-gathering loop.
	filterDatum := compute.NewDatum(mask)
	defer filterDatum.Release()
	results := make([]compute.Datum, rec.NumCols())
	cols := make([]arrow.Array, rec.NumCols())
	for ci := 0; ci < int(rec.NumCols()); ci++ {
		col := rec.Column(ci)
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
	out := array.NewRecord(rec.Schema(), cols, int64(cols[0].Len()))
	for _, r := range results {
		r.Release()
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
	default:
		return nil, fmt.Errorf("arrow cast to FLOAT: unsupported source %T", arr)
	}
	return b.NewArray(), nil
}

// arrowOperandColumn returns the input record column with the given field name.
func arrowOperandColumn(rec arrow.Record, name string) arrow.Array {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		panic(fmt.Sprintf("arrow operand column %q not found", name))
	}
	return rec.Column(idx[0])
}
