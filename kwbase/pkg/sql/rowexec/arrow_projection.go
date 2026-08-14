// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// ArrowArg is a single argument to an Arrow compute function. It is either an
// input record column (ColName set), a constant literal (Scalar set), or a
// nested arithmetic expression (Binary set).
type ArrowArg struct {
	// ColName is the name of the input record column (e.g. "col0"). Non-empty
	// when the argument is a column reference.
	ColName string
	// Scalar is a constant argument. Non-nil when the argument is a literal
	// (takes precedence over ColName when both are set).
	Scalar compute.Datum
	// Binary is a nested arithmetic expression (add/sub/mul/div) whose arguments
	// are themselves ArrowArgs.
	Binary *ArrowArgBinary
	// Cast is a type conversion applied to Arg before the parent function
	// consumes it. It supports CAST(col AS ...) inside arrow-computable
	// expressions (e.g. CAST(i AS STRING) LIKE '1%').
	Cast *ArrowArgCast
	// ConstSet is a set of constant values used as the right operand of an IN
	// / NOT IN predicate (e.g. col IN (1,2,3)). Exactly one of Scalar or
	// ConstSet is set; it is only honored by the arrow filter "in"/"not_in"
	// kernels.
	ConstSet []compute.Datum
	// Computed is a nested projection function (e.g. substring/trim/concat on a
	// column) used as an argument inside an arrow-computable filter or
	// projection expression. It lets "substring(col,1,3) = 'abc'" run fully in
	// the Arrow engine without falling back to a row-by-row tree.Datum path.
	Computed *ArrowProjectionSpec
	// Case is a CASE/COALESCE value leaf inside a filter predicate. It reuses
	// the projection CASE spec form so all value types and nested branches are
	// supported (evaluated by the projection CASE evaluator).
	Case *ArrowProjectionSpec
}

// ArrowArgBinary is a nested arithmetic expression used as a filter operand.
type ArrowArgBinary struct {
	Func string
	Args []ArrowArg
}

// ArrowArgCast is a type conversion applied to a leaf operand. It is evaluated
// by the arrow filter/projection kernels to support CAST inside predicates and
// renders.
type ArrowArgCast struct {
	Type arrow.DataType
	Arg  ArrowArg
	// SourceType is the planner (KWDB) type of the operand before the cast. It is
	// needed because Arrow stores both INT32 and DATE as int32, so the string
	// conversion of a plain INT32 must print the integer value while a DATE must
	// print the "2006-01-02" layout. Without it, CAST(int_col AS STRING) would be
	// mistaken for a date and render as an epoch day.
	SourceType *types.T
}

// ArrowProjectionSpec describes one projected output column computed from input
// record columns via an Arrow compute function (e.g. "add", "sub", "mul",
// "div", "negate", "copy"). These function names correspond to the kernels
// registered in github.com/apache/arrow/go/v17/arrow/compute.
type ArrowProjectionSpec struct {
	// OutputName is the name of the produced column.
	OutputName string
	// Func is the arrow/compute function name, e.g. "add".
	Func string
	// Args are the function arguments (columns and/or scalar constants).
	Args []ArrowArg
	// Kind overrides the evaluation mode. When empty (the default), the spec is
	// a plain arrow/compute function (Func + Args). "case" selects one of the
	// Branches' Then values by evaluating each Branch.When as a boolean mask,
	// falling back to Else. "isnull" produces a boolean mask marking null input
	// rows (used as a CASE/COALESCE WHEN condition).
	Kind string `json:"kind,omitempty"`
	// Branches are the WHEN/THEN pairs of a case spec. Only set when Kind=="case".
	Branches []ArrowProjectionBranch `json:"branches,omitempty"`
	// Else is the fallback value of a case spec. Only set when Kind=="case".
	Else *ArrowProjectionSpec `json:"else,omitempty"`
	// TZ marks that the timestamp operand of a datetime function is a
	// TIMESTAMPTZ, so the executor must honor the session time zone. Only set
	// when Kind == "datetime".
	TZ bool `json:"tz,omitempty"`
}

// ArrowProjectionBranch is a single WHEN/THEN pair of a case spec. When is a
// boolean-producing spec (comparison or "isnull"); Then is the value produced
// when When is true for a given row.
type ArrowProjectionBranch struct {
	When *ArrowProjectionSpec `json:"when,omitempty"`
	Then *ArrowProjectionSpec `json:"then,omitempty"`
}

// arrowProjection is a UnifiedProcessor that evaluates projection expressions
// over Arrow Records using arrow/compute kernels. Per the unification plan this
// is the "deep unification" of a hot operator: the projection runs as a single
// vectorized pass over Arrow arrays (zero-copy ExecSpan views inside the
// kernel) instead of one scalar tree.Datum evaluation per row.
type arrowProjection struct {
	alloc   memory.Allocator
	input   UnifiedProcessor
	specs   []ArrowProjectionSpec
	evalCtx *tree.EvalContext
}

// NewArrowProjection builds a projection operator over the given input.
func NewArrowProjection(alloc memory.Allocator, input UnifiedProcessor, specs []ArrowProjectionSpec, evalCtx *tree.EvalContext) UnifiedProcessor {
	return &arrowProjection{alloc: alloc, input: input, specs: specs, evalCtx: evalCtx}
}

// Init implements UnifiedProcessor.
func (p *arrowProjection) Init(ctx context.Context) { p.input.Init(ctx) }

// Allocator implements UnifiedProcessor.
func (p *arrowProjection) Allocator() memory.Allocator { return p.alloc }

// Next implements UnifiedProcessor.
func (p *arrowProjection) Next(ctx context.Context) (arrow.Record, bool, error) {
	in, done, err := p.input.Next(ctx)
	if err != nil || done {
		return nil, done, err
	}
	defer in.Release()

	outFields := make([]arrow.Field, 0, len(p.specs))
	outCols := make([]arrow.Array, 0, len(p.specs))
	n := in.NumRows()
	for _, spec := range p.specs {
		col, err := p.eval(ctx, in, spec)
		if err != nil {
			for _, c := range outCols {
				c.Release()
			}
			return nil, false, err
		}
		// Arity guard (§7.9): every projected column must carry exactly n rows.
		// A scalar/constant column that eval emitted as a single element is
		// broadcast to n rows; any other arity mismatch is a genuine bug and is
		// surfaced as an error rather than a panic inside array.NewRecord.
		if col.Len() == 1 && n != 1 {
			broadcast := broadcastTo(p.alloc, col, int(n))
			col.Release()
			col = broadcast
		} else if col.Len() != int(n) {
			col.Release()
			for _, c := range outCols {
				c.Release()
			}
			return nil, false, fmt.Errorf("arrow projection: column %q has %d rows, expected %d", spec.OutputName, col.Len(), n)
		}
		outFields = append(outFields, arrow.Field{Name: spec.OutputName, Type: col.DataType(), Nullable: true})
		outCols = append(outCols, col)
	}
	schema := arrow.NewSchema(outFields, nil)
	return array.NewRecord(schema, outCols, n), false, nil
}

// eval evaluates a single projection spec against the input Record by invoking
// the arrow/compute kernel. Input columns are wrapped as compute.ArrayDatum
// (a zero-copy view over the Arrow buffer) and the result is unwrapped back
// into an Arrow array.
func (p *arrowProjection) eval(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	switch spec.Kind {
	case "case":
		return p.evalCase(ctx, in, spec)
	case "isnull", "isnotnull":
		return p.evalIsNull(ctx, in, spec)
	case "datetime":
		return p.evalArrowDatetimeFunc(ctx, in, spec)
	}
	if spec.Func == "copy" {
		// Passthrough: copy an input column directly, or materialize a scalar
		// constant into a single-value array (used for CASE/COALESCE THEN/ELSE
		// values and WHEN keys).
		if spec.Args[0].Scalar != nil {
			ad, ok := spec.Args[0].Scalar.(*compute.ScalarDatum)
			if !ok {
				return nil, fmt.Errorf("projection copy: expected scalar datum, got %T", spec.Args[0].Scalar)
			}
			// Broadcast the constant to the full input arity. A scalar literal
			// appearing in a projection must repeat for every input row; emitting
			// a single-element array would make array.NewRecord (Next) panic with
			// a row-count mismatch against the other (per-row) output columns.
			return scalar.MakeArrayFromScalar(ad.Value, int(in.NumRows()), p.alloc)
		}
		idx := in.Schema().FieldIndices(spec.Args[0].ColName)
		if len(idx) == 0 {
			return nil, fmt.Errorf("projection input column %q not found", spec.Args[0].ColName)
		}
		col := in.Column(idx[0])
		out := array.NewSlice(col, 0, int64(col.Len()))
		// Render-side CAST: apply the target type conversion when the planner
		// attached one (e.g. SELECT CAST(col AS DATE)). This makes projection
		// top-level CAST a full Arrow operation instead of a silent passthrough.
		if cast := spec.Args[0].Cast; cast != nil {
			casted, err := castArrowArray(p.alloc, out, cast.Type, cast.SourceType)
			out.Release()
			if err != nil {
				return nil, err
			}
			return casted, nil
		}
		return out, nil
	}
	// String-function kernels: the vendored arrow/compute module does not ship
	// string kernels, so we evaluate them as native vectorized Go loops over the
	// Arrow string arrays. This keeps string projection on the same Arrow path.
	switch spec.Func {
	case "length", "octet_length", "lower", "upper", "concat", "substring",
		"trim", "ltrim", "rtrim", "btrim", "replace", "overlay", "split_part":
		return p.evalArrowStringFunc(ctx, in, spec)
	}
	args := make([]compute.Datum, len(spec.Args))
	for i, a := range spec.Args {
		if a.Scalar != nil {
			args[i] = a.Scalar
			continue
		}
		idx := in.Schema().FieldIndices(a.ColName)
		if len(idx) == 0 {
			return nil, fmt.Errorf("projection input column %q not found", a.ColName)
		}
		col := in.Column(idx[0])
		if a.Cast != nil {
			// Render-side CAST: convert the operand to the target type before
			// feeding it to the compute function. Reuses the same cast kernels
			// as the Arrow filter path.
			casted, err := castArrowArray(p.alloc, col, a.Cast.Type, a.Cast.SourceType)
			if err != nil {
				return nil, err
			}
			col = casted
		}
		args[i] = compute.NewDatum(col)
	}
	if casted, err := arrowNormalizeIntWidths(p.alloc, args); err != nil {
		return nil, err
	} else if casted != nil {
		defer func() {
			for _, c := range casted {
				c.Release()
			}
		}()
	}
	res, err := compute.CallFunction(ctx, spec.Func, nil, args...)
	if err != nil {
		return nil, err
	}
	ad, ok := res.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("expected array result from %q, got %T", spec.Func, res)
	}
	return ad.MakeArray(), nil
}

// evalIsNull produces a boolean mask array marking the rows where the single
// argument (a column or scalar) is NULL ("isnull") or is NOT NULL
// ("isnotnull"). COALESCE rewrites to CASE WHEN arg IS NOT NULL THEN arg ...,
// so the WHEN branch uses the "isnotnull" polarity.
func (p *arrowProjection) evalIsNull(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if len(spec.Args) != 1 {
		return nil, fmt.Errorf("arrow isnull expects exactly one argument, got %d", len(spec.Args))
	}
	notNull := spec.Kind == "isnotnull"
	b := array.NewBooleanBuilder(p.alloc)
	defer b.Release()
	if spec.Args[0].Scalar != nil {
		sd, ok := spec.Args[0].Scalar.(*compute.ScalarDatum)
		if !ok {
			return nil, fmt.Errorf("arrow isnull: expected scalar datum, got %T", spec.Args[0].Scalar)
		}
		isNull := !sd.Value.IsValid()
		if notNull {
			b.Append(!isNull)
		} else {
			b.Append(isNull)
		}
		return b.NewArray(), nil
	}
	idx := in.Schema().FieldIndices(spec.Args[0].ColName)
	if len(idx) == 0 {
		return nil, fmt.Errorf("projection input column %q not found", spec.Args[0].ColName)
	}
	col := in.Column(idx[0])
	for i := 0; i < col.Len(); i++ {
		if notNull {
			b.Append(!col.IsNull(i))
		} else {
			b.Append(col.IsNull(i))
		}
	}
	return b.NewArray(), nil
}

// evalCase evaluates a CASE/COALESCE expression. For each row it picks the
// first branch whose When mask is true, falling back to Else. All Then/Else
// values are expected to share the result type (the planner casts
// heterogeneous branches to the result type).
func (p *arrowProjection) evalCase(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if spec.Else == nil {
		return nil, fmt.Errorf("arrow case requires an ELSE branch")
	}
	// n is the number of input rows; branch/else scalars are broadcast to n.
	n := in.Column(0).Len()
	elseArr, err := p.eval(ctx, in, *spec.Else)
	if err != nil {
		return nil, err
	}
	defer elseArr.Release()
	elseArr = broadcastTo(p.alloc, elseArr, n)
	type branchEval struct {
		mask *array.Boolean
		val  arrow.Array
	}
	branches := make([]branchEval, len(spec.Branches))
	defer func() {
		for _, b := range branches {
			if b.mask != nil {
				b.mask.Release()
			}
			if b.val != nil {
				b.val.Release()
			}
		}
	}()
	for i, br := range spec.Branches {
		m, err := p.eval(ctx, in, *br.When)
		if err != nil {
			return nil, err
		}
		mb, ok := m.(*array.Boolean)
		if !ok {
			m.Release()
			return nil, fmt.Errorf("arrow case branch WHEN did not evaluate to a boolean array (got %T)", m)
		}
		v, err := p.eval(ctx, in, *br.Then)
		if err != nil {
			mb.Release()
			return nil, err
		}
		maskArr := broadcastTo(p.alloc, mb, n)
		boolMask, ok := maskArr.(*array.Boolean)
		if !ok {
			return nil, fmt.Errorf("arrow case: expected boolean mask, got %T", maskArr)
		}
		branches[i] = branchEval{mask: boolMask, val: broadcastTo(p.alloc, v, n)}
	}
	// The planner relies on tree.TypedExpr.Equivalent (width-insensitive for the
	// IntFamily) when deciding whether a CASE branch needs a cast to the result
	// type, so a narrow int branch (e.g. INT4 column) and a wide branch (e.g.
	// a*100 -> INT64) can reach the executor with mismatched physical widths.
	// Widen every Int16/Int32 branch value and the ELSE value to INT64 here so
	// the builder switch below only has to handle one integer width. This matches
	// Cockroach's CASE result typing (narrowest common type is INT64 for any
	// int/int64 mix) and keeps the produced array consistent with the planner's
	// declared output type.
	for i := range branches {
		if branches[i].val.DataType().ID() == arrow.INT16 || branches[i].val.DataType().ID() == arrow.INT32 {
			widened, err := castArrowArray(p.alloc, branches[i].val, arrow.PrimitiveTypes.Int64, nil)
			if err != nil {
				return nil, err
			}
			branches[i].val.Release()
			branches[i].val = widened
		}
	}
	if elseArr.DataType().ID() == arrow.INT16 || elseArr.DataType().ID() == arrow.INT32 {
		widened, err := castArrowArray(p.alloc, elseArr, arrow.PrimitiveTypes.Int64, nil)
		if err != nil {
			return nil, err
		}
		elseArr.Release()
		elseArr = widened
	}
	switch e := elseArr.(type) {
	case *array.Int16:
		b := array.NewInt16Builder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			chosen := e
			for _, br := range branches {
				if br.mask.Value(i) {
					chosen = br.val.(*array.Int16)
					break
				}
			}
			if chosen.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(chosen.Value(i))
			}
		}
		return b.NewArray(), nil
	case *array.Int32:
		b := array.NewInt32Builder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			chosen := e
			for _, br := range branches {
				if br.mask.Value(i) {
					chosen = br.val.(*array.Int32)
					break
				}
			}
			if chosen.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(chosen.Value(i))
			}
		}
		return b.NewArray(), nil
	case *array.Int64:
		b := array.NewInt64Builder(p.alloc)
		defer b.Release()
	for i := 0; i < n; i++ {
		chosen := e
		for _, br := range branches {
			if br.mask.Value(i) {
				chosen = br.val.(*array.Int64)
				break
			}
		}
		if chosen.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(chosen.Value(i))
		}
	}
	return b.NewArray(), nil
case *array.Float64:
		b := array.NewFloat64Builder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			chosen := e
			for _, br := range branches {
				if br.mask.Value(i) {
					chosen = br.val.(*array.Float64)
					break
				}
			}
			if chosen.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(chosen.Value(i))
			}
		}
		return b.NewArray(), nil
	case *array.String:
		b := array.NewStringBuilder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			chosen := e
			for _, br := range branches {
				if br.mask.Value(i) {
					chosen = br.val.(*array.String)
					break
				}
			}
			if chosen.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(chosen.Value(i))
			}
		}
		return b.NewArray(), nil
	case *array.Boolean:
		b := array.NewBooleanBuilder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			chosen := e
			for _, br := range branches {
				if br.mask.Value(i) {
					chosen = br.val.(*array.Boolean)
					break
				}
			}
			if chosen.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(chosen.Value(i))
			}
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("arrow case result type %T is not supported", elseArr)
	}
}

// evalArrowDatetimeFunc evaluates a datetime projection function (extract /
// date_trunc) as a native vectorized Go loop over the Arrow Timestamp column.
// The vendored arrow/compute module has no datetime kernels, and reusing the
// canonical builtins.ExtractTimeSpanFromTimestamp / TruncateTimestamp helpers
// keeps the Arrow path bit-for-bit consistent with the row-by-row path.
//
// The timestamp operand is stored as an Arrow Timestamp_us (microseconds since
// the epoch); we reconstruct a time.Time at the session location for TIMESTAMPTZ
// operands (spec.TZ) or at UTC for TIMESTAMP operands, matching the row path
// (which calls fromTS.Time / fromTSTZ.Time.In(ctx.GetLocation())).
func (p *arrowProjection) evalArrowDatetimeFunc(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if spec.Func == "now" {
		// now() / current_timestamp / transaction_timestamp: emits the current
		// statement timestamp as a constant TIMESTAMPTZ column (timezone-aware).
		// The row path resolves now() to evalCtx.GetStmtTimestamp(), so we match
		// that rather than sampling time per row.
		n := int(in.NumRows())
		loc := p.evalCtx.GetLocation()
		ts := p.evalCtx.GetStmtTimestamp().In(loc)
		us := arrow.Timestamp(ts.UnixMicro())
		b := array.NewTimestampBuilder(p.alloc, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()})
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(us)
		}
		return b.NewArray(), nil
	}
	if spec.Func == "age" {
		// age(ts)        =>  transaction_timestamp - ts
		// age(end, begin)=>  begin - end
		// The first argument is either the "__TXN_TS__" sentinel (single-arg
		// form, resolved against evalCtx.GetTxnTimestamp) or a timestamp column
		// (begin). The second argument is always the timestamp column / operand.
		beginTs, err := p.arrowDatetimeTimestampArg(ctx, in, spec.Args[0])
		if err != nil {
			return nil, err
		}
		endTs, err := p.arrowDatetimeTimestampArg(ctx, in, spec.Args[1])
		if err != nil {
			return nil, err
		}
		defer beginTs.Release()
		defer endTs.Release()
		n := int(in.NumRows())
		b := array.NewStringBuilder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if beginTs.IsNull(i) || endTs.IsNull(i) {
				b.AppendNull()
				continue
			}
			begin := arrowMicroToTime(beginTs.Value(i), spec.TZ, p)
			end := arrowMicroToTime(endTs.Value(i), spec.TZ, p)
			di, err := tree.TimestampDifference(
				p.evalCtx,
				&tree.DTimestampTZ{Time: begin},
				&tree.DTimestampTZ{Time: end},
			)
			if err != nil {
				return nil, err
			}
			b.Append(di.String())
		}
		return b.NewStringArray(), nil
	}
	if len(spec.Args) != 2 {
		return nil, fmt.Errorf("arrow datetime func expects 2 args, got %d", len(spec.Args))
	}
	// First argument: the constant field string (e.g. "year").
	fd, ok := spec.Args[0].Scalar.(*compute.ScalarDatum)
	if !ok {
		return nil, fmt.Errorf("arrow datetime func: field must be a scalar, got %T", spec.Args[0].Scalar)
	}
	fieldScalar, ok := fd.Value.(*scalar.String)
	if !ok {
		return nil, fmt.Errorf("arrow datetime func: field must be a string scalar, got %T", fd.Value)
	}
	field := strings.ToLower(string(fieldScalar.Value.Bytes()))
	// Second argument: the timestamp column.
	idx := in.Schema().FieldIndices(spec.Args[1].ColName)
	if len(idx) == 0 {
		return nil, fmt.Errorf("arrow datetime func: input column %q not found", spec.Args[1].ColName)
	}
	tsCol := in.Column(idx[0])
	tsArr, ok := tsCol.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("arrow datetime func: expected Timestamp column, got %T", tsCol)
	}
	n := tsArr.Len()
	loc := time.UTC
	if spec.TZ {
		loc = p.evalCtx.GetLocation()
	}
	switch spec.Func {
	case "extract":
		b := array.NewFloat64Builder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if tsArr.IsNull(i) {
				b.AppendNull()
				continue
			}
			t := time.UnixMicro(int64(tsArr.Value(i))).In(loc)
			var d tree.Datum
			var err error
			if spec.TZ {
				d, err = builtins.ExtractTimeSpanFromTimestampTZ(p.evalCtx, t, field)
			} else {
				d, err = builtins.ExtractTimeSpanFromTimestamp(p.evalCtx, t, field)
			}
			if err != nil {
				return nil, err
			}
			f, ok := d.(*tree.DFloat)
			if !ok {
				return nil, fmt.Errorf("arrow extract: expected float, got %T", d)
			}
			b.Append(float64(*f))
		}
		return b.NewFloat64Array(), nil
	case "date_trunc":
		b := array.NewTimestampBuilder(p.alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		defer b.Release()
		for i := 0; i < n; i++ {
			if tsArr.IsNull(i) {
				b.AppendNull()
				continue
			}
			t := time.UnixMicro(int64(tsArr.Value(i))).In(loc)
			d, err := builtins.TruncateTimestamp(p.evalCtx, t, field)
			if err != nil {
				return nil, err
			}
			ts, ok := d.(*tree.DTimestampTZ)
			if !ok {
				return nil, fmt.Errorf("arrow date_trunc: expected timestamp, got %T", d)
			}
			b.Append(arrow.Timestamp(ts.Time.UnixMicro()))
		}
		return b.NewTimestampArray(), nil
	}
	return nil, fmt.Errorf("arrow datetime func: unsupported func %q", spec.Func)
}

// ageTxnTsSentinel matches the planner-side placeholder for the transaction
// timestamp in a single-argument age(ts) projection.
const ageTxnTsSentinelExecutor = "__TXN_TS__"

// arrowDatetimeTimestampArg resolves one age() argument into a Timestamp array
// of length n. A literal "__TXN_TS__" sentinel expands to the runtime
// transaction timestamp (a constant broadcast column); an ordinary column is
// pulled by name and broadcast if it is a single-element constant.
func (p *arrowProjection) arrowDatetimeTimestampArg(ctx context.Context, in arrow.Record, arg ArrowArg) (*array.Timestamp, error) {
	n := int(in.NumRows())
	if arg.Scalar != nil {
		if sd, ok := arg.Scalar.(*compute.ScalarDatum); ok {
			if s, ok := sd.Value.(*scalar.String); ok {
				if string(s.Value.Bytes()) == ageTxnTsSentinelExecutor {
					txn := p.evalCtx.GetTxnTimestamp(time.Microsecond)
					us := arrow.Timestamp(txn.Time.UnixMicro())
					b := array.NewTimestampBuilder(p.alloc, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: txn.Time.Location().String()})
					defer b.Release()
					for i := 0; i < n; i++ {
						b.Append(us)
					}
					return b.NewTimestampArray(), nil
				}
			}
		}
	}
	if arg.ColName == "" {
		return nil, fmt.Errorf("arrow age: timestamp argument is neither a column nor the txn sentinel")
	}
	idx := in.Schema().FieldIndices(arg.ColName)
	if len(idx) == 0 {
		return nil, fmt.Errorf("arrow age: input column %q not found", arg.ColName)
	}
	tsCol := in.Column(idx[0])
	tsArr, ok := tsCol.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("arrow age: expected Timestamp column, got %T", tsCol)
	}
	if tsArr.Len() == n {
		return tsArr, nil
	}
	// Broadcast a single-element constant across all rows.
	b := array.NewTimestampBuilder(p.alloc, tsArr.DataType().(*arrow.TimestampType))
	defer b.Release()
	if tsArr.Len() == 1 && !tsArr.IsNull(0) {
		v := tsArr.Value(0)
		for i := 0; i < n; i++ {
			b.Append(v)
		}
	} else {
		for i := 0; i < n; i++ {
			b.AppendNull()
		}
	}
	return b.NewTimestampArray(), nil
}

// arrowMicroToTime converts an Arrow microsecond timestamp into a time.Time in
// the projection's timezone (UTC when spec.TZ is false).
func arrowMicroToTime(us arrow.Timestamp, tz bool, p *arrowProjection) time.Time {
	loc := time.UTC
	if tz {
		loc = p.evalCtx.GetLocation()
	}
	return time.UnixMicro(int64(us)).In(loc)
}

// broadcastTo returns an array of length n. If src already has length n it is
// returned unchanged (and the caller still owns/releases it). If src is a single
// scalar value (length 1) it is replicated into a fresh length-n array. This
// lets CASE/COALESCE THEN/ELSE constants broadcast across every input row.
func broadcastTo(alloc memory.Allocator, src arrow.Array, n int) arrow.Array {
	if src.Len() == n {
		return src
	}
	if src.Len() != 1 {
		// Unexpected shape: fall back to the source as-is; the type switch in
		// evalCase will surface a clearer error if lengths still mismatch.
		return src
	}
	defer src.Release()
	switch a := src.(type) {
	case *array.Int64:
		b := array.NewInt64Builder(alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if a.IsNull(0) {
				b.AppendNull()
			} else {
				b.Append(a.Value(0))
			}
		}
		return b.NewArray()
	case *array.Float64:
		b := array.NewFloat64Builder(alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if a.IsNull(0) {
				b.AppendNull()
			} else {
				b.Append(a.Value(0))
			}
		}
		return b.NewArray()
	case *array.String:
		b := array.NewStringBuilder(alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if a.IsNull(0) {
				b.AppendNull()
			} else {
				b.Append(a.Value(0))
			}
		}
		return b.NewArray()
	case *array.Boolean:
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if a.IsNull(0) {
				b.AppendNull()
			} else {
				b.Append(a.Value(0))
			}
		}
		return b.NewArray()
	default:
		return src
	}
}

// resolvedStrArg is a string operand resolved to either a column or a constant.
type resolvedStrArg struct {
	col       *array.String
	constStr  string
	constNull bool
	isCol     bool
}

// resolvedIntArg is an int operand resolved to either a column or a constant.
type resolvedIntArg struct {
	col       *array.Int64
	constVal  int64
	constNull bool
	isCol     bool
}

// resolveStrArg resolves a string projection operand to a column or constant.
func (p *arrowProjection) resolveStrArg(in arrow.Record, a ArrowArg) (resolvedStrArg, error) {
	if a.Scalar != nil {
		sd, ok := a.Scalar.(*compute.ScalarDatum)
		if !ok {
			return resolvedStrArg{}, fmt.Errorf("expected scalar datum for string arg, got %T", a.Scalar)
		}
		sc := sd.Value
		if !sc.IsValid() {
			return resolvedStrArg{constNull: true}, nil
		}
		s, ok := sc.(*scalar.String)
		if !ok {
			return resolvedStrArg{}, fmt.Errorf("expected string constant, got %s", sc.DataType())
		}
		return resolvedStrArg{constStr: s.String()}, nil
	}
	idxs := in.Schema().FieldIndices(a.ColName)
	if len(idxs) == 0 {
		return resolvedStrArg{}, fmt.Errorf("projection input column %q not found", a.ColName)
	}
	c, ok := in.Column(idxs[0]).(*array.String)
	if !ok {
		return resolvedStrArg{}, fmt.Errorf("column %q is not a string array (%T)", a.ColName, in.Column(idxs[0]))
	}
	return resolvedStrArg{col: c, isCol: true}, nil
}

// resolveIntArg resolves an int projection operand to a column or constant.
func (p *arrowProjection) resolveIntArg(in arrow.Record, a ArrowArg) (resolvedIntArg, error) {
	if a.Scalar != nil {
		sd, ok := a.Scalar.(*compute.ScalarDatum)
		if !ok {
			return resolvedIntArg{}, fmt.Errorf("expected scalar datum for int arg, got %T", a.Scalar)
		}
		sc := sd.Value
		if !sc.IsValid() {
			return resolvedIntArg{constNull: true}, nil
		}
		v, ok := sc.(*scalar.Int64)
		if !ok {
			return resolvedIntArg{}, fmt.Errorf("expected int constant, got %s", sc.DataType())
		}
		return resolvedIntArg{constVal: v.Value}, nil
	}
	idxs := in.Schema().FieldIndices(a.ColName)
	if len(idxs) == 0 {
		return resolvedIntArg{}, fmt.Errorf("projection input column %q not found", a.ColName)
	}
	c, ok := in.Column(idxs[0]).(*array.Int64)
	if !ok {
		return resolvedIntArg{}, fmt.Errorf("column %q is not an int array (%T)", a.ColName, in.Column(idxs[0]))
	}
	return resolvedIntArg{col: c, isCol: true}, nil
}

// strValue returns the string for row i of a resolved string arg.
func strValue(a resolvedStrArg, i int) string {
	if a.isCol {
		return a.col.Value(i)
	}
	return a.constStr
}

// strIsNull reports whether row i of a resolved string arg is NULL.
func strIsNull(a resolvedStrArg, i int) bool {
	if a.isCol {
		return a.col.IsNull(i)
	}
	return a.constNull
}

// intArgValue returns the int for row i of a resolved int arg.
func intArgValue(a resolvedIntArg, i int) int64 {
	if a.isCol {
		return a.col.Value(i)
	}
	return a.constVal
}

// intIsNull reports whether row i of a resolved int arg is NULL.
func intIsNull(a resolvedIntArg, i int) bool {
	if a.isCol {
		return a.col.IsNull(i)
	}
	return a.constNull
}

// stringLen returns the character length (or byte length for octet mode) of s.
func stringLen(s string, octet bool) int64 {
	if octet {
		return int64(len(s))
	}
	return int64(utf8.RuneCountInString(s))
}

// sqlSubstring implements the SQL SUBSTRING(string, start[, length]) semantics
// on runes: start is 1-based and inclusive. A negative length is rejected to
// match the classic (rowexec/colexec) path, which raises
// "negative substring length N not allowed" rather than returning the empty
// string (PostgreSQL behavior).
func sqlSubstring(s string, start, length int64) (string, error) {
	runes := []rune(s)
	n := int64(len(runes))
	if start < 1 {
		start = 1
	}
	if start > n {
		return "", nil
	}
	if length < 0 {
		return "", fmt.Errorf("negative substring length %d not allowed", length)
	}
	rs := int(start) - 1
	re := int(n)
	if length >= 0 {
		re = rs + int(length)
		if re > int(n) {
			re = int(n)
		}
	}
	if re < rs {
		return "", nil
	}
	return string(runes[rs:re]), nil
}

// evalArrowStringFunc evaluates the string-function projection kernels (length/
// lower/upper/concat/substring) as native vectorized loops. arrow/compute in
// this vendored version does not ship string kernels.
func (p *arrowProjection) evalArrowStringFunc(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	n := int(in.NumRows())
	switch spec.Func {
	case "trim", "ltrim", "rtrim", "btrim":
		return p.evalArrowTrim(ctx, in, spec)
	case "replace":
		return p.evalArrowReplace(ctx, in, spec)
	case "overlay":
		return p.evalArrowOverlay(ctx, in, spec)
	case "split_part":
		return p.evalArrowSplitPart(ctx, in, spec)
	case "length", "octet_length":
		arg, err := p.resolveStrArg(in, spec.Args[0])
		if err != nil {
			return nil, err
		}
		b := array.NewInt64Builder(p.alloc)
		defer b.Release()
		if arg.isCol {
			for i := 0; i < n; i++ {
				if arg.col.IsNull(i) {
					b.AppendNull()
					continue
				}
				b.Append(stringLen(arg.col.Value(i), spec.Func == "octet_length"))
			}
		} else if arg.constNull {
			b.AppendNull()
		} else {
			b.Append(stringLen(arg.constStr, spec.Func == "octet_length"))
		}
		return b.NewArray(), nil
	case "lower", "upper":
		arg, err := p.resolveStrArg(in, spec.Args[0])
		if err != nil {
			return nil, err
		}
		f := strings.ToLower
		if spec.Func == "upper" {
			f = strings.ToUpper
		}
		b := array.NewStringBuilder(p.alloc)
		defer b.Release()
		if arg.isCol {
			for i := 0; i < n; i++ {
				if arg.col.IsNull(i) {
					b.AppendNull()
					continue
				}
				b.Append(f(arg.col.Value(i)))
			}
		} else if arg.constNull {
			b.AppendNull()
		} else {
			b.Append(f(arg.constStr))
		}
		return b.NewArray(), nil
	case "concat":
		args := make([]resolvedStrArg, len(spec.Args))
		for i, a := range spec.Args {
			r, err := p.resolveStrArg(in, a)
			if err != nil {
				return nil, err
			}
			args[i] = r
		}
		b := array.NewStringBuilder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			null := false
			var sb strings.Builder
			for _, a := range args {
				if a.isCol {
					if a.col.IsNull(i) {
						null = true
						break
					}
					sb.WriteString(a.col.Value(i))
				} else if a.constNull {
					null = true
					break
				} else {
					sb.WriteString(a.constStr)
				}
			}
			if null {
				b.AppendNull()
			} else {
				b.Append(sb.String())
			}
		}
		return b.NewArray(), nil
	case "substring":
		strArg, err := p.resolveStrArg(in, spec.Args[0])
		if err != nil {
			return nil, err
		}
		startArg, err := p.resolveIntArg(in, spec.Args[1])
		if err != nil {
			return nil, err
		}
		var lenArg *resolvedIntArg
		if len(spec.Args) == 3 {
			l, err := p.resolveIntArg(in, spec.Args[2])
			if err != nil {
				return nil, err
			}
			lenArg = &l
		}
		b := array.NewStringBuilder(p.alloc)
		defer b.Release()
		for i := 0; i < n; i++ {
			if strIsNull(strArg, i) || intIsNull(startArg, i) {
				b.AppendNull()
				continue
			}
			start := intArgValue(startArg, i)
			length := int64(-1)
			if lenArg != nil {
				if intIsNull(*lenArg, i) {
					b.AppendNull()
					continue
				}
				length = intArgValue(*lenArg, i)
			}
			res, err := sqlSubstring(strValue(strArg, i), start, length)
			if err != nil {
				return nil, err
			}
			b.Append(res)
		}
		return b.NewArray(), nil
	}
	return nil, fmt.Errorf("unsupported string projection function %q", spec.Func)
}

// evalArrowOverlay implements the SQL OVERLAY(str PLACING substr FROM start) as a
// native vectorized loop. The arguments arrive as (str, substr, start) where
// start is 1-based (the KWDB planner lowers the PLACING/FROM syntax into a
// 3-argument FuncExpr). NULL in any operand yields NULL; a start position less
// than 1 is rejected to match the classic path ("'start' must be positive").
func (p *arrowProjection) evalArrowOverlay(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if len(spec.Args) != 3 {
		return nil, fmt.Errorf("overlay expects 3 arguments, got %d", len(spec.Args))
	}
	strArg, err := p.resolveStrArg(in, spec.Args[0])
	if err != nil {
		return nil, err
	}
	subArg, err := p.resolveStrArg(in, spec.Args[1])
	if err != nil {
		return nil, err
	}
	startArg, err := p.resolveIntArg(in, spec.Args[2])
	if err != nil {
		return nil, err
	}
	b := array.NewStringBuilder(p.alloc)
	defer b.Release()
	n := int(in.NumRows())
	for i := 0; i < n; i++ {
		if strIsNull(strArg, i) || strIsNull(subArg, i) || intIsNull(startArg, i) {
			b.AppendNull()
			continue
		}
		str := strValue(strArg, i)
		sub := strValue(subArg, i)
		pos := intArgValue(startArg, i)
		if pos < 1 {
			return nil, fmt.Errorf("'start' must be positive: %d", pos)
		}
		runes := []rune(str)
		start := int(pos - 1)
		if start > len(runes) {
			start = len(runes)
		}
		b.Append(string(runes[:start]) + sub + string(runes[start:]))
	}
	return b.NewArray(), nil
}

// evalArrowSplitPart implements SQL SPLIT_PART(str, sep, n) as a native
// vectorized loop. The string is split on the separator and the n-th (1-based)
// field is returned. A non-positive field position is rejected to match the
// classic path ("field position N must be greater than zero").
func (p *arrowProjection) evalArrowSplitPart(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if len(spec.Args) != 3 {
		return nil, fmt.Errorf("split_part expects 3 arguments, got %d", len(spec.Args))
	}
	strArg, err := p.resolveStrArg(in, spec.Args[0])
	if err != nil {
		return nil, err
	}
	sepArg, err := p.resolveStrArg(in, spec.Args[1])
	if err != nil {
		return nil, err
	}
	nArg, err := p.resolveIntArg(in, spec.Args[2])
	if err != nil {
		return nil, err
	}
	b := array.NewStringBuilder(p.alloc)
	defer b.Release()
	n := int(in.NumRows())
	for i := 0; i < n; i++ {
		if strIsNull(strArg, i) || strIsNull(sepArg, i) || intIsNull(nArg, i) {
			b.AppendNull()
			continue
		}
		field := intArgValue(nArg, i)
		if field <= 0 {
			return nil, fmt.Errorf("field position %d must be greater than zero", field)
		}
		str := strValue(strArg, i)
		sep := strValue(sepArg, i)
		parts := strings.Split(str, sep)
		if field > int64(len(parts)) {
			b.AppendNull()
			continue
		}
		b.Append(parts[field-1])
	}
	return b.NewArray(), nil
}

// evalArrowTrim implements the SQL TRIM family as a native vectorized loop.
// With a single string argument it trims leading/trailing whitespace (BOTH);
// an optional second string argument names the set of characters to strip
// instead of whitespace. ltrim/rtrim/btrim restrict stripping to the left,
// right, or both sides respectively (btrim is the PostgreSQL name for BOTH).
// All variants return NULL when the string operand is NULL. The no-cut-set
// whitespace variant uses unicode.IsSpace to match the classic path
// (strings.TrimSpace / TrimLeftFunc / TrimRightFunc), which strips all Unicode
// whitespace (e.g. U+00A0) rather than only ASCII whitespace.
func (p *arrowProjection) evalArrowTrim(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if len(spec.Args) < 1 || len(spec.Args) > 2 {
		return nil, fmt.Errorf("trim expects 1 or 2 arguments, got %d", len(spec.Args))
	}
	strArg, err := p.resolveStrArg(in, spec.Args[0])
	if err != nil {
		return nil, err
	}
	var cutSet string
	hasCutSet := len(spec.Args) == 2
	if hasCutSet {
		c, err := p.resolveStrArg(in, spec.Args[1])
		if err != nil {
			return nil, err
		}
		cutSet = c.constStr
		if c.isCol {
			// The cut-set column is constant across rows for a single spec; we
			// materialize using row 0 and stop if any later row differs (which
			// the planner does not currently produce, since all operands are
			// resolved to either a column or a single constant, never a mix).
			if c.col.Len() == 0 {
				cutSet = ""
			} else {
				cutSet = c.col.Value(0)
			}
		}
	}
	left, right := true, true
	switch spec.Func {
	case "ltrim":
		right = false
	case "rtrim":
		left = false
	}
	trimOne := func(s string) string {
		if !hasCutSet {
			// Unicode whitespace: align with the classic path.
			if left && right {
				return strings.TrimSpace(s)
			}
			if left {
				return strings.TrimLeftFunc(s, unicode.IsSpace)
			}
			return strings.TrimRightFunc(s, unicode.IsSpace)
		}
		if left && right {
			return strings.Trim(s, cutSet)
		}
		if left {
			return strings.TrimLeft(s, cutSet)
		}
		return strings.TrimRight(s, cutSet)
	}
	b := array.NewStringBuilder(p.alloc)
	defer b.Release()
	if strArg.isCol {
		for i := 0; i < int(in.NumRows()); i++ {
			if strArg.col.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(trimOne(strArg.col.Value(i)))
		}
	} else if strArg.constNull {
		b.AppendNull()
	} else {
		b.Append(trimOne(strArg.constStr))
	}
	return b.NewArray(), nil
}

// evalArrowReplace implements the SQL REPLACE(str, from, to) as a native
// vectorized loop: every non-overlapping occurrence of the `from` substring is
// replaced with `to`. This mirrors the classic path, which calls
// strings.Replace(str, from, to, -1); in particular an empty `from` inserts
// `to` between every character (Go's strings.Replace behavior). Returns NULL
// when the string operand is NULL; the `from` and `to` operands may be
// constants only (the planner only routes the literal variants through this
// path).
func (p *arrowProjection) evalArrowReplace(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if len(spec.Args) != 3 {
		return nil, fmt.Errorf("replace expects 3 arguments, got %d", len(spec.Args))
	}
	strArg, err := p.resolveStrArg(in, spec.Args[0])
	if err != nil {
		return nil, err
	}
	fromArg, err := p.resolveStrArg(in, spec.Args[1])
	if err != nil {
		return nil, err
	}
	toArg, err := p.resolveStrArg(in, spec.Args[2])
	if err != nil {
		return nil, err
	}
	fromStr := fromArg.constStr
	toStr := toArg.constStr
	b := array.NewStringBuilder(p.alloc)
	defer b.Release()
	replaceOne := func(s string) string {
		return strings.Replace(s, fromStr, toStr, -1)
	}
	if strArg.isCol {
		for i := 0; i < int(in.NumRows()); i++ {
			if strArg.col.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(replaceOne(strArg.col.Value(i)))
		}
	} else if strArg.constNull {
		b.AppendNull()
	} else {
		b.Append(replaceOne(strArg.constStr))
	}
	return b.NewArray(), nil
}
