// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowUUIDType maps a KWDB UUID column onto an Arrow FixedSizeBinary(16) so
// the grouping/hashing kernels can treat the 16 canonical octets as raw bytes.
var arrowUUIDType = &arrow.FixedSizeBinaryType{ByteWidth: 16}

// UnifiedProcessor is the unified execution interface described in
// docs/arrow-unification-architecture.md. Both rowexec and colexec operators
// can be adapted to expose columnar Arrow Records, so the planner can mix
// vectorized and row-based operators in a single DAG without format conversion
// at every boundary.
//
// This is the "shallow unification" entry point: operators still compute
// however they like internally (row-by-row for rowexec, batch-by-batch for
// colexec), but they agree on one external contract — an Arrow Record.
type UnifiedProcessor interface {
	// Init performs one-time setup.
	Init(ctx context.Context)
	// Next returns the next batch of results as an Arrow Record. done is true
	// when the stream is exhausted.
	Next(ctx context.Context) (rec arrow.Record, done bool, err error)
	// Allocator returns the shared Arrow memory allocator.
	Allocator() memory.Allocator
}

// rowToArrowConverter adapts rowexec's native EncDatumRows batch into a single
// Arrow Record. It is the "rowexec base" side of the unification: the operator
// still consumes rows internally, but emits a columnar Record so it can be fed
// directly into Arrow-based (or colexec-bridged) downstream operators.
type rowToArrowConverter struct {
	alloc  memory.Allocator
	typs   []*types.T
	rows   sqlbase.EncDatumRows
	da     *sqlbase.DatumAlloc
	idx    int
	schema *arrow.Schema
	// initErr captures a schema-build failure (e.g. unsupported type family)
	// so it can be surfaced from Next instead of panicking on a nil schema.
	initErr error
}

// NewRowToArrowConverter builds a converter for one batch of rows.
func NewRowToArrowConverter(alloc memory.Allocator, typs []*types.T, rows sqlbase.EncDatumRows) *rowToArrowConverter {
	fields := make([]arrow.Field, len(typs))
	for i, t := range typs {
		dt, err := arrowDataTypeForKWType(t)
		if err != nil {
			return &rowToArrowConverter{initErr: err}
		}
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: dt, Nullable: true}
	}
	return &rowToArrowConverter{
		alloc:  alloc,
		typs:   typs,
		rows:   rows,
		da:     &sqlbase.DatumAlloc{},
		schema: arrow.NewSchema(fields, nil),
	}
}

// Init implements UnifiedProcessor.
func (c *rowToArrowConverter) Init(ctx context.Context) {}

// Allocator implements UnifiedProcessor.
func (c *rowToArrowConverter) Allocator() memory.Allocator { return c.alloc }

// Next implements UnifiedProcessor. It emits exactly one Record then reports
// done, matching the single-batch input.
func (c *rowToArrowConverter) Next(ctx context.Context) (arrow.Record, bool, error) {
	if c.initErr != nil {
		return nil, false, c.initErr
	}
	if c.idx > 0 {
		return nil, true, nil
	}
	c.idx++
	cols, err := buildArrowColumns(c.alloc, c.typs, c.rows, c.da)
	if err != nil {
		return nil, false, err
	}
	return array.NewRecord(c.schema, cols, int64(len(c.rows))), false, nil
}

// arrowRecordSource yields a single pre-built Arrow Record as a UnifiedProcessor,
// so a Record produced by a rowexec operator can be fed into an Arrow-based
// downstream operator (e.g. arrowProjection) without re-encoding.
type arrowRecordSource struct {
	alloc memory.Allocator
	rec   arrow.Record
	sent  bool
}

// NewArrowRecordSource wraps an existing Record as a UnifiedProcessor.
func NewArrowRecordSource(alloc memory.Allocator, rec arrow.Record) *arrowRecordSource {
	return &arrowRecordSource{alloc: alloc, rec: rec}
}

// Init implements UnifiedProcessor.
func (s *arrowRecordSource) Init(ctx context.Context) {}

// Allocator implements UnifiedProcessor.
func (s *arrowRecordSource) Allocator() memory.Allocator { return s.alloc }

// Next implements UnifiedProcessor.
func (s *arrowRecordSource) Next(ctx context.Context) (arrow.Record, bool, error) {
	if s.sent {
		return nil, true, nil
	}
	s.sent = true
	return s.rec, false, nil
}

// arrowDataTypeForKWType maps a kwbase logical type onto the Arrow DataType used
// to build the unified Record. Unsupported families are reported as an explicit
// error rather than silently falling back to Int64 — a silent fallback would let
// the schema claim Int64 while appendEncDatum later fails with a confusing type
// assertion panic. The default branch is intentionally converged to error out so
// that every unsupported family fails fast at schema-build time.
func arrowDataTypeForKWType(t *types.T) (arrow.DataType, error) {
	switch t.Family() {
	case types.IntFamily:
		// Preserve the integer width. Collapsing every integer to Int64 loses
		// the planner-declared width (int2/int4), so a downstream consumer that
		// reads the column by its logical type (e.g. colexec selecting Int32 for
		// an int4 column, or generate_series's int4 result) would see a width
		// mismatch. Materializing at the true width keeps the Arrow schema and
		// the planner schema consistent end-to-end.
		switch t.Width() {
		case 16:
			return arrow.PrimitiveTypes.Int16, nil
		case 32:
			return arrow.PrimitiveTypes.Int32, nil
		default:
			// width 0/8/64 → Int64
			return arrow.PrimitiveTypes.Int64, nil
		}
	case types.FloatFamily:
		return arrow.PrimitiveTypes.Float64, nil
	case types.StringFamily:
		return arrow.BinaryTypes.String, nil
	case types.BoolFamily:
		return arrow.FixedWidthTypes.Boolean, nil
	case types.DecimalFamily:
		return &arrow.Decimal128Type{Precision: 38, Scale: t.Scale()}, nil
	case types.TimestampTZFamily, types.TimestampFamily:
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case types.UuidFamily:
		// 16-byte canonical UUID octets -> FixedSizeBinary(16) so the Arrow
		// grouping machinery can hash/compare them by raw bytes.
		return arrowUUIDType, nil
	case types.JsonFamily:
		// JSON is stored as its canonical text form in a String column; the
		// value bytes are compared/hashed directly (see arrowGroupHash/Equal).
		return arrow.BinaryTypes.String, nil
	case types.DateFamily:
		// Date has no native Arrow type; store as the number of days since the
		// Unix epoch (int32), matching KWDB's on-disk representation
		// (DDate.Date.UnixEpochDays), so grouping/comparison stays correct.
		return arrow.PrimitiveTypes.Int32, nil
	case types.IntervalFamily:
		// Interval has no native Arrow type. Store its canonical text form in a
		// String column (same treatment as JSON) so CAST(interval AS string)
		// reuses the value verbatim. Grouping/comparison is by text bytes.
		return arrow.BinaryTypes.String, nil
	case types.BytesFamily:
		return arrow.BinaryTypes.Binary, nil
	default:
		return nil, fmt.Errorf("unsupported type family %s for arrow schema", t.Family())
	}
}

// buildArrowColumns decodes EncDatumRows column-by-column into Arrow arrays.
// This is where rowexec's per-row EncDatum decoding is amortized into a single
// columnar build, replacing the row-at-a-time EncDatumRow handling with Arrow's
// contiguous, cache-friendly buffers.
func buildArrowColumns(alloc memory.Allocator, typs []*types.T, rows sqlbase.EncDatumRows, da *sqlbase.DatumAlloc) ([]arrow.Array, error) {
	// Guard against an arity mismatch between the planner-declared output schema
	// (typs) and the actual decoded rows. When these disagree (e.g. an upstream
	// Arrow operator advertised a different column count than it emitted), the
	// per-column type switches below would index/convert the wrong Datum and
	// panic. Bail out so the caller can fall back to the row output instead of
	// crashing the whole flow.
	if len(rows) > 0 && len(typs) != len(rows[0]) {
		return nil, fmt.Errorf("buildArrowColumns: arity mismatch (%d types, %d row cols)", len(typs), len(rows[0]))
	}
	n := len(rows)
	cols := make([]arrow.Array, len(typs))
	for ci, t := range typs {
		switch t.Family() {
		case types.IntFamily:
			// Deep-unification fast path (§7.6): decode each integer directly
			// from its encoded bytes via EncDatum.GetInt, avoiding the
			// per-row tree.Datum heap allocation (EnsureDecoded + tree.AsDInt)
			// that previously dominated the row->Arrow bridge cost. The builder
			// is pre-reserved so the append loop performs a single allocation.
			// The integer width must agree with arrowDataTypeForKWType (which
			// already chose the column's Arrow schema type by t.Width()), so the
			// builder's concrete type is selected to match instead of always
			// Int64; otherwise the produced array would type-mismatch the schema
			// (e.g. int4 -> got=int64, want=int32).
			var b array.Builder
			switch t.Width() {
			case 16:
				b = array.NewInt16Builder(alloc)
			case 32:
				b = array.NewInt32Builder(alloc)
			default:
				b = array.NewInt64Builder(alloc)
			}
			b.Reserve(n)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if ed.IsNull() {
					b.AppendNull()
					continue
				}
				v, err := ed.GetInt()
				if err != nil {
					b.Release()
					return nil, err
				}
				switch t.Width() {
				case 16:
					b.(*array.Int16Builder).Append(int16(v))
				case 32:
					b.(*array.Int32Builder).Append(int32(v))
				default:
					b.(*array.Int64Builder).Append(v)
				}
			}
			cols[ci] = b.NewArray()
		case types.FloatFamily:
			// §fetcher 直出 Arrow: decode each float directly from its encoded
			// VALUE bytes via EncDatum.GetFloat, avoiding the per-row tree.Datum
			// heap allocation (EnsureDecoded + tree.AsDFloat) that previously
			// dominated the row->Arrow bridge cost.
			b := array.NewFloat64Builder(alloc)
			b.Reserve(n)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if ed.IsNull() {
					b.AppendNull()
					continue
				}
				v, err := ed.GetFloat(t, da)
				if err != nil {
					b.Release()
					return nil, err
				}
				b.Append(v)
			}
			cols[ci] = b.NewArray()
		case types.StringFamily:
			b := array.NewStringBuilder(alloc)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if err := ed.EnsureDecoded(t, da); err != nil {
					b.Release()
					return nil, err
				}
				if ed.Datum == tree.DNull {
					b.AppendNull()
					continue
				}
				d, ok := tree.AsDString(ed.Datum)
				if !ok {
					b.Release()
					return nil, fmt.Errorf("col %d: expected string, got %T", ci, ed.Datum)
				}
			b.Append(string(d))
		}
		cols[ci] = b.NewArray()
	case types.BytesFamily:
		// §fetcher 直出 Arrow: decode each bytes value directly from its
		// encoded VALUE bytes via EncDatum.GetBytes.
		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		b.Reserve(n)
		for ri := 0; ri < n; ri++ {
			ed := &rows[ri][ci]
			if ed.IsNull() {
				b.AppendNull()
				continue
			}
			v, err := ed.GetBytes(t, da)
			if err != nil {
				b.Release()
				return nil, err
			}
			b.Append(v)
		}
		cols[ci] = b.NewArray()
	case types.BoolFamily:
			b := array.NewBooleanBuilder(alloc)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if err := ed.EnsureDecoded(t, da); err != nil {
					b.Release()
					return nil, err
				}
				if ed.Datum == tree.DNull {
					b.AppendNull()
					continue
				}
				d, ok := tree.AsDBool(ed.Datum)
				if !ok {
					b.Release()
					return nil, fmt.Errorf("col %d: expected bool, got %T", ci, ed.Datum)
				}
				b.Append(bool(d))
			}
			cols[ci] = b.NewArray()
		case types.DecimalFamily:
			scale := t.Scale()
			// §fetcher 直出 Arrow: decode each decimal directly from its encoded
			// VALUE bytes via EncDatum.GetDecimal (returns the underlying
			// apd.Decimal), skipping EnsureDecoded + tree.DDecimal allocation.
			b := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: scale})
			b.Reserve(n)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if ed.IsNull() {
					b.AppendNull()
					continue
				}
				d, err := ed.GetDecimal(t, da)
				if err != nil {
					b.Release()
					return nil, err
				}
				num, err := apdToDecimal128(&d, scale)
				if err != nil {
					b.Release()
					return nil, err
				}
				b.Append(num)
			}
			cols[ci] = b.NewArray()
		case types.TimestampTZFamily, types.TimestampFamily:
			b := array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			// §fetcher 直出 Arrow: decode each timestamp directly from its
			// encoded VALUE bytes via EncDatum.GetTime, skipping EnsureDecoded +
			// tree.DTimestamp(TZ) allocation.
			b.Reserve(n)
			for ri := 0; ri < n; ri++ {
				ed := &rows[ri][ci]
				if ed.IsNull() {
					b.AppendNull()
					continue
				}
				tm, err := ed.GetTime(t, da)
				if err != nil {
					b.Release()
					return nil, err
				}
				b.Append(arrow.Timestamp(tm.UnixMicro()))
			}
			cols[ci] = b.NewArray()
	case types.DateFamily:
		// §fetcher 直出 Arrow: decode each date directly from its encoded VALUE
		// bytes via EncDatum.GetDate, skipping EnsureDecoded + tree.DDate
		// allocation. Infinite dates have no finite int32 representation; treat
		// as NULL.
		b := array.NewInt32Builder(alloc)
		b.Reserve(n)
		for ri := 0; ri < n; ri++ {
			ed := &rows[ri][ci]
			if ed.IsNull() {
				b.AppendNull()
				continue
			}
			d, err := ed.GetDate(t, da)
			if err != nil {
				b.Release()
				return nil, err
			}
			// Infinite dates (sentinel encodings) decode to out-of-range epoch
			// days; int32 Arrow columns cannot represent them, so emit NULL.
			if d > 1<<29 || d < -(1<<29) {
				b.AppendNull()
				continue
			}
			b.Append(d)
		}
		cols[ci] = b.NewArray()
	case types.UuidFamily:
		// §fetcher 直出 Arrow: decode each uuid directly from its encoded VALUE
		// bytes via EncDatum.GetUUID, skipping EnsureDecoded + tree.DUuid
		// allocation.
		b := array.NewFixedSizeBinaryBuilder(alloc, arrowUUIDType)
		b.Reserve(n)
		for ri := 0; ri < n; ri++ {
			ed := &rows[ri][ci]
			if ed.IsNull() {
				b.AppendNull()
				continue
			}
			u, err := ed.GetUUID(t, da)
			if err != nil {
				b.Release()
				return nil, err
			}
			b.Append(u.GetBytes())
		}
		cols[ci] = b.NewArray()
	case types.JsonFamily:
		b := array.NewStringBuilder(alloc)
		for ri := 0; ri < n; ri++ {
			ed := &rows[ri][ci]
			if err := ed.EnsureDecoded(t, da); err != nil {
				b.Release()
				return nil, err
			}
			if ed.Datum == tree.DNull {
				b.AppendNull()
				continue
			}
			dj, ok := ed.Datum.(*tree.DJSON)
			if !ok {
				b.Release()
				return nil, fmt.Errorf("col %d: expected json, got %T", ci, ed.Datum)
			}
			b.Append(dj.JSON.String())
		}
		cols[ci] = b.NewArray()
	case types.IntervalFamily:
		b := array.NewStringBuilder(alloc)
		for ri := 0; ri < n; ri++ {
			ed := &rows[ri][ci]
			if err := ed.EnsureDecoded(t, da); err != nil {
				b.Release()
				return nil, err
			}
			if ed.Datum == tree.DNull {
				b.AppendNull()
				continue
			}
			di, ok := ed.Datum.(*tree.DInterval)
			if !ok {
				b.Release()
				return nil, fmt.Errorf("col %d: expected interval, got %T", ci, ed.Datum)
			}
			// Store the canonical text form; CAST(interval AS string) reuses it.
			b.Append(di.String())
		}
		cols[ci] = b.NewArray()
	default:
		return nil, fmt.Errorf("unsupported type family %s for arrow unification", t.Family())
		}
	}
	return cols, nil
}

// arrowScanSupported reports whether every column type can be decoded into an
// Arrow representation, i.e. whether a table reader over typs may feed the Arrow
// engine. Used by the planner as a type gate alongside ArrowScanEnabled.
func arrowScanSupported(typs []*types.T) bool {
	for _, t := range typs {
		if _, err := arrowDataTypeForKWType(t); err != nil {
			return false
		}
	}
	return true
}

// newArrowBuilder returns a fresh Arrow builder for the given kwbase type.
// Unsupported families are reported as an explicit error rather than silently
// falling back to an Int64Builder (which would let appendEncDatum fail later
// with a confusing type assertion). The default branch is converged to error
// out so the behavior matches arrowDataTypeForKWType.
func newArrowBuilder(alloc memory.Allocator, t *types.T) (array.Builder, error) {
	switch t.Family() {
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return array.NewInt16Builder(alloc), nil
		case 32:
			return array.NewInt32Builder(alloc), nil
		default:
			return array.NewInt64Builder(alloc), nil
		}
	case types.FloatFamily:
		return array.NewFloat64Builder(alloc), nil
	case types.StringFamily:
		return array.NewStringBuilder(alloc), nil
	case types.BoolFamily:
		return array.NewBooleanBuilder(alloc), nil
	case types.DecimalFamily:
		return array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: t.Scale()}), nil
	case types.TimestampTZFamily, types.TimestampFamily:
		return array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType)), nil
	case types.UuidFamily:
		return array.NewFixedSizeBinaryBuilder(alloc, arrowUUIDType), nil
	case types.JsonFamily:
		return array.NewStringBuilder(alloc), nil
	case types.DateFamily:
		return array.NewInt32Builder(alloc), nil
	case types.IntervalFamily:
		return array.NewStringBuilder(alloc), nil
	case types.BytesFamily:
		return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary), nil
	default:
		return nil, fmt.Errorf("unsupported type family %s for arrow builder", t.Family())
	}
}

// appendEncDatum appends one decoded EncDatum value into its column builder.
func appendEncDatum(b array.Builder, t *types.T, ed *sqlbase.EncDatum, da *sqlbase.DatumAlloc) error {
	if ed.IsNull() {
		b.AppendNull()
		return nil
	}
	switch t.Family() {
	case types.IntFamily:
		v, err := ed.GetInt()
		if err != nil {
			return err
		}
		// Match the width chosen by arrowDataTypeForKWType so the builder's
		// concrete type agrees with the Arrow schema declaring this column.
		switch t.Width() {
		case 16:
			b.(*array.Int16Builder).Append(int16(v))
		case 32:
			b.(*array.Int32Builder).Append(int32(v))
		default:
			b.(*array.Int64Builder).Append(v)
		}
	case types.FloatFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		d, ok := ed.Datum.(*tree.DFloat)
		if !ok {
			return fmt.Errorf("expected float, got %T", ed.Datum)
		}
		b.(*array.Float64Builder).Append(float64(*d))
	case types.StringFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		d, ok := tree.AsDString(ed.Datum)
		if !ok {
			return fmt.Errorf("expected string, got %T", ed.Datum)
		}
		b.(*array.StringBuilder).Append(string(d))
	case types.BytesFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		d, ok := tree.AsDBytes(ed.Datum)
		if !ok {
			return fmt.Errorf("expected bytes, got %T", ed.Datum)
		}
		b.(*array.BinaryBuilder).Append([]byte(d))
	case types.BoolFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		d, ok := tree.AsDBool(ed.Datum)
		if !ok {
			return fmt.Errorf("expected bool, got %T", ed.Datum)
		}
		b.(*array.BooleanBuilder).Append(bool(d))
	case types.DecimalFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		dd, ok := ed.Datum.(*tree.DDecimal)
		if !ok {
			return fmt.Errorf("expected decimal, got %T", ed.Datum)
		}
		num, err := apdToDecimal128(&dd.Decimal, t.Scale())
		if err != nil {
			return err
		}
		b.(*array.Decimal128Builder).Append(num)
	case types.TimestampTZFamily, types.TimestampFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		var micros int64
		switch d := ed.Datum.(type) {
		case *tree.DTimestampTZ:
			micros = d.UnixMicro()
		case *tree.DTimestamp:
			micros = d.UnixMicro()
		default:
			return fmt.Errorf("expected timestamp, got %T", ed.Datum)
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(micros))
	case types.DateFamily:
		// Encoded as int64 days since the Unix epoch (see column_type_encoding).
		// Infinite dates land on MaxInt32/MinInt32 and have no finite int32
		// representation, so map them to NULL.
		v, err := ed.GetInt()
		if err != nil {
			return err
		}
		if v == math.MaxInt32 || v == math.MinInt32 {
			b.(*array.Int32Builder).AppendNull()
			return nil
		}
		b.(*array.Int32Builder).Append(int32(v))
	case types.UuidFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		du, ok := ed.Datum.(*tree.DUuid)
		if !ok {
			return fmt.Errorf("expected uuid, got %T", ed.Datum)
		}
		b.(*array.FixedSizeBinaryBuilder).Append(du.UUID.GetBytes())
	case types.JsonFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		dj, ok := ed.Datum.(*tree.DJSON)
		if !ok {
			return fmt.Errorf("expected json, got %T", ed.Datum)
		}
		b.(*array.StringBuilder).Append(dj.JSON.String())
	case types.IntervalFamily:
		if err := ed.EnsureDecoded(t, da); err != nil {
			return err
		}
		if ed.Datum == tree.DNull {
			b.AppendNull()
			return nil
		}
		di, ok := ed.Datum.(*tree.DInterval)
		if !ok {
			return fmt.Errorf("expected interval, got %T", ed.Datum)
		}
		// Store the canonical text form; CAST(interval AS string) reuses it.
		b.(*array.StringBuilder).Append(di.String())
	default:
		return fmt.Errorf("unsupported type family %s for arrow scan", t.Family())
	}
	return nil
}
