// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

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
}

// NewRowToArrowConverter builds a converter for one batch of rows.
func NewRowToArrowConverter(alloc memory.Allocator, typs []*types.T, rows sqlbase.EncDatumRows) *rowToArrowConverter {
	fields := make([]arrow.Field, len(typs))
	for i, t := range typs {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrowTypeForKWType(t), Nullable: true}
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

// arrowTypeForKWType maps a kwbase logical type onto the Arrow DataType used to
// build the unified Record. Unsupported families fall back to Int64 so that
// buildArrowColumns can return a precise error instead of a confusing crash.
func arrowTypeForKWType(t *types.T) arrow.DataType {
	switch t.Family() {
	case types.IntFamily:
		return arrow.PrimitiveTypes.Int64
	case types.FloatFamily:
		return arrow.PrimitiveTypes.Float64
	case types.StringFamily:
		return arrow.BinaryTypes.String
	case types.BoolFamily:
		return arrow.FixedWidthTypes.Boolean
	case types.DecimalFamily:
		return &arrow.Decimal128Type{Precision: 38, Scale: t.Scale()}
	case types.TimestampTZFamily, types.TimestampFamily:
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.PrimitiveTypes.Int64
	}
}

// buildArrowColumns decodes EncDatumRows column-by-column into Arrow arrays.
// This is where rowexec's per-row EncDatum decoding is amortized into a single
// columnar build, replacing the row-at-a-time EncDatumRow handling with Arrow's
// contiguous, cache-friendly buffers.
func buildArrowColumns(alloc memory.Allocator, typs []*types.T, rows sqlbase.EncDatumRows, da *sqlbase.DatumAlloc) ([]arrow.Array, error) {
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
			b := array.NewInt64Builder(alloc)
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
				b.Append(v)
			}
			cols[ci] = b.NewArray()
		case types.FloatFamily:
			b := array.NewFloat64Builder(alloc)
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
				d, ok := ed.Datum.(*tree.DFloat)
				if !ok {
					b.Release()
					return nil, fmt.Errorf("col %d: expected float, got %T", ci, ed.Datum)
				}
				b.Append(float64(*d))
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
			b := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: scale})
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
				dd, ok := ed.Datum.(*tree.DDecimal)
				if !ok {
					b.Release()
					return nil, fmt.Errorf("col %d: expected decimal, got %T", ci, ed.Datum)
				}
				num, err := apdToDecimal128(&dd.Decimal, scale)
				if err != nil {
					b.Release()
					return nil, err
				}
				b.Append(num)
			}
			cols[ci] = b.NewArray()
		case types.TimestampTZFamily, types.TimestampFamily:
			b := array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
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
				var micros int64
				switch d := ed.Datum.(type) {
				case *tree.DTimestampTZ:
					micros = d.UnixMicro()
				case *tree.DTimestamp:
					micros = d.UnixMicro()
				default:
					b.Release()
					return nil, fmt.Errorf("col %d: expected timestamp, got %T", ci, ed.Datum)
				}
				b.Append(arrow.Timestamp(micros))
			}
			cols[ci] = b.NewArray()
		default:
			return nil, fmt.Errorf("unsupported type family %s for arrow unification", t.Family())
		}
	}
	return cols, nil
}

// arrowScan is the "native Arrow scan" (§7.9). It reads rows from an underlying
// RowSource and builds the unified Arrow Record incrementally: each value is
// appended straight into its per-column Arrow builder as the row arrives, so the
// input is never materialized into an intermediate buffer (no [][]int64 / [][]bool
// 2D slices, no full EncDatumRows drain). This removes the first-node bridge that
// newArrowInputSource previously imposed on arrow flows — the scan now produces a
// native Arrow Record directly.
type arrowScan struct {
	alloc memory.Allocator
	input execinfra.RowSource
	da    *sqlbase.DatumAlloc
	typs  []*types.T
	rec   arrow.Record
	sent  bool
}

// newArrowScan wraps a RowSource as a UnifiedProcessor that emits a single native
// Arrow Record built directly from the streamed rows.
func newArrowScan(alloc memory.Allocator, input execinfra.RowSource, da *sqlbase.DatumAlloc) *arrowScan {
	inTypes := input.OutputTypes()
	typs := make([]*types.T, len(inTypes))
	for i := range inTypes {
		t := inTypes[i]
		typs[i] = &t
	}
	return &arrowScan{alloc: alloc, input: input, da: da, typs: typs}
}

// Init implements UnifiedProcessor.
func (s *arrowScan) Init(ctx context.Context) {}

// Allocator implements UnifiedProcessor.
func (s *arrowScan) Allocator() memory.Allocator { return s.alloc }

// Next implements UnifiedProcessor. It drains the input once, appending each row
// directly into per-column Arrow builders, then emits a single Record.
func (s *arrowScan) Next(ctx context.Context) (arrow.Record, bool, error) {
	if s.sent {
		return nil, true, nil
	}
	s.sent = true
	rec, err := s.build()
	if err != nil {
		return nil, false, err
	}
	s.rec = rec
	return rec, false, nil
}

// build reads every input row exactly once and appends each value into the
// matching column builder, producing a single native Arrow Record.
func (s *arrowScan) build() (arrow.Record, error) {
	nIn := len(s.typs)
	builders := make([]array.Builder, nIn)
	fields := make([]arrow.Field, nIn)
	for i, t := range s.typs {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrowTypeForKWType(t), Nullable: true}
		builders[i] = newArrowBuilder(s.alloc, t)
	}
	release := func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}
	n := 0
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if meta.Err != nil {
				release()
				return nil, meta.Err
			}
			continue
		}
		if row == nil {
			break
		}
		for i := range row {
			if err := appendEncDatum(builders[i], s.typs[i], &row[i], s.da); err != nil {
				release()
				return nil, err
			}
		}
		n++
	}
	cols := make([]arrow.Array, nIn)
	for i := range builders {
		cols[i] = builders[i].NewArray()
		builders[i].Release()
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(n)), nil
}

// newArrowBuilder returns a fresh Arrow builder for the given kwbase type.
func newArrowBuilder(alloc memory.Allocator, t *types.T) array.Builder {
	switch t.Family() {
	case types.IntFamily:
		return array.NewInt64Builder(alloc)
	case types.FloatFamily:
		return array.NewFloat64Builder(alloc)
	case types.StringFamily:
		return array.NewStringBuilder(alloc)
	case types.BoolFamily:
		return array.NewBooleanBuilder(alloc)
	case types.DecimalFamily:
		return array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: t.Scale()})
	case types.TimestampTZFamily, types.TimestampFamily:
		return array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
	default:
		return array.NewInt64Builder(alloc)
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
		b.(*array.Int64Builder).Append(v)
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
	default:
		return fmt.Errorf("unsupported type family %s for arrow scan", t.Family())
	}
	return nil
}
