// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
	"github.com/cockroachdb/apd"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// ============================================================================
// Arrow aggregate kernels (pure-Arrow compute).
//
// The vendored Apache Arrow Go v17 does not ship the arrow/compute/aggregate
// package that the C++ engine provides (see
// arrow/cpp/src/arrow/compute/kernels/{aggregate_internal.h, aggregate_basic.cc,
// hash_aggregate.cc}). We implement the missing kernels here, modelled directly
// on the C++ design so that aggregation in the Arrow path runs through a proper
// Arrow compute kernel instead of ad-hoc Go loops:
//
//   - ScalarAggregator mirrors C++ ScalarAggregator: Consume one array (a batch
//     or a group segment), MergeFrom another state, Finalize to a scalar. It is
//     the building block for both the whole-batch (scalar) and the grouped
//     (hash) variants, exactly like C++ ScalarAggregateFunction vs
//     HashAggregateFunction.
//   - Accumulator types follow C++ FindAccumulatorType: integer columns widen to
//     int64, floating columns widen to float64, and boolean SUM counts true
//     values (int64).
//   - Null handling mirrors C++ ScalarAggregateOptions (skip_nulls = true,
//     min_count = 1): nulls are skipped while accumulating, but SUM/MIN/MAX/MEAN
//     finalize to a null scalar when no non-null value was observed; COUNT and
//     COUNT(*) ignore nulls entirely (COUNT(col) counts non-nulls, COUNT(*)
//     counts rows).
//   - arrowHashAggregator keeps one ScalarAggregator per (group, aggregate) and feeds
//     each group's value slice through the kernel, mirroring C++'s
//     HashAggregateFunction hashing rows into group segments and consuming them.
// ============================================================================

// aggOp identifies an aggregate function, mirroring the scalar/hash aggregate
// kernels in the C++ engine (sum/count/min/max/mean, plus count_all which is
// COUNT(*) and is handled as a group counter rather than a value kernel).
type aggOp int

const (
	aggOpSum aggOp = iota
	aggOpCount
	aggOpCountAll
	aggOpMin
	aggOpMax
	aggOpMean
)

func parseAggOp(s string) (aggOp, error) {
	switch s {
	case "sum":
		return aggOpSum, nil
	case "count":
		return aggOpCount, nil
	case "count_all":
		return aggOpCountAll, nil
	case "min":
		return aggOpMin, nil
	case "max":
		return aggOpMax, nil
	case "mean":
		return aggOpMean, nil
	}
	return 0, fmt.Errorf("unsupported arrow aggregate op %q", s)
}

// aggAccumKind distinguishes the accumulator representation. It mirrors C++'s
// FindAccumulatorType widening: integer -> int64, float -> float64.
type aggAccumKind int

const (
	aggAccInt aggAccumKind = iota
	aggAccFloat
)

// scalarAggregator mirrors C++ ScalarAggregator: Consume accumulates the values
// of one array, MergeFrom folds another state in (for parallel / hash merges),
// and Finalize emits the aggregate as a scalar. This is the pure-Arrow kernel:
// it operates directly on arrow.Array data, never on materialized Go rows.
// meanDecimalType is the Arrow decimal128 type emitted by the mean (AVG) kernel
// for integer inputs. SQL AVG over integers returns DECIMAL, so the Arrow kernel
// must produce a decimal value rather than a float. Precision is the maximum for
// decimal128 (38); scale 9 keeps full fidelity for typical integer sums while
// leaving headroom for large totals (values with more than ~28 integer digits
// would overflow decimal128 and are out of scope for a single batch).
var meanDecimalType = &arrow.Decimal128Type{Precision: 38, Scale: 9}

// apdToDecimal128 converts an arbitrary-precision apd.Decimal (the internal
// representation of SQL DECIMAL) into a fixed-scale decimal128.Num so it can be
// stored in an Arrow decimal128 array. If the requested scale forces truncation,
// the value is rounded half-away-from-zero, mirroring SQL decimal semantics.
func apdToDecimal128(d *apd.Decimal, scale int32) (decimal128.Num, error) {
	// value = Coeff * 10^Exponent ; we want Num * 10^-scale == value.
	shift := int64(d.Exponent) + int64(scale)
	coeff := new(big.Int).Set(&d.Coeff)
	if d.Negative {
		coeff.Neg(coeff)
	}
	if shift >= 0 {
		coeff.Mul(coeff, new(big.Int).Exp(big.NewInt(10), big.NewInt(shift), nil))
	} else {
		denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(-shift), nil)
		q := new(big.Int)
		r := new(big.Int)
		q.QuoRem(coeff, denom, r)
		// Round half away from zero.
		if new(big.Int).Mul(r, big.NewInt(2)).Cmp(denom) >= 0 {
			if q.Sign() >= 0 {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
		coeff = q
	}
	if coeff.BitLen() > 127 {
		return decimal128.Num{}, fmt.Errorf("decimal128 overflow computing AVG: value magnitude exceeds 38 digits")
	}
	return decimal128.FromBigInt(coeff), nil
}

// decimal128ToApd converts a fixed-scale decimal128.Num back into the
// arbitrary-precision apd.Decimal used by the kernels for accumulation. It is the
// inverse of apdToDecimal128 and mirrors the decode logic in
// arrowRecordToEncDatumRows.
func decimal128ToApd(num decimal128.Num, scale int32) apd.Decimal {
	bi := num.BigInt()
	var d apd.Decimal
	d.Negative = bi.Sign() < 0
	d.Coeff = *bi
	if d.Negative {
		d.Coeff.Abs(&d.Coeff)
	}
	d.Exponent = -scale
	return d
}

type scalarAggregator interface {
	// Consume accumulates the (non-null) values of arr, i.e. one batch or one
	// group's value segment. When sel is non-nil it is a selection vector of
	// indices into arr (the rows that belong to this group); when sel is nil the
	// whole array is consumed. Passing a selection vector lets the hash
	// aggregator feed each group's rows straight from the contiguous column
	// buffer (exactly like colexec's selection-vector feeding) instead of doing a
	// per-group copy.
	Consume(arr arrow.Array, sel []int32) error
	// MergeFrom folds the state of other (same concrete type) into this one.
	MergeFrom(other scalarAggregator) error
	// Finalize emits the aggregate value, or a null scalar when no non-null
	// value was observed (C++ min_count = 1 semantics).
	Finalize() (scalar.Scalar, error)
}

// newScalarAggregator builds a fresh ScalarAggregator for op over an input of
// type inType. This mirrors how C++ resolves a kernel implementation from the
// value type (FindAccumulatorType / kernel dispatch).
func newScalarAggregator(op aggOp, inType arrow.DataType) (scalarAggregator, error) {
	switch op {
	case aggOpSum:
		out := inType
		if inType.ID() == arrow.BOOL {
			// SUM over booleans counts true values, returning int64 (C++ result).
			out = arrow.PrimitiveTypes.Int64
		}
		return &sumAgg{outType: out, inType: inType}, nil
	case aggOpCount:
		return &countAgg{}, nil
	case aggOpMin, aggOpMax:
		kind := aggAccInt
		if inType.ID() == arrow.FLOAT64 {
			kind = aggAccFloat
		}
		return &minMaxAgg{op: op, kind: kind, inType: inType}, nil
	case aggOpMean:
		return &meanAgg{inType: inType}, nil
	case aggOpCountAll:
		return nil, fmt.Errorf("count_all has no per-array aggregator; use a group counter")
	}
	return nil, fmt.Errorf("unsupported arrow aggregate op %d", op)
}

// ---------------------------------------------------------------------------
// sumAgg mirrors C++ SumImpl: integer values widen to int64, float values to
// float64 (FindAccumulatorType), non-null values are counted, and the result is
// null when nothing was observed.
// ---------------------------------------------------------------------------
type sumAgg struct {
	outType arrow.DataType
	inType  arrow.DataType
	accI    int64
	accF    float64
	accD    apd.Decimal
	count   int64
}

func (s *sumAgg) Consume(arr arrow.Array, sel []int32) error {
	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		hasNulls := a.NullN() > 0
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					s.accI += vals[i]
				}
				s.count += int64(len(vals))
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					s.accI += vals[i]
					s.count++
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					s.accI += vals[i]
				}
				s.count += int64(len(sel))
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					s.accI += vals[ii]
					s.count++
				}
			}
		}
	case *array.Float64:
		vals := a.Float64Values()
		hasNulls := a.NullN() > 0
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					s.accF += vals[i]
				}
				s.count += int64(len(vals))
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					s.accF += vals[i]
					s.count++
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					s.accF += vals[i]
				}
				s.count += int64(len(sel))
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					s.accF += vals[ii]
					s.count++
				}
			}
		}
	case *array.Boolean:
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				if a.Value(i) {
					s.accI++
				}
				s.count++
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				if a.Value(ii) {
					s.accI++
				}
				s.count++
			}
		}
	case *array.Decimal128:
		sc := a.DataType().(*arrow.Decimal128Type).Scale
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				v := decimal128ToApd(a.Value(i), sc)
				if _, err := tree.ExactCtx.Add(&s.accD, &s.accD, &v); err != nil {
					return err
				}
				s.count++
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				v := decimal128ToApd(a.Value(ii), sc)
				if _, err := tree.ExactCtx.Add(&s.accD, &s.accD, &v); err != nil {
					return err
				}
				s.count++
			}
		}
	default:
		return fmt.Errorf("arrow sum unsupported on input type %s", arr.DataType())
	}
	return nil
}

func (s *sumAgg) MergeFrom(other scalarAggregator) error {
	o := other.(*sumAgg)
	s.accI += o.accI
	s.accF += o.accF
	if _, err := tree.ExactCtx.Add(&s.accD, &s.accD, &o.accD); err != nil {
		return err
	}
	s.count += o.count
	return nil
}

func (s *sumAgg) Finalize() (scalar.Scalar, error) {
	if s.count == 0 {
		return scalar.MakeNullScalar(s.outType), nil
	}
	switch s.inType.ID() {
	case arrow.FLOAT64:
		return scalar.NewFloat64Scalar(s.accF), nil
	case arrow.DECIMAL128:
		dt := s.outType.(*arrow.Decimal128Type)
		num, err := apdToDecimal128(&s.accD, dt.Scale)
		if err != nil {
			return nil, err
		}
		return scalar.NewDecimal128Scalar(num, s.outType), nil
	default:
		return scalar.NewInt64Scalar(s.accI), nil
	}
}

// ---------------------------------------------------------------------------
// countAgg mirrors C++ CountImpl: COUNT(col) counts only non-null values.
// ---------------------------------------------------------------------------
type countAgg struct {
	n int64
}

func (c *countAgg) Consume(arr arrow.Array, sel []int32) error {
	if arr == nil {
		return nil
	}
	if sel == nil {
		c.n += int64(arr.Len() - arr.NullN())
		return nil
	}
	for _, i := range sel {
		if !arr.IsNull(int(i)) {
			c.n++
		}
	}
	return nil
}

func (c *countAgg) MergeFrom(other scalarAggregator) error {
	c.n += other.(*countAgg).n
	return nil
}

func (c *countAgg) Finalize() (scalar.Scalar, error) {
	return scalar.NewInt64Scalar(c.n), nil
}

// ---------------------------------------------------------------------------
// minMaxAgg mirrors C++ MinMaxImpl: tracks the running min (or max) of the
// non-null values; the result is null when nothing was observed. For booleans
// min = all-true (AND) and max = any-true (OR), matching the C++ boolean kernel.
// ---------------------------------------------------------------------------
type minMaxAgg struct {
	op      aggOp
	kind    aggAccumKind
	inType  arrow.DataType
	set     bool
	accI    int64
	accF    float64
	accDMin apd.Decimal
	accDMax apd.Decimal
	boolMin bool
	boolMax bool
}

func (m *minMaxAgg) Consume(arr arrow.Array, sel []int32) error {
	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		hasNulls := a.NullN() > 0
		visit := func(v int64) {
			if !m.set {
				m.set = true
				m.accI = v
			} else if m.op == aggOpMin {
				if v < m.accI {
					m.accI = v
				}
			} else {
				if v > m.accI {
					m.accI = v
				}
			}
		}
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					visit(vals[i])
				}
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					visit(vals[i])
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					visit(vals[i])
				}
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					visit(vals[ii])
				}
			}
		}
	case *array.Float64:
		vals := a.Float64Values()
		hasNulls := a.NullN() > 0
		visit := func(v float64) {
			if !m.set {
				m.set = true
				m.accF = v
			} else if m.op == aggOpMin {
				if v < m.accF {
					m.accF = v
				}
			} else {
				if v > m.accF {
					m.accF = v
				}
			}
		}
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					visit(vals[i])
				}
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					visit(vals[i])
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					visit(vals[i])
				}
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					visit(vals[ii])
				}
			}
		}
	case *array.Boolean:
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				bv := a.Value(i)
				if !m.set {
					m.set = true
					m.boolMin = bv
					m.boolMax = bv
				} else {
					m.boolMin = m.boolMin && bv
					m.boolMax = m.boolMax || bv
				}
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				bv := a.Value(ii)
				if !m.set {
					m.set = true
					m.boolMin = bv
					m.boolMax = bv
				} else {
					m.boolMin = m.boolMin && bv
					m.boolMax = m.boolMax || bv
				}
			}
		}
	case *array.Decimal128:
		sc := a.DataType().(*arrow.Decimal128Type).Scale
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				v := decimal128ToApd(a.Value(i), sc)
				if !m.set {
					m.set = true
					m.accDMin = v
					m.accDMax = v
				} else if m.op == aggOpMin {
					if v.Cmp(&m.accDMin) < 0 {
						m.accDMin = v
					}
				} else {
					if v.Cmp(&m.accDMax) > 0 {
						m.accDMax = v
					}
				}
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				v := decimal128ToApd(a.Value(ii), sc)
				if !m.set {
					m.set = true
					m.accDMin = v
					m.accDMax = v
				} else if m.op == aggOpMin {
					if v.Cmp(&m.accDMin) < 0 {
						m.accDMin = v
					}
				} else {
					if v.Cmp(&m.accDMax) > 0 {
						m.accDMax = v
					}
				}
			}
		}
	case *array.Timestamp:
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				v := int64(a.Value(i))
				if !m.set {
					m.set = true
					m.accI = v
				} else if m.op == aggOpMin {
					if v < m.accI {
						m.accI = v
					}
				} else {
					if v > m.accI {
						m.accI = v
					}
				}
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				v := int64(a.Value(ii))
				if !m.set {
					m.set = true
					m.accI = v
				} else if m.op == aggOpMin {
					if v < m.accI {
						m.accI = v
					}
				} else {
					if v > m.accI {
						m.accI = v
					}
				}
			}
		}
	default:
		return fmt.Errorf("arrow %v unsupported on input type %s", m.op, arr.DataType())
	}
	return nil
}

func (m *minMaxAgg) MergeFrom(other scalarAggregator) error {
	o := other.(*minMaxAgg)
	if m.inType.ID() == arrow.DECIMAL128 {
		if !o.set {
			return nil
		}
		if !m.set {
			m.set = true
			m.accDMin = o.accDMin
			m.accDMax = o.accDMax
			return nil
		}
		if m.op == aggOpMin {
			if o.accDMin.Cmp(&m.accDMin) < 0 {
				m.accDMin = o.accDMin
			}
		} else {
			if o.accDMax.Cmp(&m.accDMax) > 0 {
				m.accDMax = o.accDMax
			}
		}
		return nil
	}
	if !o.set {
		return nil
	}
	if !m.set {
		m.set = true
		m.accI = o.accI
		m.accF = o.accF
		m.boolMin = o.boolMin
		m.boolMax = o.boolMax
		return nil
	}
	switch m.kind {
	case aggAccInt:
		if m.op == aggOpMin {
			if o.accI < m.accI {
				m.accI = o.accI
			}
		} else {
			if o.accI > m.accI {
				m.accI = o.accI
			}
		}
	case aggAccFloat:
		if m.op == aggOpMin {
			if o.accF < m.accF {
				m.accF = o.accF
			}
		} else {
			if o.accF > m.accF {
				m.accF = o.accF
			}
		}
	}
	m.boolMin = m.boolMin && o.boolMin
	m.boolMax = m.boolMax || o.boolMax
	return nil
}

func (m *minMaxAgg) Finalize() (scalar.Scalar, error) {
	if !m.set {
		return scalar.MakeNullScalar(m.inType), nil
	}
	switch m.inType.ID() {
	case arrow.FLOAT64:
		return scalar.NewFloat64Scalar(m.accF), nil
	case arrow.BOOL:
		if m.op == aggOpMin {
			return scalar.NewBooleanScalar(m.boolMin), nil
		}
		return scalar.NewBooleanScalar(m.boolMax), nil
	case arrow.DECIMAL128:
		dt := m.inType.(*arrow.Decimal128Type)
		if m.op == aggOpMin {
			num, err := apdToDecimal128(&m.accDMin, dt.Scale)
			if err != nil {
				return nil, err
			}
			return scalar.NewDecimal128Scalar(num, m.inType), nil
		}
		num, err := apdToDecimal128(&m.accDMax, dt.Scale)
		if err != nil {
			return nil, err
		}
		return scalar.NewDecimal128Scalar(num, m.inType), nil
	case arrow.TIMESTAMP:
		return scalar.NewTimestampScalar(arrow.Timestamp(m.accI), m.inType), nil
	default:
		return scalar.NewInt64Scalar(m.accI), nil
	}
}

// ---------------------------------------------------------------------------
// meanAgg mirrors C++ MeanImpl. The sum is accumulated over non-null values and
// divided by the count at finalize time. For integer inputs the result is a
// DECIMAL (SQL AVG over integers returns DECIMAL), so the sum is accumulated as
// an arbitrary-precision apd.Decimal and divided with tree.DecimalCtx, exactly
// mirroring the standard SQL engine. For floating-point inputs the result is a
// float64 (SQL AVG over floats returns FLOAT).
// ---------------------------------------------------------------------------
type meanAgg struct {
	inType arrow.DataType
	sumD   apd.Decimal
	cntI   int64
	sumF   float64
	cntF   int64
}

func (m *meanAgg) Consume(arr arrow.Array, sel []int32) error {
	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		hasNulls := a.NullN() > 0
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					tree.ExactCtx.Add(&m.sumD, &m.sumD, apd.New(vals[i], 0))
					m.cntI++
				}
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					tree.ExactCtx.Add(&m.sumD, &m.sumD, apd.New(vals[i], 0))
					m.cntI++
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					tree.ExactCtx.Add(&m.sumD, &m.sumD, apd.New(vals[i], 0))
					m.cntI++
				}
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					tree.ExactCtx.Add(&m.sumD, &m.sumD, apd.New(vals[ii], 0))
					m.cntI++
				}
			}
		}
	case *array.Decimal128:
		sc := a.DataType().(*arrow.Decimal128Type).Scale
		dvals := a.Values()
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := range dvals {
				if hasNulls && a.IsNull(i) {
					continue
				}
				v := decimal128ToApd(dvals[i], sc)
				tree.ExactCtx.Add(&m.sumD, &m.sumD, &v)
				m.cntI++
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				v := decimal128ToApd(dvals[ii], sc)
				tree.ExactCtx.Add(&m.sumD, &m.sumD, &v)
				m.cntI++
			}
		}
	case *array.Float64:
		vals := a.Float64Values()
		hasNulls := a.NullN() > 0
		if sel == nil {
			if !hasNulls {
				for i := range vals {
					m.sumF += vals[i]
					m.cntF++
				}
			} else {
				for i := range vals {
					if a.IsNull(i) {
						continue
					}
					m.sumF += vals[i]
					m.cntF++
				}
			}
		} else {
			if !hasNulls {
				for _, i := range sel {
					m.sumF += vals[i]
					m.cntF++
				}
			} else {
				for _, i := range sel {
					ii := int(i)
					if a.IsNull(ii) {
						continue
					}
					m.sumF += vals[ii]
					m.cntF++
				}
			}
		}
	default:
		return fmt.Errorf("arrow mean unsupported on input type %s", arr.DataType())
	}
	return nil
}

func (m *meanAgg) MergeFrom(other scalarAggregator) error {
	o := other.(*meanAgg)
	if m.inType.ID() == arrow.INT64 {
		tree.ExactCtx.Add(&m.sumD, &m.sumD, &o.sumD)
		m.cntI += o.cntI
	} else {
		m.sumF += o.sumF
		m.cntF += o.cntF
	}
	return nil
}

func (m *meanAgg) Finalize() (scalar.Scalar, error) {
	if m.inType.ID() != arrow.FLOAT64 {
		// Integer or decimal input -> SQL AVG returns DECIMAL.
		if m.cntI == 0 {
			return scalar.MakeNullScalar(meanDecimalType), nil
		}
		var res apd.Decimal
		if _, err := tree.DecimalCtx.Quo(&res, &m.sumD, apd.New(m.cntI, 0)); err != nil {
			return nil, err
		}
		num, err := apdToDecimal128(&res, meanDecimalType.Scale)
		if err != nil {
			return nil, err
		}
		return scalar.NewDecimal128Scalar(num, meanDecimalType), nil
	}
	if m.cntF == 0 {
		return scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64), nil
	}
	return scalar.NewFloat64Scalar(m.sumF / float64(m.cntF)), nil
}

// aggregateArray applies a single scalar aggregate kernel to arr and returns the
// resulting scalar. It is the pure-Arrow equivalent of calling
// arrow::compute::CallFunction("hash_sum"/"sum", ...) in C++.
func aggregateArray(fn string, arr arrow.Array) (scalar.Scalar, error) {
	op, err := parseAggOp(fn)
	if err != nil {
		return nil, err
	}
	if op == aggOpCountAll {
		return nil, fmt.Errorf("count_all has no per-array aggregator")
	}
	agg, err := newScalarAggregator(op, arr.DataType())
	if err != nil {
		return nil, err
	}
	if err := agg.Consume(arr, nil); err != nil {
		return nil, err
	}
	return agg.Finalize()
}

// ---------------------------------------------------------------------------
// arrowHashAggregator mirrors C++ HashAggregateFunction: it hashes input rows into
// groups (by the grouping columns), keeps one ScalarAggregator per
// (group, aggregate), feeds each group's value segment through the kernel, and
// finalizes to one output row per group.
// ---------------------------------------------------------------------------
type arrowHashAggregator struct {
	alloc     memory.Allocator
	groupCols []string
	aggs      []ArrowAggExpr

	states     [][]scalarAggregator // per dense group id, per agg (nil for count_all/ident)
	counts     []int64               // COUNT(*) per dense group id
	order      []int32               // dense group ids in first-seen order
	sels       [][]int32             // row selection vector per dense group id (batched feed)
	keyRecs    []arrow.Record   // retained group-key row per dense group id (captured at discovery)
	groupTypes []arrow.DataType // group col arrow types, captured from first input batch
	table      *arrowGroupTable // open-addressing group table (replaces map[string])
	hashSeed   maphash.Hash     // reused hashing state
	inTypes    []arrow.DataType // resolved input type per agg (for output typing)

	// singleInt fast path: when there is exactly one grouping column and it is a
	// non-nullable-able INT64, we skip the general hash table entirely and map the
	// raw int64 key directly to a dense group id with a Go map. This removes the
	// per-row maphash + linear-probe + value-equality cost that dominated the
	// grouped path and is the single largest lever for closing the ~1.4x gap to
	// colexec on the common "GROUP BY <int id>" workload.
	singleInt   bool
	intGroups   map[int64]int32 // group value -> dense group id (singleInt mode)
	hasNullGrp  bool            // whether the null group key has been seen
	nullGID     int32           // dense id for the NULL group key (singleInt mode)
}

func newArrowHashAggregator(alloc memory.Allocator, groupCols []string, aggs []ArrowAggExpr) *arrowHashAggregator {
	return &arrowHashAggregator{
		alloc:     alloc,
		groupCols: groupCols,
		aggs:      aggs,
		states:    make([][]scalarAggregator, 1, 256), // id 0 is invalid; groups start at id 1
		counts:    make([]int64, 1, 256),
		keyRecs:   make([]arrow.Record, 0, 1),
		sels:      make([][]int32, 1, 256),
		order:     nil,
		table:     newArrowGroupTable(5),
		inTypes:   make([]arrow.DataType, len(aggs)),
		intGroups: make(map[int64]int32),
	}
}

// allocGroup allocates a fresh dense group id (1-based; 0 is the invalid
// sentinel), appends the per-group accumulator state, count, retained key row,
// selection vector, and first-seen order entry. It is the single allocation
// point shared by both the general hashing path and the single-int fast path.
func (h *arrowHashAggregator) allocGroup(rec arrow.Record, colIdxs []int, row int) int32 {
	id := int32(len(h.states))
	h.states = append(h.states, nil)
	h.counts = append(h.counts, 0)
	if len(colIdxs) > 0 {
		h.keyRecs = append(h.keyRecs, materializeGroupKeyRow(h.alloc, rec, colIdxs, row))
	}
	h.sels = append(h.sels, nil)
	h.order = append(h.order, id)
	return id
}

// Consume accumulates one input batch into the aggregated state. It is safe to
// call multiple times (e.g. streaming batches); the per-group state is merged
// across calls.
//
// Grouping uses an open-addressing hash table (arrowGroupTable) keyed only by a
// dense group id — mirroring Apache Arrow C++'s GrouperFastImpl, which backs
// native grouped aggregation in Arrow C++. The actual group-key values are
// captured once, at the moment a group is first discovered, into a small
// retained one-row record (see materializeGroupKeyRow) and released only after
// Finalize. This makes aggregation correct across multiple streaming input
// batches: each group's key is owned independently of the batch it was seen in,
// so a later batch cannot clobber an earlier group's key.
func (h *arrowHashAggregator) Consume(ctx context.Context, rec arrow.Record) error {
	n := int(rec.NumRows())

	if len(h.groupCols) == 0 {
		// Global aggregation: a single group containing every row. We set up the
		// group and resolve aggregate input types even for an empty batch so that
		// Finalize can emit the single NULL/zero row (SQL semantics for empty input).
		const gid = 1
		if len(h.order) == 0 {
			h.order = []int32{gid}
			h.states = append(h.states, h.newStates(rec))
			h.counts = append(h.counts, 0)
		}
		return h.feedGroup(ctx, gid, rec, allRows(n))
	}

	colIdxs := resolveColIdxs(rec, h.groupCols)
	if len(h.groupTypes) == 0 {
		for _, ci := range colIdxs {
			h.groupTypes = append(h.groupTypes, rec.Column(ci).DataType())
		}
		// Fast path eligibility: exactly one grouping column and it is INT64.
		h.singleInt = len(h.groupCols) == 1 && h.groupTypes[0].ID() == arrow.INT64
	}

	if h.singleInt {
		// Single INT64 group key: map the raw int64 directly to a dense group id.
		// No hashing, no linear probing, no value-equality comparison per row.
		gcol := rec.Column(colIdxs[0]).(*array.Int64)
		for i := 0; i < n; i++ {
			var gid int32
			if gcol.IsNull(i) {
				if !h.hasNullGrp {
					gid = h.allocGroup(rec, colIdxs, i)
					h.hasNullGrp = true
					h.nullGID = gid
					h.states[gid] = h.newStates(rec)
				} else {
					gid = h.nullGID
				}
			} else {
				v := gcol.Value(i)
				if id, ok := h.intGroups[v]; ok {
					gid = id
				} else {
					gid = h.allocGroup(rec, colIdxs, i)
					h.intGroups[v] = gid
					h.states[gid] = h.newStates(rec)
				}
			}
			h.sels[gid] = append(h.sels[gid], int32(i))
		}
	} else {
		for i := 0; i < n; i++ {
			gid, isNew := h.table.findOrInsert(rec, colIdxs, i, h.keyRecs, &h.hashSeed, func() int32 {
				return h.allocGroup(rec, colIdxs, i)
			})
			if isNew {
				// order was already appended inside allocGroup; only the per-agg
				// accumulator state remains to be initialized here.
				h.states[gid] = h.newStates(rec)
			}
			h.sels[gid] = append(h.sels[gid], int32(i))
		}
	}
	// Feed each group's value segment in ONE call with a contiguous selection
	// vector, exactly like colexec feeds a selection vector into its accumulators
	// (and unlike the previous per-row Consume). This collapses ~n scalar-agg
	// Consume calls into ~|groups| batched calls, which is the dominant win for
	// the grouped path: colexec does the same and avoids the per-row IsNull/Value
	// accessor overhead that dominated our earlier per-row feed.
	//
	// The feed happens within Consume, against the CURRENT batch's columns, and
	// the per-group selection vector is reset afterwards. This is what makes the
	// aggregator correct across multiple streaming input batches: a group's
	// selection vector only ever contains row indices valid for the batch being
	// fed, so indices from an earlier batch can never leak into a later batch's
	// column. The scalar kernels accumulate state across batches internally.
	for _, gid := range h.order {
		if err := h.feedGroup(ctx, gid, rec, h.sels[gid]); err != nil {
			return err
		}
		h.sels[gid] = h.sels[gid][:0]
	}
	return nil
}

// allRows returns a contiguous selection vector [0..n) used for global agg.
func allRows(n int) []int32 {
	idxs := make([]int32, n)
	for i := range idxs {
		idxs[i] = int32(i)
	}
	return idxs
}

// feedGroup pushes one group's value segment through its kernel(s).
func (h *arrowHashAggregator) feedGroup(ctx context.Context, gid int32, rec arrow.Record, idxs []int32) error {
	for i := range h.aggs {
		agg := h.aggs[i]
		switch agg.Func {
		case "count_all":
			h.counts[gid] += int64(len(idxs))
		case "ident":
			// pass-through: nothing to accumulate
		default:
			// Feed the group's value segment straight from the contiguous
			// column buffer via a selection vector (idxs), exactly like colexec
			// feeds a selection vector into its accumulators.
			valCol := arrowOperandColumn(rec, agg.Input)
			if err := h.states[gid][i].Consume(valCol, idxs); err != nil {
				return err
			}
		}
	}
	return nil
}

// newStates builds the per-agg ScalarAggregator for a freshly seen group and
// records the resolved input type of each aggregate for output typing.
func (h *arrowHashAggregator) newStates(rec arrow.Record) []scalarAggregator {
	st := make([]scalarAggregator, len(h.aggs))
	for i, agg := range h.aggs {
		switch agg.Func {
		case "count_all":
			st[i] = nil
			h.inTypes[i] = arrow.PrimitiveTypes.Int64
		case "ident":
			st[i] = nil
			h.inTypes[i] = arrowOperandColumn(rec, agg.Input).DataType()
		case "count":
			st[i] = &countAgg{}
			h.inTypes[i] = arrowOperandColumn(rec, agg.Input).DataType()
		default:
			inType := arrowOperandColumn(rec, agg.Input).DataType()
			h.inTypes[i] = inType
			op, err := parseAggOp(agg.Func)
			if err != nil {
				st[i] = nil
				continue
			}
			agg2, err := newScalarAggregator(op, inType)
			if err != nil {
				st[i] = nil
				continue
			}
			st[i] = agg2
		}
	}
	return st
}

// Finalize emits one output row per group (group columns first, then aggregates
// in Aggs order). Group-key columns are rebuilt from the per-group key records
// captured at discovery time (see materializeGroupKeyRow), so aggregation is
// correct across multiple streaming input batches.
func (h *arrowHashAggregator) Finalize() (arrow.Record, error) {
	// h.order is appended in first-seen order as groups are discovered, so it is
	// already deterministic; no re-sort is needed (and sorting by a per-batch row
	// index would be incorrect across batches).
	defer func() {
		for _, k := range h.keyRecs {
			k.Release()
		}
		h.keyRecs = nil
	}()

	// Output width: the grouping columns (pass-through, one value per group)
	// followed by the aggregate columns. Global aggregation emits no group
	// columns, matching the pre-existing contract consumed by arrowAggregatorCore.
	numOut := len(h.groupCols) + len(h.aggs)
	fields := make([]arrow.Field, numOut)
	cols := make([]arrow.Array, numOut)
	outIdx := 0

	// Pass-through grouping columns: rebuild them directly from the per-group key
	// records captured at discovery time (see materializeGroupKeyRow). We build
	// fresh arrays owned solely by the output record (rather than borrowing the
	// columns out of the key records), so there is no aliasing / double-release and
	// the result is correct across streaming multi-batch input.
	if len(h.groupCols) > 0 {
		for c := 0; c < len(h.groupCols); c++ {
			dt := h.groupTypes[c]
			b := array.NewBuilder(h.alloc, dt)
			for _, gid := range h.order {
				s := arrayScalarAt(h.keyRecs[gid-1].Column(c), 0)
				appendScalar(b, s, dt)
			}
			cols[outIdx] = b.NewArray()
			b.Release()
			fields[outIdx] = arrow.Field{Name: fmt.Sprintf("col%d", outIdx), Type: dt, Nullable: true}
			outIdx++
		}
	}

	// Aggregate columns.
	for i, agg := range h.aggs {
		outType := aggOutputType(agg.Func, h.inTypes[i])
		b := array.NewBuilder(h.alloc, outType)
		for _, gid := range h.order {
			var sc scalar.Scalar
			if agg.Func == "count_all" {
				sc = scalar.NewInt64Scalar(h.counts[gid])
			} else {
				var err error
				sc, err = h.states[gid][i].Finalize()
				if err != nil {
					b.Release()
					h.releaseUpTo(cols, outIdx)
					return nil, err
				}
			}
			appendScalar(b, sc, outType)
		}
		cols[outIdx] = b.NewArray()
		b.Release()
		fields[outIdx] = arrow.Field{Name: fmt.Sprintf("col%d", outIdx), Type: outType, Nullable: true}
		outIdx++
	}

	schema := arrow.NewSchema(fields, nil)
	out := array.NewRecord(schema, cols, int64(len(h.order)))
	// array.NewRecord retains every column; release the builder's initial ref so
	// the record is the sole owner and releasing it frees the buffers.
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}

func (h *arrowHashAggregator) releaseUpTo(cols []arrow.Array, upTo int) {
	for i := 0; i < upTo; i++ {
		if cols[i] != nil {
			cols[i].Release()
		}
	}
}

// arrowGroupHash computes a uint64 hash over a row's grouping columns, mirroring
// colexec's per-row hash (it folds a type-discriminating tag and the value bytes
// into a maphash). It allocates nothing, so it can run once per input row
// instead of building a string key. Collisions are resolved by value equality in
// arrowGroupRowEqual, so the hash need not be perfect — just well-distributed.
//
// The variant taking []int column indices (arrowGroupHashIdx) avoids the
// per-call Schema.FieldIndices name lookup on the hot grouping path.
func arrowGroupHash(rec arrow.Record, cols []string, row int, h *maphash.Hash) uint64 {
	idx := resolveColIdxs(rec, cols)
	return arrowGroupHashIdx(rec, idx, row, h)
}

func arrowGroupHashIdx(rec arrow.Record, idxs []int, row int, h *maphash.Hash) uint64 {
	h.Reset()
	for _, ci := range idxs {
		col := rec.Column(ci)
		if col.IsNull(row) {
			h.Write([]byte{0xff})
			continue
		}
		switch col.DataType().ID() {
		case arrow.INT64:
			h.Write([]byte{'i'})
			v := uint64(col.(*array.Int64).Value(row))
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			h.Write(b[:])
		case arrow.TIMESTAMP:
			h.Write([]byte{'i'})
			v := uint64(int64(col.(*array.Timestamp).Value(row)))
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			h.Write(b[:])
		case arrow.FLOAT64:
			h.Write([]byte{'f'})
			v := math.Float64bits(col.(*array.Float64).Value(row))
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			h.Write(b[:])
		case arrow.BOOL:
			h.Write([]byte{'b'})
			if col.(*array.Boolean).Value(row) {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case arrow.STRING:
			h.Write([]byte{'s'})
			h.WriteString(col.(*array.String).Value(row))
		case arrow.DECIMAL128:
			h.Write([]byte{'d'})
			num := col.(*array.Decimal128).Value(row)
			var b [16]byte
			binary.BigEndian.PutUint64(b[0:8], uint64(num.HighBits()))
			binary.BigEndian.PutUint64(b[8:16], num.LowBits())
			h.Write(b[:])
		default:
			// Degenerate grouping column (non-comparable type): make every row
			// its own group, matching the row-indexed string-key fallback above.
			h.Write([]byte{'x'})
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(row))
			h.Write(b[:])
		}
	}
	return h.Sum64()
}

// arrowGroupRowEqual reports whether rows i and j have equal values across all
// grouping columns (null-aware). It is used to split a hash bucket that happens
// to contain more than one real group due to a hash collision.
func arrowGroupRowEqual(rec arrow.Record, cols []string, i, j int) bool {
	idx := resolveColIdxs(rec, cols)
	return arrowGroupRowEqualIdx(rec, idx, i, j)
}

func arrowGroupRowEqualIdx(rec arrow.Record, idxs []int, i, j int) bool {
	for _, ci := range idxs {
		col := rec.Column(ci)
		ni, nj := col.IsNull(i), col.IsNull(j)
		if ni || nj {
			if ni != nj {
				return false
			}
			continue
		}
		switch col.DataType().ID() {
		case arrow.INT64:
			if col.(*array.Int64).Value(i) != col.(*array.Int64).Value(j) {
				return false
			}
		case arrow.TIMESTAMP:
			if int64(col.(*array.Timestamp).Value(i)) != int64(col.(*array.Timestamp).Value(j)) {
				return false
			}
		case arrow.FLOAT64:
			if col.(*array.Float64).Value(i) != col.(*array.Float64).Value(j) {
				return false
			}
		case arrow.BOOL:
			if col.(*array.Boolean).Value(i) != col.(*array.Boolean).Value(j) {
				return false
			}
		case arrow.STRING:
			if col.(*array.String).Value(i) != col.(*array.String).Value(j) {
				return false
			}
		case arrow.DECIMAL128:
			if col.(*array.Decimal128).Value(i) != col.(*array.Decimal128).Value(j) {
				return false
			}
		default:
			return i == j
		}
	}
	return true
}

// arrowGroupKeyEqual reports whether the row's grouping-column values equal a
// previously captured group key (null-aware). Unlike arrowGroupRowEqualIdx, the
// key is a retained 1-row record (see materializeGroupKeyRow), so equality is
// correct across input batches: a group first seen in an earlier batch keeps its
// key independent of the batch currently being processed.
func arrowGroupKeyEqual(rec arrow.Record, idxs []int, row int, key arrow.Record) bool {
	for c, ci := range idxs {
		col := rec.Column(ci)
		ni, nk := col.IsNull(row), key.Column(c).IsNull(0)
		if ni || nk {
			if ni != nk {
				return false
			}
			continue
		}
		switch col.DataType().ID() {
		case arrow.INT64:
			if col.(*array.Int64).Value(row) != key.Column(c).(*array.Int64).Value(0) {
				return false
			}
		case arrow.TIMESTAMP:
			if int64(col.(*array.Timestamp).Value(row)) != int64(key.Column(c).(*array.Timestamp).Value(0)) {
				return false
			}
		case arrow.FLOAT64:
			if col.(*array.Float64).Value(row) != key.Column(c).(*array.Float64).Value(0) {
				return false
			}
		case arrow.BOOL:
			if col.(*array.Boolean).Value(row) != key.Column(c).(*array.Boolean).Value(0) {
				return false
			}
		case arrow.STRING:
			if col.(*array.String).Value(row) != key.Column(c).(*array.String).Value(0) {
				return false
			}
		case arrow.DECIMAL128:
			if col.(*array.Decimal128).Value(row) != key.Column(c).(*array.Decimal128).Value(0) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// resolveColIdxs resolves grouping column names to integer indices once, so the
// hot grouping path never pays Schema.FieldIndices (a map-backed lookup) per row.
func resolveColIdxs(rec arrow.Record, cols []string) []int {
	idx := make([]int, len(cols))
	for k, gc := range cols {
		fi := rec.Schema().FieldIndices(gc)
		if len(fi) == 0 {
			panic(fmt.Sprintf("arrow grouping column %q not found", gc))
		}
		idx[k] = fi[0]
	}
	return idx
}

// arrowGroupTable is an open-addressing hash table mapping each distinct group
// (identified by its grouping-column values in the *current* input batch) to a
// dense group id. It replaces the original map[string]*arrowGroupKey used by
// both the standalone arrowHashAggregator and arrowAggregatorCore.
//
// Rationale (mirrors Apache Arrow C++'s GrouperFastImpl, which backs the native
// grouped aggregation in Arrow C++): the group id is the sole piece of state we
// need to keep between batches; the actual group-key values are re-materialized
// from the input record on demand via MaterializeRow. This keeps the long-lived
// state to a single growable array of uint32 ids (no per-group heap allocation,
// no Go string materialization), closing the gap to colexec's grouped path.
//
// Collisions are resolved by value equality (arrowGroupRowEqual), so the hash
// need only be well-distributed. Groups are never deleted, so the table uses a
// simple linear-probing layout with empty markers only (no tombstones), which
// keeps the probe logic trivially correct.
type arrowGroupTable struct {
	groups []int32  // dense group id per slot; -1 = empty
	hashes []uint64 // cached hash per occupied slot
	mask   uint64   // len(groups)-1, len is a power of two
}

const arrowGroupEmpty int32 = -1

// newArrowGroupTable returns an empty table with initial capacity 2^minBits.
func newArrowGroupTable(minBits uint) *arrowGroupTable {
	cap := uint64(1) << minBits
	groups := make([]int32, cap)
	for i := range groups {
		groups[i] = arrowGroupEmpty
	}
	return &arrowGroupTable{
		groups: groups,
		hashes: make([]uint64, cap),
		mask:   cap - 1,
	}
}

// grow doubles the table capacity and re-inserts all live entries.
func (t *arrowGroupTable) grow() {
	oldGroups, oldHashes := t.groups, t.hashes
	cap := uint64(len(oldGroups)) << 1
	groups := make([]int32, cap)
	for i := range groups {
		groups[i] = arrowGroupEmpty
	}
	t.groups = groups
	t.hashes = make([]uint64, cap)
	t.mask = cap - 1
	for i := range oldGroups {
		if g := oldGroups[i]; g != arrowGroupEmpty {
			t.insert(oldHashes[i], g)
		}
	}
}

func (t *arrowGroupTable) insert(h uint64, id int32) {
	slot := h & t.mask
	for {
		if t.groups[slot] == arrowGroupEmpty {
			t.groups[slot] = id
			t.hashes[slot] = h
			return
		}
		slot = (slot + 1) & t.mask
	}
}

// findOrInsert returns the dense group id for the row's grouping columns,
// creating a new id (via allocGroupID) if the group is unseen. idxs are the
// pre-resolved integer column indices (resolved once in Consume); keyRecs holds
// the captured group-key record per dense group id (so value equality is correct
// regardless of which input batch a group was first seen in); h is reused across
// calls to avoid allocation.
func (t *arrowGroupTable) findOrInsert(rec arrow.Record, idxs []int, row int, keyRecs []arrow.Record, h *maphash.Hash, allocGroupID func() int32) (int32, bool) {
	hash := arrowGroupHashIdx(rec, idxs, row, h)
	// Probe for an existing group, stopping at the first empty slot.
	slot := hash & t.mask
	var firstEmpty uint64 = ^uint64(0)
	for {
		g := t.groups[slot]
		if g == arrowGroupEmpty {
			firstEmpty = slot
			break
		}
		if t.hashes[slot] == hash && arrowGroupKeyEqual(rec, idxs, row, keyRecs[g-1]) {
			return g, false
		}
		slot = (slot + 1) & t.mask
	}
	// Not found: grow if the load is high, then insert at the first empty slot.
	if t.used()*2 >= len(t.groups) {
		t.grow()
		return t.findOrInsert(rec, idxs, row, keyRecs, h, allocGroupID)
	}
	id := allocGroupID()
	t.groups[firstEmpty] = id
	t.hashes[firstEmpty] = hash
	return id, true
}

// used returns the number of occupied slots (tracked by scanning is O(cap); only
// called on the insert path as a cheap load-factor check).
func (t *arrowGroupTable) used() int {
	n := 0
	for _, g := range t.groups {
		if g != arrowGroupEmpty {
			n++
		}
	}
	return n
}

// materializeGroupKeyRow copies one input row's grouping columns into a new,
// owned one-row record. The copy is independent of the source batch's lifetime,
// so the group key survives after the batch is released — this is what makes
// streaming multi-batch aggregation correct (a group first seen in an earlier
// batch keeps its key even after that batch is gone).
func materializeGroupKeyRow(alloc memory.Allocator, rec arrow.Record, colIdxs []int, row int) arrow.Record {
	nc := len(colIdxs)
	fields := make([]arrow.Field, nc)
	cols := make([]arrow.Array, nc)
	for c, ci := range colIdxs {
		dt := rec.Column(ci).DataType()
		fields[c] = rec.Schema().Field(ci)
		b := array.NewBuilder(alloc, dt)
		src := rec.Column(ci)
		if src.IsNull(row) {
			b.AppendNull()
		} else {
			s := arrayScalarAt(src, row)
			appendScalar(b, s, dt)
		}
		cols[c] = b.NewArray()
		b.Release()
	}
	out := array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return out
}


