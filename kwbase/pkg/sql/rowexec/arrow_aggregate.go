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
	"sort"
	"strings"

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
		hasNulls := a.NullN() > 0
		if sel == nil {
			for i := 0; i < a.Len(); i++ {
				if hasNulls && a.IsNull(i) {
					continue
				}
				v := decimal128ToApd(a.Value(i), sc)
				tree.ExactCtx.Add(&m.sumD, &m.sumD, &v)
				m.cntI++
			}
		} else {
			for _, i := range sel {
				ii := int(i)
				if hasNulls && a.IsNull(ii) {
					continue
				}
				v := decimal128ToApd(a.Value(ii), sc)
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

	states   map[string][]scalarAggregator // per group, per agg (nil for count_all/ident)
	counts   map[string]int64              // COUNT(*) per group
	order    []string                      // first-seen group order
	groupRec map[string]arrow.Record       // retained record holding each group's first row
	groupRow map[string]int32              // first row index of each group
	inTypes  []arrow.DataType              // resolved input type per agg (for output typing)
}

func newArrowHashAggregator(alloc memory.Allocator, groupCols []string, aggs []ArrowAggExpr) *arrowHashAggregator {
	return &arrowHashAggregator{
		alloc:     alloc,
		groupCols: groupCols,
		aggs:      aggs,
		states:    make(map[string][]scalarAggregator),
		counts:    make(map[string]int64),
		groupRec:  make(map[string]arrow.Record),
		groupRow:  make(map[string]int32),
		inTypes:   make([]arrow.DataType, len(aggs)),
	}
}

// Consume accumulates one input batch into the aggregated state. It is safe to
// call multiple times (e.g. streaming batches); the per-group state is merged
// across calls.
// groupFeed pairs a group's string key (for the states map) with the selection
// vector of its rows in this batch.
type groupFeed struct {
	key string
	sel []int32
}

func (h *arrowHashAggregator) Consume(ctx context.Context, rec arrow.Record) error {
	n := int(rec.NumRows())
	if n == 0 && len(h.order) > 0 {
		// Empty batch: nothing to add to existing groups.
		return nil
	}

	var feeds []groupFeed

	if len(h.groupCols) == 0 {
		// Global aggregation: a single group containing every row.
		idxs := make([]int32, 0, n)
		for i := 0; i < n; i++ {
			idxs = append(idxs, int32(i))
		}
		feeds = []groupFeed{{key: "", sel: idxs}}
	} else {
		// colexec-style hash grouping: hash each row into a uint64 (no per-row
		// string allocation, the dominant cost of the old arrowGroupKey path),
		// bucket rows by hash, then resolve the rare hash collision by comparing
		// the actual group-column values. A string key is materialized only once
		// per *distinct* group (for the states map), not once per row. Mirrors
		// colexec's hashAggregator, which never builds a string key either.
		var seed maphash.Hash
		seed.SetSeed(maphash.MakeSeed())
		hashes := make([]uint64, n)
		for i := 0; i < n; i++ {
			hashes[i] = arrowGroupHash(rec, h.groupCols, i, &seed)
		}
		buckets := make(map[uint64][]int32, n)
		for i := 0; i < n; i++ {
			buckets[hashes[i]] = append(buckets[hashes[i]], int32(i))
		}
		for _, rows := range buckets {
			// Partition this hash bucket into distinct groups by value equality
			// (the bucket may hold more than one real group on a hash collision).
			var reps []int32
			groupOf := make([]int, len(rows))
			for gi, r := range rows {
				ri := int(r)
				matched := -1
				for ri2, rep := range reps {
					if arrowGroupRowEqual(rec, h.groupCols, ri, int(rep)) {
						matched = ri2
						break
					}
				}
				if matched < 0 {
					reps = append(reps, r)
					matched = len(reps) - 1
				}
				groupOf[gi] = matched
			}
			for g := 0; g < len(reps); g++ {
				sel := make([]int32, 0, len(rows))
				for gi, r := range rows {
					if groupOf[gi] == g {
						sel = append(sel, r)
					}
				}
				feeds = append(feeds, groupFeed{key: arrowGroupKey(rec, h.groupCols, int(reps[g])), sel: sel})
			}
		}
	}

	for _, gf := range feeds {
		key := gf.key
		if _, exists := h.states[key]; !exists {
			h.order = append(h.order, key)
			h.states[key] = h.newStates(rec)
			if n > 0 {
				rec.Retain()
				h.groupRec[key] = rec
				h.groupRow[key] = gf.sel[0]
			}
		}
		if err := h.feedGroup(ctx, key, rec, gf.sel); err != nil {
			return err
		}
	}
	return nil
}

// feedGroup pushes one group's value segment through its kernel(s).
func (h *arrowHashAggregator) feedGroup(ctx context.Context, key string, rec arrow.Record, idxs []int32) error {
	for i := range h.aggs {
		agg := h.aggs[i]
		switch agg.Func {
		case "count_all":
			h.counts[key] += int64(len(idxs))
		case "ident":
			// pass-through: nothing to accumulate
		default:
			// Feed the group's value segment straight from the contiguous
			// column buffer via a selection vector (idxs), exactly like colexec
			// feeds a selection vector into its accumulators. This avoids the
			// per-group compute.take allocation + copy that the previous
			// implementation performed for every (group, aggregate).
			valCol := arrowOperandColumn(rec, agg.Input)
			if err := h.states[key][i].Consume(valCol, idxs); err != nil {
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
// in Aggs order). The retained group records are released before returning.
func (h *arrowHashAggregator) Finalize() (arrow.Record, error) {
	// The grouping in Consume partitions rows by hash into a Go map, whose
	// iteration order is intentionally non-deterministic. That makes the order
	// in which distinct groups are first discovered (and thus appended to
	// h.order) non-deterministic too. Restore the original "first-seen row
	// order" semantic by sorting the output groups by the first row index of
	// each group. This is a pure output-ordering fix: per-group state is keyed
	// by group and accumulated commutatively, so the feed order never affects
	// aggregate values.
	sort.SliceStable(h.order, func(i, j int) bool {
		return h.groupRow[h.order[i]] < h.groupRow[h.order[j]]
	})

	defer func() {
		for _, rec := range h.groupRec {
			rec.Release()
		}
	}()

	numOut := len(h.groupCols) + len(h.aggs)
	fields := make([]arrow.Field, numOut)
	cols := make([]arrow.Array, numOut)
	outIdx := 0

	// Pass-through grouping columns: one value per group (its first row).
	for _, gc := range h.groupCols {
		var colType arrow.DataType
		if len(h.order) > 0 {
			colType = arrowOperandColumn(h.groupRec[h.order[0]], gc).DataType()
		} else {
			colType = arrow.PrimitiveTypes.Int64
		}
		b := array.NewBuilder(h.alloc, colType)
		for _, key := range h.order {
			col := arrowOperandColumn(h.groupRec[key], gc)
			sc := arrayScalarAt(col, int(h.groupRow[key]))
			appendScalar(b, sc, colType)
		}
		cols[outIdx] = b.NewArray()
		b.Release()
		fields[outIdx] = arrow.Field{Name: fmt.Sprintf("col%d", outIdx), Type: colType, Nullable: true}
		outIdx++
	}

	// Aggregate columns.
	for i, agg := range h.aggs {
		outType := aggOutputType(agg.Func, h.inTypes[i])
		b := array.NewBuilder(h.alloc, outType)
		for _, key := range h.order {
			var sc scalar.Scalar
			if agg.Func == "count_all" {
				sc = scalar.NewInt64Scalar(h.counts[key])
			} else if agg.Func == "ident" {
				col := arrowOperandColumn(h.groupRec[key], agg.Input)
				sc = arrayScalarAt(col, int(h.groupRow[key]))
			} else {
				var err error
				sc, err = h.states[key][i].Finalize()
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
	return array.NewRecord(schema, cols, int64(len(h.order))), nil
}

func (h *arrowHashAggregator) releaseUpTo(cols []arrow.Array, upTo int) {
	for i := 0; i < upTo; i++ {
		if cols[i] != nil {
			cols[i].Release()
		}
	}
}

// arrowGroupKey builds a canonical, collision-free key for row over the grouping
// columns. It is the free-function form of arrowAggregatorCore.groupKey so the
// standalone arrowHashAggregator can reuse the same grouping semantics.
func arrowGroupKey(rec arrow.Record, cols []string, row int) string {
	var sb strings.Builder
	for _, gc := range cols {
		col := arrowOperandColumn(rec, gc)
		if col.IsNull(row) {
			sb.WriteString("\x00∅\x00")
			continue
		}
		switch col.DataType().ID() {
		case arrow.INT64:
			fmt.Fprintf(&sb, "i%d\x1f", col.(*array.Int64).Value(row))
		case arrow.TIMESTAMP:
			fmt.Fprintf(&sb, "i%d\x1f", int64(col.(*array.Timestamp).Value(row)))
		case arrow.FLOAT64:
			fmt.Fprintf(&sb, "f%g\x1f", col.(*array.Float64).Value(row))
		case arrow.BOOL:
			fmt.Fprintf(&sb, "b%v\x1f", col.(*array.Boolean).Value(row))
		case arrow.STRING:
			sb.WriteString("s")
			sb.WriteString(col.(*array.String).Value(row))
			sb.WriteString("\x1f")
		case arrow.DECIMAL128:
			num := col.(*array.Decimal128).Value(row)
			var buf [16]byte
			binary.BigEndian.PutUint64(buf[0:8], uint64(num.HighBits()))
			binary.BigEndian.PutUint64(buf[8:16], num.LowBits())
			sb.WriteString("d")
			sb.Write(buf[:])
			sb.WriteString("\x1f")
		default:
			fmt.Fprintf(&sb, "x%d\x1f", row)
		}
	}
	return sb.String()
}

// arrowGroupHash computes a uint64 hash over a row's grouping columns, mirroring
// colexec's per-row hash (it folds a type-discriminating tag and the value bytes
// into a maphash). It allocates nothing, so it can run once per input row
// instead of building a string key. Collisions are resolved by value equality in
// arrowGroupRowEqual, so the hash need not be perfect — just well-distributed.
func arrowGroupHash(rec arrow.Record, cols []string, row int, h *maphash.Hash) uint64 {
	h.Reset()
	for _, gc := range cols {
		col := arrowOperandColumn(rec, gc)
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
			h.WriteString(gc)
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
	for _, gc := range cols {
		col := arrowOperandColumn(rec, gc)
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
