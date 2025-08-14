package ape

import (
	"errors"
	"fmt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	duck "github.com/duckdb-go-bindings"
	"math/big"
	"time"
	"unsafe"
)

// vector storage of a DuckDB column.
type vector struct {
	// The underlying DuckDB vector.
	vec duck.Vector
	// The underlying data ptr.
	dataPtr unsafe.Pointer
	// The vector's validity mask.
	maskPtr unsafe.Pointer
	// A callback function to get a value from this vector.
	getFn fnGetVectorValue
	// A callback function to write to this vector.
	setFn fnSetVectorValue
	// The child vectors of nested data types.
	childVectors []vector

	// The vector's type information.
	duck.Type
	// The internal type for ENUM and DECIMAL values.
	internalType duck.Type
}

const aliasJSON = "JSON"

func (vec *vector) init(logicalType duck.LogicalType, colIdx int) error {
	t := duck.GetTypeId(logicalType)
	name, inMap := typeToStringMap[t]
	if !inMap {
		_ = name
		return addIndexToError(errors.New("unsupportedTypeError(name)"), int(colIdx))
	}

	alias := duck.LogicalTypeGetAlias(logicalType)
	switch alias {
	case aliasJSON:
		// todo
		//vec.initJSON()
		return nil
	}

	switch t {
	case duck.TypeBoolean:
		initBool(vec)
	case duck.TypeTinyInt:
		initInt8(vec, t)
	case duck.TypeSmallInt:
		initInt16(vec, t)
	case duck.TypeInteger:
		initInt32(vec, t)
	case duck.TypeBigInt:
		initInt64(vec, t)
	case duck.TypeFloat:
		initFloat32(vec, t)
	case duck.TypeDouble:
		initFloat64(vec, t)
	case duck.TypeDate:
		initDate(vec, t)
	case duck.TypeVarchar, duck.TypeBlob:
		initBytes(vec, t)
	case duck.TypeDecimal:
		return vec.initDecimal(logicalType, colIdx)

	default:
		return addIndexToError(errors.New(" unsupportedTypeError(unknownTypeErrMsg)"), colIdx)
	}
	return nil
}

func (vec *vector) initVectors(v duck.Vector, writable bool) {
	vec.vec = v
	vec.dataPtr = duck.VectorGetData(v)
	if writable {
		duck.VectorEnsureValidityWritable(v)
	}
	vec.maskPtr = duck.VectorGetValidity(v)
	//TODO(ZXY)
	//vec.initChildVectors(v, writable)
}

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx duck.IdxT) tree.Datum

// fnSetVectorValue is the setter callback function for any (nested) vector.
type fnSetVectorValue func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error

func (vec *vector) getNull(rowIdx duck.IdxT) bool {
	if vec.maskPtr == nil {
		return false
	}
	return !duck.ValidityMaskValueIsValid(vec.maskPtr, rowIdx)
}

func (vec *vector) setNull(rowIdx duck.IdxT) {
	duck.ValiditySetRowInvalid(vec.maskPtr, rowIdx)
	if vec.Type == duck.TypeStruct || vec.Type == duck.TypeUnion {
		for i := 0; i < len(vec.childVectors); i++ {
			vec.childVectors[i].setNull(rowIdx)
		}
	}
}

func initBool(vec *vector) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		res := tree.DBool(getBool(vec, rowIdx))
		return &res
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBool(vec, rowIdx, val)
	}
	vec.Type = duck.TypeBoolean
}

func getBool(vec *vector, rowIdx duck.IdxT) bool {
	xs := (*[1 << 31]bool)(vec.dataPtr)
	return xs[rowIdx]
}

func setBool(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]bool)(vec.dataPtr)
	xs[rowIdx] = bool(tree.MustBeDBool(v))
	return nil
}

func initInt8(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDInt(tree.DInt(getInt8(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInt8(vec, rowIdx, val)
	}
	vec.Type = t
}

func getInt8(vec *vector, rowIdx duck.IdxT) int8 {
	xs := (*[1 << 31]int8)(vec.dataPtr)
	return xs[rowIdx]
}

func setInt8(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]int8)(vec.dataPtr)
	xs[rowIdx] = int8(tree.MustBeDInt(v))
	return nil
}

func initInt16(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDInt(tree.DInt(getInt16(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInt16(vec, rowIdx, val)
	}
	vec.Type = t
}

func getInt16(vec *vector, rowIdx duck.IdxT) int16 {
	xs := (*[1 << 31]int16)(vec.dataPtr)
	return xs[rowIdx]
}

func setInt16(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]int16)(vec.dataPtr)
	xs[rowIdx] = int16(tree.MustBeDInt(v))
	return nil
}

func initInt32(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDInt(tree.DInt(getInt32(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInt32(vec, rowIdx, val)
	}
	vec.Type = t
}

func getInt32(vec *vector, rowIdx duck.IdxT) int32 {
	xs := (*[1 << 31]int32)(vec.dataPtr)
	return xs[rowIdx]
}

func setInt32(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]int32)(vec.dataPtr)
	xs[rowIdx] = int32(tree.MustBeDInt(v))
	return nil
}

func initInt64(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDInt(tree.DInt(getInt64(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInt64(vec, rowIdx, val)
	}
	vec.Type = t
}

func getInt64(vec *vector, rowIdx duck.IdxT) int64 {
	xs := (*[1 << 31]int64)(vec.dataPtr)
	return xs[rowIdx]
}

func setInt64(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]int64)(vec.dataPtr)
	xs[rowIdx] = int64(tree.MustBeDInt(v))
	return nil
}

func initFloat32(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDFloat(tree.DFloat(getFloat32(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setFloat32(vec, rowIdx, val)
	}
	vec.Type = t
}

func getFloat32(vec *vector, rowIdx duck.IdxT) float32 {
	xs := (*[1 << 31]float32)(vec.dataPtr)
	return xs[rowIdx]
}

func setFloat32(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]float32)(vec.dataPtr)
	xs[rowIdx] = float32(tree.MustBeDFloat(v))
	return nil
}

func initFloat64(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		return tree.NewDFloat(tree.DFloat(getFloat64(vec, rowIdx)))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setFloat64(vec, rowIdx, val)
	}
	vec.Type = t
}

func getFloat64(vec *vector, rowIdx duck.IdxT) float64 {
	xs := (*[1 << 31]float64)(vec.dataPtr)
	return xs[rowIdx]
}

func setFloat64(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	xs := (*[1 << 31]float64)(vec.dataPtr)
	xs[rowIdx] = float64(tree.MustBeDFloat(v))
	return nil
}

func initDate(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		time := vec.getDate(rowIdx)
		d, err := tree.NewDDateFromTime(time)
		if err != nil {
			panic(err)
		}
		return d
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setDate(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) getDate(rowIdx duck.IdxT) time.Time {
	date := (*[1 << 31]duck.Date)(vec.dataPtr)[rowIdx]
	d := duck.FromDate(date)
	year, month, day := duck.DateStructMembers(&d)
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
}

func setDate(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	switch val := v.(type) {
	case *tree.DDate:
		date := duck.NewDate(int32(val.UnixEpochDays()))
		xs := (*[1 << 31]duck.Date)(vec.dataPtr)
		xs[rowIdx] = *date
	}
	return nil
}

func initBytes(vec *vector, t duck.Type) {
	vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
		if vec.getNull(rowIdx) {
			return nil
		}
		b := getBytes(vec, rowIdx)
		if t == duck.TypeVarchar {
			return tree.NewDString(b)
		}
		return tree.NewDBytes(tree.DBytes(b))
	}
	vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBytes(vec, rowIdx, val)
	}
	vec.Type = t
}

func getBytes(vec *vector, rowIdx duck.IdxT) string {
	strT := (*[1 << 31]duck.StringT)(vec.dataPtr)[rowIdx]
	str := duck.StringTData(&strT)
	return str
}

func setBytes(vec *vector, rowIdx duck.IdxT, v tree.Datum) error {
	switch val := v.(type) {
	case *tree.DString:
		duck.VectorAssignStringElement(vec.vec, rowIdx, string(*val))
	case *tree.DBytes:
		duck.VectorAssignStringElementLen(vec.vec, rowIdx, []byte(*val))
	default:
		return errors.New("unsupported type")
	}
	return nil
}

func (vec *vector) initDecimal(logicalType duck.LogicalType, colIdx int) error {

	t := duck.DecimalInternalType(logicalType)
	switch t {
	case duck.TypeSmallInt, duck.TypeInteger, duck.TypeBigInt, duck.TypeHugeInt:
		vec.getFn = func(vec *vector, rowIdx duck.IdxT) tree.Datum {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getDecimal(rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
			if val == nil {
				vec.setNull(rowIdx)
				return nil
			}
			return setDecimal(vec, rowIdx, val)
		}
	default:
		return addIndexToError(errors.New(" unsupportedTypeError(typeToStringMap[t])"), colIdx)
	}

	vec.Type = duck.TypeDecimal
	vec.internalType = t
	return nil
}

func (vec *vector) getDecimal(rowIdx duck.IdxT) tree.Datum {
	var val *big.Int
	var d tree.DDecimal
	switch vec.internalType {
	case duck.TypeSmallInt:
		v := getInt16(vec, rowIdx)
		val = big.NewInt(int64(v))
	case duck.TypeInteger:
		v := getInt32(vec, rowIdx)
		val = big.NewInt(int64(v))
	case duck.TypeBigInt:
		v := getInt64(vec, rowIdx)
		val = big.NewInt(v)
	case duck.TypeHugeInt:
		panic(errors.New(fmt.Sprintf("hugeInt not supported yet")))
	}
	d.Coeff = *val
	return &d
}

func setDecimal(vec *vector, rowIdx duck.IdxT, val tree.Datum) error {
	d, ok := val.(*tree.DDecimal)
	if !ok {
		return errors.New("setDecimal needs a DDecimal")
	}
	v := tree.NewDInt(tree.DInt(d.Coeff.Int64()))
	switch vec.internalType {
	case duck.TypeSmallInt:
		return setInt16(vec, rowIdx, v)
	case duck.TypeInteger:
		return setInt32(vec, rowIdx, v)
	case duck.TypeBigInt:
		return setInt64(vec, rowIdx, v)
	case duck.TypeHugeInt:
		return errors.New(fmt.Sprintf("hugeInt not supported yet"))
		//return setHugeint(vec, rowIdx, val)
	}
	return nil
}
