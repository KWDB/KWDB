package ape

import (
	"errors"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	duck "github.com/duckdb-go-bindings"
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
