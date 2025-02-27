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

package typeconv

import (
	"fmt"
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/pkg/errors"
)

// FromColumnType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to
// colexec.allSupportedSQLTypes as well.
func FromColumnType(ct *types.T) coltypes.T {
	switch ct.Family() {
	case types.BoolFamily:
		return coltypes.Bool
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		return coltypes.Bytes
	case types.DateFamily, types.OidFamily:
		return coltypes.Int64
	case types.DecimalFamily:
		return coltypes.Decimal
	case types.IntFamily:
		switch ct.Width() {
		case 8:
			return coltypes.Int16
		case 16:
			return coltypes.Int16
		case 32:
			return coltypes.Int32
		case 0, 64:
			return coltypes.Int64
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("integer with unknown width %d", ct.Width()))
	case types.FloatFamily:
		return coltypes.Float64
	case types.TimestampFamily:
		return coltypes.Timestamp
	case types.TimestampTZFamily:
		return coltypes.Timestamp
	case types.IntervalFamily:
		return coltypes.Interval
	}
	return coltypes.Unhandled
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []types.T) ([]coltypes.T, error) {
	typs := make([]coltypes.T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(&cts[i])
		if typs[i] == coltypes.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

// ToColumnType converts a types.T that corresponds to the column type. Note
// that due to the fact that multiple types.T's are represented by a single
// column type, this conversion might return the type that is unexpected.
// NOTE: this should only be used in tests.
func ToColumnType(t coltypes.T) *types.T {
	switch t {
	case coltypes.Bool:
		return types.Bool
	case coltypes.Bytes:
		return types.Bytes
	case coltypes.Decimal:
		return types.Decimal
	case coltypes.Int16:
		return types.Int2
	case coltypes.Int32:
		return types.Int4
	case coltypes.Int64:
		return types.Int
	case coltypes.Float64:
		return types.Float
	case coltypes.Timestamp:
		return types.Timestamp
	case coltypes.Interval:
		return types.Interval
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected coltype %s", t.String()))
	return nil
}

// ToColumnTypes calls ToColumnType on each element of typs returning the
// resulting slice.
func ToColumnTypes(typs []coltypes.T) []types.T {
	cts := make([]types.T, len(typs))
	for i := range cts {
		t := ToColumnType(typs[i])
		cts[i] = *t
	}
	return cts
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) (interface{}, error) {
	switch ct.Family() {
	case types.BoolFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case types.BytesFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.IntFamily:
		switch ct.Width() {
		case 8:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int16(*d), nil
			}
		case 16:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int16(*d), nil
			}
		case 32:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int32(*d), nil
			}
		case 0, 64:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int64(*d), nil
			}
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case types.DateFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return d.UnixEpochDaysWithOrig(), nil
		}
	case types.FloatFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case types.OidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case types.StringFamily:
		return func(datum tree.Datum) (interface{}, error) {
			// Handle other STRING-related OID types, like oid.T_name.
			wrapper, ok := datum.(*tree.DOidWrapper)
			if ok {
				datum = wrapper.Wrapped
			}

			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.DecimalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	case types.UuidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DUuid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DUuid, found %s", reflect.TypeOf(datum))
			}
			return d.UUID.GetBytesMut(), nil
		}
	case types.TimestampFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestamp)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestamp, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	case types.TimestampTZFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestampTZ)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestampTZ, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	case types.IntervalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DInterval)
			if !ok {
				return nil, errors.Errorf("expected *tree.DInterval, found %s", reflect.TypeOf(datum))
			}
			return d.Duration, nil
		}
	}
	// It would probably be more correct to return an error here, rather than a
	// function which always returns an error. But since the function tends to be
	// invoked immediately after GetDatumToPhysicalFn is called, this works just
	// as well and makes the error handling less messy for the caller.
	return func(datum tree.Datum) (interface{}, error) {
		return nil, errors.Errorf("unhandled type %s", ct.DebugString())
	}
}
