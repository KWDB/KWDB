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

package colexec

import (
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var alloc = sqlbase.DatumAlloc{}

func TestEncDatumRowsToColVecBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test input: [[false, true], [true, false]]
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.DBoolFalse},
			sqlbase.EncDatum{Datum: tree.DBoolTrue},
		},
		sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.DBoolTrue},
			sqlbase.EncDatum{Datum: tree.DBoolFalse},
		},
	}
	vec := testAllocator.NewMemColumn(coltypes.Bool, 2)
	ct := types.Bool

	// Test converting column 0.
	if err := EncDatumRowsToColVec(testAllocator, rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := testAllocator.NewMemColumn(coltypes.Bool, 2)
	expected.Bool()[0] = false
	expected.Bool()[1] = true
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}

	// Test converting column 1.
	if err := EncDatumRowsToColVec(testAllocator, rows, vec, 1 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected.Bool()[0] = true
	expected.Bool()[1] = false
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}

func TestEncDatumRowsToColVecInt16(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(17)}},
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(42)}},
	}
	vec := testAllocator.NewMemColumn(coltypes.Int16, 2)
	if err := EncDatumRowsToColVec(testAllocator, rows, vec, 0 /* columnIdx */, types.Int2, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := testAllocator.NewMemColumn(coltypes.Int16, 2)
	expected.Int16()[0] = 17
	expected.Int16()[1] = 42
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}

func TestEncDatumRowsToColVecString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDString("foo")}},
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDString("bar")}},
	}
	vec := testAllocator.NewMemColumn(coltypes.Bytes, 2)
	for _, width := range []int32{0, 25} {
		ct := types.MakeString(width)
		vec.Bytes().Reset()
		if err := EncDatumRowsToColVec(testAllocator, rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
			t.Fatal(err)
		}
		expected := testAllocator.NewMemColumn(coltypes.Bytes, 2)
		expected.Bytes().Set(0, []byte("foo"))
		expected.Bytes().Set(1, []byte("bar"))
		if !reflect.DeepEqual(vec, expected) {
			t.Errorf("expected vector %+v, got %+v", expected, vec)
		}
	}
}

func TestEncDatumRowsToColVecDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nRows := 3
	rows := make(sqlbase.EncDatumRows, nRows)
	expected := testAllocator.NewMemColumn(coltypes.Decimal, 3)
	for i, s := range []string{"1.0000", "-3.12", "NaN"} {
		var err error
		dec, err := tree.ParseDDecimal(s)
		if err != nil {
			t.Fatal(err)
		}
		rows[i] = sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: dec}}
		expected.Decimal()[i] = dec.Decimal
	}
	vec := testAllocator.NewMemColumn(coltypes.Decimal, 3)
	ct := types.Decimal
	if err := EncDatumRowsToColVec(testAllocator, rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}
