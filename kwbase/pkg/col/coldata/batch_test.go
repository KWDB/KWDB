// Copyright 2019 The Cockroach Authors.
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

package coldata_test

import (
	"reflect"
	"testing"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestBatchReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	resetAndCheck := func(b coldata.Batch, typs []coltypes.T, n int, shouldReuse bool) {
		t.Helper()
		// Use the data backing the ColVecs slice as a proxy for when things get
		// reallocated.
		vecsBefore := b.ColVecs()
		ptrBefore := (*reflect.SliceHeader)(unsafe.Pointer(&vecsBefore))
		b.Reset(typs, n)
		vecsAfter := b.ColVecs()
		ptrAfter := (*reflect.SliceHeader)(unsafe.Pointer(&vecsAfter))
		assert.Equal(t, shouldReuse, ptrBefore.Data == ptrAfter.Data)
		assert.Equal(t, n, b.Length())
		assert.Equal(t, len(typs), b.Width())

		assert.Nil(t, b.Selection())
		b.SetSelection(true)
		// Invariant: selection vector length matches batch length
		// Invariant: all cap(column) is >= cap(selection vector)
		assert.Equal(t, n, len(b.Selection()))
		selCap := cap(b.Selection())

		for i, vec := range b.ColVecs() {
			assert.False(t, vec.MaybeHasNulls())
			assert.False(t, vec.Nulls().NullAt(0))
			assert.Equal(t, typs[i], vec.Type())
			// Sanity check that we can actually use the column. This is mostly for
			// making sure a flat bytes column gets reset.
			vec.Nulls().SetNull(0)
			assert.True(t, vec.Nulls().NullAt(0))
			switch vec.Type() {
			case coltypes.Int64:
				x := vec.Int64()
				assert.True(t, len(x) >= n)
				assert.True(t, cap(x) >= selCap)
				x[0] = 1
			case coltypes.Bytes:
				x := vec.Bytes()
				assert.True(t, x.Len() >= n)
				x.Set(0, []byte{1})
			default:
				panic(vec.Type())
			}
		}
	}

	typsInt := []coltypes.T{coltypes.Int64}
	typsBytes := []coltypes.T{coltypes.Bytes}
	typsIntBytes := []coltypes.T{coltypes.Int64, coltypes.Bytes}
	var b coldata.Batch

	// Simple case, reuse
	b = coldata.NewMemBatch(typsInt)
	resetAndCheck(b, typsInt, 1, true)

	// Types don't match, don't reuse
	b = coldata.NewMemBatch(typsInt)
	resetAndCheck(b, typsBytes, 1, false)

	// Columns are a prefix, reuse
	b = coldata.NewMemBatch(typsIntBytes)
	resetAndCheck(b, typsInt, 1, true)

	// Exact length, reuse
	b = coldata.NewMemBatchWithSize(typsInt, 1)
	resetAndCheck(b, typsInt, 1, true)

	// Insufficient capacity, don't reuse
	b = coldata.NewMemBatchWithSize(typsInt, 1)
	resetAndCheck(b, typsInt, 2, false)

	// Selection vector gets reset
	b = coldata.NewMemBatchWithSize(typsInt, 1)
	b.SetSelection(true)
	b.Selection()[0] = 7
	resetAndCheck(b, typsInt, 1, true)

	// Nulls gets reset
	b = coldata.NewMemBatchWithSize(typsInt, 1)
	b.ColVec(0).Nulls().SetNull(0)
	resetAndCheck(b, typsInt, 1, true)

	// Bytes columns use a different impl than everything else
	b = coldata.NewMemBatch(typsBytes)
	resetAndCheck(b, typsBytes, 1, true)
}
