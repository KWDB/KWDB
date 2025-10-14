// Copyright 2020 The Cockroach Authors.
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

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for hash_utils.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the
	// import block. This was picked because it sorts after
	// "pkg/sql/colexec/execgen" and has no deps.
	_ "gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
)

// {{/*

// Dummy import to pull in "unsafe" package
var _ unsafe.Pointer

// Dummy import to pull in "reflect" package
var _ reflect.SliceHeader

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// _ASSIGN_HASH is the template equality function for assigning the first input
// to the result of the hash value of the second input.
func _ASSIGN_HASH(_, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{/*
func _REHASH_BODY(
	ctx context.Context,
	buckets []uint64,
	keys _GOTYPESLICE,
	nulls *coldata.Nulls,
	nKeys int,
	sel []int,
	_HAS_SEL bool,
	_HAS_NULLS bool,
) { // */}}
	// {{define "rehashBody" -}}
	// Early bounds checks.
	_ = buckets[nKeys-1]
	// {{ if .HasSel }}
	_ = sel[nKeys-1]
	// {{ else }}
	_ = execgen.UNSAFEGET(keys, nKeys-1)
	// {{ end }}
	for i := 0; i < nKeys; i++ {
		cancelChecker.check(ctx)
		// {{ if .HasSel }}
		selIdx := sel[i]
		// {{ else }}
		selIdx := i
		// {{ end }}
		// {{ if .HasNulls }}
		if nulls.NullAt(selIdx) {
			continue
		}
		// {{ end }}
		v := execgen.UNSAFEGET(keys, selIdx)
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v)
		buckets[i] = uint64(p)
	}
	// {{end}}

	// {{/*
}

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func rehash(
	ctx context.Context,
	buckets []uint64,
	t coltypes.T,
	col coldata.Vec,
	nKeys int,
	sel []int,
	cancelChecker CancelChecker,
	decimalScratch decimalOverloadScratch,
) {
	switch t {
	// {{range $hashType := .}}
	case _TYPES_T:
		keys, nulls := col._TemplateType(), col.Nulls()
		if col.MaybeHasNulls() {
			if sel != nil {
				_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, true, true)
			} else {
				_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, false, true)
			}
		} else {
			if sel != nil {
				_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, true, false)
			} else {
				_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, false, false)
			}
		}

	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", t))
	}
}
