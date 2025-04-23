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

package sqlsmith

import (
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func typeFromName(name string) *types.T {
	typ, err := parser.ParseType(name)
	if err != nil {
		panic(errors.AssertionFailedf("failed to parse type: %v", name))
	}
	return typ
}

// pickAnyType returns a concrete type if typ is types.Any or types.AnyArray,
// otherwise typ.
func (s *Smither) pickAnyType(typ *types.T) (_ *types.T, ok bool) {
	switch typ.Family() {
	case types.AnyFamily:
		typ = s.randType()
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			typ = sqlbase.RandArrayContentsType(s.rnd)
		}
	}
	return typ, s.allowedType(typ)
}

func (s *Smither) randScalarType() *types.T {
	for {
		t := sqlbase.RandScalarType(s.rnd)
		if !s.allowedType(t) {
			continue
		}
		return t
	}
}

func (s *Smither) randType() *types.T {
	for {
		t := sqlbase.RandType(s.rnd)
		if !s.allowedType(t) {
			continue
		}
		return t
	}
}

// allowedType returns whether t is ok to be used. This is useful to filter
// out undesirable types to enable certain execution paths to be taken (like
// vectorization).
func (s *Smither) allowedType(types ...*types.T) bool {
	for _, t := range types {
		if s.vectorizable && typeconv.FromColumnType(t) == coltypes.Unhandled {
			return false
		}
		if _, ok := NotAllowedTypeName[t.Oid()]; ok {
			return false
		}
	}
	return true
}

func (s *Smither) makeDesiredTypes() []*types.T {
	var typs []*types.T
	for {
		typs = append(typs, s.randType())
		if s.d6() < 2 || !s.canRecurse() {
			break
		}
	}
	return typs
}

// NotAllowedTypeName represents a map of types
// that are not allowed in test.
var NotAllowedTypeName = map[oid.Oid]string{
	types.T__int1:     "_INT1",
	types.T__nchar:    "_NCHAR",
	types.T__nvarchar: "_NVARCHAR",
	types.T__varbytea: "_VARBYTES",
	types.T__geometry: "_GEOMETRY",
	types.T__clob:     "_CLOB",
	types.T__blob:     "_BLOB",
}
