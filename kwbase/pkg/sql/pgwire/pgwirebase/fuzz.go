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

//go:build gofuzz
// +build gofuzz

package pgwirebase

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
)

var (
	timeCtx = tree.NewParseTimeContext(timeutil.Now())
	// Compile a slice of all oids.
	oids = func() []oid.Oid {
		var ret []oid.Oid
		for oid := range types.OidToType {
			ret = append(ret, oid)
		}
		return ret
	}()
)

func FuzzDecodeOidDatum(data []byte) int {
	if len(data) < 2 {
		return 0
	}

	id := oids[int(data[1])%len(oids)]
	code := FormatCode(data[0]) % (FormatBinary + 1)
	b := data[2:]

	_, err := DecodeOidDatum(timeCtx, id, code, b)
	if err != nil {
		return 0
	}
	return 1
}
