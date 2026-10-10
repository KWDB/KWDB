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

package enginepb_test

import (
	"encoding/binary"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

func BenchmarkScanDecodeKeyValue(b *testing.B) {
	key := roachpb.Key("blah blah blah")
	ts := hlc.Timestamp{WallTime: int64(1000000)}
	value := []byte("foo foo foo")
	rep := make([]byte, 8)
	keyBytes := relational.EncodeKey(relational.MVCCKey{Key: key, Timestamp: ts})
	binary.LittleEndian.PutUint64(rep, uint64(len(keyBytes)<<32)|uint64(len(value)))
	rep = append(rep, keyBytes...)
	rep = append(rep, value...)
	b.Run("getTs=true", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var err error
			_, _, _, _, err = enginepb.ScanDecodeKeyValue(rep)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("getTs=false", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var err error
			_, _, _, err = enginepb.ScanDecodeKeyValueNoTS(rep)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
