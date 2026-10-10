// Copyright 2016 The Cockroach Authors.
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

package protoutil_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
)

func TestCloneProto(t *testing.T) {
	testCases := []struct {
		pb          protoutil.Message
		shouldPanic bool
	}{
		// Uncloneable types (all contain UUID fields).
		{&roachpb.StoreIdent{}, true},
		{&enginepb.TxnMeta{}, true},
		{&roachpb.Transaction{}, true},
		{&roachpb.Error{}, true},
		{&protoutil.RecursiveAndUncloneable{}, true},

		// Cloneable types. This includes all types for which a
		// protoutil.Clone call exists in the codebase as of 2016-11-21.
		{&zonepb.ZoneConfig{}, false},
		{&gossip.Info{}, false},
		{&gossip.BootstrapInfo{}, false},
		{&sqlbase.IndexDescriptor{}, false},
		{&roachpb.SplitTrigger{}, false},
		{&roachpb.Value{}, false},
		{&storagepb.ReplicaState{}, false},
		{&roachpb.RangeDescriptor{}, false},
		{&sqlbase.PartitioningDescriptor{}, false},
	}
	for _, tc := range testCases {
		var clone protoutil.Message
		var panicObj interface{}
		func() {
			defer func() {
				panicObj = recover()
			}()
			clone = protoutil.Clone(tc.pb)
		}()

		if tc.shouldPanic {
			if panicObj == nil {
				t.Errorf("%T: expected panic but didn't get one", tc.pb)
			} else {
				if panicStr := fmt.Sprint(panicObj); !strings.Contains(panicStr, "attempt to clone") {
					t.Errorf("%T: got unexpected panic %s", tc.pb, panicStr)
				}
			}
		} else {
			if panicObj != nil {
				t.Errorf("%T: got unexpected panic %v", tc.pb, panicObj)
			}
		}

		if panicObj == nil {
			realClone := proto.Clone(tc.pb)
			if !reflect.DeepEqual(clone, realClone) {
				t.Errorf("%T: clone did not equal original. expected:\n%+v\ngot:\n%+v", tc.pb, realClone, clone)
			}
		}
	}
}
