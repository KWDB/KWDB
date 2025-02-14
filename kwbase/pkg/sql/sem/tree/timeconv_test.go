// Copyright 2017 The Cockroach Authors.
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

package tree_test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	_ "gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// Test that EvalContext.GetClusterTimestamp() gets its timestamp from the
// transaction, and also that the conversion to decimal works properly.
func TestClusterTimestampConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		walltime int64
		logical  int32
		expected string
	}{
		{42, 0, "42.0000000000"},
		{-42, 0, "-42.0000000000"},
		{42, 69, "42.0000000069"},
		{42, 2147483647, "42.2147483647"},
		{9223372036854775807, 2147483647, "9223372036854775807.2147483647"},
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	senderFactory := kv.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			panic("unused")
		})
	db := kv.NewDB(
		testutils.MakeAmbientCtx(),
		senderFactory,
		clock)

	for _, d := range testData {
		ts := hlc.Timestamp{WallTime: d.walltime, Logical: d.logical}
		txnProto := roachpb.MakeTransaction(
			"test",
			nil, // baseKey
			roachpb.NormalUserPriority,
			ts,
			0, /* maxOffsetNs */
		)

		ctx := tree.EvalContext{
			Txn: kv.NewTxnFromProto(
				context.Background(),
				db,
				1, /* gatewayNodeID */
				ts,
				kv.RootTxn,
				&txnProto,
			),
		}

		dec, err := ctx.GetClusterTimestamp()
		if err != nil {
			t.Fatal(err)
		}
		final := dec.Text('f')
		if final != d.expected {
			t.Errorf("expected %s, but found %s", d.expected, final)
		}
	}
}
