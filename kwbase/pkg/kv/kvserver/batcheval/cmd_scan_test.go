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

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestScanReverseScanTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Sanity checks for the TargetBytes scan option. We're not checking the specifics here, just
	// that the plumbing works. TargetBytes is tested in-depth via TestMVCCHistories.

	const (
		tbNone = 0      // no limit, i.e. should return all kv pairs
		tbOne  = 1      // one byte = return first key only
		tbLots = 100000 // de facto ditto tbNone
	)
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		for _, tb := range []int64{tbNone, tbOne, tbLots} {
			t.Run(fmt.Sprintf("targetBytes=%d", tb), func(t *testing.T) {
				for _, sf := range []roachpb.ScanFormat{roachpb.KEY_VALUES, roachpb.BATCH_RESPONSE} {
					t.Run(fmt.Sprintf("format=%s", sf), func(t *testing.T) {
						testScanReverseScanInner(t, tb, sf, reverse, tb != tbOne)
					})
				}
			})
		}
	})
}

func testScanReverseScanInner(
	t *testing.T, tb int64, sf roachpb.ScanFormat, reverse bool, expBoth bool,
) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	k1, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts := hlc.Timestamp{WallTime: 1}

	eng := relational.NewDefaultInMem()
	defer eng.Close()

	// Write to k1 and k2.
	for _, k := range []roachpb.Key{k1, k2} {
		err := relational.MVCCPut(ctx, eng, nil, k, ts, roachpb.MakeValueFromString("value-"+string(k)), nil)
		require.NoError(t, err)
	}

	var req roachpb.Request
	var resp roachpb.Response
	if !reverse {
		req = &roachpb.ScanRequest{ScanFormat: sf}
		resp = &roachpb.ScanResponse{}
	} else {
		req = &roachpb.ReverseScanRequest{ScanFormat: sf}
		resp = &roachpb.ReverseScanResponse{}
	}
	req.SetHeader(roachpb.RequestHeader{Key: k1, EndKey: roachpb.KeyMax})

	cArgs := CommandArgs{
		Args: req,
		Header: roachpb.Header{
			Timestamp:   ts,
			TargetBytes: tb,
		},
	}

	if !reverse {
		_, err := Scan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	} else {
		_, err := ReverseScan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	}
	expN := 1
	if expBoth {
		expN = 2
	}

	require.EqualValues(t, expN, resp.Header().NumKeys)
	require.NotZero(t, resp.Header().NumBytes)

	var rows []roachpb.KeyValue
	if !reverse {
		rows = resp.(*roachpb.ScanResponse).Rows
	} else {
		rows = resp.(*roachpb.ReverseScanResponse).Rows
	}

	if rows != nil {
		require.Len(t, rows, expN)
	}
}
