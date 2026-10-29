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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// instrumentedEngine wraps a relational.Engine and allows for various methods in
// the interface to be instrumented for testing purposes.
type instrumentedEngine struct {
	relational.Engine

	onNewIterator func(relational.IterOptions)
	// ... can be extended ...
}

func (ie *instrumentedEngine) NewIterator(opts relational.IterOptions) relational.Iterator {
	if ie.onNewIterator != nil {
		ie.onNewIterator(opts)
	}
	return ie.Engine.NewIterator(opts)
}

// TestCollectIntentsUsesSameIterator tests that all uses of CollectIntents
// (currently only by READ_UNCOMMITTED Gets, Scans, and ReverseScans) use the
// same cached iterator (prefix or non-prefix) for their initial read and their
// provisional value collection for any intents they find.
func TestCollectIntentsUsesSameIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	key := roachpb.Key("key")
	ts := hlc.Timestamp{WallTime: 123}
	header := roachpb.Header{
		Timestamp:       ts,
		ReadConsistency: roachpb.READ_UNCOMMITTED,
	}

	testCases := []struct {
		name              string
		run               func(*testing.T, relational.ReadWriter) (intents []roachpb.KeyValue, _ error)
		expPrefixIters    int
		expNonPrefixIters int
	}{
		{
			name: "get",
			run: func(t *testing.T, db relational.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: key},
				}
				var resp roachpb.GetResponse
				if _, err := Get(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				if resp.IntentValue == nil {
					return nil, nil
				}
				return []roachpb.KeyValue{{Key: key, Value: *resp.IntentValue}}, nil
			},
			expPrefixIters:    2,
			expNonPrefixIters: 0,
		},
		{
			name: "scan",
			run: func(t *testing.T, db relational.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &roachpb.ScanRequest{
					RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp roachpb.ScanResponse
				if _, err := Scan(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				return resp.IntentRows, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
		{
			name: "reverse scan",
			run: func(t *testing.T, db relational.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &roachpb.ReverseScanRequest{
					RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp roachpb.ReverseScanResponse
				if _, err := ReverseScan(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				return resp.IntentRows, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Test with and without deletion intents. If a READ_UNCOMMITTED request
			// encounters an intent whose provisional value is a deletion tombstone,
			// the request should ignore the intent and should not return any
			// corresponding intent row.
			testutils.RunTrueAndFalse(t, "deletion intent", func(t *testing.T, delete bool) {
				db := &instrumentedEngine{Engine: relational.NewDefaultInMem()}
				defer db.Close()

				// Write an intent.
				val := roachpb.MakeValueFromBytes([]byte("val"))
				txn := roachpb.MakeTransaction("test", key, roachpb.NormalUserPriority, ts, 0)
				var err error
				if delete {
					err = relational.MVCCDelete(ctx, db, nil, key, ts, &txn)
				} else {
					err = relational.MVCCPut(ctx, db, nil, key, ts, val, &txn)
				}
				require.NoError(t, err)

				// Instrument iterator creation, count prefix vs. non-prefix iters.
				var prefixIters, nonPrefixIters int
				db.onNewIterator = func(opts relational.IterOptions) {
					if opts.Prefix {
						prefixIters++
					} else {
						nonPrefixIters++
					}
				}

				intents, err := c.run(t, db)
				require.NoError(t, err)

				// Assert proper intent values.
				if delete {
					require.Len(t, intents, 0)
				} else {
					expIntentVal := val
					expIntentVal.Timestamp = ts
					expIntentKeyVal := roachpb.KeyValue{Key: key, Value: expIntentVal}
					require.Len(t, intents, 1)
					require.Equal(t, expIntentKeyVal, intents[0])
				}

				// Assert proper iterator use.
				require.Equal(t, c.expPrefixIters, prefixIters)
				require.Equal(t, c.expNonPrefixIters, nonPrefixIters)
				require.Equal(t, c.expNonPrefixIters, nonPrefixIters)
			})
		})
	}
}
