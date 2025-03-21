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

package batcheval

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestRefreshRangeTimeBoundIterator is a regression test for
// https://gitee.com/kwbasedb/kwbase/issues/31823. RefreshRange
// uses a time-bound iterator, which has a bug that can cause old
// resolved intents to incorrectly appear to be pending. This test
// constructs the necessary arrangement of sstables to reproduce the
// bug and ensures that the workaround (and later, the permanent fix)
// are effective.
//
// The bug is that resolving an intent does not contribute to the
// sstable's timestamp bounds, so that if there is no other
// timestamped data expanding the bounds, time-bound iterators may
// open fewer sstables than necessary and only see the intent, not its
// resolution.
//
// This test creates two sstables. The first contains a pending intent
// at ts1 and another key at ts4, giving it timestamp bounds 1-4 (and
// putting it in scope for transactions at timestamps higher than
// ts1).
func TestRefreshRangeTimeBoundIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	k := roachpb.Key("a")
	v := roachpb.MakeValueFromString("hi")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}

	db := storage.NewDefaultInMem()
	defer db.Close()

	// Create an sstable containing an unresolved intent.
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            k,
			ID:             uuid.MakeV4(),
			Epoch:          1,
			WriteTimestamp: ts1,
		},
		ReadTimestamp: ts1,
	}
	if err := storage.MVCCPut(ctx, db, nil, k, txn.ReadTimestamp, v, txn); err != nil {
		t.Fatal(err)
	}
	if err := storage.MVCCPut(ctx, db, nil, roachpb.Key("unused1"), ts4, v, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// Create a second sstable containing the resolution of the intent
	// (committed). The sstable also has a second write at a different (older)
	// timestamp, because if it were empty other than the deletion tombstone, it
	// would not have any timestamp bounds and would be selected for every read.
	intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: k})
	intent.Status = roachpb.COMMITTED
	if _, err := storage.MVCCResolveWriteIntent(ctx, db, nil, intent); err != nil {
		t.Fatal(err)
	}
	if err := storage.MVCCPut(ctx, db, nil, roachpb.Key("unused2"), ts1, v, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// TODO(peter): Make this work for Pebble as well.
	if rocksDB, ok := db.(*storage.RocksDB); ok {
		// Double-check that we've created the SSTs we intended to.
		userProps, err := rocksDB.GetUserProperties()
		if err != nil {
			t.Fatal(err)
		}
		require.Len(t, userProps.Sst, 2)
		require.Equal(t, userProps.Sst[0].TsMin, &ts1)
		require.Equal(t, userProps.Sst[0].TsMax, &ts4)
		require.Equal(t, userProps.Sst[1].TsMin, &ts1)
		require.Equal(t, userProps.Sst[1].TsMax, &ts1)
	}

	// We should now have a committed value at k@ts1. Read it back to make sure.
	// This represents real-world use of time-bound iterators where callers must
	// have previously performed a consistent read at the lower time-bound to
	// prove that there are no intents present that would be missed by the time-
	// bound iterator.
	if val, intent, err := storage.MVCCGet(ctx, db, k, ts1, storage.MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if intent != nil {
		t.Fatalf("got unexpected intent: %v", intent)
	} else if !val.EqualData(v) {
		t.Fatalf("expected %v, got %v", v, val)
	}

	// Now the real test: a transaction at ts2 has been pushed to ts3
	// and must refresh. It overlaps with our committed intent on k@ts1,
	// which is fine because our timestamp is higher (but if that intent
	// were still pending, the new txn would be blocked). Prior to
	// https://gitee.com/kwbasedb/kwbase/pull/32211, a bug in the
	// time-bound iterator meant that we would see the first sstable but
	// not the second and incorrectly report the intent as pending,
	// resulting in an error from RefreshRange.
	var resp roachpb.RefreshRangeResponse
	_, err := RefreshRange(ctx, db, CommandArgs{
		Args: &roachpb.RefreshRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    k,
				EndKey: keys.MaxKey,
			},
			RefreshFrom: ts2,
		},
		Header: roachpb.Header{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: ts3,
				},
				ReadTimestamp: ts2,
			},
			Timestamp: ts3,
		},
	}, &resp)
	if err != nil {
		t.Fatal(err)
	}
}
