// Copyright 2015 The Cockroach Authors.
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

package rditer

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func fakePrevKey(k []byte) roachpb.Key {
	const maxLen = 100
	length := len(k)

	// When the byte array is empty.
	if length == 0 {
		panic("cannot get the prev key of an empty key")
	}
	if length > maxLen {
		panic(fmt.Sprintf("test does not support key longer than %d characters: %q", maxLen, k))
	}

	// If the last byte is a 0, then drop it.
	if k[length-1] == 0 {
		return k[0 : length-1]
	}

	// If the last byte isn't 0, subtract one from it and append "\xff"s
	// until the end of the key space.
	return bytes.Join([][]byte{
		k[0 : length-1],
		{k[length-1] - 1},
		bytes.Repeat([]byte{0xff}, maxLen-length),
	}, nil)
}

func uuidFromString(input string) uuid.UUID {
	u, err := uuid.FromString(input)
	if err != nil {
		panic(err)
	}
	return u
}

// createRangeData creates sample range data in all possible areas of
// the key space. Returns a slice of the encoded keys of all created
// data.
func createRangeData(
	t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor,
) []storage.MVCCKey {
	testTxnID := uuidFromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	testTxnID2 := uuidFromString("9855a1ef-8eb9-4c06-a106-cab1dda78a2b")

	ts0 := hlc.Timestamp{}
	ts := hlc.Timestamp{WallTime: 1}
	keyTSs := []struct {
		key roachpb.Key
		ts  hlc.Timestamp
	}{
		{keys.AbortSpanKey(desc.RangeID, testTxnID), ts0},
		{keys.AbortSpanKey(desc.RangeID, testTxnID2), ts0},
		{keys.RangeLastGCKey(desc.RangeID), ts0},
		{keys.RangeAppliedStateKey(desc.RangeID), ts0},
		{keys.RaftAppliedIndexLegacyKey(desc.RangeID), ts0},
		{keys.RaftTruncatedStateLegacyKey(desc.RangeID), ts0},
		{keys.RangeLeaseKey(desc.RangeID), ts0},
		{keys.LeaseAppliedIndexLegacyKey(desc.RangeID), ts0},
		{keys.RangeStatsLegacyKey(desc.RangeID), ts0},
		{keys.RangeTombstoneKey(desc.RangeID), ts0},
		{keys.RaftHardStateKey(desc.RangeID), ts0},
		{keys.RaftLogKey(desc.RangeID, 1), ts0},
		{keys.RaftLogKey(desc.RangeID, 2), ts0},
		{keys.RangeLastReplicaGCTimestampKey(desc.RangeID), ts0},
		{keys.RangeDescriptorKey(desc.StartKey), ts},
		{keys.TransactionKey(roachpb.Key(desc.StartKey), uuid.MakeV4()), ts0},
		{keys.TransactionKey(roachpb.Key(desc.StartKey.Next()), uuid.MakeV4()), ts0},
		{keys.TransactionKey(fakePrevKey(desc.EndKey), uuid.MakeV4()), ts0},
		// TODO(bdarnell): KeyMin.Next() results in a key in the reserved system-local space.
		// Once we have resolved https://gitee.com/kwbasedb/kwbase/issues/437,
		// replace this with something that reliably generates the first valid key in the range.
		//{r.Desc().StartKey.Next(), ts},
		// The following line is similar to StartKey.Next() but adds more to the key to
		// avoid falling into the system-local space.
		{append(append([]byte{}, desc.StartKey...), '\x02'), ts},
		{fakePrevKey(desc.EndKey), ts},
	}

	keys := []storage.MVCCKey{}
	for _, keyTS := range keyTSs {
		if err := storage.MVCCPut(context.Background(), eng, nil, keyTS.key, keyTS.ts, roachpb.MakeValueFromString("value"), nil); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, storage.MVCCKey{Key: keyTS.key, Timestamp: keyTS.ts})
	}
	return keys
}

func verifyRDIter(
	t *testing.T,
	desc *roachpb.RangeDescriptor,
	readWriter storage.ReadWriter,
	replicatedOnly bool,
	expectedKeys []storage.MVCCKey,
) {
	t.Helper()
	verify := func(t *testing.T, useSpanSet, reverse bool) {
		if useSpanSet {
			var spans spanset.SpanSet
			spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    keys.MakeRangeIDPrefix(desc.RangeID),
				EndKey: keys.MakeRangeIDPrefix(desc.RangeID).PrefixEnd(),
			})
			spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
				EndKey: keys.MakeRangeKeyPrefix(desc.EndKey),
			})
			spans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    desc.StartKey.AsRawKey(),
				EndKey: desc.EndKey.AsRawKey(),
			}, hlc.Timestamp{WallTime: 42})
			readWriter = spanset.NewReadWriterAt(readWriter, &spans, hlc.Timestamp{WallTime: 42})
		}
		iter := NewReplicaDataIterator(desc, readWriter, replicatedOnly, reverse /* seekEnd */)
		defer iter.Close()
		i := 0
		if reverse {
			i = len(expectedKeys) - 1
		}
		for {
			if ok, err := iter.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}
			if !reverse && i >= len(expectedKeys) {
				t.Fatal("there are more keys in the iteration than expected")
			}
			if reverse && i < 0 {
				t.Fatal("there are more keys in the iteration than expected")
			}
			if key := iter.Key(); !key.Equal(expectedKeys[i]) {
				k1, ts1 := key.Key, key.Timestamp
				k2, ts2 := expectedKeys[i].Key, expectedKeys[i].Timestamp
				t.Errorf("%d: expected %q(%d); got %q(%d)", i, k2, ts2, k1, ts1)
			}
			if reverse {
				i--
				iter.Prev()
			} else {
				i++
				iter.Next()
			}
		}
		if (reverse && i >= 0) || (!reverse && i != len(expectedKeys)) {
			t.Fatal("there are fewer keys in the iteration than expected")
		}
	}
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		testutils.RunTrueAndFalse(t, "spanset", func(t *testing.T, useSpanSet bool) {
			verify(t, useSpanSet, reverse)
		})
	})
}

// TestReplicaDataIterator verifies correct operation of iterator if
// a range contains no data and never has.
func TestReplicaDataIteratorEmptyRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMem()
	defer eng.Close()

	desc := &roachpb.RangeDescriptor{
		RangeID:  12345,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	verifyRDIter(t, desc, eng, false /* replicatedOnly */, []storage.MVCCKey{})
}

// TestReplicaDataIterator creates three ranges {"a"-"b" (pre), "b"-"c"
// (main test range), "c"-"d" (post)} and fills each with data. It
// first verifies the contents of the "b"-"c" range. Next, it makes sure
// a replicated-only iterator does not show any unreplicated keys from
// the range. Then, it deletes the range and verifies it's empty. Finally,
// it verifies the pre and post ranges still contain the expected data.
func TestReplicaDataIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMem()
	defer eng.Close()

	descPre := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("b"),
	}
	desc := roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
	}
	descPost := roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKeyMax,
	}

	// Create range data for all three ranges.
	preKeys := createRangeData(t, eng, descPre)
	curKeys := createRangeData(t, eng, desc)
	postKeys := createRangeData(t, eng, descPost)

	// Verify the contents of the "b"-"c" range.
	t.Run("cur", func(t *testing.T) {
		verifyRDIter(t, &desc, eng, false /* replicatedOnly */, curKeys)
	})

	// Verify that the replicated-only iterator ignores unreplicated keys.
	unreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(desc.RangeID)
	iter := NewReplicaDataIterator(&desc, eng,
		true /* replicatedOnly */, false /* seekEnd */)
	defer iter.Close()
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		if bytes.HasPrefix(iter.Key().Key, unreplicatedPrefix) {
			t.Fatalf("unexpected unreplicated key: %s", iter.Key().Key)
		}
	}

	// Verify the keys in pre & post ranges.
	for _, test := range []struct {
		name string
		desc *roachpb.RangeDescriptor
		keys []storage.MVCCKey
	}{
		{"pre", &descPre, preKeys},
		{"post", &descPost, postKeys},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifyRDIter(t, test.desc, eng, false /* replicatedOnly */, test.keys)
		})
	}
}
