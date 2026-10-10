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

package kvserver

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestSpanSetBatchBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := relational.NewDefaultInMem()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")})
	outsideKey := relational.MakeMVCCMetadataKey(roachpb.Key("a"))
	outsideKey2 := relational.MakeMVCCMetadataKey(roachpb.Key("b"))
	outsideKey3 := relational.MakeMVCCMetadataKey(roachpb.Key("g"))
	outsideKey4 := relational.MakeMVCCMetadataKey(roachpb.Key("m"))
	insideKey := relational.MakeMVCCMetadataKey(roachpb.Key("c"))
	insideKey2 := relational.MakeMVCCMetadataKey(roachpb.Key("d"))
	insideKey3 := relational.MakeMVCCMetadataKey(roachpb.Key("f"))

	// Write values outside the range that we can try to read later.
	if err := eng.Put(outsideKey, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}
	if err := eng.Put(outsideKey3, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()

	// Writes inside the range work. Write twice for later read testing.
	if err := batch.Put(insideKey, []byte("value")); err != nil {
		t.Fatalf("failed to write inside the range: %+v", err)
	}
	if err := batch.Put(insideKey2, []byte("value2")); err != nil {
		t.Fatalf("failed to write inside the range: %+v", err)
	}

	// Writes outside the range fail. We try to cover all write methods
	// in the failure case to make sure the CheckAllowed call is
	// present, but we don't attempt successful versions of all
	// methods since those are harder to set up.
	isWriteSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot write undeclared span")
	}

	t.Run("writes before range", func(t *testing.T) {
		if err := batch.Clear(outsideKey); !isWriteSpanErr(err) {
			t.Errorf("Clear: unexpected error %v", err)
		}
		if err := batch.ClearRange(outsideKey, outsideKey2); !isWriteSpanErr(err) {
			t.Errorf("ClearRange: unexpected error %v", err)
		}
		{
			iter := batch.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
			err := batch.ClearIterRange(iter, outsideKey.Key, outsideKey2.Key)
			iter.Close()
			if !isWriteSpanErr(err) {
				t.Errorf("ClearIterRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(outsideKey, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.Put(outsideKey, nil); !isWriteSpanErr(err) {
			t.Errorf("Put: unexpected error %v", err)
		}
	})

	t.Run("writes after range", func(t *testing.T) {
		if err := batch.Clear(outsideKey3); !isWriteSpanErr(err) {
			t.Errorf("Clear: unexpected error %v", err)
		}
		if err := batch.ClearRange(insideKey2, outsideKey4); !isWriteSpanErr(err) {
			t.Errorf("ClearRange: unexpected error %v", err)
		}
		{
			iter := batch.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
			err := batch.ClearIterRange(iter, insideKey2.Key, outsideKey4.Key)
			iter.Close()
			if !isWriteSpanErr(err) {
				t.Errorf("ClearIterRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(outsideKey3, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.Put(outsideKey3, nil); !isWriteSpanErr(err) {
			t.Errorf("Put: unexpected error %v", err)
		}
	})

	t.Run("reads inside range", func(t *testing.T) {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if value, err := batch.Get(insideKey); err != nil {
			t.Errorf("failed to read inside the range: %+v", err)
		} else if !bytes.Equal(value, []byte("value")) {
			t.Errorf("failed to read previously written value, got %q", value)
		}
		//lint:ignore SA1019 historical usage of deprecated batch.GetProto is OK
		if _, _, _, err := batch.GetProto(insideKey, nil); err != nil {
			t.Errorf("GetProto: unexpected error %v", err)
		}
		if err := batch.Iterate(insideKey.Key, insideKey2.Key,
			func(v relational.MVCCKeyValue) (bool, error) {
				return false, nil
			},
		); err != nil {
			t.Errorf("Iterate: unexpected error %v", err)
		}
	})

	// Reads outside the range fail.
	isReadSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot read undeclared span")
	}

	t.Run("reads before range", func(t *testing.T) {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if _, err := batch.Get(outsideKey); !isReadSpanErr(err) {
			t.Errorf("Get: unexpected error %v", err)
		}
		//lint:ignore SA1019 historical usage of deprecated batch.GetProto is OK
		if _, _, _, err := batch.GetProto(outsideKey, nil); !isReadSpanErr(err) {
			t.Errorf("GetProto: unexpected error %v", err)
		}
		if err := batch.Iterate(outsideKey.Key, insideKey2.Key,
			func(v relational.MVCCKeyValue) (bool, error) {
				return false, errors.Errorf("unexpected callback: %v", v)
			},
		); !isReadSpanErr(err) {
			t.Errorf("Iterate: unexpected error %v", err)
		}
	})

	t.Run("reads after range", func(t *testing.T) {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if _, err := batch.Get(outsideKey3); !isReadSpanErr(err) {
			t.Errorf("Get: unexpected error %v", err)
		}
		//lint:ignore SA1019 historical usage of deprecated batch.GetProto is OK
		if _, _, _, err := batch.GetProto(outsideKey3, nil); !isReadSpanErr(err) {
			t.Errorf("GetProto: unexpected error %v", err)
		}
		if err := batch.Iterate(insideKey2.Key, outsideKey4.Key,
			func(v relational.MVCCKeyValue) (bool, error) {
				return false, errors.Errorf("unexpected callback: %v", v)
			},
		); !isReadSpanErr(err) {
			t.Errorf("Iterate: unexpected error %v", err)
		}
	})

	t.Run("forward scans", func(t *testing.T) {
		iter := batch.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()

		// Iterators check boundaries on seek and next/prev
		iter.SeekGE(outsideKey)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("Seek: unexpected error %v", err)
		}
		iter.SeekGE(outsideKey3)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("Seek: unexpected error %v", err)
		}
		// Seeking back in bounds restores validity.
		iter.SeekGE(insideKey)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey) {
			t.Fatalf("expected key %s, got %s", insideKey, iter.Key())
		}
		iter.Next()
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey2) {
			t.Fatalf("expected key %s, got %s", insideKey2, iter.Key())
		}
		// Scan out of bounds.
		iter.Next()
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		} else if err != nil {
			// Scanning out of bounds sets Valid() to false but is not an error.
			t.Errorf("unexpected error on iterator: %+v", err)
		}
	})

	// Same test in reverse. We commit the batch and wrap an iterator on
	// the raw engine because we don't support bidirectional iteration
	// over a pending batch.
	if err := batch.Commit(true, relational.NormalCommitType); err != nil {
		t.Fatal(err)
	}

	t.Run("reverse scans", func(t *testing.T) {
		iter := spanset.NewIterator(eng.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax}), &ss)
		defer iter.Close()
		iter.SeekLT(outsideKey)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("SeekLT: unexpected error %v", err)
		}
		// Seeking back in bounds restores validity.
		iter.SeekLT(insideKey3)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey2) {
			t.Fatalf("expected key %s, got %s", insideKey2, iter.Key())
		}
		iter.Prev()
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), insideKey) {
			t.Fatalf("expected key %s, got %s", insideKey, iter.Key())
		}
		// Scan out of bounds.
		iter.Prev()
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		} else if err != nil {
			t.Errorf("unexpected error on iterator: %+v", err)
		}

		// Seeking back in bounds restores validity.
		iter.SeekLT(insideKey2)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		// SeekLT to the lower bound is invalid.
		iter.SeekLT(insideKey)
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		} else if !isReadSpanErr(err) {
			t.Fatalf("SeekLT: unexpected error %v", err)
		}
		// SeekLT to upper bound restores validity.
		iter.SeekLT(outsideKey3)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
	})
}

func TestSpanSetBatchTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := relational.NewDefaultInMem()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddMVCC(spanset.SpanReadOnly,
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, hlc.Timestamp{WallTime: 2})
	ss.AddMVCC(spanset.SpanReadWrite,
		roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("f")}, hlc.Timestamp{WallTime: 2})

	rkey := relational.MakeMVCCMetadataKey(roachpb.Key("b"))
	wkey := relational.MakeMVCCMetadataKey(roachpb.Key("e"))

	value := []byte("value")

	// Write value that we can try to read later.
	if err := eng.Put(rkey, value); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 0})
	defer batchNonMVCC.Close()

	batchBefore := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchBefore.Close()

	batchDuring := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 2})
	defer batchDuring.Close()

	batchAfter := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 3})
	defer batchAfter.Close()

	// Writes.
	for _, batch := range []relational.Batch{batchAfter, batchDuring} {
		if err := batch.Put(wkey, value); err != nil {
			t.Fatalf("failed to write inside the range at same or greater ts than latch declaration: %+v", err)
		}
	}

	for _, batch := range []relational.Batch{batchBefore, batchNonMVCC} {
		if err := batch.Put(wkey, value); err == nil {
			t.Fatalf("was able to write inside the range at ts less than latch declaration: %+v", err)
		}
	}

	// We try to cover all write methods in the failure case to make sure
	// the CheckAllowedAt call is present, but we don't attempt to successful
	// versions of all methods since those are harder to set up.
	isWriteSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot write undeclared span")
	}

	for _, batch := range []relational.Batch{batchBefore, batchNonMVCC} {
		if err := batch.Clear(wkey); !isWriteSpanErr(err) {
			t.Errorf("Clear: unexpected error %v", err)
		}
		{
			iter := batch.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
			err := batch.ClearIterRange(iter, wkey.Key, wkey.Key)
			iter.Close()
			if !isWriteSpanErr(err) {
				t.Errorf("ClearIterRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(wkey, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.Put(wkey, nil); !isWriteSpanErr(err) {
			t.Errorf("Put: unexpected error %v", err)
		}
	}

	// Reads.
	for _, batch := range []relational.Batch{batchBefore, batchDuring} {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if res, err := batch.Get(rkey); err != nil {
			t.Errorf("failed to read inside the range: %+v", err)
		} else if !bytes.Equal(res, value) {
			t.Errorf("failed to read previously written value, got %q", res)
		}
	}

	isReadSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot read undeclared span")
	}

	for _, batch := range []relational.Batch{batchAfter, batchNonMVCC} {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if _, err := batch.Get(rkey); !isReadSpanErr(err) {
			t.Errorf("Get: unexpected error %v", err)
		}

		//lint:ignore SA1019 historical usage of deprecated batch.GetProto is OK
		if _, _, _, err := batch.GetProto(rkey, nil); !isReadSpanErr(err) {
			t.Errorf("GetProto: unexpected error %v", err)
		}
		if err := batch.Iterate(rkey.Key, rkey.Key,
			func(v relational.MVCCKeyValue) (bool, error) {
				return false, errors.Errorf("unexpected callback: %v", v)
			},
		); !isReadSpanErr(err) {
			t.Errorf("Iterate: unexpected error %v", err)
		}
	}
}

func TestSpanSetIteratorTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := relational.NewDefaultInMem()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, hlc.Timestamp{WallTime: 1})
	ss.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, hlc.Timestamp{WallTime: 2})

	k1, v1 := relational.MakeMVCCMetadataKey(roachpb.Key("b")), []byte("b-value")
	k2, v2 := relational.MakeMVCCMetadataKey(roachpb.Key("d")), []byte("d-value")

	// Write values that we can try to read later.
	if err := eng.Put(k1, v1); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}
	if err := eng.Put(k2, v2); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 0})
	defer batchNonMVCC.Close()

	batchAt1 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchAt1.Close()

	batchAt2 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 2})
	defer batchAt2.Close()

	batchAt3 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 3})
	defer batchAt3.Close()

	func() {
		// When accessing at t=1, we're able to read through latches declared at t=1 and t=2.
		iter := batchAt1.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), k1) {
			t.Fatalf("expected key %s, got %s", k1, iter.Key())
		}

		iter.Next()
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), k2) {
			t.Fatalf("expected key %s, got %s", k2, iter.Key())
		}
	}()

	{
		// When accessing at t=2, we're only able to read through the latch declared at t=2.
		iter := batchAt2.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		}

		iter.SeekGE(k2)
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.Key(), k2) {
			t.Fatalf("expected key %s, got %s", k2, iter.Key())
		}
	}

	for _, batch := range []relational.Batch{batchAt3, batchNonMVCC} {
		// When accessing at t=3, we're unable to read through any of the declared latches.
		// Same is true when accessing without a timestamp.
		iter := batch.NewIterator(relational.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		}

		iter.SeekGE(k2)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.Key())
		}
	}
}

func TestSpanSetNonMVCCBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := relational.NewDefaultInMem()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")})
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("f")})

	rkey := relational.MakeMVCCMetadataKey(roachpb.Key("b"))
	wkey := relational.MakeMVCCMetadataKey(roachpb.Key("e"))

	value := []byte("value")

	// Write value that we can try to read later.
	if err := eng.Put(rkey, value); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batchNonMVCC.Close()

	batchMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchMVCC.Close()

	// Writes.
	for _, batch := range []relational.Batch{batchNonMVCC, batchMVCC} {
		if err := batch.Put(wkey, value); err != nil {
			t.Fatalf("write disallowed through non-MVCC latch: %+v", err)
		}
	}

	// Reads.
	for _, batch := range []relational.Batch{batchNonMVCC, batchMVCC} {
		//lint:ignore SA1019 historical usage of deprecated batch.Get is OK
		if res, err := batch.Get(rkey); err != nil {
			t.Errorf("read disallowed through non-MVCC latch: %+v", err)
		} else if !bytes.Equal(res, value) {
			t.Errorf("failed to read previously written value, got %q", res)
		}
	}
}

// TestSpanSetMVCCResolveWriteIntentRangeUsingIter verifies that
// MVCCResolveWriteIntentRangeUsingIter does not stray outside of the passed-in
// key range (which it only used to do in this corner case tested here).
//
// See #20894.
func TestSpanSetMVCCResolveWriteIntentRangeUsingIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	eng := relational.NewDefaultInMem()
	defer eng.Close()

	ctx := context.Background()

	value := roachpb.MakeValueFromString("irrelevant")

	if err := relational.MVCCPut(
		ctx,
		eng,
		nil, // ms
		roachpb.Key("b"),
		hlc.Timestamp{WallTime: 10}, // irrelevant
		value,
		nil, // txn
	); err != nil {
		t.Fatal(err)
	}

	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")})

	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()

	intent := roachpb.LockUpdate{
		Span:   roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")},
		Txn:    enginepb.TxnMeta{}, // unused
		Status: roachpb.PENDING,
	}

	iterAndBuf := relational.GetIterAndBuf(batch, relational.IterOptions{UpperBound: intent.Span.EndKey})
	defer iterAndBuf.Cleanup()

	if _, _, err := relational.MVCCResolveWriteIntentRangeUsingIter(
		ctx, batch, iterAndBuf, nil /* ms */, intent, 0,
	); err != nil {
		t.Fatal(err)
	}
}
