// Copyright 2014 The Cockroach Authors.
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

package abortspan

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func uuidFromString(input string) uuid.UUID {
	u, err := uuid.FromString(input)
	if err != nil {
		panic(err)
	}
	return u
}

var (
	testTxnID        = uuidFromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	testTxnKey       = []byte("a")
	testTxnTimestamp = hlc.Timestamp{WallTime: 123, Logical: 456}
	testTxnPriority  = enginepb.TxnPriority(123)
)

// createTestAbortSpan creates an in-memory engine and
// returns a AbortSpan using the supplied Range ID.
func createTestAbortSpan(
	t *testing.T, rangeID roachpb.RangeID, stopper *stop.Stopper,
) (*AbortSpan, relational.Engine) {
	eng := relational.NewDefaultInMem()
	stopper.AddCloser(eng)
	return New(rangeID), eng
}

// TestAbortSpanPutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestAbortSpanPutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc, e := createTestAbortSpan(t, 1, stopper)
	// Start with a get for an uncached id.
	entry := roachpb.AbortSpanEntry{}
	if aborted, readErr := sc.Get(context.Background(), e, testTxnID, &entry); aborted {
		t.Errorf("expected not aborted for id %s", testTxnID)
	} else if readErr != nil {
		t.Fatalf("unexpected read error: %s", readErr)
	}

	entry = roachpb.AbortSpanEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	if err := sc.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting response: %+v", err)
	}

	tryHit := func(expAbort bool, expEntry roachpb.AbortSpanEntry) {
		var actual roachpb.AbortSpanEntry
		if aborted, readErr := sc.Get(context.Background(), e, testTxnID, &actual); readErr != nil {
			t.Errorf("unexpected failure getting response: %s", readErr)
		} else if expAbort != aborted {
			t.Errorf("got aborted: %t; expected %t", aborted, expAbort)
		} else if !reflect.DeepEqual(expEntry, actual) {
			t.Fatalf("wanted %v, got %v", expEntry, actual)
		}
	}

	tryHit(true, entry)
	if err := sc.ClearData(e); err != nil {
		t.Error(err)
	}
	tryHit(false, roachpb.AbortSpanEntry{})
}

// TestAbortSpanEmptyParams tests operation with empty parameters.
func TestAbortSpanEmptyParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc, e := createTestAbortSpan(t, 1, stopper)

	entry := roachpb.AbortSpanEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	// Put value for test response.
	if err := sc.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting response: %+v", err)
	}
}
