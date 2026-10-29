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

package batcheval

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency/lock"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// CollectIntentRows collects the provisional key-value pairs for each intent
// provided.
//
// The method accepts a reader and flag indicating whether a prefix iterator
// should be used when creating an iterator from the reader. This flexibility
// works around a limitation of the Engine.NewReadOnly interface where prefix
// iterators and non-prefix iterators pulled from the same read-only engine are
// not guaranteed to provide a consistent snapshot of the underlying engine.
// This function expects to be able to retrieve the corresponding provisional
// value for each of the provided intents. As such, it is critical that it
// observes the engine in the same state that it was in when the intent keys
// were originally collected. Because of this, callers are tasked with
// indicating whether the intents were originally collected using a prefix
// iterator or not.
//
// TODO(nvanbenschoten): remove the usePrefixIter complexity when we're fully on
// Pebble and can guarantee that all iterators created from a read-only engine
// are consistent.
//
// TODO(nvanbenschoten): mvccGetInternal should return the intent values
// directly when reading at the READ_UNCOMMITTED consistency level. Since this
// is only currently used for range lookups and when watching for a merge (both
// of which are off the hot path), this is ok for now.
func CollectIntentRows(
	ctx context.Context, reader relational.Reader, usePrefixIter bool, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for i := range intents {
		kv, err := readProvisionalVal(ctx, reader, usePrefixIter, &intents[i])
		if err != nil {
			switch t := err.(type) {
			case *roachpb.WriteIntentError:
				log.Fatalf(ctx, "unexpected %T in CollectIntentRows: %+v", t, t)
			case *roachpb.ReadWithinUncertaintyIntervalError:
				log.Fatalf(ctx, "unexpected %T in CollectIntentRows: %+v", t, t)
			}
			return nil, err
		}
		if kv.Value.IsPresent() {
			res = append(res, kv)
		}
	}
	return res, nil
}

// readProvisionalVal retrieves the provisional value for the provided intent
// using the reader and the specified access method (i.e. with or without the
// use of a prefix iterator). The function returns an empty KeyValue if the
// intent is found to contain a deletion tombstone as its provisional value.
func readProvisionalVal(
	ctx context.Context, reader relational.Reader, usePrefixIter bool, intent *roachpb.Intent,
) (roachpb.KeyValue, error) {
	if usePrefixIter {
		val, _, err := relational.MVCCGetAsTxn(
			ctx, reader, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
		)
		if err != nil {
			return roachpb.KeyValue{}, err
		}
		if val == nil {
			// Intent is a deletion.
			return roachpb.KeyValue{}, nil
		}
		return roachpb.KeyValue{Key: intent.Key, Value: *val}, nil
	}
	res, err := relational.MVCCScanAsTxn(
		ctx, reader, intent.Key, intent.Key.Next(), intent.Txn.WriteTimestamp, intent.Txn,
	)
	if err != nil {
		return roachpb.KeyValue{}, err
	}
	if len(res.KVs) > 1 {
		log.Fatalf(ctx, "multiple key-values returned from single-key scan: %+v", res.KVs)
	} else if len(res.KVs) == 0 {
		// Intent is a deletion.
		return roachpb.KeyValue{}, nil
	}
	return res.KVs[0], nil

}

// acquireUnreplicatedLocksOnKeys adds an unreplicated lock acquisition by the
// transaction to the provided result.Result for each key in the scan result.
func acquireUnreplicatedLocksOnKeys(
	res *result.Result,
	txn *roachpb.Transaction,
	scanFmt roachpb.ScanFormat,
	scanRes *relational.MVCCScanResult,
) error {
	switch scanFmt {
	case roachpb.BATCH_RESPONSE:
		res.Local.AcquiredLocks = make([]roachpb.LockUpdate, scanRes.NumKeys)
		var i int
		return relational.MVCCScanDecodeKeyValues(scanRes.KVData, func(key relational.MVCCKey, _ []byte) error {
			res.Local.AcquiredLocks[i] = roachpb.LockUpdate{
				Span:       roachpb.Span{Key: key.Key},
				Txn:        txn.TxnMeta,
				Status:     roachpb.PENDING,
				Durability: lock.Unreplicated,
			}
			i++
			return nil
		})
	case roachpb.KEY_VALUES:
		res.Local.AcquiredLocks = make([]roachpb.LockUpdate, scanRes.NumKeys)
		for i, row := range scanRes.KVs {
			res.Local.AcquiredLocks[i] = roachpb.LockUpdate{
				Span:       roachpb.Span{Key: row.Key},
				Txn:        txn.TxnMeta,
				Status:     roachpb.PENDING,
				Durability: lock.Unreplicated,
			}
		}
		return nil
	default:
		panic("unexpected scanFormat")
	}
}
