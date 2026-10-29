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

package row

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
)

// KVFetcher wraps kvBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	kvBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
	bytesRead     int64
	Span          roachpb.Span
	newSpan       bool
}

// NewKVFetcher creates a new KVFetcher.
func NewKVFetcher(
	txn *kv.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStr sqlbase.ScanLockingStrength,
	returnRangeInfo bool,
) (*KVFetcher, error) {
	kvBatchFetcher, err := makeKVBatchFetcher(
		txn, spans, reverse, useBatchLimit, firstBatchLimit, lockStr, returnRangeInfo,
	)
	return newKVFetcher(&kvBatchFetcher), err
}

func newKVFetcher(batchFetcher kvBatchFetcher) *KVFetcher {
	return &KVFetcher{
		kvBatchFetcher: batchFetcher,
	}
}

// NextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, and any errors that may have
// occurred.
func (f *KVFetcher) NextKV(
	ctx context.Context,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	for {
		newSpan = f.newSpan
		f.newSpan = false
		if len(f.kvs) != 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			return true, kv, newSpan, nil
		}
		if len(f.batchResponse) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			if err != nil {
				return false, kv, false, err
			}
			return true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes: rawBytes,
				},
			}, newSpan, nil
		}

		ok, f.kvs, f.batchResponse, f.Span, err = f.nextBatch(ctx)
		if err != nil {
			return ok, kv, false, err
		}
		if !ok {
			return false, kv, false, nil
		}
		f.newSpan = true
		f.bytesRead += int64(len(f.batchResponse))
	}
}
