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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency/lock"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Scan, DefaultDeclareIsolatedKeys, Scan)
}

func getTableDesc(v roachpb.Value) (*sqlbase.Descriptor, error) {
	desc := sqlbase.Descriptor{}
	err := v.GetProto(&desc)
	return &desc, err
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)
	if h.Txn != nil && h.Txn.UnConsistency {
		h.ReadConsistency = roachpb.READ_UNCOMMITTED
	}
	var res result.Result
	var scanRes storage.MVCCScanResult
	var err error

	opts := storage.MVCCScanOptions{
		Inconsistent:     h.ReadConsistency != roachpb.CONSISTENT,
		Txn:              h.Txn,
		MaxKeys:          h.MaxSpanRequestKeys,
		TargetBytes:      h.TargetBytes,
		FailOnMoreRecent: args.KeyLocking != lock.None,
		Reverse:          false,
	}
	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		scanRes, err = storage.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = scanRes.KVData
	case roachpb.KEY_VALUES:
		if args.IsScanAllMvccVerForOneTable {
			if err := reader.Iterate(args.Key, args.EndKey, func(kv storage.MVCCKeyValue) (bool, error) {
				if kv.Key.Timestamp.Equal(hlc.Timestamp{}) {
					log.Infof(ctx, "Find write intent.")
					return false, nil
				}
				v := roachpb.Value{RawBytes: kv.Value}
				if len(v.RawBytes) != 0 {
					scanRes.KVs = append(scanRes.KVs, roachpb.KeyValue{Key: kv.Key.Key, Value: roachpb.Value{RawBytes: kv.Value, Timestamp: kv.Key.Timestamp}})
					return false, nil
				}
				return true, nil
			}); err != nil {
				return result.Result{}, err
			}
			reply.Rows = scanRes.KVs
		} else {
			scanRes, err = storage.MVCCScan(
				ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
			if err != nil {
				return result.Result{}, err
			}
			reply.Rows = scanRes.KVs
		}
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	reply.NumKeys = scanRes.NumKeys
	reply.NumBytes = scanRes.NumBytes

	if scanRes.ResumeSpan != nil {
		reply.ResumeSpan = scanRes.ResumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		// NOTE: MVCCScan doesn't use a Prefix iterator, so we don't want to use
		// one in CollectIntentRows either so that we're guaranteed to use the
		// same cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = false
		reply.IntentRows, err = CollectIntentRows(ctx, reader, usePrefixIter, scanRes.Intents)
		if err != nil {
			return result.Result{}, err
		}
	}

	if args.KeyLocking != lock.None && h.Txn != nil {
		err = acquireUnreplicatedLocksOnKeys(&res, h.Txn, args.ScanFormat, &scanRes)
		if err != nil {
			return result.Result{}, err
		}
	}
	res.Local.EncounteredIntents = scanRes.Intents
	return res, nil
}
