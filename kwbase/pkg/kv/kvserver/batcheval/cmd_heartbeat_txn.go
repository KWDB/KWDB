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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.HeartbeatTxn, declareKeysHeartbeatTransaction, HeartbeatTxn)
}

func declareKeysHeartbeatTransaction(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	declareKeysWriteTransaction(desc, header, req, latchSpans)
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func HeartbeatTxn(
	ctx context.Context, readWriter relational.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.HeartbeatTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.HeartbeatTxnResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING, roachpb.STAGING); err != nil {
		return result.Result{}, err
	}

	if args.Now.IsEmpty() {
		return result.Result{}, fmt.Errorf("now not specified for heartbeat")
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	var txn roachpb.Transaction
	if ok, err := relational.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &txn, relational.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// No existing transaction record was found - create one by writing
		// it below.
		txn = *h.Txn

		// Verify that it is safe to create the transaction record.
		if err := CanCreateTxnRecord(ctx, cArgs.EvalCtx, &txn); err != nil {
			return result.Result{}, err
		}
	}

	if !txn.Status.IsFinalized() {
		// NOTE: this only updates the LastHeartbeat. It doesn't update any other
		// field from h.Txn, even if it could. Whether that's a good thing or not
		// is up for debate.
		txn.LastHeartbeat.Forward(args.Now)
		txnRecord := txn.AsRecord()
		if err := relational.MVCCPutProto(ctx, readWriter, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			return result.Result{}, err
		}
	}

	reply.Txn = &txn
	return result.Result{}, nil
}
