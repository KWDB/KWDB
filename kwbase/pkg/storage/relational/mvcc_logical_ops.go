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

package relational

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// MVCCLogicalOpType is an enum with values corresponding to each of the
// enginepb.MVCCLogicalOp variants.
//
// LogLogicalOp takes an MVCCLogicalOpType and a corresponding
// MVCCLogicalOpDetails instead of an enginepb.MVCCLogicalOp variant for two
// reasons. First, it serves as a form of abstraction so that callers of the
// method don't need to construct protos themselves. More importantly, it also
// avoids allocations in the common case where Writer.LogLogicalOp is a no-op.
// This makes LogLogicalOp essentially free for cases where logical op logging
// is disabled.
type MVCCLogicalOpType int

const (
	// MVCCWriteValueOpType corresponds to the MVCCWriteValueOp variant.
	MVCCWriteValueOpType MVCCLogicalOpType = iota
	// MVCCWriteIntentOpType corresponds to the MVCCWriteIntentOp variant.
	MVCCWriteIntentOpType
	// MVCCUpdateIntentOpType corresponds to the MVCCUpdateIntentOp variant.
	MVCCUpdateIntentOpType
	// MVCCCommitIntentOpType corresponds to the MVCCCommitIntentOp variant.
	MVCCCommitIntentOpType
	// MVCCAbortIntentOpType corresponds to the MVCCAbortIntentOp variant.
	MVCCAbortIntentOpType
	// MVCCCommitTxnOpType corresponds to the MVCCCommitTxnOp variant.
	MVCCCommitTxnOpType
	// MVCCAbortOpType corresponds to the MVCCAbortOp variant.
	MVCCAbortOpType
	// MVCCRangeSplitIDsType corresponds to the MVCCRangeSplitIDs variant.
	MVCCRangeSplitIDsType
	// MVCCRangeMergeIDsType corresponds to the MVCCRangeMergeIDs variant.
	MVCCRangeMergeIDsType
)

// MVCCLogicalOpDetails contains details about the occurrence of an MVCC logical
// operation.
type MVCCLogicalOpDetails struct {
	Txn       enginepb.TxnMeta
	Key       roachpb.Key
	Value     []byte
	Timestamp hlc.Timestamp

	// Safe indicates that the values in this struct will never be invalidated
	// at a later point. If the details object cannot promise that its values
	// will never be invalidated, an OpLoggerBatch will make a copy of all
	// references before adding it to the log. TestMVCCOpLogWriter fails without
	// this.
	Safe     bool
	Publish  bool
	RangeIDs []int64
}

// OpLoggerBatch records a log of logical MVCC operations.
type OpLoggerBatch struct {
	Batch
	distinct     distinctOpLoggerBatch
	distinctOpen bool

	ops      []enginepb.MVCCLogicalOp
	opsAlloc bufalloc.ByteAllocator
}

// NewOpLoggerBatch creates a new batch that logs logical mvcc operations and
// wraps the provided batch.
func NewOpLoggerBatch(b Batch) *OpLoggerBatch {
	ol := &OpLoggerBatch{Batch: b}
	ol.distinct.parent = ol
	return ol
}

var _ Batch = &OpLoggerBatch{}

// LogLogicalOp implements the Writer interface.
func (ol *OpLoggerBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	if ol.distinctOpen {
		panic("distinct batch already open")
	}
	ol.logLogicalOp(op, details)
	ol.Batch.LogLogicalOp(op, details)
}

func (ol *OpLoggerBatch) logLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	if keys.IsLocal(details.Key) {
		// Ignore mvcc operations on local keys.
		return
	}

	switch op {
	case MVCCWriteValueOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
		}

		ol.recordOp(&enginepb.MVCCWriteValueOp{
			Key:       details.Key,
			Timestamp: details.Timestamp,
		})
	case MVCCWriteIntentOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
			ol.opsAlloc, details.Txn.Key = ol.opsAlloc.Copy(details.Txn.Key, 0)
		}
		ol.recordOp(&enginepb.MVCCWriteIntentOp{
			Key:             details.Key,
			Value:           details.Value,
			TxnID:           details.Txn.ID,
			TxnKey:          details.Txn.Key,
			TxnMinTimestamp: details.Txn.MinTimestamp,
			Timestamp:       details.Timestamp,
			Publish:         details.Publish,
		})
	case MVCCUpdateIntentOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
		}
		ol.recordOp(&enginepb.MVCCUpdateIntentOp{
			Key:       details.Key,
			Value:     details.Value,
			TxnID:     details.Txn.ID,
			Timestamp: details.Timestamp,
			Publish:   details.Publish,
		})
	case MVCCCommitIntentOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
		}
		ol.recordOp(&enginepb.MVCCCommitIntentOp{
			TxnID:     details.Txn.ID,
			Key:       details.Key,
			Timestamp: details.Timestamp,
		})
	case MVCCAbortIntentOpType:
		ol.recordOp(&enginepb.MVCCAbortIntentOp{
			TxnID: details.Txn.ID,
			Key:   details.Key,
		})
	case MVCCCommitTxnOpType:
		ol.recordOp(&enginepb.MVCCCommitTxnOp{
			TxnID:         details.Txn.ID,
			WriteKeyCount: details.Txn.WriteKeyCount,
			Timestamp:     details.Txn.WriteTimestamp,
		})
	case MVCCAbortOpType:
		ol.recordOp(&enginepb.MVCCAbortOp{
			TxnID:         details.Txn.ID,
			WriteKeyCount: details.Txn.WriteKeyCount,
		})
	default:
		panic(fmt.Sprintf("unexpected op type %v", op))
	}
}

func (ol *OpLoggerBatch) recordOp(op interface{}) {
	ol.ops = append(ol.ops, enginepb.MVCCLogicalOp{})
	ol.ops[len(ol.ops)-1].MustSetValue(op)
}

// LogicalOps returns the list of all logical MVCC operations that have been
// recorded by the logger.
func (ol *OpLoggerBatch) LogicalOps() []enginepb.MVCCLogicalOp {
	if ol == nil {
		return nil
	}
	return ol.ops
}

// Distinct implements the Batch interface.
func (ol *OpLoggerBatch) Distinct() ReadWriter {
	if ol.distinctOpen {
		panic("distinct batch already open")
	}
	ol.distinctOpen = true
	ol.distinct.ReadWriter = ol.Batch.Distinct()
	return &ol.distinct
}

type distinctOpLoggerBatch struct {
	ReadWriter
	parent *OpLoggerBatch
}

// LogLogicalOp implements the Writer interface.
func (dlw *distinctOpLoggerBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	dlw.parent.logLogicalOp(op, details)
	dlw.ReadWriter.LogLogicalOp(op, details)
}

// Close implements the Reader interface.
func (dlw *distinctOpLoggerBatch) Close() {
	if !dlw.parent.distinctOpen {
		panic("distinct batch not open")
	}
	dlw.parent.distinctOpen = false
	dlw.ReadWriter.Close()
}
