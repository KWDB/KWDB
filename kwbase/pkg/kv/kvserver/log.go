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

package kvserver

import (
	"context"
	"encoding/json"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func (s *Store) insertRangeLogEvent(
	ctx context.Context, txn *kv.Txn, event storagepb.RangeLogEvent,
) error {
	// Record range log event to console log.
	var info string
	if event.Info != nil {
		info = event.Info.String()
	}
	if log.V(1) {
		log.Infof(ctx, "Range Event: %q, range: %d, info: %s",
			event.EventType, event.RangeID, info)
	}

	const insertEventTableStmt = `
	INSERT INTO system.rangelog (
		timestamp, "rangeID", "storeID", "eventType", "otherRangeID", info
	)
	VALUES(
		$1, $2, $3, $4, $5, $6
	)
	`
	args := []interface{}{
		event.Timestamp,
		event.RangeID,
		event.StoreID,
		event.EventType.String(),
		nil, // otherRangeID
		nil, // info
	}
	if event.OtherRangeID != 0 {
		args[4] = event.OtherRangeID
	}
	if event.Info != nil {
		infoBytes, err := json.Marshal(*event.Info)
		if err != nil {
			return err
		}
		args[5] = string(infoBytes)
	}

	// Update range event metrics. We do this close to the insertion of the
	// corresponding range log entry to reduce potential skew between metrics and
	// range log.
	switch event.EventType {
	case storagepb.RangeLogEventType_split:
		s.metrics.RangeSplits.Inc(1)
	case storagepb.RangeLogEventType_merge:
		s.metrics.RangeMerges.Inc(1)
	case storagepb.RangeLogEventType_add:
		s.metrics.RangeAdds.Inc(1)
	case storagepb.RangeLogEventType_remove:
		s.metrics.RangeRemoves.Inc(1)
	}

	rows, err := s.cfg.SQLExecutor.ExecEx(ctx, "log-range-event", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}

// logSplit logs a range split event into the event table. The affected range is
// the range which previously existed and is being split in half; the "other"
// range is the new range which is being created.
//
// TODO(mrtracy): There are several different reasons that a range split
// could occur, and that information should be logged.
func (s *Store) logSplit(
	ctx context.Context, txn *kv.Txn, updatedDesc, newDesc roachpb.RangeDescriptor,
) error {
	if !s.cfg.LogRangeEvents {
		return nil
	}
	return s.insertRangeLogEvent(ctx, txn, storagepb.RangeLogEvent{
		Timestamp:    selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:      updatedDesc.RangeID,
		EventType:    storagepb.RangeLogEventType_split,
		StoreID:      s.StoreID(),
		OtherRangeID: newDesc.RangeID,
		Info: &storagepb.RangeLogEvent_Info{
			UpdatedDesc: &updatedDesc,
			NewDesc:     &newDesc,
		},
	})
}

// logMerge logs a range split event into the event table. The affected range is
// the subsuming range; the "other" range is the subsumed range.
//
// TODO(benesch): There are several different reasons that a range merge
// could occur, and that information should be logged.
func (s *Store) logMerge(
	ctx context.Context, txn *kv.Txn, updatedLHSDesc, rhsDesc roachpb.RangeDescriptor,
) error {
	if !s.cfg.LogRangeEvents {
		return nil
	}
	return s.insertRangeLogEvent(ctx, txn, storagepb.RangeLogEvent{
		Timestamp:    selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:      updatedLHSDesc.RangeID,
		EventType:    storagepb.RangeLogEventType_merge,
		StoreID:      s.StoreID(),
		OtherRangeID: rhsDesc.RangeID,
		Info: &storagepb.RangeLogEvent_Info{
			UpdatedDesc: &updatedLHSDesc,
			RemovedDesc: &rhsDesc,
		},
	})
}

// logChange logs a replica change event, which represents a replica being added
// to or removed from a range.
// TODO(mrtracy): There are several different reasons that a replica change
// could occur, and that information should be logged.
func (s *Store) logChange(
	ctx context.Context,
	txn *kv.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason storagepb.RangeLogEventReason,
	details string,
) error {
	if !s.cfg.LogRangeEvents {
		return nil
	}

	var logType storagepb.RangeLogEventType
	var info storagepb.RangeLogEvent_Info
	switch changeType {
	case roachpb.ADD_REPLICA:
		logType = storagepb.RangeLogEventType_add
		info = storagepb.RangeLogEvent_Info{
			AddedReplica: &replica,
			UpdatedDesc:  &desc,
			Reason:       reason,
			Details:      details,
		}
	case roachpb.REMOVE_REPLICA:
		logType = storagepb.RangeLogEventType_remove
		info = storagepb.RangeLogEvent_Info{
			RemovedReplica: &replica,
			UpdatedDesc:    &desc,
			Reason:         reason,
			Details:        details,
		}
	default:
		return errors.Errorf("unknown replica change type %s", changeType)
	}

	return s.insertRangeLogEvent(ctx, txn, storagepb.RangeLogEvent{
		Timestamp: selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:   desc.RangeID,
		EventType: logType,
		StoreID:   s.StoreID(),
		Info:      &info,
	})
}

// selectEventTimestamp selects a timestamp for this log message. If the
// transaction this event is being written in has a non-zero timestamp, then that
// timestamp should be used; otherwise, the store's physical clock is used.
// This helps with testing; in normal usage, the logging of an event will never
// be the first action in the transaction, and thus the transaction will have an
// assigned database timestamp. However, in the case of our tests log events
// *are* the first action in a transaction, and we must elect to use the store's
// physical time instead.
func selectEventTimestamp(s *Store, input hlc.Timestamp) time.Time {
	if input == (hlc.Timestamp{}) {
		return s.Clock().PhysicalTime()
	}
	return input.GoTime()
}
