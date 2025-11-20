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

package hashrouter

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
)

func getStoreIDByNodeID(
	nodeID roachpb.NodeID, stores map[roachpb.StoreID]roachpb.StoreDescriptor,
) roachpb.StoreID {
	var id roachpb.StoreID
	for storeID, store := range stores {
		if store.Node.NodeID == nodeID {
			id = storeID
			break
		}
	}
	return id
}

// GetTableNodeIDs gets nodeids
func GetTableNodeIDs(
	ctx context.Context, db *kv.DB, tableID uint32, hashNum uint64,
) ([]roachpb.NodeID, error) {
	var nodeIDs []roachpb.NodeID
	nodeIDList := make(map[roachpb.NodeID]struct{})
	var retErr error
	// get range when replica is voter_incoming, because we will
	// miss the node when alter the table on some node.
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     4 * time.Second,
		Multiplier:     2,
		MaxRetries:     20,
	}); r.Next(); {
		if retErr = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			ranges, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
				Key:    sqlbase.MakeTsRangeKey(sqlbase.ID(tableID), 0, hashNum),
				EndKey: sqlbase.MakeTsRangeKey(sqlbase.ID(tableID), hashNum, hashNum),
			})
			if err != nil {
				return err
			}
			if len(ranges) == 0 {
				return pgerror.Newf(pgcode.Warning, "can not get table : %v ranges.", tableID)
			}
			for _, r := range ranges {
				var desc roachpb.RangeDescriptor
				if err := r.ValueProto(&desc); err != nil {
					return err
				}
				for _, replica := range desc.InternalReplicas {
					nodeIDList[replica.NodeID] = struct{}{}
				}
			}
			return nil
		}); retErr != nil {
			log.Warningf(ctx, "get table node id failed: %s", retErr.Error())
			continue
		}
		break
	}
	if len(nodeIDList) == 0 {
		return nil, retErr
	}
	for nodeID := range nodeIDList {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs, nil
}

// CreateTSTable create ts table
func CreateTSTable(
	ctx context.Context, tableID uint32, hashNum uint64, nodeID roachpb.NodeID, tsMeta []byte,
) error {
	return hrMgr.tseDB.CreateTSTable(ctx, sqlbase.ID(tableID), hashNum, nodeID, tsMeta)
}
