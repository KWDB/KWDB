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
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func init() {
	api.GetHealthyNodeIDs = GetHealthyNodeIDs
	api.GetTableNodeIDs = GetTableNodeIDs
	api.CreateTSTable = CreateTSTable
	api.PreLeaseholderDistribute = PreLeaseholderDistribute
	api.GetDistributeInfo = GetDistributeInfo
}

// hashRouterInfo  the group distribute by table
type hashRouterInfo struct {
	tableID           uint32
	cacheVersion      sqlbase.DescriptorVersion
	hashPartitionNum  int64
	hashPartitionSize int
	groupsMap         map[api.EntityRangeGroupID]*api.EntityRangeGroup
	mu                syncutil.RWMutex
}

// HRManager all group distribute
type HRManager struct {
	// context from server
	ctx context.Context
	// cluster settings
	cs            *cluster.Settings
	routerCaches  map[uint32]*hashRouterInfo
	gossipMessage api.GossipEntityRangeGroupMessage
	execConfig    *sql.ExecutorConfig
	db            *kv.DB
	tseDB         *kvcoord.DB
	gossip        *gossip.Gossip
	leaseMgr      *sql.LeaseManager
	nodeLiveness  *kvserver.NodeLiveness
	storePool     *kvserver.StorePool
	mu            syncutil.RWMutex
}

var hrMgr *HRManager

// NewHashRouterManager init manager when server start
func NewHashRouterManager(
	ctx context.Context,
	cs *cluster.Settings,
	db *kv.DB,
	tseDB *kvcoord.DB,
	execConfig *sql.ExecutorConfig,
	gossip *gossip.Gossip,
	leaseMgr *sql.LeaseManager,
	nodeLiveness *kvserver.NodeLiveness,
	storePool *kvserver.StorePool,
) (*HRManager, error) {
	if execConfig.StartMode == sql.StartSingleReplica {
		api.MppMode = true
	} else if execConfig.StartMode == sql.StartSingleNode {
		api.MppMode = true
		api.SingleNode = true
	}
	hrMgr = &HRManager{
		ctx:          ctx,
		cs:           cs,
		routerCaches: make(map[uint32]*hashRouterInfo),
		db:           db,
		tseDB:        tseDB,
		execConfig:   execConfig,
		gossip:       gossip,
		leaseMgr:     leaseMgr,
		nodeLiveness: nodeLiveness,
		storePool:    storePool,
	}
	return hrMgr, nil
}

// GetDistributeInfo get distribute info on create table
func GetDistributeInfo(
	ctx context.Context, tableID uint32, hashNum uint64,
) ([]api.HashPartition, error) {
	var partitions []api.HashPartition
	partitionBalanceNumber := settings.DefaultPartitionCoefficient.Get(hrMgr.execConfig.SV())
	if partitionBalanceNumber == 0 {
		return nil, errors.Errorf("the cluster setting partitionBalanceNumber is 0.please wait and try again")
	}
	var groupNum int
	if hrMgr.execConfig.StartMode == sql.StartSingleNode {
		groupNum = 1
	} else {
		groupNum = int(hashNum)
	}
	splitMode := int(settings.TSRangeSplitModeSetting.Get(hrMgr.execConfig.SV()))
	if splitMode == 0 {
		for i := 0; i < groupNum; i += int(partitionBalanceNumber) {
			partitions = append(partitions, api.HashPartition{
				StartPoint:     api.HashPoint(i),
				StartTimeStamp: math.MinInt64,
			})
		}
	} else if splitMode == 1 {
		for i := 0; i < groupNum; i += int(partitionBalanceNumber) {
			splitType := api.SplitType(i / int(partitionBalanceNumber))
			switch splitType {
			case api.SplitWithOneHashPoint:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
				}
			case api.SplitWithHashPointAndPositiveTimeStamp:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: 1681111110000,
					})
				}
			case api.SplitWithHashPointAndNegativeTimeStamp:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: -1681111110000,
					})
				}
			default:
				partitions = append(partitions, api.HashPartition{
					StartPoint:     api.HashPoint(i),
					StartTimeStamp: math.MinInt64,
				})
			}
		}
	}
	return partitions, nil
}

// PreLeaseholderDistribute get the pre distribute on single replica mode
func PreLeaseholderDistribute(
	ctx context.Context, txn *kv.Txn, partitions []api.HashPartition,
) ([][]roachpb.ReplicaDescriptor, error) {
	var distributeReplica [][]roachpb.ReplicaDescriptor
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	var nodeList []roachpb.NodeID
	for _, n := range nodeStatus.Nodes {
		// filter nodes which can't be used
		if nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_LIVE {
			nodeList = append(nodeList, n.Desc.NodeID)
		}

	}
	if len(nodeList) == 0 {
		return nil, errors.Errorf("the cluster not have live node now")
	}
	for index := range partitions {
		var internalReplica []roachpb.ReplicaDescriptor
		start := index % len(nodeList)
		nodes := []roachpb.NodeID{
			nodeList[start],
			nodeList[(start+1)%len(nodeList)],
			nodeList[(start+2)%len(nodeList)],
		}
		for _, nodeID := range nodes {
			internalReplica = append(internalReplica, roachpb.ReplicaDescriptor{
				NodeID:  nodeID,
				StoreID: getStoreIDByNodeID(nodeID, hrMgr.storePool.GetStores()),
			})
		}
		distributeReplica = append(distributeReplica, internalReplica)
	}
	return distributeReplica, nil
}

// GetHealthyNodeIDs get all healthy nodes
func GetHealthyNodeIDs(ctx context.Context) ([]roachpb.NodeID, error) {
	var NodeIDList []roachpb.NodeID
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	for id, n := range nodeStatus.LivenessByNodeID {
		switch n {
		case storagepb.NodeLivenessStatus_LIVE:
			NodeIDList = append(NodeIDList, id)
		}
	}
	if len(NodeIDList) == 0 {
		return NodeIDList, pgerror.New(pgcode.Warning, "all nodes are unhealthy")
	}
	return NodeIDList, nil
}
