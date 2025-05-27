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

package api

import (
	"context"
	"hash/fnv"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// GetDistributeInfo get distribute info on create table
var GetDistributeInfo func(ctx context.Context, tableID uint32, hashNum uint64) ([]HashPartition, error)

// PreLeaseholderDistribute get the leaseholder pre distribute.
var PreLeaseholderDistribute func(ctx context.Context, txn *kv.Txn, partitions []HashPartition) ([][]roachpb.ReplicaDescriptor, error)

// GetHashPointByPrimaryTag get hashPoint by primaryTag
func GetHashPointByPrimaryTag(hashNum uint64, primaryTags ...[]byte) ([]HashPoint, error) {
	fnv32 := fnv.New32()
	var hashPoints []HashPoint
	for _, primaryTag := range primaryTags {
		_, err := fnv32.Write(primaryTag)
		if err != nil {
			return nil, err
		}
		hashPoints = append(hashPoints, HashPoint(fnv32.Sum32()%uint32(hashNum)))
		log.Eventf(context.TODO(), "hashID: +%v , primaryTag: +%v", HashPoint(fnv32.Sum32()%uint32(hashNum)), primaryTag)
		fnv32.Reset()
	}

	return hashPoints, nil
}

// GetHealthyNodeIDs get all healthy nodes
var GetHealthyNodeIDs func(ctx context.Context) ([]roachpb.NodeID, error)

// GetTableNodeIDs get all healthy nodes
var GetTableNodeIDs func(ctx context.Context, db *kv.DB, tableID uint32, hashNum uint64) ([]roachpb.NodeID, error)

// CreateTSTable ...
var CreateTSTable func(ctx context.Context, tableID uint32, hashNum uint64, nodeID roachpb.NodeID, tsMeta []byte) error
