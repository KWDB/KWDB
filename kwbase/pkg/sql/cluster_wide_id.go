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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uint128"
)

// ClusterWideID represents an identifier that is guaranteed to be unique across
// a cluster. It is a wrapper around a uint128. It logically consists of 96 bits
// of HLC timestamp, and 32 bits of node ID.
type ClusterWideID struct {
	uint128.Uint128
}

// GenerateClusterWideID takes a timestamp and node ID, and generates a
// ClusterWideID.
func GenerateClusterWideID(timestamp hlc.Timestamp, nodeID roachpb.NodeID) ClusterWideID {
	loInt := (uint64)(nodeID)
	loInt = loInt | ((uint64)(timestamp.Logical) << 32)

	return ClusterWideID{Uint128: uint128.FromInts((uint64)(timestamp.WallTime), loInt)}
}

// StringToClusterWideID converts a string to a ClusterWideID. If the string is
// not a valid uint128, an error is returned.
func StringToClusterWideID(s string) (ClusterWideID, error) {
	id, err := uint128.FromString(s)
	if err != nil {
		return ClusterWideID{}, err
	}
	return ClusterWideID{Uint128: id}, nil
}

// BytesToClusterWideID converts raw bytes into a ClusterWideID.
// The caller is responsible for ensuring the byte slice contains 16 bytes.
func BytesToClusterWideID(b []byte) ClusterWideID {
	id := uint128.FromBytes(b)
	return ClusterWideID{Uint128: id}
}

// GetNodeID extracts the node ID from a ClusterWideID.
func (id ClusterWideID) GetNodeID() int32 {
	return int32(0xFFFFFFFF & id.Lo)
}
