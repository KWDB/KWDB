// Copyright 2016 The Cockroach Authors.
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

package base

import (
	"context"
	"strconv"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// NodeIDContainer is used to share a single roachpb.NodeID instance between
// multiple layers. It allows setting and getting the value. Once a value is
// set, the value cannot change.
type NodeIDContainer struct {
	_ util.NoCopy

	// nodeID is atomically updated under the mutex; it can be read atomically
	// without the mutex.
	nodeID int32
}

// String returns the node ID, or "?" if it is unset.
func (n *NodeIDContainer) String() string {
	val := n.Get()
	if val == 0 {
		return "?"
	}
	return strconv.Itoa(int(val))
}

// Get returns the current node ID; 0 if it is unset.
func (n *NodeIDContainer) Get() roachpb.NodeID {
	return roachpb.NodeID(atomic.LoadInt32(&n.nodeID))
}

// Set sets the current node ID. If it is already set, the value must match.
func (n *NodeIDContainer) Set(ctx context.Context, val roachpb.NodeID) {
	if val <= 0 {
		log.Fatalf(ctx, "trying to set invalid NodeID: %d", val)
	}
	oldVal := atomic.SwapInt32(&n.nodeID, int32(val))
	if oldVal == 0 {
		if log.V(2) {
			log.Infof(ctx, "NodeID set to %d", val)
		}
	} else if oldVal != int32(val) {
		log.Fatalf(ctx, "different NodeIDs set: %d, then %d", oldVal, val)
	}
}

// Reset changes the NodeID regardless of the old value.
//
// Should only be used in testing code.
func (n *NodeIDContainer) Reset(val roachpb.NodeID) {
	atomic.StoreInt32(&n.nodeID, int32(val))
}
