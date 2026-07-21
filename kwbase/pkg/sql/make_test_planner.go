// Copyright 2015 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// MakeTestPlanner returns a generic planner for testing
func MakeTestPlanner() *GenericPlanner {
	// Initialize an Executorconfig sufficiently for the purposes of creating a
	// GenericPlanner.
	var nodeID base.NodeIDContainer
	nodeID.Set(context.TODO(), 1)
	execCfg := ExecutorConfig{
		Settings: cluster.MakeTestingClusterSettings(),
		NodeInfo: NodeInfo{
			NodeID: &nodeID,
			ClusterID: func() uuid.UUID {
				return uuid.MakeV4()
			},
		},
	}

	// TODO(andrei): pass the cleanup along to the caller.
	p, _ /* cleanup */ := newInternalPlanner(
		"test", nil /* txn */, security.RootUser, &MemoryMetrics{}, &execCfg,
	)
	return p
}
