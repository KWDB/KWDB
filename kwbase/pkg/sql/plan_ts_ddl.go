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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// tsDDLNode is a planNode created for distributed execution plans,
// used for the second phase execution of DDL in time-series scenarios.
// You can refer to the relevant code in ts_schema_changer.go.
var _ PlanNode = &tsDDLNode{}

type tsDDLNode struct {
	d      jobspb.SyncMetaCacheDetails
	nodeID []roachpb.NodeID
	// ts txn struct for TS DDL commit or rollback
	tsTxn
	// compressInterval is set if stmt is alter compress interval
	compressInterval string
	// vacuumInterval is set if stmt is alter vacuum interval
	vacuumInterval string
}

// tsTxn is used to record ts txn ID and txn event
// (or other txn args in the future) to send to WAL.
type tsTxn struct {
	txnID []byte
	txnEvent
}

func (c *tsDDLNode) StartExec(params RunParams) error {
	return nil
}

func (c *tsDDLNode) Next(RunParams) (bool, error) { return false, nil }

func (c *tsDDLNode) Values() tree.Datums { return tree.Datums{} }

func (c *tsDDLNode) Close(ctx context.Context) {}
