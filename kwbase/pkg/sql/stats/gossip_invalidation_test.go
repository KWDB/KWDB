// Copyright 2017 The Cockroach Authors.
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

package stats_test

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestGossipInvalidation verifies that the cache gets invalidated automatically
// when a new stat is generated.
func TestGossipInvalidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sc := stats.NewTableStatisticsCache(
		10, /* cacheSize */
		tc.Server(0).GossipI().(*gossip.Gossip),
		tc.Server(0).DB(),
		tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
	)

	sr0 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sr0.Exec(t, "CREATE DATABASE test")
	sr0.Exec(t, "CREATE TABLE test.t (k INT PRIMARY KEY, v INT)")
	sr0.Exec(t, "INSERT INTO test.t VALUES (1, 1), (2, 2), (3, 3)")

	tableDesc := sqlbase.GetTableDescriptor(tc.Server(0).DB(), "test", "t")
	tableID := tableDesc.ID

	expectNStats := func(n int) error {
		stats, err := sc.GetTableStats(ctx, tableID)
		if err != nil {
			t.Fatal(err)
		}
		if len(stats) != n {
			return fmt.Errorf("expected %d stats, got: %v", n, stats)
		}
		return nil
	}

	if err := expectNStats(0); err != nil {
		t.Fatal(err)
	}
	sr1 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
	sr1.Exec(t, "CREATE STATISTICS k ON k FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(1)
	})

	sr2 := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	sr2.Exec(t, "CREATE STATISTICS v ON v FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(2)
	})
}
