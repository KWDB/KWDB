// Copyright 2019 The Cockroach Authors.
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

package ddl_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestShowRangesWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT SELECT i FROM generate_series(0, 20) AS g(i)`)

	const leaseHolderIdx = 0
	const leaseHolderLocalityIdx = 1
	const replicasColIdx = 2
	const localitiesColIdx = 3
	replicas := make([]int, 3)

	// TestClusters get some localities by default.
	q := `SELECT lease_holder, lease_holder_locality, replicas, replica_localities from [SHOW RANGES FROM TABLE t]`
	result := sqlDB.QueryStr(t, q)
	for _, row := range result {
		// Verify the leaseholder localities.
		leaseHolder := row[leaseHolderIdx]
		leaseHolderLocalityExpected := fmt.Sprintf(`region=test,dc=dc%s`, leaseHolder)
		if row[leaseHolderLocalityIdx] != leaseHolderLocalityExpected {
			t.Fatalf("expected %s found %s", leaseHolderLocalityExpected, row[leaseHolderLocalityIdx])
		}

		// Verify the replica localities.
		_, err := fmt.Sscanf(row[replicasColIdx], "{%d,%d,%d}", &replicas[0], &replicas[1], &replicas[2])
		if err != nil {
			t.Fatal(err)
		}
		var builder strings.Builder
		builder.WriteString("{")
		for i, replica := range replicas {
			builder.WriteString(fmt.Sprintf(`"region=test,dc=dc%d"`, replica))
			if i != len(replicas)-1 {
				builder.WriteString(",")
			}
		}
		builder.WriteString("}")
		expected := builder.String()
		if row[localitiesColIdx] != expected {
			t.Fatalf("expected %s found %s", expected, row[localitiesColIdx])
		}
	}
}
