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

package tse_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// setClusterArgs set cluster args
func setClusterArgs(nodes int, baseDir string) base.TestClusterArgs {
	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}

	for i := 0; i < nodes; i++ {
		args := base.TestServerArgs{}
		storeID := roachpb.StoreID(i + 1)
		path := filepath.Join(baseDir, fmt.Sprintf("s%d", storeID))
		args.StoreSpecs = []base.StoreSpec{{Path: path}}
		args.CatchCoreDump = true
		clusterArgs.ServerArgsPerNode[i] = args
	}
	return clusterArgs
}

// prepareTsTable create timeseries table return ts info
func prepareTsTable(
	t *testing.T, ctx context.Context, tc *testcluster.TestCluster,
) (uint64, uint64, uint64, int64, int64, roachpb.NodeID, []roachpb.NodeID) {
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TS DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.ts (ts timestamp not null, e1 int) tags(attr1 int not null) primary tags (attr1)`)
	for i := 0; i <= 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO d1.ts(ts, e1, attr1) VALUES (now(), %d, %d)`, i, i))
	}

	var tableID, dbID uint64
	var startHashPoint, endHashPoint uint64
	var startKey, endKey []byte
	var leaseHolder roachpb.NodeID
	var replicasStr string
	const tableIDQuery = `
SELECT tables.id, tables."parentID" FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	const partitionQuery = `
SELECT start_key, end_key, lease_holder, replicas
FROM kwdb_internal.ranges
WHERE database_name = $1
  AND table_name = $2
  LIMIT 1
`
	sqlDB.QueryRow(t, tableIDQuery, "d1", "ts").Scan(&tableID, &dbID)
	sqlDB.QueryRow(t, partitionQuery, "d1", "ts").Scan(&startKey, &endKey, &leaseHolder, &replicasStr)

	replicas := make([]roachpb.NodeID, 3)
	_, err := fmt.Sscanf(replicasStr, "{%d,%d,%d}", &replicas[0], &replicas[1], &replicas[2])
	require.NoError(t, err)

	var startTs, endTs int64
	_, startHashPoint, endHashPoint, startTs, endTs, err = sqlbase.DecodeTSRangeKey(startKey, endKey)
	require.NoError(t, err)

	return tableID, startHashPoint, endHashPoint, startTs, endTs, leaseHolder, replicas
}

// getOptServer return opt servers
func getOptServer(
	nodes int, tc *testcluster.TestCluster, leaseHolder roachpb.NodeID, replicas []roachpb.NodeID,
) (*server.TestServer, *server.TestServer) {
	var leaseServer, targetServer *server.TestServer
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		if curServer.NodeID() == leaseHolder {
			leaseServer = curServer
			break
		}
	}
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		found := false
		for _, replica := range replicas {
			if curServer.NodeID() == replica {
				found = true
				break
			}
		}
		if !found {
			targetServer = curServer
			break
		}
	}
	return leaseServer, targetServer
}

// TestSnapshotMultiNode test same group on diff nodes
// n1(g1) -> n4(g1)
func TestSnapshotMultiNode(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID, startHashPoint, endHashPoint, startTs, endTs, leaseHolder, replicas := prepareTsTable(t, ctx, tc)

	// get opt server
	leaseServer, targetServer := getOptServer(nodes, tc, leaseHolder, replicas)

	srcSnapshotID, err := leaseServer.TSEngine().CreateSnapshotForRead(tableID, startHashPoint, endHashPoint, startTs, endTs)
	require.NoError(t, err)

	destSnapshotID, err := targetServer.TSEngine().CreateSnapshotForWrite(tableID, startHashPoint, endHashPoint, startTs, endTs)
	require.NoError(t, err)

	apply := false
	var data []byte
	for {
		data, err = leaseServer.TSEngine().GetSnapshotNextBatchData(tableID, srcSnapshotID)
		require.NoError(t, err)

		if len(data) == 0 {
			apply = true
			break
		}

		err = targetServer.TSEngine().WriteSnapshotBatchData(tableID, destSnapshotID, data)
		require.NoError(t, err)
	}

	if apply {
		err = targetServer.TSEngine().WriteSnapshotSuccess(tableID, destSnapshotID)
		require.NoError(t, err)
	} else {
		err = targetServer.TSEngine().WriteSnapshotRollback(tableID, destSnapshotID)
		require.NoError(t, err)
	}

	err = targetServer.TSEngine().DeleteSnapshot(tableID, destSnapshotID)
	require.NoError(t, err)

	err = leaseServer.TSEngine().DeleteSnapshot(tableID, srcSnapshotID)
	require.NoError(t, err)
}

func TestCreateTsTableFailed(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	clusterArgs.ServerArgs.Knobs = base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			RunCreateTableFailedAndRollback: func() error {
				return errors.Errorf("create ts table failed. try roll back")
			},
		},
		Store: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
		},
	}
	for k := range clusterArgs.ServerArgsPerNode {
		v := clusterArgs.ServerArgsPerNode[k]
		v.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
			RunCreateTableFailedAndRollback: func() error {
				return errors.Errorf("create ts table failed. try roll back")
			},
		}
		v.Knobs.Store = &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
		}
		clusterArgs.ServerArgsPerNode[k] = v
	}
	c := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer c.Stopper().Stop(ctx)

	s := c.Conns[0]
	_, err := s.Exec("create ts database tsdb")
	require.Equal(t, err, nil)
	_, err = s.Exec("create table tsdb.tab1(k_timestamp timestamp not null,e1 float not null, e2 float4 not null, e3 float8 not null, e4 double precision not null,e5 real not null,e6 int not null, e7 int2 not null, e8 int4 not null, e9 int8 not null, e10 int64 not null, e11 bigint not null, e12 smallint not null,e13 integer not null,e14 bool not null,e15 char not null, e16 char(100) not null, e17 varbytes(100) not null,e18 timestamp not null ,e19 nchar(100) not null,e20 varchar(100) not null,e21 nvarchar(100) not null,e22 varbytes(100) not null, e23 varbytes not null,e24 nchar not null,e25 varchar not null,e26 nvarchar not null,e27 varbytes not null) tags (tag1 int not null)primary tags(tag1);")
	require.NotEqual(t, err, nil)

	// When create ts table is submitted in the first phase, its first range will be created.
	// This step will wait for the second phase to succeed or fail before proceeding.
	// After the second phase fails, the first range query tableDesc fails.And change
	// splitType to DEFAULT and then split the first range.
	// Sleep for a while to wait for the first range to split.
	time.Sleep(time.Second * 3)

	rows, err := c.Servers[0].InternalExecutor().(*sql.InternalExecutor).Query(ctx, "", nil, "select start_pretty, end_pretty from kwdb_internal.ranges where end_pretty = '/Max';")
	require.Equal(t, "/Table/78", string(*(rows[0][0].(*tree.DString))))
	require.Equal(t, "/Max", string(*(rows[0][1].(*tree.DString))))
	rows, err = c.Servers[0].InternalExecutor().(*sql.InternalExecutor).Query(ctx, "", nil, "select range_type from kwdb_internal.ranges_no_leases where end_pretty = '/Max';")
	require.Equal(t, "", "")
	require.Equal(t, "DEFAULT_RANGE", string(*(rows[0][0].(*tree.DString))))

	rows, err = c.Servers[0].InternalExecutor().(*sql.InternalExecutor).Query(ctx, "", nil, "select * from system.namespace where name = 'tab1';")
	require.Equal(t, []tree.Datums([]tree.Datums(nil)), rows)
	require.Equal(t, nil, err)

}

func TestGetTableMetaByVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params, _ := tests.CreateTestServerParams()
	params.StoreSpecs = []base.StoreSpec{{Path: baseDir}}
	params.CatchCoreDump = true
	s, sqlDB, kvDB := serverutils.StartServer(t, params)

	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec("create ts database tsdb"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("create table tsdb.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag)"); err != nil {
		t.Fatal(err)
	}
	// When create ts table is submitted in the first phase, its first range will be created.
	// This step will wait for the second phase to succeed or fail before proceeding.
	// After the second phase fails, the first range query tableDesc fails.And change
	// splitType to DEFAULT and then split the first range.
	// Sleep for a while to wait for the first range to split.
	time.Sleep(time.Second * 3)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "tsdb", "t")
	desc := sqlbase.NewMutableExistingTableDescriptor(*tableDesc)
	alterStmt := "alter table tsdb.t add column b int"
	stmt, err := parser.ParseOne(alterStmt)
	if err != nil {
		t.Fatal(err)
	}
	alterTable, ok := stmt.AST.(*tree.AlterTable)
	if !ok {
		t.Fatalf("expected tree.AlterTABLE, but got %T", stmt)
	}

	for _, cmd := range alterTable.Cmds {
		switch c := cmd.(type) {
		case *tree.AlterTableAddColumn:
			d := c.ColumnDef
			TSColumn, _, err := sqlbase.MakeTSColumnDefDescs(string(d.Name), d.Type, true, false, sqlbase.ColumnType_TYPE_DATA, d.DefaultExpr.Expr, nil)
			if err != nil {
				t.Fatal(err)
			}
			desc.AddColumn(TSColumn)
			if err := desc.AllocateIDs(); err != nil {
				t.Fatal(err)
			}
			desc.Version++
			desc.TsTable.TsVersion++
			desc.TsTable.NextTsVersion++
			err1 := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.SetSystemConfigTrigger(); err != nil {
					return err
				}
				desc.ModificationTime, err = txn.CommitTimestamp()
				if err := desc.ValidateTable(); err != nil {
					t.Fatal(err)
				}
				batch := txn.NewBatch()
				descKey := sqlbase.MakeDescMetadataKey(desc.ID)
				descDesc := sqlbase.WrapDescriptor(desc)
				batch.Put(descKey, descDesc)
				return txn.CommitInBatch(ctx, batch)
			})
			require.NoError(t, err1)
		}
	}

	tsEngine := s.(*server.TestServer).TSEngine()
	resErr := tsEngine.GetTableMetaByVersion(uint64(desc.ID), 2)
	if resErr != nil {
		t.Fatal(resErr)
	}
	res, err := tsEngine.GetTsVersion(uint64(desc.ID))
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 2, int(res))

	//_, err2 := sqlDB.Exec("insert into tsdb.t values (now(),1,2,3)")
	//if err2 != nil {
	//	t.Fatal(err2)
	//}
	//rows := sqlDB.QueryRow("select * from tsdb.t")
	//var ts string
	//var a, b, ptag int
	//// Valid row.
	//require.NoError(t, rows.Scan(&ts, &a, &b, &ptag))
	//if a != 1 || b != 2 || ptag != 3 {
	//	t.Fatalf("expect: [1,2,3] got [%d,%d,%d]", a, b, ptag)
	//}
	//require.NoError(t, err2)
}
