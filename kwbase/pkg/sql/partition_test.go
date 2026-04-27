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

package sql_test

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// Test the behavior of a binary that doesn't link in CCL when it comes to
// dealing with partitions. Some things are expected to work, others aren't.
func TestRemovePartitioningOSS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("todo(fyx):is not working")
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(ctx)

	const numRows = 100
	if err := tests.CreateKVTable(sqlDBRaw, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)

	// Hack in partitions. Doing this properly requires a CCL binary.
	tableDesc.PrimaryIndex.Partitioning = sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		Range: []sqlbase.PartitioningDescriptor_Range{{
			Name:          "p1",
			FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
			ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
		}},
	}
	tableDesc.Indexes[0].Partitioning = sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		Range: []sqlbase.PartitioningDescriptor_Range{{
			Name:          "p2",
			FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
			ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
		}},
	}
	if err := kvDB.Put(ctx, tableKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		t.Fatal(err)
	}
	exp := `CREATE TABLE kv (
	k INT8 NOT NULL,
	v INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX foo (v ASC) PARTITION BY RANGE (v) (
		PARTITION p2 VALUES FROM (1) TO (2)
	),
	FAMILY fam_0_k (k),
	FAMILY fam_1_v (v)
) PARTITION BY RANGE (k) (
	PARTITION p1 VALUES FROM (1) TO (2)
)
-- Warning: Partitioned table with no zone configurations.`
	if a := sqlDB.QueryStr(t, "SHOW CREATE t.kv")[0][1]; exp != a {
		t.Fatalf("expected:\n%s\n\ngot:\n%s\n\n", exp, a)
	}

	// Hack in partition zone configs. This also requires a CCL binary to do
	// properly.
	zoneConfig := zonepb.ZoneConfig{
		Subzones: []zonepb.Subzone{
			{
				IndexID:       uint32(tableDesc.PrimaryIndex.ID),
				PartitionName: "p1",
				Config:        s.(*server.TestServer).Cfg.DefaultZoneConfig,
			},
			{
				IndexID:       uint32(tableDesc.Indexes[0].ID),
				PartitionName: "p2",
				Config:        s.(*server.TestServer).Cfg.DefaultZoneConfig,
			},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, zoneConfigBytes)
	for _, p := range []string{
		"PARTITION p1 OF INDEX t.public.kv@primary",
		"PARTITION p2 OF INDEX t.public.kv@foo",
	} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, p); !exists {
			t.Fatalf("zone config for %s does not exist", p)
		}
	}

	// Some things don't work.
	sqlDB.ExpectErr(t,
		"OSS binaries do not include enterprise features",
		`ALTER PARTITION p1 OF TABLE t.kv CONFIGURE ZONE USING DEFAULT`)
	sqlDB.ExpectErr(t,
		"OSS binaries do not include enterprise features",
		`ALTER PARTITION p2 OF INDEX t.kv@foo CONFIGURE ZONE USING DEFAULT`)

	// But removing partitioning works.
	sqlDB.Exec(t, `ALTER TABLE t.kv PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t.kv@foo PARTITION BY NOTHING`)
	sqlDB.Exec(t, `DELETE FROM system.zones WHERE id = $1`, tableDesc.ID)
	sqlDB.Exec(t, `ALTER TABLE t.kv PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t.kv@foo PARTITION BY NOTHING`)

	exp = `CREATE TABLE kv (
	k INT8 NOT NULL,
	v INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX foo (v ASC),
	FAMILY fam_0_k (k),
	FAMILY fam_1_v (v)
)`
	if a := sqlDB.QueryStr(t, "SHOW CREATE t.kv")[0][1]; exp != a {
		t.Fatalf("expected:\n%s\n\ngot:\n%s\n\n", exp, a)
	}
}

// TestNewPartitioningDescriptor tests the NewPartitioningDescriptor function
func TestNewPartitioningDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	evalCtx := &tree.EvalContext{}

	// Test case 1: Nil partitionBy
	t.Run("NilPartitionBy", func(t *testing.T) {
		tableDesc := &sqlbase.MutableTableDescriptor{}
		indexDesc := &sqlbase.IndexDescriptor{}
		partDesc, err := sql.NewPartitioningDescriptor(ctx, evalCtx, tableDesc, indexDesc, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(partDesc.HashPoint) != 0 || len(partDesc.List) != 0 || len(partDesc.Range) != 0 {
			t.Fatalf("expected empty partitioning descriptor, got: %v", partDesc)
		}
	})

	// Test case 2: TS Table with HashPoint
	t.Run("TSTableHashPoint", func(t *testing.T) {
		tableDesc := &sqlbase.MutableTableDescriptor{
			TableDescriptor: sqlbase.TableDescriptor{
				TableType: tree.TimeseriesTable,
			},
		}
		indexDesc := sqlbase.IndexDescriptor{}
		hashPoint1 := tree.HashPointPartition{
			Name:       tree.UnrestrictedName("p1"),
			HashPoints: []int32{1, 2, 3},
		}
		hashPoint2 := tree.HashPointPartition{
			Name: tree.UnrestrictedName("p2"),
			From: 10,
			To:   20,
		}
		partBy := &tree.PartitionBy{
			HashPoint: []tree.HashPointPartition{hashPoint1, hashPoint2},
		}
		partDesc, err := sql.NewPartitioningDescriptor(ctx, evalCtx, tableDesc, &indexDesc, partBy)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if partDesc.HashPoint[0].Name != "p1" || len(partDesc.HashPoint[0].HashPoints) != 3 {
			t.Fatalf("expected hash point p1 with 3 points, got: %v", partDesc.HashPoint[0])
		}
		if partDesc.HashPoint[1].Name != "p2" || partDesc.HashPoint[1].FromPoint != 10 || partDesc.HashPoint[1].ToPoint != 20 {
			t.Fatalf("expected hash point p2 with from=10 and to=20, got: %v", partDesc.HashPoint[1])
		}
	})

	// Test case 3: TS Table with invalid HashPoint (from >= to)
	t.Run("TSTableInvalidHashPoint", func(t *testing.T) {
		tableDesc := &sqlbase.MutableTableDescriptor{
			TableDescriptor: sqlbase.TableDescriptor{
				TableType: tree.TimeseriesTable,
			},
		}
		indexDesc := &sqlbase.IndexDescriptor{}
		hashPoint := tree.HashPointPartition{
			Name: tree.UnrestrictedName("p1"),
			From: 20,
			To:   10, // from >= to, which is invalid
		}
		partBy := &tree.PartitionBy{
			HashPoint: []tree.HashPointPartition{hashPoint},
		}
		_, err := sql.NewPartitioningDescriptor(ctx, evalCtx, tableDesc, indexDesc, partBy)
		if err == nil {
			t.Fatal("expected error for invalid hash point, got nil")
		}
		if !strings.Contains(err.Error(), "from point must smaller than to point") {
			t.Fatalf("expected error message to contain 'from point must smaller than to point', got: %s", err.Error())
		}
	})
}
