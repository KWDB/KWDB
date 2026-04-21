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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestShowZoneConfigColumns tests the definition of result columns
func TestShowZoneConfigColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify showZoneConfigColumns
	require.Equal(t, 13, len(showZoneConfigColumns))
	require.Equal(t, "zone_id", showZoneConfigColumns[0].Name)
	require.Equal(t, "subzone_id", showZoneConfigColumns[1].Name)
	require.Equal(t, "target", showZoneConfigColumns[2].Name)
	require.Equal(t, "range_name", showZoneConfigColumns[3].Name)
	require.Equal(t, "database_name", showZoneConfigColumns[4].Name)
	require.Equal(t, "table_name", showZoneConfigColumns[5].Name)
	require.Equal(t, "index_name", showZoneConfigColumns[6].Name)
	require.Equal(t, "partition_name", showZoneConfigColumns[7].Name)
	require.Equal(t, "raw_config_yaml", showZoneConfigColumns[8].Name)
	require.Equal(t, "raw_config_sql", showZoneConfigColumns[9].Name)
	require.Equal(t, "raw_config_protobuf", showZoneConfigColumns[10].Name)
	require.Equal(t, "full_config_yaml", showZoneConfigColumns[11].Name)
	require.Equal(t, "full_config_sql", showZoneConfigColumns[12].Name)

	// Verify column types
	require.Equal(t, types.Int, showZoneConfigColumns[0].Typ)
	require.Equal(t, types.Int, showZoneConfigColumns[1].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[2].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[3].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[4].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[5].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[6].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[7].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[8].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[9].Typ)
	require.Equal(t, types.Bytes, showZoneConfigColumns[10].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[11].Typ)
	require.Equal(t, types.String, showZoneConfigColumns[12].Typ)
}

// TestShowZoneConfigColumnsHidden tests hidden columns
func TestShowZoneConfigColumnsHidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify hidden columns
	require.True(t, showZoneConfigColumns[0].Hidden)  // zone_id
	require.True(t, showZoneConfigColumns[1].Hidden)  // subzone_id
	require.False(t, showZoneConfigColumns[2].Hidden) // target
	require.True(t, showZoneConfigColumns[3].Hidden)  // range_name
	require.True(t, showZoneConfigColumns[4].Hidden)  // database_name
	require.True(t, showZoneConfigColumns[5].Hidden)  // table_name
	require.True(t, showZoneConfigColumns[6].Hidden)  // index_name
	require.True(t, showZoneConfigColumns[7].Hidden)  // partition_name
	require.True(t, showZoneConfigColumns[8].Hidden)  // raw_config_yaml
	require.False(t, showZoneConfigColumns[9].Hidden) // raw_config_sql
	require.True(t, showZoneConfigColumns[10].Hidden) // raw_config_protobuf
	require.True(t, showZoneConfigColumns[11].Hidden) // full_config_yaml
	require.True(t, showZoneConfigColumns[12].Hidden) // full_config_sql
}

// TestShowZoneConfigNode tests the ShowZoneConfig node
func TestShowZoneConfigNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a ShowZoneConfig node
	zs := tree.ZoneSpecifier{
		Database: "test_db",
	}
	n := &tree.ShowZoneConfig{ZoneSpecifier: zs}
	require.Equal(t, "test_db", string(n.Database))
}

// TestShowZoneConfigBasic tests the basic SHOW ZONE CONFIGURATION functionality
func TestShowZoneConfigBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create test database
	_, err := db.Exec("CREATE DATABASE zone_db")
	require.NoError(t, err)

	// Configure zone config for the database
	_, err = db.Exec("ALTER DATABASE zone_db CONFIGURE ZONE USING num_replicas = 3")
	require.NoError(t, err)

	// Query zone config
	rows, err := db.Query("SHOW ZONE CONFIGURATION FOR DATABASE zone_db")
	require.NoError(t, err)
	defer rows.Close()

	// Verify result columns
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(cols), 2)

	// Read results
	count := 0
	for rows.Next() {
		var target, configSQL string
		err = rows.Scan(&target, &configSQL)
		require.NoError(t, err)
		require.NotEmpty(t, target)
		require.NotEmpty(t, configSQL)
		count++
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, count, 1, "expected zone config data to be present")
}

// TestShowZoneConfigForTable tests zone config for tables
func TestShowZoneConfigForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create test database and table
	_, err := db.Exec("CREATE DATABASE table_zone_db")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE table_zone_db.test_table (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// Configure zone config for the table
	_, err = db.Exec("ALTER TABLE table_zone_db.test_table CONFIGURE ZONE USING num_replicas = 5")
	require.NoError(t, err)

	// Query zone config
	rows, err := db.Query("SHOW ZONE CONFIGURATION FOR TABLE table_zone_db.test_table")
	require.NoError(t, err)
	defer rows.Close()

	// Read results
	count := 0
	for rows.Next() {
		var target, configSQL string
		err = rows.Scan(&target, &configSQL)
		require.NoError(t, err)
		require.NotEmpty(t, target)
		require.NotEmpty(t, configSQL)
		count++
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, count, 1, "expected zone config data to be present")
}

// TestShowZoneConfigForRange tests zone config for ranges
func TestShowZoneConfigForRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Query zone config for the default range
	rows, err := db.Query("SHOW ZONE CONFIGURATION FOR RANGE default")
	require.NoError(t, err)
	defer rows.Close()

	// Read results
	count := 0
	for rows.Next() {
		var target, configSQL string
		err = rows.Scan(&target, &configSQL)
		require.NoError(t, err)
		require.NotEmpty(t, target)
		require.NotEmpty(t, configSQL)
		count++
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, count, 1, "expected zone config data to be present")
}

// TestShowZoneConfigNonExistent tests non-existent objects
func TestShowZoneConfigNonExistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Query zone config for a non-existent database
	_, err := db.Query("SHOW ZONE CONFIGURATION FOR DATABASE non_existent_db")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

// TestAscendZoneSpecifier tests the ascendZoneSpecifier function
func TestAscendZoneSpecifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test default zone
	zs1 := tree.ZoneSpecifier{NamedZone: "default"}
	result1 := ascendZoneSpecifier(zs1, keys.RootNamespaceID, keys.RootNamespaceID, nil)
	require.Equal(t, string(zonepb.DefaultZoneName), string(result1.NamedZone))

	// Test database zone
	zs2 := tree.ZoneSpecifier{
		Database: "test_db",
	}
	result2 := ascendZoneSpecifier(zs2, 50, 50, nil)
	require.Equal(t, "test_db", string(result2.Database))

	// Test table zone
	zs3 := tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableName("test_db", "test_table"),
		},
	}
	result3 := ascendZoneSpecifier(zs3, 51, 51, nil)
	require.Equal(t, "test_db", string(result3.TableOrIndex.Table.CatalogName))
	require.Equal(t, "test_table", string(result3.TableOrIndex.Table.TableName))
}

// TestZoneConfigToSQL tests the zoneConfigToSQL function
func TestZoneConfigToSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create zone config
	zone := &zonepb.ZoneConfig{
		NumReplicas:   func() *int32 { n := int32(3); return &n }(),
		RangeMinBytes: func() *int64 { n := int64(16777216); return &n }(),
		RangeMaxBytes: func() *int64 { n := int64(67108864); return &n }(),
		GC: &zonepb.GCPolicy{
			TTLSeconds: 90000,
		},
	}

	zs := &tree.ZoneSpecifier{
		Database: "test_db",
	}

	sql, err := zoneConfigToSQL(zs, zone)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.Contains(t, sql, "ALTER")
	require.Contains(t, sql, "CONFIGURE ZONE USING")
	require.Contains(t, sql, "num_replicas = 3")
	require.Contains(t, sql, "range_min_bytes = 16777216")
	require.Contains(t, sql, "range_max_bytes = 67108864")
	require.Contains(t, sql, "gc.ttlseconds = 90000")
}

// TestZoneConfigToSQLWithConstraints tests zone config with constraints
func TestZoneConfigToSQLWithConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create zone config with constraints
	zone := &zonepb.ZoneConfig{
		NumReplicas: func() *int32 { n := int32(5); return &n }(),
		Constraints: []zonepb.Constraints{
			{
				NumReplicas: 2,
				Constraints: []zonepb.Constraint{
					{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "us-east"},
				},
			},
		},
		InheritedConstraints: false,
	}

	zs := &tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableName("test_db", "test_table"),
		},
	}

	sql, err := zoneConfigToSQL(zs, zone)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.Contains(t, sql, "num_replicas = 5")
	require.Contains(t, sql, "constraints =")
}

// TestGenerateZoneConfigIntrospectionValues tests the generateZoneConfigIntrospectionValues function
func TestGenerateZoneConfigIntrospectionValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create zone config
	zone := &zonepb.ZoneConfig{
		NumReplicas: func() *int32 { n := int32(3); return &n }(),
	}

	zs := &tree.ZoneSpecifier{
		Database: "test_db",
	}

	values := make(tree.Datums, len(showZoneConfigColumns))
	err := generateZoneConfigIntrospectionValues(
		values,
		tree.NewDInt(tree.DInt(1)),
		tree.NewDInt(tree.DInt(0)),
		zs,
		zone,
		nil,
	)
	require.NoError(t, err)

	// Verify results
	require.NotNil(t, values[0])  // zone_id
	require.NotNil(t, values[1])  // subzone_id
	require.NotNil(t, values[2])  // target
	require.NotNil(t, values[8])  // raw_config_yaml
	require.NotNil(t, values[9])  // raw_config_sql
	require.NotNil(t, values[10]) // raw_config_protobuf
	require.NotNil(t, values[11]) // full_config_yaml
	require.NotNil(t, values[12]) // full_config_sql
}

// TestYamlMarshalFlow tests the yamlMarshalFlow function
func TestYamlMarshalFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test simple data structure
	data := []string{"a", "b", "c"}
	result, err := yamlMarshalFlow(data)
	require.NoError(t, err)
	require.NotEmpty(t, result)
	require.Contains(t, result, "a")
	require.Contains(t, result, "b")
	require.Contains(t, result, "c")
}

// TestWriteComma tests the writeComma function
func TestWriteComma(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := tree.NewFmtCtx(tree.FmtParsable)

	// First call, useComma = false
	writeComma(f, false)
	require.Equal(t, "", f.String())

	// Second call, useComma = true
	writeComma(f, true)
	require.Equal(t, ",\n", f.String())
}
