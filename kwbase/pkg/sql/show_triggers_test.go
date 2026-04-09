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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestShowTriggersColumns tests the result column definitions
func TestShowTriggersColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify showTriggersColumns
	require.Equal(t, 6, len(showTriggersColumns))
	require.Equal(t, "trigger_name", showTriggersColumns[0].Name)
	require.Equal(t, types.String, showTriggersColumns[0].Typ)
	require.Equal(t, "trigger_action_time", showTriggersColumns[1].Name)
	require.Equal(t, types.String, showTriggersColumns[1].Typ)
	require.Equal(t, "trigger_event", showTriggersColumns[2].Name)
	require.Equal(t, types.String, showTriggersColumns[2].Typ)
	require.Equal(t, "trigger_order", showTriggersColumns[3].Name)
	require.Equal(t, types.Int, showTriggersColumns[3].Typ)
	require.Equal(t, "on_table", showTriggersColumns[4].Name)
	require.Equal(t, types.String, showTriggersColumns[4].Typ)
	require.Equal(t, "enabled", showTriggersColumns[5].Name)
	require.Equal(t, types.Bool, showTriggersColumns[5].Typ)
}

// TestShowTriggersColumnsTypes tests the data types of the result columns
func TestShowTriggersColumnsTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify the type family of each column
	require.Equal(t, types.String.Family(), showTriggersColumns[0].Typ.Family())
	require.Equal(t, types.String.Family(), showTriggersColumns[1].Typ.Family())
	require.Equal(t, types.String.Family(), showTriggersColumns[2].Typ.Family())
	require.Equal(t, types.Int.Family(), showTriggersColumns[3].Typ.Family())
	require.Equal(t, types.String.Family(), showTriggersColumns[4].Typ.Family())
	require.Equal(t, types.Bool.Family(), showTriggersColumns[5].Typ.Family())
}

// TestShowTriggersNode tests the ShowTriggers node
func TestShowTriggersNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a ShowTriggers node
	tableName, err := tree.NewUnresolvedObjectName(1, [3]string{"test_table"}, 0)
	require.NoError(t, err)
	n := &tree.ShowTriggers{Table: tableName}
	require.NotNil(t, n.Table)
}

// TestShowTriggersBasic tests the basic SHOW TRIGGERS functionality
func TestShowTriggersBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create test database and table
	_, err := db.Exec("CREATE DATABASE trigger_db")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE trigger_db.test_table (id INT PRIMARY KEY, name STRING)")
	require.NoError(t, err)

	// Create a trigger - use a simple INSERT statement as the trigger body
	_, err = db.Exec(`
		CREATE TRIGGER test_trigger
		BEFORE INSERT ON trigger_db.test_table
		FOR EACH ROW
		INSERT INTO trigger_db.test_table VALUES (1, 'trigger_test')
	`)
	require.NoError(t, err)

	// Query triggers
	rows, err := db.Query("SHOW TRIGGERS FROM trigger_db.test_table")
	require.NoError(t, err)
	defer rows.Close()

	// Verify result columns
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, 6, len(cols))
	require.Equal(t, "trigger_name", cols[0])
	require.Equal(t, "trigger_action_time", cols[1])
	require.Equal(t, "trigger_event", cols[2])
	require.Equal(t, "trigger_order", cols[3])
	require.Equal(t, "on_table", cols[4])
	require.Equal(t, "enabled", cols[5])

	// Read results
	count := 0
	for rows.Next() {
		var triggerName, actionTime, event, onTable string
		var triggerOrder int64
		var enabled bool
		err = rows.Scan(&triggerName, &actionTime, &event, &triggerOrder, &onTable, &enabled)
		require.NoError(t, err)
		require.Equal(t, "test_trigger", triggerName)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 1, count, "expected 1 trigger")
}

// TestShowTriggersNoTriggers tests a table with no triggers
func TestShowTriggersNoTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create test database and table
	_, err := db.Exec("CREATE DATABASE no_trigger_db")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE no_trigger_db.test_table (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// Query triggers
	rows, err := db.Query("SHOW TRIGGERS FROM no_trigger_db.test_table")
	require.NoError(t, err)
	defer rows.Close()

	// Should return no results
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 0, count, "expected no triggers")
}

// TestShowTriggersNonExistentTable tests a non-existent table
func TestShowTriggersNonExistentTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Query triggers for a non-existent table
	_, err := db.Query("SHOW TRIGGERS FROM non_existent_db.non_existent_table")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}
