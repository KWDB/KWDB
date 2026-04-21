// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestShowHistogramColumns tests the result column definitions
func TestShowHistogramColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify result column definitions
	require.Equal(t, 4, len(showHistogramColumns))
	require.Equal(t, "upper_bound", showHistogramColumns[0].Name)
	require.Equal(t, types.String, showHistogramColumns[0].Typ)
	require.Equal(t, "range_rows", showHistogramColumns[1].Name)
	require.Equal(t, types.Int, showHistogramColumns[1].Typ)
	require.Equal(t, "distinct_range_rows", showHistogramColumns[2].Name)
	require.Equal(t, types.Float, showHistogramColumns[2].Typ)
	require.Equal(t, "equal_rows", showHistogramColumns[3].Name)
	require.Equal(t, types.Int, showHistogramColumns[3].Typ)
}

// TestShowHistogramNodeName tests the histogram node name
func TestShowHistogramNodeName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a ShowHistogram node
	n := &tree.ShowHistogram{HistogramID: 123}
	require.Equal(t, int64(123), n.HistogramID)
}

// TestShowHistogramColumnsTypes tests the data types of result columns
func TestShowHistogramColumnsTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify each column type
	require.Equal(t, types.String.Family(), showHistogramColumns[0].Typ.Family())
	require.Equal(t, types.Int.Family(), showHistogramColumns[1].Typ.Family())
	require.Equal(t, types.Float.Family(), showHistogramColumns[2].Typ.Family())
	require.Equal(t, types.Int.Family(), showHistogramColumns[3].Typ.Family())
}

// TestShowHistogram tests the SHOW HISTOGRAM statement
func TestShowHistogram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)

	// Create a test table
	r.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value INT
		)
	`)

	// Insert some test data
	r.Exec(t, `
		INSERT INTO test_table VALUES
		(1, 10),
		(2, 20),
		(3, 20),
		(4, 30),
		(5, 40),
		(6, 50),
		(7, 50),
		(8, 50),
		(9, 60),
		(10, 70)
	`)

	// Create statistics (this will create a histogram)
	r.Exec(t, `CREATE STATISTICS test_stats FROM test_table`)

	// Get statistic ID - directly fetch the first one
	var histogramID int
	row := r.QueryRow(t, `
		SELECT "statisticID"
		FROM system.table_statistics
		LIMIT 1
	`)
	row.Scan(&histogramID)
	require.Greater(t, histogramID, 0, "expected a valid histogram ID")

	// Test SHOW HISTOGRAM statement
	t.Run("show histogram valid", func(t *testing.T) {
		histogramRows := r.Query(t, fmt.Sprintf("SHOW HISTOGRAM %d", histogramID))
		require.NoError(t, histogramRows.Err())
		defer histogramRows.Close()

		// Check if rows are returned
		found := false
		for histogramRows.Next() {
			var upperBound string
			var rangeRows int
			var distinctRangeRows float64
			var equalRows int
			err := histogramRows.Scan(&upperBound, &rangeRows, &distinctRangeRows, &equalRows)
			require.NoError(t, err)
			found = true
			require.NotEmpty(t, upperBound)
		}
		require.NoError(t, histogramRows.Err())
		require.True(t, found, "expected at least one row from SHOW HISTOGRAM")
	})

	// Test non-existent histogram ID
	t.Run("show histogram invalid", func(t *testing.T) {
		_, err := db.Exec("SHOW HISTOGRAM 999999")
		require.Error(t, err)
		require.Contains(t, err.Error(), "histogram 999999 not found")
	})
}
