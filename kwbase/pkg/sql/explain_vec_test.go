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

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExplainVec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("ExplainVec", func(t *testing.T) {
		_, err := db.Exec(`
			SET CLUSTER SETTING sql.defaults.vectorize_row_count_threshold=0
		`)
		require.NoError(t, err)

		_, err = db.Exec(`
			CREATE TABLE t1(e1 int, e2 string, e3 timestamptz)
		`)
		require.NoError(t, err)

		_, err = db.Exec(`
			CREATE TABLE t2(e4 int, e5 string, e6 timestamptz)
		`)
		require.NoError(t, err)

		_, err1 := db.Exec(`
			INSERT INTO t1 VALUES (1, 'abc', '2000-1-1')
		`)
		require.NoError(t, err1)

		_, err2 := db.Exec(`
			INSERT INTO t2 VALUES (2, 'edf', '2000-1-2')
		`)
		require.NoError(t, err2)

		_, err3 := db.Exec(`EXPLAIN SELECT max(e1) FROM t1`)
		require.NoError(t, err3)

		_, err4 := db.Exec(`EXPLAIN SELECT max(e1) FROM t1 join t2 on t1.e1=t2.e4`)
		require.NoError(t, err4)

		_, err5 := db.Exec(`EXPLAIN SELECT max(e1) FROM t1 WHERE t1.e1 < (SELECT max(e4) FROM t2)`)
		require.NoError(t, err5)
	})
}
