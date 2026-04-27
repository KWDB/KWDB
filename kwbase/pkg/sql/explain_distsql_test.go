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

func TestExplainDistSQLNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("ExplainDistSqlBasic1", func(t *testing.T) {
		_, err := db.Exec(`
			EXPLAIN (DISTSQL) SELECT 1;
		`)
		require.NoError(t, err)
	})

	t.Run("ExplainDistSqlBasic2", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE t1(e1 int, e2 string, e3 timestamptz)
		`)
		require.NoError(t, err)

		_, err = db.Exec(`EXPLAIN (DISTSQL) SELECT * FROM t1`)
		require.NoError(t, err)

		_, err = db.Exec(`EXPLAIN ANALYZE SELECT * FROM t1`)
		require.NoError(t, err)
	})

	t.Run("ExplainDistSqlBasic3", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE t2(e1 int, e2 string, e3 timestamptz)
		`)
		require.NoError(t, err)

		_, err1 := db.Exec(`
			CREATE TABLE t3(e1 int, e2 string, e3 timestamptz)
		`)
		require.NoError(t, err1)

		_, err = db.Exec(`EXPLAIN (DISTSQL) SELECT * FROM t2 WHERE t2.e1 > (SELECT max(e1) FROM t3)`)
		require.NoError(t, err)

		_, err = db.Exec(`EXPLAIN ANALYZE SELECT * FROM t2 WHERE t2.e1 > (SELECT max(e1) FROM t3)`)
		require.NoError(t, err)

	})
}
