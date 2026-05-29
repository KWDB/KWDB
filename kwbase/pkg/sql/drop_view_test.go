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

package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropView(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropViewBasic", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_drop_view_base (a INT, b INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_drop_view_v1 AS SELECT a FROM test_drop_view_base WHERE a > 0`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP VIEW test_drop_view_v1`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_drop_view_base`)
		require.NoError(t, err)
	})

	t.Run("DropViewIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP VIEW IF EXISTS non_existent_view`)
		require.NoError(t, err)
	})

	t.Run("DropViewNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP VIEW non_existent_view`)
		require.Error(t, err)
	})

	t.Run("DropViewIfExistsWithRealView", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_view_exists_base (a INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_view_exists_view AS SELECT a FROM test_view_exists_base`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP VIEW IF EXISTS test_view_exists_view`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_view_exists_base`)
		require.NoError(t, err)
	})

	t.Run("DropViewRestrict", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_restrict_base (a INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_restrict_view AS SELECT a FROM test_restrict_base`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_restrict_base RESTRICT`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "depends")

		_, err = db.Exec(`DROP VIEW test_restrict_view`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_restrict_base RESTRICT`)
		require.NoError(t, err)
	})

	t.Run("DropViewCascade", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_cascade_base (a INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_cascade_view1 AS SELECT a FROM test_cascade_base`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_cascade_view2 AS SELECT a FROM test_cascade_view1`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP VIEW test_cascade_view1 CASCADE`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_cascade_base`)
		require.NoError(t, err)
	})

	t.Run("DropViewMultiple", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_multi_base (a INT)`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_multi_view1 AS SELECT a FROM test_multi_base`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE VIEW test_multi_view2 AS SELECT a FROM test_multi_base`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP VIEW test_multi_view1, test_multi_view2`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP TABLE test_multi_base`)
		require.NoError(t, err)
	})
}
