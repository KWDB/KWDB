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

func TestDropSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropScheduleIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP SCHEDULE IF EXISTS non_existent_schedule`)
		require.NoError(t, err)
	})

	t.Run("DropScheduleNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP SCHEDULE non_existent_schedule`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("DropScheduleInvalidName", func(t *testing.T) {
		_, err := db.Exec(`DROP SCHEDULE ''`)
		require.Error(t, err)
	})
}
