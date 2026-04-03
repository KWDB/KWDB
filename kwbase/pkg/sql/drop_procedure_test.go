package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropProcedureBasic", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE PROCEDURE test_drop_proc1()
			BEGIN
				SELECT 1;
			END
		`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP PROCEDURE test_drop_proc1`)
		require.NoError(t, err)
	})

	t.Run("DropProcedureIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP PROCEDURE IF EXISTS non_existent_proc`)
		require.NoError(t, err)
	})

	t.Run("DropProcedureNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP PROCEDURE non_existent_proc`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})
}
