package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropRole(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropRoleBasic", func(t *testing.T) {
		_, err := db.Exec(`CREATE ROLE test_role1`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP ROLE test_role1`)
		require.NoError(t, err)
	})

	t.Run("DropRoleIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP ROLE IF EXISTS non_existent_role`)
		require.NoError(t, err)
	})

	t.Run("DropRoleNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP ROLE non_existent_role`)
		require.Error(t, err)
	})

	t.Run("DropUserBasic", func(t *testing.T) {
		_, err := db.Exec(`CREATE USER test_user1`)
		require.NoError(t, err)

		_, err = db.Exec(`DROP USER test_user1`)
		require.NoError(t, err)
	})

	t.Run("DropUserIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP USER IF EXISTS non_existent_user`)
		require.NoError(t, err)
	})

	t.Run("DropUserNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP USER non_existent_user`)
		require.Error(t, err)
	})

	t.Run("DropSpecialUserRoot", func(t *testing.T) {
		_, err := db.Exec(`DROP USER root`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot drop")
	})

	t.Run("DropSpecialRoleAdmin", func(t *testing.T) {
		_, err := db.Exec(`DROP ROLE admin`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot drop")
	})
}
