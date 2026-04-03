package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropFunctionNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP FUNCTION non_existent_func`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown function")
	})

	t.Run("DropFunctionInvalidName", func(t *testing.T) {
		_, err := db.Exec(`DROP FUNCTION ''`)
		require.Error(t, err)
	})
}
