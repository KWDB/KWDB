package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	t.Run("DropStreamIfExists", func(t *testing.T) {
		_, err := db.Exec(`DROP STREAM IF EXISTS non_existent_stream`)
		require.NoError(t, err)
	})

	t.Run("DropStreamNonExistent", func(t *testing.T) {
		_, err := db.Exec(`DROP STREAM non_existent_stream`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("DropStreamInvalidName", func(t *testing.T) {
		_, err := db.Exec(`DROP STREAM ''`)
		require.Error(t, err)
	})
}
