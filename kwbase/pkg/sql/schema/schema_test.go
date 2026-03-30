package schema_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGetForDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	dbID := sqlbase.ID(100)
	schemaID := sqlbase.ID(101)
	schemaName := "test_schema"

	// Write a mock schema entry
	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		schemaKey := sqlbase.NewSchemaKey(dbID, schemaName).Key()
		val := roachpb.Value{}
		val.SetInt(int64(schemaID))
		return txn.Put(ctx, schemaKey, &val)
	})
	}) 
	require.NoError(t, err)

	err = kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		schemas, err := schema.GetForDatabase(ctx, txn, dbID)
		require.NoError(t, err)

		// Should at least contain public schema and test_schema
		require.Equal(t, string(tree.PublicSchemaName), schemas[keys.PublicSchemaID])
		require.Equal(t, schemaName, schemas[schemaID])
		return nil
	})
	require.NoError(t, err)
}

func TestResolveNameByID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	dbID := sqlbase.ID(200)
	schemaID := sqlbase.ID(201)
	schemaName := "another_schema"

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		schemaKey := sqlbase.NewSchemaKey(dbID, schemaName).Key()
		val := roachpb.Value{}
		val.SetInt(int64(schemaID))
		return txn.Put(ctx, schemaKey, &val)
	})
	require.NoError(t, err)

	err = kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Test resolving public schema (fast-path)
		name, err := schema.ResolveNameByID(ctx, txn, dbID, keys.PublicSchemaID)
		require.NoError(t, err)
		require.Equal(t, string(tree.PublicSchemaName), name)

		// Test resolving existing custom schema
		name, err = schema.ResolveNameByID(ctx, txn, dbID, schemaID)
		require.NoError(t, err)
		require.Equal(t, schemaName, name)

		// Test resolving non-existent schema
		_, err = schema.ResolveNameByID(ctx, txn, dbID, sqlbase.ID(999))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to resolve schema id")

		return nil
	})
	require.NoError(t, err)
}
