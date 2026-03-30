package delegate

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type mockSchema struct {
	dbType tree.EngineType
}

func (m *mockSchema) Equals(other cat.Object) bool       { return false }
func (m *mockSchema) ID() cat.StableID                   { return 1 }
func (m *mockSchema) PostgresDescriptorID() cat.StableID { return 1 }
func (m *mockSchema) Name() *cat.SchemaName              { return &cat.SchemaName{SchemaName: "mock_schema"} }
func (m *mockSchema) GetDatabaseType() tree.EngineType   { return m.dbType }
func (m *mockSchema) GetDataSourceNames(ctx context.Context) ([]cat.DataSourceName, error) {
	return nil, nil
}
func (m *mockSchema) IsZonemapsFilter() bool           { return false }
func (m *mockSchema) TableCount() int                  { return 0 }
func (m *mockSchema) Table(int) cat.Table              { return nil }
func (m *mockSchema) TableByID(cat.StableID) cat.Table { return nil }

type mockCatalogForSchema struct {
	mockCatalog
	schema *mockSchema
}

func (m *mockCatalogForSchema) ResolveSchema(ctx context.Context, flags cat.Flags, name *cat.SchemaName) (cat.Schema, cat.SchemaName, error) {
	if m.schema == nil {
		return nil, *name, nil
	}
	return m.schema, *name, nil
}

func TestCheckDBSupportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test CheckDBSupportShow
	d1 := &delegator{
		ctx:     context.Background(),
		catalog: &mockCatalogForSchema{schema: &mockSchema{dbType: tree.EngineTypeTimeseries}},
	}
	err := CheckDBSupportShow(d1, "testdb", &tree.ShowIndexes{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "show indexes is not supported")

	// Test CheckTsDBSupportShow
	d2 := &delegator{
		ctx:     context.Background(),
		catalog: &mockCatalogForSchema{schema: &mockSchema{dbType: tree.EngineTypeRelational}},
	}
	err = CheckTsDBSupportShow(d2, "testdb", &tree.ShowPartitions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported in relation database")

	// Test TSUnsupportedShowError
	err = TSUnsupportedShowError(&tree.ShowDatabaseIndexes{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "show database indexes is not supported")

	err = TSUnsupportedShowError(&tree.ShowRanges{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "show ranges is not supported")

	err = TSUnsupportedShowError(&tree.ShowZoneConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "show zone config is not supported")
}
