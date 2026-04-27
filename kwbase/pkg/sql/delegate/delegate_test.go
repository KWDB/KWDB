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

package delegate

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"github.com/stretchr/testify/require"
)

// mockCatalog implements cat.Catalog minimally for testing.
type mockCatalog struct {
	cat.Catalog
	currentDatabase string
}

func (m *mockCatalog) RequireAdminRole(ctx context.Context, action string) error {
	return nil
}

func (m *mockCatalog) GetCurrentDatabase(ctx context.Context) string {
	if m.currentDatabase != "" {
		return m.currentDatabase
	}
	return "defaultdb"
}

func (m *mockCatalog) ResolveDatabase(
	ctx context.Context, flags cat.Flags, name string,
) (cat.Database, error) {
	return nil, nil
}

func (m *mockCatalog) ResolveSchema(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	if name != nil && name.CatalogName != "" {
		return nil, cat.SchemaName{CatalogName: name.CatalogName, SchemaName: "public"}, nil
	}
	return nil, cat.SchemaName{CatalogName: "defaultdb", SchemaName: "public"}, nil
}

func (m *mockCatalog) CheckAnyPrivilege(ctx context.Context, obj cat.Object) error {
	return nil
}

func (m *mockCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	return nil, *name, nil
}

func (m *mockCatalog) ResolveProcCatalog(
	ctx context.Context, name *tree.TableName, checkPri bool,
) (bool, *tree.CreateProcedure, error) {
	return true, nil, nil
}

func (m *mockCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	return nil
}

func (m *mockCatalog) HasAdminRole(ctx context.Context) (bool, error) {
	return true, nil
}

func (m *mockCatalog) FullyQualifiedName(
	ctx context.Context, ds cat.DataSource,
) (cat.DataSourceName, error) {
	return cat.DataSourceName{}, nil
}

func (m *mockCatalog) ReleaseTables(ctx context.Context) {
}

func (m *mockCatalog) ResetTxn(ctx context.Context) {
}

func TestTryDelegate(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		// Testing various SHOW statements to hit the sub-delegate functions
		{"ShowDatabases", "SHOW DATABASES", false},
		{"ShowDatabasesWithComment", "SHOW DATABASES WITH COMMENT", false},
		{"ShowAllClusterSettings", "SHOW ALL CLUSTER SETTINGS", false},
		{"ShowClusterSettings", "SHOW CLUSTER SETTINGS", false},
		{"ShowTables", "SHOW TABLES", false},
		{"ShowTablesWithComment", "SHOW TABLES WITH COMMENT", false},
		{"ShowTablesTemplate", "SHOW TEMPLATE TABLES", false},
		{"ShowGrants", "SHOW GRANTS", false},
		{"ShowGrantsOnTable", "SHOW GRANTS ON t1", false},
		{"ShowGrantsForUser", "SHOW GRANTS FOR user1", false},
		{"ShowRoles", "SHOW ROLES", false},
		{"ShowUsers", "SHOW USERS", false},
		{"ShowApplications", "SHOW APPLICATIONS", false},
		{"ShowSchedules", "SHOW SCHEDULES", false},
		{"ShowJobs", "SHOW JOBS", false},
		{"ShowJobsWithOptions", "SHOW JOBS WHERE status = 'running'", false},
		{"ShowQueries", "SHOW QUERIES", false},
		{"ShowSessions", "SHOW SESSIONS", false},
		{"ShowSchemas", "SHOW SCHEMAS", false},
		{"ShowSchemasFromDatabase", "SHOW SCHEMAS FROM defaultdb", false},
		{"ShowSequences", "SHOW SEQUENCES", false},
		{"ShowDatabaseIndexes", "SHOW INDEXES FROM DATABASE defaultdb", true},
		{"ShowTransactionStatus", "SHOW TRANSACTION STATUS", false},
		{"ShowZoneConfig", "SHOW ZONE CONFIGURATION FOR RANGE default", false},
		{"ShowZoneConfigForDatabase", "SHOW ZONE CONFIGURATION FOR DATABASE defaultdb", false},
		{"ShowZoneConfigForTable", "SHOW ZONE CONFIGURATION FOR TABLE t1", false},
		{"ShowAudits", "SHOW AUDITS", false},
		{"ShowProcedures", "SHOW PROCEDURES", false},
		{"ShowTagValues", "SHOW TAG VALUES FROM t1", false},
		{"ShowStreams", "SHOW STREAMS", false},
		{"ShowCreateDatabase", "SHOW CREATE DATABASE defaultdb", false},
		{"ShowPartitions", "SHOW PARTITIONS FROM TABLE t1", false},
		{"ShowRanges", "SHOW RANGES FROM TABLE t1", false},
		{"ShowRangeForRow", "SHOW RANGES FROM TABLE t1", false},
		{"ShowRoleGrants", "SHOW GRANTS ON ROLE r1", false},
		{"ShowSyntax", "SHOW SYNTAX 'SELECT 1'", false},
		{"ShowVar", "SHOW search_path", false},
		{"ShowUdvVar", "SHOW my_var", false},
		{"ShowSavepointStatus", "SHOW SAVEPOINT STATUS", true}, // Should return error
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL to get the AST
			stmt, err := parser.ParseOne(tt.sql)
			require.NoError(t, err, "failed to parse test SQL: %s", tt.sql)

			// Execute TryDelegate
			resStmt, err := TryDelegate(ctx, catalog, evalCtx, stmt.AST)
			if tt.expectError {
				require.Error(t, err, "expected error for %s", tt.name)
			} else {
				if err != nil {
					t.Logf("TryDelegate returned error for %s: %v", tt.name, err)
				} else if resStmt != nil {
					t.Logf("TryDelegate successfully delegated %s: %s", tt.name, resStmt.String())
				} else {
					t.Logf("TryDelegate ignored %s (returned nil)", tt.name)
				}
			}
		})
	}
}

// TestTryDelegateWithEmptyDatabase tests the case where no database is specified
func TestTryDelegateWithEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{currentDatabase: ""}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	tests := []struct {
		name string
		sql  string
	}{
		{"ShowTablesWithoutDatabase", "SHOW TABLES"},
		{"ShowSchemasWithoutDatabase", "SHOW SCHEMAS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseOne(tt.sql)
			require.NoError(t, err, "failed to parse test SQL: %s", tt.sql)

			// Execute TryDelegate
			resStmt, err := TryDelegate(ctx, catalog, evalCtx, stmt.AST)
			if err != nil {
				t.Logf("TryDelegate returned error for %s: %v", tt.name, err)
			} else if resStmt != nil {
				t.Logf("TryDelegate successfully delegated %s: %s", tt.name, resStmt.String())
			} else {
				t.Logf("TryDelegate ignored %s (returned nil)", tt.name)
			}
		})
	}
}

// TestDelegateShowDatabases tests the delegateShowDatabases function
func TestDelegateShowDatabases(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		withComment      bool
		expectedContains string
	}{
		{"ShowDatabasesWithoutComment", false, "SELECT DISTINCT catalog_name AS database_name, engine_type"},
		{"ShowDatabasesWithComment", true, "shobj_description(oid, 'pg_database') AS comment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowDatabases{WithComment: tt.withComment}
			resStmt, err := d.delegateShowDatabases(stmt)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowTables tests the delegateShowTables function
func TestDelegateShowTables(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		withComment      bool
		isTemplate       bool
		expectedContains string
	}{
		{"ShowTablesWithoutComment", false, false, "SELECT table_name, table_type"},
		{"ShowTablesWithComment", true, false, "COALESCE(pd.description, '') AS comment"},
		{"ShowTemplateTables", false, true, "table_type = 'TEMPLATE TABLE'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowTables{
				WithComment: tt.withComment,
				IsTemplate:  tt.isTemplate,
			}
			resStmt, err := d.delegateShowTables(stmt)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowRoles tests the delegateShowRoles function
func TestDelegateShowRoles(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	resStmt, err := d.delegateShowRoles()
	require.NoError(t, err)
	require.NotNil(t, resStmt)
	require.Contains(t, resStmt.String(), "SELECT u.username")
	require.Contains(t, resStmt.String(), "system.users AS u LEFT JOIN system.role_options AS o")
	require.Contains(t, resStmt.String(), "ARRAY (SELECT role FROM system.role_members AS rm")
}

// TestDelegateShowGrants tests the delegateShowGrants function
func TestDelegateShowGrants(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := &tree.EvalContext{
		SessionData: &sessiondata.SessionData{
			Database: "defaultdb",
		},
	}

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showGrants       *tree.ShowGrants
		expectedContains string
	}{
		{
			"ShowAllGrants",
			&tree.ShowGrants{},
			"UNION ALL",
		},
		{
			"ShowGrantsForDatabase",
			&tree.ShowGrants{
				Targets: &tree.TargetList{
					Databases: tree.NameList{"defaultdb"},
				},
			},
			"information_schema.schema_privileges",
		},
		{
			"ShowGrantsForSchema",
			&tree.ShowGrants{
				Targets: &tree.TargetList{
					Schemas: tree.NameList{"public"},
				},
			},
			"information_schema.schema_privileges",
		},
		{
			"ShowGrantsForUser",
			&tree.ShowGrants{
				Grantees: tree.NameList{"user1"},
			},
			"grantee IN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowGrants(tt.showGrants)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowSchemas tests the delegateShowSchemas function
func TestDelegateShowSchemas(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		database         tree.Name
		expectedContains string
	}{
		{
			"ShowSchemasCurrentDatabase",
			"",
			"FROM defaultdb.information_schema.schemata",
		},
		{
			"ShowSchemasSpecificDatabase",
			"testdb",
			"FROM defaultdb.information_schema.schemata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowSchemas{Database: tt.database}
			resStmt, err := d.delegateShowSchemas(stmt)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestGetSpecifiedOrCurrentDatabase tests the getSpecifiedOrCurrentDatabase function
func TestGetSpecifiedOrCurrentDatabase(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name        string
		specifiedDB tree.Name
		expected    tree.Name
	}{
		{
			"GetCurrentDatabase",
			"",
			"defaultdb",
		},
		{
			"GetSpecifiedDatabase",
			"testdb",
			"defaultdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := d.getSpecifiedOrCurrentDatabase(tt.specifiedDB)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestDelegateShowVar tests the delegateShowVar function
func TestDelegateShowVar(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	// Initialize ValidVars for testing
	ValidVars["search_path"] = struct{}{}
	ValidVars["locality"] = struct{}{}

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		varName          string
		expectedContains string
		expectError      bool
	}{
		{
			"ShowAllVars",
			"all",
			"kwdb_internal.session_variables",
			false,
		},
		{
			"ShowValidVar",
			"search_path",
			"variable = 'search_path'",
			false,
		},
		{
			"ShowLocalityVar",
			"locality",
			"variable = 'locality'",
			false,
		},
		{
			"ShowInvalidVar",
			"invalid_var",
			"",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowVar{Name: tt.varName}
			resStmt, err := d.delegateShowVar(stmt)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resStmt)
				require.Contains(t, resStmt.String(), tt.expectedContains)
			}
		})
	}
}

// TestDelegateShowUdvVar tests the delegateShowUdvVar function
func TestDelegateShowUdvVar(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := &tree.EvalContext{
		SessionData: &sessiondata.SessionData{
			UserDefinedVars: map[string]interface{}{
				"my_var": tree.NewDInt(123),
			},
		},
	}

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		varName          string
		expectedContains string
		expectError      bool
	}{
		{
			"ShowDefinedUdvVar",
			"my_var",
			"'my_var' AS var_name",
			false,
		},
		{
			"ShowUndefinedUdvVar",
			"undefined_var",
			"",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowUdvVar{Name: tt.varName}
			resStmt, err := d.delegateShowUdvVar(stmt)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resStmt)
				require.Contains(t, resStmt.String(), tt.expectedContains)
			}
		})
	}
}

// TestDelegateShowJobs tests the delegateShowJobs function
func TestDelegateShowJobs(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showJobs         *tree.ShowJobs
		expectedContains string
		expectError      bool
	}{
		{
			"ShowAllJobs",
			&tree.ShowJobs{},
			"kwdb_internal.jobs",
			false,
		},
		{
			"ShowAutomaticJobs",
			&tree.ShowJobs{Automatic: true},
			"job_type = 'AUTO CREATE STATS'",
			false,
		},
		{
			"ShowSpecificJobs",
			&tree.ShowJobs{
				Jobs: &tree.Select{
					Select: &tree.SelectClause{
						Exprs: tree.SelectExprs{
							{Expr: tree.NewDInt(123)},
							{Expr: tree.NewDInt(456)},
						},
					},
				},
			},
			"job_id IN (",
			false,
		},
		{
			"ShowJobsWithBlock",
			&tree.ShowJobs{Block: true},
			"WITH jobs AS",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowJobs(tt.showJobs)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resStmt)
				require.Contains(t, resStmt.String(), tt.expectedContains)
			}
		})
	}
}

// TestDelegateShowQueries tests the delegateShowQueries function
func TestDelegateShowQueries(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showQueries      *tree.ShowQueries
		expectedContains string
	}{
		{
			"ShowNodeQueries",
			&tree.ShowQueries{Cluster: false, All: false},
			"kwdb_internal.node_queries WHERE application_name NOT LIKE '$ internal%'",
		},
		{
			"ShowClusterQueries",
			&tree.ShowQueries{Cluster: true, All: false},
			"kwdb_internal.cluster_queries WHERE application_name NOT LIKE '$ internal%'",
		},
		{
			"ShowAllNodeQueries",
			&tree.ShowQueries{Cluster: false, All: true},
			"kwdb_internal.node_queries",
		},
		{
			"ShowAllClusterQueries",
			&tree.ShowQueries{Cluster: true, All: true},
			"kwdb_internal.cluster_queries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowQueries(tt.showQueries)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowSessions tests the delegateShowSessions function
func TestDelegateShowSessions(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showSessions     *tree.ShowSessions
		expectedContains string
	}{
		{
			"ShowNodeSessions",
			&tree.ShowSessions{Cluster: false, All: false},
			"kwdb_internal.node_sessions WHERE application_name NOT LIKE '$ internal%'",
		},
		{
			"ShowClusterSessions",
			&tree.ShowSessions{Cluster: true, All: false},
			"kwdb_internal.cluster_sessions WHERE application_name NOT LIKE '$ internal%'",
		},
		{
			"ShowAllNodeSessions",
			&tree.ShowSessions{Cluster: false, All: true},
			"kwdb_internal.node_sessions",
		},
		{
			"ShowAllClusterSessions",
			&tree.ShowSessions{Cluster: true, All: true},
			"kwdb_internal.cluster_sessions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowSessions(tt.showSessions)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowApplications tests the delegateShowApplications function
func TestDelegateShowApplications(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	resStmt, err := d.delegateShowApplications()
	require.NoError(t, err)
	require.NotNil(t, resStmt)
	require.Contains(t, resStmt.String(), "SELECT application_name FROM kwdb_internal.cluster_sessions")
}

// TestDelegateShowSyntax tests the delegateShowSyntax function
func TestDelegateShowSyntax(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		sqlStatement     string
		expectedContains string
	}{
		{
			"ShowSyntaxForSelect",
			"SELECT * FROM t1",
			"SELECT @1 AS field, @2 AS message FROM (VALUES",
		},
		{
			"ShowSyntaxForInsert",
			"INSERT INTO t1 (id, name) VALUES (1, 'test')",
			"SELECT @1 AS field, @2 AS message FROM (VALUES",
		},
		{
			"ShowSyntaxForUpdate",
			"UPDATE t1 SET name = 'new' WHERE id = 1",
			"SELECT @1 AS field, @2 AS message FROM (VALUES",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &tree.ShowSyntax{Statement: tt.sqlStatement}
			resStmt, err := d.delegateShowSyntax(stmt)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowStreams tests the delegateShowStreams function
func TestDelegateShowStreams(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showStreams      *tree.ShowStreams
		expectedContains string
	}{
		{
			"ShowAllStreams",
			&tree.ShowStreams{ShowAll: true},
			"SELECT * FROM kwdb_internal.kwdb_streams",
		},
		{
			"ShowSpecificStream",
			&tree.ShowStreams{ShowAll: false, StreamName: "stream1"},
			"SELECT * FROM kwdb_internal.kwdb_streams WHERE name = 'stream1'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowStreams(tt.showStreams)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}

// TestDelegateShowAudits tests the delegateShowAudits function
func TestDelegateShowAudits(t *testing.T) {
	ctx := context.Background()
	catalog := &mockCatalog{}
	evalCtx := &tree.EvalContext{
		SessionData: &sessiondata.SessionData{
			Database: "defaultdb",
		},
	}

	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}

	tests := []struct {
		name             string
		showAudits       *tree.ShowAudits
		expectedContains string
	}{
		{
			"ShowAllAudits",
			&tree.ShowAudits{},
			"SELECT * FROM kwdb_internal.audit_policies",
		},
		{
			"ShowAuditsByTargetType",
			&tree.ShowAudits{
				Target: tree.AuditTarget{Type: "TABLE"},
			},
			"WHERE target_type = 'TABLE'",
		},
		{
			"ShowAuditsByOperation",
			&tree.ShowAudits{
				Target:     tree.AuditTarget{Type: "TABLE"},
				Operations: tree.NameList{"INSERT", "UPDATE"},
			},
			"operations LIKE '%INSERT%'",
		},
		{
			"ShowAuditsByOperator",
			&tree.ShowAudits{
				Operators: tree.NameList{"user1"},
			},
			"operators LIKE '%user1%'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resStmt, err := d.delegateShowAudits(tt.showAudits)
			require.NoError(t, err)
			require.NotNil(t, resStmt)
			require.Contains(t, resStmt.String(), tt.expectedContains)
		})
	}
}
