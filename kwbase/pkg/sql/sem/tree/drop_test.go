package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropDatabaseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropDatabase
		expected string
	}{
		{
			name:     "basic drop database",
			node:     &DropDatabase{Name: "testdb"},
			expected: `DROP DATABASE testdb`,
		},
		{
			name: "drop database if exists cascade",
			node: &DropDatabase{
				Name:         "testdb",
				IfExists:     true,
				DropBehavior: DropCascade,
			},
			expected: `DROP DATABASE IF EXISTS testdb CASCADE`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropIndexFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropIndex
		expected string
	}{
		{
			name: "basic drop index",
			node: &DropIndex{
				IndexList: TableIndexNames{
					&TableIndexName{Table: func() TableName { return MakeTableName("db", "tbl") }(), Index: "idx1"},
				},
			},
			expected: `DROP INDEX db.public.tbl@idx1`,
		},
		{
			name: "drop index concurrently if exists restrict",
			node: &DropIndex{
				IndexList: TableIndexNames{
					&TableIndexName{Table: func() TableName { return MakeTableName("db", "tbl") }(), Index: "idx1"},
					&TableIndexName{Table: func() TableName { return MakeTableName("db", "tbl") }(), Index: "idx2"},
				},
				IfExists:     true,
				Concurrently: true,
				DropBehavior: DropRestrict,
			},
			expected: `DROP INDEX CONCURRENTLY IF EXISTS db.public.tbl@idx1, db.public.tbl@idx2 RESTRICT`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropTableFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropTable
		expected string
	}{
		{
			name: "basic drop table",
			node: &DropTable{
				Names: TableNames{func() TableName { return MakeTableName("db", "tbl") }()},
			},
			expected: `DROP TABLE db.public.tbl`,
		},
		{
			name: "drop multiple tables if exists cascade",
			node: &DropTable{
				Names:        TableNames{func() TableName { return MakeTableName("db", "tbl1") }(), func() TableName { return MakeTableName("db", "tbl2") }()},
				IfExists:     true,
				DropBehavior: DropCascade,
			},
			expected: `DROP TABLE IF EXISTS db.public.tbl1, db.public.tbl2 CASCADE`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropViewFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropView
		expected string
	}{
		{
			name: "basic drop view",
			node: &DropView{
				Names: TableNames{func() TableName { return MakeTableName("db", "v1") }()},
			},
			expected: `DROP VIEW db.public.v1`,
		},
		{
			name: "drop materialized view if exists",
			node: &DropView{
				Names:          TableNames{func() TableName { return MakeTableName("db", "mv1") }()},
				IfExists:       true,
				IsMaterialized: true,
			},
			expected: `DROP MATERIALIZED VIEW IF EXISTS db.public.mv1`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropSequenceFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropSequence
		expected string
	}{
		{
			name: "basic drop sequence",
			node: &DropSequence{
				Names: TableNames{func() TableName { return MakeTableName("db", "seq1") }()},
			},
			expected: `DROP SEQUENCE db.public.seq1`,
		},
		{
			name: "drop multiple sequences cascade",
			node: &DropSequence{
				Names:        TableNames{func() TableName { return MakeTableName("db", "seq1") }(), func() TableName { return MakeTableName("db", "seq2") }()},
				IfExists:     true,
				DropBehavior: DropCascade,
			},
			expected: `DROP SEQUENCE IF EXISTS db.public.seq1, db.public.seq2 CASCADE`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropRoleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropRole
		expected string
	}{
		{
			name: "drop role",
			node: &DropRole{
				Names:  Exprs{NewStrVal("role1")},
				IsRole: true,
			},
			expected: `DROP ROLE 'role1'`,
		},
		{
			name: "drop user if exists",
			node: &DropRole{
				Names:    Exprs{NewStrVal("user1"), NewStrVal("user2")},
				IsRole:   false,
				IfExists: true,
			},
			expected: `DROP USER IF EXISTS 'user1', 'user2'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropSchemaFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropSchema
		expected string
	}{
		{
			name: "basic drop schema",
			node: &DropSchema{
				Names: []string{"schema1"},
			},
			expected: `DROP SCHEMA schema1`,
		},
		{
			name: "drop multiple schemas if exists cascade",
			node: &DropSchema{
				Names:        []string{"schema1", "schema2"},
				IfExists:     true,
				DropBehavior: DropCascade,
			},
			expected: `DROP SCHEMA IF EXISTS schema1, schema2 CASCADE`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropFunctionFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropFunction
		expected string
	}{
		{
			name: "basic drop function",
			node: &DropFunction{
				Names: []string{"func1"},
			},
			expected: `DROP FUNCTION func1`,
		},
		{
			name: "drop multiple functions if exists",
			node: &DropFunction{
				Names:    []string{"func1", "func2"},
				IfExists: true,
			},
			expected: `DROP FUNCTION IF EXISTS func1, func2`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropProcedureFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropProcedure
		expected string
	}{
		{
			name: "basic drop procedure",
			node: &DropProcedure{
				Name: func() TableName { return MakeTableName("db", "proc1") }(),
			},
			expected: `DROP PROCEDURE db.public.proc1`,
		},
		{
			name: "drop procedure if exists",
			node: &DropProcedure{
				Name:     func() TableName { return MakeTableName("db", "proc1") }(),
				IfExists: true,
			},
			expected: `DROP PROCEDURE IF EXISTS db.public.proc1`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropTriggerFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropTrigger
		expected string
	}{
		{
			name: "basic drop trigger",
			node: &DropTrigger{
				Name:  "trig1",
				Table: func() TableName { return MakeTableName("db", "tbl") }(),
			},
			expected: `DROP TRIGGER trig1 ON db.public.tbl`,
		},
		{
			name: "drop trigger if exists",
			node: &DropTrigger{
				Name:     "trig1",
				Table:    func() TableName { return MakeTableName("db", "tbl") }(),
				IfExists: true,
			},
			expected: `DROP TRIGGER IF EXISTS trig1 ON db.public.tbl`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropAuditFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropAudit
		expected string
	}{
		{
			name: "basic drop audit",
			node: &DropAudit{
				Names: NameList{"aud1"},
			},
			expected: `DROP AUDIT aud1`,
		},
		{
			name: "drop multiple audits if exists",
			node: &DropAudit{
				Names:    NameList{"aud1", "aud2"},
				IfExists: true,
			},
			expected: `DROP AUDIT IF EXISTS aud1, aud2`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestDropStreamFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *DropStream
		expected string
	}{
		{
			name: "basic drop stream",
			node: &DropStream{
				StreamName: "stream1",
			},
			expected: `DROP STREAM stream1`,
		},
		{
			name: "drop stream if exists",
			node: &DropStream{
				StreamName: "stream1",
				IfExists:   true,
			},
			expected: `DROP STREAM IF EXISTS stream1`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}
