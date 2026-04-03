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

package sql

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestIsCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		stmt     tree.Statement
		expected bool
	}{
		{
			name:     "nil statement",
			stmt:     nil,
			expected: false,
		},
		{
			name:     "select statement",
			stmt:     &tree.Select{},
			expected: false,
		},
		{
			name:     "commit transaction",
			stmt:     &tree.CommitTransaction{},
			expected: true,
		},
		{
			name:     "insert statement",
			stmt:     &tree.Insert{},
			expected: false,
		},
		{
			name:     "delete statement",
			stmt:     &tree.Delete{},
			expected: false,
		},
		{
			name:     "update statement",
			stmt:     &tree.Update{},
			expected: false,
		},
		{
			name:     "rollback transaction",
			stmt:     &tree.RollbackTransaction{},
			expected: false,
		},
		{
			name:     "create database",
			stmt:     &tree.CreateDatabase{},
			expected: false,
		},
		{
			name:     "drop table",
			stmt:     &tree.DropTable{},
			expected: false,
		},
		{
			name:     "alter table",
			stmt:     &tree.AlterTable{},
			expected: false,
		},
		{
			name:     "create index",
			stmt:     &tree.CreateIndex{},
			expected: false,
		},
		{
			name:     "drop index",
			stmt:     &tree.DropIndex{},
			expected: false,
		},
		{
			name:     "set session",
			stmt:     &tree.SetVar{},
			expected: false,
		},
		{
			name:     "scrub database",
			stmt:     &tree.Scrub{},
			expected: false,
		},
		{
			name:     "begin transaction",
			stmt:     &tree.BeginTransaction{},
			expected: false,
		},
		{
			name:     "savepoint",
			stmt:     &tree.Savepoint{},
			expected: false,
		},
		{
			name:     "release savepoint",
			stmt:     &tree.ReleaseSavepoint{},
			expected: false,
		},
		{
			name:     "rollback to savepoint",
			stmt:     &tree.RollbackToSavepoint{},
			expected: false,
		},
		{
			name:     "grant",
			stmt:     &tree.Grant{},
			expected: false,
		},
		{
			name:     "revoke",
			stmt:     &tree.Revoke{},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isCommit(tc.stmt)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestStmtHasNoData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("nil statement", func(t *testing.T) {
		result := stmtHasNoData(nil)
		require.True(t, result)
	})

	t.Run("commit transaction returns no data", func(t *testing.T) {
		result := stmtHasNoData(&tree.CommitTransaction{})
		require.True(t, result)
	})

	t.Run("nil returns true", func(t *testing.T) {
		require.True(t, stmtHasNoData(nil))
	})
}

func TestErrIsRetriable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("retriable error", func(t *testing.T) {
		retriableErr := roachpb.NewTransactionRetryWithProtoRefreshError(
			"test txn", uuid.MakeV4(), roachpb.Transaction{},
		)
		require.True(t, errIsRetriable(retriableErr))
	})

	t.Run("non-retriable error", func(t *testing.T) {
		nonRetriableErr := fmt.Errorf("non retriable error")
		require.False(t, errIsRetriable(nonRetriableErr))
	})

	t.Run("wrapped retriable error", func(t *testing.T) {
		retriableErr := roachpb.NewTransactionRetryWithProtoRefreshError(
			"test txn", uuid.MakeV4(), roachpb.Transaction{},
		)
		wrappedErr := fmt.Errorf("wrapped: %w", retriableErr)
		require.True(t, errIsRetriable(wrappedErr))
	})

	t.Run("pq error is not retriable", func(t *testing.T) {
		pqErr := pq.Error{Code: "42P01"}
		require.False(t, errIsRetriable(&pqErr))
	})

	t.Run("nil error", func(t *testing.T) {
		require.False(t, errIsRetriable(nil))
	})
}

func TestStatementContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("withStatement and statementFromCtx", func(t *testing.T) {
		ctx := context.Background()

		result := statementFromCtx(ctx)
		require.Nil(t, result)

		stmt := &tree.Select{}
		ctxWithStmt := withStatement(ctx, stmt)

		result = statementFromCtx(ctxWithStmt)
		require.Equal(t, stmt, result)
	})

	t.Run("withStatement nil statement", func(t *testing.T) {
		ctx := context.Background()
		ctxWithNilStmt := withStatement(ctx, nil)
		result := statementFromCtx(ctxWithNilStmt)
		require.Nil(t, result)
	})
}

func TestCtxHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("ctx returns connCtx when sessionTracingCtx is nil", func(t *testing.T) {
		connCtx := context.Background()
		ch := &ctxHolder{connCtx: connCtx}
		require.Equal(t, connCtx, ch.ctx())
	})

	t.Run("ctx returns sessionTracingCtx when set", func(t *testing.T) {
		type ctxKey string
		connCtx := context.Background()
		sessionCtx := context.WithValue(context.Background(), ctxKey("key"), "value")
		ch := &ctxHolder{
			connCtx:           connCtx,
			sessionTracingCtx: sessionCtx,
		}
		require.Equal(t, sessionCtx, ch.ctx())
	})

	t.Run("hijack and unhijack", func(t *testing.T) {
		type ctxKey string
		connCtx := context.Background()
		ch := &ctxHolder{connCtx: connCtx}

		sessionCtx := context.WithValue(context.Background(), ctxKey("key"), "value")
		ch.hijack(sessionCtx)
		require.Equal(t, sessionCtx, ch.sessionTracingCtx)

		ch.unhijack()
		require.Nil(t, ch.sessionTracingCtx)
	})

	t.Run("hijack panic when already hijacked", func(t *testing.T) {
		type ctxKey string
		connCtx := context.Background()
		ch := &ctxHolder{connCtx: connCtx}

		sessionCtx := context.WithValue(context.Background(), ctxKey("key"), "value")
		ch.hijack(sessionCtx)

		require.Panics(t, func() {
			ch.hijack(sessionCtx)
		})
	})

	t.Run("unhijack panic when not hijacked", func(t *testing.T) {
		connCtx := context.Background()
		ch := &ctxHolder{connCtx: connCtx}

		require.Panics(t, func() {
			ch.unhijack()
		})
	})
}

func TestPrepStmtNamespace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("String method", func(t *testing.T) {
		ns := prepStmtNamespace{
			prepStmts: map[string]*PreparedStatement{
				"stmt1": nil,
				"stmt2": nil,
			},
			portals: map[string]PreparedPortal{
				"portal1": {},
				"portal2": {},
			},
		}

		result := ns.String()
		require.Contains(t, result, "Prep stmts:")
		require.Contains(t, result, "stmt1")
		require.Contains(t, result, "stmt2")
		require.Contains(t, result, "Portals:")
		require.Contains(t, result, "portal1")
		require.Contains(t, result, "portal2")
	})

	t.Run("String empty namespace", func(t *testing.T) {
		ns := prepStmtNamespace{
			prepStmts: map[string]*PreparedStatement{},
			portals:   map[string]PreparedPortal{},
		}

		result := ns.String()
		require.Equal(t, "Prep stmts: Portals: ", result)
	})
}

func TestContextStatementKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("contextStatementKey is empty type", func(t *testing.T) {
		var key contextStatementKey
		require.NotNil(t, key)
	})
}
