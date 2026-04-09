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
	gosql "database/sql"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCommentOnProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE PROCEDURE p() BEGIN SELECT 1; END;
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec          string
		query         string
		expectComment gosql.NullString
	}{
		{
			`COMMENT ON PROCEDURE p IS 'test comment'`,
			`SELECT "comment" FROM system.comments WHERE type = 4`,
			gosql.NullString{String: `test comment`, Valid: true},
		},
		{
			`COMMENT ON PROCEDURE p IS 'updated comment'`,
			`SELECT "comment" FROM system.comments WHERE type = 4`,
			gosql.NullString{String: `updated comment`, Valid: true},
		},
		{
			`COMMENT ON PROCEDURE p IS NULL`,
			`SELECT "comment" FROM system.comments WHERE type = 4`,
			gosql.NullString{Valid: false},
		},
	}

	for _, tc := range testCases {
		if _, err := db.Exec(tc.exec); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(tc.query)
		var comment gosql.NullString
		err := row.Scan(&comment)
		if tc.expectComment.Valid {
			if err != nil {
				t.Fatal(err)
			}
			if tc.expectComment != comment {
				t.Fatalf("expected comment %v, got %v", tc.expectComment, comment)
			}
		} else {
			if err != gosql.ErrNoRows {
				if err != nil {
					t.Fatal(err)
				}
				t.Fatal("expected no rows, but got a comment")
			}
		}
	}
}

func TestCommentOnNonExistentProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
	`); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(`COMMENT ON PROCEDURE non_existent IS 'comment'`)
	if err == nil {
		t.Fatal("expected error for non-existent procedure")
	}
}

func TestCommentOnProcedureWhenDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE PROCEDURE p() BEGIN SELECT 1; END;
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON PROCEDURE p IS 'foo'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP PROCEDURE p`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT "comment" FROM system.comments WHERE type = 4 LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("dropped procedure comment remains in system.comments")
	}
}

func TestCommentOnMultipleProcedures(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE PROCEDURE p1() BEGIN SELECT 1; END;
		CREATE PROCEDURE p2() BEGIN SELECT 2; END;
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON PROCEDURE p1 IS 'first procedure'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON PROCEDURE p2 IS 'second procedure'`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT "comment" FROM system.comments WHERE type = 4 ORDER BY "comment"`)
	require.NoError(t, err)
	defer rows.Close()

	var comments []string
	for rows.Next() {
		var comment string
		require.NoError(t, rows.Scan(&comment))
		comments = append(comments, comment)
	}

	require.Len(t, comments, 2)
	require.Equal(t, "first procedure", comments[0])
	require.Equal(t, "second procedure", comments[1])
}
