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

package mysqlwire_test

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestMySQLDefaultIntSize checks that a SERIAL column is sized by the session's
// default_int_size, as it is over pgwire (logictest int_size). The width of a
// SERIAL is decided when the statement is parsed, from the int size the
// listener hands the parser; a listener that always passes INT8 makes
// SHOW default_int_size report 4 while the column it creates is INT8. Casts
// such as 1::INT are not affected: they are re-resolved from the session when
// the statement is type-checked.
func TestMySQLDefaultIntSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	ctx := context.Background()
	// One connection: default_int_size is session state.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for _, size := range []string{"4", "8"} {
		t.Run("default_int_size="+size, func(t *testing.T) {
			// The sequence behind the SERIAL is created in the current database.
			table := "serial" + size
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS e2e",
				"USE e2e",
				"SET default_int_size = " + size,
				"SET experimental_serial_normalization = 'sql_sequence'",
				"CREATE TABLE " + table + " (a SERIAL)",
			} {
				if _, err := conn.ExecContext(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			var name, create string
			if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&name, &create); err != nil {
				t.Fatal(err)
			}
			if want := "a INT" + size + " NOT NULL"; !strings.Contains(create, want) {
				t.Fatalf("SERIAL with default_int_size = %s: want %q in\n%s", size, want, create)
			}
		})
	}
}
