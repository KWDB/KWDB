// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package cli

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// Example_sql_lex tests the usage of the lexer in the sql subcommand.
func Example_sql_lex() {
	c := newCLITest(cliTestParams{insecure: true})
	defer c.cleanup()

	conn := makeSQLConn(fmt.Sprintf("postgres://%s@%s/?sslmode=disable",
		security.RootUser, c.ServingSQLAddr()))
	defer conn.Close()

	tests := []string{`
select '
\?
;
';
`,
		`
select ''''
;

select '''
;
''';
`,
		`select 1 as "1";
-- just a comment without final semicolon`,
	}

	setCLIDefaultsForTests()

	// We need a temporary file with a name guaranteed to be available.
	// So open a dummy file.
	f, err := ioutil.TempFile("", "input")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return
	}
	// Get the name and close it.
	fname := f.Name()
	f.Close()

	// At every point below, when t.Fatal is called we should ensure the
	// file is closed and removed.
	f = nil
	defer func() {
		if f != nil {
			f.Close()
		}
		_ = os.Remove(fname)
		stdin = os.Stdin
	}()

	for _, test := range tests {
		// Populate the test input.
		if f, err = os.OpenFile(fname, os.O_WRONLY, 0644); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		if _, err := f.WriteString(test); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		f.Close()
		// Make it available for reading.
		if f, err = os.Open(fname); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		// Override the standard input for runInteractive().
		stdin = f

		err := runInteractive(conn)
		if err != nil {
			fmt.Fprintln(stderr, err)
		}
	}

	// Output:
	// ?column?
	// ------------
	//
	//   \?
	//   ;
	//
	// (1 row)
	//   ?column?
	// ------------
	//   '
	// (1 row)
	//   ?column?
	// ------------
	//   '
	//   ;
	//   '
	// (1 row)
	//   1
	// -----
	//   1
	// (1 row)
}

func TestIsEndOfStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		in         string
		isEnd      bool
		isNotEmpty bool
	}{
		{
			in:         ";",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; /* comment */",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SET; SELECT 1;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "SELECT ''''; SET;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in: "  -- hello",
		},
		{
			in:         "select 'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         "'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         `SELECT e'\xaa';`, // invalid token but last token is semicolon
			isEnd:      true,
			isNotEmpty: true,
		},
	}

	for _, test := range tests {
		lastTok, isNotEmpty := parser.LastLexicalToken(test.in)
		if isNotEmpty != test.isNotEmpty {
			t.Errorf("%q: isNotEmpty expected %v, got %v", test.in, test.isNotEmpty, isNotEmpty)
		}
		var c cliState
		c = setupTestCliState()
		isEnd := c.isEndOfStatement(lastTok)
		if isEnd != test.isEnd {
			t.Errorf("%q: isEnd expected %v, got %v", test.in, test.isEnd, isEnd)
		}
	}
}

// Test handleCliCmd cases for client-side commands that are aliases for sql
// statements.
func TestHandleCliCmdSqlAlias(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clientSideCommandTestsTable := []struct {
		commandString string
		wantSQLStmt   string
	}{
		{`\l`, `SHOW DATABASES`},
		{`\dt`, `SHOW TABLES`},
		{`\du`, `SHOW USERS`},
		{`\d mytable`, `SHOW COLUMNS FROM mytable`},
		{`\d`, `SHOW TABLES`},
	}

	var c cliState
	for _, tt := range clientSideCommandTestsTable {
		c = setupTestCliState()
		c.lastInputLine = tt.commandString
		gotState := c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))

		assert.Equal(t, cliRunStatement, gotState)
		assert.Equal(t, tt.wantSQLStmt, c.concatLines)
	}
}

func TestHandleCliCmdSlashDInvalidSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clientSideCommandTests := []string{`\d goodarg badarg`, `\dz`}

	var c cliState
	for _, tt := range clientSideCommandTests {
		c = setupTestCliState()
		c.lastInputLine = tt
		gotState := c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))

		assert.Equal(t, cliStateEnum(0), gotState)
		assert.Equal(t, errInvalidSyntax, c.exitErr)
	}
}

func setupTestCliState() cliState {
	c := cliState{}
	c.ins = noLineEditor
	return c
}
