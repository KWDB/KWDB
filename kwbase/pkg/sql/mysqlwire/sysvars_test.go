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

package mysqlwire

import (
	"fmt"
	"strings"
	"testing"
)

// driverProbeQuery is what Connector/J 8.4.0 sends immediately after the
// handshake, taken verbatim from NativeSession.loadServerVariables. Against a
// server reporting 8.0.11 it carries nineteen columns: the language branch is
// gated on < 5.5 and the two query_cache columns on < 8.0.3, and the
// transaction_isolation spelling wins over tx_isolation. Every quirk of the
// text is deliberate and has to survive byte for byte -- the block comment with
// no space before SELECT, the two spaces after it, the explicit alias on every
// item.
//
// A failure of any single column here is a connection that does not come up,
// not a feature that degrades.
const driverProbeQuery = `/*mysql-connector-j-8.4.0 (Revision: 1c3f5c149e0bfe31c7fbeb24e2d260cd890972c4) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

const driverProbeQueryRewritten = `/*mysql-connector-j-8.4.0 (Revision: 1c3f5c149e0bfe31c7fbeb24e2d260cd890972c4) */SELECT  current_setting('auto_increment_increment', true) AS auto_increment_increment, current_setting('character_set_client', true) AS character_set_client, current_setting('character_set_connection', true) AS character_set_connection, current_setting('character_set_results', true) AS character_set_results, 'utf8mb4' AS character_set_server, current_setting('collation_server', true) AS collation_server, current_setting('collation_connection', true) AS collation_connection, current_setting('init_connect', true) AS init_connect, current_setting('interactive_timeout', true) AS interactive_timeout, current_setting('license', true) AS license, '1' AS lower_case_table_names, current_setting('max_allowed_packet', true) AS max_allowed_packet, current_setting('net_write_timeout', true) AS net_write_timeout, current_setting('performance_schema', true) AS performance_schema, coalesce(current_setting('sql_mode', true) || ',', '') || 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES' AS sql_mode, current_setting('system_time_zone', true) AS system_time_zone, current_setting('time_zone', true) AS time_zone, upper(replace(current_setting('transaction_isolation', true), ' ', '-')) AS transaction_isolation, current_setting('wait_timeout', true) AS wait_timeout`

func TestRewriteSelectCompat(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// 1) The shapes a client actually sends.
		{"driver's banner query",
			`SELECT @@version_comment`,
			`SELECT current_setting('version_comment', true) AS "@@version_comment"`},
		{"bare name with a covering row",
			`SELECT @@sql_mode`,
			`SELECT coalesce(current_setting('sql_mode', true) || ',', '') || 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES' AS "@@sql_mode"`},
		{"explicit alias is kept as written",
			`SELECT @@a AS x`,
			`SELECT current_setting('a', true) AS x`},
		{"two items",
			`SELECT @@a, @@b`,
			`SELECT current_setting('a', true) AS "@@a", current_setting('b', true) AS "@@b"`},
		{"no space around the comma",
			`SELECT @@a,@@b`,
			`SELECT current_setting('a', true) AS "@@a",current_setting('b', true) AS "@@b"`},
		{"one aliased item and one not",
			`SELECT @@a AS x, @@b`,
			`SELECT current_setting('a', true) AS x, current_setting('b', true) AS "@@b"`},
		{"trailing semicolon",
			`SELECT @@a;`,
			`SELECT current_setting('a', true) AS "@@a";`},
		// The mysql command line client sends exactly this on an interactive
		// connect. The tail after the item list is copied without inspection.
		{"tail passes through",
			`select @@version_comment limit 1`,
			`select current_setting('version_comment', true) AS "@@version_comment" limit 1`},

		// The name is looked up folded, but the synthesized label keeps the
		// client's own spelling: MySQL labels SELECT @@SQL_MODE as @@SQL_MODE,
		// and a client that reads its result by label has to find it.
		{"name is folded, label is not",
			`SELECT @@SQL_MODE`,
			`SELECT coalesce(current_setting('sql_mode', true) || ',', '') || 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES' AS "@@SQL_MODE"`},
		{"mixed case unknown name",
			`SELECT @@Some_Var`,
			`SELECT current_setting('some_var', true) AS "@@Some_Var"`},

		// 2) Scope prefixes. session and local both name the one value space
		// KWDB has; global names a second one it does not have, so it is
		// refused rather than answered with the session value.
		{"session scope",
			`SELECT @@session.transaction_isolation`,
			`SELECT upper(replace(current_setting('transaction_isolation', true), ' ', '-')) AS "@@session.transaction_isolation"`},
		{"local scope",
			`SELECT @@local.time_zone`,
			`SELECT current_setting('time_zone', true) AS "@@local.time_zone"`},
		{"scope keyword is case insensitive",
			`SELECT @@SESSION.autocommit`,
			`SELECT CASE current_setting('autocommit', true) WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE NULL END AS "@@SESSION.autocommit"`},
		{"global scope is refused",
			`SELECT @@global.time_zone`,
			`SELECT @@global.time_zone`},

		// 3) The covering table, row by row. Each of these exists because
		// handing the client KWDB's own answer would have it read the value
		// wrongly rather than fail.
		{"transaction_isolation is respelled, not revalued",
			`SELECT @@transaction_isolation`,
			`SELECT upper(replace(current_setting('transaction_isolation', true), ' ', '-')) AS "@@transaction_isolation"`},
		{"tx_isolation is the 5.7 name for it",
			`SELECT @@tx_isolation`,
			`SELECT upper(replace(current_setting('transaction_isolation', true), ' ', '-')) AS "@@tx_isolation"`},
		{"autocommit answers 1/0, not on/off",
			`SELECT @@autocommit`,
			`SELECT CASE current_setting('autocommit', true) WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE NULL END AS "@@autocommit"`},
		{"transaction_read_only answers 1/0",
			`SELECT @@transaction_read_only`,
			`SELECT CASE current_setting('transaction_read_only', true) WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE NULL END AS "@@transaction_read_only"`},
		{"tx_read_only reads the same variable",
			`SELECT @@tx_read_only`,
			`SELECT CASE current_setting('transaction_read_only', true) WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE NULL END AS "@@tx_read_only"`},
		{"lower_case_table_names is a constant",
			`SELECT @@lower_case_table_names`,
			`SELECT '1' AS "@@lower_case_table_names"`},
		{"character_set_server is a constant",
			`SELECT @@character_set_server`,
			`SELECT 'utf8mb4' AS "@@character_set_server"`},
		{"version repeats the handshake string",
			`SELECT @@version`,
			`SELECT '8.0.11-KWDB' AS "@@version"`},
		{"a name with no covering row falls through",
			`SELECT @@max_allowed_packet`,
			`SELECT current_setting('max_allowed_packet', true) AS "@@max_allowed_packet"`},

		// 4) Leading trivia. Whatever is skipped is still copied out verbatim,
		// so the parser sees the client's own bytes ahead of the item list.
		{"leading block comment",
			`/* c */SELECT @@a`,
			`/* c */SELECT current_setting('a', true) AS "@@a"`},
		{"nested block comment",
			`/* a /* b */ */SELECT @@a`,
			`/* a /* b */ */SELECT current_setting('a', true) AS "@@a"`},
		// Read with MySQL's non-nesting rule this would look terminated at the
		// inner */ and the statement would be rewritten. KWDB nests, so the
		// comment swallows the rest of the input and there is no SELECT left.
		{"unterminated block comment",
			`/* a SELECT @@a`,
			`/* a SELECT @@a`},
		{"leading line comment",
			"-- c\nSELECT @@a",
			"-- c\nSELECT current_setting('a', true) AS \"@@a\""},
		// # is KWDB's bitwise XOR operator, not a comment introducer.
		{"hash is not a comment",
			"# c\nSELECT @@a",
			"# c\nSELECT @@a"},
		{"leading whitespace",
			"  \t SELECT @@a",
			"  \t SELECT current_setting('a', true) AS \"@@a\""},

		// 5) Injection and quote-fusing defenses. Each of these ends the
		// expression on a byte that is not an item boundary, so the whole
		// statement is handed over untouched.
		{"quote directly after the name",
			`SELECT @@a'--`,
			`SELECT @@a'--`},
		{"double quote directly after the name",
			`SELECT @@a"b"`,
			`SELECT @@a"b"`},
		{"closing paren after the name",
			`SELECT @@a)`,
			`SELECT @@a)`},
		{"arithmetic after the name",
			`SELECT @@a+1`,
			`SELECT @@a+1`},
		{"cast after the name",
			`SELECT @@a::INT`,
			`SELECT @@a::INT`},

		// 6) All or nothing. One unrecognized item discards the rewrite of
		// every item before it, because a half-rewritten list is a statement
		// nobody sent.
		{"second item is a function call",
			`SELECT @@a, database()`,
			`SELECT @@a, database()`},
		{"second item is a literal",
			`SELECT @@a, '@@b'`,
			`SELECT @@a, '@@b'`},
		{"trailing comma",
			`SELECT @@a,`,
			`SELECT @@a,`},

		// 7) Lexical boundaries.
		{"nothing after the sigils",
			`SELECT @@`,
			`SELECT @@`},
		{"three sigils",
			`SELECT @@@a`,
			`SELECT @@@a`},
		{"space before the scope dot",
			`SELECT @@ .x`,
			`SELECT @@ .x`},
		// A name is scanned as ASCII only: a multi-byte byte must never reach
		// the string literal the rewrite builds.
		{"non-ascii name",
			`SELECT @@中文`,
			`SELECT @@中文`},
		{"two scope dots",
			`SELECT @@session.a.b`,
			`SELECT @@session.a.b`},
		{"name starting with a digit",
			`SELECT @@1`,
			`SELECT @@1`},
		// A single @ is not a syntax error in KWDB: SELECT @x parses and then
		// fails name resolution. The boundary is "two sigils", not "@ is
		// invalid anyway".
		{"single sigil",
			`SELECT @x`,
			`SELECT @x`},
		// R1 copies the bytes ahead of the item verbatim, so with no space
		// after the keyword the replacement would be spliced onto SELECT
		// itself. Both forms are a syntax error; a fused one is a worse one.
		{"no space after the keyword",
			`SELECT@@a`,
			`SELECT@@a`},
		{"keyword continues into a name",
			`SELECTED @@a`,
			`SELECTED @@a`},
		{"not a select",
			`UPDATE t SET c = @@a`,
			`UPDATE t SET c = @@a`},
		// Only the leading statement is examined; there is no statement
		// splitter in this package.
		{"not the leading statement",
			`SELECT 1; SELECT @@a`,
			`SELECT 1; SELECT @@a`},
		{"leading statement is rewritten, the rest is tail",
			`SELECT @@a; SELECT 1`,
			`SELECT current_setting('a', true) AS "@@a"; SELECT 1`},

		// 8) Aliases the scanner cannot finish reading fail the statement open
		// rather than being guessed at.
		{"AS with nothing after it",
			`SELECT @@a AS`,
			`SELECT @@a AS`},
		{"unterminated quoted alias",
			`SELECT @@a AS "x`,
			`SELECT @@a AS "x`},
		{"quoted alias",
			`SELECT @@a AS "my col"`,
			`SELECT current_setting('a', true) AS "my col"`},
		{"alias starting with a digit",
			`SELECT @@a AS 1x`,
			`SELECT @@a AS 1x`},

		// 9) Literals and identifiers that merely contain the sigils. The
		// prefix-shaped recognition cannot reach any of them.
		{"sigils inside a literal",
			`SELECT '@@version_comment' AS s`,
			`SELECT '@@version_comment' AS s`},
		{"sigils inside a quoted alias",
			`SELECT 1 AS "@@a""b"`,
			`SELECT 1 AS "@@a""b"`},
		{"sigils inside a comment",
			`SELECT /* @@x */ 1 AS c`,
			`SELECT /* @@x */ 1 AS c`},
		{"index hint",
			`SELECT count(*) FROM system.users@primary`,
			`SELECT count(*) FROM system.users@primary`},

		// 10) The driver's connection-phase query, byte for byte.
		{"connector/J loadServerVariables", driverProbeQuery, driverProbeQueryRewritten},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rewriteSelectCompat(tc.in, dialectContext{}); got != tc.want {
				t.Errorf("rewriteSelectCompat(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRewriteZeroArgFunctions covers the two MySQL zero-argument functions this
// package answers and, at greater length, the many it must not touch.
//
// Neither could be answered anywhere else. USER is a reserved word in KWDB's
// grammar, so `SELECT USER()` fails at the opening paren and a builtin added
// outside this package would never be reached; CONNECTION_ID()'s value exists
// only in the protocol layer, so it is inlined here as the literal the greeting
// already reported.
//
// Everything else has to arrive at the parser byte for byte. That is what the
// second group is for: the table of names is closed, so a call this package
// does not cover fails the whole statement open and KWDB's own builtins go on
// answering for themselves.
func TestRewriteZeroArgFunctions(t *testing.T) {
	// The number this connection announced in its greeting. CONNECTION_ID()
	// has to answer with the same one.
	dc := dialectContext{connID: 7}

	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// 1) The forms this package answers.
		{"USER",
			`SELECT USER()`,
			`SELECT current_user() AS "USER()"`},
		{"the name is matched folded",
			`SELECT user()`,
			`SELECT current_user() AS "user()"`},
		{"CONNECTION_ID repeats the greeting's number",
			`SELECT CONNECTION_ID()`,
			`SELECT 7 AS "CONNECTION_ID()"`},
		{"explicit alias is kept as written",
			`SELECT USER() AS u`,
			`SELECT current_user() AS u`},
		{"spaces inside the call",
			`SELECT USER ( )`,
			`SELECT current_user() AS "USER ( )"`},
		{"mixed with system variables",
			`SELECT USER(), @@version, CONNECTION_ID()`,
			`SELECT current_user() AS "USER()", '8.0.11-KWDB' AS "@@version", 7 AS "CONNECTION_ID()"`},
		{"tail passes through",
			`SELECT CONNECTION_ID() limit 1`,
			`SELECT 7 AS "CONNECTION_ID()" limit 1`},

		// 2) Calls the table does not cover. KWDB answers most of these today,
		// and the rest are the client's own error to make; either way the
		// parser has to see what was sent.
		{"database",
			`SELECT database()`,
			`SELECT database()`},
		{"now",
			`SELECT now()`,
			`SELECT now()`},
		// current_user() and session_user() are KWDB's own spellings and
		// already answer correctly. A row for either would shadow a working
		// builtin with a rewrite, which is why the table has neither.
		{"current_user is left to KWDB",
			`SELECT current_user()`,
			`SELECT current_user()`},
		{"session_user is left to KWDB",
			`SELECT session_user()`,
			`SELECT session_user()`},
		{"a function the client made up",
			`SELECT myuser()`,
			`SELECT myuser()`},
		{"a name that merely ends in a covered one",
			`SELECT myconnection_id()`,
			`SELECT myconnection_id()`},
		{"qualified by a table name",
			`SELECT t.USER()`,
			`SELECT t.USER()`},
		{"quoted identifier",
			`SELECT "user"()`,
			`SELECT "user"()`},
		// Valid today: without the parens this is the grammar's user keyword,
		// and it returns the current user.
		{"no parentheses",
			`SELECT user`,
			`SELECT user`},
		{"an argument makes it another call entirely",
			`SELECT user(1)`,
			`SELECT user(1)`},
		{"nested in another call",
			`SELECT concat(USER())`,
			`SELECT concat(USER())`},

		// 3) Rule R3: a covered call still has to end where a select item may
		// end, or what the client wrote is larger than what was recognized.
		{"arithmetic after the call",
			`SELECT USER()+1`,
			`SELECT USER()+1`},
		{"cast after the call",
			`SELECT CONNECTION_ID()::INT`,
			`SELECT CONNECTION_ID()::INT`},

		// 4) Rule R2: one unrecognized item discards the whole rewrite.
		{"one covered item and one not",
			`SELECT USER(), database()`,
			`SELECT USER(), database()`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rewriteSelectCompat(tc.in, dc); got != tc.want {
				t.Errorf("rewriteSelectCompat(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestSysVarExprShape pins rule R5 for the fall-through path: a name with no
// covering row must become the two-argument current_setting and nothing else.
// The one-argument form raises 42704 for an unknown name, which would turn one
// renamed variable anywhere in KWDB into a refused connection.
func TestSysVarExprShape(t *testing.T) {
	for _, name := range []string{"init_connect", "license", "wait_timeout", "zz_no_such_var"} {
		if _, covered := mysqlSystemVarExpr[name]; covered {
			t.Fatalf("%s now has a covering row; pick another name for this test", name)
		}
		want := fmt.Sprintf("current_setting('%s', true)", name)
		if got := sysVarExpr(name); got != want {
			t.Errorf("sysVarExpr(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestSysVarExprAreNullSafe pins rule R5 for the covering rows: each one is
// either a bare literal, which cannot fail, or reads every variable it touches
// through the two-argument current_setting, which yields NULL for a name the
// server does not know. The driver asks for nineteen of these in one statement
// during connect, and a single raising expression fails all nineteen.
func TestSysVarExprAreNullSafe(t *testing.T) {
	for name, expr := range mysqlSystemVarExpr {
		reads := strings.Count(expr, "current_setting(")
		if reads == 0 {
			if !strings.HasPrefix(expr, "'") || !strings.HasSuffix(expr, "'") {
				t.Errorf("%s: %q reads no variable and is not a literal", name, expr)
			}
			continue
		}
		if nullSafe := strings.Count(expr, ", true)"); nullSafe != reads {
			t.Errorf("%s: %q makes %d current_setting call(s) but only %d of them "+
				"yield NULL instead of raising", name, expr, reads, nullSafe)
		}
	}
}

// TestSysVarKeysAreLowercase pins that no row of either lookup table is dead.
// The scanner folds a name before looking it up, so an upper-case key could
// never match.
func TestSysVarKeysAreLowercase(t *testing.T) {
	for name := range mysqlSystemVarExpr {
		if name != strings.ToLower(name) {
			t.Errorf("mysqlSystemVarExpr key %q can never be matched: the lookup folds "+
				"the name first", name)
		}
	}
	for name := range mysqlZeroArgFunc {
		if name != strings.ToLower(name) {
			t.Errorf("mysqlZeroArgFunc key %q can never be matched: the lookup folds "+
				"the name first", name)
		}
	}
}

// TestSysVarVersionMatchesHandshake pins that @@version is assembled from the
// constant the greeting reports rather than spelled out again. Two literals
// would let the server report two different versions on the same connection.
func TestSysVarVersionMatchesHandshake(t *testing.T) {
	expr, ok := mysqlSystemVarExpr["version"]
	if !ok {
		t.Fatal("no covering row for version")
	}
	if !strings.Contains(expr, serverVersion) {
		t.Errorf("@@version expands to %q, which does not carry serverVersion %q",
			expr, serverVersion)
	}
}

// TestSysVarCoverageIsPinned spells out the covering table's key set so that
// adding or removing a row is a deliberate edit rather than a silent one. The
// end-to-end test TestMySQLSystemVariablesAreAnswerable carries the same list
// -- it lives in the external test package and cannot read this map -- and is
// what proves each of these expressions actually evaluates on a live node.
func TestSysVarCoverageIsPinned(t *testing.T) {
	want := map[string]bool{
		"autocommit": true, "character_set_server": true, "lower_case_table_names": true,
		"sql_mode": true, "transaction_isolation": true, "transaction_read_only": true,
		"tx_isolation": true, "tx_read_only": true, "version": true,
	}
	for name := range mysqlSystemVarExpr {
		if !want[name] {
			t.Errorf("new covering row %q: add it to TestMySQLSystemVariablesAreAnswerable too",
				name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("covering row %q is gone: drop it from TestMySQLSystemVariablesAreAnswerable too",
			name)
	}
}

// TestZeroArgFuncCoverageIsPinned spells out the zero-argument table's key set
// so that adding a row is a deliberate edit. Every name in it is a name taken
// away from the SQL layer: the rewrite answers from a context that knows only
// the connection, so covering a function KWDB already has would replace a real
// answer with a poorer one.
func TestZeroArgFuncCoverageIsPinned(t *testing.T) {
	want := map[string]bool{"user": true, "connection_id": true}
	for name := range mysqlZeroArgFunc {
		if !want[name] {
			t.Errorf("new zero-argument function %q: it now shadows whatever the SQL "+
				"layer answers for that name", name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("zero-argument function %q is gone: TestRewriteZeroArgFunctions and "+
			"TestMySQLUserFunction still expect it", name)
	}
}
