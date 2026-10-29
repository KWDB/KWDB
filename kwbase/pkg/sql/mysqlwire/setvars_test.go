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
	"strings"
	"testing"
)

func TestRewriteSetCharsetResultsNull(t *testing.T) {
	// noop is spelled out here rather than taken from the constant so that a
	// change to the substituted expression has to be made twice, deliberately.
	const noop = `current_setting('character_set_results')`

	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// 1) The statement Connector/J sends during the connection phase, and
		//    the spellings a client is free to write it in. Only the four bytes
		//    of NULL are replaced; everything else is the client's own text.
		{"the form the driver sends",
			`SET character_set_results = NULL`,
			`SET character_set_results = ` + noop},
		{"lower case throughout",
			`set character_set_results = null`,
			`set character_set_results = ` + noop},
		{"mixed case NULL",
			`SET character_set_results = Null`,
			`SET character_set_results = ` + noop},
		{"no space around the equals",
			`SET character_set_results=NULL`,
			`SET character_set_results=` + noop},
		{"TO instead of equals",
			`SET character_set_results TO NULL`,
			`SET character_set_results TO ` + noop},
		{"SESSION scope",
			`SET SESSION character_set_results = NULL`,
			`SET SESSION character_set_results = ` + noop},
		{"LOCAL scope",
			`SET local character_set_results = NULL`,
			`SET local character_set_results = ` + noop},
		{"trailing semicolon is kept",
			`SET character_set_results = NULL;`,
			`SET character_set_results = ` + noop + `;`},
		{"trailing whitespace is kept",
			"SET character_set_results = NULL\n",
			"SET character_set_results = " + noop + "\n"},
		{"leading trivia is copied verbatim",
			"/* c */ SET  character_set_results\t=\tNULL",
			"/* c */ SET  character_set_results\t=\t" + noop},

		// 2) Untouched because the statement works today. character_set_results
		//    accepts utf8, utf8mb3, utf8mb4 and DEFAULT; substituting the
		//    current value into any of those would discard the value the client
		//    chose.
		{"a real charset value",
			`SET character_set_results = 'utf8'`,
			`SET character_set_results = 'utf8'`},
		{"an unquoted charset value",
			`SET character_set_results = utf8mb4`,
			`SET character_set_results = utf8mb4`},
		{"DEFAULT",
			`SET character_set_results = DEFAULT`,
			`SET character_set_results = DEFAULT`},

		// 3) Untouched because the shape was not recognized in full. Every one
		//    of these fails today and has to keep failing the same way: a
		//    rewrite reaching any of them would be one that matched on a prefix
		//    rather than on the whole statement.
		{"NULLIF is a different word",
			`SET character_set_results = NULLIF('a','b')`,
			`SET character_set_results = NULLIF('a','b')`},
		{"NULL leading a larger expression",
			`SET character_set_results = NULL::STRING`,
			`SET character_set_results = NULL::STRING`},
		{"NULL followed by another assignment",
			`SET character_set_results = NULL, autocommit = 1`,
			`SET character_set_results = NULL, autocommit = 1`},
		{"another variable's NULL",
			`SET character_set_client = NULL`,
			`SET character_set_client = NULL`},
		{"a longer variable name",
			`SET character_set_results_x = NULL`,
			`SET character_set_results_x = NULL`},
		{"no assignment operator",
			`SET character_set_results NULL`,
			`SET character_set_results NULL`},
		{"no value at all",
			`SET character_set_results =`,
			`SET character_set_results =`},
		{"GLOBAL is not a scope KWDB has",
			`SET GLOBAL character_set_results = NULL`,
			`SET GLOBAL character_set_results = NULL`},
		{"the keyword has to end where it does",
			`SETcharacter_set_results = NULL`,
			`SETcharacter_set_results = NULL`},

		// 4) Untouched because it does not lead the statement. This is the same
		//    limit stripSetNamesCollate documents, for the same reason: there is
		//    no statement splitter in this package.
		{"not the leading statement",
			`SELECT 1; SET character_set_results = NULL`,
			`SELECT 1; SET character_set_results = NULL`},
		{"inside a string literal",
			`SELECT 'SET character_set_results = NULL'`,
			`SELECT 'SET character_set_results = NULL'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rewriteSetCharsetResultsNull(tc.in); got != tc.want {
				t.Errorf("rewriteSetCharsetResultsNull(%q)\n got %q\nwant %q",
					tc.in, got, tc.want)
			}
		})
	}
}

// TestSetCharsetResultsNoopReadsBackTheSameVariable pins the shape of the
// substituted expression rather than only its text.
//
// It has to read the variable it is assigned to, so the statement is an exact
// no-op whatever the value is. DEFAULT would parse and is indistinguishable
// today -- character_set_results is a compat variable whose getter returns a
// captured constant -- but it would silently discard a value the client set
// earlier if the variable ever became stateful.
//
// The one-argument form of current_setting is deliberate, against the
// two-argument form every expression in sysvars.go uses: if the variable is
// ever removed, this should fail with an accurate "unrecognized configuration
// parameter" naming it, not feed NULL back into the SET and produce an error
// about types.
func TestSetCharsetResultsNoopReadsBackTheSameVariable(t *testing.T) {
	if want := `current_setting('character_set_results')`; setCharsetResultsNoop != want {
		t.Errorf("setCharsetResultsNoop = %q, want %q", setCharsetResultsNoop, want)
	}
}

// TestRewriteSetSessionTransaction pins the one rewrite in this package that
// changes what an already valid statement does, so the boundary it is confined
// to has to be spelled out case by case.
//
// SET SESSION TRANSACTION and SET SESSION CHARACTERISTICS AS TRANSACTION share
// a grammar rule but not an execution path. Anything the first accepts the
// second parses, which is what makes a prefix-shaped rewrite between them look
// safe and is why the accepted shape here is a single isolation clause and
// nothing else: set_default_isolation.go refuses a PRIORITY outright and aborts
// the transaction with it, never reads AS OF SYSTEM TIME at all, and resets the
// session isolation level to SERIALIZABLE whenever no ISOLATION clause is
// present.
func TestRewriteSetSessionTransaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// 1) The three levels KWDB has, which is what a connection pool sends
		//    from Connection.setTransactionIsolation(). Only the words
		//    CHARACTERISTICS AS are inserted; every other byte is the client's.
		{"read committed",
			`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"repeatable read",
			`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`,
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ`},
		{"serializable",
			`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`,
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE`},
		{"lower case",
			`set session transaction isolation level read committed`,
			`set session CHARACTERISTICS AS transaction isolation level read committed`},
		{"trailing semicolon is kept",
			`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;`,
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;`},
		{"the client's own spacing is kept",
			"SET  SESSION\tTRANSACTION\n ISOLATION  LEVEL  READ\tCOMMITTED ",
			"SET  SESSION CHARACTERISTICS AS\tTRANSACTION\n ISOLATION  LEVEL  READ\tCOMMITTED "},
		{"leading trivia is copied verbatim",
			"/* pool */ SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"/* pool */ SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"},

		// 2) The read-only pair, which is Connection.setReadOnly(). It becomes
		//    a plain session variable rather than a CHARACTERISTICS statement:
		//    that statement carries no ISOLATION clause, and the branch handling
		//    it resets the session isolation level to SERIALIZABLE, so writing
		//    it here would silently undo the level set by the group above.
		{"read only",
			`SET SESSION TRANSACTION READ ONLY`,
			`SET default_transaction_read_only = true`},
		{"read write",
			`SET SESSION TRANSACTION READ WRITE`,
			`SET default_transaction_read_only = false`},
		{"read only lower case with semicolon",
			`set session transaction read only;`,
			`set default_transaction_read_only = true;`},
		{"read only keeps leading trivia",
			"/* pool */ SET  SESSION TRANSACTION READ ONLY",
			"/* pool */ SET  default_transaction_read_only = true"},

		// 3) Mode lists this rewrite must never absorb. Each of these works
		//    today and would break, loudly or silently, if it were handed to
		//    SET SESSION CHARACTERISTICS AS TRANSACTION instead.
		{"priority alone",
			`SET SESSION TRANSACTION PRIORITY HIGH`,
			`SET SESSION TRANSACTION PRIORITY HIGH`},
		{"as of system time alone",
			`SET SESSION TRANSACTION AS OF SYSTEM TIME '-3600s'`,
			`SET SESSION TRANSACTION AS OF SYSTEM TIME '-3600s'`},
		{"isolation level followed by priority",
			`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED PRIORITY HIGH`,
			`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED PRIORITY HIGH`},
		{"isolation level followed by a comma",
			`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY`,
			`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY`},
		{"read only followed by another mode",
			`SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED`,
			`SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED`},

		// 4) Shapes outside the boundary. The first two are levels and scopes
		//    KWDB does not have: the parser already names the word it cannot
		//    take, which is a more accurate answer than quietly substituting a
		//    level the client did not ask for.
		{"read uncommitted is a level KWDB does not have",
			`SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`,
			`SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`},
		{"GLOBAL is a scope KWDB does not have",
			`SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"without SESSION the statement is about the current transaction",
			`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"LOCAL is not MySQL's spelling of this statement",
			`SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"the target spelling is not recognized again",
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"LEVEL is not optional",
			`SET SESSION TRANSACTION ISOLATION READ COMMITTED`,
			`SET SESSION TRANSACTION ISOLATION READ COMMITTED`},
		{"no level at all",
			`SET SESSION TRANSACTION ISOLATION LEVEL`,
			`SET SESSION TRANSACTION ISOLATION LEVEL`},
		{"READ without a following word",
			`SET SESSION TRANSACTION READ`,
			`SET SESSION TRANSACTION READ`},
		{"the keyword has to end where it does",
			`SET SESSIONTRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SET SESSIONTRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"not the leading statement",
			`SELECT 1; SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`,
			`SELECT 1; SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`},
		{"inside a string literal",
			`SELECT 'SET SESSION TRANSACTION READ ONLY'`,
			`SELECT 'SET SESSION TRANSACTION READ ONLY'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rewriteSetSessionTransaction(tc.in); got != tc.want {
				t.Errorf("rewriteSetSessionTransaction(%q)\n got %q\nwant %q",
					tc.in, got, tc.want)
			}
		})
	}
}

// TestSetSessionTransactionReadOnlyNeverBecomesCharacteristics states invariant
// I19 on its own, because the mistake it guards against is one the table above
// would still read as correct if the expected value were changed to match a
// wrong implementation.
//
// SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY parses and appears to do
// the right thing. It also carries no ISOLATION clause, and that branch of
// set_default_isolation.go resets the session isolation level to SERIALIZABLE.
// A pool that sets READ COMMITTED and then calls setReadOnly(true) -- the order
// HikariCP and Spring use -- would end up on SERIALIZABLE without being told.
func TestSetSessionTransactionReadOnlyNeverBecomesCharacteristics(t *testing.T) {
	for _, in := range []string{
		`SET SESSION TRANSACTION READ ONLY`,
		`SET SESSION TRANSACTION READ WRITE`,
		`set session transaction read only;`,
	} {
		if got := rewriteSetSessionTransaction(in); strings.Contains(got, "CHARACTERISTICS") {
			t.Errorf("rewriteSetSessionTransaction(%q) = %q, which resets the isolation level", in, got)
		}
	}
}

// TestSetSessionTransactionRewriteTargetAcceptsEverythingTheSourceDid drives
// every one- and two-mode list the source statement accepts through the rewrite
// and requires each to be either left alone or turned into one of the six
// outputs whose behavior is pinned above.
//
// The table test says what the rewrite does to the inputs someone thought of.
// This says what it does to the ones nobody did, which is the half that matters
// here: the two statements share transaction_mode_list, so every combination
// below parses on both sides, and a rewrite that widened by one keyword would
// still produce valid SQL -- SQL that drops AS OF SYSTEM TIME, or aborts the
// transaction on PRIORITY, or silently resets the isolation level.
func TestSetSessionTransactionRewriteTargetAcceptsEverythingTheSourceDid(t *testing.T) {
	// rewritten is the complete set of statements this package may change, and
	// what it must change them into.
	rewritten := map[string]string{
		`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`:  `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED`,
		`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`: `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ`,
		`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`:    `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE`,
		`SET SESSION TRANSACTION READ ONLY`:                       `SET default_transaction_read_only = true`,
		`SET SESSION TRANSACTION READ WRITE`:                      `SET default_transaction_read_only = false`,
	}
	modes := []string{
		"ISOLATION LEVEL READ COMMITTED",
		"ISOLATION LEVEL REPEATABLE READ",
		"ISOLATION LEVEL SERIALIZABLE",
		"ISOLATION LEVEL READ UNCOMMITTED",
		"PRIORITY LOW",
		"PRIORITY NORMAL",
		"PRIORITY HIGH",
		"READ ONLY",
		"READ WRITE",
		"AS OF SYSTEM TIME '-3600s'",
	}
	check := func(in string) {
		t.Helper()
		want, ok := rewritten[in]
		if !ok {
			want = in
		}
		if got := rewriteSetSessionTransaction(in); got != want {
			t.Errorf("rewriteSetSessionTransaction(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	for _, a := range modes {
		check("SET SESSION TRANSACTION " + a)
		for _, b := range modes {
			// opt_comma in the grammar makes the separator optional, so both
			// spellings of a two-mode list have to be covered.
			check("SET SESSION TRANSACTION " + a + ", " + b)
			check("SET SESSION TRANSACTION " + a + " " + b)
		}
	}
}
