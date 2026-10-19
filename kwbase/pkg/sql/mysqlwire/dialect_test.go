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
	"regexp"
	"strings"
	"testing"
)

// dialectUntouched is every statement the dispatcher has to hand to the parser
// byte for byte.
//
// The first block is valid SQL today: rewriting any of it would change what a
// working client gets back. The second block is invalid today and has to stay
// invalid in the same way -- leaving it alone is what keeps a client's mistake
// from becoming a different, less recognizable mistake, and, for the entries
// ending inside a quote, what keeps a generated expression out of a literal the
// client opened.
var dialectUntouched = []string{
	`SELECT '@@version_comment' AS s`,
	`INSERT INTO t VALUES ('@@a')`,
	`SELECT 1 AS "@@a""b"`,
	`SELECT /* @@x */ 1 AS c`,
	`SELECT 1 AS c -- @@x`,
	// @primary is an index hint, not a variable sigil.
	`SELECT count(*) FROM system.users@primary`,
	// A single @ parses; it fails at name resolution, not in the scanner.
	`SELECT @x`,
	`SELECT database()`,
	`SELECT now()`,
	`SELECT random()`,
	// # is KWDB's bitwise XOR operator; this evaluates to 6.
	`SELECT 5 # 3`,
	// ? is the JSONB existence operator here.
	`SELECT j ? 'a' FROM t`,
	// The statement handleInitDB synthesizes for COM_INIT_DB.
	`SET database = "mydb"`,
	`SET character_set_results = 'utf8'`,
	`SET character_set_results = DEFAULT`,
	// NULLIF is a different word, and this statement is evaluated: it reaches
	// the variable with the value 'a'. GLOBAL is a scope KWDB does not have.
	`SET character_set_results = NULLIF('a','b')`,
	`SET GLOBAL character_set_results = NULL`,
	`SET character_set_client = NULL`,
	`SELECT 1; SET character_set_results = NULL`,
	// Transaction modes this package must never absorb: SET SESSION
	// TRANSACTION and SET SESSION CHARACTERISTICS AS TRANSACTION share a
	// grammar rule but not an execution path, so a mode list rewritten from one
	// to the other can lose PRIORITY or AS OF SYSTEM TIME silently.
	`SET SESSION TRANSACTION PRIORITY HIGH`,
	`SET SESSION TRANSACTION AS OF SYSTEM TIME '-3600s'`,
	`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED PRIORITY HIGH`,
	`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY`,
	`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`,
	`SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`,
	`SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED`,
	// The write position is not supported: stripping the sigils would turn
	// `SET x = @@y` into a statement KWDB accepts with a wrong value.
	`SET @@sql_mode = 'x'`,
	`SET @@GLOBAL.sql_mode='x'`,
	`SELECT myuser()`,
	// KWDB answers both of these itself; a zero-argument table row for
	// either would shadow a working builtin with a rewrite.
	`SELECT current_user()`,
	`SELECT session_user()`,
	`SELECT t.USER()`,
	`SELECT "user"()`,
	// Valid today: it returns the current user.
	`SELECT user`,

	`SELECT @@a'--`,
	`SELECT @@a"b"`,
	`SELECT @@a)`,
	`SELECT @@a+1`,
	`SELECT @@a::INT`,
	`SELECT @@global.read_only`,
	`SELECT @@`,
	`SELECT @@@a`,
	`SELECT @@ .x`,
	`SELECT @@中文`,
	`SELECT @@session.a.b`,
	`SELECT @@1`,
	`SELECT @@a, database()`,
	`SELECT @@a, '@@b'`,
	`SELECT 1; SELECT @@a`,
	`/* a /* b */ SELECT @@x`,
	"# c\nSELECT @@x",
	`SELECT@@a`,
	`SELECT @@a AS`,
	`SELECT @@a AS "x`,
}

// dialectRewritten is the other half of the property corpus. A property that
// only ever sees input nothing touches proves nothing.
var dialectRewritten = []string{
	`SELECT @@version_comment`,
	`select @@version_comment limit 1`,
	`SELECT @@sql_mode`,
	`SELECT @@version`,
	`SELECT @@autocommit`,
	`SELECT USER()`,
	`SELECT CONNECTION_ID()`,
	`SELECT @@a, @@b`,
	`SELECT @@a,@@b`,
	`SELECT @@a AS x, @@b`,
	`SELECT @@session.transaction_isolation`,
	`SELECT @@local.time_zone`,
	`/* c */SELECT @@a`,
	"-- c\nSELECT @@a",
	`SELECT @@a; SELECT 1`,
	`SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'`,
	`SET character_set_results = NULL`,
	`SET SESSION character_set_results TO NULL;`,
	`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`,
	`SET SESSION TRANSACTION READ ONLY`,
	`set session transaction read write;`,
	driverProbeQuery,
}

// dialectCorpus is the shared input set for the properties every rewrite has to
// hold whatever it does to any single statement.
var dialectCorpus = append(append([]string{}, dialectUntouched...), dialectRewritten...)

// TestDialectPreservesValidStatements asserts byte-for-byte identity, not
// merely "still parses". A rewrite that reformatted a statement it had no
// reason to touch would make every later error message quote SQL the client
// never sent, and would do it on exactly the statements nobody thinks to check.
func TestDialectPreservesValidStatements(t *testing.T) {
	for _, in := range dialectUntouched {
		if got := rewriteMySQLDialect(in, dialectContext{}); got != in {
			t.Errorf("rewriteMySQLDialect(%q) = %q, want it unchanged", in, got)
		}
	}
}

// TestDialectRewriteIsIdempotent pins that no rewrite recognizes its own
// output. Without it, a second pass -- which the prepare path is one refactor
// away from making -- could substitute into an expression this package built.
func TestDialectRewriteIsIdempotent(t *testing.T) {
	for _, in := range dialectCorpus {
		once := rewriteMySQLDialect(in, dialectContext{})
		if twice := rewriteMySQLDialect(once, dialectContext{}); twice != once {
			t.Errorf("rewriteMySQLDialect(%q) is not idempotent:\n once %q\ntwice %q",
				in, once, twice)
		}
	}
}

// TestDialectRewritesAreDisjoint pins that at most one rewrite fires on any
// input. That is what makes the order they run in a detail rather than a
// contract, and it is the property a rewrite added later is most likely to
// break.
func TestDialectRewritesAreDisjoint(t *testing.T) {
	rewrites := []struct {
		name string
		fn   func(string) string
	}{
		{"stripSetNamesCollate", stripSetNamesCollate},
		{"rewriteSetCharsetResultsNull", rewriteSetCharsetResultsNull},
		{"rewriteSetSessionTransaction", rewriteSetSessionTransaction},
		{"rewriteSelectCompat", func(q string) string {
			return rewriteSelectCompat(q, dialectContext{})
		}},
	}
	for _, in := range dialectCorpus {
		var fired []string
		for _, r := range rewrites {
			if r.fn(in) != in {
				fired = append(fired, r.name)
			}
		}
		if len(fired) > 1 {
			t.Errorf("%q is claimed by more than one rewrite: %v", in, fired)
		}
	}
}

// generatedLiteral matches the string literal a generated current_setting call
// carries.
var generatedLiteral = regexp.MustCompile(`current_setting\('([^']*)'`)

// TestDialectRewriteQuotesNothingUnsafe pins rule R4 against the output rather
// than trusting the scanner: every byte reaching a generated string literal
// comes from a folded ASCII variable name, so a quote can never get in and
// close the literal early. This is the assertion that catches an injection if
// the name scanner is ever widened.
func TestDialectRewriteQuotesNothingUnsafe(t *testing.T) {
	for _, in := range dialectCorpus {
		out := rewriteMySQLDialect(in, dialectContext{})
		if out == in {
			continue
		}
		for _, m := range generatedLiteral.FindAllStringSubmatch(out, -1) {
			for i := 0; i < len(m[1]); i++ {
				b := m[1][i]
				if !(b == '_' || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')) {
					t.Errorf("rewriteMySQLDialect(%q) put byte %q into the literal %q",
						in, b, m[1])
				}
			}
		}
	}
}

// TestDialectRewriteAddsNoPlaceholder pins that a rewrite never introduces a
// `?`. On the prepare path rewritePlaceholders runs first and numbers the
// placeholders; one conjured afterwards would leave client and server
// disagreeing about how many parameters the statement has.
func TestDialectRewriteAddsNoPlaceholder(t *testing.T) {
	for _, in := range dialectCorpus {
		out := rewriteMySQLDialect(in, dialectContext{})
		if got, want := strings.Count(out, "?"), strings.Count(in, "?"); got != want {
			t.Errorf("rewriteMySQLDialect(%q) = %q: %d question mark(s), want %d",
				in, out, got, want)
		}
	}
}

// TestDialectDispatchLeavesSetNamesToItsOwnRewrite pins that routing SET NAMES
// through the dispatcher changes nothing about how it is handled. That rewrite
// predates this file and TestStripSetNamesCollate pins its behavior in detail.
func TestDialectDispatchLeavesSetNamesToItsOwnRewrite(t *testing.T) {
	for _, in := range []string{
		`SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'`,
		`SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;`,
		`SET NAMES utf8mb4`,
		`SELECT a FROM t ORDER BY a COLLATE 'en_US'`,
	} {
		want := stripSetNamesCollate(in)
		if got := rewriteMySQLDialect(in, dialectContext{}); got != want {
			t.Errorf("rewriteMySQLDialect(%q) = %q, want %q", in, got, want)
		}
	}
}
