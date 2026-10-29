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
	"strconv"
	"strings"
)

// txIsolationExpr changes the spelling of the isolation level and nothing else.
// The session variable behind it reports the level the statement actually runs
// at, so the answer stays true. Connector/J looks it up in a case-sensitive map
// keyed READ-COMMITTED / REPEATABLE-READ / SERIALIZABLE, which KWDB's
// `serializable` matches nowhere: during connect the driver silently keeps a
// wrong answer, and a later getTransactionIsolation() throws SQLState S1000.
const txIsolationExpr = `upper(replace(current_setting('transaction_isolation', true), ' ', '-'))`

// boolAsBitExpr renders an on/off session variable the way MySQL spells a
// boolean. KWDB answers `on`/`off`; the driver runs Boolean.parseBoolean over
// the autocommit answer, where "on" reads as false, and Integer.parseInt over
// the read-only one, where "off" throws. Both failures are silent or
// misattributed, so the shape has to change here.
//
// An unknown name lands in the ELSE branch as NULL rather than raising, which
// is what rule R5 requires of every expression in this file.
func boolAsBitExpr(name string) string {
	return fmt.Sprintf(
		"CASE current_setting('%s', true) WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE NULL END", name)
}

// mysqlSystemVarExpr overrides what a given `@@name` is rewritten to, for the
// names where handing a MySQL client KWDB's own answer would have it misread
// the value. Anything not listed falls through to sysVarExpr, which is why the
// table is short: a variable KWDB already carries under its MySQL name needs no
// row, and reading it through current_setting makes the answer follow the live
// session instead of freezing it into a constant.
//
// Keys are lower case because that is what the scanner looks them up with; an
// upper-case key would be a line that can never match.
var mysqlSystemVarExpr = map[string]string{
	// KWDB stores and emits UTF-8 and nothing else, so there is no server-side
	// charset setting to read. Answering with a constant states a fact about
	// the server rather than inventing a knob to hold it.
	"character_set_server": `'utf8mb4'`,

	// MySQL's 1 makes the driver report storesLowerCaseIdentifiers() and fold
	// names before it compares metadata, which matches KWDB's treatment of
	// unquoted identifiers -- the path almost every client takes. Answering 0
	// would stop the driver normalising at all, which is worse for that path.
	// The residual difference (KWDB keeps the case of a quoted identifier,
	// MySQL under 1 does not) is a known deviation, not an oversight.
	"lower_case_table_names": `'1'`,

	// Both appended modes are true statements about KWDB's lexer rather than
	// compatibility flattery: `"` really does quote identifiers and a backtick
	// really is a syntax error (ANSI_QUOTES), and standard_conforming_strings
	// is pinned on, so a backslash inside a literal is a backslash
	// (NO_BACKSLASH_ESCAPES). A client not told the latter escapes with
	// backslashes and corrupts every value carrying one. coalesce keeps the
	// whole expression NULL-safe per rule R5, which matters because sql_mode
	// is an ignored variable whose value cannot be set.
	"sql_mode": `coalesce(current_setting('sql_mode', true) || ',', '') || ` +
		`'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'`,

	"transaction_isolation": txIsolationExpr,
	// tx_isolation and tx_read_only are the MySQL 5.7 names for the same two
	// values. Connector/J 8.4.0 does not ask for them, but tools that predate
	// 8.0 do, and answering costs one table row.
	"tx_isolation": txIsolationExpr,

	"autocommit":            boolAsBitExpr("autocommit"),
	"transaction_read_only": boolAsBitExpr("transaction_read_only"),
	"tx_read_only":          boolAsBitExpr("transaction_read_only"),

	// Assembled from the same constant the handshake greeting reports, so the
	// server cannot end up telling two version stories. This coexists with
	// SELECT version(), which answers with the product identity: version() is
	// who this server is, @@version is the dialect level it claims on 3306.
	"version": `'` + serverVersion + `'`,
}

// mysqlZeroArgFunc maps a MySQL zero-argument function to the expression that
// answers it. The table is closed on purpose: a name absent from it fails the
// whole statement open, which is what keeps SELECT database(), SELECT now() and
// every other genuine builtin byte for byte untouched.
//
// current_user() and session_user() are deliberately absent although MySQL has
// both. KWDB already answers them, and a row here would shadow a working
// builtin with an expression built from what this package alone knows.
//
// Keys are lower case because that is what the scanner looks them up with; an
// upper-case key would be a line that can never match.
var mysqlZeroArgFunc = map[string]func(dialectContext) string{
	// MySQL's USER() names the account the session authenticated as. KWDB
	// accounts are not host-qualified, so there is no `@host` half being left
	// out: current_user() is the whole answer rather than a truncation of one.
	//
	// USER is a reserved word in the grammar, so SELECT USER() fails at the
	// opening paren rather than at builtin lookup. A builtin registered outside
	// this package would never be reached; only sql.y could change that, and
	// that would carry the MySQL spelling over to pgwire clients too.
	"user": func(dialectContext) string { return "current_user()" },

	// CONNECTION_ID() has to answer with the number this connection already
	// reported in its greeting, so it is inlined as a literal carried in from
	// the connection rather than resolved by the SQL layer. The value exists
	// only in the protocol layer, and a second place to get it from would
	// eventually be a second answer.
	"connection_id": func(dc dialectContext) string {
		return strconv.FormatUint(uint64(dc.connID), 10)
	},
}

// sysVarExpr is what a `@@name` with no row in mysqlSystemVarExpr becomes.
//
// The two-argument form of current_setting yields SQL NULL for a name the
// server does not know, where the one-argument form raises 42704. That matters
// because the driver asks for nineteen of these in a single statement during
// connect: with the raising form, one variable renamed or removed anywhere in
// KWDB would turn a degraded answer into a refused connection. NULL means "this
// server has no value under that name", which is the truth.
func sysVarExpr(name string) string {
	return fmt.Sprintf("current_setting('%s', true)", name)
}

// rewriteSelectCompat replaces the MySQL-only expressions in a leading SELECT's
// item list -- system variable references and the zero-argument functions in
// mysqlZeroArgFunc -- with expressions KWDB can evaluate.
//
// Every form recognized here fails today, so nothing that works now can change
// meaning: `@@` is not one token to KWDB's scanner -- it lexes as two separate
// `@` bytes and appears nowhere in the grammar -- while USER() fails at the
// paren because USER is a reserved word, and CONNECTION_ID() is a function
// KWDB does not have.
//
// Recognition is all-or-nothing (rule R2): if any item in the list is not one
// this understands, the original string is returned and the parser sees exactly
// what the client wrote. A partially rewritten item list would be a statement
// nobody sent. The cost is the mixed forms -- `SELECT id, @@sql_mode FROM t` is
// left alone -- which is a deliberate trade of reach for safety; the driver's
// connection-phase queries are pure `@@` lists.
//
// Only a leading SELECT is examined. `SELECT 1; SELECT @@a` is left alone, the
// same limit stripSetNamesCollate documents and for the same reason: there is
// no statement splitter here.
func rewriteSelectCompat(query string, dc dialectContext) string {
	i := skipLeadingTrivia(query, 0)
	if !matchKeyword(query, &i, "SELECT") {
		return query
	}
	// The item list has to be separated from the keyword by whitespace. Rule R1
	// copies the bytes ahead of the item verbatim, so rewriting `SELECT@@a`
	// would splice the replacement straight onto the keyword and emit
	// `SELECTcurrent_setting(...)`. Both forms are a syntax error; the fused one
	// is the harder of the two to read.
	if skipSpaces(query, i) == i {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 64)
	last := 0
	for {
		j := skipSpaces(query, i)
		repl, orig, exprEnd, ok := scanCompatExpr(query, j, dc)
		if !ok {
			return query
		}
		// Rule R3: the expression has to end where a select item may end. A
		// quote, an operator or a `(` here means the client wrote something
		// larger than what was recognized, and substituting into the middle of
		// it would produce a statement with a different meaning.
		if exprEnd < len(query) && !isItemBoundary(query[exprEnd]) {
			return query
		}
		aliasEnd, hasAlias, ok := scanOptionalAlias(query, exprEnd)
		if !ok {
			return query
		}

		// Rule R1: everything already read -- leading trivia, the SELECT
		// keyword, the whitespace and commas between items -- is copied byte
		// for byte, so an error message still quotes the SQL the client sent.
		b.WriteString(query[last:j])
		b.WriteString(repl)
		if !hasAlias {
			// MySQL labels the column with the expression as written, e.g.
			// `@@session.autocommit`. Without this the label would become
			// `current_setting`, and a client that reads its result set by
			// label would take the wrong column or none at all.
			if strings.ContainsRune(orig, '"') {
				// Unreachable by construction: orig is `@@`, an optional
				// whitelisted scope and an ASCII name. Asserting it anyway
				// keeps the guarantee next to the line that relies on it.
				return query
			}
			b.WriteString(` AS "` + orig + `"`)
		}
		b.WriteString(query[exprEnd:aliasEnd])
		last = aliasEnd

		k := skipSpaces(query, aliasEnd)
		if k < len(query) && query[k] == ',' {
			i = k + 1
			continue
		}
		// Everything after the last item is the client's own bytes and is not
		// inspected: a FROM, a LIMIT, a further statement, anything at all.
		b.WriteString(query[last:])
		return b.String()
	}
}

// scanCompatExpr recognizes one select item this package can answer, returning
// the expression to substitute, the original text of the item -- which becomes
// the synthesized column label -- and the offset just past it.
//
// The two shapes it knows are told apart by their first bytes alone, so neither
// can consume input meant for the other.
func scanCompatExpr(s string, i int, dc dialectContext) (repl, orig string, end int, ok bool) {
	if byteAt(s, i) == '@' && byteAt(s, i+1) == '@' {
		return scanSystemVar(s, i)
	}
	return scanZeroArgCall(s, i, dc)
}

// scanSystemVar recognizes a `@@name` reference, with an optional scope.
//
// `@@global.` is refused. MySQL's global and session scopes are two value
// spaces and KWDB has only one, so answering a global read with the session
// value would be a plausible wrong answer rather than an error.
//
// A single `@` is never touched. `SELECT @x` is not a syntax error in KWDB --
// it lexes and parses, then fails name resolution with 42704 -- so the boundary
// has to be drawn at "there must be two of them" and cannot lean on `@` being
// invalid.
func scanSystemVar(s string, i int) (repl, orig string, end int, ok bool) {
	name, j, ok := scanVarName(s, i+2)
	if !ok {
		return "", "", i, false
	}
	if byteAt(s, j) == '.' {
		if !strings.EqualFold(name, "session") && !strings.EqualFold(name, "local") {
			return "", "", i, false
		}
		scoped, k, scopeOK := scanVarName(s, j+1)
		if !scopeOK || byteAt(s, k) == '.' {
			return "", "", i, false
		}
		name, j = scoped, k
	}

	lower := strings.ToLower(name)
	expr, found := mysqlSystemVarExpr[lower]
	if !found {
		expr = sysVarExpr(lower)
	}
	return expr, s[i:j], j, true
}

// scanZeroArgCall recognizes a call to one of the functions in
// mysqlZeroArgFunc: a covered name, then a parenthesis pair with nothing
// between them.
//
// The name is looked up before the parentheses are read, so every other
// `something(` in a statement -- database(), now(), a function the client
// defined itself -- ends the scan at its first byte and reaches the parser as
// written. An argument, a table qualifier and a quoted name all fail here for
// the same reason: none of them is the shape this answers.
func scanZeroArgCall(s string, i int, dc dialectContext) (repl, orig string, end int, ok bool) {
	name, j, ok := scanVarName(s, i)
	if !ok {
		return "", "", i, false
	}
	build, covered := mysqlZeroArgFunc[strings.ToLower(name)]
	if !covered {
		return "", "", i, false
	}
	j = skipSpaces(s, j)
	if byteAt(s, j) != '(' {
		// Without the parentheses this is a bare word, and `SELECT user` is a
		// statement KWDB answers correctly today.
		return "", "", i, false
	}
	j = skipSpaces(s, j+1)
	if byteAt(s, j) != ')' {
		return "", "", i, false
	}
	return build(dc), s[i : j+1], j + 1, true
}

// scanOptionalAlias returns the offset just past an explicit `AS <alias>`
// following the expression that ended at i, and whether one was found. It
// reports false only for an `AS` it cannot read to the end, which fails the
// whole statement open.
//
// An implicit alias -- a bare word with no AS -- is deliberately not
// recognized: nothing distinguishes it from the keyword that starts the next
// clause, and reading `limit` in `SELECT @@version_comment limit 1` as a column
// label would destroy both the label and the statement. Without an AS the item
// simply gets its synthesized label and the rest of the statement passes
// through untouched.
func scanOptionalAlias(s string, i int) (end int, hasAlias bool, ok bool) {
	j := skipSpaces(s, i)
	if j == i {
		// No whitespace after the expression, so no `AS` can follow it.
		return i, false, true
	}
	k := j
	if !matchKeyword(s, &k, "AS") {
		return i, false, true
	}
	m := skipSpaces(s, k)
	if m == k || m == len(s) {
		return i, false, false
	}

	if s[m] == '"' {
		n := scanDoubleQuoted(s, m)
		if n <= m+1 || s[n-1] != '"' {
			// Unterminated: the parser will reject the statement, and it has to
			// see the client's own bytes when it does.
			return i, false, false
		}
		return n, true, true
	}
	if c := s[m]; !isNameByte(c) || (c >= '0' && c <= '9') {
		return i, false, false
	}
	n := m + 1
	for n < len(s) && isNameByte(s[n]) {
		n++
	}
	return n, true, true
}
