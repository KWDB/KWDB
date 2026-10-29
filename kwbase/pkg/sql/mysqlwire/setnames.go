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

import "strings"

// stripSetNamesCollate removes the COLLATE clause from a MySQL `SET NAMES`
// statement, leaving the charset for KWDB's client_encoding to handle.
//
// MySQL spells this `SET NAMES charset [COLLATE collation]`; PostgreSQL, whose
// grammar KWDB inherits, has no COLLATE on SET NAMES. Both spellings fail
// today, in different ways. Unquoted, `utf8mb4 COLLATE utf8mb4_0900_ai_ci`
// parses as a single collated expression — a_expr COLLATE collation_name is
// valid — so the whole string arrives as the client_encoding value and is
// rejected as an invalid value. Quoted, collation_name is an unrestricted_name
// and will not accept a string literal, so it is a syntax error. Connector
// libraries send the quoted form on every connect, which makes this the
// difference between connecting and not.
//
// Doing this here rather than in the grammar follows rewritePlaceholders: it
// confines a MySQL-only reading to the MySQL port instead of handing MySQL
// syntax to PostgreSQL clients. Adding a NAMES var_value COLLATE production
// would also collide with the existing a_expr COLLATE collation_name rule.
//
// Dropping the collation loses nothing. KWDB compares strings with a single
// fixed collation, which is why collation_connection and its siblings are
// already accepted and ignored as session variables.
//
// Anything this cannot confidently recognize is returned unchanged, leaving the
// SQL layer to reject it as before. Guessing wrong here would corrupt a
// statement rather than merely fail to parse it.
//
// This only recognizes SET NAMES when it leads the query: a later occurrence
// in a semicolon-batched statement, e.g. `SELECT 1; SET NAMES utf8 COLLATE x`,
// is not stripped. That limit is intentional, not an oversight — handling it
// would need a statement splitter, which this MySQL port does not have.
func stripSetNamesCollate(query string) string {
	rest, ok := consumeKeyword(query, "SET")
	if !ok {
		return query
	}
	rest, ok = consumeKeyword(rest, "NAMES")
	if !ok {
		return query
	}

	// Skip the charset: one quoted literal or one bare word.
	charsetEnd, ok := scanValue(rest)
	if !ok {
		return query
	}
	afterCharset := rest[charsetEnd:]

	collateRest, ok := consumeKeyword(afterCharset, "COLLATE")
	if !ok {
		return query
	}
	collationEnd, ok := scanValue(collateRest)
	if !ok {
		return query
	}

	// Everything the statement carries after the collation — a semicolon, say —
	// is preserved so the caller still sees the statement it sent.
	head := query[:len(query)-len(afterCharset)]
	tail := collateRest[collationEnd:]
	if tail != "" && !isSpace(tail[0]) && tail[0] != ';' {
		// scanValue's bare-word branch treats a quote as a separator (needed so
		// a directly-abutting quoted value, e.g. COLLATE'x', still scans), so an
		// unquoted collation that itself abuts a quote with no space — already
		// malformed SQL, e.g. `COLLATE e'x'` or `COLLATE c'` — gets cut short
		// there, leaving the quote and everything after it dangling in tail.
		// Splicing that onto head would fuse two literals into one or open an
		// unterminated one; a clean tail is the only case the fail-open contract
		// above allows this function to rewrite.
		return query
	}
	return strings.TrimRight(head, " \t\r\n") + tail
}

// consumeKeyword matches a case-insensitive keyword at the start of s, after
// leading whitespace, and returns what follows it. It requires the keyword to
// end at a boundary so that `NAMESPACE` does not match `NAMES`.
func consumeKeyword(s, kw string) (string, bool) {
	s = strings.TrimLeft(s, " \t\r\n")
	if len(s) < len(kw) || !strings.EqualFold(s[:len(kw)], kw) {
		return "", false
	}
	rest := s[len(kw):]
	if rest != "" && !isSeparator(rest[0]) {
		return "", false
	}
	return rest, true
}

// scanValue reports the end offset of one value at the start of s, after
// leading whitespace: either a single-quoted literal or a bare word. It returns
// false when s holds neither, which is the signal to leave the query alone.
//
// The quoted branch has to agree with scanSingleQuoted in placeholder.go,
// which does the same job for this package's other MySQL-syntax rewrite: a
// doubled quote is one literal quote, not the end of the literal. Getting
// this wrong does not just fail to parse — it misreads where the literal
// ends, so a later value can be spliced onto an earlier one after the quote
// character is silently miscounted, producing a rewrite the fail-open
// contract forbids. Unlike scanSingleQuoted, this does not treat a bare
// backslash as starting an escape: that rule only applies after an e/E
// string prefix, which never appears here — not because the byte
// immediately before the quote is always whitespace (it need not be, e.g.
// COLLATE'x' leaves the quote directly preceded by COLLATE's own trailing
// E), but because consumeKeyword has already consumed that keyword as its
// own token before scanValue ever runs, so the preceding letter is never
// part of the value scanValue itself is scanning. scanValue must still
// return the `false` signal to fail open on an unterminated literal, which
// scanSingleQuoted's len(s) fallback would lose.
func scanValue(s string) (int, bool) {
	i := 0
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	if i == len(s) {
		return 0, false
	}
	if s[i] == '\'' || s[i] == '"' || s[i] == '`' {
		quote := s[i]
		for j := i + 1; j < len(s); j++ {
			if s[j] != quote {
				continue
			}
			if j+1 < len(s) && s[j+1] == quote {
				j++ // a doubled quote is one literal quote, not the end
				continue
			}
			return j + 1, true
		}
		// Unterminated quote: not something to rewrite.
		return 0, false
	}
	start := i
	for i < len(s) && !isSeparator(s[i]) {
		i++
	}
	if i == start {
		return 0, false
	}
	return i, true
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// isSeparator reports whether b can end an unquoted keyword or word. `;` ends a
// statement and the quote characters can begin the next token without space.
func isSeparator(b byte) bool {
	return isSpace(b) || b == ';' || b == '\'' || b == '"' || b == '`'
}
