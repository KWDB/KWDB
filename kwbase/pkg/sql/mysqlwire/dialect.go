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

// dialectContext carries the per-connection facts a rewrite may need to answer
// with. It is a value rather than a *conn so that every rewrite stays a pure
// function a table test can drive without a socket.
type dialectContext struct {
	// connID is the number this connection already reported in its handshake
	// greeting. Anything answering MySQL's CONNECTION_ID() has to use that same
	// number, because the value exists only in the protocol layer and the SQL
	// layer cannot be asked for it.
	connID uint32
}

// rewriteMySQLDialect turns the MySQL-only spellings a client sends into SQL
// KWDB's parser accepts. Both command paths go through it, so COM_QUERY and
// COM_STMT_PREPARE cannot drift apart.
//
// Each rewrite recognizes a different leading shape -- SET NAMES, SET
// character_set_results, SET SESSION TRANSACTION, SELECT -- so at most one can
// fire on any input, and none recognizes another's output.
// Their order is therefore not load-bearing; TestDialectRewritesAreDisjoint and
// TestDialectRewriteIsIdempotent pin both properties rather than leaving them
// to the reader.
//
// Every one of them returns its input unchanged for anything it cannot fully
// recognize. That is the contract stripSetNamesCollate states, for the same
// reason: guessing wrong here corrupts a statement rather than merely failing
// to parse it.
//
// Recognition is deliberately prefix-shaped. A rewrite only replaces bytes at
// the end of a run it has read byte by byte from offset 0, and abandons the
// whole statement at the first byte it does not recognize. It therefore never
// has to decide whether an `@@` somewhere inside a statement sits in a string
// literal, a comment or a quoted identifier: it cannot reach one. A general
// whole-statement token scanner would instead have to get literals, three
// comment forms, backticks, e-prefixed literals and dollar quoting all right,
// and this package
// has already been bitten once by two of its scanners disagreeing about where
// a literal ends.
func rewriteMySQLDialect(query string, dc dialectContext) string {
	query = stripSetNamesCollate(query)
	query = rewriteSetCharsetResultsNull(query)
	query = rewriteSetSessionTransaction(query)
	query = rewriteSelectCompat(query, dc)
	return query
}

// skipSpaces returns the offset of the first byte at or after i that is not
// whitespace.
func skipSpaces(s string, i int) int {
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	return i
}

// skipLeadingTrivia returns the offset of the first byte at or after i that can
// begin a statement, stepping over whitespace, block comments and `--` line
// comments.
//
// `#` is deliberately not trivia. In KWDB it is the bitwise XOR operator --
// `SELECT 5 # 3` evaluates to 6, and `SELECT 1 # nosuchcol` fails on the column
// name rather than on syntax -- so reading it as a comment introducer would
// swallow the right operand of a valid expression.
//
// Block comments are read with placeholder.go's scanBlockComment, which counts
// nesting depth. The rule to follow is the one held by the parser that finally
// consumes this output, and KWDB nests block comments the way PostgreSQL does.
// An unterminated comment runs to the end of the input, which leaves no keyword
// for the caller to match and so fails open.
func skipLeadingTrivia(s string, i int) int {
	for i < len(s) {
		switch {
		case isSpace(s[i]):
			i++
		case s[i] == '/' && byteAt(s, i+1) == '*':
			i = scanBlockComment(s, i)
		case s[i] == '-' && byteAt(s, i+1) == '-':
			n := strings.IndexByte(s[i:], '\n')
			if n < 0 {
				return len(s)
			}
			i += n + 1
		default:
			return i
		}
	}
	return i
}

// matchKeyword matches kw at i case-insensitively and, on success, advances *i
// past it. The byte after the keyword must not be able to continue an
// identifier, so `SELECT@@a` matches while `SELECTED` does not, and neither
// does a name that merely starts with the keyword's bytes.
func matchKeyword(s string, i *int, kw string) bool {
	j := *i
	if len(s)-j < len(kw) || !strings.EqualFold(s[j:j+len(kw)], kw) {
		return false
	}
	if isNameByte(byteAt(s, j+len(kw))) {
		return false
	}
	*i = j + len(kw)
	return true
}

// scanVarName reads one identifier at i, returning it and the offset just past
// it. The character set is narrower than isNameByte's on purpose: a MySQL
// system variable name is ASCII, and letting a multi-byte byte into the string
// literal this package builds around the name is exactly what rule R4 forbids.
// A leading digit is refused so that `@@1` is not read as a name.
func scanVarName(s string, i int) (string, int, bool) {
	if i >= len(s) {
		return "", i, false
	}
	if c := s[i]; !(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
		return "", i, false
	}
	j := i + 1
	for j < len(s) && isIdentByte(s[j]) {
		j++
	}
	return s[i:j], j, true
}

// isNameByte reports whether b can continue an identifier. It is deliberately
// wider than placeholder.go's isIdentByte: KWDB, like PostgreSQL, allows
// non-ASCII bytes in an identifier, and a keyword must not be seen to end in
// the middle of one.
func isNameByte(b byte) bool { return isIdentByte(b) || b >= 0x80 }

// isItemBoundary reports whether b may immediately follow an expression this
// package rewrote.
//
// It deliberately does not treat a quote as a separator, unlike setnames.go's
// isSeparator. That one counts quotes so `COLLATE'x'` can be scanned; the same
// rule here would accept `SELECT @@a'--`, splicing a rewritten expression onto
// a quote the client opened, and would let `SELECT @@a"b"` fuse the synthesized
// column label with the client's own quote into an escaped one.
func isItemBoundary(b byte) bool {
	return isSpace(b) || b == ',' || b == ';'
}
