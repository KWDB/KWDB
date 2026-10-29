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

// rewritePlaceholders converts MySQL's positional `?` placeholders into the
// `$1`, `$2`, ... form KWDB's parser accepts, and reports how many it replaced.
//
// The translation cannot be done in the grammar. KWDB's scanner already gives
// `?` a meaning: it leads the JSON existence operators `?`, `?|` and `?&`
// inherited from PostgreSQL. Teaching the grammar to also read a bare `?` as a
// placeholder would make `data ? 'key'` ambiguous. Doing it here confines the
// MySQL reading of `?` to the MySQL port, where it is the only reading a
// client can intend.
//
// Only a bare `?` is rewritten. The two-character JSON operators are left
// alone: no MySQL client emits them, and a placeholder immediately followed by
// `|` or `&` is not valid MySQL either, so treating them as operators cannot
// misread a client's intent. `??` is likewise skipped — it is KWDB's help
// token, and a pair of adjacent placeholders is not legal MySQL.
//
// Text inside string literals, quoted identifiers and comments is copied
// verbatim. Rewriting a `?` there would corrupt data rather than merely fail
// to parse, which is why this scans the statement rather than replacing on the
// whole string.
func rewritePlaceholders(query string) (string, int) {
	// Nothing to do for the overwhelmingly common case of a statement with no
	// question mark anywhere, including inside literals.
	if !strings.ContainsRune(query, '?') {
		return query, 0
	}

	var b strings.Builder
	b.Grow(len(query) + 8)

	count := 0
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '\'':
			// A single-quoted string. An e/E prefix makes backslash an escape
			// character; otherwise only a doubled quote escapes.
			backslashEscapes := i > 0 && (query[i-1] == 'e' || query[i-1] == 'E') &&
				!isIdentByte(byteAt(query, i-2))
			n := scanSingleQuoted(query, i, backslashEscapes)
			b.WriteString(query[i:n])
			i = n

		case c == '"':
			n := scanDoubleQuoted(query, i)
			b.WriteString(query[i:n])
			i = n

		case c == '-' && byteAt(query, i+1) == '-':
			n := strings.IndexByte(query[i:], '\n')
			if n < 0 {
				b.WriteString(query[i:])
				i = len(query)
			} else {
				b.WriteString(query[i : i+n])
				i += n
			}

		case c == '/' && byteAt(query, i+1) == '*':
			n := scanBlockComment(query, i)
			b.WriteString(query[i:n])
			i = n

		case c == '$':
			// Either a dollar-quoted string ($tag$...$tag$) or a $N the client
			// wrote itself. scanDollar returns the end of whichever it is, so
			// a placeholder the client already numbered passes through
			// untouched.
			n := scanDollar(query, i)
			b.WriteString(query[i:n])
			i = n

		case c == '?':
			// Leave the JSON operators and the help token alone; see above.
			if next := byteAt(query, i+1); next == '|' || next == '&' || next == '?' {
				b.WriteString(query[i : i+2])
				i += 2
				continue
			}
			count++
			b.WriteByte('$')
			b.WriteString(itoa(count))
			i++

		default:
			b.WriteByte(c)
			i++
		}
	}

	if count == 0 {
		// Every `?` was quoted or an operator, so the statement is unchanged.
		return query, 0
	}
	return b.String(), count
}

// byteAt returns the byte at i, or 0 when i is outside s. Returning a
// sentinel rather than panicking keeps the callers above free of bounds
// checks, and 0 never appears in SQL text this cares about.
func byteAt(s string, i int) byte {
	if i < 0 || i >= len(s) {
		return 0
	}
	return s[i]
}

// isIdentByte reports whether b can appear inside an identifier. It exists to
// tell the string prefix in `e'...'` from the tail of a name like `type'...'`,
// where the e belongs to the identifier and starts no escape sequence.
func isIdentByte(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// scanSingleQuoted returns the index just past the string literal starting at
// the quote at i. An unterminated literal consumes the rest of the statement;
// the parser reports it, and this only has to avoid rewriting inside it.
func scanSingleQuoted(s string, i int, backslashEscapes bool) int {
	i++ // opening quote
	for i < len(s) {
		switch s[i] {
		case '\\':
			if backslashEscapes {
				i += 2
				continue
			}
			i++
		case '\'':
			if byteAt(s, i+1) == '\'' {
				// A doubled quote is one literal quote, not the end.
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return len(s)
}

// scanDoubleQuoted returns the index just past the quoted identifier starting
// at i.
func scanDoubleQuoted(s string, i int) int {
	i++
	for i < len(s) {
		if s[i] == '"' {
			if byteAt(s, i+1) == '"' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(s)
}

// scanBlockComment returns the index just past the comment starting at i.
// Block comments nest in PostgreSQL and in KWDB, so this counts depth rather
// than stopping at the first `*/`.
func scanBlockComment(s string, i int) int {
	depth := 0
	for i < len(s) {
		switch {
		case s[i] == '/' && byteAt(s, i+1) == '*':
			depth++
			i += 2
		case s[i] == '*' && byteAt(s, i+1) == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(s)
}

// scanDollar returns the index just past whatever starts with the `$` at i.
//
// A dollar-quoted string ($tag$ ... $tag$) is consumed whole so that a `?`
// inside it survives. Anything else — `$1`, or a stray `$` — advances by one
// byte, leaving it to be copied verbatim.
func scanDollar(s string, i int) int {
	// A tag is an optional identifier between two dollar signs.
	j := i + 1
	for j < len(s) && isIdentByte(s[j]) && !(s[j] >= '0' && s[j] <= '9' && j == i+1) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		// Not a dollar quote: `$1`, `$`, or `$foo` without the closing dollar.
		return i + 1
	}
	tag := s[i : j+1] // includes both dollar signs
	if end := strings.Index(s[j+1:], tag); end >= 0 {
		return j + 1 + end + len(tag)
	}
	// Unterminated; the parser will report it.
	return len(s)
}

// itoa renders a small positive integer without pulling in strconv, which
// matters only in that this runs once per placeholder on the prepare path.
func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
