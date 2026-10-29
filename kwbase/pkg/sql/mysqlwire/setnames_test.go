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

import "testing"

func TestStripSetNamesCollate(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		// The statement mysql-connector-python sends unconditionally on connect.
		{"quoted charset and collation",
			"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'",
			"SET NAMES 'utf8mb4'"},
		{"bare charset and collation",
			"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
			"SET NAMES utf8mb4"},
		{"lowercase keywords",
			"set names 'utf8mb4' collate 'utf8mb4_general_ci'",
			"set names 'utf8mb4'"},
		{"trailing semicolon is kept",
			"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;",
			"SET NAMES utf8mb4;"},
		{"leading whitespace",
			"  SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
			"  SET NAMES utf8mb4"},

		// Untouched: no COLLATE to remove.
		{"no collate", "SET NAMES utf8mb4", "SET NAMES utf8mb4"},
		{"bare set names", "SET NAMES", "SET NAMES"},

		// Untouched: not a SET NAMES statement. COLLATE is meaningful here and
		// removing it would change the query's result.
		{"collate in a select",
			"SELECT a FROM t ORDER BY a COLLATE 'en_US'",
			"SELECT a FROM t ORDER BY a COLLATE 'en_US'"},
		{"collate in a set of another var",
			"SET client_encoding = 'utf8' COLLATE 'x'",
			"SET client_encoding = 'utf8' COLLATE 'x'"},
		{"set names inside a string literal of another statement",
			"SELECT 'SET NAMES utf8 COLLATE c'",
			"SELECT 'SET NAMES utf8 COLLATE c'"},

		// Untouched: the word COLLATE appears inside the charset literal rather
		// than as a clause, so there is nothing to strip.
		{"collate inside a quoted charset",
			"SET NAMES 'utf8 COLLATE x'",
			"SET NAMES 'utf8 COLLATE x'"},

		// A doubled quote inside the collation literal is one literal quote,
		// not the end of the literal — the same rule scanSingleQuoted applies
		// in placeholder.go. Misreading '' as the closing quote would leave a
		// stray "'b'" to glue onto the head after the (mis-detected) end of
		// the statement, producing a rewrite performed after a misread, which
		// the fail-open contract forbids. Read correctly, 'a''b' is one
		// well-formed collation value like any other, so it is recognized and
		// stripped in full.
		{"doubled quote in collation",
			"SET NAMES 'utf8' COLLATE 'a''b'",
			"SET NAMES 'utf8'"},

		// Untouched: the collation value is unquoted and abuts a quote with no
		// separating space — already malformed SQL, since a bare word cannot run
		// directly into a string literal. scanValue's bare-word branch stops at
		// the quote (a quote is a separator, the same rule that lets COLLATE'x'
		// parse when the quote opens the value itself) and leaves the quote
		// dangling in tail. Splicing that onto head would fuse two literals into
		// one or open an unterminated one, which the fail-open contract forbids;
		// the fix is to notice the dirty tail and return the input unchanged.
		{"unquoted collation abutting a quote fuses two literals",
			"SET NAMES 'utf8' COLLATE e'x'",
			"SET NAMES 'utf8' COLLATE e'x'"},
		{"unquoted collation abutting a quote opens an unterminated literal",
			"SET NAMES utf8 COLLATE c'",
			"SET NAMES utf8 COLLATE c'"},

		// The only guard against rewriting SET NAMESPACE: NAMES must not match
		// as a prefix of a longer keyword.
		{"namespace near miss", "SET NAMESPACE utf8 COLLATE c", "SET NAMESPACE utf8 COLLATE c"},
		// The only exercise of the head+tail arithmetic with a non-empty tail
		// that is itself more than just a bare semicolon.
		{"multi statement", "SET NAMES utf8 COLLATE c; SELECT 1", "SET NAMES utf8; SELECT 1"},
		// COLLATE present but with nothing after it: scanValue on the empty
		// remainder must fail open rather than panic or misindex.
		{"dangling collate", "SET NAMES utf8 COLLATE", "SET NAMES utf8 COLLATE"},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripSetNamesCollate(tc.in); got != tc.want {
				t.Errorf("stripSetNamesCollate(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}
