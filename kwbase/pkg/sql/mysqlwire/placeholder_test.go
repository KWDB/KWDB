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

// TestRewritePlaceholders covers the cases where getting the scan wrong would
// corrupt a statement rather than merely fail to parse it.
func TestRewritePlaceholders(t *testing.T) {
	for _, tc := range []struct {
		name  string
		in    string
		want  string
		count int
	}{
		{
			name: "no placeholders",
			in:   "SELECT 1", want: "SELECT 1", count: 0,
		},
		{
			name: "single",
			in:   "SELECT * FROM t WHERE id = ?",
			want: "SELECT * FROM t WHERE id = $1", count: 1,
		},
		{
			name: "numbered by position",
			in:   "INSERT INTO t VALUES (?, ?, ?)",
			want: "INSERT INTO t VALUES ($1, $2, $3)", count: 3,
		},
		{
			name: "ten or more",
			in:   "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
			want: "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", count: 11,
		},

		// A `?` inside a literal is data. Rewriting it would silently change
		// what the statement stores or matches.
		{
			name: "inside string literal",
			in:   "SELECT * FROM t WHERE name = 'why?' AND id = ?",
			want: "SELECT * FROM t WHERE name = 'why?' AND id = $1", count: 1,
		},
		{
			name: "doubled quote inside literal",
			in:   "SELECT 'it''s a ?' , ?",
			want: "SELECT 'it''s a ?' , $1", count: 1,
		},
		{
			name: "escaped quote in e-string",
			in:   `SELECT e'a\'? b', ?`,
			want: `SELECT e'a\'? b', $1`, count: 1,
		},
		{
			name: "backslash is literal in a plain string",
			in:   `SELECT 'a\', ?`,
			want: `SELECT 'a\', $1`, count: 1,
		},
		{
			name: "identifier ending in e does not start an e-string",
			in:   `SELECT type'?', ?`,
			want: `SELECT type'?', $1`, count: 1,
		},
		{
			name: "inside quoted identifier",
			in:   `SELECT "we?rd" FROM t WHERE id = ?`,
			want: `SELECT "we?rd" FROM t WHERE id = $1`, count: 1,
		},
		{
			name: "doubled quote inside identifier",
			in:   `SELECT "a""?b", ?`,
			want: `SELECT "a""?b", $1`, count: 1,
		},
		{
			name: "inside dollar-quoted string",
			in:   "SELECT $$ what? $$, ?",
			want: "SELECT $$ what? $$, $1", count: 1,
		},
		{
			name: "inside tagged dollar quote",
			in:   "SELECT $tag$ a ? b $tag$, ?",
			want: "SELECT $tag$ a ? b $tag$, $1", count: 1,
		},

		// Comments are not executed, so a `?` there must survive untouched.
		{
			name: "inside line comment",
			in:   "SELECT ? -- really?\nFROM t",
			want: "SELECT $1 -- really?\nFROM t", count: 1,
		},
		{
			name: "line comment at end of input",
			in:   "SELECT ? -- trailing?",
			want: "SELECT $1 -- trailing?", count: 1,
		},
		{
			name: "inside block comment",
			in:   "SELECT /* huh? */ ?",
			want: "SELECT /* huh? */ $1", count: 1,
		},
		{
			name: "nested block comment",
			in:   "SELECT /* a /* ? */ ? */ ?",
			want: "SELECT /* a /* ? */ ? */ $1", count: 1,
		},

		// The JSON existence operators share the `?` character. A MySQL client
		// never sends them, and a placeholder cannot legally be followed by
		// these characters, so leaving them alone cannot misread a client.
		{
			name: "JSON some-exists operator",
			in:   `SELECT data ?| ARRAY['a'] FROM t WHERE id = ?`,
			want: `SELECT data ?| ARRAY['a'] FROM t WHERE id = $1`, count: 1,
		},
		{
			name: "JSON all-exists operator",
			in:   `SELECT data ?& ARRAY['a'], ?`,
			want: `SELECT data ?& ARRAY['a'], $1`, count: 1,
		},
		{
			name: "help token",
			in:   "SELECT ??",
			want: "SELECT ??", count: 0,
		},

		// A client that already numbered its placeholders is left alone, so a
		// caller mixing conventions does not get them renumbered underneath it.
		{
			name: "already dollar-numbered",
			in:   "SELECT * FROM t WHERE id = $1",
			want: "SELECT * FROM t WHERE id = $1", count: 0,
		},
		{
			name: "unterminated literal is not rewritten inside",
			in:   "SELECT 'oops ?",
			want: "SELECT 'oops ?", count: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, n := rewritePlaceholders(tc.in)
			if got != tc.want {
				t.Errorf("rewrote\n  %q\nas\n  %q\nwant\n  %q", tc.in, got, tc.want)
			}
			if n != tc.count {
				t.Errorf("counted %d placeholders, want %d", n, tc.count)
			}
		})
	}
}

// TestRewritePlaceholdersPreservesUnrelatedStatements checks the fast path:
// a statement with no question mark at all must come back byte-identical.
func TestRewritePlaceholdersPreservesUnrelatedStatements(t *testing.T) {
	for _, q := range []string{
		"SELECT 1",
		"INSERT INTO t VALUES ($1, $2)",
		"SELECT 'a string', \"an ident\", $$dollar$$ -- comment\n",
		"CREATE TABLE t (id INT PRIMARY KEY, v STRING)",
	} {
		got, n := rewritePlaceholders(q)
		if got != q {
			t.Errorf("%q was altered to %q", q, got)
		}
		if n != 0 {
			t.Errorf("%q reported %d placeholders", q, n)
		}
	}
}
