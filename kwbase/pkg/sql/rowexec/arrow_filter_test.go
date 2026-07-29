// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import "testing"

// TestLikeMatch locks the SQL LIKE semantics implemented by likeMatch:
// '%' matches any sequence, '_' matches exactly one character, and the
// default escape '\' quotes the next character.
func TestLikeMatch(t *testing.T) {
	cases := []struct {
		s, pat string
		ci     bool
		want   bool
	}{
		{"alice", "a%", false, true},
		{"alice", "a%e", false, true},
		{"alice", "b%", false, false},
		{"alice", "a__i%", false, false}, // too few chars before 'i'
		{"alice", "a_ice", false, true},
		{"alice", "_____", false, true}, // exactly five chars
		{"alice", "______", false, false},
		{"alice", "%i%", false, true},
		{"alice", "%x%", false, false},
		{"alice", "ALICE", false, false},
		{"alice", "ALICE", true, true}, // case-insensitive
		{"alice", "A%", true, true},
		{"100", "1%", false, true},
		{"a%b", `a\%b`, false, true}, // escaped '%' is a literal
		{"axb", `a\%b`, false, false},
		{"a_b", `a\_b`, false, true}, // escaped '_' is a literal
		{"axyb", `a\_b`, false, false},
		{"", "%", false, true}, // empty matches '%'
		{"", "a%", false, false},
		{"a", "_", false, true},
	}
	for _, c := range cases {
		if got := likeMatch(c.s, c.pat, c.ci); got != c.want {
			t.Errorf("likeMatch(%q, %q, ci=%v) = %v, want %v", c.s, c.pat, c.ci, got, c.want)
		}
	}
}
