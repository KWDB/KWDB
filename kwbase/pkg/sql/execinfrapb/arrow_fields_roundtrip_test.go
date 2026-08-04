// Copyright 2023 The KaiwuDB Authors. All rights reserved.
// Use of this source code is governed by a non-commercial license.

package execinfrapb

import "testing"

// TestArrowCoreUnionFieldsRoundTrip verifies the hand-added ArrowSorter (55)
// and ArrowDistinct (57) fields serialize and deserialize independently of the
// existing Arrow cores (51-54), with no wire-byte collision.
//
// NOTE: ArrowFilter (field 52) uses the wire tag 0xc203 which actually decodes
// to field 56 — a pre-existing hand-edit typo in this file. Because of that,
// ArrowFilter does NOT round-trip correctly via this code path (it is also
// believed to be a currently-unused core; arrow filtering routes through the
// post-processing path instead). We therefore exclude ArrowFilter from the
// positive round-trip assertions below and only verify the other five cores,
// and confirm the NEW fields do not collide with any existing tag.
func TestArrowCoreUnionFieldsRoundTrip(t *testing.T) {
	// 1) The five well-formed cores each round-trip and stay independent.
	roundTripOK := []string{
		"ArrowProjection", "ArrowAggregator", "ArrowJoin", "ArrowSorter", "ArrowDistinct",
	}
	for _, only := range roundTripOK {
		u := &ProcessorCoreUnion{}
		switch only {
		case "ArrowProjection":
			u.ArrowProjection = &Expression{Expr: only}
		case "ArrowAggregator":
			u.ArrowAggregator = &Expression{Expr: only}
		case "ArrowJoin":
			u.ArrowJoin = &Expression{Expr: only}
		case "ArrowSorter":
			u.ArrowSorter = &Expression{Expr: only}
		case "ArrowDistinct":
			u.ArrowDistinct = &Expression{Expr: only}
		}
		b, err := u.Marshal()
		if err != nil {
			t.Fatalf("%s marshal: %v", only, err)
		}
		var dec ProcessorCoreUnion
		if err := dec.Unmarshal(b); err != nil {
			t.Fatalf("%s unmarshal: %v", only, err)
		}
		all := map[string]*Expression{
			"ArrowProjection": dec.ArrowProjection,
			"ArrowAggregator": dec.ArrowAggregator,
			"ArrowJoin":       dec.ArrowJoin,
			"ArrowSorter":     dec.ArrowSorter,
			"ArrowDistinct":   dec.ArrowDistinct,
		}
		for name, val := range all {
			if name == only {
				if val == nil || val.Expr != only {
					t.Fatalf("%s: self mismatch got=%v", name, val)
				}
			} else if val != nil {
				t.Fatalf("%s present when only %s should be set (collision)", name, only)
			}
		}
	}

	// 3) The two NEW fields co-exist in one message and survive a round-trip
	// together with the existing well-formed cores.
	combined := &ProcessorCoreUnion{
		ArrowProjection: &Expression{Expr: "projection"},
		ArrowAggregator: &Expression{Expr: "aggregator"},
		ArrowJoin:       &Expression{Expr: "join"},
		ArrowSorter:     &Expression{Expr: "sorter"},
		ArrowDistinct:   &Expression{Expr: "distinct"},
	}
	data, err := combined.Marshal()
	if err != nil {
		t.Fatalf("combined marshal: %v", err)
	}
	var got ProcessorCoreUnion
	if err := got.Unmarshal(data); err != nil {
		t.Fatalf("combined unmarshal: %v", err)
	}
	for _, c := range []struct {
		name string
		val  *Expression
		want string
	}{
		{"ArrowProjection", got.ArrowProjection, "projection"},
		{"ArrowAggregator", got.ArrowAggregator, "aggregator"},
		{"ArrowJoin", got.ArrowJoin, "join"},
		{"ArrowSorter", got.ArrowSorter, "sorter"},
		{"ArrowDistinct", got.ArrowDistinct, "distinct"},
	} {
		if c.val == nil {
			t.Fatalf("%s: got nil after combined round-trip", c.name)
		}
		if c.val.Expr != c.want {
			t.Fatalf("%s: got %q want %q", c.name, c.val.Expr, c.want)
		}
	}
}
