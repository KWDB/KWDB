// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"hash/maphash"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestArrowBytesTypeSupport locks the foundational BYTES -> arrow.Binary type
// support introduced for the Arrow pipeline:
//   1. buildArrowColumns decodes BytesFamily into an arrow.Binary column (no
//      longer "unsupported type family BytesFamily for arrow schema").
//   2. The grouping hash/equality machinery treats equal byte sequences as the
//      same group and distinct byte sequences as different groups (the silent
//      correctness bug that previously made GROUP BY / DISTINCT / JOIN on BYTES
//      produce wrong results).
//
// Note: this exercises the schema/builder + grouping layers directly. The
// compute-filter kernel path for BYTES (arrow/compute FilterBinary) is a
// separate, still-deferred gap tracked in docs/arrow-unify-roadmap.md §6.3.
func TestArrowBytesTypeSupport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	bytesTyp := types.Bytes

	// Round-trip Bytes -> arrow.Binary via buildArrowColumns.
	rows := sqlbase.EncDatumRows{
		{sqlbase.DatumToEncDatum(bytesTyp, tree.NewDBytes(tree.DBytes("aaa")))},
		{sqlbase.DatumToEncDatum(bytesTyp, tree.NewDBytes(tree.DBytes("bbb")))},
		{sqlbase.DatumToEncDatum(bytesTyp, tree.NewDBytes(tree.DBytes("aaa")))},
	}
	cols, err := buildArrowColumns(alloc, []*types.T{bytesTyp}, rows, &sqlbase.DatumAlloc{})
	if err != nil {
		t.Fatalf("buildArrowColumns: %v", err)
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	col := cols[0]
	if col.DataType().ID() != arrow.BINARY {
		t.Fatalf("expected arrow.BINARY column, got %s", col.DataType())
	}
	barr := col.(*array.Binary)
	if barr.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", barr.Len())
	}
	wantVals := [][]byte{[]byte("aaa"), []byte("bbb"), []byte("aaa")}
	for i, w := range wantVals {
		if string(barr.Value(i)) != string(w) {
			t.Fatalf("row %d: got %q want %q", i, barr.Value(i), w)
		}
	}

	rec := array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: "col0", Type: arrow.BinaryTypes.Binary}}, nil),
		[]arrow.Array{col}, 3,
	)
	defer rec.Release()

	// Grouping correctness: rows 0 and 2 are equal ("aaa"), row 1 differs.
	var h maphash.Hash
	h0 := arrowGroupHashIdx(rec, []int{0}, 0, &h)
	h1 := arrowGroupHashIdx(rec, []int{0}, 1, &h)
	h2 := arrowGroupHashIdx(rec, []int{0}, 2, &h)
	if h0 != h2 {
		t.Errorf("equal bytes rows hashed differently: h0=%d h2=%d", h0, h2)
	}
	if h0 == h1 {
		t.Errorf("distinct bytes rows hashed identically: h0=%d h1=%d", h0, h1)
	}
	if !arrowGroupRowEqualIdx(rec, []int{0}, 0, 2) {
		t.Errorf("equal bytes rows reported unequal")
	}
	if arrowGroupRowEqualIdx(rec, []int{0}, 0, 1) {
		t.Errorf("distinct bytes rows reported equal")
	}

	// arrowGroupKeyEqual against a captured 1-row key record ("aaa").
	keyRows := sqlbase.EncDatumRows{
		{sqlbase.DatumToEncDatum(bytesTyp, tree.NewDBytes(tree.DBytes("aaa")))},
	}
	keyCols, err := buildArrowColumns(alloc, []*types.T{bytesTyp}, keyRows, &sqlbase.DatumAlloc{})
	if err != nil {
		t.Fatalf("buildArrowColumns key: %v", err)
	}
	defer func() {
		for _, c := range keyCols {
			c.Release()
		}
	}()
	keyRec := array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: "col0", Type: arrow.BinaryTypes.Binary}}, nil),
		[]arrow.Array{keyCols[0]}, 1,
	)
	defer keyRec.Release()
	if !arrowGroupKeyEqual(rec, []int{0}, 0, keyRec) {
		t.Errorf("row 0 should match captured 'aaa' key")
	}
	if arrowGroupKeyEqual(rec, []int{0}, 1, keyRec) {
		t.Errorf("row 1 ('bbb') should not match captured 'aaa' key")
	}
}
